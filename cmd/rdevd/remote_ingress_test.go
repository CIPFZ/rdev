package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestRemoteBrokerIngressIsolation(t *testing.T) {
	d, _, sshRun := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "ingress-shared", ProjectID: "pressure"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "other-project"}
	p := broker.NewPolicy()
	for _, owner := range []broker.Owner{a, b} {
		if err := p.Grant(owner.Key(), "status"); err != nil {
			t.Fatal(err)
		}
		if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(proto.OpPing), proto.OpPing); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(d.dir, "ingress-response-gate")
	t.Cleanup(func() { _ = os.Remove(gate) })
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(mutationSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_MUTATION_GATE="+gate)
	d.start()
	// Reuse provisioned tokens so race-instrumented administrative process exit
	// delay cannot consume a partial-frame timeout during a pressure loop.
	ta, tb := d.token(a, "5m"), d.token(b, "5m")
	wa, wb := d.dial(a, ta, true), d.dial(b, tb, true)
	status := func(owner broker.Owner, w *runtimeWire) broker.Response {
		r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: "status"})
		if !r.OK || r.Ingress == nil || r.Scheduler == nil {
			t.Fatal("missing owned ingress status")
		}
		return r
	}
	pingB := func() {
		r := policyRuntimeRequest(t, wb, broker.Request{Owner: b, Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
		if !r.OK || r.Wire == nil || !r.Wire.OK {
			t.Fatal("other project lost remote ping", r.Error)
		}
	}
	agentPID := func() int {
		out, err := sshRun(remoteAgentPIDScript)
		var pids []int
		if err != nil || json.Unmarshal(out, &pids) != nil || len(pids) != 1 {
			t.Fatalf("expected one real base agent: %v %s", err, out)
		}
		return pids[0]
	}
	pingB()
	beforePID := agentPID()
	var pressure []*runtimeWire
	for range broker.MaxOwnerConnections - 1 {
		pressure = append(pressure, d.dial(a, ta, true))
	}
	excess := d.dial(a, ta, false)
	excess.Close()
	if r := status(a, wa); r.Ingress.Connections != broker.MaxOwnerConnections {
		t.Fatal("owner connection ceiling not reached", r.Ingress)
	}
	if r := status(b, wb); r.Ingress.Connections != 1 {
		t.Fatal("status revealed other project connections", r.Ingress)
	}
	extraB := d.dial(b, tb, true)
	extraB.Close()
	pingB()
	for _, w := range pressure {
		w.Close()
	}
	awaitRuntime(t, 3*time.Second, "connection credits returned", func() bool { return status(a, wa).Ingress.Connections == 1 })

	// Saturating anonymous admission never closes an established principal.
	var anonymous []net.Conn
	for range broker.MaxUnauthenticatedConnections {
		c, err := net.DialTimeout("unix", d.socket, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		anonymous = append(anonymous, c)
		t.Cleanup(func() { c.Close() })
	}
	pingB()
	handshakeStarted := time.Now()
	lateByte := make(chan struct{})
	go func() {
		defer close(lateByte)
		time.Sleep(3 * time.Second)
		_, _ = anonymous[0].Write([]byte("{"))
	}()
	for _, c := range anonymous {
		_ = c.SetReadDeadline(time.Now().Add(brokerFrameTimeout + 2*time.Second))
		var byte [1]byte
		_, err := c.Read(byte[:])
		if err == nil {
			t.Fatal("anonymous socket survived handshake deadline")
		}
		if e, ok := err.(net.Error); ok && e.Timeout() {
			t.Fatal("daemon did not expire anonymous handshake")
		}
		c.Close()
	}
	<-lateByte
	if time.Since(handshakeStarted) > brokerFrameTimeout+time.Second {
		t.Fatal("late first byte extended absolute handshake deadline")
	}
	extraB = d.dial(b, tb, true)
	extraB.Close()

	// Each partial document is smaller than the document limit; their aggregate
	// exceeds only A's byte quota. The fifth writer must be cut off promptly.
	var partial []*runtimeWire
	payload := []byte(`{"target":"` + strings.Repeat("x", 7<<20))
	for range 4 {
		w := d.dial(a, ta, true)
		partial = append(partial, w)
		_ = w.SetWriteDeadline(time.Now().Add(3 * time.Second))
		if _, err := w.Write(payload); err != nil {
			t.Fatal("partial document below owner budget rejected", err)
		}
	}
	if r := status(a, wa); r.Ingress.Bytes < 27<<20 || r.Ingress.Bytes > broker.MaxOwnerIngressBytes {
		t.Fatal("partial document accounting invalid", r.Ingress)
	}
	if r := status(b, wb); r.Ingress.Bytes > 16<<10 {
		t.Fatal("status revealed other owner bytes", r.Ingress)
	}
	last := d.dial(a, ta, true)
	_ = last.SetDeadline(time.Now().Add(3 * time.Second))
	_, quotaErr := last.Write(payload)
	if quotaErr == nil {
		var byte [1]byte
		_, quotaErr = last.Read(byte[:])
	}
	if quotaErr == nil {
		t.Fatal("owner byte quota did not reject excess")
	}
	if e, ok := quotaErr.(net.Error); ok && e.Timeout() {
		t.Fatal("owner quota rejection waited for frame timeout")
	}

	last.Close()
	pingB()
	for _, w := range partial {
		w.Close()
	}
	awaitRuntime(t, 3*time.Second, "partial request credits returned", func() bool { r := status(a, wa); return r.Ingress.Connections == 1 && r.Ingress.Bytes < 16<<10 })
	oversized := d.dial(a, ta, true)
	_ = oversized.SetDeadline(time.Now().Add(3 * time.Second))
	_, err := oversized.Write([]byte(`{"target":"` + strings.Repeat("x", int(maxBrokerRequestBytes)+1)))
	if err == nil {
		var byte [1]byte
		_, err = oversized.Read(byte[:])
	}
	if err == nil {
		t.Fatal("oversized unterminated JSON accepted")
	}
	if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("oversize detection waited for timeout")
	}
	oversized.Close()

	// A valid large echoed request ID fills the real Unix response socket. The
	// peer deliberately never reads it; the write deadline must reclaim it.
	slow := d.dial(a, ta, true)
	_ = slow.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := slow.enc.Encode(broker.Request{ID: strings.Repeat("s", 1<<20), Owner: a, Operation: "status"}); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, time.Second, "slow response retains request charge", func() bool { r := status(a, wa); return r.Ingress.Connections == 2 && r.Ingress.Bytes >= 1<<20 })
	awaitRuntime(t, brokerWriteTimeout+2*time.Second, "slow-reader response resources released", func() bool { return status(a, wa).Ingress.Connections == 1 })
	slow.Close()
	pingB()

	// Hold a real SSH reply so queue overflow cannot depend on dispatch speed.
	// The active request must cancel even though the decoder's queue is full.
	op := "op_runtime_ingress_held_ping"
	data, _ := json.Marshal(map[string]string{"operation_id": op})
	if err := os.WriteFile(gate, data, 0600); err != nil {
		t.Fatal(err)
	}
	held := d.dial(a, ta, true)
	if err := held.enc.Encode(broker.Request{ID: "held", Owner: a, Host: "runtime-host", Operation: proto.OpPing, Wire: &proto.Request{Op: proto.OpPing, OperationID: op}}); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "real ping response held", func() bool { _, err := os.Stat(gate + ".entered"); return err == nil })
	if r := status(a, wa); r.Scheduler.Active != 1 {
		t.Fatal("held request not active")
	}
	for range 4 {
		if err := held.enc.Encode(broker.Request{Owner: a, Operation: "status"}); err != nil {
			break
		}
	}
	awaitRuntime(t, 3*time.Second, "pipeline overflow cancels active owner request", func() bool {
		r := status(a, wa)
		return r.Scheduler.Active == 0 && r.Scheduler.Queued == 0 && r.Ingress.Connections == 1
	})
	held.Close()
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	pingB()
	if after := agentPID(); after != beforePID {
		t.Fatalf("ingress pressure replaced shared agent: %d -> %d", beforePID, after)
	}
	t.Logf("real daemon ingress: owner cap=%d; anonymous cap=%d and handshake expiry; 28 MiB charged partial JSON plus excess rejected by owner byte budget; oversize frame rejected; 2-second slow-reader cleanup; full pipeline cancels real held SSH ping; exact-project status isolation; shared remote agent PID %d unchanged", broker.MaxOwnerConnections, broker.MaxUnauthenticatedConnections, beforePID)
}
