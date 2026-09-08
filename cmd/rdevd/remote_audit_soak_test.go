package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

const auditSoakCanary = "audit-soak-request-payload-must-not-enter-any-audit-segment"

type auditSoakChild struct {
	cmd    *exec.Cmd
	done   chan error
	log    bytes.Buffer
	result string
}

func TestRemoteBrokerAuditSoak(t *testing.T) {
	if os.Getenv("RDEV_AUDIT_SOAK_HELPER") == "1" {
		runAuditSoakClient(t)
		return
	}
	d, _, sshRun := newRemoteRuntime(t)
	seconds := 600
	if value := os.Getenv("RDEV_AUDIT_SOAK_SECONDS"); value != "" {
		var err error
		seconds, err = strconv.Atoi(value)
		if err != nil {
			t.Fatal("invalid audit soak duration")
		}
	}
	if seconds < 120 || seconds > 3600 || seconds%60 != 0 {
		t.Fatal("audit soak requires 120..3600 seconds in full minute epochs")
	}
	a := broker.Owner{ClientID: "audit-soak", ProjectID: "a"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "b"}
	admin := broker.Owner{ClientID: "audit-soak-health", ProjectID: "operations"}
	p := broker.NewPolicy()
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{"status", "audit_query"} {
			if err := p.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(proto.OpPing), proto.OpPing); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Grant(admin.Key(), "audit.health"); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	tokens := map[broker.Owner]string{}
	for _, owner := range []broker.Owner{a, b, admin} {
		tokens[owner] = d.token(owner, "2h")
	}
	var unclean, totalCalls, totalWritten, totalRotations uint64
	var maxRSSKB, maxFD int
	started := time.Now()
	health := func() *broker.AuditSinkStatus {
		w := d.dial(admin, tokens[admin], true)
		defer w.Close()
		r := policyRuntimeRequest(t, w, broker.Request{Owner: admin, Operation: "audit.health"})
		if !r.OK || r.AuditHealth == nil {
			t.Fatal("missing audit soak health")
		}
		return r.AuditHealth
	}
	verifyOwners := func() {
		for _, owner := range []broker.Owner{a, b} {
			w := d.dial(owner, tokens[owner], true)
			r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: "audit_query"})
			w.Close()
			if !r.OK || r.AuditIncomplete != (unclean > 0) {
				t.Fatal("audit soak query omitted gap state")
			}
			for _, e := range r.Audit {
				if e.Owner != broker.AuditOwnerID(owner.Key()) {
					t.Fatal("audit soak recovery crossed project")
				}
			}
		}
	}
	for epoch := 0; epoch < seconds/60; epoch++ {
		d.start()
		verifyOwners()
		if st := health(); st.UncleanRecoveries != unclean || st.Incomplete != (unclean > 0) {
			t.Fatal("audit soak lost durable recovery count")
		}
		gate := filepath.Join(d.dir, fmt.Sprintf("audit-start-%d", epoch))
		var children []*auditSoakChild
		pids := map[int]bool{}
		for i := 0; i < 20; i++ {
			owner := a
			if i >= 10 {
				owner = b
			}
			result := filepath.Join(d.dir, fmt.Sprintf("audit-result-%d-%d", epoch, i))
			cmd := exec.Command(os.Args[0], "-test.run=^TestRemoteBrokerAuditSoak$", "-test.timeout=90s")
			cmd.Env = append(os.Environ(), "RDEV_AUDIT_SOAK_HELPER=1", "RDEV_AUDIT_SOAK_START="+gate, "RDEV_AUDIT_SOAK_RESULT="+result, "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+owner.ClientID, "RDEV_PROJECT_ID="+owner.ProjectID, "RDEV_PRINCIPAL_TOKEN="+tokens[owner])
			child := &auditSoakChild{cmd: cmd, done: make(chan error, 1), result: result}
			cmd.Stdout, cmd.Stderr = &child.log, &child.log
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			go func() { child.done <- cmd.Wait() }()
			children = append(children, child)
			pids[cmd.Process.Pid] = true
		}
		awaitRuntime(t, 5*time.Second, "twenty audit producers ready", func() bool {
			for _, child := range children {
				if _, err := os.Stat(child.result + ".ready"); err != nil {
					return false
				}
			}
			return true
		})
		if len(pids) != 20 {
			t.Fatal("audit producers not independent processes")
		}
		if err := os.WriteFile(gate, nil, 0600); err != nil {
			t.Fatal(err)
		}
		for range 60 {
			time.Sleep(time.Second)
			for _, child := range children {
				select {
				case err := <-child.done:
					t.Fatalf("audit producer exited before epoch end: %v %s", err, child.log.String())
				default:
				}
			}
			st := health()
			if st.Dropped != 0 || st.Errors != 0 {
				t.Fatalf("audit soak lost events or sink failed: %+v", st)
			}
			status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", d.cmd.Process.Pid))
			if err != nil {
				t.Fatal(err)
			}
			for _, line := range strings.Split(string(status), "\n") {
				if strings.HasPrefix(line, "VmRSS:") {
					fields := strings.Fields(line)
					n, _ := strconv.Atoi(fields[1])
					maxRSSKB = max(maxRSSKB, n)
				}
			}
			fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", d.cmd.Process.Pid))
			if err != nil {
				t.Fatal(err)
			}
			maxFD = max(maxFD, len(fds))
		}
		verifyOwners()
		st := health()
		if st.Rotations == 0 || st.Dropped != 0 || st.Errors != 0 {
			t.Fatalf("minute of live audit pressure did not rotate cleanly: %+v", st)
		}
		totalWritten += st.Written
		totalRotations += st.Rotations
		for _, suffix := range []string{".audit", ".audit.1"} {
			path := d.socket + suffix
			data, err := os.ReadFile(path)
			if err != nil || len(data) > 8<<20 || bytes.Contains(data, []byte(auditSoakCanary)) {
				t.Fatal("audit retention/privacy bound failed")
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("audit segment not private")
			}
		}
		out, err := sshRun(remoteAgentPIDScript)
		var agents []int
		if err != nil || json.Unmarshal(out, &agents) != nil || len(agents) != 1 || countDaemonSSHChildren(t, d.cmd.Process.Pid) != 1 {
			t.Fatal("audit soak did not share one real base SSH agent")
		}
		signal := syscall.SIGTERM
		if epoch%2 == 0 {
			signal = syscall.SIGKILL
			unclean++
		}
		d.stop(signal)
		var calls uint64
		for _, child := range children {
			select {
			case err := <-child.done:
				if err != nil {
					t.Fatalf("audit producer failed: %v %s", err, child.log.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("audit producer stranded after daemon exit")
			}
			data, err := os.ReadFile(child.result)
			var count uint64
			if err != nil || json.Unmarshal(data, &count) != nil || count < 500 {
				t.Fatal("audit producer lacked sustained completed calls")
			}
			calls += count
		}
		totalCalls += calls
		t.Logf("audit epoch=%d: processes=20 calls=%d written_before_stop=%d rotations=%d signal=%s unclean_recoveries_expected=%d max_rss_kb=%d max_fd=%d", epoch+1, calls, st.Written, st.Rotations, signal, unclean, maxRSSKB, maxFD)
	}
	d.start()
	verifyOwners()
	if st := health(); st.UncleanRecoveries != unclean || !st.Incomplete {
		t.Fatal("audit soak final restart erased crash history")
	}
	d.stop(syscall.SIGTERM)
	t.Logf("real audit soak completed: active_seconds=%d elapsed_seconds=%.3f calls=%d written_before_stops=%d rotations=%d unclean_recoveries=%d max_rss_kb=%d max_fd=%d; bounded private segments, exact-project queries, zero reported sink drops/errors, explicit persistent possible crash-tail loss", seconds, time.Since(started).Seconds(), totalCalls, totalWritten, totalRotations, unclean, maxRSSKB, maxFD)
}

func runAuditSoakClient(t *testing.T) {
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	ctx, cancel := context.WithTimeout(context.Background(), 85*time.Second)
	defer cancel()
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result := os.Getenv("RDEV_AUDIT_SOAK_RESULT")
	if err := os.WriteFile(result+".ready", nil, 0600); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "audit start barrier", func() bool { _, err := os.Stat(os.Getenv("RDEV_AUDIT_SOAK_START")); return err == nil })
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var calls uint64
	for {
		select {
		case <-ctx.Done():
			t.Fatal("audit producer deadline expired")
		case <-ticker.C:
		}
		req := broker.Request{Owner: owner, Operation: "status", Target: auditSoakCanary}
		if calls%10 == 0 {
			req.Operation = proto.OpPing
			req.Host = "runtime-host"
			req.Wire = &proto.Request{Op: proto.OpPing}
		}
		response, err := c.DoContext(ctx, req)
		if err != nil || !response.OK {
			break
		}
		calls++
	}
	data, _ := json.Marshal(calls)
	if err := os.WriteFile(result, data, 0600); err != nil {
		t.Fatal(err)
	}
}
