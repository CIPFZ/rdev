package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func qosSecretOwners(t *testing.T, p *broker.Policy) []broker.Owner {
	t.Helper()
	owners := []broker.Owner{{ClientID: "qos-credential", ProjectID: "archive-a"}, {ClientID: "qos-credential", ProjectID: "archive-b"}}
	if err := p.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, o := range owners {
		for _, op := range []string{"secret.set", "secret.list", proto.OpReadFile} {
			if err := p.GrantHost(o.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	return owners
}

// Populate via authenticated, separately approved mutations, not a hand-written
// persistence fixture. Each project reaches its 256-version retention limit;
// the earlier 255 values remain redactors although only the last is injectable.
func provisionQoSSecrets(t *testing.T, d *runtimeDaemon, owners []broker.Owner, namespace string, ssh func(string) ([]byte, error)) {
	t.Helper()
	admin := runtimeApprovalAdmin()
	adminWire := d.dial(admin, d.token(admin, "10m"), true)
	values := make([]string, 0, len(owners)*256)
	for i, o := range owners {
		wire := d.dial(o, d.token(o, "10m"), true)
		for j := 0; j < 256; j++ {
			value := fmt.Sprintf("qos-credential-%02d-%03d-\"quoted\"-雪\nend", i, j)
			// Whitespace-only accepted values must not disable the fast path
			// for every other project's ordinary output.
			if j == 0 {
				value = strings.Repeat(" ", 6)
			}
			if j == 1 {
				value = strings.Repeat("\t", 16)
			}
			values = append(values, value)
			secret := &broker.SecretParams{Name: "rotating", Value: value}
			approval := policyRuntimeRequest(t, adminWire, broker.Request{Owner: admin, Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: o, Host: "runtime-host", Operation: "secret.set", Secret: secret, TTL: time.Minute}})
			if !approval.OK || approval.Approval == nil {
				t.Fatal("archive approval failed")
			}
			id, err := proto.NewOperationID()
			if err != nil {
				t.Fatal(err)
			}
			result := policyRuntimeRequest(t, wire, broker.Request{Owner: o, Operation: "secret.set", Host: "runtime-host", Secret: secret, OperationID: id, Approval: approval.Approval.Token})
			if !result.OK || result.Mutation == nil || result.Mutation.State != "completed" {
				t.Fatal("archive mutation failed")
			}
		}
		wire.Close()
	}
	adminWire.Close()
	raw, err := os.ReadFile(d.socket + ".secrets")
	if err != nil {
		t.Fatal(err)
	}
	var archive struct{ Records []struct{ Active bool } }
	if json.Unmarshal(raw, &archive) != nil || len(archive.Records) != len(values) {
		t.Fatal("missing retained versions")
	}
	active := 0
	for _, record := range archive.Records {
		if record.Active {
			active++
		}
	}
	if active != len(owners) {
		t.Fatal("rotation retained injection authority")
	}
	// Positive checks include every retired value, on both sides of SIGKILL.
	data, _ := json.Marshal(strings.Join(values, "\n"))
	script := "import os,json\np=os.path.expanduser('~/" + namespace + "/archive-fixture')\nwith open(p,'w') as f: f.write(json.loads(" + fmt.Sprintf("%q", string(data)) + "))\n"
	if _, err := ssh(script); err != nil {
		t.Fatal("archive fixture creation failed")
	}
	verify := func() {
		o := owners[0]
		wire := d.dial(o, d.token(o, "10m"), true)
		defer wire.Close()
		r := policyRuntimeRequest(t, wire, broker.Request{Owner: o, Operation: proto.OpReadFile, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: namespace + "/archive-fixture", Limit: 65536}}})
		if !r.OK || r.Wire == nil || r.Wire.Read == nil {
			t.Fatal("archive redaction read failed")
		}
		for _, value := range values {
			if strings.Contains(r.Wire.Read.Content, value) {
				t.Fatal("retained credential disclosed")
			}
		}
		if strings.Count(r.Wire.Read.Content, "<redacted:broker_") != len(values) {
			t.Fatal("missing retained value redaction")
		}
		list := policyRuntimeRequest(t, wire, broker.Request{Owner: o, Operation: "secret.list", Host: "runtime-host", Secret: &broker.SecretParams{}})
		if !list.OK || len(list.Secrets) != 1 {
			t.Fatal("archive active project scope failed")
		}
	}
	verify()
	d.stop(syscall.SIGKILL)
	d.start()
	verify()
	t.Logf("real approved secret archive: %d versions, %d active, %d retired; all versions redacted before and after SIGKILL", len(values), active, len(values)-active)
}
