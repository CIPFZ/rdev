package session

import (
	"testing"

	"github.com/CIPFZ/rdev/internal/transport"
)

func FuzzHostConfig(f *testing.F) {
	for _, s := range []string{`{"hosts":[{"name":"local","addr":"user@[::1]:2222","remote_dir":".cache/rdev-test"}]}`, `{"hosts":[{"name":"bad","addr":"-F/tmp/config"}]}`, `{"hosts":[{"name":"bad","addr":"host","remote_dir":"../escape"}]}`, `{"hosts":null}`, `{"hosts":[]}{}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 64<<10 {
			t.Skip()
		}
		r := NewRegistry()
		err := r.LoadGlobalData("fuzz-config", input)
		if err != nil {
			if len(r.hosts) != 0 {
				t.Fatal("invalid config partially published hosts")
			}
			return
		}
		for _, host := range r.hosts {
			if host.Name == "" || transport.ValidateHost(host) != nil {
				t.Fatal("invalid config reached normalized host")
			}
			normal, err := transport.NormalizeHost(host)
			if err != nil || normal != host {
				t.Fatal("noncanonical config host")
			}
		}
	})
}
