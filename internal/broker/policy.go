package broker

import (
	"encoding/json"
	"os"
	"sync"
)

type Decision struct {
	Allow  bool
	Reason string
}
type Policy struct {
	mu     sync.RWMutex
	grants map[string]map[string]bool
}

func NewPolicy() *Policy { return &Policy{grants: make(map[string]map[string]bool)} }
func (p *Policy) Grant(owner, operation string) {
	p.mu.Lock()
	if p.grants[owner] == nil {
		p.grants[owner] = make(map[string]bool)
	}
	p.grants[owner][operation] = true
	p.mu.Unlock()
}
func (p *Policy) Decide(owner, operation string) Decision {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.grants[owner][operation] {
		return Decision{Allow: true, Reason: "granted"}
	}
	return Decision{Reason: "denied by default"}
}

func (p *Policy) Save(path string) error {
	p.mu.RLock()
	data, err := json.Marshal(p.grants)
	p.mu.RUnlock()
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (p *Policy) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var grants map[string]map[string]bool
	if err := json.Unmarshal(data, &grants); err != nil {
		return err
	}
	p.mu.Lock()
	p.grants = grants
	p.mu.Unlock()
	return nil
}
