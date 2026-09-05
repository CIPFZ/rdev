package broker

import "sync"

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
