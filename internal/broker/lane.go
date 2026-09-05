package broker

import "sync"

type Lane string

const (
	LaneControl Lane = "control"
	LaneExec    Lane = "exec"
	LaneBulk    Lane = "bulk"
)

// Lanes reserves capacity for control traffic while bounding exec and bulk.
type Lanes struct {
	mu     sync.Mutex
	limits map[Lane]int
	active map[Lane]int
}

func NewLanes(control, exec, bulk int) *Lanes {
	return &Lanes{limits: map[Lane]int{LaneControl: control, LaneExec: exec, LaneBulk: bulk}, active: make(map[Lane]int)}
}

func (l *Lanes) Acquire(kind Lane) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[kind] >= l.limits[kind] {
		return false
	}
	l.active[kind]++
	return true
}
func (l *Lanes) Release(kind Lane) {
	l.mu.Lock()
	if l.active[kind] > 0 {
		l.active[kind]--
	}
	l.mu.Unlock()
}
func (l *Lanes) Active(kind Lane) int { l.mu.Lock(); defer l.mu.Unlock(); return l.active[kind] }
