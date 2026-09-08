package broker

import (
	"errors"
	"sync"
)

const MaxBrokerConnections = 128
const MaxOwnerConnections = 32
const MaxUnauthenticatedConnections = 16
const MaxIngressBytes int64 = 64 << 20
const MaxOwnerIngressBytes int64 = 32 << 20
const MaxBrokerHelloBytes int64 = 16 << 10

var ErrIngressLimit = errors.New("broker ingress limit reached")

type IngressSnapshot struct {
	Connections      int   `json:"connections"`
	ObservationBytes int64 `json:"observation_request_bytes"`
	Bytes            int64 `json:"reserved_request_bytes"`
}

type Ingress struct {
	mu                     sync.Mutex
	connections, anonymous int
	bytes                  int64
	owners                 map[string]IngressSnapshot
}

type IngressLease struct {
	ingress *Ingress
	owner   string
	bytes   int64
	closed  bool
}

func NewIngress() *Ingress { return &Ingress{owners: make(map[string]IngressSnapshot)} }

// Open runs before spawning a connection goroutine. Anonymous connections
// cannot displace already-authenticated clients. Handshake deadlines in the
// socket server recycle these slots; Open does not promise admission under a
// continuous flood of unauthenticated connection attempts.
func (i *Ingress) Open() (*IngressLease, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.connections >= MaxBrokerConnections || i.anonymous >= MaxUnauthenticatedConnections {
		return nil, ErrIngressLimit
	}
	i.connections++
	i.anonymous++
	return &IngressLease{ingress: i}, nil
}

func (l *IngressLease) Bind(owner Owner) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	i := l.ingress
	i.mu.Lock()
	defer i.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if l.owner != "" {
		if l.owner == owner.Key() {
			return nil
		}
		return errors.New("ingress principal cannot change")
	}
	state := i.owners[owner.Key()]
	if state.Connections >= MaxOwnerConnections || state.Bytes+l.bytes > MaxOwnerIngressBytes {
		return ErrIngressLimit
	}
	l.owner = owner.Key()
	i.anonymous--
	state.Connections++
	state.Bytes += l.bytes
	i.owners[l.owner] = state
	return nil
}

func (l *IngressLease) Reserve(n int64) error {
	if n < 0 {
		return ErrIngressLimit
	}
	i := l.ingress
	i.mu.Lock()
	defer i.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if n > MaxIngressBytes-i.bytes {
		return ErrIngressLimit
	}
	if l.owner == "" && n > MaxBrokerHelloBytes-l.bytes {
		return ErrIngressLimit
	}
	state := i.owners[l.owner]
	if l.owner != "" && n > MaxOwnerIngressBytes-state.Bytes {
		return ErrIngressLimit
	}
	l.bytes += n
	i.bytes += n
	if l.owner != "" {
		state.Bytes += n
		i.owners[l.owner] = state
	}
	return nil
}

func (l *IngressLease) Release(n int64) {
	i := l.ingress
	i.mu.Lock()
	defer i.mu.Unlock()
	if l.closed || n <= 0 {
		return
	}
	n = min(n, l.bytes)
	l.bytes -= n
	i.bytes -= n
	if l.owner != "" {
		state := i.owners[l.owner]
		state.Bytes -= n
		i.owners[l.owner] = state
	}
}

func (l *IngressLease) Close() {
	i := l.ingress
	i.mu.Lock()
	defer i.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	i.connections--
	i.bytes -= l.bytes
	if l.owner == "" {
		i.anonymous--
	} else {
		state := i.owners[l.owner]
		state.Connections--
		state.Bytes -= l.bytes
		if state.Connections == 0 && state.Bytes == 0 {
			delete(i.owners, l.owner)
		} else {
			i.owners[l.owner] = state
		}
	}
	l.bytes = 0
}

func (i *Ingress) Snapshot(owner string) IngressSnapshot {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.owners[owner]
}

// Hold accounts for a detached observation after its initiating socket closes.
// It is additional to the frontend's charge while that frontend remains alive.
func (i *Ingress) Hold(owner string, n int64) (func(), error) {
	i.mu.Lock()
	state := i.owners[owner]
	if owner == "" || n <= 0 || n > MaxIngressBytes-i.bytes || n > MaxOwnerIngressBytes-state.Bytes {
		i.mu.Unlock()
		return nil, ErrIngressLimit
	}
	i.bytes += n
	state.Bytes += n
	state.ObservationBytes += n
	i.owners[owner] = state
	i.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			i.mu.Lock()
			defer i.mu.Unlock()
			i.bytes -= n
			state := i.owners[owner]
			state.Bytes -= n
			state.ObservationBytes -= n
			if state.Connections == 0 && state.Bytes == 0 {
				delete(i.owners, owner)
			} else {
				i.owners[owner] = state
			}
		})
	}, nil
}
