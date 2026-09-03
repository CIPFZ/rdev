package observe

// ConnectionEvent is the bounded vocabulary used by the connection manager.
// Reasons are likewise bounded so host keys and error strings cannot create
// unbounded metric series.
type ConnectionEvent string

const (
	ConnectionDialStarted   ConnectionEvent = "connection.dial_started"
	ConnectionDialSucceeded ConnectionEvent = "connection.dial_succeeded"
	ConnectionDialFailed    ConnectionEvent = "connection.dial_failed"
	ConnectionBackoff       ConnectionEvent = "connection.backoff"
	ConnectionEvicted       ConnectionEvent = "connection.evicted"
	ConnectionDisconnected  ConnectionEvent = "connection.disconnected"
)

var connectionEvents = [...]ConnectionEvent{
	ConnectionDialStarted, ConnectionDialSucceeded, ConnectionDialFailed,
	ConnectionBackoff, ConnectionEvicted, ConnectionDisconnected,
}

func validConnectionEvent(event ConnectionEvent) bool {
	for _, candidate := range connectionEvents {
		if event == candidate {
			return true
		}
	}
	return false
}

// Connection records a lifecycle event and optional bounded reason.
func (r *Registry) Connection(event ConnectionEvent, reason string) {
	if r == nil || !validConnectionEvent(event) {
		return
	}
	r.mu.Lock()
	if r.connectionEvents == nil {
		r.connectionEvents = make(map[ConnectionEvent]uint64)
	}
	r.connectionEvents[event]++
	if reason != "" {
		if r.connectionReasons == nil {
			r.connectionReasons = make(map[string]uint64)
		}
		if validConnectionReason(reason) {
			r.connectionReasons[reason]++
		}
	}
	sink := r.sink
	r.mu.Unlock()
	if sink != nil {
		sink.Log(Event{Name: string(event), Level: "info"})
	}
}

// RecordConnection lets packages such as connmgr connect without importing
// each other's concrete event types. Unknown names/reasons are ignored.
func (r *Registry) RecordConnection(name, reason string) {
	r.Connection(ConnectionEvent(name), reason)
}

func validConnectionReason(reason string) bool {
	switch reason {
	case "canceled", "timeout", "auth", "network", "resource", "unknown", "LRU", "idle TTL", "drain complete", "health_failed", "explicit":
		return true
	default:
		return false
	}
}
