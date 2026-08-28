package observe

// Phase 3 metrics use closed vocabularies so attacker-controlled identifiers,
// argv, output, paths, credentials, and error text can never create labels.
type RequestEvent string

const (
	RequestQueued    RequestEvent = "request.queued"
	RequestAccepted  RequestEvent = "request.accepted"
	RequestCompleted RequestEvent = "request.completed"
	RequestCanceled  RequestEvent = "request.canceled"
	RequestAmbiguous RequestEvent = "request.ambiguous"
)

var requestEvents = [...]RequestEvent{
	RequestQueued, RequestAccepted, RequestCompleted, RequestCanceled, RequestAmbiguous,
}

type ProtocolEvent string

const (
	ProtocolFrameRejected     ProtocolEvent = "protocol.frame_rejected"
	ProtocolFeatureNegotiated ProtocolEvent = "protocol.feature_negotiated"
	ProtocolStreamRejected    ProtocolEvent = "protocol.stream_rejected"
)

var protocolEvents = [...]ProtocolEvent{
	ProtocolFrameRejected, ProtocolFeatureNegotiated, ProtocolStreamRejected,
}

type ResourceEvent string

const (
	ResourceQueueRejected   ResourceEvent = "resource.queue_rejected"
	ResourceWatcherRejected ResourceEvent = "resource.watcher_rejected"
	ResourceSlowConsumer    ResourceEvent = "resource.slow_consumer"
)

var resourceEvents = [...]ResourceEvent{
	ResourceQueueRejected, ResourceWatcherRejected, ResourceSlowConsumer,
}

type DedupeEvent string

const (
	DedupeHit      DedupeEvent = "dedupe.hit"
	DedupeConflict DedupeEvent = "dedupe.conflict"
	DedupeEvicted  DedupeEvent = "dedupe.evicted"
)

var dedupeEvents = [...]DedupeEvent{DedupeHit, DedupeConflict, DedupeEvicted}

func validRequestEvent(event RequestEvent) bool {
	for _, candidate := range requestEvents {
		if event == candidate {
			return true
		}
	}
	return false
}

func validProtocolEvent(event ProtocolEvent) bool {
	for _, candidate := range protocolEvents {
		if event == candidate {
			return true
		}
	}
	return false
}

func validResourceEvent(event ResourceEvent) bool {
	for _, candidate := range resourceEvents {
		if event == candidate {
			return true
		}
	}
	return false
}

func validDedupeEvent(event DedupeEvent) bool {
	for _, candidate := range dedupeEvents {
		if event == candidate {
			return true
		}
	}
	return false
}

func (r *Registry) Request(event RequestEvent) {
	if r == nil || !validRequestEvent(event) {
		return
	}
	r.mu.Lock()
	r.requestEvents[event]++
	r.mu.Unlock()
}

func (r *Registry) Protocol(event ProtocolEvent) {
	if r == nil || !validProtocolEvent(event) {
		return
	}
	r.mu.Lock()
	r.protocolEvents[event]++
	r.mu.Unlock()
}

func (r *Registry) Resource(event ResourceEvent) {
	if r == nil || !validResourceEvent(event) {
		return
	}
	r.mu.Lock()
	r.resourceEvents[event]++
	r.mu.Unlock()
}

func (r *Registry) Dedupe(event DedupeEvent) {
	if r == nil || !validDedupeEvent(event) {
		return
	}
	r.mu.Lock()
	r.dedupeEvents[event]++
	r.mu.Unlock()
}
