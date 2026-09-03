# Phase 4 connection manager (P4-01–P4-03)

The first Phase 4 slice introduces a reusable transport identity and a small,
race-safe lifecycle manager in `internal/connmgr`.

`transport.CanonicalConnectionKey` hashes the normalized SSH destination,
effective port, remote state directory, authentication identity, proxy and host
key policy. The local alias and `ForceAgentUpload` are intentionally excluded:
aliases can refer to one endpoint, while upload policy does not alter an
already-established transport. The key is opaque, so credentials and paths do
not become metric labels or log values. New profile fields are persisted in the
host registry and are passed as explicit SSH arguments.

The manager coalesces concurrent dials (`COLD → DIALING → WARM`) and publishes a
single dial result to all waiters. Dial failures are observable as `BACKOFF`.
Each lease, queued request and in-flight request is accounted under one mutex.
Eviction first enters `DRAINING`; a new request can cancel that transition while
close has not started. A connection is only moved to `EVICTING` and closed when
lease, in-flight and queue counts are all zero. Close runs outside the state
lock, then the entry is removed and returns to `COLD`, preventing a slow close
from blocking unrelated hosts.

This slice deliberately does not add global dial semaphores, LRU/TTL policy,
ControlMaster ownership, durable jobs, or broker functionality; those remain
the subsequent Phase 4 tasks.
