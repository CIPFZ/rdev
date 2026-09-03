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

P4-03A adds bounded warm-host admission (`MaxWarmHosts`), deterministic idle
reaping (`IdleTTL` plus the shorter `LastClientGrace` after the final lease is
released), and least-recently-used eviction when the pool is full. Busy entries
are never selected: they first enter `DRAINING` and close automatically only
after lease, in-flight, and queued counts all reach zero. `Config.Now` makes
policy tests deterministic; `Config.Validate` rejects negative limits and an
inverted TTL pair.

P4-03B adds an optional `GracefulConnection` close hook with a bounded drain
context, followed by idempotent `Close`. Transport connections expose explicit
ControlMaster ownership. A normal `Dial` connection is shared by default and
therefore never sends `ssh -O exit`; only a broker that created the master may
call `SetControlMasterOwnership(true)`. The manager invokes
`CloseControlMaster` only after the connection has drained, so a shared socket
cannot be removed by another profile or process.

P4-04 adds a process-wide FIFO dial semaphore (default six), per-key
singleflight with context cancellation, and bounded exponential reconnect
backoff. A canceled leader releases its per-key flight so a waiter with a live
context can take over; canceled waiters never retain a global dial token.
Failed dials publish one result to all waiters; subsequent attempts wait for the
backoff window, with optional deterministic jitter for tests. Dial failures are
classified into a fixed vocabulary (`canceled`, `timeout`, `auth`, `network`,
`resource`, `unknown`) and lifecycle/disconnect events can be forwarded to the
existing low-cardinality observability registry. Durable jobs and broker
functionality remain outside this slice.
