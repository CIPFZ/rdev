# Phase 5 acceptance and risk record

This record is maintained against the P5 requirements in
`docs/rdev-evolution-security-plan.md`. A green repository test run is not
treated as proof of a production integration requirement unless the listed
runtime path is exercised.

| Item | Current evidence | Status | Remaining risk |
|---|---|---|---|
| P5-01 | Broker hello version/min-version negotiation and incompatible-peer tests | Complete | Cross-version release matrix still needs CI coverage |
| P5-02 | 0600 socket, 0700 parent, flock lock, stale socket recovery, peer credentials | Complete | Cross-UID integration test is platform-dependent |
| P5-03 | `rdevd` owns `client.Client`, host registry, secrets, configurable agent lookup, wire dispatch; real Unix multi-client test | In progress | Installed frontend adoption and multi-process remote-session benchmark remain |
| P5-04 | Owner validation and wire client/project binding; persisted job owner | Complete | Capability administration is still internal |
| P5-05 | Per-connection context cancellation and shared client pool | In progress | Cancellation under an in-flight transport retry needs an integration test |
| P5-06 | Quota admission held through dispatch, per-host accounting, and multi-client Unix test | In progress | Queued quota pressure needs stress coverage |
| P5-07 | Per-lane fair queues with owner weights and worker pools | In progress | Weighted fairness benchmark is still required |
| P5-08 | Control/exec/bulk classification and separate worker pools | In progress | Latency SLO benchmark is still required |
| P5-09 | Shared job-wait dispatch coalescing and WatchHub publication | In progress | WatchHub event streaming across reconnects is not yet exposed |
| P5-10 | Atomic job registry and startup remote re-discovery | In progress | Crash/restart mutation integration test is still required |
| P5-11 | Lease grace and idle connection reaper | Complete | Reaper timing needs long-running service test |
| P5-12 | Default-deny owner policy, persisted grants, readiness gate | In progress | Capability/principal administration RPC is still required |
| P5-13 | Digest-bound, expiring, one-time approval tokens on risky requests | Complete | Risk taxonomy needs broader operation coverage |
| P5-14 | Bounded/sanitized in-memory + rotating JSONL audit and query RPC | In progress | Query authorization policy and secret scrub integration need dedicated tests |
| P5-15 | Readiness gate, flock recovery, systemd/launchd templates | In progress | Service-manager smoke tests are still required |
| P5-16 | JSON config reload on SIGHUP, bounded drain, ordered persistence | In progress | Mutation replay/failure injection test is still required |

The final Phase 5 gate requires every “In progress” row to have runtime
evidence and a passing targeted test, in addition to `make check`.
