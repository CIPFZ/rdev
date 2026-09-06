# Phase 5 acceptance and risk record

This record is maintained against the P5 requirements in
`docs/rdev-evolution-security-plan.md`. A green repository test run is not
treated as proof of a production integration requirement unless the listed
runtime path is exercised.

| Item | Current evidence | Status | Remaining risk |
|---|---|---|---|
| P5-01 | Broker hello version/min-version negotiation and incompatible-peer tests | Complete | Cross-version release matrix still needs CI coverage |
| P5-02 | 0600 socket, 0700 parent, flock lock, stale socket recovery, peer credentials | Complete | Cross-UID integration test is platform-dependent |
| P5-03 | `rdevd` owns `client.Client`, host registry, secrets, configurable agent lookup, wire dispatch; real Unix 20-client test | In progress | Installed frontend adoption and multi-process remote-session benchmark remain |
| P5-04 | Owner validation and wire client/project binding; persisted job owner | Complete | Capability administration is still internal |
| P5-05 | Per-connection context cancellation, shared client pool, and disconnect integration test | In progress | Cancellation under an in-flight transport retry still needs transport-level evidence |
| P5-06 | Quota admission held through dispatch, per-host accounting, bounded queued admission, and 20-client Unix test | In progress | Long-duration pressure test is still required |
| P5-07 | Per-lane fair queues with owner weights, config reload, and worker pools | In progress | Sustained weighted fairness benchmark is still required |
| P5-08 | Control/exec/bulk classification, separate worker pools, queued admission, and control-under-bulk latency test | In progress | Production latency SLO benchmark is still required |
| P5-09 | Shared job-wait dispatch coalescing and WatchHub publication | In progress | WatchHub event streaming across reconnects is not yet exposed |
| P5-10 | Atomic job registry and startup remote re-discovery | In progress | Crash/restart mutation integration test is still required |
| P5-11 | Lease grace and idle connection reaper | Complete | Reaper timing needs long-running service test |
| P5-12 | Default-deny owner policy, persisted grants, capability-scoped decisions, and policy administration RPC | In progress | Principal lifecycle and weighted-policy configuration still need runtime coverage |
| P5-13 | Digest-bound, expiring, one-time approval tokens on risky requests | Complete | Risk taxonomy needs broader operation coverage |
| P5-14 | Bounded/sanitized rotating JSONL audit, history restore, owner-scoped query RPC, secret/token redaction | In progress | Long-running rotation/recovery test is still required |
| P5-15 | Readiness gate, optional readiness file, flock recovery, signal-driven listener close, systemd/launchd templates, and reproducible `make smoke-rdevd` | In progress | Manager-specific systemd/launchd installation tests are still required |
| P5-16 | JSON config reload on SIGHUP, bounded drain, ordered persistence, and worker waitgroup shutdown | In progress | Mutation replay/failure injection test is still required |

The final Phase 5 gate requires every “In progress” row to have runtime
evidence and a passing targeted test, in addition to `make check`.
