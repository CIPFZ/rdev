# Phase 5 acceptance and risk record

This record is maintained against the P5 requirements in
`docs/rdev-evolution-security-plan.md`. A green repository test run is not
treated as proof of a production integration requirement unless the listed
runtime path is exercised.

| Item | Current evidence | Status | Remaining risk |
|---|---|---|---|
| P5-01 | Broker hello version/min-version negotiation, incompatible-peer tests, and pipelined hello/request integration test | Complete | Cross-version release matrix still needs CI coverage |
| P5-02 | 0600 socket, 0700 parent, flock lock, stale socket recovery, peer credentials | Complete | Cross-UID integration test is platform-dependent |
| P5-03 | `rdevd` owns `client.Client`, host registry, secrets, configurable agent lookup, wire dispatch; real CLI job/file/capability process tests through Unix broker; broker-backed `rdev serve` MCP ping/exec/read/write/capability and full job start/list/status/logs/stop/wait/rm tools with registration test; pipelined request test; `make stress-broker` passed 20-client wire test 100 times, with an additional 1000-run stress pass | In progress | Multi-process remote-session benchmark remains |
| P5-04 | Owner validation, handshake-declared and connection-level owner binding, wire client/project binding, persisted job owner, optional HMAC principal token bound to owner identity, and real `serveConn` valid/invalid-token test | In progress | Deployment still needs documented secret provisioning and rotation |
| P5-05 | Per-connection context cancellation, broker frontend `DoContext` cancellation that closes only the local socket, shared client pool, disconnect integration test, and full race test | In progress | Cancellation under an in-flight transport retry still needs broker-to-transport integration evidence |
| P5-06 | Quota admission held through dispatch, runtime `MaxHosts` reload, per-host accounting, bounded queued admission, and 20-client stress target; 1000 repeated broker pressure runs passed | In progress | Sustained weighted multi-owner pressure benchmark remains |
| P5-07 | Per-lane fair queues with owner weights, config reload, and worker pools; weighted multi-owner benchmark passed 3 x 10s runs (93–159 ns/op on Apple M5) | In progress | Sustained end-to-end weighted fairness under remote I/O remains |
| P5-08 | Control/exec/bulk classification, separate worker pools, queued admission, and control-under-bulk latency test | In progress | Production latency SLO benchmark is still required |
| P5-09 | Shared job-wait dispatch coalescing, WatchHub publication, and latest-event replay on reconnect | In progress | Full durable event history and external streaming RPC remain |
| P5-10 | Atomic job registry replacement on restart load, concurrent mutation recovery test, and startup remote re-discovery | In progress | Full daemon crash/restart mutation integration test remains |
| P5-11 | Lease grace and idle connection reaper, with in-flight request accounting and runtime `IdleTTL` reload | Complete | Reaper timing needs long-running service test |
| P5-12 | Default-deny owner policy, persisted grants, capability-scoped decisions, policy administration RPC, connection owner switching rejection, and runtime HMAC principal-token validation via `RDEV_PRINCIPAL_SECRET` with daemon handshake coverage | In progress | Principal secret provisioning/rotation lifecycle remains |
| P5-13 | Digest-bound, expiring, one-time approval tokens on risky requests | Complete | Risk taxonomy needs broader operation coverage |
| P5-14 | Bounded/sanitized rotating JSONL audit, restart restoration across active and rotated segments, owner-scoped query RPC, secret/token redaction | In progress | Long-running rotation/recovery test is still required |
| P5-15 | Readiness gate, optional readiness file, flock recovery, signal-driven listener close, manager syntax checks (`systemd-analyze verify`/`plutil -lint` when installed), and reproducible `make smoke-rdevd` | In progress | Manager-specific install/enable/start tests remain environment-dependent |
| P5-16 | JSON config reload on SIGHUP, bounded drain, ordered persistence, and worker waitgroup shutdown | In progress | Mutation replay/failure injection test is still required |

The final Phase 5 gate requires every “In progress” row to have runtime
evidence and a passing targeted test, in addition to `make check`.
