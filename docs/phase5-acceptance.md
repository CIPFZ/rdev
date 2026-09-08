# Phase 5 acceptance and risk record

This record is maintained against the P5 requirements in
`docs/rdev-evolution-security-plan.md`. A green repository test run is not
treated as proof of a production integration requirement unless the listed
runtime path is exercised.

| Item | Current evidence | Status | Remaining risk |
|---|---|---|---|
| P5-01 | Broker hello version/min-version negotiation, incompatible-peer tests, and pipelined hello/request integration test | Complete | Cross-version release matrix still needs CI coverage |
| P5-02 | Private parent and lock validation, refusal of symlink/non-socket objects, idempotent close retaining the lock through drain; real daemon duplicate-start/SIGKILL recovery and UID 65534 denial on Linux (`TestDaemonRuntimeLifecycle`) | Complete (Linux runtime) | Darwin peer-credential/runtime coverage remains part of the platform matrix |
| P5-03 | `rdevd` owns `client.Client`, host registry, secrets, configurable agent lookup, wire dispatch; real CLI job/file/capability process tests through Unix broker; broker-backed `rdev serve` MCP ping/exec/read/write/capability and full job start/list/status/logs/stop/wait/rm tools with registration test; pipelined request test; `make stress-broker` passed 20-client wire test 100 times, with an additional 1000-run stress pass; Linux amd64 binary remote smoke passed | In progress | Multi-process remote-session benchmark remains |
| P5-04 | Owner validation, handshake-declared and connection-level owner binding, wire client/project binding, persisted job owner, required daemon authentication by default, 0600 key provisioning, expiring HMAC token bound to owner identity, live SIGHUP revocation, and real daemon credential lifecycle tests | In progress | Credential provisioning/expiry/live rotation now have process evidence; remote protocol client identity and complete owner/secret/host isolation still require fixes |
| P5-05 | Per-connection context cancellation, broker frontend `DoContext` cancellation that closes only the local socket, shared client pool, disconnect integration test, transport cancellation/late-frame tests, and full race test | In progress | Dedicated broker-to-transport retry integration remains |
| P5-06 | Quota admission held through dispatch, runtime `MaxHosts` reload, per-host accounting, bounded queued admission, and 20-client stress target; 1000 repeated broker pressure runs passed | In progress | Sustained weighted multi-owner pressure benchmark remains |
| P5-07 | Per-lane fair queues with owner weights, config reload, and worker pools; weighted multi-owner benchmark passed 3 x 10s runs (93–159 ns/op on Apple M5) | In progress | Sustained end-to-end weighted fairness under remote I/O remains |
| P5-08 | Control/exec/bulk classification, separate worker pools, queued admission, and control-under-bulk latency test repeated 100 times (all under 100ms while bulk sleeps 150ms) | In progress | Production latency SLO benchmark under remote I/O remains |
| P5-09 | Shared job-wait dispatch coalescing, WatchHub publication, latest-event replay on reconnect, and detached wait context that survives the initiating frontend disconnect | In progress | Full durable event history and external streaming RPC remain |
| P5-10 | Atomic job registry replacement on restart load, concurrent mutation recovery test, startup remote re-discovery, and mutation state persisted before broker acknowledgement | In progress | Full daemon crash/restart mutation integration test remains |
| P5-11 | Lease grace and idle connection reaper, with in-flight request accounting and runtime `IdleTTL` reload | In progress | Review found Reapable/Close are not atomic with new admission; fix and long-running runtime test required |
| P5-12 | Default-deny owner policy, persisted grants, capability-scoped decisions, policy administration RPC, connection owner switching rejection, and runtime HMAC principal-token validation via `RDEV_PRINCIPAL_SECRET` with daemon handshake coverage | In progress | Principal lifecycle is now proven by real daemon tests; target-scoped capability/secret isolation and stable policy decisions still require review and runtime negatives |
| P5-13 | Digest-bound, expiring, one-time approval primitives and wire test | In progress | Review found client-controlled Risk and Target, so mandatory risk classification and binding to actual wire parameters/policy must be implemented and tested |
| P5-14 | Rotating JSONL audit, schema-1 exact owner fingerprints, timestamps for all events, allowlisted operation/result fields, explicit legacy omission marker, and real daemon collision/denial/restart query tests | In progress | Long-running rotation/recovery, sink failure visibility, bounded recovery reads and request/approval/policy digest correlation are still required |
| P5-15 | Readiness gate, optional readiness file, flock recovery, signal-driven listener close, manager syntax checks (`systemd-analyze verify`/`plutil -lint` when installed), reproducible `make smoke-rdevd`, and fail-fast `make remote-smoke`; `make remote-phase5-runtime` runs actual daemon lifecycle/credential tests and systemd user service installation on `service-deploy` | In progress | Linux systemd install/enable/start/reload/SIGKILL automatic recovery/stop/start passed; actual launchd installation/recovery requires a macOS host |
| P5-16 | Private bounded JSON parse/validate, rejected invalid/null/unknown config, live key rotation only after valid config, preservation of malformed state on startup failure, lock retained through shutdown; real daemon startup/reload/crash tests | In progress | Full mutation crash/replay and bounded drain failure injection still required; recovery must not discard ownership on transient remote errors |

The final Phase 5 gate requires every “In progress” row to have runtime
evidence and a passing targeted test, in addition to `make check`.

## Runtime evidence and re-audit (2026-09-08)

See [runtime evidence](phase5-runtime-evidence.md) for reproducible commands,
results, review findings and implementation commit references, and
[broker operation guide](rdevd-operations.md) for credential provisioning and
rotation. The work started from clean `d5622477e2999faac8b8ec83a222de8c3140ec64`,
verified equal to origin/main after fetch; the supplied `6879e65` is its parent.

The original Complete labels for P5-11 and P5-13 were withdrawn after examining
the actual admission/reaper and approval paths. Existing isolated tests did not
prove their runtime invariants. No Phase5-wide Complete claim is made.

## Multi Agent Gate

| Gate | Evidence | Status / remaining work |
|---|---|---|
| 20 independent local clients, one host, one base transport/agent | Existing 20-client wire test uses a dispatcher replacement | Not proven; real SSH process/session counts and benchmark required |
| Per-client quota and cancellation isolation | Wire disconnect tests and transport cancellation tests | Retry integration, sustained owner pressure and atomic lease reaping required |
| No starvation under long exec/job wait/status/sync | In-memory queue/lane tests | Real mixed remote I/O and fair admission required |
| Bulk transport closes after idle TTL | Logical lane accounting | Dedicated transport lifecycle and runtime TTL proof missing |
| Crash/restart never duplicates mutation | Registry snapshots persisted before acknowledgment | Crash windows before registry commit and replay identity require runtime tests/fixes |
| status/doctor exposes pool, quotas, queue, lane bytes and eviction reasons | Partial client metrics | Broker-scoped observable snapshot and cross-client isolation required |
| Unauthorized host/secret/job/Fleet use denied | Actual daemon authentication and default-deny process tests | Granted-owner target/secret boundaries and remote owner identity still incomplete |
| Destructive approval binds exact target snapshot and digest | Approval primitive tests | Server-derived risk/digest, policy version binding and negative runtime tests required |
| Reload/upgrade/crash preserve detached background jobs | Actual daemon reload/SIGKILL/service restart lifecycle | Real detached remote jobs and mutation failure injection still required |


Batch commit `020230b43e0ac6e74322728e72b7c7c02fee206e` passed committed-source
`make check`, full `go test -race ./...`, `make stress-broker`, local smoke and
`make remote-phase5-runtime` (daemon credentials/crash tests and real systemd
user service recovery). Logs and artifact SHA-256 are linked from the runtime
evidence record. Phase5 and the Multi Agent Gate remain **In progress**.


Audit follow-up `455ff163dc15cd4db932a567271f45b2ac720f59` passed `make check`,
broker race and the real remote daemon suite. It proves exact owner-query
isolation and explicit legacy omissions across restart, not the outstanding
long-duration rotation/recovery portion of P5-14.
