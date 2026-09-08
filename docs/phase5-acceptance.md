# Phase 5 acceptance and risk record

This record is maintained against the P5 requirements in
`docs/rdev-evolution-security-plan.md`. A green repository test run is not
treated as proof of a production integration requirement unless the listed
runtime path is exercised.

| Item | Current evidence | Status | Remaining risk |
|---|---|---|---|
| P5-01 | Broker hello version/min-version negotiation, incompatible-peer tests, and pipelined hello/request integration test | Complete | Cross-version release matrix still needs CI coverage |
| P5-02 | Private parent and lock validation, refusal of symlink/non-socket objects, idempotent close retaining the lock through drain; real daemon duplicate-start/SIGKILL recovery and UID 65534 denial on Linux (`TestDaemonRuntimeLifecycle`) | Complete (Linux runtime) | Darwin peer-credential/runtime coverage remains part of the platform matrix |
| P5-03 | `rdevd` owns `client.Client`, host registry, secrets, configurable agent lookup, wire dispatch; real CLI job/file/capability process tests through Unix broker; broker-backed `rdev serve` MCP ping/exec/read/write/capability and full job start/list/status/logs/stop/wait/rm tools with registration test; pipelined request test; `make stress-broker` passed 20-client wire test 100 times, with an additional 1000-run stress pass; Linux amd64 binary remote smoke passed; `make remote-session-benchmark` ran 3 x 500 real SSH requests from 20 independent client processes, with one SSH child, one remote PID and 20 distinct remote principal IDs | In progress | Real 20-process / one-agent benchmark now passes; shared sync/secret/session frontend routing and independent review remain open |
| P5-04 | Owner validation, handshake-declared and connection-level owner binding, wire client/project binding, persisted job owner, required daemon authentication by default, 0600 key provisioning, expiring HMAC token bound to owner identity, live SIGHUP revocation, and real daemon credential lifecycle tests | In progress | Credential lifecycle and distinct remote client/project identities now have process evidence; complete owner/secret/host permissions remain open |
| P5-05 | Per-connection context cancellation, broker frontend `DoContext` cancellation that closes only the local socket, shared client pool, disconnect integration test, transport cancellation/late-frame tests, full race test, and `make remote-lifecycle` real daemon/OpenSSH retry cancellation | In progress | Real SSH retry barrier, frontend context cancellation, SIGKILL and other-owner exec preservation now pass; independent review and shared job lifecycle integration remain |
| P5-06 | Quota admission held through dispatch, runtime `MaxHosts` reload, per-host accounting, bounded queued admission, and 20-client stress target; 1000 repeated broker pressure runs passed | In progress | Sustained weighted multi-owner pressure benchmark remains |
| P5-07 | Per-lane fair queues with owner weights, config reload, and worker pools; weighted multi-owner benchmark passed 3 x 10s runs (93–159 ns/op on Apple M5) | In progress | Sustained end-to-end weighted fairness under remote I/O remains |
| P5-08 | Control/exec/bulk classification, separate worker pools, queued admission, and control-under-bulk latency test repeated 100 times (all under 100ms while bulk sleeps 150ms) | In progress | Production latency SLO benchmark under remote I/O remains |
| P5-09 | Shared job-wait dispatch coalescing, WatchHub publication, latest-event replay on reconnect, and detached wait context that survives the initiating frontend disconnect | In progress | Full durable event history and external streaming RPC remain |
| P5-10 | Atomic job registry replacement on restart load, concurrent mutation recovery test, startup remote re-discovery, and mutation state persisted before broker acknowledgement | In progress | Full daemon crash/restart mutation integration test remains |
| P5-11 | Atomic lease admission/pool detachment, cleanup outside the lease lock, in-flight accounting, runtime `IdleTTL` reload, and six real remote lifecycle cycles over 65 seconds (normal exit/SIGKILL, 60 new-owner requests) | In progress | Atomic reaping defect fixed; repeated real SSH lifecycle passed. Independent review and detached wait/shutdown interaction still require closure |
| P5-12 | Default-deny owner policy, persisted grants, capability-scoped decisions, policy administration RPC, connection owner switching rejection, and runtime HMAC principal-token validation, private strict policy loading, durable grant/revoke, exact-host grants, server-derived capability, policy digests and `make remote-policy` real queued/crash/denial/failure-injection evidence | In progress | Principal lifecycle is now proven by real daemon tests; exact-host grants, server-selected capability and stable policy digests now have real queued/denial/SIGKILL evidence; complete secret resource permissions and independent review remain; wire approval now binds the host/session snapshot and exact request |
| P5-13 | Server-mandatory approval for every mutating wire operation, administrator issuance RPC/CLI, exact owner/host-session/request/policy binding, single use/expiry, pre-dial target validation, and real SSH substitution/replay/restart/privacy tests | In progress | Client-controlled Risk/Target defects are fixed for wire operations; independent review and integration with remaining shared sync/secret/Fleet mutation routes are open |
| P5-14 | Rotating JSONL audit, schema-1 exact owner fingerprints, timestamps for all events, allowlisted operation/result fields, explicit legacy omission marker, and real daemon collision/denial/restart query tests | In progress | Long-running rotation/recovery, sink failure visibility, bounded recovery reads and end-to-end operation IDs are still required; wire request/target digests, opaque approval reference and policy digest now correlate real issuance/use/result across restart |
| P5-15 | Readiness gate, optional readiness file, flock recovery, signal-driven listener close, manager syntax checks (`systemd-analyze verify`/`plutil -lint` when installed), reproducible `make smoke-rdevd`, and fail-fast `make remote-smoke`; `make remote-phase5-runtime` runs actual daemon lifecycle/credential tests and systemd user service installation on `service-deploy` | In progress | Linux systemd install/enable/start/reload/SIGKILL automatic recovery/stop/start passed; actual launchd installation/recovery requires a macOS host |
| P5-16 | Private bounded JSON parse/validate, rejected invalid/null/unknown config, live key rotation only after valid config, preservation of malformed state on startup failure, lock retained through shutdown; real daemon startup/reload/crash tests | In progress | Policy grant/revoke ACKs now survive SIGKILL; full remote job mutation crash/replay and bounded drain failure injection remain, and recovery must not discard ownership on transient remote errors |

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
| 20 independent local clients, one host, one base transport/agent | `TestRemoteBrokerProcesses`: 20 independent PIDs, 3 x 500 real SSH calls, one remote agent PID, one daemon SSH child, 20 remote principal IDs | Runtime passed; independent review pending |
| Per-client quota and cancellation isolation | Wire/transport tests plus real SSH retry cancellation, foreground SIGKILL isolation and atomic lease lifecycle | Runtime cancellation and reaping passed; sustained owner pressure and independent review remain |
| No starvation under long exec/job wait/status/sync | In-memory queue/lane tests | Real mixed remote I/O and fair admission required |
| Bulk transport closes after idle TTL | Logical lane accounting | Dedicated transport lifecycle and runtime TTL proof missing |
| Crash/restart never duplicates mutation | Registry snapshots persisted before acknowledgment | Crash windows before registry commit and replay identity require runtime tests/fixes |
| status/doctor exposes pool, quotas, queue, lane bytes and eviction reasons | Partial client metrics | Broker-scoped observable snapshot and cross-client isolation required |
| Unauthorized host/secret/job/Fleet use denied | Actual daemon authentication and default-deny process tests | Exact-host/project boundaries and capability-substitution negatives now pass; complete granted-owner secret/job/Fleet resource isolation remains |
| Destructive approval binds exact target snapshot and digest | Real SSH mandatory write approval, owner/host/parameter substitution, expiry/replay/policy-change/restart negatives and audit correlation | Wire runtime passed; remaining shared mutation routes and independent review still open |
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


Shared-session follow-up `b5e9422be7be55ccdb38a371e3aa76638a5e8e2d` passed
committed-source `make check remote-session-benchmark` (three real SSH runs)
and changed-package race checks. The runtime evidence links the logs and
measurements; the remaining routing, policy, fairness and lifecycle gates
retain their In progress status.

Cancellation/reaping follow-up `2a61674dfeb852338f3b0eb08e354401220a7e40`
passed committed-source `make check remote-lifecycle`, changed-package race,
and a real SSH integration using a race-instrumented daemon. P5-05/P5-11 now
have actual retry, other-owner foreground execution, normal-exit/SIGKILL lease,
and idle-reclamation evidence. Detached job wait/shutdown, sustained QoS and
independent review remain unfinished; no overall Complete claim is made.

Scoped-policy follow-up `669f18f5779473fc8f2e0ecaa0c227632f3b8836` passed
committed-source `make check remote-policy remote-phase5-runtime`, broker/daemon
race, and real SSH policy integration with a race-instrumented daemon. The
runtime evidence records the three policy runs and remote systemd recovery.
The P5-12/P5-14/P5-16 remaining requirements above retain In progress status.
