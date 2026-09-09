# Phase 5 acceptance and risk record

This record is maintained against the P5 requirements in
`docs/rdev-evolution-security-plan.md`. A green repository test run is not
treated as proof of a production integration requirement unless the listed
runtime path is exercised.

| Item | Current evidence | Status | Remaining risk |
|---|---|---|---|
| P5-01 | Broker hello version/min-version negotiation, incompatible-peer tests, and pipelined hello/request integration test | Complete | Cross-version release matrix still needs CI coverage |
| P5-02 | Private parent and lock validation, refusal of symlink/non-socket objects, idempotent close retaining the lock through drain; real daemon duplicate-start/SIGKILL recovery and UID 65534 denial on Linux (`TestDaemonRuntimeLifecycle`) | Complete (Linux runtime) | Darwin peer-credential/runtime coverage remains part of the platform matrix |
| P5-03 | `rdevd` owns `client.Client`, host registry, secrets, configurable agent lookup, wire dispatch; real CLI job/file/capability process tests through Unix broker; broker-backed `rdev serve` MCP ping/exec/read/write/capability and full job start/list/status/logs/stop/wait/rm tools with registration test; pipelined request test; `make stress-broker` passed 20-client wire test 100 times, with an additional 1000-run stress pass; Linux amd64 binary remote smoke passed; `make remote-session-benchmark` ran 3 x 500 real SSH requests from 20 independent client processes, with one SSH child, one remote PID and 20 distinct remote principal IDs | In progress | Real 20-process / one-agent benchmark now passes; shared CLI now rejects unsupported routes and directory listing uses the broker; shared owner-scoped secret set/list/delete and use now have initial runtime evidence; shared push/pull/delete previews have real CLI/MCP/rsync evidence; manifest-bound sync execution/session and declarative secret delegation plus independent review remain open |
| P5-04 | Owner validation, handshake-declared and connection-level owner binding, wire client/project binding, persisted job owner, required daemon authentication by default, 0600 key provisioning, expiring HMAC token bound to owner identity, live SIGHUP revocation, and real daemon credential lifecycle tests | In progress | Credential lifecycle and distinct remote client/project identities now have process evidence; owner-scoped secret set/list/delete/use now have initial runtime evidence; declarative delegation and host administration remain open |
| P5-05 | Per-connection context cancellation, broker frontend `DoContext` cancellation that closes only the local socket, shared client pool, disconnect integration test, transport cancellation/late-frame tests, full race test, and `make remote-lifecycle` real daemon/OpenSSH retry cancellation | In progress | Real SSH retry barrier, frontend context cancellation, SIGKILL and other-owner exec preservation now pass; independent review and shared job lifecycle integration remain |
| P5-06 | Atomic fair admission with separate global/host/owner execution and queue limits, reserved control capacity, cancellable queued work and owner-only status snapshots; real 20-process remote bulk overload and owner SIGKILL tests | In progress | Remote file-I/O owner pressure now has runtime evidence; frontend ingress budgets now implemented with targeted real pressure coverage; warm pool has committed real 100-alias capacity/LRU/control/exec/reload/zero-subscriber tests and owner lane-byte projection; broader exec/job/sync pressure, response allocations and active-host saturation remain |
| P5-07 | Eligible-owner weighted scheduling, removal of empty/canceled queues, live weight changes for existing backlog; real continuously queued SSH file reads with 3:1 weights reversed to 1:3 by SIGHUP | In progress | Remote weighted file-I/O evidence passes; mixed long exec/job wait/status/sync and independent review remain |
| P5-08 | Reserved control handlers/queues, dedicated on-demand bulk transport, actual payload byte pacing, and real baseline-versus-bulk control p95 assertions in `make remote-qos` | In progress | Committed three-run real 20-process SLO passed on `1481edc` with 512 retained secret versions (ratios 1.172–1.403) and without an archive (1.200–1.391); wider payload/job/sync/streaming and independent review remain |
| P5-09 | Bounded owner-scoped wait coalescing with independently canceled subscribers and observation leases; real 20-process wait test covers initiator/follower SIGKILL, reconnect, SIGHUP, shared terminal operation ID/tail and prompt SIGTERM observation cancellation; private bounded event history persists state changes in the observation worker, including zero-subscriber completion, and exposes job.events plus CLI/MCP cursor replay | In progress | Owner-scoped durable metadata history and CLI/MCP cursor replay now have initial real zero-subscriber/crash/removal/storage-failure evidence; full push streaming, extended retention pressure and independent review remain |
| P5-10 | Versioned private bounded job registry with persist-before-publish updates and fail-closed uncertain commits; real two-project detached jobs survive repeated daemon SIGKILL, SSH-unavailable recovery, SIGHUP and remote-delete/local-rename failure (`make remote-jobs`); `make remote-mutation` exercises execution-before-ACK SIGKILL, SSH outage, durable job rediscovery, append replay refusal, intent rename failure, CLI/MCP stable IDs and exact-project outcome isolation | In progress | Pre-ACK job-start and append SIGKILL recovery, owner-bound stable IDs, fresh-agent durable tombstone recovery, and actual CLI/MCP outcome queries now pass; acknowledged-job upgrades from `e3ac9c2` and `c5707ee` have committed old/new daemon and agent evidence on `4ded2a4`; rollback/schema matrix, identity retirement and independent review remain |
| P5-11 | Atomic lease admission/pool detachment, cleanup outside the lease lock, in-flight accounting, runtime `IdleTTL` reload, and six real remote lifecycle cycles over 65 seconds (normal exit/SIGKILL, 60 new-owner requests) | In progress | Atomic reaping defect fixed; repeated real SSH lifecycle passed. Real detached wait now keeps an independent lease through zero subscribers and releases it on observation shutdown; independent review and full drain failure matrix remain |
| P5-12 | Default-deny owner policy, persisted grants, capability-scoped decisions, policy administration RPC, connection owner switching rejection, and runtime HMAC principal-token validation, private strict policy loading, durable grant/revoke, exact-host grants, server-derived capability, policy digests and `make remote-policy` real queued/crash/denial/failure-injection evidence | In progress | Principal lifecycle is now proven by real daemon tests; exact-host grants, server-selected capability and stable policy digests now have real queued/denial/SIGKILL evidence; host-scoped administrative targets and mutation queries have committed real isolation evidence on `74da502`; owner/host-bound secret permissions now have initial runtime evidence; declarative delegation and independent review remain; wire approval now binds the host/session snapshot and exact request |
| P5-13 | Server-mandatory approval for every mutating wire operation, administrator issuance RPC/CLI, exact owner/host-session/request/policy binding, single use/expiry, pre-dial target validation, and real SSH substitution/replay/restart/privacy tests | In progress | Client-controlled Risk/Target defects are fixed for wire operations; independent review and remaining shared sync mutation routes are open; reserved Fleet mutations and approval issuance have real denial evidence on `4bfdf06` (Fleet implementation belongs to Phase7) |
| P5-14 | Exact owner audit fingerprints and wire approval/policy correlation; bounded async sink, private bounded recovery, torn-tail repair, visible rotation/write/sync errors and drops, owner-query durability barrier and separately authorized sink health | In progress | Real profiling identified and removed audit fsync from the shared request lock; targeted stall/rotation-failure/recovery tests pass; three real load runs wrote 146438 events with three rotations and zero drops/errors. Hashed mutation operation references now correlate with durable intents; durable crash-tail uncertainty and predecessor seal checks pass actual process tests; committed 600-second/20-producer soak passed 603079 calls, 21 rotations, five SIGKILL recoveries and zero reported sink drops/errors; extended retention/storage-failure matrix, implemented authenticated routes now have committed per-request/outcome correlation on `8af4fb7`; remaining shared-route correlation and independent review remain |
| P5-15 | Readiness gate, optional readiness file, flock recovery, signal-driven listener close, manager syntax checks (`systemd-analyze verify`/`plutil -lint` when installed), reproducible `make smoke-rdevd`, and fail-fast `make remote-smoke`; `make remote-phase5-runtime` runs actual daemon lifecycle/credential tests and systemd user service installation on `service-deploy` | In progress | Linux systemd install/enable/start/reload/SIGKILL automatic recovery/stop/start passed; actual launchd installation/recovery requires a macOS host |
| P5-16 | Private bounded JSON parse/validate, rejected invalid/null/unknown config, live key rotation only after valid config, preservation of malformed state on startup failure, lock retained through shutdown; real daemon startup/reload/crash tests | In progress | Policy grant/revoke ACKs now survive SIGKILL; Remote pre-ACK crash/replay, intent persistence failure and stalled-response SIGTERM are under the mutation runtime gate; full failure/upgrade matrix and independent review remain |

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
| Per-client quota and cancellation isolation | Real retry/SIGKILL isolation plus sustained per-owner remote file-I/O overload, bounded queue rejection and cancellation cleanup; bounded socket/JSON ingress and slow-reader cancellation targeted runtime | Runtime file-I/O and initial ingress pressure evidence; three real ingress pressure runs passed; broader workloads and independent review remain |
| No starvation under long exec/job wait/status/sync | In-memory queue/lane tests | Real mixed remote I/O and fair admission required |
| Bulk transport closes after idle TTL | Real dedicated bulk/base SSH processes during remote I/O, active bulk preservation, one-second idle TTL plus five-second sweep, same base agent retained | Runtime passed on Linux; independent review and broader host/payload matrix remain |
| Crash/restart never duplicates mutation | Private persist-before-dispatch intent registry; real pre-ACK remote job and append SIGKILL, stable-ID conflict/replay refusal, fresh-agent job tombstones and original-owner recovery | Wire job/write crash windows now have runtime evidence; remaining shared routes, retirement/upgrade matrix and independent review remain |
| status/doctor exposes pool, quotas, queue, lane bytes and eviction reasons | Authenticated owner-only scheduler snapshot, queue wait and file payload byte counters; separately granted audit sink health | CLI/MCP now project owner scheduler/ingress/wait usage; separately authorized CLI/MCP warm capacity/lease/closing/eviction snapshots now have initial runtime evidence; all three lanes now have initial real byte-for-byte CLI/MCP/SSH traffic evidence; owner dial/retry/fixed-stage diagnostics now have initial real failure/cancellation evidence; broader failure matrix and independent review remain |
| Unauthorized host/secret/job/Fleet use denied | Actual daemon authentication and default-deny process tests | Exact-host/project boundaries and capability-substitution negatives now pass; granted-owner job status/logs/wait/stop/rm and scoped pagination negatives now pass; secret set/list/delete/use now have initial runtime negatives; declarative delegation and independent review remain; reserved Fleet denial now passes the real pre/post-SIGKILL substitution matrix on `4bfdf06` |
| Destructive approval binds exact target snapshot and digest | Real SSH mandatory write approval, owner/host/parameter substitution, expiry/replay/policy-change/restart negatives and audit correlation | Wire runtime passed; remaining shared mutation routes and independent review still open |
| Reload/upgrade/crash preserve detached background jobs | Actual daemon reload/SIGKILL/service restart lifecycle | Real acknowledged and pre-ACK detached jobs survive SIGKILL/SSH outage; live waits survive reload and cancel on shutdown; committed real upgrades on `4ded2a4` from two predecessor daemon/agent pairs preserve supervisor executable identity and output; full rollback/schema/drain failure matrix remains |


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

Mandatory wire-approval follow-up `915fb5e2f15d48ac078f44a417543517d5627d90`
passed committed-source `make check remote-approval remote-session-benchmark
remote-lifecycle remote-phase5-runtime`, changed-package race, and real SSH
approval integration with a race-instrumented daemon. The evidence records
separate administrator issuance and executor principals throughout. Remaining
shared mutation routes and independent review keep P5-13 In progress.

Fair admission/bulk/audit follow-up replaces the quota-before-queue path. The
real runtime benchmark exposed control p95 ratios above three even with a
separate SSH transport; a diagnostic daemon profile attributed 99.87% of mutex
wait to synchronous audit append. Bounded asynchronous audit plus an explicit
8 MiB/s bulk payload budget made the initial same-workload SLO pass without
changing the two-times threshold. Repeated committed-source results and remaining
risks are maintained in the runtime evidence record. No new Complete label is
claimed for P5-06/P5-07/P5-08/P5-14 or the overall Multi Agent Gate.

Fair admission/bulk/audit implementation `6f62f60e513c2c4dd560668dc6b530de2de6494a`
passed committed-source `make check remote-qos stress-broker smoke-rdevd
remote-phase5-runtime`, full repository race and actual daemon race integrations.
Three 20-process runs met the control p95 SLO (1.27–1.49 times baseline), exact
owner queue isolation, weight reload, SIGKILL isolation, bulk TTL and bounded
audit rotation checks. Runtime logs, artifact digests and remaining risks are
linked from the runtime evidence record. Overall Phase5 remains In progress.


Detached-job recovery implementation `e3ac9c2` passed committed-source
`make check remote-jobs remote-approval remote-policy remote-phase5-runtime`,
full repository race and three real recovery runs using a race-instrumented
daemon. Private versioned snapshots, scoped pagination and acknowledged-job
SIGKILL/SSH-outage/reload/deletion-failure recovery now have runtime evidence.
Pre-ACK job-start mutation intents, event history, upgrade and bounded shutdown
remain incomplete; evidence and remaining risks are linked above.


Shared-wait/TERM implementation `bd03436` passed committed-source `make check
remote-wait remote-jobs remote-lifecycle stress-broker smoke-rdevd
remote-phase5-runtime`, full repository race and two actual daemon race runs.
Three normal and two race runs each verified 20 independent subscribers,
initiator/follower SIGKILL, owner isolation, shared terminal operation ID/tail,
SIGHUP and observation shutdown preserving the remote supervisor. Live wait
semantics now have runtime evidence; durable event replay, pre-ACK mutation
recovery and broader Phase5 gates remain In progress.


Replay digest follow-up adds the previously omitted state dry_run and capability
refresh controls to remote operation identity. `make remote-replay-digest` has
three real SSH substitution negatives with no state manifest creation. Stable
IDs across broker recovery and durable mutation intents remain unfinished.


Replay digest implementation `20b4615` passed committed-source `make check
remote-replay-digest remote-wait remote-jobs remote-approval remote-policy
stress-broker smoke-rdevd remote-phase5-runtime` and full repository race. The
linked logs include real state/capability replay negatives and repeated Linux
systemd recovery. No new Complete claim follows from this narrower correction.

## Evidence commit index

The status and risks in the primary table above remain authoritative. Each
implementation commit below has its own committed-source log in the runtime
evidence document; a passing command applies only to the scenario it exercises.

| Item | Implementation commits | Passing evidence / verification |
|---|---|---|
| P5-01 | `020230b` and predecessors | Broker negotiation/pipelining tests; repeated full check/race |
| P5-02 | `020230b` | Real daemon duplicate-start, SIGKILL, private socket and foreign UID denial |
| P5-03 | `b5e9422`, `08ff4fb`, `a47c59a`, `74da502`, `c5707ee`, `21996d4` | Three 20-process / 500-call SSH shared-session benchmarks; bounded 100-alias host pool and fail-closed broker routes |
| P5-04 | `020230b`, `b5e9422`, `e3ac9c2`, `c5707ee`, `21996d4` | Real principal provision/expiry/rotation, remote identity and two-project job negatives |
| P5-05 | `2a61674`, `bd03436`, `7026bdf`, `c5707ee`, `21996d4` | Real retry/frontend cancellation/SIGKILL and shared-wait disconnect/reconnect |
| P5-06 | `6f62f60`, `bd03436`, `7026bdf`, `a47c59a`, `a8dbe3d`, `0be7127`, `c5707ee`, `21996d4` | Remote sustained file-I/O, ingress and observer bounds, 100-alias capacity/leases, exact owner lane-byte ledgers |
| P5-07 | `6f62f60` | Continuously backlogged real SSH owners, live 3:1 to 1:3 weight reversal |
| P5-08 | `6f62f60`, `a47c59a`, `a8dbe3d`, `c5707ee`, `21996d4` | Repeated three-run real bulk/control p95 SLO; latest ratios 1.340–1.436 |
| P5-09 | `bd03436`, `371fa30`, `7026bdf` | Real shared waits plus 20-client zero-subscriber durable terminal history, restart cursors, CLI/MCP replay, removal isolation and storage repair |
| P5-10 | `e3ac9c2`, `bd03436`, `a96b359`, `a3ccb8d`, `c5707ee`, `21996d4` | Acknowledged and pre-ACK job/append recovery, fresh-agent durable tombstones, CLI/MCP queries and owner isolation |
| P5-11 | `2a61674`, `bd03436` | Six real lease cycles, zero-subscriber observation lease and shutdown release |
| P5-12 | `020230b`, `669f18f`, `e3ac9c2`, `08ff4fb`, `74da502`, `c5707ee`, `21996d4` | Default deny, exact-host policy/crash, project job isolation and host-bound administration |
| P5-13 | `915fb5e`, `74da502`, `c5707ee`, `21996d4` | Real mandatory exact-request approval substitution/replay/expiry/restart negatives and scoped issuance |
| P5-14 | `455ff16`, `6f62f60`, `a96b359`, `41cddb4`, `8af4fb7`, `c5707ee`, `21996d4` | Exact owner privacy, persistent crash continuity, 600-second/603079-call soak with 21 rotations and five SIGKILL recoveries |
| P5-15 | `020230b`; repeated through `c5707ee`, `21996d4` | Actual Linux systemd install/enable/start/reload/SIGKILL recovery/stop/start |
| P5-16 | `020230b`, `669f18f`, `e3ac9c2`, `bd03436`, `20b4615`, `a96b359`, `c5707ee`, `21996d4` | Invalid reload preservation, durable policy ACK, job deletion recovery, wait drain, replay digest negatives |


## Durable mutation intent follow-up

The mutation batch persists exact owner/host/request/target/policy bindings before
remote dispatch. Only outcome metadata is retained. Generic uncertain mutations
are never silently replayed. Job starts use deterministic principal-bound IDs and
remote durable tombstones that survive agent cache loss and job deletion.

`make remote-mutation` adds real OpenSSH execution-before-ACK crash tests,
SSH-unavailable restart, owner-scoped job rediscovery, append-once assertions,
pre-dispatch rename failure, actual CLI/MCP queries and duplicate prevention.
Definitive remote rejection and handler failure have explicit recorded outcomes;
a second agent rejecting a retry cannot erase first-attempt uncertainty.

Retention is bounded without eviction (8192 total / 1024 per owner). Safe identity
retirement, durable event replay, remaining shared routes, complete workload and
platform matrices and independent external review are still open. This batch does
not make Phase5 or any additional P5 item Complete. The runtime evidence document
records commands, measurements, review findings and committed-source results.


Mutation implementation `a96b359` passed clean-commit full check, full race,
actual daemon race, all mutation/job/wait/policy/approval/session/lifecycle/QoS
runtime targets, stress, readiness and Linux systemd recovery. Three held-response
SIGTERM runs exited in 7.017–7.018 seconds; control p95 ratios were 1.255–1.607.
Logs and artifact hashes are linked from the runtime evidence. Concurrent
resolution received an additional follow-up finding; event history, identity
retirement, shared routes and platform/review requirements remain In progress.


The concurrent job-resolution follow-up accepts the already-durable matching
success when another caller wins the transition. The regression reproduces the
old error, passes ten 32-caller race runs after the fix, and extends the real
mutation scenario to twenty independent status processes released from a remote
response barrier. This corrects concurrent recovery without changing owner/
target/digest checks or enabling mutation replay. Full validation is recorded
in the runtime evidence; overall Phase5 remains In progress.


Concurrent recovery implementation `a3ccb8d` passed committed-source full check,
broker/daemon race, three real mutation runs with twenty independent resolvers,
three shared-wait runs and an actual daemon race recovery run. The record links
normal and race measurements and the artifact digest. This closes the concurrent
transition defect; the remaining P5/Multi Agent Gate items stay In progress.


## Job event history follow-up

The event-history batch stores only owner-scoped job state metadata before
acknowledgment, with explicit stream/sequence cursors and retention-gap signals.
Twenty disconnected wait frontends still leave one worker to persist the terminal
event. History survives daemon SIGKILL and job removal, while other projects
remain denied. The watch cache now holds bounded small completion hints instead
of unbounded raw wait responses.

`make remote-events` covers actual CLI/MCP cursor replay, a real event-snapshot
rename failure and later status repair. A response-scope review also rejects
unrelated job lifecycle fields before ownership/event projection. These initial
runtime assertions do not close push streaming, extended retention-pressure or
independent-review gaps. Full validation and commit evidence are recorded below
in the linked runtime evidence document; Phase5 remains In progress.


Event-history implementation `371fa30` passed final-code full check/race and an
actual daemon race history test, then committed-artifact remote history, mutation,
wait, 100-run stress, readiness and Linux systemd recovery. Six normal real
history runs and one actual daemon race run cover the new observed-state replay
path. Logs distinguish pre-commit code validation from committed artifacts.
Push streaming, prolonged retention pressure and independent review remain open.


## Frontend ingress follow-up

Socket/owner admission, bounded JSON document and aggregate request-byte budgets,
two queued requests per connection, absolute handshake/partial-frame deadlines and
bounded response writes now cover the formerly unbounded frontend path. Shared
wait workers keep their own request charge after every subscriber disconnects.
The real ingress gate tests exact-project isolation and unchanged shared remote
agent identity during connection, byte, pipeline and slow-reader pressure. The
runtime evidence record tracks final validation and remaining matrix/review gaps;
no additional P5 item or overall gate is marked Complete by this change.


Ingress implementation `7026bdf` passed full check/race, actual daemon race,
repeated real ingress and detached-observer tests, stress, readiness and Linux
systemd recovery. The committed-artifact QoS record retains a 2.043-times p95
failure during overlapping unrelated tests and three subsequent isolated passes
at 1.297–1.443 times baseline. Broader host contention remains unproven. The
linked runtime evidence contains both logs and artifact digests; the overall
Phase5 and Multi Agent Gate remain In progress.


Shared frontend routing now rejects unsupported commands before standalone state
or transport initialization. Actual CLI/MCP status and directory listing have
initial exact-project/host/default-deny evidence, with unchanged shared remote
agent identity. Full shared sync/secret/session implementation, full pool status
and independent review remain unfinished; no additional Complete label is added.


Shared frontend implementation `08ff4fb` passed committed-artifact full check,
three real CLI/MCP status/list/default-deny/fallback scenarios, history and
mutation regressions, stress, readiness and Linux systemd recovery. Actual CLI
and daemon race binaries also passed the frontend scenario. The runtime record
links the original fallback reproduction, passing logs, artifact digests and
remaining shared-route/status/review risks. Overall Phase5 remains In progress.


Audit continuity now records a durable open marker and clean segment seals.
SIGKILL, marker close failure and writes by an older daemon are detectable after
restart; historical uncertainty is not erased by a later clean shutdown. The
remaining audit close uses the broker shutdown budget and no redundant policy/
job saves follow drain. Actual audit and predecessor tests plus the forthcoming
soak are recorded in the runtime evidence. No additional Complete status is
claimed before full validation and remaining platform/review requirements.


Audit implementation `41cddb4` passed committed-source full check, repeated real
continuity/predecessor/mutation/history tests, three unchanged-threshold QoS runs,
stress, readiness and Linux systemd recovery. The 600-active-second/20-process
soak passed with 603079 calls, 21 rotations and five SIGKILL recoveries; peak
observed RSS/FD were 41432 KiB/34. Crash-tail uncertainty remained explicit across
subsequent clean restarts. Full repository race and actual daemon race continuity
also passed. The runtime evidence record links all logs and the artifact digest.
Full retention/storage-failure/upgrade coverage and independent review remain;
Phase5 and the Multi Agent Gate remain In progress.


## Warm pool admission and lifecycle follow-up

The broker now reserves bounded host slots before allocating scheduler workers.
Cold requests retain owner quotas and fair queue positions; canceled requests
release demand without touching another owner's active transport. Idle LRU,
capacity reload, warm TTL and final-client cleanup preserve active host leases,
including independently running shared observations. Detached SSH cleanup remains
charged to the host; bulk cleanup no longer blocks the daemon signal/reload loop.
Global pool health has separate CLI/MCP authorization and host-scoped health
requests cannot expose global data.

The initial 100-alias actual daemon/OpenSSH run found a control-worker admission
defect in the first implementation; moving reservation into scheduler eligibility
fixed that reproduced failure. The revised scenario passed capacity/control/exec,
reload and TTL behavior, with repeated frontend and race tests. Full batch and
committed-artifact logs, including zero-subscriber wait coverage, follow in the
runtime evidence record. This is implementing-agent review, not independent
external review. Full mixed sync/exec/job load, multi-machine recovery, remaining
shared secret/session routes and macOS runtime still prevent Complete status.


Concurrent warm-pool/mutation tests exposed a failure-injection wrapper defect:
stdout EOF was incorrectly treated as permission to terminate OpenSSH before its
exit. The wrapper now waits for the real result with bounded cleanup, and a
pipe/process regression preserves both exit 0 and exit 7. Native OpenSSH passed
three paired full-master and probe-to-install saturation scenarios without
changed transport retry behavior. Corrected overlap and committed evidence follow
in the runtime record; Complete status remains withheld.


The final warm-pool production code passed full check/race and a 97.02-second
actual daemon/CLI race suite covering pool, detached observation, frontend
permissions and native mux saturation. Ten mutation crash/recovery tests passed
concurrently after the EOF wrapper correction. The runtime record separates this
pre-commit validation from the subsequent committed-artifact gate and preserves
the failed-wrapper diagnosis. Full mixed workload, remaining shared routes,
platform and independent review work are still open.


Committed warm-pool implementation `a47c59a` passed full check, actual 100-alias
capacity/LRU/reload/exec/zero-subscriber observation coverage, three paired native
mux-saturation and mutation regressions, stress, readiness and Linux systemd
recovery. Three real QoS runs passed at 1.474–1.668 times baseline, with 146313
audit records, three rotations and zero reported sink drops/errors. Full race
and actual daemon/CLI race passed before commit. Logs and artifact digests are
linked in the runtime evidence record. Multi-machine recovery, full mixed load,
shared routes, macOS and independent review keep Phase5 In progress.


## Broker route and host-scoped administration follow-up

Actual daemon regressions reproduced false success for absent/missing-wire
handlers, nested policy/approval target substitution and out-of-host mutation
outcome disclosure. Validation now follows default-deny policy and precedes
approval use, durable state updates and transport admission. Same-host
administration works; global/other-host target substitution fails. Real tests
verify unchanged durable grants after rejection, grant/revoke across SIGKILL,
project isolation, approval preservation and exact append counts across restart.
The runtime evidence records the predecessor failures, correction and final-code
check/race/SSH regressions. Complete shared routes, independent external review,
mixed workload and macOS runtime remain outstanding; status remains In progress.


Route implementation `74da502` passed committed-source full check, three complete
actual route/host-administration scenarios, policy/approval/mutation/frontend
regressions, stress, readiness and Linux systemd recovery. Full repository race
and real race-daemon tests passed before commit. Runtime logs preserve both the
old-version reproduction and corrected audit-enum failure, with only passing
final-code runs counted. Shared routes, mixed workload, macOS and independent
review remain open; Phase5 and Multi Agent Gate retain In progress status.


## Protocol lane traffic follow-up

Owner status now exposes sent/received application protocol bytes for control,
exec and bulk. Meter identity survives retry, caller cancellation and late-frame
drain; partial writes count actual accepted bytes and rejected frames count zero.
Initial real CLI/MCP results match an independent byte-only SSH recorder through
read-terminal loss/retry, streaming, frontend SIGKILL and generated cancellation.
The same client ID in another project retains separate totals. Full batch and
committed-artifact evidence are tracked in the runtime record. Dial/failure
reasons, remaining shared routes, mixed load, macOS and independent review remain
open; Phase5 and Multi Agent Gate are In progress.


Lane implementation `a8dbe3d` passed committed-source full check, three actual
byte-ledger/CLI/MCP scenarios, three 20-process QoS runs at 1.229–1.663 times
baseline, stress, readiness and Linux systemd recovery. The audit sinks wrote
146314 records, rotated three times and reported zero drops/errors. Full race,
actual daemon race and retry/lease/wait/mutation regressions passed before commit.
The runtime record links passing logs, diagnostic failures and artifact hashes.
Dial/failure reasons, shared routes, mixed workload, macOS and independent review
remain unfinished; no additional Complete label is applied.


## Local audit outcomes and request correlation follow-up

Authenticated request responses and audit events now share a server-generated
request reference. Local query outcomes and pre-dispatch validation failures are
recorded; mutation queries share their original operation reference. The initial
real daemon/SSH regression verifies exact approval/admission/append/query chains,
reused-client-ID separation, exact-project filtering and recovery of flushed
traces across SIGKILL. Asynchronous crash-tail uncertainty remains explicit.
The runtime record tracks batch checks and remaining retention/storage/upgrade,
shared-route, platform and independent-review gaps. Phase5 remains In progress.


Audit-route implementation `8af4fb7` passed committed-source full check, repeated
actual correlation/continuity/predecessor/mutation scenarios, three QoS runs at
1.316–1.553 times baseline, stress, readiness and Linux systemd recovery. The sinks
had written 146147 records at measurement, with six rotations, zero drops/errors
and one self-generated health-query event pending per run. Full race and actual
daemon race passed before commit. Logs and artifact digests are linked in the
runtime record. Async crash uncertainty and remaining retention/storage/shared
route/platform/review work keep Phase5 and Multi Agent Gate In progress.


## Connection diagnostic projection follow-up

Owner status now includes base/bulk setup attempts, successes, in-flight count,
setup durations, application retries and fixed failure-stage counters. Actual
probe failure, missing agent artifact, remote installation obstruction and held
handshake cancellation tests preserve another project's shared agent and counters.
CLI/MCP byte-ledger tests also verify three setups plus one retry for the initiating
project and zero borrowed counts for the other project. Full check/race and real
race-daemon regressions passed; committed-artifact evidence follows in the runtime
record. Full failure/host matrices, shared routes, mixed load, macOS and independent
review keep Phase5 and the Multi Agent Gate In progress.


Connection diagnostics `0be7127` passed committed-source full check, three real
setup/failure/cancellation scenarios, three exact lane-byte ledgers, three
20-process QoS runs (p95 ratios 1.316–1.657), stress, readiness and Linux systemd
recovery. The sink measurements total 146101 written records, six rotations and
zero errors/drops. Runtime and race logs plus artifact digests are linked in the
runtime evidence record. Shared routes, mixed workloads, broader failure cases,
macOS and independent review remain open; Phase5 is In progress.


## Principal-owned shared secrets follow-up

The new shared secret routes and explicit `secret.use` permission now have actual
daemon/CLI/MCP/SSH evidence. Two projects using the same client ID and key name
receive distinct values; host/default/use-grant negatives hold. Exact version
approvals, expanded-byte admission/cancellation, durable rotation/deletion,
original-supervisor output redaction after SIGKILL, repeated mutation IDs, real
rename failure/recovery, missing-state startup denial and host replacement/return
retirement pass the targeted runtime. Malformed/private-file startup and bounded
retention/concurrent-resolution checks are included. Final batch and committed
validation results are maintained in the runtime evidence record.

No additional Complete claim is made: shared file imports/declarative delegation,
sync/session work, secret archive retirement and broader failure/load matrices,
macOS runtime and independent review remain open.


Secret implementation `c5707ee` passed committed-source full check, three actual
secret/CLI/MCP/SSH scenarios, repeated approval/mutation crash regressions, three
20-process QoS runs (p95 ratios 1.313–1.616), stress, readiness and Linux systemd
recovery. The runtime record links logs, exact artifact digests and the earlier
fixture diagnoses. Secret archives were empty in these QoS fixtures; archive
load, file imports/declarative delegation, retirement/storage/upgrade cases,
sync/session routes, macOS and independent review remain open. No new Complete
label is applied.


## Principal-bound remote secret file import follow-up

Shared CLI/MCP file import now requires both exact-host secret-import and
file-read authority before queuing. Real source reads retain client/project
identity and use the existing bulk transport; approval binds the path and actual
validated value. Missing/short/binary/oversized source rejection, the 64 KiB
boundary, path/content substitution, repeated raw import, exact project scope,
held source-response cancellation and original base-agent preservation now have
targeted runtime evidence. Internal read payload is included in bulk admission
accounting, with a real exact-byte and other-project negative assertion. Final
batch and committed-source evidence are tracked in the runtime record. Remaining
declarative delegation, retention/load/upgrade matrices, sync/session, macOS and
independent review keep Phase5 In progress.


Remote file-import implementation `21996d4` passed committed-source full check,
three import/core-secret/approval scenarios, three real QoS runs (control p95
1.340–1.436 times baseline), stress, readiness and actual Linux systemd recovery.
The runtime evidence links complete logs and artifact SHA-256. Source bytes are
charged to the exact owner, including invalid sources; cancellation preserves
the previous value and the other project's base transport. Source paths/values
are absent from flushed audit/mutation records. No new Complete label is claimed:
populated credential archive pressure, declarative delegation, remaining shared
routes, retirement/storage/upgrade matrices, macOS and independent review remain.


## Populated secret archive QoS follow-up

A real 512-version archive (two exact projects, 256 versions each) exposed
three-second owner starvation in the original redaction path. Every version was
registered through a separately approved daemon mutation. An immutable cached
redaction plan, conservative candidate filter and allocation-free unmatched
wrapped scan remove the repeated rule construction and whole-output copies.
The normal corrected run met the unchanged control SLO at 1.185–1.203 times
baseline. This initial run preceded the final whitespace-only credential guard.

The final fixture includes accepted whitespace-only values. Changed-package race
passed. The actual race daemon passed core-secret/import scenarios; its initial
25-second QoS window collected only 43 admissions for the lower-weight owner
(the minimum is 50), while every owner progressed each second. A longer
40-second observation kept all thresholds unchanged and passed 203:68 then
68:203 admissions, all 40 backlog samples per phase and control p95 ratios
1.405–1.408. All 512 values remained redacted before and after SIGKILL. Audit
reported the injected unclean recovery explicitly, with zero sink errors/drops.

Committed-source default-window repeats follow. P5-06/P5-07/P5-08/P5-14 and the
Multi Agent Gate retain In progress status. The global 4096-version/16 MiB
capacity, concurrent secret mutation, mixed exec/job/sync, archive retirement,
macOS and independent review remain open.


Populated archive implementation `1481edc` passed committed-source full check,
three default-window 512-version QoS runs, three empty-archive QoS regressions,
three core-secret/import runs, stress, readiness and Linux systemd recovery.
The preserved complete log was recovered after the session interruption and is
now linked with race/diagnostic logs in the runtime evidence record. Control p95
ratios were 1.172–1.403 with the archive; fairness, owner progress, historical
redaction across SIGKILL, bulk TTL and base-agent preservation all passed.
No new Complete label is applied; wider workloads, remaining shared routes,
archive/storage/upgrade cases, macOS and independent review remain open.


Reserved Fleet authority tests `4bfdf06` passed full check, three actual boundary,
route, approval and policy runs, actual race-daemon regression, stress, readiness
and Linux systemd recovery. The test proves exact-project default denial,
capability/inner-wire/approval substitution rejection before SSH/mutation state,
owner-only decision/outcome correlation, preservation of the valid approval and
operation ID, and the same boundary after SIGKILL. `make remote-fleet-boundary`
reproduces the negative gate; Fleet orchestration remains Phase7. Other authority,
shared-route, mixed-load, upgrade/platform and independent-review requirements
remain incomplete.


## Detached jobs across actual daemon and agent upgrades

The new `make remote-job-upgrade` replaces both executables, starting jobs with
`e3ac9c2` and `c5707ee` artifacts and upgrading after either SIGTERM or SIGKILL.
Initial three-run matrices preserve the original supervisor PIDs and old binary
hashes while the base agent demonstrably runs the new binary. Exact-project
listing/management, reload, observed-wait shutdown, terminal output, durable
removal and once-only execution assertions pass.

The oldest predecessor exposed a real compatibility defect: TERM killed a
legacy supervisor before it could flush buffered logs. The new agent now signals
the legacy child group while allowing its supervisor to finish; configured grace
and KILL escalation remain in force. A local subprocess regression fails against
the previous implementation and passes after the fix. Committed validation and
remaining upgrade boundaries are maintained in the runtime evidence record.
The complete Phase5 and Multi Agent Gate remain In progress.


Upgrade fix `4ded2a4` passed committed-source full check, 12 real old/new
daemon-and-agent upgrade scenarios, three job/wait/mutation/event/Fleet
regressions, stress, readiness and Linux systemd recovery. Full repository race
and 12 actual race-daemon/race-agent upgrades also passed. The runtime record
links the full logs, old/new artifact digests, before-fix reproduction and
remaining compatibility limits. Phase5 and the Multi Agent Gate remain
In progress; independent review and the other requirements above are not waived.


## Shared sync preview and cancellation follow-up

The broker now owns CLI/MCP push, pull and delete previews. Exact-project and
exact-host grants are checked before transport admission; delete previews also
require `sync.delete`. Malformed envelopes and mutating requests fail before
SSH. Bulk admission and independent worker/output reservations survive frontend
cancellation until the real rsync process and its SSH descendants finish.

A real local rsync regression reproduces the predecessor's cancellation hang:
the SSH descendant retained output pipes after its rsync parent exited. Group
cancellation now terminates those descendants without closing the pooled master.
The remote regression holds a real server response and verifies cancellation,
other-project base-agent preservation, unchanged source/destination trees, bounded
and redacted output, audit privacy and the same authority after SIGKILL.

This is an intermediate implementation. Shared execution still needs immutable
source/destination plans, exact approval and durable mutation outcomes. Actual
rsync network bytes are not yet included in protocol traffic or bulk pacing.
Mixed long exec/job/status/sync, remaining shared routes, macOS and independent
review still prevent Phase5 and Multi Agent Gate Complete status.


Shared preview implementation `6d2eeb3` passed committed-source full check,
three real preview/cancellation/ingress runs, six lease lifecycle cycles, stress,
readiness and Linux systemd recovery. All-package race and three actual daemon/
CLI race runs also passed. Logs and artifact digests are linked in the runtime
evidence record. This completes preview validation only; immutable execution,
rsync traffic budgets and all remaining Phase5 requirements stay In progress.


## Content-complete source observations

A real predecessor-daemon regression reproduced unchanged manifest digests after
a 5 MiB file was modified with its size and mtime preserved. The new bounded
scanner hashes every file, confines link resolution to a pinned root, enforces
no-follow leaf opens with `openat`, and enumerates directories in small batches.
Shared worker admission now includes manifest memory. Actual source size/entry
limits fail without writing the destination. These changes improve the preview
guard and provide full entries for subsequent plan construction; retained source
content, explicit destination deletion plans and approved durable execution are
still required. Full Phase5 and Multi Agent Gate status remain In progress.
