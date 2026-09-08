# Phase5 runtime evidence and review

## 2026-09-08: authentication and service lifecycle batch

Baseline: clean `d5622477e2999faac8b8ec83a222de8c3140ec64`, equal to
`origin/main` after fetch. Development platform: Linux amd64, Go 1.25.0 at
`/data/tmp/rdev-toolchain/go/bin/go` (the supplied macOS path does not exist in
this container). Remote: the authorized `service-deploy` SSH host, Linux amd64,
systemd user manager running. No SSH private key is included in these artifacts.

Implementation commit: `020230b43e0ac6e74322728e72b7c7c02fee206e`
(`fix: authenticate broker sessions and verify daemon service lifecycle`).

| Evidence | Command / actual scope | Result |
|---|---|---|
| [Baseline](evidence/phase5/2026-09-08/baseline-check.log) | `make check` before the batch | Failed: pgrep checker matched itself on Linux; systemd verification assumed an installed `/root/bin/rdevd` |
| [Daemon lifecycle](evidence/phase5/2026-09-08/daemon-runtime.log) | `go test ./cmd/rdevd -run TestDaemonRuntimeLifecycle -count=1 -v` | Passed with actual daemon and provisioning subprocesses; private keys, expiry, owner/project binding, default deny, live rotation, restart, UID isolation, startup failures |
| [Repository and pressure](evidence/phase5/2026-09-08/check-stress.log) | `make GO=/data/tmp/rdev-toolchain/go/bin/go check stress-broker` | All package tests, vet and four embedded agent consistency checks passed; 20-client wire test passed 100 repetitions |
| [Broker race](evidence/phase5/2026-09-08/broker-race.log) | `go test -race ./cmd/rdevd ./internal/broker -count=1` | Passed; this is a batch race check, not the final repository-wide race gate |
| [Remote runtime](evidence/phase5/2026-09-08/remote-runtime.log) | `make remote-phase5-runtime RDEV_SSH_CONFIG=<private SSH config> GO=/data/tmp/rdev-toolchain/go/bin/go` | Passed on real Linux: authenticated readiness smoke; same principal/daemon crash suite; real UID 65534 denial; systemd user unit link/enable/start/reload, SIGKILL automatic recovery and stop/start |

The remote systemd run observed the daemon PID change from 735171 to 735230,
required `NRestarts >= 1`, then stopped and started it again. A subsequent
`systemctl --user list-unit-files 'rdevd-phase5-*'` and search for the run's
temporary directories both returned no entries after cleanup. This tests an
actual installed unit rendered from the shipped template, not manager mocks.
Local and remote smoke both fail immediately if the expected Unix socket or
readiness state is missing; a negative smoke regression covers this failure.

The process-group test now records the actual grandchild PID and verifies it
is absent or a non-running zombie after timeout. It no longer relies on a
self-matching `pgrep -f` command. The corrected test passed five repetitions.
The systemd syntax check renders an executable path before verification and
uses the user-manager context. Runtime installation is separately proven above.

## Review performed separately from implementation

The review traced successful and failing startup, duplicate launch, key/config
reload, signal shutdown, and filesystem ownership. Its findings and fixes:

- The previous startup deleted the ready marker before acquiring the singleton
  lock. Lock acquisition now precedes removal, including validation failures;
  a duplicate instance preserves live readiness and a failed restart clears a
  stale marker.
- Closing the listener on SIGTERM released the lock before state drain/save.
  `StopAccepting` now retains the lock; final `Close` is idempotent and cannot
  unlink a replacement instance's socket. Dedicated tests cover both cases.
- Private parent and lock validation previously trusted existing objects.
  Public parents, foreign owners, symlink locks and non-socket target files are
  now rejected. The broker does not delete a regular file to simulate recovery.
- Weak/missing signing material previously enabled unauthenticated owner claims.
  Production startup now requires a credential source; explicit compatibility
  mode is named and documented. Keys are private regular files with bounded
  reads. Existing authenticated sessions are revoked on rotation or expiration.
- Startup failures could have overwritten corrupt policy/job state through
  cleanup. Saves are conditional on successful load; the process suite verifies
  malformed bytes survive a rejected startup unchanged.
- Config/key reload validates both inputs before changing either authority.
  Null, unknown fields, invalid limits, public files and missing previously
  loaded config are rejected. Tests exercise invalid config during key rotation.
- The old smoke shell could return success after a failed assertion, and its
  remote cleanup referred to an unset process variable. Scripts now fail fast,
  retain explicit process ownership and check duplicate-start readiness.

This was a separate root-agent review pass, not a second agent's review. No
claim of independent human or external-agent approval is made.

## Remaining findings and limits

This batch does **not** complete Phase5. The acceptance table records all 16
items and every Multi Agent Gate requirement. Runtime evidence here concerns
principal and daemon/service lifecycle, not shared remote I/O or mutation
recovery. In particular:

- `client.doBuilt` overwrites the broker's declared ClientID, and host/secret
  capability scopes still need full isolation enforcement.
- Quota/lane admission precedes fair scheduling; shared notifications and lease
  reaping require sustained remote pressure tests and concurrency fixes.
- Risk and target are still client-supplied in the approval path; server-derived
  risk, actual operation digest, target and policy version binding remain open.
- Recovery currently treats remote lookup failures as missing jobs. This can
  discard ownership during a transient outage; crash/retry mutation tests and
  recovery fixes are required.
- Durable job event history, actual bulk transport lifetime/control SLO,
  broker-scoped metrics and long audit rotation/recovery remain incomplete.
- Real launchd install/start/recovery cannot run on either Linux environment.
  A macOS SSH target has been requested while Linux work continues.

The P5-11 and P5-13 Complete labels were withdrawn because their runtime
invariants are not proven. Do not count the passing synthetic stress test as
the required 20-process, one-remote-session benchmark.


## Committed-source verification: 020230b

On `020230b43e0ac6e74322728e72b7c7c02fee206e`, with a clean worktree:

- [Combined check/stress/local smoke/remote runtime log](evidence/phase5/2026-09-08/committed-check-020230b.log): `make check stress-broker smoke-rdevd remote-phase5-runtime` passed. Remote output explicitly records source `020230b` and daemon SHA-256 `3c0c4e6de2d9099522c1ff1da5e52b8b8c79128c8c8dad3a2f5fd4caa496a4d8`.
- [Full repository race log](evidence/phase5/2026-09-08/full-race-020230b.log): `go test -race ./... -count=1` passed all 17 packages. The agent package ran for 51.174 seconds; no race report occurred.
- The remote systemd automatic-recovery run observed PID `738125 -> 738347`; follow-up unit-file and temporary-directory queries again returned no test artifacts.

These checks validate the scope of this batch. They do not replace the missing
remote session, fairness, mutation or audit-soak evidence listed above.


## Audit identity and sink privacy follow-up

The audit follow-up fixes a runtime authorization defect: `Owner.Key()` uses
NUL between client and project, but the old writer converted it to a space
while the query compared the original key. Queries therefore lost their own
events, and converting queries the same way would have conflated owners such
as `(a b,c)` and `(a,b c)`.

New schema-1 records store `sha256:<digest>` of the exact owner key and query
only that identity. Events without a supplied timestamp now receive one at the
sink, so policy-denied events are queryable. Operation, decision and result
fields use fixed allowlists rather than storing arbitrary input after trying
to redact a few marker strings. The real daemon test sends an unprefixed secret
canary as an unknown operation and verifies it never reaches the audit file.

[Targeted audit runtime log](evidence/phase5/2026-09-08/audit-runtime.log) covers
the two colliding display identities, allowed and denied requests, separate
owner queries before and after actual process restart, and the omission marker
for pre-existing ambiguous legacy records. Unit negatives also cover arbitrary
text in every audit field. A remote run of the same process suite passed; the
committed-source verification below identifies the final artifact.

A separate review checked exact-key hashing, schema separation on reload,
zero-time handling, fixed field vocabulary, and denial of ambiguous legacy
attribution. Old schema-0 records remain in their retained disk segments for
administrator inspection. They are not guessed into a principal's query;
`audit_incomplete=true` signals their omission. No external reviewer approval
is claimed while the independent-agent authorization question is pending.

P5-14 remains In progress: this corrects owner isolation and low-sensitivity
fields but does not prove long-running rotation/recovery, audit sink failure
behavior, or end-to-end request/policy/approval correlation.


## Committed-source verification: 455ff16

Implementation commit: `455ff163dc15cd4db932a567271f45b2ac720f59`
(`fix: preserve exact principal identity in broker audit queries`).

- [Repository and remote process verification](evidence/phase5/2026-09-08/committed-check-455ff16.log): `make check remote-phase5-runtime` passed against the clean commit, including the final legacy-omission negative on the real remote daemon. Remote daemon SHA-256: `1822a66e59fed386d729259c39507bc0833f2a465208656444ead7e60bd9bb23`.
- [Broker race verification](evidence/phase5/2026-09-08/broker-race-455ff16.log): `go test -race ./cmd/rdevd ./internal/broker -count=1` passed.
- The systemd runtime test again passed actual installation and recovery (PID `743246 -> 743306`).

P5-14 and the overall Phase5 gate remain In progress for the outstanding
requirements above. This evidence does not claim sustained audit rotation.


## Real shared-session and remote principal benchmark

`make remote-session-benchmark` now starts the actual daemon and 20 separate
client processes. All clients authenticate and wait at a common start barrier,
then each performs 25 alternating remote ping/exec requests over real OpenSSH.
The test observes local `/proc` SSH children and remote `/proc` agent PIDs;
every returned ping also has to report that same agent PID throughout the run.
No dispatcher, remote operation or transport implementation is substituted.
A small SSH wrapper only supplies the administrator's chosen SSH config file.

[Initial full check and benchmark log](evidence/phase5/2026-09-08/session-benchmark.log):
three consecutive runs, each with 500 calls, 20 process PIDs, one daemon SSH
child, one remote agent PID, and 20 distinct remote principal IDs. Elapsed
workload time was 1.395–1.398 seconds; mixed cold/warm ping p95 was 109.958–117.849
ms and exec p95 54.162–57.258 ms. These are measurements of this short mixed
load, not the P5-08 control-under-bulk SLO or a sustained fairness result.

The benchmark includes ten client IDs reused in two different projects.
`Client.DoProtocol` previously replaced all callers with the shared Client's
random process identity. It now hashes a length-framed client/project tuple
into a stable protocol-valid principal ID, preserves that identity across
retry/restart, and leaves the broker's original request unchanged. Ordinary
non-broker client identity behavior is unchanged. Broker dispatch refuses
legacy peers that would strip principal metadata. Ping's optional `caller_id`
field echoes only the current request's identity, allowing the real agent to
prove both shared PID and separate principals; it is not a new credential or
an OS sandbox claim.

[Targeted identity negatives](evidence/phase5/2026-09-08/principal-identity.log)
cover distinct tuples, retry/restart stability, original-request immutability,
missing owner rejection, and fail-closed legacy execution. Startup process
negatives cover private explicit host registries: malformed/null documents,
unknown fields, duplicate names and public permissions all fail before READY.
The daemon's `-hosts-file` avoids altering the operator's global/project host
registry for isolated deployments and runtime tests.

A separate root review checked source immutability, unambiguous tuple framing,
legacy stripping, host config trust, process barriers/counting, per-call PID
consistency, and cleanup of only the generated remote namespace. External
independent review remains pending authorization. Shared sync/secret/session
routes, complete host/secret/job policy boundaries and sustained fairness
remain open; the first Multi Agent runtime criterion is now proven.


## Committed-source verification: b5e9422

Implementation commit: `b5e9422be7be55ccdb38a371e3aa76638a5e8e2d`
(`fix: preserve broker principals across shared remote sessions`).

- [Repository check and real SSH benchmark](evidence/phase5/2026-09-08/committed-check-b5e9422.log): `make check remote-session-benchmark` passed against the clean commit. All three runs used 20 independent client processes, 500 requests, one daemon SSH child, one remote agent PID, and 20 remote principals. Elapsed time was 1667.640–1681.066 ms; ping p95 was 108.610–116.191 ms and exec p95 was 66.284–82.107 ms.
- [Changed-package race verification](evidence/phase5/2026-09-08/committed-race-b5e9422.log): `go test -race ./internal/client ./internal/broker ./cmd/rdevd -count=1` passed.

These measurements prove shared-session identity preservation, not sustained
fairness or the control-under-bulk SLO. Phase5 remains In progress.

## Cancellation and atomic lease reclamation follow-up

The per-host connection setup mutex previously ignored a waiting caller's
context. It is now a cancellable per-host admission channel; cancellation of
one waiter does not interrupt another caller's initialization. Canceled dial
or secret initialization also leaves the shared host cold and retryable rather
than permanently denying all later callers as a credential failure.

The lease's idle check now atomically detaches the old connection pool under
the same lock used by attachment and request admission. Closing those detached
SSH processes happens outside the lock. Publication tokens prevent old cleanup
from changing a newly published connection's security state. Each idle
generation is reaped once, and startup's unowned pool also has an idle epoch.
Authenticated handshakes register their lease before acknowledging success.

`make remote-lifecycle` exercises the real daemon, independent authenticated
frontend processes, real OpenSSH, and the actual remote agent. The retry test
kills the exact test agent after verifying its executable path, then pauses
replacement startup in an SSH wrapper. It verifies a canceled waiting frontend
returns before that barrier is released, killing the initiating frontend lets
another owner redial, one survivor mutation executes exactly once, and killing
a foreground owner removes its remote process while another exec completes
on the same agent. The wrapper changes only startup timing; protocol dispatch
and remote operations are not replaced.

The [first lease soak](evidence/phase5/2026-09-08/lease-runtime-initial.log) ran six cycles over 65.363 seconds: three normal frontend
exits, three SIGKILL exits, 60 additional new-owner requests, no active-agent
replacement, zero SSH children/remote agents after each idle generation, and
exactly six reaper events. Configured grace was 250 ms with a five-second
reaper tick; observed cleanup was 4602.664–4815.387 ms after last-client exit.
This is repeated runtime lifecycle evidence, not an hours-long capacity soak.
The committed-source section below records the final implementation checks.

A separate root review checked lock ordering (lease then client pool), cleanup
outside admission, replacement publication tokens, canceled security state,
handshake acknowledgment ordering, actual process identifiers, and test cleanup.
No external independent-review approval is claimed. P5-05/P5-11 now have real
SSH cancellation and grace evidence; full job wait/shutdown behavior, prolonged
QoS pressure, and independent review remain separate unfinished work.

[Targeted final-source tests](evidence/phase5/2026-09-08/lifecycle-targeted.log) passed for `internal/client`, `internal/broker`, and `cmd/rdevd`.

## Committed-source verification: 2a61674

Implementation commit: `2a61674dfeb852338f3b0eb08e354401220a7e40`
(`fix: isolate broker reconnect cancellation and idle reclamation`).

- [Full check and real lifecycle verification](evidence/phase5/2026-09-08/committed-check-2a61674.log): `make check remote-lifecycle` passed against the clean commit. Three retry/cancellation runs observed 10.255–10.630 ms from the cancel trigger to the broker's dispatch-error audit event, one survivor mutation, preserved foreground peer execution and preserved shared transport. The six-cycle lease run took 65.358 seconds; cleanup took 4617.027–4823.669 ms with 250 ms grace and a 5 s reaper tick. All six generations ended with zero SSH children and remote agents.
- [Changed-package race tests](evidence/phase5/2026-09-08/committed-race-2a61674.log): `go test -race ./internal/client ./internal/broker ./cmd/rdevd -count=1` passed.
- [Real daemon race integration](evidence/phase5/2026-09-08/remote-race-2a61674.log): the retry/cancellation test also passed with both the frontend test binary and actual daemon built using `-race`. Daemon SHA-256: `fde893d1dbdb633db48ca31d82cf5a436d2ca4cd4818d33008be62c9da30a07d`. Cancellation measurements were 10.508–10.771 ms; no race or other-owner interruption occurred.

The one-mutation observation covers this transport retry scenario. It does not
prove broker crash/restart mutation recovery, detached job wait shutdown, or
sustained quota fairness. Those gates remain open, as does independent review.

## Durable scoped policy and stable admission decisions

The policy follow-up adds exact-host grants, server-selected capability
classification, a digest of each immutable decision snapshot, and durable
administrative grant/revoke before acknowledgement. Existing operation-wide
administrator grants remain operation-wide. A client-supplied capability can no
longer select an unrelated policy entry. Policy load is bounded/private and
rejects null, duplicate keys and malformed grant values instead of accepting a
nil map or ambiguous duplicate authorization.

`TestRemoteBrokerPolicyIsolation` uses real authenticated frontend processes,
the daemon, OpenSSH and remote agents. An SSH startup barrier holds wire calls
while their admission audit records capture a policy digest; an administrator
revokes the pending owner's host grant. The queued call completes with the old
digest and later calls are denied with the new digest. Both acknowledged grant
and revoke survive actual daemon SIGKILL/restart, without a graceful-save path.
A narrowly granted owner reaches only its permitted host alias; another project
with the same client ID and an owner holding an unrelated capability are denied.
Ungranted secret/job/Fleet operations are denied at the policy boundary; this is
not proof of full secret/job isolation for partially authorized principals.
A real target-directory obstruction makes atomic rename fail; the RPC reports
failure and the old active snapshot remains intact. The final process counts
verify one shared SSH child and one remote agent after crash recovery.

A separate root review identified the distinct late directory-sync failure:
the rename may already have committed even if durability cannot be confirmed.
The implementation latches a fail-closed policy state and refuses shutdown save
in that case. A fault-injection unit test performs the actual replacement and
then reports an uncertain durability result; it verifies no stale grant remains
usable and recovery reads the preserved replacement. This late-sync injection
is a unit-level fault test, not an actual remote filesystem power-loss claim.

Host-file edits still require daemon restart. Full secret resource grants,
immutable operation/target approval binding, job ownership crash windows,
request/operation/approval audit correlation and independent review remain open.

The [initial real policy run](evidence/phase5/2026-09-08/policy-runtime-initial.log)
and [final targeted tests](evidence/phase5/2026-09-08/policy-targeted.log) passed.
Authorization now precedes job registry lookup, so an owner lacking the job
capability receives the same policy denial independently of a supplied job ID.
The final committed-source verification additionally exercises that negative.

## Committed-source verification: 669f18f

Implementation commit: `669f18f5779473fc8f2e0ecaa0c227632f3b8836`
(`fix: persist scoped broker policy before acknowledging changes`).

- [Full check and remote verification](evidence/phase5/2026-09-08/committed-check-669f18f.log): `make check remote-policy remote-phase5-runtime` passed against the clean commit. All three real SSH policy runs proved stable queued decisions, durable acknowledged grant/revoke across SIGKILL, exact host/project boundaries, capability-substitution denial, authorization before job registry lookup, and rollback on an actual rename failure. The remote Linux daemon suite also rejected null, duplicate-key and public policy files before READY.
- [Changed-package race verification](evidence/phase5/2026-09-08/committed-race-669f18f.log): `go test -race ./internal/broker ./cmd/rdevd -count=1` passed.
- [Real daemon policy race integration](evidence/phase5/2026-09-08/remote-race-669f18f.log): the real SSH policy test passed with both the frontend test executable and daemon built using `-race`. Daemon SHA-256: `d8cb4abef225e60e8530228a35e83022892dcca451a3c16a56ea9cd2a3eea5be`.
- The remote service suite passed actual systemd user install/enable/start/reload/SIGKILL recovery/stop/start (PID `771663 -> 771730`). Remote non-race daemon SHA-256: `079c2ba4455caa9b16dc60752d12dcdf66d5ae17acc0eca35699b677b92ce6dc`.

P5-12 remains In progress for complete secret resource permissions, target
snapshot/approval binding and independent review. P5-14 still needs full
request/operation/approval correlation and sustained sink recovery evidence;
P5-16 still needs remote job mutation crash/replay and bounded drain validation.

## Mandatory request-bound approval follow-up

The approval path no longer trusts client Risk or Target. Every mutating wire
operation needs an approval for the server's exact owner, host/session snapshot,
wire parameters and policy digest. Arbitrary exec is included because argv and
stdin cannot be proved harmless from a client hint. `approval.create` is a
separately granted administrator RPC, exposed through the actual
`rdevd approval-create -request-file` command. CLI and MCP transmit one-use
executor tokens rather than gaining issuance authority.

Approvals use unambiguous JSON tuple framing and 256-bit random tokens, with a
bounded lifetime and pending store. Changed requests and other owners cannot
consume a valid token. The target snapshot is checked under the client identity
lease before dialing a replacement and before dispatch, and explicit approved
deadlines cannot be expanded by a later context. A separate review caught and
fixed the pre-dial ordering: checking only inside the request builder would
have allowed bootstrap/secret I/O against a changed target before rejecting
its mutation. Target-change tests assert zero dials.

`TestRemoteBrokerApprovalIsolation` uses the real daemon, Unix clients, SSH and
remote file operations. It proves Risk=false denial before transport creation,
separate issuer/executor capabilities, owner/host/path/content/append/operation
substitution denial, one actual append, replay/expiry/policy-change/restart
rejection, and retained owner-only approval/result correlation after restart.
The audit file is checked for absence of raw approval tokens and request content.
Targeted tests also cover argv, cwd, env, stdin, deadline, login settings,
project identity, target-session changes and ambiguous delimiter boundaries.

The actual CLI integration now obtains approvals from a separate administrator
principal for exec, write and job mutations. The real 20-process benchmark
pre-provisions a distinct one-use approval for each mutating request before
its start barrier; measured execution still consists of 500 remote calls per
run. Remote cancellation/lifecycle tests use the same administrator command.
These adaptations exercise authorization rather than disabling its checks.

Independent external review remains pending. Shared sync/secret/Fleet mutation
routes that are not yet implemented must integrate the same approval contract;
this batch does not claim those routes or the remaining Phase5 QoS/job/audit
recovery requirements are complete.

[Initial full check and runtime regression](evidence/phase5/2026-09-08/approval-check-runtime-initial.log)
passed `make check remote-approval remote-session-benchmark remote-lifecycle`:
three approval runs, three approved 20-process/500-call session runs, three
retry/cancellation runs, and six lease cycles over 65.362 seconds. Each session
run still had one SSH child, one remote agent and 20 distinct remote principals.
[Changed-package race tests](evidence/phase5/2026-09-08/approval-race-initial.log)
also passed. Committed-source verification below identifies the final artifact.

## Committed-source verification: 915fb5e

Implementation commit: `915fb5e2f15d48ac078f44a417543517d5627d90`
(`fix: require request-bound approval for shared remote mutations`).

- [Full check and runtime regression](evidence/phase5/2026-09-08/committed-check-915fb5e.log): `make check remote-approval remote-session-benchmark remote-lifecycle remote-phase5-runtime` passed against the clean commit. This includes three actual approval-negative runs, three approved 20-process/500-call SSH runs, three cancellation/retry runs, six lease cycles, remote Linux startup/credential/audit tests, and actual systemd user service recovery.
- All session runs retained one daemon SSH child, one remote agent and 20 remote principal IDs. Workload elapsed time was 2514.195–3066.663 ms; ping p95 was 108.256–114.343 ms and exec p95 140.147–161.611 ms. Approval provisioning happens before the workload barrier. These short-load measurements do not establish sustained fairness or the bulk/control SLO.
- The six-cycle lease run took 65.357 seconds and reclaimed every idle generation, ending with zero SSH children/agents. Cleanup was 4521.210–4785.471 ms for 250 ms grace plus the five-second reaper tick.
- [Changed-package race verification](evidence/phase5/2026-09-08/committed-race-915fb5e.log): `go test -race ./internal/client ./internal/broker ./cmd/rdevd ./internal/mcpsrv -count=1` passed.
- [Actual daemon approval race integration](evidence/phase5/2026-09-08/remote-race-915fb5e.log) passed with both daemon and frontend test executable built using `-race`. Daemon SHA-256: `c952bad4313ed3558b6f01deb8b300b33488d917d1d5181f013cc0dc3821d405`.
- Remote non-race daemon SHA-256: `d492b4b63ef3f2106151a61c405eaafd994a14fdda1e76868775ee9e82dd3d08`; systemd install/enable/start/reload/SIGKILL recovery/stop/start passed (PID `793066 -> 793175`).

The pending independent review and remaining shared mutation routes keep P5-13
In progress. Job mutation durability, fairness, dedicated bulk transport, audit
sink recovery and launchd runtime requirements remain separate open gates.


## Fair admission, dedicated bulk and asynchronous audit follow-up

The scheduler now selects eligible work before atomically occupying global,
per-host, per-owner and lane capacity. Queued/canceled callers consume no
execution slots. Control has reserved execution and queue capacity; existing
backlogs receive SIGHUP weight changes. Owner accounting is removed when idle,
and history is bounded. The replacement removes the old single-wake channels
and quota/lane-before-fair-queue path instead of leaving competing admission
mechanisms. Job-wait coalescing now occurs before scheduler admission and its
key uses an unambiguous owner/host/job-parameters tuple; full durable shared-job
observation remains a separate task.

Bulk file I/O opens one additional transport per host while retaining the base
agent. It holds the same immutable identity lease and uses the initialized
secret store, without another secret initialization. Idle bulk generations are
detached atomically and closed outside the admission lock; stale cleanup cannot
close a replacement or alter base security status. Read-only bulk reconnects
never replace the base; ambiguous bulk writes are not transparently replayed.

The actual remote workload first disproved the existing SLO: control p95 was
3.45–3.72 times baseline. A dedicated transport alone still produced ratios
3.68–4.29. A diagnostic build of the real daemon (CPU and mutex profiling only;
no production dispatch replacements) measured 115.15 seconds of aggregate mutex
wait in `AuditLog.Append`, 99.87% of sampled mutex wait. Synchronous file sync
under the shared audit lock was the dominant serialization point. After moving
that I/O to a bounded asynchronous writer, loaded p95 fell from about 31 ms to
3.7 ms, but the improved 1.4 ms baseline still made the ratio exceed two. Large
JSON responses also dominated CPU profiles. An explicit, configurable 8 MiB/s
bulk payload budget now paces the next bulk admission without holding a worker
or lease. The initial same-payload run passed at ratios 1.51 and 1.39; the test
still fails above two and still requires continuously backlogged owners.

Audit append retains bounded sanitized owner history and queues at most 1024
records (plus a 64-record writer batch). Sink stalls/failures do not hold the
request mutex. The file writer validates private bounded segments, repairs a
torn active-file tail, rotates within two bounded segments, reports drops and
errors, and retries after a recoverable rotation obstacle is removed. Explicit
query/flush barriers expose durability failure. Global health counters require
a separate `audit.health` grant; ordinary owners retain only scoped status and
audit queries. No raw payload or credential field was added to telemetry.

Separate source review and targeted tests cover worker activation, skipping an
ineligible host, owner queue saturation, control reservation, weight reload,
cancellation cleanup, byte pacing, bulk active/idle/cleanup races, base and
secret preservation, no mutation replay, sink stalls, full queues, actual
rotation-directory failures, private-file negatives and corrupted tails. A
review also corrected a test that checked all-owner cleanup after waiting for
only one owner's cancellation; it now waits for both actual active counts to
reach zero. External independent review has not been performed.

`make remote-qos` runs three real 20-process workloads. Each uses two separate
project principals with the same client ID, nine bulk processes per owner, two
control processes, 344064-byte integrity-checked SSH reads, a five-second
baseline and two 25-second saturated windows. Weights reverse 3:1 -> 1:3 on the
same live sessions. It verifies quotas, overload, no sampled starvation, exact
owner status, SIGKILL of all nine processes for one owner, survivor progress,
one base plus one bulk SSH process under load, bulk TTL cleanup preserving the
base PID, audit rotation with no drops/errors, and global-health denial for an
ordinary owner. Reported fairness counts are actual dispatch counters; latency
samples are measured by the independent frontend processes.

Remaining risks: this workload does not prove the complete long-exec/job-wait/
status/sync matrix, all payload sizes or all secret/job/Fleet permissions.
`max_hosts` is still an active-host bound rather than warm-pool LRU; saturation
at a small bound and frontend ingress buffers need further work. Audit crash
can lose queued in-memory records, and a short repeated workload is not a
long-duration crash/rotation soak. Full operation IDs, detached-job crash/replay,
bounded shutdown state, launchd runtime and independent external review remain.
The Phase5 and Multi Agent records remain In progress.

The [pre-commit repeated runtime log](evidence/phase5/2026-09-08/qos-runtime-precommit.log)
passed all three 66-second runs (198.265 seconds total). The six control ratios
were 1.423–1.519, with loaded p95 1.857–1.959 ms. Both owners were backlogged in
all 150 sampled windows. Each run rotated one audit segment and persisted
48798–48822 events with zero dropped events or sink errors. Each read contained
344064 verified bytes; per-owner payload counters were roughly 203 MiB and
233 MiB per run, including the survivor-only period. The
[diagnostic contention profile](evidence/phase5/2026-09-08/qos-audit-contention-before.txt)
records the preceding failure's audit-lock attribution. These are working-tree
measurements; final committed-source commands and artifact identity follow.

[Pre-commit regression](evidence/phase5/2026-09-08/qos-regression-precommit.log)
passed `make check remote-approval remote-policy remote-session-benchmark
remote-lifecycle`. This includes three actual approval runs using one base and
one file-I/O bulk agent, three policy crash/isolation runs, three 20-process/
500-call base-session runs, three retry/cancellation runs and six real idle
lease cycles (65.368 seconds, final zero SSH/agent processes). All three session
runs retained one base agent and 20 distinct remote principals. Changed-package
race tests passed, followed by
[20 repeated scheduler/audit/fair-queue race runs](evidence/phase5/2026-09-08/qos-race-repeat-precommit.log).


## Committed-source verification: 6f62f60

Implementation commit: `6f62f60e513c2c4dd560668dc6b530de2de6494a`
(`fix: enforce fair broker admission and protect control latency`).

- [Full check, QoS, stress and smoke](evidence/phase5/2026-09-08/committed-check-6f62f60.log): `make check remote-qos stress-broker smoke-rdevd remote-phase5-runtime` passed against the clean implementation commit, using official Linux Go 1.25.0. All four embedded-agent platform artifacts were rebuilt and verified.
- Three 66-second real 20-process QoS runs passed (198.276 seconds total). The six loaded/control p95 ratios were 1.267–1.488, with loaded p95 1.700–1.932 ms. Both owners were continuously backlogged in all 150 one-second samples. The measured dispatch ratio was 2.993–3.020 at weights 3:1 and 0.331–0.333 at weights 1:3.
- Each run preserved one base agent, used one independent bulk transport while busy, and closed bulk after the configured one-second idle TTL plus sweep interval. Killing all nine processes of one owner released its queue and did not interrupt the surviving owner or replace the base agent. Per-owner payload counters and global-health authorization negatives passed.
- The three runs wrote 146438 audit records in total, completed three actual rotations, and reported zero dropped records or sink errors. Both segments stayed within 8 MiB per run. This proves repeated short rotation under load; it does not prove a long crash/recovery soak or lossless SIGKILL of queued audit records.
- [Full repository race verification](evidence/phase5/2026-09-08/committed-race-6f62f60.log): `go test -race ./... -count=1` passed.
- [Real daemon race integration](evidence/phase5/2026-09-08/remote-race-6f62f60.log): approval isolation, retry/cancellation and policy isolation all passed with both the real daemon and frontend tests built using `-race`. Daemon SHA-256: `224b1159393c4a97901d2aa692dee073d8831dce80e637cea0c4673c183bba00`.
- Linux remote startup/authentication/audit/restart tests and actual systemd user install/enable/start/reload/SIGKILL recovery/stop/start passed (PID `824360 -> 824426`). Remote non-race daemon SHA-256: `13d1fbd8efe2c744ff340f86fd2d47cacdd7bd4557a126da55cc0b561fcc66c0`.

P5-06/P5-07/P5-08/P5-14 retain In progress labels for the remaining scope stated
above. In particular, the complete mixed job/sync workload, warm-host capacity,
ingress bounds, long audit recovery, durable job mutation/observations, shared
secret/Fleet routing and permissions, launchd runtime and independent external
review are not inferred from these passing tests.


## Detached job recovery and scoped pagination follow-up

A fresh review of startup recovery found that a failed SSH probe deleted durable
job ownership. The registry also changed memory before persistence and held its
reader lock across disk I/O. A remote delete followed by a local rename failure
could therefore hide the owner's job while leaving a stale disk snapshot.

The registry now serializes private bounded versioned snapshot updates, persists
before publishing, keeps readers independent of filesystem latency, and latches
a fail-closed state after uncertain post-rename durability errors. Shutdown
cannot overwrite that possibly committed snapshot. Strict legacy-array migration
rejects null, unknown/future schema, duplicate fields/references and invalid owner
keys. Startup remote errors or absent files never remove ownership; only an
explicit owned removal result does. A remote Missing result lets the same owner
finish an interrupted deletion without granting access to unknown jobs.

Review also found that remote pagination occurred before broker owner filtering.
Another project's newer jobs could hide the caller's older job with Limit=1.
The broker now captures an immutable owner ID filter before admission, including
a list with omitted parameters; the agent applies it before pagination/totals.
This uses the negotiated `job_filter_ids` feature and fails before sending to an
older agent. Returned IDs must remain inside the requested scope, and removal
results cannot change the exact approved target. Completion audit follows durable
ownership publication. Targeted negatives cover these boundaries and late sync
uncertainty, failed snapshot publication, private-file validation and recovery.

`make remote-jobs RDEV_SSH_CONFIG=/path/to/ssh/config` repeats a real OpenSSH test
three times. Two principals share a client ID and differ by project. A separate
frontend starts a detached job, both projects test Limit=1 lists, and the fully
granted other project is denied status/logs/wait/stop/rm. The daemon is SIGKILLed
while jobs run; its SSH executable refuses connections on restart; both records
must survive. Restoring SSH and SIGHUP preserve the original supervisor PIDs.
The test then forces an actual local rename failure after remote deletion,
repairs the path, completes the owner's Missing cleanup, and checks both that
removal and the other project's ownership across further SIGKILLs. Each remote
command writes a proof file exactly once. Cleanup is restricted to this test's
remote namespace and verified supervisor/child process groups.

The working-tree runtime and committed-source validation below passed. This does **not** close the pre-ACK job-start
crash window: durable mutation intents, stable operation IDs, job event history,
upgrade and bounded shutdown still need implementation/runtime evidence.
P5-09/P5-10/P5-16 and the overall gate remain In progress.


## Committed-source verification: e3ac9c2

Implementation commit: `e3ac9c2` (`fix: preserve detached job ownership through
recovery failures`).

- [Complete check and real integrations](evidence/phase5/2026-09-08/committed-check-e3ac9c2.log): `make check remote-jobs remote-approval remote-policy remote-phase5-runtime` passed from the clean commit.
- Three normal-daemon real job recovery runs passed; each tested acknowledged start, scoped/omitted-parameter listing, granted other-project denial, SSH-unavailable restart, SIGHUP and deletion persistence recovery. Supervisor PID and exactly-once proof-file assertions passed.
- [Full repository race](evidence/phase5/2026-09-08/committed-race-e3ac9c2.log): `go test -race ./... -count=1` passed.
- [Real daemon race recovery](evidence/phase5/2026-09-08/remote-race-e3ac9c2.log): three additional real job recovery runs passed with both frontend tests and actual daemon instrumented by `-race` (96.454 seconds total). Daemon SHA-256: `cb04c7147be1862cec255a7dcadfb9d2014b4ea6a68432e7aadefd11dc353838`.
- Actual Linux systemd user installation/enable/start/reload/SIGKILL recovery/stop/start passed again (PID `842964 -> 843028`). Existing remote policy and approval negatives also passed.

This evidence covers acknowledged detached-job recovery and removal failure,
not durable pre-ACK start intents/replay, persistent event history or bounded
shutdown. No additional Phase5 row or Multi Agent Gate is marked Complete.


## Shared wait disconnect, shutdown and TERM output follow-up

Review found that the initiating disconnected frontend kept its handler and
lease for the whole remote wait. Observations now run under a service-owned
context with independent lease/drain accounting. Each frontend cancellation
releases its subscriber promptly; zero subscribers preserve the bounded remote
wait for reconnect. Shutdown cancels observation contexts before drain, while
detached supervisors continue. Observer admission includes disconnected entries
and uses global/owner active-plus-queue bounds; subscribers have separate
512-global/128-owner caps. Status exposes only the authenticated owner's active
observers/subscribers. Coalescing keys include owner, host, parameters and any
explicit semantic deadline.

The first real 20-process run caught an existing remote defect: job_stop TERM
killed the supervisor before its bounded stdout sink flushed. All waiters lost
the requested terminal tail. Modern supervisors now relay TERM to the child
group and remain alive through output/ledger/status publication. Their metadata
marks relay support so legacy and forced-KILL handling remain explicit. Child
start identity is persisted, relay rejects a reused leader PID, and the periodic
ledger writer is joined before final ledger publication. A targeted process test
checks one TERM trap, retained stdout/tail and matching durable byte accounting.

`make remote-wait` repeats the real OpenSSH scenario three times. After the
initiator is SIGKILLed, status must report 1 observer/0 subscribers. Twenty
independent frontend processes reconnect to that same observation; five further
SIGKILLs leave 1/15. SIGHUP preserves those counters. The remaining 15 terminal
responses must contain the expected tail and share one nonempty remote operation
ID. A fully granted other project sees zero counters and cannot join the job.
A second running job has an active wait during SIGTERM; shutdown must finish
within five seconds, and restart must find the same live detached supervisor.
The first repaired run passed with a 10.435 ms daemon shutdown.

This proves live wait coalescing, reconnect, cancellation and observation drain;
it does not prove durable event replay after daemon crash, a full streaming API,
bounded retention of WatchHub history or all mutation shutdown windows. These
remain explicit P5-09/P5-16 gaps. Committed-source validation follows below.


## Committed-source verification: bd03436

Implementation commit: `bd03436` (`fix: detach shared job subscribers and
preserve TERM output`).

- [Complete check, stress and real lifecycle](evidence/phase5/2026-09-08/committed-check-bd03436.log): `make check remote-wait remote-jobs remote-lifecycle stress-broker smoke-rdevd remote-phase5-runtime` passed from the clean commit.
- Three real 20-process wait runs passed, including identical terminal tail/operation IDs for 15 surviving subscribers, owner isolation and zero-subscriber reconnect. Normal daemon SIGTERM drain measured 10.618–14.585 ms; the detached job supervisor survived each restart.
- Three detached-job recovery runs, three actual retry/cancellation runs and six remote lease cycles passed. Lease cycles took 65.361 seconds, retained the active agent, serviced 60 new-owner requests and ended with zero daemon SSH children. Broker stress passed 100 runs.
- [Full repository race](evidence/phase5/2026-09-08/committed-race-bd03436.log): `go test -race ./... -count=1` passed, including the process TERM/output/ledger regression.
- [Real daemon race wait](evidence/phase5/2026-09-08/remote-race-bd03436.log): two additional 20-process runs passed with the actual daemon and frontends instrumented by `-race` (89.446 seconds total). Drain measured 1.016–1.017 seconds including the race runtime's exit delay. Daemon SHA-256: `722def9da6f53652e900bb4cf7d612ea735cec5bbcbcce8d2494190bb1743279`.
- Linux remote readiness/authentication/crash tests and real systemd user install/enable/start/reload/SIGKILL recovery/stop/start passed (PID `855303 -> 855364`). Remote normal daemon SHA-256: `ad14e6c430edc421f0a7833cb3401a62c8c869da6680a712f4f990731e84a36b`.

Durable job event history, bounded replay retention, pre-ACK mutation intents,
all mutation shutdown windows and the remaining mixed workload/platform/review
gates remain incomplete. Passing live fan-out does not close those requirements.


## Replay digest semantic-control correction

The mutation-intent audit found that `CanonicalRequestDigest` omitted the
request's `State` and `Capability` fields. A reused operation ID could therefore
select a cached answer with different dry_run or refresh semantics. Both fields
now participate in the digest; targeted negative tests cover migrate/repair
preview-versus-apply and capability refresh.

`make remote-replay-digest` deploys the actual agent through the broker, then
starts another actual agent in a separate test state directory over OpenSSH.
It sends stable operation IDs directly to the remote cache and asserts
`request.operation_id_conflict` for state_migrate/state_repair dry_run changes
and capability refresh substitution. No manifest may be created. All three
working-tree runs passed. This direct-agent test intentionally avoids the local
Client's currently regenerated operation IDs, so it proves the remote digest
boundary without claiming durable broker mutation-intent recovery.


## Committed-source verification: 20b4615

Implementation commit: `20b4615` (`fix: bind state and capability controls to
replay digests`).

- [Complete check and remote regression](evidence/phase5/2026-09-08/committed-check-20b4615.log): `make check remote-replay-digest remote-wait remote-jobs remote-approval remote-policy stress-broker smoke-rdevd remote-phase5-runtime` passed from the clean commit.
- Three real remote replay-digest runs rejected state migrate/repair dry_run substitution and capability refresh substitution with operation_id_conflict; the preview state root retained no manifest. Three real 20-process wait runs, three job recovery runs, mandatory approval and policy negatives also passed.
- [Full repository race](evidence/phase5/2026-09-08/committed-race-20b4615.log): `go test -race ./... -count=1` passed.
- The 100-run broker stress, local readiness smoke, remote Linux daemon lifecycle and systemd user installation/enable/start/reload/SIGKILL recovery/stop/start all passed (PID `860724 -> 860819`). Remote daemon SHA-256: `4a45773f43a7658b49deb475716451a0480b824d13da35fdcb3339006483326b`.

This closes the discovered digest omission. It does not add durable operation
IDs, broker mutation intents or persistent remote deduplication. P5-10/P5-16
and the crash/replay gate remain In progress.


## Durable mutation intents and pre-acknowledgment recovery

The broker now persists an owner-scoped mutation intent before queue admission
and a possibly-executed boundary before remote I/O. Its private schema-1 snapshot
stores only identity/binding/outcome metadata, never request bodies or raw output.
Failed pre-rename writes preserve the active snapshot; uncertain directory sync
fails closed. Restart converts prepared intents to not_sent and dispatched
intents to ambiguous. Stable operation IDs remain reserved for every outcome.

Job starts additionally reserve an owner-bound job ID before dispatch and persist
a remote identity tombstone before launching a supervisor. A new remote agent
can recover matching metadata after cache loss but cannot start a missing or
removed replay. Original owner, host/session digest, job ID, operation ID and
request digest must all match for recovery. A pending start cannot race job_rm.

A separate post-implementation code review found and fixed these boundaries:

- A second agent's not_sent rejection cannot prove the first attempt did not
  execute. Mutating retries retain ambiguity unless durable recovery succeeds.
- Correlated first-attempt remote rejections and terminal handler failures must
  be recorded distinctly, so failed start reservations can be cleaned without
  weakening protection of actually ambiguous starts.
- Strict snapshot parsing now bounds nesting to 32 in addition to byte/count
  limits and duplicate/null/unknown-field checks. One owner's retention limit
  does not evict replay identities or alter another owner's per-owner budget.
- Audit correlation uses a hashed operation_ref; raw caller-selected operation
  IDs and payloads are excluded from the audit.

This is a second code-review pass by the implementing agent, not independent
external review. That acceptance requirement remains open.

`make remote-mutation` uses actual OpenSSH, production daemon and remote agent,
independent frontend processes and a wrapper holding selected terminal responses.
The wrapper changes response delivery only; it cannot replace remote execution.
The targeted assertions are:

1. A detached job writes once before the broker receives its start ACK; SIGKILL
   and an SSH-unavailable restart retain intent and ownership. Connectivity
   restoration finds the same supervisor and resolves only the original owner.
2. Same-ID replay and changed-command substitution are refused. A new remote
   agent recovers matching metadata, while a deleted job's tombstone refuses
   execution. Completed intents do not resurrect removed job ownership.
3. An append executes once before SIGKILL. Restart retains ambiguity, and replay
   cannot append again. A real intent-file rename failure sends no mutation.
4. Real CLI and MCP stdio processes propagate explicit operation IDs, query
   recovered outcomes, reject other projects and prevent duplicate appends.
5. Definitive pre-admission rejection and handler failure permit owner-only
   reservation cleanup without permitting the old ID to execute later.
6. SIGTERM with an already-executed write's terminal response held preserves
   ambiguity and refuses a duplicate after restart. Audit queries prove owner
   isolation, hashed operation correlation and absence of raw IDs/payloads.

The new SIGTERM injection initially failed all three normal runs and the actual
race daemon: shutdown exceeded the existing 12-second process bound. Full check
and full repository race had passed that implementation. Investigation found
that Service.Close spent its entire ten-second context on graceful drain before
starting cancellation and transport cleanup. It now reserves half the remaining
budget (at most five seconds) for grace, cancels the scheduler, bounds transport
teardown and waits for request outcome publication within the original context.
The initial fixed runs exited in about seven seconds, with ambiguous state
preserved. Committed-source verification below provides final measurements.

Remaining limits: identities are retained without eviction (8192 global, 1024
per owner; broker snapshot also 8 MiB), and safe retirement is not implemented.
Generic uncertain mutations have no stored raw output or automatic application
reconciliation. Tests cover process crashes, not physical remote-host power loss;
remote job metadata remains atomically published rather than power-loss tested.
Durable event replay, remaining shared routes, full workload/upgrade/failure
matrices, macOS launchd and external independent review remain incomplete.


## Committed-source verification: a96b359 (2026-09-09)

Implementation: `a96b3599b409b0b4e47e4e31272243ada7f2722a` (`fix: persist mutation
intents and recover pre-ACK job starts`), pushed to origin/main before verification.

- [Full check, stress and real runtime](evidence/phase5/2026-09-09/committed-check-a96b359.log): `make check remote-mutation remote-jobs remote-wait remote-policy remote-approval remote-session-benchmark remote-lifecycle remote-qos stress-broker smoke-rdevd remote-phase5-runtime` passed from the clean implementation commit.
- Three pre-ACK crash runs proved job and append once-only behavior, durable fresh-agent job recovery/tombstones, SSH-outage owner preservation, intent rename failure, actual CLI/MCP recovery queries and duplicate protection, terminal rejection cleanup, and hashed audit correlation. Stalled-response SIGTERM took 7.017–7.018 seconds, below the unchanged 12-second process bound.
- [Full repository race](evidence/phase5/2026-09-09/committed-race-a96b359.log): `go test -race ./... -count=1` passed.
- [Actual daemon race mutation test](evidence/phase5/2026-09-09/remote-race-a96b359.log): the production daemon and test frontends ran with `-race`; the real remote scenario passed in 76.655 seconds, with shutdown at 8.030 seconds including the race runtime exit delay. Daemon SHA-256: `83065555f8af58aa138ac2502fa9ee44f4c35e231a4efef48c64416c896b81b7`.
- Three 20-process QoS runs met the unchanged 2x SLO: ratios 1.255–1.607. Across these runs the audit wrote 146350 records, rotated three times, and reported zero drops/errors. Weight reversal, owner SIGKILL isolation, dedicated bulk transport TTL and base-agent preservation passed.
- Three 20-process shared-session runs, three shared-wait runs, job/policy/approval recovery, retry/cancellation and six lease lifecycle cycles passed. The lease scenario lasted 65.349 seconds, served 60 new-owner requests, and ended with no daemon SSH children.
- Local smoke, actual Linux daemon readiness/authentication/crash checks and systemd user install/enable/start/reload/SIGKILL recovery/stop/start passed (PID `902839 -> 902916`). Remote normal daemon SHA-256: `6e2e65a45d11e9a4e925067214e4d7a60ec1051b54ec2fd6c8c71cd20fc100d2`.

A subsequent review found a concurrent-resolution edge: multiple status/wait
callers can read the same ambiguous intent, and callers after the first durable
transition can receive an unnecessary transition error. A targeted regression
reproduces it in this commit; the following correction handles that race.
This does not invalidate the above single-resolver measurements or make the
unfinished event/resource/platform/review gates Complete.


## Concurrent resolution correction

After reading an ambiguous intent, multiple status/wait calls can each validate
the same remote job identity. The first persists completed successfully; the
previous implementation returned invalid transition to its peers. The correction
rereads after a transition error and accepts only an already-completed successful
record with exactly the same immutable binding. Storage uncertainty, different
bindings and other errors still fail closed. No extra remote mutation or durable
write is performed for followers.

`TestConcurrentMutationJobResolutionSharesDurableOutcome` holds the first durable
publication while 32 callers resolve it. It reproduced the old failure and passed
ten race runs after the fix. The real remote mutation test now holds a status
response while twenty independent frontend processes submit the same owned job
status, then verifies successful recovery for every process. Its initial isolated
worktree run passed all crash/append/CLI/MCP/shutdown checks in 11.844 seconds.
Committed-source commands and results follow below. This is implementing-agent
review; independent external review remains pending.


## Committed-source verification: a3ccb8d

Implementation: `a3ccb8db10e794137c0f5f8706c17f0ff00da9cb` (`fix: coalesce
concurrent durable job recovery outcomes`), pushed before validation.

- [Full check and real regressions](evidence/phase5/2026-09-09/committed-check-a3ccb8d.log): `make check remote-mutation remote-wait` passed. Three mutation runs each recovered the same pre-ACK job with twenty independent status processes, alongside all crash, replay, owner, CLI/MCP and shutdown assertions. Stalled-response shutdown was 7.017–7.020 seconds. Three separate twenty-process shared-wait regressions also passed.
- [Broker and daemon race](evidence/phase5/2026-09-09/committed-race-a3ccb8d.log): `go test -race ./internal/broker ./cmd/rdevd -count=1` passed, including the 32-caller durable-resolution regression.
- [Actual daemon race recovery](evidence/phase5/2026-09-09/remote-race-a3ccb8d.log): twenty independent status processes recovered the same intent successfully with the production daemon and test frontends built using `-race`; the full scenario passed in 98.297 seconds. Stalled-response shutdown was 8.023 seconds. Daemon SHA-256: `b26498378bb064605ef4032c65561d721d8bb98b2ed72f69a8a2400955c157da`.

The broader full-repository race, stress, QoS and Linux systemd measurements
remain those of `a96b359`; this narrower correction was checked with the full
repository check plus affected-package race and real recovery/wait paths.
Durable event history, remaining shared routes, operational retention, platform
coverage and independent external review remain unfinished.


## Durable job state history

The event-history implementation stores bounded owner/host/job-scoped metadata
in a private versioned snapshot. It deduplicates identical observations, retains
stream/sequence cursors across restart and explicitly reports retention gaps.
A shared wait persists state in its single observation worker before fan-out,
so losing every subscriber no longer loses the terminal history. The previous
unbounded WatchHub cache of full response/output objects is replaced by bounded
completion hints; the shared key is hashed rather than retaining request JSON.

The post-implementation response review found that extra job-list/lifecycle
fields could escape the operation-specific owner checks. Result-union validation
now rejects those fields before any event or frontend projection; targeted
negative coverage includes a status reply carrying another owner's job list.
This is an implementing-agent review pass, not independent external review.

`TestRemoteBrokerJobEventHistory` passed its initial real OpenSSH scenario in
2.956 seconds: twenty independent wait frontends were SIGKILLed, one remaining
observation persisted one terminal event without subscribers, daemon SIGKILL
and restart preserved the original cursor, and removed-job history remained
available only to its original owner. Actual CLI/MCP processes replayed the
terminal/removal events and rejected another project. A real `.events` rename
failure preserved the old snapshot; a later owned status repaired exactly one
terminal event. The persisted file contained no command or output canary.

Unit/race scenarios cover 32 simultaneous terminal observations sharing one
durable event, cursor paging/restart, host/project isolation, owner/job retention
gaps, immutable snapshots after write failure and uncertain-commit restart.
Daemon runtime startup negatives include malformed/null/public event snapshots.
Full and committed-source verification follows below.

This records observed state changes and exposes a pull replay API. Full push
streaming, prolonged remote event-retention pressure, physical power-loss tests
and independent external review remain open, along with the broader shared
secret/session/sync/Fleet and platform gates.


## Verification: 371fa30

Implementation: `371fa30cca13327acab161a4d5e0a640f3f9796c` (`feat: persist scoped
job state history and cursor replay`), committed and pushed after validation.

- [Implementation check and remote regressions](evidence/phase5/2026-09-09/validation-check-371fa30.log): the final code passed `make check remote-events remote-mutation remote-wait remote-jobs remote-phase5-runtime` before commit. Three real event-history runs passed all zero-subscriber, restart, removal, CLI/MCP, owner and storage-failure assertions.
- [Full repository race](evidence/phase5/2026-09-09/validation-race-371fa30.log): the final code passed `go test -race ./... -count=1` before commit.
- [Actual daemon event-history race](evidence/phase5/2026-09-09/validation-remote-race-371fa30.log): the daemon and frontend test processes ran with `-race`, passing the real twenty-client/zero-subscriber/restart/history-repair scenario in 43.093 seconds. Daemon SHA-256: `c62b133f194648bae4ce18ecabbb3442ee26893db36a5bac7b92ed44a42863a9`.
- [Committed artifact/runtime verification](evidence/phase5/2026-09-09/committed-runtime-371fa30.log): after push, `make remote-events remote-mutation remote-wait stress-broker smoke-rdevd remote-phase5-runtime` passed with artifacts rebuilt from the clean commit. Three further real history runs, concurrent mutation recovery, shared waits, 100-run broker stress, readiness and actual Linux systemd recovery all passed.
- Linux daemon startup rejected malformed, null and public event snapshots while preserving invalid state. The systemd user install/enable/start/reload/SIGKILL recovery/stop/start cycle passed (PID `923832 -> 923926`). Remote normal daemon SHA-256: `b81c729e98fd0772ef19abb334833c200071eb700d84738d390a1d2226a117c8`.

The first three logs are final-code pre-commit validation, not falsely labeled
as committed-source runs. The last log proves the rebuilt committed artifacts.
Push streaming, prolonged history-retention pressure, independent review and
the remaining broader Phase5 gates retain their incomplete status.

## Frontend ingress accounting and cancellation

The ingress follow-up bounds sockets before creating goroutines, authenticates
before owner admission, replaces unlimited JSON decoding with document and
aggregate byte budgets, and bounds per-connection queued requests and response
write time. Exact client/project status never exposes another principal's usage.

An implementing-agent review found two lifetime details requiring corrections:
first-byte arrival must not extend the absolute handshake deadline, and a shared
wait must retain an independent byte charge after its initiating frontend exits.
A separate delimiter arriving after a document also must not start a partial-frame
timer on an otherwise idle authenticated session. Targeted tests cover these
cases and idempotent detached-charge release; this is not external independent
review.

The real ingress scenario uses the actual daemon, Unix sockets, OpenSSH and
remote agent. It saturates 32 connections for one owner, sends 28 MiB of incomplete
JSON, rejects the next request by owner byte budget, rejects an oversized frame,
expires 16 anonymous handshakes including a late first byte, and reclaims a slow
reader after two seconds. A selected real SSH response is held while the frontend
queue overflows; the active request must cancel and another project must still
ping through the original remote agent PID. The existing event-history runtime
scenario additionally asserts that a zero-subscriber observation keeps its byte
charge until its terminal event has been persisted, then releases it.

An initial slow-reader assertion sampled the kernel-buffered request before the
daemon had decoded it completely. The corrected test waits for the full encoded
charge, then independently requires the two-second write timeout to release it.
No timeout, byte cap, owner-isolation assertion or control SLO was relaxed.
Final-code pre-commit `make check remote-ingress remote-events remote-wait
remote-mutation stress-broker smoke-rdevd` passed. Three ingress runs took
8.56–8.62 seconds each; each retained the original remote agent. Three event
runs verified detached observation accounting, three twenty-process shared-wait
runs passed, and three mutation crash runs drained held-response shutdown in
7.019–7.028 seconds. Full repository `go test -race ./... -count=1` also passed.
Actual-daemon race and committed-artifact evidence follow below.


## Committed ingress verification: 7026bdf

Implementation `7026bdfa7372836a8be019d4acc3c534eeb5a8aa` was pushed to
`origin/main`. This batch adds frontend resource bounds and detached-observer
accounting; it does not mark additional P5 items Complete.

- [Final-code check and regressions](evidence/phase5/2026-09-09/validation-check-7026bdf.log): full check; three real ingress/event/wait/mutation runs each; 100-run broker stress and readiness smoke passed before commit.
- [Full repository race](evidence/phase5/2026-09-09/validation-race-7026bdf.log): every package passed on the final implementation code before commit.
- [Actual daemon race](evidence/phase5/2026-09-09/validation-remote-race-7026bdf.log): zero-subscriber history/charge lifecycle passed in 42.10 seconds; ingress pressure passed in 14.97 seconds. Race daemon SHA-256: `cec346c23eb9c6c306206cfce3ac4ed754c4ff3669f3609bb577b3f98d98e193`.
- [Committed check and first QoS attempt, including failure](evidence/phase5/2026-09-09/committed-check-qos-failure-7026bdf.log): committed-artifact full check and three ingress/event runs passed. The third QoS run failed one control p95 ratio at 2.043; the first two passed. A separate full-repository build/test batch overlapped the third run. The failure is retained, not discarded.
- [Isolated QoS and service regressions](evidence/phase5/2026-09-09/committed-qos-runtime-7026bdf.log): after all other build/test processes completed, the unchanged committed code and unchanged two-times threshold passed three consecutive 20-process QoS runs, ratios 1.297–1.443. They wrote 146462 audit records, rotated three times and reported zero drops/errors. Stress, readiness, real daemon credentials/reload/crash and actual Linux systemd installation/recovery all passed. Systemd PID changed `950298 -> 950368`; remote daemon SHA-256: `d44ca0e01426af2404d1661661e753f90e601d840bdb2830bb2a3bee0e450bba`.

The evidence proves the defined remote workload when run independently.
Concurrent unrelated build/test activity is a plausible source of the observed
2.043 ratio, not a proven causal diagnosis or a passing workload guarantee.
Wider host-contention, mixed workload, response-allocation and independent-review
coverage remain open. Do not overlap unrelated builds/stress with the final
baseline-versus-bulk measurement; keep failed runs in the evidence record.


## Exclusive shared frontend routing and scoped resource projection

The CLI previously checked a subset of broker commands and then fell through to
`client.New` for other commands. A direct before/after execution reproduced the
problem: with a nonexistent broker socket, the old `secrets list` exited zero and
returned a local result. The corrected frontend rejects unsupported shared
commands before touching the standalone secret store. Unknown shared job
subcommands now return an error instead of successful empty output.

`rdev broker status` / `rdev_broker_status` project only the calling principal's
existing scheduler, ingress and wait counters, with the policy digest. Directory
listing now also uses the broker in both CLI and MCP. The actual-process test
keeps eight sockets open for project A while B sees only its own connection,
checks default-denied project/host queries, and retains the same real SSH agent.
A frontend-only SSH/rsync trap and private project directory verify that unsupported
sync/secret/host/state/env/job commands neither spawn direct transports nor alter
the local host registry. The daemon itself still uses real OpenSSH.

The initial corrected real frontend scenario passed in 2.075 seconds. This
implementing-agent review closes an unintended fallback, not the outstanding
shared sync/secret/session implementations or independent external review.
The implementation passed full check and three real frontend/history/mutation
runs, three retry-cancellation runs, and six lease cycles over 65 seconds.
Changed-package race passed. Actual daemon and CLI race binaries passed the
frontend boundary scenario in 11.41 seconds. The runtime harness accepts
`RDEV_TEST_CLI_BINARY` to verify an explicitly built CLI artifact. Committed
artifact validation and logs follow below.

## Committed shared frontend verification: 08ff4fb

Implementation `08ff4fb` was pushed to `origin/main`.

- [Before/after fallback reproduction](evidence/phase5/2026-09-09/fallback-regression-08ff4fb.log): the old CLI returned a local secret-list result despite a missing broker socket; the corrected CLI rejects it without output.
- [Full check and real regressions](evidence/phase5/2026-09-09/validation-check-08ff4fb.log): check, three actual frontend/history/mutation runs, three retry-cancellation runs and six real lease cycles over 65 seconds passed on the final production implementation before commit.
- [Changed-package race](evidence/phase5/2026-09-09/validation-race-08ff4fb.log): CLI, MCP and broker packages passed.
- [Actual daemon and CLI race](evidence/phase5/2026-09-09/validation-remote-race-08ff4fb.log): actual CLI/MCP status/list, eight other-project sockets, default/host denials and direct-transport/local-registry fallback traps passed in 11.41 seconds. CLI SHA-256: `78e0f1667f21ddd67ba6f542774893a17e74ab757dc829bc82623205ffb57ce3`; daemon is the previously verified ingress race artifact.
- [Committed artifact verification](evidence/phase5/2026-09-09/committed-runtime-08ff4fb.log): `make check remote-frontends remote-events remote-mutation stress-broker smoke-rdevd remote-phase5-runtime` passed. Linux systemd installation, enable/start/reload and SIGKILL recovery passed with PID `958110 -> 958172`. Remote daemon SHA-256: `8ecdcacab1e3b433156dca45c517bc0b9b4d4e491d77690070b5ffb2167b3acc`.

Shared sync, secret and session administration remain unimplemented and explicitly
unavailable through this frontend. Full pool lifecycle/eviction projection and
independent external review remain open. The preceding ingress QoS failure and
isolated reruns remain part of the evidence; no wider contention guarantee is
inferred from this frontend correction.
