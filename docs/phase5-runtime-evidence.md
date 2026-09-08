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
