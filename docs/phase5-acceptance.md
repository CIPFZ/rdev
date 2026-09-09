# Phase5 acceptance

Current status: **In progress**, updated 2026-09-09. Two items are marked
Complete (one is Linux-only), thirteen have runtime evidence pending review,
and the remaining service-lifecycle item needs macOS runtime coverage. These
counts are not a completion percentage.

## Remaining work

Phase5 remains **In progress**. Status is evaluated against the sixteen original
Phase5 acceptance conditions and all nine Multi Agent Gate conditions in
[rdev-evolution-security-plan.md](rdev-evolution-security-plan.md#phase-5多-agent-共享-rdevd-与-qos).
“Runtime passed; review pending” means the named implemented behavior has real
runtime evidence and still lacks the required independent review. It is not
a new Complete label or a percentage estimate. New routes must pass those same
security and lifecycle checks when introduced.

Closeout is tracked in six deliverables:

| Deliverable | Required exit evidence | Affected requirements |
|---|---|---|
| C1: Shared routes and authority boundary | Runtime passed; review pending. [9cf6103](phase5-runtime-evidence.md#9cf6103) covers approved retained push/pull/delete, substitution rejection, cancellation and pre-ACK recovery. Existing host configuration stays administrator-owned; unsupported frontend routes fail closed | P5-03/04/05/12/13/14/16; authority, approval and mutation gates |
| C2: Mixed-load QoS | Runtime passed; review pending. [9097f5d](phase5-runtime-evidence.md#9097f5d): seven processes run exec, job wait/status and two syncs with bounded queues, progress, control p95 below 2× baseline, isolated uploader SIGKILL and idle bulk reclamation preserving the base agent | P5-06/07/08/11; quota, no-starvation and bulk gates |
| C3: Sync observability | Runtime passed; review pending. [9097f5d](phase5-runtime-evidence.md#9097f5d): independent SSH byte counts exactly match CLI/MCP sync traffic; retained/executing/released resource charges remain project-scoped. Existing pool/queue/eviction/setup diagnostics retain prior evidence | Observability gate; P5-06/08 |
| C4: macOS lifecycle | Actual launchd user install/start/reload or restart/crash recovery, readiness, private socket and peer identity on a macOS host | P5-02 platform coverage, P5-15 |
| C5: Independent review | Review the final implemented permissions, shared concurrency, approval/mutation durability and audit paths; resolve findings and retain a review record | Original user requirement across P5-01–P5-16 |
| C6: Final integrated validation | Linux suite passed on [9097f5d](phase5-runtime-evidence.md#9097f5d): `make check`, full repository race, 100-round broker stress, local/remote readiness and actual systemd/daemon lifecycle. Final closure still requires C4/C5 and any regressions their findings affect | Final Phase5 gate |

C1–C3 have runtime evidence. C4 requires a macOS test environment and C5 an
independent reviewer. C6 cannot close until those deliverables are satisfied. Existing
evidence is reused where the code and required invariant have not changed; a
new defect or affected path justifies a targeted regression. There is no blanket
requirement to repeat every historical runtime suite after documentation changes.

P5-03 / ENG-020 require one shared Connection Manager and base agent session,
not an online host-configuration API. The earlier C1 session-administration
expansion was not an original Phase5 acceptance condition. Shared clients use
explicit request cwd/env; administrators load the private host registry at daemon
startup and restart for host changes. Shared host/session editing and remaining
frontend compatibility belong to Phase6 scope decisions. They remain unsupported,
with no fallback around broker policy; no existing safety gate is waived.

The original plan assigns automated full N/N-1 migration/rollback coverage to
P8-06 and 100-host/24-hour production certification to P8-05. These remain
tracked follow-up work, while the actual broker/agent upgrade and crash guarantees
required by Phase5 remain mandatory and have their own evidence below.
Declarative secret delegation, full job-event push streaming, automatic safe
archive/operation-ID retirement and exhaustive retention/power-loss matrices
remain follow-up limitations; they do not create additional Phase5 features.
Existing ownership, bounded storage, fail-closed behavior, durable outcomes and
the explicitly requested event-history/rotation/recovery evidence are still
required. A discovered defect violating those invariants must be fixed.

## Acceptance by requirement

The original requirements remain in the [evolution plan](rdev-evolution-security-plan.md).
Evidence links lead to the [runtime index](phase5-runtime-evidence.md), which
records commits, commands, actual results and the scope of each run.

| Item | Requirement | Status | Runtime evidence | Remaining work |
|---|---|---|---|---|
| P5-01 | Version negotiation | Complete | [020230b](phase5-runtime-evidence.md#020230b): Incompatible peers reject; pipelined hello/request regression passes. | C5; release CI compatibility is follow-up. |
| P5-02 | Socket, singleton and peer identity | Complete (Linux runtime) | [020230b](phase5-runtime-evidence.md#020230b): Real duplicate launch, SIGKILL recovery and foreign UID denial. | C4 macOS, C5 review. |
| P5-03 | Shared connection and secret ownership | Runtime passed; review pending | [b5e9422](phase5-runtime-evidence.md#b5e9422), [08ff4fb](phase5-runtime-evidence.md#08ff4fb), [c5707ee](phase5-runtime-evidence.md#c5707ee): 20-process/one-agent benchmark; shared secret and frontend routes verified. Shared sync: [9cf6103](phase5-runtime-evidence.md#9cf6103). | C5. |
| P5-04 | Client/project scope | Runtime passed; review pending | [020230b](phase5-runtime-evidence.md#020230b), [b5e9422](phase5-runtime-evidence.md#b5e9422), [c5707ee](phase5-runtime-evidence.md#c5707ee), [e3ac9c2](phase5-runtime-evidence.md#e3ac9c2): Real provisioning/rotation and exact-project request, secret and job isolation. | C5. |
| P5-05 | Cancellation isolation | Runtime passed; review pending | [2a61674](phase5-runtime-evidence.md#2a61674), [bd03436](phase5-runtime-evidence.md#bd03436), [6d2eeb3](phase5-runtime-evidence.md#6d2eeb3): Real retry/SIGKILL plus shared wait and rsync-preview cancellation. Shared sync CLI cancellation: [9cf6103](phase5-runtime-evidence.md#9cf6103). Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | C5. |
| P5-06 | Host/client quotas | Runtime passed; review pending | [7026bdf](phase5-runtime-evidence.md#7026bdf), [a47c59a](phase5-runtime-evidence.md#a47c59a), [5e2d9dc](phase5-runtime-evidence.md#5e2d9dc): Remote overload/ingress bounds; warm-pool and preview worker reservations. Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | C5. |
| P5-07 | Weighted fairness | Runtime passed; review pending | [6f62f60](phase5-runtime-evidence.md#6f62f60): Continuously backlogged owners; live 3:1 to 1:3 weight reversal. Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | C5. |
| P5-08 | Control/exec/bulk lanes | Runtime passed; review pending | [1481edc](phase5-runtime-evidence.md#1481edc): 512-secret archive SLO at 1.172–1.403× baseline; bulk TTL verified. Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | C5. |
| P5-09 | Shared waits and history | Runtime passed; review pending | [bd03436](phase5-runtime-evidence.md#bd03436), [371fa30](phase5-runtime-evidence.md#371fa30): 20-process coalescing, zero-subscriber completion and durable CLI/MCP replay. | C5. |
| P5-10 | Detached job recovery/upgrade | Runtime passed; review pending | [a96b359](phase5-runtime-evidence.md#a96b359), [4ded2a4](phase5-runtime-evidence.md#4ded2a4): Acknowledged/pre-ACK recovery and 12 actual old/new daemon-agent upgrades. | C5. |
| P5-11 | Client lease and last-client grace | Runtime passed; review pending | [2a61674](phase5-runtime-evidence.md#2a61674), [bd03436](phase5-runtime-evidence.md#bd03436): Six real exit/SIGKILL cycles; observation lease preserved and released on shutdown. Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | C5. |
| P5-12 | Principal/capability/policy | Runtime passed; review pending | [669f18f](phase5-runtime-evidence.md#669f18f), [74da502](phase5-runtime-evidence.md#74da502), [c5707ee](phase5-runtime-evidence.md#c5707ee), [4bfdf06](phase5-runtime-evidence.md#4bfdf06): Pre-queue stable decision; exact host/project grants, secret use and Fleet denial. | C5. |
| P5-13 | Digest-bound approval | Runtime passed; review pending | [915fb5e](phase5-runtime-evidence.md#915fb5e), [c5707ee](phase5-runtime-evidence.md#c5707ee), [21996d4](phase5-runtime-evidence.md#21996d4): Wire/secret owner/host/request/policy substitution, expiry/replay/restart negatives. Retained sync plan approvals: [9cf6103](phase5-runtime-evidence.md#9cf6103). | C5. |
| P5-14 | Private audit, rotation and query | Runtime passed; review pending | [41cddb4](phase5-runtime-evidence.md#41cddb4), [8af4fb7](phase5-runtime-evidence.md#8af4fb7): 600-second rotation/crash soak; exact-owner request/approval/outcome correlation. Sync approval/outcome correlation: [9cf6103](phase5-runtime-evidence.md#9cf6103). | C5. |
| P5-15 | User service lifecycle | In progress | [020230b](phase5-runtime-evidence.md#020230b): Actual Linux systemd install/enable/start/reload/SIGKILL/stop/start. | C4, C5. |
| P5-16 | Validated reload and bounded drain | Runtime passed; review pending | [020230b](phase5-runtime-evidence.md#020230b), [a96b359](phase5-runtime-evidence.md#a96b359): Invalid reload preserves config; durable mutation and stalled-response shutdown. Pre-ACK sync SIGKILL recovery: [9cf6103](phase5-runtime-evidence.md#9cf6103). | C5. |

No new Complete label follows from a green test run alone. Every original
requirement, the nine gates below, independent review and final integrated
validation must be satisfied before Phase5 is marked Complete.

## Multi Agent Gate

| Gate | Evidence | Status / remaining work |
|---|---|---|
| 20 independent local clients, one host, one base transport/agent | `TestRemoteBrokerProcesses`: 20 independent PIDs, 3 x 500 real SSH calls, one remote agent PID, one daemon SSH child, 20 remote principal IDs | Runtime passed; independent review pending |
| Per-client quota and cancellation isolation | Real retry/SIGKILL isolation plus sustained per-owner remote file-I/O overload, bounded queue rejection and cancellation cleanup; bounded socket/JSON ingress and slow-reader cancellation targeted runtime | File-I/O, ingress, preview and [mixed sync cancellation](phase5-runtime-evidence.md#9097f5d) runtime passed; C5 remains |
| No starvation under long exec/job wait/status/sync | [9097f5d](phase5-runtime-evidence.md#9097f5d): simultaneous long exec, job wait/status and two retained syncs; every owner progresses, control p95 below 2× baseline | Runtime passed; C5 remains |
| Bulk transport closes after idle TTL | Real dedicated bulk/base SSH processes during remote I/O, active bulk preservation, one-second idle TTL plus five-second sweep, same base agent retained | Runtime passed on Linux, including mixed sync in [9097f5d](phase5-runtime-evidence.md#9097f5d); C5 remains |
| Crash/restart never duplicates mutation | Private persist-before-dispatch intent registry; real pre-ACK remote job and append SIGKILL, stable-ID conflict/replay refusal, fresh-agent job tombstones and original-owner recovery | Wire job/write, secret and [shared sync](phase5-runtime-evidence.md#9cf6103) crash windows have runtime evidence; C5 independent review remains |
| status/doctor exposes pool, quotas, queue, lane bytes and eviction reasons | Authenticated owner-only scheduler snapshot, queue wait and file payload byte counters; separately granted audit sink health | Owner scheduler/ingress/waits, administrative pool/evictions, exact three-lane protocol bytes and fixed setup/failure diagnostics have real CLI/MCP evidence; [9097f5d](phase5-runtime-evidence.md#9097f5d) adds exact sync bytes and resource isolation; C5 remains |
| Unauthorized host/secret/job/Fleet use denied | Actual daemon authentication and default-deny process tests | Exact-host/project, job, secret/import/use and reserved Fleet pre/post-SIGKILL negatives pass for the required shared routes; C5 remains |
| Destructive approval binds exact target snapshot and digest | Real SSH mandatory write approval, owner/host/parameter substitution, expiry/replay/policy-change/restart negatives and audit correlation | Wire, secret and [retained shared sync](phase5-runtime-evidence.md#9cf6103) runtime passed; C5 remains |
| Reload/upgrade/crash preserve detached background jobs | Actual daemon reload/SIGKILL/service restart lifecycle | Acknowledged/pre-ACK jobs survive SIGKILL/SSH outage; waits survive reload and cancel on shutdown; twelve real upgrades on `4ded2a4` preserve old supervisor identity/output; C5 review remains, with full automated rollback/schema coverage assigned to P8-06 |

## Updating this record

Update the status/evidence cells and remaining deliverables in place. Add one
concise validation-index entry for new evidence; do not append chronological
progress reports here. Security findings that violate an original invariant
must be fixed. Broader follow-up work stays in its original phase.
