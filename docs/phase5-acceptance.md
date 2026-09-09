# Phase5 acceptance

Current status: **In progress**, updated 2026-09-09. Two items are marked
Complete (one is Linux-only), six have runtime evidence pending review, and
eight retain implementation/integration/platform gaps. These counts are not
a completion percentage.

## Remaining work

Phase5 remains **In progress**. Status is evaluated against the sixteen original
Phase5 acceptance conditions and all nine Multi Agent Gate conditions in
[rdev-evolution-security-plan.md](rdev-evolution-security-plan.md#phase-5多-agent-共享-rdevd-与-qos).
“Runtime passed; review pending” means the named implemented behavior has real
runtime evidence and still lacks the required independent review. It is not
a new Complete label or a percentage estimate. New routes must pass those same
security and lifecycle checks when introduced.

The remaining work is organized into six deliverables:

| Deliverable | Required exit evidence | Affected requirements |
|---|---|---|
| C1: Finish shared routes | Shared sync implemented and tested in [9cf6103](phase5-runtime-evidence.md#9cf6103): retained approved push/pull/delete, substitution rejection, cancellation and pre-ACK crash recovery. Remaining shared session administration must preserve other owners; all new routes retain these authority/approval/mutation checks | P5-03/04/05/12/13/14/16; authority, approval and mutation gates |
| C2: Mixed-load QoS | Real independent clients concurrently run long exec, job wait/status and sync; bounded owner queues, progress for every owner, cancellation isolation and control p95 at most twice the same-workload no-bulk baseline; idle bulk reclamation retains the base agent | P5-06/07/08/11; quota, no-starvation and bulk gates |
| C3: Complete sync observability | CLI/MCP status/doctor expose the remaining sync traffic and resource usage with exact owner isolation; existing pool/queue/eviction/setup diagnostics remain correct during C2 | Observability gate; P5-06/08 |
| C4: macOS lifecycle | Actual launchd user install/start/reload or restart/crash recovery, readiness, private socket and peer identity on a macOS host | P5-02 platform coverage, P5-15 |
| C5: Independent review | Review the final implemented permissions, shared concurrency, approval/mutation durability and audit paths; resolve findings and retain a review record | Original user requirement across P5-01–P5-16 |
| C6: Final integrated validation | Final-source `make check`, repository race, stress, readiness/remote smoke and the targeted runtime gates above; evidence mapped to every P5 and Multi Agent criterion | Final Phase5 gate |

C1–C3 are implementation/integration work, C4 requires a macOS test environment,
and C5 requires an independent reviewer. C6 follows those deliverables. Existing
evidence is reused where the code and required invariant have not changed; a
new defect or affected path justifies a targeted regression. There is no blanket
requirement to repeat every historical runtime suite after documentation changes.

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
| P5-01 | Version negotiation | Complete | [020230b](phase5-runtime-evidence.md#020230b): Incompatible peers reject; pipelined hello/request regression passes. | Independent review; release CI compatibility is follow-up. |
| P5-02 | Socket, singleton and peer identity | Complete (Linux runtime) | [020230b](phase5-runtime-evidence.md#020230b): Real duplicate launch, SIGKILL recovery and foreign UID denial. | C4 macOS, C5 review. |
| P5-03 | Shared connection and secret ownership | In progress | [b5e9422](phase5-runtime-evidence.md#b5e9422), [08ff4fb](phase5-runtime-evidence.md#08ff4fb), [c5707ee](phase5-runtime-evidence.md#c5707ee): 20-process/one-agent benchmark; shared secret and frontend routes verified. Shared sync: [9cf6103](phase5-runtime-evidence.md#9cf6103). | C1, C5. |
| P5-04 | Client/project scope | Runtime passed; review pending | [020230b](phase5-runtime-evidence.md#020230b), [b5e9422](phase5-runtime-evidence.md#b5e9422), [c5707ee](phase5-runtime-evidence.md#c5707ee), [e3ac9c2](phase5-runtime-evidence.md#e3ac9c2): Real provisioning/rotation and exact-project request, secret and job isolation. | C1, C5. |
| P5-05 | Cancellation isolation | Runtime passed; review pending | [2a61674](phase5-runtime-evidence.md#2a61674), [bd03436](phase5-runtime-evidence.md#bd03436), [6d2eeb3](phase5-runtime-evidence.md#6d2eeb3): Real retry/SIGKILL plus shared wait and rsync-preview cancellation. Shared sync CLI cancellation: [9cf6103](phase5-runtime-evidence.md#9cf6103). | C1, C2, C5. |
| P5-06 | Host/client quotas | In progress | [7026bdf](phase5-runtime-evidence.md#7026bdf), [a47c59a](phase5-runtime-evidence.md#a47c59a), [5e2d9dc](phase5-runtime-evidence.md#5e2d9dc): Remote overload/ingress bounds; warm-pool and preview worker reservations. | C2, C3, C5. |
| P5-07 | Weighted fairness | In progress | [6f62f60](phase5-runtime-evidence.md#6f62f60): Continuously backlogged owners; live 3:1 to 1:3 weight reversal. | C2, C5. |
| P5-08 | Control/exec/bulk lanes | In progress | [1481edc](phase5-runtime-evidence.md#1481edc): 512-secret archive SLO at 1.172–1.403× baseline; bulk TTL verified. | C2, C3, C5. |
| P5-09 | Shared waits and history | Runtime passed; review pending | [bd03436](phase5-runtime-evidence.md#bd03436), [371fa30](phase5-runtime-evidence.md#371fa30): 20-process coalescing, zero-subscriber completion and durable CLI/MCP replay. | C5. |
| P5-10 | Detached job recovery/upgrade | Runtime passed; review pending | [a96b359](phase5-runtime-evidence.md#a96b359), [4ded2a4](phase5-runtime-evidence.md#4ded2a4): Acknowledged/pre-ACK recovery and 12 actual old/new daemon-agent upgrades. | C5. |
| P5-11 | Client lease and last-client grace | Runtime passed; review pending | [2a61674](phase5-runtime-evidence.md#2a61674), [bd03436](phase5-runtime-evidence.md#bd03436): Six real exit/SIGKILL cycles; observation lease preserved and released on shutdown. | C1, C2, C5. |
| P5-12 | Principal/capability/policy | Runtime passed; review pending | [669f18f](phase5-runtime-evidence.md#669f18f), [74da502](phase5-runtime-evidence.md#74da502), [c5707ee](phase5-runtime-evidence.md#c5707ee), [4bfdf06](phase5-runtime-evidence.md#4bfdf06): Pre-queue stable decision; exact host/project grants, secret use and Fleet denial. | C1, C5. |
| P5-13 | Digest-bound approval | In progress | [915fb5e](phase5-runtime-evidence.md#915fb5e), [c5707ee](phase5-runtime-evidence.md#c5707ee), [21996d4](phase5-runtime-evidence.md#21996d4): Wire/secret owner/host/request/policy substitution, expiry/replay/restart negatives. Retained sync plan approvals: [9cf6103](phase5-runtime-evidence.md#9cf6103). | C1, C5. |
| P5-14 | Private audit, rotation and query | In progress | [41cddb4](phase5-runtime-evidence.md#41cddb4), [8af4fb7](phase5-runtime-evidence.md#8af4fb7): 600-second rotation/crash soak; exact-owner request/approval/outcome correlation. Sync approval/outcome correlation: [9cf6103](phase5-runtime-evidence.md#9cf6103). | C1, C5. |
| P5-15 | User service lifecycle | In progress | [020230b](phase5-runtime-evidence.md#020230b): Actual Linux systemd install/enable/start/reload/SIGKILL/stop/start. | C4, C5. |
| P5-16 | Validated reload and bounded drain | In progress | [020230b](phase5-runtime-evidence.md#020230b), [a96b359](phase5-runtime-evidence.md#a96b359): Invalid reload preserves config; durable mutation and stalled-response shutdown. Pre-ACK sync SIGKILL recovery: [9cf6103](phase5-runtime-evidence.md#9cf6103). | C1, C5. |

No new Complete label follows from a green test run alone. Every original
requirement, the nine gates below, independent review and final integrated
validation must be satisfied before Phase5 is marked Complete.

## Multi Agent Gate

| Gate | Evidence | Status / remaining work |
|---|---|---|
| 20 independent local clients, one host, one base transport/agent | `TestRemoteBrokerProcesses`: 20 independent PIDs, 3 x 500 real SSH calls, one remote agent PID, one daemon SSH child, 20 remote principal IDs | Runtime passed; independent review pending |
| Per-client quota and cancellation isolation | Real retry/SIGKILL isolation plus sustained per-owner remote file-I/O overload, bounded queue rejection and cancellation cleanup; bounded socket/JSON ingress and slow-reader cancellation targeted runtime | File-I/O, ingress and preview cancellation runtime passed; C2 mixed-load closure and C5 independent review remain |
| No starvation under long exec/job wait/status/sync | Real continuously queued file-I/O and live weight reversal pass; exec/job/sync have separate runtime tests | C2 must combine these workloads; C5 remains |
| Bulk transport closes after idle TTL | Real dedicated bulk/base SSH processes during remote I/O, active bulk preservation, one-second idle TTL plus five-second sweep, same base agent retained | Runtime passed on Linux; independent review and broader host/payload matrix remain |
| Crash/restart never duplicates mutation | Private persist-before-dispatch intent registry; real pre-ACK remote job and append SIGKILL, stable-ID conflict/replay refusal, fresh-agent job tombstones and original-owner recovery | Wire job/write, secret and [shared sync](phase5-runtime-evidence.md#9cf6103) crash windows have runtime evidence; C1 remaining routes and C5 independent review remain |
| status/doctor exposes pool, quotas, queue, lane bytes and eviction reasons | Authenticated owner-only scheduler snapshot, queue wait and file payload byte counters; separately granted audit sink health | Owner scheduler/ingress/waits, administrative pool/evictions, exact three-lane protocol bytes and fixed setup/failure diagnostics have real CLI/MCP evidence; C3 sync accounting and C5 remain |
| Unauthorized host/secret/job/Fleet use denied | Actual daemon authentication and default-deny process tests | Exact-host/project, job, secret/import/use and reserved Fleet pre/post-SIGKILL negatives pass; C1 must preserve the boundary for new shared routes; C5 remains |
| Destructive approval binds exact target snapshot and digest | Real SSH mandatory write approval, owner/host/parameter substitution, expiry/replay/policy-change/restart negatives and audit correlation | Wire, secret and [retained shared sync](phase5-runtime-evidence.md#9cf6103) runtime passed; C1 remaining routes and C5 remain |
| Reload/upgrade/crash preserve detached background jobs | Actual daemon reload/SIGKILL/service restart lifecycle | Acknowledged/pre-ACK jobs survive SIGKILL/SSH outage; waits survive reload and cancel on shutdown; twelve real upgrades on `4ded2a4` preserve old supervisor identity/output; C5 review remains, with full automated rollback/schema coverage assigned to P8-06 |

## Updating this record

Update the status/evidence cells and remaining deliverables in place. Add one
concise validation-index entry for new evidence; do not append chronological
progress reports here. Security findings that violate an original invariant
must be fixed. Broader follow-up work stays in its original phase.
