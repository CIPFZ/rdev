# Phase5 acceptance

Current status: **Complete for the user-approved Phase5 scope**, verified
2026-09-09. All sixteen original requirements and nine Multi Agent Gates have
implementation, Linux runtime evidence and independent review. Final integrated
validation passed on `29b5f08`; see the [review and final evidence](phase5-runtime-evidence.md#phase5-review).

**macOS runtime remains unverified.** The user explicitly deferred it on
2026-09-09 because no environment is available. Actual launchd lifecycle,
readiness, socket and peer-identity testing remains platform follow-up and is
not a blocker for this closeout. Cross-compilation is not a runtime pass.

## Scope and follow-up

P5-03 / ENG-020 require one shared Connection Manager and base agent session.
Shared clients use explicit request cwd/env; administrators load the private
host registry at startup and restart for host changes. Shared host/session editing
and remaining frontend compatibility stay in Phase6; unsupported routes fail
closed without bypassing broker policy.

Full automated N/N-1 migration/rollback coverage remains P8-06, and 100-host/24-hour
production certification remains P8-05. Declarative secret delegation, full job
event push streaming, automatic safe archive/operation-ID retirement and exhaustive
retention/power-loss matrices remain follow-up work. These limits do not waive
the ownership, bounded storage, durable outcome and crash guarantees verified here.

## Acceptance by requirement

The requirements remain in the [evolution plan](rdev-evolution-security-plan.md).
The [runtime index](phase5-runtime-evidence.md) identifies source commits, commands
and results. Its final independent-review entry applies to every row below and
records the corrections to handshake, audit, sync, host fairness and shared waits.

| Item | Requirement | Status | Runtime evidence | Follow-up |
|---|---|---|---|---|
| P5-01 | Version negotiation | Complete | [020230b](phase5-runtime-evidence.md#020230b): Incompatible peers reject; pipelined hello/request regression passes.  Final review also verifies client-side reply versions and cancellable hello. | — |
| P5-02 | Socket, singleton and peer identity | Complete (Linux runtime) | [020230b](phase5-runtime-evidence.md#020230b): Real duplicate launch, SIGKILL recovery and foreign UID denial. | macOS runtime deferred by user. |
| P5-03 | Shared connection and secret ownership | Complete | [b5e9422](phase5-runtime-evidence.md#b5e9422), [08ff4fb](phase5-runtime-evidence.md#08ff4fb), [c5707ee](phase5-runtime-evidence.md#c5707ee): 20-process/one-agent benchmark; shared secret and frontend routes verified. Shared sync: [9cf6103](phase5-runtime-evidence.md#9cf6103). | — |
| P5-04 | Client/project scope | Complete | [020230b](phase5-runtime-evidence.md#020230b), [b5e9422](phase5-runtime-evidence.md#b5e9422), [c5707ee](phase5-runtime-evidence.md#c5707ee), [e3ac9c2](phase5-runtime-evidence.md#e3ac9c2): Real provisioning/rotation and exact-project request, secret and job isolation. | — |
| P5-05 | Cancellation isolation | Complete | [2a61674](phase5-runtime-evidence.md#2a61674), [bd03436](phase5-runtime-evidence.md#bd03436), [6d2eeb3](phase5-runtime-evidence.md#6d2eeb3): Real retry/SIGKILL plus shared wait and rsync-preview cancellation. Shared sync CLI cancellation: [9cf6103](phase5-runtime-evidence.md#9cf6103). Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | — |
| P5-06 | Host/client quotas | Complete | [7026bdf](phase5-runtime-evidence.md#7026bdf), [a47c59a](phase5-runtime-evidence.md#a47c59a), [5e2d9dc](phase5-runtime-evidence.md#5e2d9dc): Remote overload/ingress bounds; warm-pool and preview worker reservations. Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | — |
| P5-07 | Weighted fairness | Complete | [6f62f60](phase5-runtime-evidence.md#6f62f60): Continuously backlogged owners; live 3:1 to 1:3 weight reversal. Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | — |
| P5-08 | Control/exec/bulk lanes | Complete | [1481edc](phase5-runtime-evidence.md#1481edc): 512-secret archive SLO at 1.172–1.403× baseline; bulk TTL verified. Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | — |
| P5-09 | Shared waits and history | Complete | [bd03436](phase5-runtime-evidence.md#bd03436), [371fa30](phase5-runtime-evidence.md#371fa30): 20-process coalescing, zero-subscriber completion and durable CLI/MCP replay. | — |
| P5-10 | Detached job recovery/upgrade | Complete | [a96b359](phase5-runtime-evidence.md#a96b359), [4ded2a4](phase5-runtime-evidence.md#4ded2a4): Acknowledged/pre-ACK recovery and 12 actual old/new daemon-agent upgrades. | — |
| P5-11 | Client lease and last-client grace | Complete | [2a61674](phase5-runtime-evidence.md#2a61674), [bd03436](phase5-runtime-evidence.md#bd03436): Six real exit/SIGKILL cycles; observation lease preserved and released on shutdown. Mixed workload: [9097f5d](phase5-runtime-evidence.md#9097f5d). | — |
| P5-12 | Principal/capability/policy | Complete | [669f18f](phase5-runtime-evidence.md#669f18f), [74da502](phase5-runtime-evidence.md#74da502), [c5707ee](phase5-runtime-evidence.md#c5707ee), [4bfdf06](phase5-runtime-evidence.md#4bfdf06): Pre-queue stable decision; exact host/project grants, secret use and Fleet denial. | — |
| P5-13 | Digest-bound approval | Complete | [915fb5e](phase5-runtime-evidence.md#915fb5e), [c5707ee](phase5-runtime-evidence.md#c5707ee), [21996d4](phase5-runtime-evidence.md#21996d4): Wire/secret owner/host/request/policy substitution, expiry/replay/restart negatives. Retained sync plan approvals: [9cf6103](phase5-runtime-evidence.md#9cf6103). | — |
| P5-14 | Private audit, rotation and query | Complete | [41cddb4](phase5-runtime-evidence.md#41cddb4), [8af4fb7](phase5-runtime-evidence.md#8af4fb7): 600-second rotation/crash soak; exact-owner request/approval/outcome correlation. Sync approval/outcome correlation: [9cf6103](phase5-runtime-evidence.md#9cf6103). | — |
| P5-15 | User service lifecycle | Complete (Linux runtime) | [020230b](phase5-runtime-evidence.md#020230b): Actual Linux systemd install/enable/start/reload/SIGKILL/stop/start. | macOS runtime deferred by user. |
| P5-16 | Validated reload and bounded drain | Complete | [020230b](phase5-runtime-evidence.md#020230b), [a96b359](phase5-runtime-evidence.md#a96b359): Invalid reload preserves config; durable mutation and stalled-response shutdown. Pre-ACK sync SIGKILL recovery: [9cf6103](phase5-runtime-evidence.md#9cf6103). | — |

## Multi Agent Gate

Historical runtime evidence below is supplemented by the final check/race,
100-round stress, actual service lifecycle, SSH mixed workload and instrumented
daemon/CLI regressions linked above. Shared waits retain the logical observation
while yielding host capacity between bounded polls; the final runtime verifies
cold-host progress, original supervisor survival and one durable terminal event.

| Gate | Evidence | Status |
|---|---|---|
| 20 independent local clients, one host, one base transport/agent | `TestRemoteBrokerProcesses`: 20 independent PIDs, 3 x 500 real SSH calls, one remote agent PID, one daemon SSH child, 20 remote principal IDs | Complete (Linux runtime) |
| Per-client quota and cancellation isolation | Real retry/SIGKILL isolation plus sustained per-owner remote file-I/O overload, bounded queue rejection and cancellation cleanup; bounded socket/JSON ingress and slow-reader cancellation targeted runtime | Complete (Linux runtime) |
| No starvation under long exec/job wait/status/sync | [9097f5d](phase5-runtime-evidence.md#9097f5d): simultaneous long exec, job wait/status and two retained syncs; every owner progresses, control p95 below 2× baseline | Complete (Linux runtime) |
| Bulk transport closes after idle TTL | Real dedicated bulk/base SSH processes during remote I/O, active bulk preservation, one-second idle TTL plus five-second sweep, same base agent retained | Complete (Linux runtime) |
| Crash/restart never duplicates mutation | Private persist-before-dispatch intent registry; real pre-ACK remote job and append SIGKILL, stable-ID conflict/replay refusal, fresh-agent job tombstones and original-owner recovery | Complete (Linux runtime) |
| status/doctor exposes pool, quotas, queue, lane bytes and eviction reasons | Authenticated owner-only scheduler snapshot, queue wait and file payload byte counters; separately granted audit sink health | Complete (Linux runtime) |
| Unauthorized host/secret/job/Fleet use denied | Actual daemon authentication and default-deny process tests | Complete (Linux runtime) |
| Destructive approval binds exact target snapshot and digest | Real SSH mandatory write approval, owner/host/parameter substitution, expiry/replay/policy-change/restart negatives and audit correlation | Complete (Linux runtime) |
| Reload/upgrade/crash preserve detached background jobs | Actual daemon reload/SIGKILL/service restart lifecycle | Complete (Linux runtime) |

## Updating this record

Update existing rows when implementation or scope changes. Keep reproducible
evidence in the runtime index, and retain the explicit macOS runtime deferral.
Do not append chronological progress reports or duplicate status documents.
