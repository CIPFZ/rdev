# Phase7 acceptance

Status: implementation complete; final independent review and release validation
in progress. Started from clean `042a952`, equal to `origin/main` after fetch.
Phase5 `29b5f08` and Phase6 `044f395` remain regression references.

The first operation allowlist is durable `job_start`. HostRun success means an
authenticated terminal job result with exit code zero, not submission success.
Inventory references the existing trusted global host configuration, retaining
one authority for connection settings. CLI and MCP use rdevd's shared models and
execution; neither has a private Fleet SSH fallback.

| Requirement | Implementation and acceptance boundary |
|---|---|
| P7-01 | Versioned trusted inventory, random nonreusable HostIDs, aliases and descriptive labels; administrator import/update with revision CAS, full-document validation and durable retirement tombstones. Old host configuration remains unchanged. |
| P7-02 | Deterministic HostID sorting/deduplication, permission-filtered selectors, durable preview and immutable digest. Empty/zero-match/invalid selectors reject. All execution requires approval; preview marks mutation, explicit all and >20 targets. Fixed alias and canonical configured identity prevent target substitution. |
| P7-03 | Private atomic Fleet/HostRun records, independent operation IDs, mutation ledger and durable job identity. Recovery queries original results; completed mutations never replay; uncertain results remain ambiguous. Frontend disconnect does not cancel the plan. |
| P7-04 | Whole-job max_parallel, all_at_once/waves/canary, durable wave deadline and pause/resume. Shared scheduler still controls every request; additional lifetime reservations and owner/plan rotation prevent one plan monopolizing detached capacity. Canary requires every canary success. |
| P7-05 | Strictly-greater failure thresholds over terminal attempted outcomes. Failed/unreachable/ambiguous count; skipped/canceled do not. Pause/cancel_remaining stop new admission; cancellation interrupts queued starts and leaves submitted jobs alive under existing individual job controls. Ambiguous reservations remain charged. |
| P7-06 | Bounded 32-host pages, owner/project isolation, consistent CLI 0/1/2 and MCP structured results. Explicit retry creates an approved child plan, new operation IDs and attempt+1 for only the chosen eligible subset; concurrent retry reservations are atomic. |
| P7-07 | Fleet capability plus exact HostID operation rights, single-policy-snapshot checks, plan/policy/expiry-bound consumed approvals. Private execution payloads are separate from digest-only audit; bounded records/bytes/retention preserve active and ambiguous recovery evidence. Support/compat and JSON schemas expose the supported workflow. |

| Fleet Gate | Evidence |
|---|---|
| Preview equals actual snapshot | Deterministic inventory/selector tests; concurrent policy/config updates; real CLI/SDK snapshot, deduplication and target-substitution rejection. |
| Failed canary stops waves | Deterministic failed/ambiguous canaries, resume cannot waive success; real SSH failed canary leaves later counter absent. |
| 100-host partial failure/restart/cancel | Actually executed deterministic 100-logical-host dispatcher tests, private state reload, partial failures, cancellation, concurrency and once-only operation counters. Real process SIGKILL and restart tested separately. |
| Retry only selected failures | Mock counters plus three real isolated agent targets: counters x/xx/x after selecting only the failed target; repeated execute and recovery do not add effects. |
| Empty/large cannot silently execute | Strict selector/size validation and mandatory exact-plan approval, including all and 100-target plans. |

Independent review covers identity/authorization/approval, recovery/retry,
scheduling/resources/cancellation and frontends/compatibility/documentation.
Confirmed defects are fixed with meaningful regressions and reviewed by someone
other than the fix author. The final source binding and executed commands will
be recorded in `docs/evidence/phase7/final-validation.json` after final gates.

The real runtime harness uses one authorized SSH machine with three independent
agent state namespaces and isolated business directories. This is real SSH and
real side-effect evidence, not three physical machines or 100 real hosts. The
100-host evidence is deterministic simulation. macOS runtime remains unverified
and explicitly deferred; cross-compilation is recorded separately. Full real
scale, 100-host/20-client/24-hour soak, signed publication and production
certification remain Phase8. No release or deployment is performed here.

Configured identity binds destination, port, namespace and session data. OpenSSH
configuration, DNS, ProxyCommand and PATH wrappers remain the existing trusted
administrator environment; Fleet does not attest physical machine identity or
detect changes to that external environment. Operational contracts and exact
limits are maintained in [rdevd operations](rdevd-operations.md#fleet-inventory-and-durable-plans).
