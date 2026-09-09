# Phase5 runtime evidence

Current status and remaining requirements are maintained only in
[phase5-acceptance.md](phase5-acceptance.md). This index records what each
implementation actually tested. It does not make a Phase5 Complete claim.

## Reproduce

Use Go 1.25.0, local OpenSSH/rsync/Python 3 and the authorized Linux SSH target:

```sh
make GO=/absolute/path/to/go check
make GO=/absolute/path/to/go RDEV_REMOTE_SSH=service-deploy \
  RDEV_SSH_CONFIG=/private/ssh/config remote-sync-preview
```

Replace `remote-sync-preview` with the target below. Remote fixtures use generated
namespaces and clean up their processes/state. Linux lifecycle tests additionally
require a systemd user manager. The final gate also runs repository race,
`stress-broker`, `smoke-rdevd` and `remote-phase5-runtime`.

Test overrides are optional:

| Variable | Meaning |
|---|---|
| `RDEV_AUDIT_SOAK_SECONDS` | 120–3600 active seconds in whole minutes; default 600 |
| `RDEV_PREDECESSOR_COMMIT` | Audit downgrade/re-upgrade predecessor; default `08ff4fb` |
| `RDEV_JOB_PREDECESSOR_COMMITS` | Space-separated daemon/agent upgrade predecessors; defaults `e3ac9c2 c5707ee` |

Runtime logs named `committed-*` identify the source built; `validation-*` and
other race logs may precede the commit. “Race” can cover a package subset: use the
command inside the log for its exact scope. “Process race” instruments the actual
daemon/frontends, and where stated the remote agent, rather than only the test
runner. Artifact SHA-256 values are in the run logs or the
[detailed historical record](https://github.com/CIPFZ/rdev/blob/3d50ae4a564593cb327cf4e0f06037589b9a9f7b/docs/phase5-runtime-evidence.md). The index does not relabel earlier
failures or pre-commit runs as successful committed-source evidence.

## Validation index

| Commit / behavior | Real runtime result | Reproduce with `make` | Runtime/check | Race | Process race |
|---|---|---|---|---|---|
| <a id="020230b"></a>`020230b` — Authentication and Linux lifecycle | Real provision/rotation, UID denial, duplicate start, crash recovery and systemd lifecycle. | `remote-phase5-runtime` | [log](evidence/phase5/2026-09-08/committed-check-020230b.log) | [log](evidence/phase5/2026-09-08/full-race-020230b.log) | — |
| <a id="455ff16"></a>`455ff16` — Audit owner identity | Exact owner fingerprints; colliding display names stay isolated through restart. | `remote-phase5-runtime` | [log](evidence/phase5/2026-09-08/committed-check-455ff16.log) | [log](evidence/phase5/2026-09-08/broker-race-455ff16.log) | — |
| <a id="b5e9422"></a>`b5e9422` — Shared session and principals | 3 × 500 calls from 20 processes; one SSH child, one agent, 20 principals. | `remote-session-benchmark` | [log](evidence/phase5/2026-09-08/committed-check-b5e9422.log) | [log](evidence/phase5/2026-09-08/committed-race-b5e9422.log) | — |
| <a id="2a61674"></a>`2a61674` — Cancellation and leases | Retry/SIGKILL isolation; six lease cycles end with no SSH/agent children. | `remote-lifecycle` | [log](evidence/phase5/2026-09-08/committed-check-2a61674.log) | [log](evidence/phase5/2026-09-08/committed-race-2a61674.log) | [log](evidence/phase5/2026-09-08/remote-race-2a61674.log) |
| <a id="669f18f"></a>`669f18f` — Scoped policy | Exact-host grants, stable queued decisions, durable grant/revoke and storage rejection. | `remote-policy` | [log](evidence/phase5/2026-09-08/committed-check-669f18f.log) | [log](evidence/phase5/2026-09-08/committed-race-669f18f.log) | [log](evidence/phase5/2026-09-08/remote-race-669f18f.log) |
| <a id="915fb5e"></a>`915fb5e` — Wire approval | Mandatory exact-request/owner/host/policy binding; expiry, replay and restart negatives. | `remote-approval` | [log](evidence/phase5/2026-09-08/committed-check-915fb5e.log) | [log](evidence/phase5/2026-09-08/committed-race-915fb5e.log) | [log](evidence/phase5/2026-09-08/remote-race-915fb5e.log) |
| <a id="6f62f60"></a>`6f62f60` — Fair queues and bulk lanes | 20-process weight reversal, queue/SIGKILL isolation, bulk TTL; control p95 1.27–1.49× baseline. | `remote-qos` | [log](evidence/phase5/2026-09-08/committed-check-6f62f60.log) | [log](evidence/phase5/2026-09-08/committed-race-6f62f60.log) | [log](evidence/phase5/2026-09-08/remote-race-6f62f60.log) |
| <a id="e3ac9c2"></a>`e3ac9c2` — Detached jobs | Owned jobs survive SIGKILL, unavailable SSH, reload and remote-delete/local-save failure. | `remote-jobs` | [log](evidence/phase5/2026-09-08/committed-check-e3ac9c2.log) | [log](evidence/phase5/2026-09-08/committed-race-e3ac9c2.log) | [log](evidence/phase5/2026-09-08/remote-race-e3ac9c2.log) |
| <a id="bd03436"></a>`bd03436` — Shared waits | 20 processes share one observation; reconnect/TERM preserve detached jobs and output. | `remote-wait` | [log](evidence/phase5/2026-09-08/committed-check-bd03436.log) | [log](evidence/phase5/2026-09-08/committed-race-bd03436.log) | [log](evidence/phase5/2026-09-08/remote-race-bd03436.log) |
| <a id="20b4615"></a>`20b4615` — Replay digest | State dry-run and capability-refresh substitutions reject without mutation. | `remote-replay-digest` | [log](evidence/phase5/2026-09-08/committed-check-20b4615.log) | [log](evidence/phase5/2026-09-08/committed-race-20b4615.log) | — |
| <a id="a96b359"></a>`a96b359` — Durable mutation intents | Pre-ACK job/append crashes, durable tombstones, CLI/MCP outcomes and bounded shutdown. | `remote-mutation` | [log](evidence/phase5/2026-09-09/committed-check-a96b359.log) | [log](evidence/phase5/2026-09-09/committed-race-a96b359.log) | [log](evidence/phase5/2026-09-09/remote-race-a96b359.log) |
| <a id="a3ccb8d"></a>`a3ccb8d` — Concurrent recovery | 20 independent outcome resolvers; 32-caller race regression preserves one durable result. | `remote-mutation` | [log](evidence/phase5/2026-09-09/committed-check-a3ccb8d.log) | [log](evidence/phase5/2026-09-09/committed-race-a3ccb8d.log) | [log](evidence/phase5/2026-09-09/remote-race-a3ccb8d.log) |
| <a id="371fa30"></a>`371fa30` — Job event history | Zero-subscriber terminal history, durable cursors, CLI/MCP replay, project isolation and storage repair. | `remote-events` | [log](evidence/phase5/2026-09-09/committed-runtime-371fa30.log) | [log](evidence/phase5/2026-09-09/validation-race-371fa30.log) | [log](evidence/phase5/2026-09-09/validation-remote-race-371fa30.log) |
| <a id="7026bdf"></a>`7026bdf` — Frontend ingress | Real owner/anonymous connection limits, bounded JSON, slow-reader and cancellation pressure. | `remote-ingress` | [log](evidence/phase5/2026-09-09/validation-check-7026bdf.log) | [log](evidence/phase5/2026-09-09/validation-race-7026bdf.log) | [log](evidence/phase5/2026-09-09/validation-remote-race-7026bdf.log) |
| <a id="08ff4fb"></a>`08ff4fb` — Exclusive broker frontends | CLI/MCP route and status isolation; unsupported routes cannot fall back to private SSH. | `remote-frontends` | [log](evidence/phase5/2026-09-09/committed-runtime-08ff4fb.log) | [log](evidence/phase5/2026-09-09/validation-race-08ff4fb.log) | [log](evidence/phase5/2026-09-09/validation-remote-race-08ff4fb.log) |
| <a id="41cddb4"></a>`41cddb4` — Audit continuity and soak | 600 active seconds, 603079 calls, 21 rotations, five SIGKILL recoveries; zero reported drops/errors, explicit crash gaps. | `remote-audit-continuity remote-audit-upgrade remote-audit-soak` | [log](evidence/phase5/2026-09-09/committed-audit-41cddb4.log) | [log](evidence/phase5/2026-09-09/validation-race-41cddb4.log) | [log](evidence/phase5/2026-09-09/validation-remote-race-41cddb4.log) |
| <a id="a47c59a"></a>`a47c59a` — Warm pool | 100 logical aliases, 16-host cap, LRU/reload/lease/control checks and actual OpenSSH session capacity. | `remote-warm-pool remote-mux-capacity` | [log](evidence/phase5/2026-09-09/committed-warm-a47c59a.log) | [log](evidence/phase5/2026-09-09/warm-final-code-race.log) | [log](evidence/phase5/2026-09-09/warm-final-native-remote-race.log) |
| <a id="74da502"></a>`74da502` — Route and administration boundaries | Malformed/missing routes fail before dial; host-scoped grants, approvals and mutation queries survive restart. | `remote-routes` | [log](evidence/phase5/2026-09-09/committed-routes-74da502.log) | [log](evidence/phase5/2026-09-09/routing-full-race.log) | [log](evidence/phase5/2026-09-09/routing-remote-race.log) |
| <a id="a8dbe3d"></a>`a8dbe3d` — Protocol lane counters | Real CLI/MCP/SSH byte ledger matches all three lanes across retry, streaming, cancellation and late frames. | `remote-lane-traffic` | [log](evidence/phase5/2026-09-09/committed-lane-a8dbe3d.log) | [log](evidence/phase5/2026-09-09/lane-full-race.log) | [log](evidence/phase5/2026-09-09/lane-remote-race.log) |
| <a id="8af4fb7"></a>`8af4fb7` — Request/audit correlation | Decision, approval, admission, outcome and query traces preserve exact-project identity across SIGKILL. | `remote-audit-routes` | [log](evidence/phase5/2026-09-09/committed-audit-routes-8af4fb7.log) | [log](evidence/phase5/2026-09-09/audit-routes-full-race.log) | [log](evidence/phase5/2026-09-09/audit-routes-remote-race.log) |
| <a id="0be7127"></a>`0be7127` — Connection diagnostics | Actual probe/install/handshake failures and retry counters remain scoped to the initiating project. | `remote-connection-diagnostics` | [log](evidence/phase5/2026-09-09/committed-diagnostics-0be7127.log) | [log](evidence/phase5/2026-09-09/diagnostics-full-race.log) | [log](evidence/phase5/2026-09-09/diagnostics-remote-race.log) |
| <a id="c5707ee"></a>`c5707ee` — Principal-owned secrets | Set/list/delete/use, version approval, durable mutation, historical output redaction and target retirement. | `remote-secrets` | [log](evidence/phase5/2026-09-09/committed-secrets-c5707ee.log) | [log](evidence/phase5/2026-09-09/secrets-full-race.log) | [log](evidence/phase5/2026-09-09/secrets-final-remote-race.log) |
| <a id="21996d4"></a>`21996d4` — Remote secret import | Dual read/import grants, exact source/value approval, 64 KiB cap, cancellation, accounting and crash recovery. | `remote-secret-import` | [log](evidence/phase5/2026-09-09/committed-secret-import-21996d4.log) | [log](evidence/phase5/2026-09-09/secret-import-full-race.log) | [log](evidence/phase5/2026-09-09/secret-import-remote-race.log) |
| <a id="1481edc"></a>`1481edc` — Populated secret QoS | 512 retained versions; control p95 1.172–1.403× baseline; fairness/redaction/TTL survive SIGKILL. | `remote-secret-qos` | [log](evidence/phase5/2026-09-09/committed-secret-load-1481edc.log) | [log](evidence/phase5/2026-09-09/secret-load-full-race.log) | [log](evidence/phase5/2026-09-09/secret-load-remote-race-40s.log) |
| <a id="4bfdf06"></a>`4bfdf06` — Reserved Fleet denial | Exact-project capability/wire/approval substitution denied before SSH, both sides of SIGKILL. | `remote-fleet-boundary` | [log](evidence/phase5/2026-09-09/committed-fleet-boundary-4bfdf06.log) | — | [log](evidence/phase5/2026-09-09/fleet-boundary-remote-race.log) |
| <a id="4ded2a4"></a>`4ded2a4` — Actual daemon/agent upgrades | 12 upgrades from two old versions via TERM/SIGKILL; original supervisor identity, owners and logs preserved. | `remote-job-upgrade` | [log](evidence/phase5/2026-09-09/committed-job-upgrade-4ded2a4.log) | [log](evidence/phase5/2026-09-09/job-upgrade-full-race.log) | [log](evidence/phase5/2026-09-09/job-upgrade-actual-daemon-agent-race.log) |
| <a id="6d2eeb3"></a>`6d2eeb3` — Shared sync previews | Real CLI/MCP/rsync/SSH preview, delete grants, bounded output, descendant cancellation and peer preservation. | `remote-sync-preview` | [log](evidence/phase5/2026-09-09/committed-shared-sync-6d2eeb3.log) | [log](evidence/phase5/2026-09-09/shared-sync-race.log) | [log](evidence/phase5/2026-09-09/shared-sync-actual-race.log) |
| <a id="5e2d9dc"></a>`5e2d9dc` — Complete source scans | Same-size/same-mtime 5 MiB content changes detected; entry/content caps and unchanged destination verified. | `remote-sync-preview` | [log](evidence/phase5/2026-09-09/committed-sync-manifest-5e2d9dc.log) | [log](evidence/phase5/2026-09-09/sync-manifest-full-race.log) | [log](evidence/phase5/2026-09-09/sync-manifest-actual-race.log) |
| <a id="9cf6103"></a>`9cf6103` — Prepared shared sync | Real CLI/MCP + SSH retained push/pull/delete; source drift, target drift, exact approvals, pre-ACK daemon SIGKILL recovery, CLI cancellation and peer preservation; fixed deletion scopes and binary staging also tested. | `remote-sync-execution` | [log](evidence/phase5/2026-09-09/prepared-sync-9cf6103.log) | [log](evidence/phase5/2026-09-09/prepared-sync-race-9cf6103.log) | [log](evidence/phase5/2026-09-09/prepared-sync-process-race.log) |
| <a id="9097f5d"></a>`9097f5d` — Mixed workload and sync accounting | Seven measurement processes run exec, job wait/status and two 12 MiB syncs at 2 MiB/s aggregate; control p95 0.82–0.99× baseline (1.07–1.16× with actual daemon/CLI race). Separate concurrent uploader SIGKILL releases reservations and preserves peer progress/base PID. CLI/MCP resource isolation and independent SSH byte totals match; idle bulk closes. Linux check/race/stress/readiness/systemd suite passed; macOS/review remain. | `remote-mixed-qos` | [combined log](evidence/phase5/2026-09-09/mixed-qos-9097f5d.log) | Full repository race in combined log | Actual daemon and CLI/MCP race in combined log; remote agent uninstrumented |

Additional final-source checks preserve coverage not contained in those runtime
logs, including the final test refinements after broader race runs:

- [validation-check-371fa30](evidence/phase5/2026-09-09/validation-check-371fa30.log).
- [validation-check-08ff4fb](evidence/phase5/2026-09-09/validation-check-08ff4fb.log).
- [warm-final-admission-race](evidence/phase5/2026-09-09/warm-final-admission-race.log).
- [warm-final-native-overlap](evidence/phase5/2026-09-09/warm-final-native-overlap.log).
- [secrets-encoded-capacity-race](evidence/phase5/2026-09-09/secrets-encoded-capacity-race.log).
- [secret-import-final-race](evidence/phase5/2026-09-09/secret-import-final-race.log).
- [secret-import-audit-race](evidence/phase5/2026-09-09/secret-import-audit-race.log).
- [secret-load-final-race](evidence/phase5/2026-09-09/secret-load-final-race.log).
- [shared-sync-local-processes](evidence/phase5/2026-09-09/shared-sync-local-processes.log).
- [sync-manifest-core-race](evidence/phase5/2026-09-09/sync-manifest-core-race.log).
- [sync-manifest-guards](evidence/phase5/2026-09-09/sync-manifest-guards.log).

## Failure and regression evidence

These runs deliberately reproduce an old defect or record an unsuccessful
validation attempt. They are not passing acceptance evidence. The validation
index identifies the correction and successful checks.

| Evidence | Meaning / correction |
|---|---|
| [qos-audit-contention-before.txt](evidence/phase5/2026-09-08/qos-audit-contention-before.txt) | Synchronous audit append dominated contention; corrected by `6f62f60`. |
| [committed-check-qos-failure-7026bdf](evidence/phase5/2026-09-09/committed-check-qos-failure-7026bdf.log) | A loaded committed run exceeded the SLO; preserved alongside its [successful isolated rerun](evidence/phase5/2026-09-09/committed-qos-runtime-7026bdf.log). Later QoS batches retain the same 2× limit. |
| [fallback-regression-08ff4fb](evidence/phase5/2026-09-09/fallback-regression-08ff4fb.log) | Predecessor frontend fallback boundary reproduced before the shared-route correction. |
| [routing-before-final-harness](evidence/phase5/2026-09-09/routing-before-final-harness.log) | Missing handler and scoped-administration defects reproduced before `74da502`. |
| [secret-load-reproduction](evidence/phase5/2026-09-09/secret-load-reproduction.log) | A populated secret archive exposed owner starvation; corrected by `1481edc`. |
| [secret-load-remote-race](evidence/phase5/2026-09-09/secret-load-remote-race.log) | 25-second race run had only 43 admissions for the lower-weight owner (minimum 50); the 40-second run retained every threshold and passed. Normal committed runs retained the default window. |
| [job-upgrade-before-fix-regression](evidence/phase5/2026-09-09/job-upgrade-before-fix-regression.log) | Old supervisor TERM discarded logs; `4ded2a4` signals the legacy child and lets its supervisor flush. |
| [shared-sync-cancellation-before](evidence/phase5/2026-09-09/shared-sync-cancellation-before.log) | Old rsync cancellation left SSH descendants holding pipes; `6d2eeb3` cancels the process group. |
| [sync-manifest-old-daemon](evidence/phase5/2026-09-09/sync-manifest-old-daemon.log) | Old daemon missed large-file content changes with size/mtime preserved; fixed by `5e2d9dc`. |
| [sync-manifest-initial](evidence/phase5/2026-09-09/sync-manifest-initial.log) | `os.Root.OpenFile` resolved a final symlink despite O_NOFOLLOW; corrected with a pinned parent and kernel `openat` enforcement. |

## Scope and maintenance

- One hundred aliases on one endpoint do not prove one hundred independent hosts.
- The ten-minute audit soak reports checkpoint counts and possible crash-tail
  loss, not complete retention through SIGKILL or a 24-hour production soak.
- Mixed exec/job/status/sync evidence comes from `remote-mixed-qos` in `9097f5d`.
- Sync previews and source observations do not authorize immutable execution.
- These batches have implementing-agent review only; independent review and
  actual macOS launchd validation are still required.

Update the relevant row after a meaningful implementation batch. Keep the final
runtime/check log, necessary race coverage and distinct failure regressions.
Superseded setup logs and repeated progress narratives remain in Git history;
they are not copied into new documents or archives. Private keys, credentials
and raw application output must not be added to evidence.
