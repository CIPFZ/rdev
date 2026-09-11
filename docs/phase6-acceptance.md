# Phase6 acceptance

Status: Complete — P6-01 through P6-09 accepted on 2026-09-09. Baseline
`de96704` was clean and equal to `origin/main`. Final build and validation source:
`044f3957958af0e654168da1e629d2b95749d1a9`. Its production code is unchanged from
`aca675a`; the final commit only isolates a sync test fixture. The closure commit
changes documentation/evidence only. Phase5's `29b5f08` remains the regression
reference.

The macOS arm64 controller runtime and launchd closure are recorded in
[macOS controller acceptance](macos-controller-acceptance.md). macOS remote-agent
runtime remains unverified. Linux
runtime and cross-build coverage are recorded separately. No hosted release,
deployment or repository permission change is part of this work.

| Item | Implementation and acceptance evidence |
|---|---|
| P6-01 | Existing interspersed parser now has per-command schemas, exact arity, single/repeat rules, numeric bounds and conflict checks. Daemon standard flags have duplicate/arity checks. `TestStrictCLIInputs`, `TestSingleFlags*`, `TestDaemonFlagErrors*`, `TestProvisioningFlags*`; typed process errors preserve stable codes. |
| P6-02 | Bounded stdin reader distinguishes EOF and non-EOF; partial data is discarded on error before any write dispatch. `TestStdinFailureCannotDispatchPartialWrite` and actual main-process stdin fault tests cover standalone/broker, partial/empty error, EOF and exact/over-limit inputs. |
| P6-03 | `proto/timeouts.go` supplies exec 60, wait 300, new-job wall 3600 defaults; zero selects default, negative/infinite/out-of-range rejected. Four-entry runtime test checks foreground termination, observer expiry with surviving job, wall termination and resource result fields. New job requires negotiated envelope support; old peers fail before launch. Durable job digest/replay regression preserves approval and recovery binding. |
| P6-04 | `transport.ParseDestination` reaches registry, immutable identity, final SSH and rsync arguments. IPv4/DNS/alias/user/IPv6, malformed ports and conflicts covered; `TestLocalIPv6SSHAndSync` uses an isolated real `::1` sshd, bootstrap, exec and rsync. |
| P6-05 | README, help, SECURITY and operations corrected to actual protocols, authority, redaction, cancellation, sync and connection lifecycle. Shared host/session editing and declarative delegation are discoverably unsupported with alternatives. State CLI/MCP uses existing host-wide administrator grants and approval, tested through actual processes and SDK. |
| P6-06 | Online audit found Go 1.25.0 and x/sys 0.41.0 advisories; targeted upgrade to Go 1.26.8/x/sys 0.44.0. Pinned govulncheck 1.8.0, four source/six binary scans, online module verification, manifest/SBOM/unsigned provenance and artifact binding are executable release gates. |
| P6-07 | CLI/MCP share support/discovery data. Static/build/experimental/runtime evidence, actual controls, own authorization and supplementary rights are distinct. No probe grant means no SSH or inventory lookup; profile/secret/other-owner state is omitted. |
| P6-08 | `rdev compat` / `rdev_compat` derive error registry, config shapes and protocol/state versions from actual validators/constants. Tests cover agent 2–3, broker 1, broker-agent 3, required feature refusal, legacy forward state migration and unsupported schema rejection. |
| P6-09 | CLI `sync ... -- -leading-local REMOTE` preserves the operand; exec argv including a second `--`, empty arguments, spaces and Unicode remains unchanged. Actual dual-mode CLI and official SDK sync verify resulting bytes. |

The existing interspersed parser was retained with per-command schemas: standard
`flag` stops at the first operand, while replacing all commands with a framework
would enlarge the compatibility change. The daemon retains `FlagSet` with a strict
outer check. Both reject invalid input before connection or provisioning effects.

## Compatibility changes

- Unknown/duplicate/conflicting flags, excess operands, malformed numbers and
  invalid ports fail instead of being ignored. Repeated `-env`/`-secret` require
  distinct keys; repeated exclusions remain ordered. CLI `exec`/`job start` now
  support explicit `-env` in shared and standalone modes.
- Exec omitted/zero now means 60 seconds across all entries. New jobs omitted/zero
  wall budget now means one hour. Explicit valid positive budgets retain their
  meaning. Already running old supervisors are not retroactively changed.
- New job starts require `job_resource_envelope`; earlier peers may continue
  common operations and existing-job observation/control but cannot silently
  ignore the runtime envelope. Approval binds submitted input; durable execution
  identity binds one normalized private request. Persisted old outcomes remain
  queryable and are never automatically re-executed.
- Bare IPv6 is the entire address. Ports require `[IPv6]:N`; embedded and separate
  ports conflict. Raw single-word hosts still require registered rdev aliases,
  whose addresses may be SSH config aliases.
- Shared state reports cover the entire host state root. Only separately granted
  administrators should receive them; migration/repair previews also need approval.

## Independent review

Three agents reviewed behavior, boundaries and implementation, with cross-review
of author changes. Confirmed findings addressed include parsed/raw operand drift,
default job durable-digest mismatch, typed CLI/MCP error projection, old-peer
resource-envelope omission, and support permission/callability scope drift.
`entry_review` closed the entry/release/compat findings on `aca675a` and independently
accepted the fixture-only `044f395` correction after the real four-entry pass.
`address_discovery` completed a read-only P6-01–09 coverage and documentation review
on `044f395`, with no remaining blockers. `release_compat` cross-reviewed daemon
flags and shared support/state boundaries and executed the final release gate.
`entry_review` also reconciled the final closure record with the logs, measured
results and artifact hashes. No confirmed review findings remain open.

## Final verification

All results below were actually executed. The compact
[validation record](evidence/phase6/final-validation.json) contains source commits,
reproducible commands, tool versions, measured results, artifact hashes and raw-log
hashes. Logs and unsigned build outputs remain outside the repository under
`/data/tmp/rdev-validation`.

| Gate | Final result |
|---|---|
| `make all daemon check` on `044f395` | PASS: tests, vet and byte comparison of all four embedded agents. |
| `go test -race ./... -count=1` on `044f395` | PASS across the repository. |
| Actual CLI processes and official MCP SDK, standalone and broker | All four entries PASS for timeout termination, invalid-input non-write, observer/job lifetime separation, wall limits and literal local paths with exact remote bytes. Repeated with instrumented CLI/MCP and daemon on `044f395`; no race reports. |
| Real SSH Phase5 regressions | Approval/owner substitution, audit privacy, frontend denial, job recovery, cancellation, six lease-reaping cycles, secrets, shared wait and retained sync passed. Final instrumented cancellation took 21.563/33.889/47.540 ms (wait/retry/exec); 20-process shared wait retained one observer, isolated five exits and drained in 17.732 ms. Remote agents were uninstrumented. |
| IPv6 | `TestLocalIPv6SSHAndSync` PASS on `aca675a`, using an isolated local `::1` sshd, actual bootstrap/exec/rsync and exact payload checks. Production code is unchanged in `044f395`. |
| Representative previous/current combination | Real `29b5f08` daemon + agent upgraded to current production after both SIGTERM and SIGKILL. Original supervisors/output and two-project ownership survived; each command ran once. Protocol/feature and unsupported/future schema cases also passed in the final unit/SDK suites. |
| Linux lifecycle | Local authenticated smoke and real remote daemon readiness, singleton, fail-closed startup, reload/drain and systemd install/enable/start/reload/SIGKILL recovery/stop/start PASS on `044f395`; isolated service removed by the harness. |
| Isolated mixed workload | Normal and actual-process race runs PASS, with seven measurement processes, concurrent exec/wait/status and two 12 MiB retained syncs. Control p95 ratios were 0.991–1.167 normal and 0.995–1.136 under race, all below the unchanged 2× limit. Every owner progressed; cancellation, exact traffic accounting, reservations and idle bulk reclamation passed. No concurrent heavy build or load test. |
| Online dependency/release gate | Go 1.26.8 + govulncheck 1.8.0: four source and six actual-binary reports have zero findings; modules verified. Manifest, CycloneDX 1.6 SBOM and unsigned SLSA provenance generated for clean `044f395`; official SBOM schema and `verify-release` PASS. Altered provenance subject was rejected, restoration passed. |

The initial online baseline reported 47 distinct advisories (27 with call traces)
in Go 1.25.0/x/sys 0.41.0. The targeted upgrade removed all findings; MCP SDK stayed
at v1.7.0. The vulnerability database was actually queried at `https://vuln.go.dev`
(database modification time `2026-09-02T19:12:04Z`). Final outputs are in
`/data/tmp/rdev-validation/phase6-release/final-044f395`; hashes are retained in the
validation record. This is an executed local gate, not a hosted CI or signed release.

The initial four-entry run exposed a fixture error: its business destination was
the agent state root, so sync's own staging changed the reviewed target snapshot.
`044f395` puts the business files in an independent child directory. Independent
review and normal/race reruns passed with all byte and drift assertions retained;
no product protection was weakened. Individual passing Phase5 tests from that
initial batch are identified separately from its failed aggregate in the record.

Phase8 retains signed releases, distribution channels, complete automated
upgrade/rollback combinations and 100-host/24-hour production certification.
