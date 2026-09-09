# Phase6 acceptance

Status: integration and final-source validation in progress. Baseline `de96704`
was clean and equal to `origin/main`. Phase5's production baseline `29b5f08`
remains the security and runtime regression reference.

macOS runtime remains unverified and explicitly deferred by the user. Linux
runtime and cross-build coverage are recorded separately. No hosted release,
deployment or repository permission change is part of this work.

| Item | Implementation and acceptance evidence |
|---|---|
| P6-01 | Existing interspersed parser now has per-command schemas, exact arity, single/repeat rules, numeric bounds and conflict checks. Daemon standard flags have duplicate/arity checks. `TestStrictCLIInputs`, `TestSingleFlags*`, `TestDaemonFlagErrors*`, `TestProvisioningFlags*`; process errors preserve stable codes. |
| P6-02 | Bounded stdin reader distinguishes EOF and non-EOF; partial data is discarded on error before any write dispatch. `TestStdinFailureCannotDispatchPartialWrite` and actual main-process stdin fault tests cover standalone/broker, partial/empty error, EOF and exact/over-limit inputs. |
| P6-03 | `proto/timeouts.go` supplies exec 60, wait 300, new-job wall 3600 defaults; zero selects default, negative/infinite/out-of-range rejected. Four-entry runtime test checks foreground termination, observer expiry with surviving job, wall termination and resource result fields. New job requires negotiated envelope support; old peers fail before launch. Durable job digest/replay regression preserves approval and recovery binding. |
| P6-04 | `transport.ParseDestination` reaches registry, immutable identity, final SSH and rsync arguments. IPv4/DNS/alias/user/IPv6, malformed ports and conflicts covered; `TestLocalIPv6SSHAndSync` uses an isolated real `::1` sshd, bootstrap, exec and rsync. |
| P6-05 | README, help, SECURITY and operations corrected to actual protocols, authority, redaction, cancellation, sync and connection lifecycle. Shared host/session editing and declarative delegation are discoverably unsupported with alternatives. State CLI/MCP uses existing host-wide administrator grants and approval, tested through actual processes and SDK. |
| P6-06 | Online audit found Go 1.25.0 and x/sys 0.41.0 advisories; targeted upgrade to Go 1.26.8/x/sys 0.44.0. Pinned govulncheck 1.8.0, four source/six binary scans, online module verification, manifest/SBOM/unsigned provenance and artifact binding are executable release gates. |
| P6-07 | CLI/MCP share support/discovery data. Static/build/experimental/runtime evidence, actual controls, own authorization and supplementary rights are distinct. No probe grant means no SSH or inventory lookup; profile/secret/other-owner state is omitted. |
| P6-08 | `rdev compat` / `rdev_compat` derive error registry, config shapes and protocol/state versions from actual validators/constants. Tests cover agent 2–3, broker 1, broker-agent 3, required feature refusal, legacy forward state migration and unsupported schema rejection. |
| P6-09 | CLI `sync ... -- -leading-local REMOTE` preserves the operand; exec argv including a second `--`, empty arguments, spaces and Unicode remains unchanged. Actual dual-mode CLI and official SDK sync verify resulting bytes. |

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
  ports conflict. Raw single-word hosts still require registered SSH aliases.
- Shared state reports cover the entire host state root. Only separately granted
  administrators should receive them; migration/repair previews also need approval.

## Independent review

Three agents reviewed behavior, boundaries and implementation, with cross-review
of author changes. Confirmed findings addressed include parsed/raw operand drift,
default job durable-digest mismatch, typed CLI/MCP error projection, old-peer
resource-envelope omission, and support permission/callability scope drift.
Final review and source identity are recorded after the final gate below.

## Final verification

Pending the final committed-source run. Raw temporary logs stay outside the
repository under `/data/tmp/rdev-validation`; this table will retain the final
source, commands and measured outcomes rather than duplicate historical logs.

Phase8 retains signed releases, distribution channels, complete automated
upgrade/rollback combinations and 100-host/24-hour production certification.
