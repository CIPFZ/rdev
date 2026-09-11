# macOS controller acceptance

Status: **validated for macOS arm64 controller → Linux amd64 remote** (2026-09-11).

This record closes the macOS controller runtime gap from Phases 5–8 for the
tested topology. It does not certify macOS as a remote agent, macOS amd64,
Linux arm64, logout/reboot persistence, or production release signing.

## Environment and evidence

- Controller: macOS 26.6.2, Darwin arm64, Go 1.26.8, cgo enabled.
- Target: the preconfigured `service-deploy` SSH alias, Linux x86_64, OpenSSH
  public-key/BatchMode authentication with the existing host-key policy.
- Local broker: shipped LaunchAgent template exercised in the current GUI
  launchd domain; the unique test service was removed after every run.
- Evidence directory: `/Users/tonyny/works/rdev-validation/macos-20260910`.

`launchd.json` records bootstrap, authenticated readiness, duplicate-instance
rejection, invalid/valid reload, SIGKILL KeepAlive recovery, bootout cleanup and
rebootstrap. `final-check3.log` records the final native cgo build, vet,
all-package tests and embedded-agent consistency; `final-race.log` records the
Darwin race run. `remote-core3` and
`remote-sync3` are JSON event logs and summaries for the real SSH suites. The
remote agent was intentionally uninstrumented; this is runtime evidence for the
macOS controller and Linux target, not a Linux agent race run.

## Covered behavior

The real SSH suites exercised standalone and shared CLI/MCP entry points,
principal and project isolation, approvals and audit binding, connection reuse,
20 independent client processes, cancellation and lease lifetimes, detached
job recovery, shared waits, mutation replay protection, credentials, support and
state boundaries, prepared push/pull, rsync preview/cancellation and exact
binary/Unicode payloads. The target namespace and release policy were random
private fixtures and were removed after each run.

The macOS implementation had two portability corrections during this closure:
test policy fixtures now use the resolved `/private/tmp` path; and staged sync
rejects a replacement that would remove an excluded descendant even when the
installed rsync's `--force` behavior differs. Symlink permission bits are
canonicalized to the portable `0777` representation because Darwin and Linux
expose different link modes; link targets remain protected by the existing
no-follow checks. Linux execution of the exclusion regression also passed.

## Remaining boundaries

The cross-UID broker denial check was not run under an administrator-authenticated
root helper; the current-user peer credential path is exercised by launchd and
shared broker connections. This is a separately bounded OS-permission evidence
item, not a reason to claim another user was tested. macOS launchd logout/reboot
survival and macOS-as-remote-agent SSH tests remain outside this controller
closure.
