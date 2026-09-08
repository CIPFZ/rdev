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
