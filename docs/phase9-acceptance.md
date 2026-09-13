# Phase 9 acceptance

Status: **Experimental implementation; native Windows runtime and real SSH
acceptance remain unverified. This is not Phase9 Complete.**

The implementation scope and P9-01 through P9-10 gates are defined in
[phase9-plan.md](phase9-plan.md). The available implementation environment is
Linux amd64. A Windows SSH runtime endpoint has been requested; no native runtime
result is claimed until recorded here. The Windows CI job targets Windows Server
2025; even a future passing run does not by itself certify Windows 11 or the
OpenSSH session lifecycle.

Phase 8's independent production gates remain pending as documented in
[phase8-acceptance.md](phase8-acceptance.md).

## Implemented behavior

| Gate | Implementation | Runtime boundary |
|---|---|---|
| P9-01 | Separate Unix/Windows execution, process, state, file-lock and installer implementations; fifth embedded agent target | Cross-build evidence only |
| P9-02 | Windows/Cygwin/MSYS discovery; short UTF-16 encoded PowerShell loaders; bounded stdin script frame followed by exact executable bytes; native stdin/stdout inheritance | Default cmd.exe and PowerShell SSH shells need real endpoint tests |
| P9-03/04 | Direct native argv, cwd/env/stdin, byte-preserving output, stream integration; suspended launch → Job Object assignment → resume; timeout/disconnect contain descendants | Native runtime test binary compiled; execution pending |
| P9-05 | Detached supervisor, creation-time process identity, continuous state-writer readiness barrier, durable start replay, logs/status/stop, job GC records | SSH server must permit process breakaway; failure rejects job start |
| P9-06 | Owner/SYSTEM DACLs, trusted ancestor ACL checks, reparse refusal, native byte-range locks, private atomic record publication | Elevated Administrators ownership is trusted; ACL grants remain user/SYSTEM only |
| P9-07 | File read/write/append/chunk transfer; native staged shared sync; volume/file-index identity; Windows name/case rejection | Local drives only; native Windows standalone rsync is unsupported |
| P9-08 | Existing controller release admission; digest-named executables; active release record; real hello/state checks; journal recovery and signed direction checks | At most eight retained versions and four reserved upload slots; actual Windows crash/restart validation pending |
| P9-09 | Windows remote experimental support discovery; release manifest, embedded-agent, SBOM and reproducibility lists updated | Historical six-binary signed payloads remain readable; new bundles contain seven binaries |
| P9-10 | Native Windows tests and CI job; opt-in Linux-controller → Windows SSH gate | No native CI run or Windows SSH pass is claimed |

## Windows remote usage

For preparation by an agent running on the Windows machine, use the complete
[Windows preparation prompt](phase9-windows-preparation-prompt.md). It separates
host configuration, controller SSH connectivity, and actual rdev runtime evidence.

Use a supported Linux controller. Provision Windows OpenSSH Server, verify the
host key, and confirm noninteractive public-key authentication through an SSH
alias. Keep the existing administrator release policy; Windows support does not
enable unsigned installation automatically. Development binaries require the
same explicit unsigned-dev policy described in Phase8.

```sh
make GO=/path/to/go all daemon
rdev hosts add win win-build -cwd 'C:/work/project' -save
rdev hosts trust
rdev hosts approve-project APPROVED_DIGEST
rdev ping win
rdev support win
rdev exec win -- whoami.exe
rdev exec win -- powershell.exe -NoLogo -NoProfile -NonInteractive -Command '[Console]::WriteLine("hello")'
```

`win-build` is the user's configured SSH alias, not a supplied host. Remote
PowerShell command text in `exec` is an explicit business command subject to the
usual broker authorization/approval. Bootstrap paths and arguments travel as
encoded data rather than interpolated shell fragments.

`login_shell=true` has POSIX semantics and is refused on Windows. Invoke
`powershell.exe`, `pwsh.exe`, or `cmd.exe` explicitly when shell behavior is needed;
direct `.bat`/`.cmd` execution is refused. Arbitrary native output is not silently
converted from an assumed console code page: invalid UTF-8 is returned as base64.

Shared directory synchronization uses the existing `sync -prepare` / `-plan`
workflow, approvals and delete confirmation. Paths such as `C:/work/project/`
are accepted by prepared JSON transfers. Remote rsync/standalone sync is refused;
the controller may still use local rsync to evaluate existing filter rules.
Windows supports regular files/directories on local drives. UNC/device paths,
alternate streams, reparse points, reserved device names and case-colliding
manifest entries are refused. POSIX mode bits are best effort (Windows read-only
bit); NTFS timestamps have 100 ns precision. ACL/xattr/owner preservation is not
promised.

Background jobs require a successful breakaway from an enclosing SSH session
job. There is no fallback that silently binds a supposedly detached job to SSH.
`TERM` requests supervisor cancellation and tree termination; it does not deliver
a POSIX signal or promise console-control/GUI shutdown. `KILL` also addresses the
identity-bound child Job Object. CPU, memory, PID and POSIX FD budgets remain
unsupported; wall-time and job-count admission retain their explicit contracts.

Windows cannot provide unprivileged POSIX directory `fsync`. Writable file data
is flushed and atomic publication uses write-through moves; equivalent power-loss
durability across filesystems/hardware has not been certified. Keep Windows
state separate from Unix state. Unknown/corrupt state is retained, not repaired
implicitly to admit an upgrade.

Executables live at `versions/rdev-agent-DIGEST.exe` beneath the selected remote
namespace. `.rdev-release.json` selects the active verified version. Running
images are not overwritten or stopped for upgrades. Full/busy version or upload
budgets fail explicitly. Bootstrap reclaims a reservation only after obtaining
its exclusive file handle, validating its exact private layout, observing at
least ten minutes of age, and proving the recorded process creation identity is
gone. A still-mapped candidate image cannot be deleted. Unknown or unverifiable
reservations remain for deliberate inspection.

## Reproducible validation commands

On the controller:

```sh
make GO=/path/to/go all daemon check
go test -race ./... -count=1
RDEV_WINDOWS_SSH=win-build make GO=/path/to/go windows-remote-smoke
```

The SSH gate creates and removes one random `.cache/rdev-phase9-*` namespace.
It uses the locally built Windows agent as an explicitly trusted test artifact;
it does not claim an official signing identity or exercise production policy
administration. Missing `RDEV_WINDOWS_SSH` fails the make target; ordinary full
Go tests explicitly skip this opt-in gate.

On Windows, from this checkout, with Go 1.26.8:

```powershell
$env:CGO_ENABLED = '0'
$env:RDEV_WINDOWS_AGENT = Join-Path $env:TEMP 'rdev-phase9-agent.exe'
go build -trimpath -o $env:RDEV_WINDOWS_AGENT ./cmd/rdev-agent
go test -v -count=1 -timeout=10m ./internal/winutil ./internal/windowsruntime ./internal/agentinstall
go test -v -count=1 -timeout=5m ./internal/transport -run '^TestWindows'
```

The native suite covers private ACLs, junction refusal, replacement/file identity,
shared/exclusive locks and cancellation, actual agent NDJSON, quoted/Unicode argv,
environment/cwd/stdin, binary files, exit codes, timeout/disconnect descendant
cleanup, detached restart/durable replay, stop/migration fencing, staged sync
outcomes and installation interruption barriers. Windows CI retains OS/build,
toolchain, agent digest and structured test output. Bootstrap tests run Windows
PowerShell through cmd.exe, exercise quoted/Unicode native argv and binary pipe
inheritance, and install the actual agent through the framed stdin loader.
End-to-end signature/channel
rollback on Windows and OS/power-loss fault drills still need native evidence.

## Local results (2026-09-13)

Environment: macOS controller workspace, pinned Go toolchain selected by the
repository Makefile, main at `52dbfa2`. These are local engineering results,
not signed release or hosted CI evidence.

| Check | Result |
|---|---|
| `make all daemon check` with the pinned Go toolchain | Passed: CLI/broker, all package tests, vet and five embedded agent checks |
| `go test -race ./... -count=1 -timeout=20m` | Passed |
| `make test` on the current checkout | Passed |
| `go test -race ./cmd/rdev-agent -count=1 -timeout=5m` after the log-handle change | Passed |
| Final `make all daemon check-agents` after Windows device-name validation | Passed |
| Windows amd64 agent and winutil/windowsruntime/agentinstall/transport test executables | Cross-compiled successfully; not executed |
| Historical six-binary SSHSIG bundle | Passed actual test-signature and metadata verification |
| Bootstrap quoting, encoded framing and command length | Passed platform-independent tests |
| Diff whitespace, shell script syntax, workflow YAML and release-schema JSON | Passed |
| Real Windows SSH | Skipped in ordinary tests: `RDEV_WINDOWS_SSH` was not provided |
| Windows native execution / Windows CI | Pending; no run is claimed |

Local logs are at `/data/tmp/rdev-phase9-final-check.log`,
`/data/tmp/rdev-phase9-race.log`, `/data/tmp/rdev-phase9-agent-race.log` and
`/data/tmp/rdev-phase9-build.log`. They are not repository artifacts and do not
replace the required Windows/SSH evidence.

## Repair execution (R12/R15)

| Check | Result |
|---|---|
| Plan digest, explicit confirmation and strict transaction phases | Implemented in `internal/agentrepair`; unit tests pass |
| Candidate byte validation and pre-mutation snapshot binding | Implemented and tested with synthetic bytes |
| Transport mutation entrypoint and install lock | `Conn.RepairAgent` and `RepairAgentWithReconnect` reuse the existing atomic installer transaction |
| Fresh dedicated-key reconnect and rollback on failed reconnect | Passed in disposable `home-ubuntu` container: real candidate install, `IdentitiesOnly` fresh-auth version probe, truncated-candidate health refusal, unchanged active SHA-256, and valid active agent after failure |
| Password via inherited private FD; no TTY/MCP/job fallback | Existing bootstrap boundary retained; repair-specific end-to-end evidence pending |

The isolated executable smoke entrypoint is `scripts/repair-fresh-auth-smoke.sh`; it runs synthetic-key fresh-auth repair hooks without touching a remote account.

2026-09-13 `home-ubuntu` isolated container evidence: a disposable
`rdev-onboarding-sandbox:port-test` container (`rdev-repair-ssh2`) was started
with a synthetic Ed25519 key installed only in the container's `/root/.ssh`.
`docker exec ... ssh -p 2222 -o IdentitiesOnly=yes -i /tmp/id root@127.0.0.1 true`
returned `fresh-auth-ok`; no host `authorized_keys` or service was changed.

The same disposable container then received the cached Linux amd64 agent,
executed the real `-install-candidate` entrypoint with an in-container
synthetic unsigned-dev decision, and reported the installed
`/tmp/rdev-state/rdev-agent -version` identity. The container was stopped and
removed immediately afterward.

The standalone MCP repair tool is intentionally the mutation route; broker MCP
does not expose password/key-bearing repair inputs and therefore remains a
separate unsupported boundary until an administrator-owned broker credential
transport is defined.

Final container closure: the real candidate installer succeeded, and the
installed agent was invoked over a new SSH connection using a synthetic
dedicated key with `IdentitiesOnly=yes`; it returned the expected version and
installer identity. A second run used a 128-byte truncated candidate; health
verification failed with `RDEV_AGENT_INSTALL_NOT_SENT:verify`, the active and
pre-install SHA-256 remained identical, and the active agent still reported a
valid version. The disposable container was then removed.

2026-09-13 service-deploy runtime evidence: `RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE=service-deploy go test ./cmd/rdevd -run '^TestRemotePhase8MasterDisappearance$'` passed. The owned SSH master was killed, a new serving agent reconnected, the detached supervisor and exact marker were retained, and cross-project access remained denied. This validates the reconnect/recovery substrate used by repair; it does not by itself prove dedicated-key repair authentication.
