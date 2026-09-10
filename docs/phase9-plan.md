# Phase 9 — Windows remote execution

Scope: a Linux/macOS controller connects through OpenSSH to a native Windows
agent. The first target is Windows 11 x64, NTFS, Windows OpenSSH Server and
Windows PowerShell 5.1. Windows Server is a separate runtime validation target.
Native Windows controllers, interactive desktops, elevation prompts and PTYs
are outside this phase. Existing Phase 8 production gates remain unchanged.

## Delivery and acceptance

| ID | Work | Acceptance |
|---|---|---|
| P9-01 | Platform boundary and Windows agent build | Existing Linux/Darwin builds and checks continue to pass; Windows amd64 agent builds |
| P9-02 | Windows SSH probe/bootstrap | Default cmd.exe and PowerShell SSH shells accept fixed encoded commands; home/paths and binary digests are validated |
| P9-03 | Native command execution | argv, cwd, environment, stdin, Unicode/binary output, nonzero exit, streaming and cancellation retain protocol contracts |
| P9-04 | Process containment | Request-owned Job Objects contain children before user code starts; timeout/disconnect terminate descendants without affecting other work |
| P9-05 | Durable background jobs | Supervisor survives serving-agent disconnect; durable start identity prevents replay; status/logs/stop work from a new agent; PID reuse cannot stop unrelated work |
| P9-06 | Private state and locking | Owned protected ACLs, reparse-point rejection, cross-process locks, atomic record replacement and migration fencing |
| P9-07 | Files and synchronization | Bounded file read/write/chunk transfer; native staged sync; explicit Windows path/name/case/link restrictions; deletion still uses approved plans |
| P9-08 | Signed install, upgrade and rollback | Existing local trust admission; immutable versioned executables and atomic active record; health/state checks, concurrent install fencing and crash reconciliation |
| P9-09 | Capability and release integration | Windows target included in embedded agents and exact release manifests; unsupported resource/shell/rsync operations report explicit boundaries |
| P9-10 | Runtime and regression evidence | Native Windows protocol/process/filesystem tests and SSH harness; Windows CI; Linux regressions; runtime-unverified until tests actually execute on Windows |

## Design decisions

- Reuse the agent NDJSON protocol and existing CLI/MCP/broker authorization.
- Keep Unix lifecycle implementations and add Windows platform implementations.
- Use `powershell.exe -NoLogo -NoProfile -NonInteractive -EncodedCommand` only
  for fixed bootstrap programs. Dynamic values are encoded data, never raw shell
  fragments. Keep the command under the default cmd.exe length limit; installation
  reads a bounded base64/gzip script line and then the raw executable from the
  same stdin stream without text-reader buffering. Business commands use direct native process creation; an explicit
  PowerShell executable/script is supported without POSIX login-shell semantics.
- Use Job Objects for foreground and supervisor-owned child trees. Detached
  supervisors require a successful breakaway from any SSH session job; failure
  is explicit and cannot silently create a disconnect-sensitive background job.
- Publish versioned `.exe` files and atomically select a verified version rather
  than replacing an executable which another session may still be running.
- Keep Windows state separate from Unix state. Preserve unknown/future records
  and existing ambiguity/replay semantics.
- Do not infer runtime certification from cross-compilation or workflow files.

Implementation evidence and remaining runtime gates are tracked in
[phase9-acceptance.md](phase9-acceptance.md).
