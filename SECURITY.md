# Security Policy

## System and Scope

rdev is a local CLI and stdio MCP server that connects to user-selected remote
machines through the system OpenSSH client. It bootstraps a remote `rdev-agent`,
exchanges ID-correlated NDJSON requests, persists remote job state, and invokes
local `rsync` for file synchronization.

This policy covers the Go source, local configuration and trust state, SSH and
rsync process construction, bootstrap scripts, the wire protocol, secret
handling, and rdev-managed local and remote state. A caller that selects a host
is intentionally authorized to exercise the command and file authority of that
host's SSH account; rdev does not sandbox commands within that account.

## Threat Model and Trust Boundaries

- A checked-out repository, its path, and `<project>/.rdev/hosts.json` are
  attacker-controlled until the user approves the exact absolute path and file
  digest. Merely opening a repository is not approval.
- Global config and the project trust store are controlled by the local OS user.
  Other local OS users are untrusted.
- SSH destinations, ports, remote state paths, sync paths, environment values,
  and protocol fields are data even when they came from an approved config or a
  tool call. Approval does not authorize option or shell injection.
- The remote host and all bytes received from it are untrusted with respect to
  local integrity, memory, logs, and secrets. rdev assumes the user separately
  verifies SSH host keys and configures authentication.
- Other processes using the same remote account can race with agent bootstrap
  and rdev-managed state. Atomic replacement and ownership checks must preserve
  integrity under those races.
- Multiple local AI agents or future broker clients are distinct principals.
  Shared connections or state must not silently grant one principal another's
  hosts, secrets, jobs, or approvals.

## Security Invariants

- An unapproved or changed project config cannot override a global host, trigger
  SSH, bootstrap an agent, load a remote secret, or widen policy. Approval binds
  the exact canonical project path and SHA-256 content digest.
- Every SSH process creation validates its destination and port at the final
  shared boundary. Destinations that are empty, option-shaped, contain whitespace
  or control characters, or use an invalid port fail closed.
- `remote_dir` is a canonical, home-relative path made only of safe components.
  Dynamic bootstrap values are passed as positional parameters or standard input
  and never concatenated into shell program text.
- rsync terminates option parsing before operands. Local and remote operands are
  validated for the representation used by local rsync and the remote shell.
- Config and trust files are regular files beneath non-symlink config
  directories. POSIX reads require effective-user ownership, no group/other
  write authority, and no recognized extended ACL. Darwin ACL inspection is
  bound to the already-open descriptor; builds without that native capability
  fail closed. Writes use a same-directory 0600 temporary file, file fsync,
  atomic rename, and directory fsync; final directories are 0700. The first
  successful post-rename directory fsync is the commit point: failures before
  it require a durably verified rollback, ambiguous rollback stops the Registry,
  and failures that only clean a backup are reported as committed warnings.
- A pooled connection is valid only for the current immutable canonical host
  fingerprint and Registry generation. Alias replacement atomically publishes
  approval/config state and invalidates old connections used by the public
  exec/read/write/sync sinks. Their operation leases are per alias, so one
  host's long operation does not block unrelated host updates. Host-secret
  initialization holds the same immutable identity/generation lease, atomically
  loads every declaration before pool publication, and fails closed; request
  construction, secret resolution, remote I/O, recursive output/error redaction,
  and secret rotation remain inside the corresponding read/write lease.
- Agent bootstrap writes only through an exclusively-created unpredictable
  staging object. Regular-file type is checked without localized `stat` text;
  owner, link count, inode, and digest stay bound to open descriptors and a
  verified hard-link snapshot through installation. Its explicit `STAGED →
  VERIFIED → INSTALLING → COMMITTED` state machine only rolls back a target
  still bound to this publication inode, verifies the restored inode and digest,
  preserves evidence on ambiguous rollback, and reports post-commit cleanup as
  a committed warning. First publication is no-replace and never deletes a
  concurrently occupied target.
- Project config remains data after approval. Invalid destinations, paths, ports,
  and unsupported schema fail before any entry is merged into live state.
- Registered secret values do not persist to config, cross host/principal scope,
  or enter tool results, structured logs, metric labels, traces, diagnostics, or
  errors. Paths and identifiers are minimized or hashed at observability sinks.
- Remote frames, outputs, waits, concurrency, logs, and rdev-managed storage must
  have system-enforced hard bounds. Project config cannot raise those hard caps.
- Mutating requests are not replayed unless a stable operation identity proves
  the replay is safe. Unknown execution outcomes remain explicitly ambiguous.
- Job control validates durable process identity and state ownership before
  signaling or deletion. Rdev never deletes unknown or user-owned paths.

## Reportable Findings and Severity Context

Report issues that let repository data or remote input execute locally, escape
the selected SSH destination, inject bootstrap shell, expose secrets, cross
host/principal ownership, overwrite files through links, replay mutations, kill
unrelated processes, or cause unbounded local/remote resource consumption.

Repository-triggered local or remote code execution, cross-host credential use,
and unauthorized shared-broker access are high-severity contexts. Secret leaks,
mutation replay, and practical unbounded-memory paths are normally medium or
higher. Same-account state integrity and information exposure remain reportable
when rdev promises stronger ownership or permissions.

## Out of Scope, Exclusions, and Accepted Risk

- A caller deliberately running `sh -c`, destructive commands, or file mutations
  within the authority of the explicitly selected SSH account is expected product
  behavior, not command injection by itself.
- Compromise already requiring control of the same local OS user is outside the
  current privilege boundary, though corruption-resistant state remains a
  reliability goal.
- Weak SSH keys, `~/.ssh/config`, agent forwarding, ProxyJump policy, and host-key
  enrollment are user-managed. rdev must not recommend disabling host-key checks.
- Native Windows execution, interactive PTY/TUI forwarding, port forwarding,
  full ACL/xattr fidelity, and multi-tenant remote sandboxing are not currently
  supported security guarantees.

There are no accepted exceptions to the invariants above. Known incomplete
controls are tracked in `docs/rdev-evolution-security-plan.md`; incompleteness is
not evidence that a finding is safe.

## Known Limitations and Compensating Controls

The current release is a single-process client rather than the planned shared
`rdevd`; callers therefore have no broker-enforced capability model yet. Project
config approval, strict process-argument validation, SSH host-key verification,
in-memory secret storage, and narrow file permissions are the present controls.

Tier and capability claims are maintained in the README and the machine-readable
support snapshot. Build-only platforms are not promoted to supported runtime
tiers without isolated real-SSH and rsync certification.
