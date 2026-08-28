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
- Each protocol direction has one fixed writer loop with bounded priority queues
  and a total queued/in-flight frame budget. A stalled underlying pipe wakes all
  waiters after a fixed write budget. Because closing a Unix pipe descriptor does
  not reliably interrupt an already-blocked syscall on that descriptor, the agent
  then performs bounded attached-work cleanup and exits the serving process; OS
  process exit closes the channel definitively. No request may create a per-write
  goroutine, and detached supervisors are outside that connection teardown.
- Protocol-3 terminal frames are accepted only for the tracked request and
  operation, with one terminal, a valid non-empty execution state, and a
  success/error combination consistent with that state. Unary shape fallback is
  selected only from the negotiated protocol version.
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

Protocol-3 mutation deduplication is deliberately process-local, bounded by both
capacity and TTL, and keyed by caller identity, operation ID, operation type, and
request digest. It prevents duplicate execution while the accepting agent retains
the record, but it is not a durable transaction log. After agent restart, cache
eviction, or reconnect through a newly started SSH agent, an unprovable mutation
outcome is returned as `ambiguous_outcome`; callers must reconcile state rather
than retry with a new operation ID. Protocol-2 peers remain compatible for common
unary operations but do not acquire protocol-3 cancel, streaming, deduplication,
or structured truncation guarantees.

Protocol cancellation and disconnect cleanup apply to attached foreground
operations and target only their dedicated process groups. Detached jobs have an
independent supervisor lifetime and intentionally survive the control connection;
immediate/detached mutations never receive an inferred protocol cancel or
deadline. If their request was sent but the caller stops waiting, the client
reports `possibly_executed`/`ambiguous_outcome` instead of falsely claiming
`canceled`; a cancel racing a successful mutating handler is normalized the same
way. Only a foreground operation whose registry contract is `DisconnectCancel`
may receive a wire cancel/deadline, and cancel-before-request state is bound to
that target operation type. Host-side terminal commit and context cancellation
use one pending state machine under the connection mutex: an already-committed
terminal wins even if Go's select chooses `ctx.Done`, while a winning cancel
marks/removes the pending call before its cancel frame is queued outside the
lock. A success arriving after that cancel boundary is a protocol violation,
never silently rewritten as canceled. Their durable storage budgets and
cross-process ownership model remain Phase 4/5
work. TERM-to-KILL escalation retains the original leader as an unreaped child
until the group-level grace and KILL decision complete, so an early-exiting leader
cannot cancel escalation or allow its PID/PGID to be reused for an unrelated
request. The current hard memory, frame, watcher, queue, and output limits do not
cap the size of detached job log files on disk.

Foreground exec, file reads, protocol frames, agent diagnostics, auxiliary SSH
probes, and local rsync stdout/stderr all retain bounded data while continuing to
drain their producers. Rsync reports exact original/retained/dropped byte counts
per stream and uses base64 for retained binary data. The default retention is
256 KiB per stream and callers may select a value only within the 512 KiB absolute
per-stream cap. Binary exec/read/rsync fields are decoded before the client
redaction boundary and losslessly re-encoded afterward, so base64 cannot hide a
registered secret. A truncation report describes what rdev retained; it is not a
durable archive of the discarded bytes.

Agent business failures cross one typed mapping boundary. Invalid requests,
resource limits, missing objects, process-start failures, and invalid process
states use registry-backed code/category/retry/execution-state values; unknown
failures alone become `internal.failure`. Public messages are fixed registry text
and intentionally omit paths, argv, and raw operating-system errors.

Tier and capability claims are maintained in the README and the machine-readable
support snapshot. Build-only platforms are not promoted to supported runtime
tiers without isolated real-SSH and rsync certification.
