# Security Policy

## System and Scope

rdev is a local CLI and stdio MCP server that connects to user-selected remote
machines through the system OpenSSH client. Shared mode uses an authenticated
local `rdevd` broker for connection ownership, policy, approvals, audit and quotas.
MCP uses the official SDK's JSON-RPC 2.0 over stdio. The internal client/broker
and client-or-broker/agent protocols have separate version negotiation and
ID-correlated NDJSON framing; neither internal protocol is JSON-RPC. rdev persists
remote job state and uses rsync or the broker's prepared sync path for transfers.

This policy covers the Go source, local configuration and trust state, SSH and
rsync process construction, bootstrap scripts, the wire protocol, secret
handling, dependency/release-check metadata, and rdev-managed local and remote
state. Standalone exercises the selected SSH account's authority. Broker clients
additionally require the exact principal/project/host policy and any applicable
approval. Neither mode sandboxes arbitrary commands within the remote account.

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
- Multiple local AI agents and broker clients are distinct principals.
  Shared connections or state must not silently grant one principal another's
  hosts, secrets, jobs, or approvals.

## Security Invariants

- An unapproved or changed project config cannot override a global host, trigger
  SSH, bootstrap an agent, load a remote secret, or widen policy. Approval binds
  the exact canonical project path and SHA-256 content digest.
- Every SSH process creation validates its destination and port at the final
  shared boundary. Destinations that are empty, option-shaped, contain whitespace
  or control characters, malformed IPv6 brackets, or invalid ports fail closed.
  Bare IPv6 is always an address; its port requires `[IPv6]:port`. An embedded
  port conflicts with a separately supplied nonzero port. Normalized user,
  address and port participate in connection identity; rsync uses the bracketed
  IPv6 spelling required by its own host:path grammar.
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
- Project config remains data after approval. Invalid declared destinations,
  paths and ports fail before any entry is merged into live state. Host config
  is currently unversioned: standalone ignores unknown JSON fields, while the
  administrator hosts-file reader rejects them. Versioned trust/state artifacts
  follow their own explicit schema validators, exposed by `rdev compat`.
- Host configuration stores secret declarations as paths, not secret values.
  Standalone values stay in process memory; broker principal-owned credentials
  and historical redaction versions persist in a private 0600 archive. Matching
  registered values are scrubbed at result boundaries. Authorization and lookup
  cannot cross host/principal scope; observability sinks omit secret values and
  raw output and minimize or hash paths and identifiers. Redaction limitations
  are described below.
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

- Broker tokens authenticate the exact `(client_id, project_id)` and expire;
  the administrator signing key is never a client credential. Policy defaults
  to deny, supports exact-host grants, and classifies capabilities server-side.
  Explicit unauthenticated compatibility mode is not a principal-authentication
  boundary. Same-OS-user file access is outside this isolation guarantee.
- Mutation approval binds owner, target identity, effective request digest and
  policy snapshot. A client risk hint cannot bypass approval. Mutation intents
  and job identities survive broker restart; restart does not re-execute an
  unproven mutation. Ambiguous outcomes require reconciliation.
- Broker jobs, secrets, observations and ordinary status are owner/project
  scoped. Shared waits retain independent subscriber budgets and cancellation;
  peer cancellation does not close the shared base transport. Warm/bulk eviction
  respects active leases and resource accounting. Pool health and whole-host
  state administration require separate grants.
- Support discovery returns only the caller's policy decisions. Without a
  capability-probe grant it performs no SSH or host-registry lookup. Static
  support, current runtime capability and permission denial are distinct.
- CLI unknown, missing, duplicate, conflicting and out-of-range arguments fail
  before business I/O. A non-EOF stdin failure, including partial data plus an
  error, cannot submit that partial data to standalone or broker writes. Input
  remains size-bounded. `--` stops rdev option parsing but does not stop the
  invoking shell from evaluating unquoted text.

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

Shared mode never falls back to private SSH for an unsupported frontend.
Host/session editing and automatic delegation of administrator declarative
secrets are unavailable to shared business clients. Administrators update the
private host registry and restart; clients supply request cwd/env and explicitly
import principal-owned secrets. The state frontend is whole-host administration:
its report can include other owners' root-relative record paths. Grant it only
to administrators; migration and repair, including previews, require approval.

Protocol-3 generic mutation deduplication is bounded and process-local. Durable
broker mutation records and job-start identities provide additional recovery,
not permanent exactly-once execution for arbitrary commands. After restart or
record retirement, an unprovable outcome remains ambiguous and must not be
retried under a fresh operation ID. The machine-readable `rdev compat` contract
states actual protocol/schema ranges and migrations. Protocol-2 common unary
operations do not acquire v3 cancellation, streaming or deduplication guarantees;
unsupported features and disjoint versions fail closed. New job start requires
the negotiated `job_resource_envelope` feature so an older agent cannot silently
ignore the requested wall budget. Existing job inspection, wait and stop remain
available within their own negotiated contracts.

Across standalone/broker CLI/MCP, omitted or zero exec runtime means 60 seconds,
job-wait observation means 300 seconds, and new-job wall runtime means 3600
seconds. Positive values are capped at 3600 seconds; negatives and infinity are
rejected. Connection/request deadlines are separate and may end earlier. This
changes legacy unbounded defaults; already-running old supervisors are not
retroactively modified. A wait timeout does not kill its job, while the job's
own supervisor enforces its runtime limit. CPU, memory and PID tree budgets are
currently unsupported even when cgroup is detected; unsupported resource
requests are rejected instead of claiming enforcement.

Protocol cancellation targets attached foreground operations by caller and
operation identity, using dedicated process groups. TERM-to-KILL escalation
retains the original leader through the group-level decision to avoid PID reuse
or surviving descendants. Immediate/detached mutations do not receive an
inferred wire cancel/deadline: a caller that stops waiting after send can receive
`possibly_executed`/`ambiguous_outcome`. A committed terminal wins its atomic
race with cancellation. Detached jobs survive control-connection teardown, but
not necessarily host reboot; killed supervisors may leave an observable,
stoppable orphan without a recoverable exit code.

Foreground output, protocol frames, auxiliary probes, job logs and managed
storage have bounded retention. Rsync drains stdout/stderr while retaining
256 KiB per stream by default, with a 512 KiB absolute per-stream cap and byte
ledgers. Job logs use bounded storage policy and report dropped bytes. These
controls do not bound arbitrary command writes outside rdev-managed storage or
restore discarded output. Binary exec/read/sync fields pass through decoded
byte redaction before re-encoding; registered values cannot bypass that boundary
merely by selecting the protocol's base64 representation.

Redaction reduces accidental disclosure of registered whole values and selected
whitespace-folded forms. It is not a secret detector or an exfiltration sandbox:
unregistered values, fragments, independently re-encoded/hash/transformed values
and malicious extraction are outside its matching guarantee. Client redaction
does not rewrite raw remote log files or encrypt the broker credential archive.

Agent failures use the stable code/category/retry/execution-state registry;
unknown codes and invalid envelopes are not treated as safe-to-retry results.
Broker authentication, routing and local CLI diagnostics may also return textual
errors. Neither textual error wording nor an output truncation marker proves
that a mutation was not executed.

Fleet is broker-only and initially allows durable `job_start` with explicit argv,
cwd, environment and bounded resources. Inventory is administrator-managed static
metadata referencing the trusted host registry. HostIDs cannot be reused or
transferred to a changed connection identity; alias and label changes cannot
expand a persisted target snapshot. Descriptive owner labels do not grant access.
Snapshots bind the trusted registry destination, port, remote_dir and session
configuration. External OpenSSH config, DNS, ProxyCommand and PATH wrappers are
trusted administrator inputs; Fleet does not detect their drift or attest a
physical machine identity.
Discovery, plan/result pages and retry history retain exact client/project
isolation. Fleet grants are separate from each HostID's `job_start`/`job_status`
grants; inventory management has its own capability.

Every Fleet execution requires an explicit Fleet approval bound to its principal,
immutable targets, connection and operation identities, rollout/failure policy,
policy version and expiry. A single-host approval is not a Fleet approval.
New dispatch checks current permissions and identity again. Submitted attempts
retain their operation IDs across restart and use result queries without replay;
unprovable outcomes remain ambiguous. Cancel stops pending dispatch but does not
implicitly stop detached jobs or erase mutation evidence. Stopping an existing
job remains a separate authorized, approved operation. Fleet execution data is
private, bounded durable state; audit excludes argv, environment values and raw
output. Storage failure freezes new admissions while preserving recorded queries.

The support snapshot identifies Linux amd64 runtime evidence separately from
build-only combinations and macOS's historical development baseline. **macOS
runtime remains unverified and explicitly deferred**; cross-compilation is not
a runtime pass. Complex ProxyCommand is experimental. A successful runtime
probe does not certify an entire OS/architecture combination.

The executable release gate pins Go 1.26.8 through `go.mod` and govulncheck
v1.8.0, audits online dependencies and source/binaries, and verifies module
checksums and artifact-bound manifest/SBOM/provenance metadata. `verify-release`
rechecks the local output. Local unsigned provenance does not authenticate a
publisher; configured or skipped CI is not an executed gate. Phase8 adds SSHSIG manifests, administrator channel/root/pin policy, linked
dependency notices and namespace-locked health/rollback transactions. Official
signer identity, complete platform/upgrade matrices and 24h production
certification remain pending. Hosted validation has actually passed for the
recorded source; exact run/artifact identities and later-run status are in
[Phase8 acceptance](docs/phase8-acceptance.md). Go binaries link
dependency code, including the MCP SDK, regardless of whether it is vendored.

Release trust is read only from the administrator-owned private policy, including
its protected ancestors and native ACL checks. Project files and broker requests
cannot supply keys, unsigned opt-in or rollback permissions. Stable/beta never
fall back to unsigned on verification failure. SSHSIG authenticates the release;
SSH host keys authenticate the remote endpoint. Offline verification uses the
local policy's bounded validity and revocation snapshot. Test roots require an
explicit test opt-in and never constitute official release identity.

Signed rollback requires an exact administrator target/digest grant and retains
channel, signature, pin and state-readability checks. The binary transaction does
not downgrade durable state. New writers hold a permanent shared state lease;
migration/repair takes it exclusively. Legacy writers cannot be retroactively
fenced and must be drained before first migration. Unknown journal/state and
unconfirmed rollback remain preserved for recovery. Policy refusal before the
first business send is distinguishable from an uncertain prior mutation.
