# Operating the shared broker

Shared `rdevd` requires principal credentials by default. The administrator owns
the signing key; each client receives only a short-lived bearer token for its
exact `(client_id, project_id)`. The token authenticates identity, while the
policy still defaults to deny. Sharing the signing key with clients defeats
this boundary. Processes with access to the same OS account's files are not
sandboxed by this mechanism.

Build the daemon with Go 1.25.0 and build the agents using `make agents`.
Install `rdevd` in `~/bin` and the four agent binaries in
`~/.local/share/rdev/agents`. On the original macOS development machine, use
`/Users/tonyny/sdk/go1.25.0/bin/go`; on other platforms pass the corresponding
absolute path as `make GO=/absolute/path/to/go`.

## Provisioning

Run these commands as the daemon's OS user. Key generation refuses to overwrite
an existing file and creates a random 256-bit key with mode 0600.

```sh
umask 077
mkdir -p "$HOME/.config/rdev" "$HOME/.cache/rdev"
chmod 700 "$HOME/.config/rdev" "$HOME/.cache/rdev"
rdevd principal-keygen -out "$HOME/.config/rdev/principal.key"
rdevd principal-token -key-file "$HOME/.config/rdev/principal.key" \
  -client-id agent-a -project-id project-a -ttl 1h > agent-a.token
```

Deliver `agent-a.token` through the client's credential channel. Set
`RDEV_CLIENT_ID=agent-a`, `RDEV_PROJECT_ID=project-a`, and `RDEV_PRINCIPAL_TOKEN`
to that file's contents in the client process. Never pass the signing key to a
CLI/MCP client. Tokens expire after at most 24 hours, and expiration also closes
already connected clients. Issue a fresh token before expiry and reconnect.

Start the broker:

```sh
rdevd -principal-key-file "$HOME/.config/rdev/principal.key" \
  -ready-file "$HOME/.cache/rdev/ready"
```

The socket's parent directory must be owned by this OS user with mode 0700;
the socket and lock use 0600. A duplicate instance cannot clear the running
instance's readiness. Invalid credential/config/policy/job state fails startup
without advertising READY or replacing malformed persisted authorization state.

`RDEV_PRINCIPAL_SECRET` is retained for administrator-managed environments. A
file is preferable for rotation without restarting the process. Explicit
`-allow-unauthenticated` compatibility mode accepts self-declared owner IDs and
must not be used as an authentication boundary between untrusted clients.

## Rotation and config reload

Generate a replacement in the same private directory and atomically rename it,
then signal the daemon or use the service manager's reload command:

```sh
rdevd principal-keygen -out "$HOME/.config/rdev/principal.key.next"
mv "$HOME/.config/rdev/principal.key.next" "$HOME/.config/rdev/principal.key"
systemctl --user reload rdevd.service
```

For a foreground daemon, send SIGHUP to its PID. After a successful reload,
all sessions authenticated with the old key close and old tokens are rejected.
Issue new tokens using the replacement key and reconnect clients. There is no
overlap window; key rotation revokes every old token. This does not signal
detached remote jobs. Full detached-job crash/reload acceptance is tracked
separately in `phase5-acceptance.md`.

Configuration defaults to `<socket>.json`. If present it must be a private 0600
regular file, contain one JSON object, and contain only recognized fields. An
explicit `-config` path must exist. Duration fields are nanoseconds:

```json
{"max_hosts":128,"idle_ttl":300000000000,"bulk_idle_ttl":30000000000,"qos":{"max_active":12,"per_host":11,"per_owner":4,"max_queued":256,"per_owner_queued":32,"bulk_bytes_per_second":8388608},"owner_weights":{"agent-a\u0000project-a":2}}
```

SIGHUP parses and validates config and key before applying them. Invalid input
retains the running configuration and key; restoring valid files and sending
SIGHUP retries reload. Deleting a previously loaded config is a reload failure.
`qos` controls execution and queue capacity separately from host count. Zero or
omitted QoS fields use the values above. `max_hosts` currently bounds distinct
hosts with active broker work; it no longer changes the per-host handler limit.
Warm-pool capacity/LRU and fairness when a small active-host bound is saturated
still need connection-manager integration, and are not covered by that setting.

Admission is weighted by the authenticated `(client_id, project_id)` pair.
Weights count dispatches, so a weight of three receives three turns per turn of
a continuously backlogged weight-one owner; it is not a CPU-time entitlement.
Queued work does not occupy handlers. The broker reserves two execution slots
for control globally/per host and one per owner. A single owner can occupy only
one of the two control workers. One eighth of global/per-owner queue capacity
(at least one owner slot) is reserved for control. Reloaded weights apply to
already queued requests; canceled entries and empty owner queues are removed.

Large file reads/writes use one on-demand bulk transport per host, alongside the
shared base agent for control/exec. Bulk uses the base client's identity lease
and initialized secret store. `bulk_idle_ttl` defaults to 30 seconds, independent
of connected frontends; the daemon checks idle transports every five seconds.
Actual file payload bytes are charged to `bulk_bytes_per_second` globally, with
one response-sized burst. The next bulk request waits without occupying a
worker or a transport lease. This budget protects control latency from the CPU
and allocation cost of large responses as well as network contention. It does
not yet account for every sync/streaming route or constitute an OS bandwidth
limit. `status` returns only the calling owner's scheduler counts, queue timing
and bulk payload bytes; it does not reveal another owner's host or workload.

Audit events enter a bounded 1024-entry asynchronous queue. A writer flushes at
25 ms or 64 events, maintaining the current segment and one rotated segment,
each bounded by 8 MiB. `audit_query` performs a one-second durability barrier,
then returns the calling owner's in-memory history and an incomplete marker if
flush/recovery failed. The administrator-only `audit.health` operation requires
its own policy grant and exposes pending, accepted/written, dropped, rotation,
recovery and error counters. Ordinary owners cannot query those global counts.
Writes continue after a recoverable sink failure, while historical errors remain
visible. Closed/overloaded sinks count dropped events instead of blocking RPCs.

Audit shutdown drains the bounded queue with a five-second caller deadline.
SIGKILL can lose records still in memory (normally the last 25 ms; longer under
sink failure). An invalid/torn on-disk record is reported as a recovery omission;
an incomplete active-file tail is truncated before appending. Detecting an
unclean shutdown's lost in-memory tail, a long-duration rotation/restart soak,
and durable end-to-end operation correlation remain open Phase5 requirements.

## Services and repeatable validation

The systemd unit in `deploy/systemd/rdevd.service` points to `~/bin/rdevd`, the
private signing key above and `~/.local/share/rdev/agents`. Install it under
`~/.config/systemd/user/`, run `systemctl --user daemon-reload`, then
`systemctl --user enable --now rdevd.service`. It restarts on failure, supports
reload and allows 15 seconds for the broker's 10-second drain. Readiness is at
`$XDG_RUNTIME_DIR/rdevd.ready`. Enabling user linger is an administrator choice
when the service must survive logout.

For launchd, replace `/Users/REPLACE` in the supplied plist with the installation
home, install it under `~/Library/LaunchAgents/`, and bootstrap it into the
user's GUI domain. The template specifies a private key and readiness path.
Actual launchd installation/recovery has not yet been verified in this Linux
workspace; it remains an explicit P5-15 acceptance requirement.

```sh
make smoke-rdevd
make remote-smoke
make remote-service-smoke
make remote-phase5-runtime
```

Remote targets use `service-deploy` by default. Override `RDEV_REMOTE_SSH` or
`RDEV_SSH_CONFIG` for another target/config file. The scripts use uniquely named
temporary artifacts; the service test installs a uniquely named systemd user
unit, verifies enable/start/reload/crash recovery/stop/start, then disables and
removes it. `remote-phase5-runtime` also uploads a compiled test executable and
runs the actual daemon principal/startup/crash tests, so remote Go is unnecessary.
It requires Linux amd64, Python 3, and a running systemd user manager.


## Audit identity compatibility

New audit records use schema 1 and an opaque SHA-256 fingerprint of the exact
client/project owner key. Owner query RPCs return only that fingerprint's
records. Operation and result fields are fixed codes; arbitrary request text,
secret values and outputs are excluded. Legacy schema-0 records used lossy
owner display strings. They remain on disk subject to normal retention, but
are omitted from principal queries with `audit_incomplete=true`, because their
ownership cannot safely be reconstructed. Administrators can inspect the old
segments locally. Long-running rotation and sink failure acceptance remain
open in the Phase5 record.


## Explicit host registry and real session benchmark

`rdevd -hosts-file /private/hosts.json` loads only the administrator-selected
0600 regular file, with a 4 MiB bound, normal host/security validation, and
strict JSON fields. The object must contain a `hosts` array; duplicate names,
null and unknown fields are rejected. It replaces implicit global/project
registry discovery for that daemon. Host-file changes currently require a
restart; SIGHUP reloads the broker config and principal key.

`make remote-session-benchmark RDEV_SSH_CONFIG=/path/to/ssh/config` runs three
20-process / 500-request real-SSH workloads against `service-deploy` (or
`RDEV_REMOTE_SSH`). It creates a unique remote state directory, observes one
shared remote agent PID and the daemon's SSH child count, and verifies distinct
client/project protocol identities. It requires a Linux test runner and Linux
remote with Python 3. This is a short session benchmark; it does not claim
long-running fairness or the bulk/control latency SLO.

Broker-mode protocol dispatch requires a version-3 agent to preserve principal
metadata. Direct compatibility clients retain their legacy behavior. The new
optional ping `caller_id` reports only the current request's opaque protocol
identity; older ping clients can ignore it.

`make remote-lifecycle RDEV_SSH_CONFIG=/path/to/ssh/config` runs real SSH
cancellation and lease tests. It kills the test namespace's verified agent PID,
pauses replacement-agent startup at an OpenSSH wrapper barrier, cancels a
waiting frontend context, and kills the initiating frontend process. It checks
that another owner completes exactly one mutation and that foreground
cancellation preserves other owners' execs and the shared agent. Six additional
lease cycles alternate normal frontend exit and SIGKILL, hold each connection
across a reaper tick, and verify that both the SSH child and remote agent exit
after the final lease expires. The current reaper tick is five seconds, so
observed reclamation takes the configured `idle_ttl` plus up to one tick and
process-exit latency. These tests require Linux `/proc` locally and remotely,
Python 3, and an authorized SSH target; all resources use generated namespaces.

Policy changes made through `policy.grant` are written to a unique 0600 file,
synced, atomically renamed and directory-synced before success is returned.
`grant_host` restricts a grant/revoke to that exact host alias; omit it only when
an operation-wide grant across all targets is intended. Existing operation-wide
file grants retain that administrator-selected scope. Capability is determined
by the server: `exec`, `file.read`, `file.write`, `job`, `sync`, `secret`, `fleet`,
or the operation name for control/administration operations. Old arbitrary
capability labels (for example `operator` for `exec`) must be migrated to the
server's classification; request hints cannot choose a different capability.

Responses and owner-scoped audit records carry `policy_digest`, a SHA-256 of
the policy snapshot used at admission. Revocation affects new admissions;
already-authorized queued requests finish under their captured decision.
Policy files must be private regular files no larger than 4 MiB. Null, duplicate
keys, non-boolean grants and malformed maps fail startup. Errors before rename
retain the old active snapshot. If rename succeeds but directory durability
cannot be confirmed, the policy stops admitting requests and preserves the
possibly committed disk state for administrator recovery instead of overwriting
it during shutdown. Inspect the private policy file and restart after fixing
storage health; a failed acknowledgment in this case has an uncertain outcome.

`make remote-policy RDEV_SSH_CONFIG=/path/to/ssh/config` runs three real SSH
policy tests. Each holds actual remote startup to test a queued decision across
revocation, SIGKILLs the daemon after grant/revoke acknowledgments, checks exact
host/project boundaries and capability substitution negatives, and injects a
real atomic-rename failure without publishing the attempted permission change.

## Exact-request approvals

Shared-broker mutating wire operations require approval, including arbitrary
exec, writes, job start/stop/removal and state/storage mutations. `Risk=false`
cannot disable this requirement and `Target` is ignored as an authorization
hint. Read-only requests still need their normal policy grant. The administrator
has a separate principal explicitly granted `approval.create`; executors do not
receive that capability merely because they can execute an operation.

The administrator writes the reviewed `ApprovalSpec` to a private 0600 JSON
file. Its `owner`, `operation`, `host` and `wire` must exactly match the intended
request, including argv, cwd, env, stdin, write content/mode/append, job parameters
and any explicit semantic deadline. For example, a CLI write without flags uses:

```json
{"owner":{"client_id":"agent-a","project_id":"project-a"},"operation":"write_file","host":"dev","wire":{"op":"write_file","write":{"path":"/tmp/reviewed.txt","content":"reviewed"}}}
```

Using the administrator's `RDEV_CLIENT_ID`, `RDEV_PROJECT_ID` and
`RDEV_PRINCIPAL_TOKEN`, issue one approval:

```sh
rdevd approval-create -socket /private/rdevd.sock \
  -request-file /private/reviewed-request.json > /private/approval.json
```

The response contains a token and its immutable plan. Give only the token to
the intended executor. The CLI reads `RDEV_APPROVAL_TOKEN`; broker MCP exec,
write and mutating job tools also accept `approval_token` per call. For the
example above, the executor sends exactly `reviewed` on stdin to
`rdev write dev /tmp/reviewed.txt`, without a trailing newline. Approval cannot
be used for different content, path, host, operation, owner/project or policy.

Lifetime defaults to one minute and is bounded to ten minutes (`ttl` in the
JSON spec is nanoseconds). Tokens are consumed once at admission; approval
expiry controls admission, not the completion time of already admitted work.
Invalid substitution attempts do not consume another principal's valid token.
All outstanding tokens are invalid after daemon restart. An unconsumed token
must be reissued after a policy or host/session snapshot change. Pending
approvals are bounded in memory and expired entries are reclaimed when issuing
new approvals; raw tokens are never written to the audit sink.

The broker captures the configured host/session snapshot before approval and
checks it under the client's identity lease before any new connection setup
and before dispatch. An operation's explicit deadline can be shortened by its
context but never extended. Audits correlate request/target digests and an
opaque SHA-256 approval reference across issuance, use and result, without
recording raw request bodies, outputs or approval tokens.

`make remote-approval RDEV_SSH_CONFIG=/path/to/ssh/config` runs three real SSH
approval tests covering mandatory classification, separate issuer/executor,
owner/host/parameter substitution, expiry, one-use, policy change, restart and
owner-scoped audit privacy. The session benchmark and lifecycle tests now issue
explicit administrator approvals for each mutation during setup; they retain
separate executor principals and their real shared-transport assertions.


## Detached job ownership recovery

The private `.jobs` snapshot beside the socket has schema 1, a 4 MiB bound and
a maximum of 8192 records. Valid historical arrays migrate on startup. Malformed,
future-version, duplicate or non-private snapshots cause startup to fail while
preserving the file. Back up the file before offline repair.

A failed remote startup probe does not discard ownership. Restore connectivity
and use the original client/project to status, stop or remove its job. If remote
removal succeeded but the broker reports a persistence failure, repair the local
storage and obtain a new approval for the exact same job removal. The owned
Missing result completes durable cleanup. An uncertain directory-sync failure
requires restarting the broker to load the authoritative snapshot; unrelated
control requests remain available. Use the recorded mutation operation ID to query an uncertain job start; the
recovery workflow below preserves its identity across broker restart.

Shared job listing requires an agent advertising `job_filter_ids`; an older
agent receives no scoped request and the client reports unsupported_feature.
Upgrade the agent artifact to regain scoped listing. Filtering precedes the
remote limit and totals, so another project's newer jobs cannot hide owned jobs.


## Shared job wait lifecycle

A disconnected wait frontend releases its subscription immediately. The broker
keeps one observation and its transport lease until the remote wait budget ends,
the job becomes terminal or the broker shuts down. Reconnecting with the same
owner, host, wait parameters and explicit deadline joins that active observation.
Different owners never share broker results. Authenticated `status` includes
owner-scoped `shared_waits.observers` and `shared_waits.subscribers`.

SIGTERM cancels observations before draining requests; this does not stop the
detached remote job. Reconnect after restart using the original owner. Durable state-history cursor replay is described below; push streaming remains
unfinished. Modern remote supervisors relay job_stop TERM
to the command group and persist output before exiting, preserving tail-on-exit.
Forced KILL and old supervisors can still lose in-memory output; their existing
stop semantics are retained.


## Durable mutation outcomes

In shared mode, set `RDEV_OPERATION_ID` to a valid unique operation ID before a
CLI mutation when the invoking process must recover after its own crash. MCP
exec/write/job start/stop/rm tools accept `operation_id` per call. Without an
explicit ID, the broker frontend generates one and includes it in transport
errors; persist an explicit ID in the caller before submitting critical work.
IDs are opaque labels, not a place for credentials or command text.

With the original client/project credentials and a `mutation.status` policy grant
(server capability `mutation.read`), query without submitting another mutation:

```sh
RDEV_BROKER_SOCKET=/private/rdevd.sock rdev mutation status op_example_123
```

The broker MCP tool is `rdev_mutation_status` with `operation_id`. Other projects
cannot query the record, even if granted the same operation capability. The
response includes the original operation, host, digests, optional job ID and:

| State | Meaning |
|---|---|
| `prepared` | Intent durable; remote dispatch has not begun |
| `dispatched` | Remote I/O may have started; no durable terminal outcome yet |
| `not_sent` | No dispatch, or a correlated remote pre-admission rejection |
| `completed` | Terminal outcome recorded; `remote_ok` distinguishes success from a handler failure |
| `ambiguous` | Execution may have happened; do not repeat the side effect |

Every recorded ID remains reserved, including rejected and failed operations.
Repeating it returns the recorded state; changing its host/parameters returns an
identity conflict. A fresh approval does not bypass this protection. For an
ambiguous job start, query the returned job ID with the original owner. Matching
remote start identity resolves the intent and permits management of the existing
job. During an outage, ownership is retained. An unresolved start cannot be
removed until identity is verified; an ambiguous generic exec/write requires
application-specific inspection because raw output is not stored in the intent.

The broker's 0600 `.mutations` snapshot is schema 1, bounded to 8 MiB, 8192
records globally and 1024 per owner. Startup rejects malformed/duplicate/null/
future/private-mode violations and nesting over 32; invalid files are preserved.
`prepared` becomes `not_sent`, and `dispatched` becomes `ambiguous` on restart.
A failure before rename leaves the old active snapshot. Uncertain directory sync
fails closed until restart. The audit uses a SHA-256 `operation_ref` to correlate
these records without logging caller-selected IDs or request contents.

Shared job starts require the negotiated `durable_job_start` agent feature. The
remote private `job-start-intents` directory stores fsynced identity tombstones
before any supervisor launches. A new agent may recover matching metadata but
will never start a missing or removed replay. Job removal and GC retain these
tombstones. The same global/per-principal count bounds apply. These limits fail
closed; automatic retirement is not implemented. Do not delete the broker
snapshot or remote tombstones to reset the limits: that would discard replay
protection. Identity retirement remains a Phase5 operational gap.

`make remote-mutation RDEV_SSH_CONFIG=/path/to/ssh/config` exercises real SSH,
SIGKILL before broker acknowledgment, SSH-outage restart, job identity recovery,
append-once proof, rename failure before dispatch, actual CLI/MCP processes,
owner isolation, definitive rejection cleanup and SIGTERM with a held remote
terminal response. The test intercepts only selected responses in an OpenSSH
wrapper; remote commands and agent behavior are production code. It proves
process-crash semantics, not storage survival after a remote machine power loss.

On shutdown, the broker uses up to five seconds (half the remaining shutdown
budget) for graceful requests, then cancels scheduled work, closes transports
and waits for durable outcome publication within the original ten-second
context. A held remote response becomes ambiguous; no shutdown path replays it.
An uninterruptible filesystem operation and physical power-loss durability still
require broader failure testing.


## Job state history and replay

Grant `job.events` with the server-selected `job` capability for each allowed
host. The authenticated owner can query its retained history even after removing
the corresponding remote job. Other projects receive the same unknown/expired
error as any unavailable owned history.

```sh
rdev job events dev job_ID -limit 1
rdev job events dev job_ID -stream STREAM_FROM_CURSOR -after SEQUENCE -limit 64
```

These commands require shared broker mode (`RDEV_BROKER_SOCKET`) and the original
principal credentials. MCP exposes `rdev_job_events` with `host`, `id`, optional
`cursor` (`stream`, `sequence`) and `limit`. Results have `events`, the next
`cursor`, `more` and `truncated`. Use the returned cursor for the next page.
A changed stream or evicted prefix sets `truncated`; fully expired histories
return an explicit error. A cursor ahead of available history is rejected.

Events contain job ID, host, state, PID, exit code, observation time and a hashed
operation reference. They exclude command text, cwd, labels, environment and raw
output. Repeated identical observations are coalesced; a late running reply does
not reverse a terminal state. They describe observed state changes, not every
remote process transition. Retrieve owned output through job logs.

The private `.events` schema-1 snapshot beside the daemon socket retains at most
8192 events globally, 1024 per owner and 64 per job, within 16 MiB. Oldest events
are evicted under those budgets. A completely evicted history receives a new
stream ID when observed again, making an old cursor's gap visible. The snapshot
is fsynced and atomically replaced before publishing observations. Malformed,
null, duplicate, future-schema and non-private snapshots fail startup without
being overwritten. Late directory-sync uncertainty requires restart.

A shared wait's worker records its terminal observation even if all local
subscribers disconnect. A disk failure returns an error and preserves the old
visible history; after repairing storage, an owned status call can record the
terminal state exactly once. Daemon shutdown/crash never replays the job command
to repair history. The in-memory watch cache retains only completion hints, with
1024 keys, 512 subscriptions, a 1024-byte key bound and a 64 KiB event bound.
Detailed replay uses the durable history query.

`make remote-events RDEV_SSH_CONFIG=/path/to/ssh/config` runs twenty actual wait
frontends, kills all of them, observes terminal persistence without subscribers,
SIGKILLs/restarts the daemon, queries cursors through actual CLI/MCP processes,
checks project isolation after job removal, injects a real rename failure and
repairs the history by an owned status call. Push streaming and extended remote
retention pressure remain outstanding; passing this test does not complete
the entire Phase5 gate.

## Frontend ingress and slow readers

The listener admits at most 128 sockets, including at most 16 unauthenticated
handshakes. Admission happens before spawning the connection worker. An
unauthenticated handshake has a fixed five-second deadline and a 16 KiB encoded
JSON limit. Successful authentication binds the socket to the exact client and
project and enforces a 32-connection owner limit. Token expiry and rotation still
close the session. Anonymous pressure cannot close existing authenticated
sessions; continuous anonymous connection flooding can still delay new admission.

Request JSON is bounded to the wire absolute request limit (8 MiB) plus 16 KiB
for the broker envelope. A partial document must complete within five seconds.
Concatenated and pretty JSON, and pipelined hello plus request, remain supported.
Idle authenticated connections do not expire because of JSON delimiter whitespace.
A connection may queue two decoded requests. Queue overflow closes that frontend
and cancels its active connection-owned work. Clients should normally send one
request and read its response before sending the next.

Encoded request bytes, including reserved read capacity, are bounded to 64 MiB
across the daemon and 32 MiB per exact owner. Bytes stay charged through handling
and response writing. A newly shared wait observer has its own additional charge,
retained after the initiating socket and every subscriber disconnect; followers
share that observer charge. Completion releases it. `status` requires an explicit
policy grant and returns only the caller's `ingress.connections`,
`reserved_request_bytes` and `observation_request_bytes`. These are resource
accounting values, not RSS or remote payload lane measurements.

Every response write has a two-second deadline capped by principal expiry.
A stalled reader is closed and its connection work is canceled. This bounds
socket write retention; the broader response-allocation, persistence-stall,
mixed-workload and host-capacity matrix remains under the Phase5 acceptance gate.
`make remote-ingress` exercises real sockets and SSH with owner connection/byte
pressure, anonymous/partial JSON, slow output and a held remote response.


## Shared CLI routing and resource status

Setting `RDEV_BROKER_SOCKET` selects the authenticated broker path for the whole
CLI invocation. `ping`, `exec`, `read`, `ls`, `write`, `capability`, supported `job`
commands, mutation queries and `serve` use daemon-owned state and transports.
Unsupported shared commands fail before constructing a standalone client. The
remaining shared sync, secret, host/session administration and state workflows
are still incomplete; they no longer silently bypass broker policy. Local help,
version and static support metadata remain available.

With a `status` grant, `rdev broker status` and MCP `rdev_broker_status` return the
principal's ingress usage, detached observation bytes, scheduler quotas and lane
counts, queue timing, bulk payload bytes, wait subscribers and the policy digest.
Other owners' connection counts and identities are excluded. Pool lifecycle and
eviction-reason projection remain open acceptance work.

`rdev ls HOST [PATH] [-limit N]` and MCP `rdev_list` require the broker's exact-host
`list` decision. They return the remote listing, including truncation/cursor and
operation metadata, through the shared agent. `make remote-frontends` checks
actual CLI/MCP processes, default-deny and cross-project status, directory
listing, and absence of SSH/rsync fallback or local registry writes for
unsupported shared commands.
