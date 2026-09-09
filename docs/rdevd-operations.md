# Operating the shared broker

Shared `rdevd` requires principal credentials by default. The administrator owns
the signing key; each client receives only a short-lived bearer token for its
exact `(client_id, project_id)`. The token authenticates identity, while the
policy still defaults to deny. Sharing the signing key with clients defeats
this boundary. Processes with access to the same OS account's files are not
sandboxed by this mechanism.

Build the daemon with Go 1.25.0 and build the agents using `make agents`.
Install `rdevd` in `~/bin` and the four agent binaries in
`~/.local/share/rdev/agents`; select the toolchain with
`make GO=/absolute/path/to/go`.

Validation commands and results live in the [runtime index](phase5-runtime-evidence.md);
current completion gaps live in the [acceptance record](phase5-acceptance.md).

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
{"max_hosts":128,"max_warm_hosts":16,"warm_idle_ttl":300000000000,"idle_ttl":300000000000,"bulk_idle_ttl":30000000000,"qos":{"max_active":12,"per_host":11,"per_owner":4,"max_queued":256,"per_owner_queued":32,"bulk_bytes_per_second":8388608},"owner_weights":{"agent-a\u0000project-a":2}}
```

SIGHUP parses and validates config and key before applying them. Invalid input
retains the running configuration and key; restoring valid files and sending
SIGHUP retries reload. Deleting a previously loaded config is a reload failure.
`qos` controls execution and queue capacity separately from host count. Zero or
omitted QoS fields use the values above. `max_hosts` currently bounds distinct
hosts with active broker work; it no longer changes the per-host handler limit.
`max_warm_hosts` separately limits reserved host slots (default 16, range 1–1024).
Setup and detached cleanup still occupy slots. `warm_idle_ttl` defaults to five
minutes and reaps idle hosts even while frontend sockets remain connected. The
existing `idle_ttl` controls final-client grace. Both use the five-second sweep;
in-flight requests retain their host leases. Detached shared observations retain
the broker lifecycle lease and reacquire a host lease for each bounded poll.

Cold-host requests stay in the weighted scheduler queue until a slot is available;
they do not occupy lane workers. Idle entries are retired by LRU. Eligible cold
waiters stop new warm exec/bulk admission while active work finishes. Existing
work retains one control request so job stop remains possible; overlapping hot
control requests cannot keep an otherwise idle host busy indefinitely. The active
`max_hosts` limit hands slots to queued hosts independently of the warm-pool cap.
A lower
live capacity drains idle entries first; existing active entries may temporarily
exceed the new limit until their leases end. No new cold slot is allocated above
the limit. This bounds retained logical hosts, not independent remote machines or
concurrent bootstrap dials; the broader multi-host recovery/backoff matrix remains
open.

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
flush/recovery failed or a durable continuity gap is known. The administrator-only `audit.health` operation requires
its own policy grant and exposes pending, accepted/written, dropped, rotation,
recovery and error counters. Ordinary owners cannot query those global counts.
Writes continue after a recoverable sink failure, while historical errors remain
visible. Closed/overloaded sinks count dropped events instead of blocking RPCs.

Audit shutdown uses the remaining time in the daemon's shared ten-second drain
budget. Policy/job updates are already durable before acknowledgment and are not
saved redundantly after drain. A standalone `AuditLog.Close` still has its own
five-second default. SIGKILL can lose records still in memory (normally the last
25 ms; longer under sink failure). A fixed private `.audit.continuity` marker is
published before audit admission or torn-tail repair, and sealed only after the
writer has flushed and closed both segments. Its schema-1 JSON is bounded to
1 KiB and contains only active/incomplete state, an unclean-recovery count and
SHA-256 digests of the sealed segments. It contains no principal names or payloads.

A marker left active, an older writer changing sealed segments, migration from
segments without a marker, or detected corruption makes completeness explicitly
unknown. This flag survives later successful flushes, normal exits and restarts.
`audit.health.incomplete` describes that historical uncertainty independently of
current I/O health; `unclean_recoveries` counts reopened active markers, not the
number of lost events. Ordinary owner queries expose only `audit_incomplete`,
not global recovery counts. A clean seal is not an archive guarantee: query
history and the two retained files still have their documented retention bounds.

Do not delete the marker to hide a gap. It does not reconstruct missing events or
protect against edits by the same OS user. Marker/segment publication failures
leave conservative recovery evidence; malformed, duplicate, null and public
markers are rejected before readiness.

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
segments locally. Rotation, continuity and known limitations are recorded in the
[runtime index](phase5-runtime-evidence.md#41cddb4).

## Host registry and policy

`rdevd -hosts-file /private/hosts.json` loads only the administrator-selected
0600 regular file, with a 4 MiB bound, normal host/security validation, and
strict JSON fields. The object must contain a `hosts` array; duplicate names,
null and unknown fields are rejected. It replaces implicit global/project
registry discovery for that daemon. Host-file changes currently require a
restart; SIGHUP reloads the broker config and principal key.

Broker-mode protocol dispatch requires a version-3 agent to preserve principal
metadata. Direct compatibility clients retain their legacy behavior. The new
optional ping `caller_id` reports only the current request's opaque protocol
identity; older ping clients can ignore it.

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

## Fleet inventory and durable plans

Fleet uses the same daemon, policy, mutation ledger, job registry, scheduler and
resource accounting as single-host requests. First-version operation allowlist:
`job_start` only, with explicit `job.spec.argv`, cwd, env and login_shell plus
`job.resources` and label. Secret references are unsupported; host session cwd/env are not inherited.
New job wall time defaults to 3600 seconds and requires the negotiated resource
feature. Other operations fail closed. No broker means no Fleet execution.

[Inventory JSON Schema](schemas/fleet-inventory.schema.json) and
[FleetSpec JSON Schema](schemas/fleet-spec.schema.json) describe input shapes;
the broker additionally enforces identity, permission and cross-field rules.
Inventory is schema 1, revisioned private metadata referring to the trusted
administrator host registry; it contains no second authoritative SSH config.
Start with `fleet inventory-list`, then `fleet inventory-import -revision N`.
Import groups aliases of the same canonical connection/session identity and
preserves existing IDs and labels. To edit labels or alias membership, save the
complete snapshot, edit its records, then `fleet inventory-update -file FILE`.
The request revision is a CAS; successful updates increment it. A new record has
an empty `host_id` and a broker-generated random 128-bit ID. Retired IDs are
server-maintained tombstones: deleting/recreating a host cannot reuse its ID.
The snapshot binds the global registry destination, port, remote_dir and session
configuration. External OpenSSH configuration, DNS, ProxyCommand and PATH wrappers
remain trusted administrator environment; Fleet does not detect their changes or
claim a physical machine identity.
Changing an alias name retains identity; changing connection/session identity
requires a new ID and new host grants. Host-file edits still require daemon
restart and an explicit inventory import/update. Project host files cannot
expand inventory. Inventory management uses explicit `fleet.inventory.import`,
`fleet.inventory.update` and `fleet.inventory.list` grants (the `broker.admin`
capability), separately from ordinary Fleet use.

Inventory limits are 1024 records, 16 aliases and 32 labels per host. Label keys
are at most 63 bytes and values 128 bytes, case-sensitive ASCII letters/digits
with `._-/` allowed after the first character; case-fold collisions such as
`env` and `Env` on the same host are rejected. Labels are descriptive data,
including any owner label, and never confer permission. Inventory schema,
unknown/duplicate fields, retired IDs, alias collisions, duplicate identities
and partial updates are validated before publication. The stored snapshot has
deterministic HostID, alias and tombstone sorting.

Selectors are exactly `all`, `id=ID[,ID]`, `alias=NAME[,NAME]`, or
`label:KEY=VALUE[&KEY=VALUE]` (AND predicates). Whitespace, empty selectors,
unknown syntax and zero authorized matches fail; no error becomes `all`.
Explicit names outside the caller's discoverable inventory fail without exposing
other targets. Results sort and deduplicate by HostID. A plan supports at most
128 targets; more than 20 is a large plan. `fleet plan -file FILE` persists the
preview snapshot; inspect its operation, digest and every `results` page before
approving. Every plan requires explicit approval, covering all/large/destructive
work without a client-controlled risk bypass.
Each preview creates a new plan; after a lost preview response, use `fleet list`
to recover the caller's recorded plan before creating another. Repeated or
concurrent execute calls for a running or terminal plan with the same digest
return its current state without dispatching another attempt. A paused plan
requires resume with valid consumed approval, or fresh approval followed by execute.

Use the same principal with an explicit `fleet.approve` grant to call
`fleet approve PLAN -digest SHA -ttl SEC` (default 60, at most 600), then
`fleet execute PLAN -digest SHA -approval TOKEN`. Fleet approvals are distinct
from single-host approvals. They bind the plan, principal, target connection
identity, operation and rollout/failure policy plus policy version and expiry.
The unscoped `fleet.plan` grant admits plan/status/results/list;
`fleet.execute` also controls pause/resume/cancel/retry/reconcile. Each selected
HostID additionally requires `job_start` and `job_status` permission. Exact
HostID grants constrain those inner operations; global grants also apply, while
alias-only grants do not transfer to Fleet.
Discovery evaluates all required grants from one policy snapshot.
New dispatch rechecks grants and target
identity. Changed connection configuration fails closed; selectors are never
re-resolved during execution or retry.

Rollout defaults: `waves`, max_parallel 4 (1..16), wave_size 10 (1..128),
canary 1 for `canary`, and pause_between_waves_sec 0 (0..3600). Numeric zero
selects the documented default where applicable; negative/overflow and conflicting
strategy parameters fail. `all_at_once` rejects canary/wave/pause settings.
Parallelism counts whole HostRuns until job completion and remains subject to
broker global/owner/host/lane limits. Detached Fleet reservations use at most
half of each non-control QoS envelope (default global 5, owner 1, host 4);
The global lifetime limit is also capped by the exec lane limit (8); each owner
can hold at most global-minus-one slots when global capacity exceeds one.
Owner/plan round-robin admission prevents a large plan monopolizing released slots.
`max_parallel` is an upper bound and does not override that reservation.
Canary must completely succeed before any
later wave; ambiguous, unreachable and failed canaries never pass. Admission is
the durable transition to `dispatching`. Pause stops new admission and preserves
already admitted starts and in-flight jobs; wave boundaries and next-wave times
are durable. Resume preserves successful targets; expired authorization needs
new approval and execute. Previously admitted jobs may finish after expiry.

`max_failures` and `max_failure_ratio` omitted means disabled; explicit zero
allows no failure. A threshold triggers when the observed value is strictly
**greater than** the configured maximum, after each result and before another
dispatch. Failed, unreachable and ambiguous count as failures; the ratio's
denominator is terminal attempted success/failed/unreachable/ambiguous only.
Skipped and canceled are excluded. `on_threshold` is `pause` (default) or
`cancel_remaining`; submitted jobs retain their evidence and keep running.
Batch cancel cancels pending targets and queued/submitting starts that have not
committed. Proven unsent cancellation is canceled; unconfirmed submission is
ambiguous. Submitted detached jobs continue. Every ambiguous result retains its
lifetime reservation and pauses new dispatch until reconciliation. A separate job stop
uses normal job permissions and approval; canceled observers never cancel plans.

Plans and HostRuns persist intent before dispatch. A HostRun's attempt number
and operation ID stay fixed through restart and reconciliation. Recovery queries
existing mutation/job results; a missing/failed query never licenses replay.
Unprovable submissions remain ambiguous. `fleet reconcile PLAN` queries those
original attempts. `fleet retry PLAN HOST_ID...` creates one child plan only for
an explicit terminal failed/unreachable/skipped/canceled subset; success and
ambiguous are rejected, concurrent retries cannot dispatch the same attempt
twice, and the child needs its own approval. Retry does not reparse selectors.

Status/results pages contain at most 32 HostRuns; list pages contain bounded
plan metadata. `next_offset` indicates another page. CLI status/results return
`exit_status`: 0 completed with all success, 1 failed/canceled, 2 unfinished or
ambiguous; control commands return 0 for admission. MCP `rdev_fleet` takes
`{action, request, approval_token?}` with the same request model and returns
`{plan?, plans?, approval?, inventory?}`. Raw argv/env execution data remains in
private state, while audit records only identity/digest/decision/result links.
Results contain metadata, not raw job logs. Fleet storage is bounded to 64 plans,
8 per owner and 16 MiB. New plans reserve 4096 bytes per plan plus 2048
per HostRun for later approval/result/cancel metadata; control and recovery can
use that reserve. Terminal history is eligible after seven days; a retry
chain is reclaimed together only after every related plan is terminal and past
that horizon. Active, ambiguous and still-needed recovery records are retained. Storage pressure rejects new plans; recorded queries and
reconciliation remain available. Disk errors freeze new admission until storage
health is repaired and the daemon restarted.

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

## Detached job ownership recovery

Forward upgrades from daemon/agent pairs `e3ac9c2` and `c5707ee` have runtime
validation. Existing jobs keep their original supervisor executable; upgrading
the base agent does not replace those processes. TERM to a pre-relay supervisor's
job stops its child group and lets the supervisor flush output. A configured
grace deadline still escalates unresponsive jobs to KILL.

These checks cover acknowledged jobs and the named predecessor schemas; they do
not make untested downgrades or arbitrary state schemas supported. Keep the local
ownership/mutation files and remote job records through an upgrade.

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
keeps one observation per owner/host/job until the latest accepted observation
budget ends, the job becomes terminal or the broker shuts down. Subscribers share
that observation while keeping their own timeout, deadline, tail length, batch
order and wait-any condition. Short remote polls yield the execution quota between
polls; bounded response snapshots remain charged to their owner. Pool pressure
can evict an idle transport between polls; the logical observation continues via
reconnection without stopping the detached job. Different
owners never share broker results. Authenticated `status` includes
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
socket write retention; the remaining validation gaps are tracked in the acceptance record.
`make remote-ingress` exercises real sockets and SSH with owner connection/byte
pressure, anonymous/partial JSON, slow output and a held remote response.

## Shared CLI routing and resource status

Setting `RDEV_BROKER_SOCKET` selects the authenticated broker path for the whole
CLI invocation. `ping`, `exec`, `read`, `ls`, `write`, `capability`, supported `job`
commands, `secret`, `sync`, mutation queries and `serve` use daemon-owned state
and transports. Unsupported shared commands fail before constructing a standalone
client. Shared host/session editing and declarative secret delegation remain unsupported.
`state inspect|migrate|repair` and MCP `rdev_state` use the existing administrative
routes. State reports cover the entire host state root, including other owners'
record paths: grant these operations only to administrators. Migration and repair,
including previews, require exact-request approval. Hosts come from the administrator's private registry at
daemon startup; change that registry and restart to update hosts. Supply cwd/env
explicitly per request. CLI `exec` and `job start` accept repeated `-env K=V` with distinct keys; MCP
accepts an `env` object. Use principal-owned `secret set` or `set_from_file` plus
`secret:NAME` and a `secret.use` grant instead of declarative delegation.
`rdev support` and MCP `rdev_support` list these boundaries and alternatives.
Local help, version, `compat` and static support remain available without a socket.

With a `status` grant, `rdev broker status` and MCP `rdev_broker_status` return the
principal's ingress usage, detached observation bytes, scheduler quotas and lane
counts, queue timing, bulk payload bytes, wait subscribers and the policy digest.
Other owners' connection counts and identities are excluded. Pool lifecycle and
connection setup/retry diagnostics are described below.

`rdev ls HOST [PATH] [-limit N]` and MCP `rdev_list` require the broker's exact-host
`list` decision. They return the remote listing, including truncation/cursor and
operation metadata, through the shared agent. `make remote-frontends` checks
actual CLI/MCP processes, default-deny and cross-project status, directory
listing, and absence of SSH/rsync fallback or local registry writes for
unsupported shared commands.

## Administrative warm pool health

`rdev broker status --pool` and MCP `rdev_broker_pool` require a separate
`pool.health` grant. They return global reserved/active/closing host counts,
active host leases, pending host requests, current retained base/bulk transport
objects, detached bulk cleanup count and fixed eviction-reason counters with last
idle/lifetime/drain durations. Counts are snapshots; a detached transport is no
longer in the retained-object count but its host slot stays reserved through
cleanup. `queued` includes scheduler-queued host work, not only capacity waits.

Ordinary `status` remains owner-scoped and never includes global pool data. Neither
pool projection contains host names, owner names, commands, paths, secret values
or output. Global `pool.health` and `audit.health` reject host-scoped/wire request
envelopes; an exact-host health grant cannot expose global information.

Warm reasons include `capacity_lru`, `capacity_reload`, `idle_ttl`,
`last_client` and `shutdown`. A blocked detached bulk close keeps
its host slot reserved, with at most one such closer per host; it runs outside
the daemon signal/reload loop. Shutdown waits within its existing deadline.

## Broker route and administrative host boundaries

Every authorized request must match an implemented local handler or a registered
remote operation with a matching `wire.op` and nonempty host. Policy denial runs
first; absent handlers and malformed envelopes then fail before approval use,
state mutation or transport admission. Granting an unimplemented operation does
not make it available. Online session administration remains unsupported; shared sync and secret file import are
documented below.

Local `status`, `pool.health`, `audit.health` and `audit_query` require an empty
host and no wire envelope, because these queries do not filter their data by
host. `job.events` and `mutation.status` support a host filter. Mutation queries
return the same error for absent, other-project and out-of-host records.

A host-scoped `policy.grant` request must set both `host` and `grant_host` to the
same alias; it can grant/revoke within that host but cannot change global or
another host's grants. A host-scoped `approval.create` request must bind its outer
host to `approval_spec.host`. Administrators with explicit global grants can use
an empty outer host. Administrative requests cannot include an outer wire frame.
These checks do not replace the target principal's operation policy or exact
request/target/policy approval binding.

## Owner-scoped protocol lane traffic

CLI `rdev broker status` and MCP `rdev_broker_status` include
`scheduler.traffic.control`, `.exec` and `.bulk`, each with `sent_bytes` and
`received_bytes`. These are application NDJSON bytes, including JSON/base64
encoding and one LF delimiter, measured before secret redaction. Stream data and
the retained terminal payload each count when actually transmitted. They are not
SSH/TCP ciphertext sizes or unique file/output payload sizes.

Writes count only bytes accepted by the underlying pipe, including partial writes
and completion after caller cancellation. Rejected frames count zero. Received
frames are charged through their validated request-ID association; canceled
streams and automatic cancel requests retain the original owner/lane meter while
they drain. Retry attempts accumulate in the same meter. Counters never change
another project's traffic. The hot transport path uses atomic counters and does
not acquire the scheduler or audit lock.

A parseable frame associated with a known request is counted even if later
semantic validation rejects it. Undecodable/unattributable frames, bootstrap
uploads/handshakes, SSH overhead and
broker-internal startup recovery are outside these authenticated request counters.
Counters are snapshots of bounded recent owner history; they reset on daemon
restart or idle-history eviction and are not durable billing records. CRLF input
is normalized to the protocol's LF delimiter. The existing `bulk_payload_bytes`
field and pacing behavior retain their prior semantics.

## Correlating broker request outcomes

After successful owner binding, broker responses include a server-generated
`request_ref`. Audit events for that request use the same opaque reference for
its policy decision, approval use, admission and outcome. A client may reuse its
own request ID without conflating traces. The reference does not contain or hash
client request text, payloads, secret names or output. Existing clients may ignore
this additive response field; older audit entries can lack it.

Local pool/audit health, mutation outcome and audit queries now record their
results, as do authorized requests rejected before dispatch for invalid wire
ownership, mutation identity, job scope/removal or approval specifications.
`mutation.status` uses the same hashed operation reference as its original wire
mutation. Policy snapshot, approval, request and target digests remain associated
with the individual request chain. Queries continue to filter by exact principal
and project. Identity/ingress failures rejected before owner binding are outside
this authenticated request trace.

Ordinary reads, denied requests and policy changes carry request/target HMACs
under a private per-broker-instance key (`digest_scope: broker_instance`). These
digests are comparable only within that instance and do not expose low-entropy
request values to offline guessing. `target_scope` distinguishes the submitted
target from an authorized configured snapshot. Denied requests do not resolve or
dial hosts. Approved operations retain their approval-bound digests and references
(`digest_scope` and `target_scope`: `approval`).

`audit_query` records its own query before its durability barrier. Its response
can therefore contain that event. Audit health polling also generates an event,
so accepted/written/pending counts are live snapshots. Request acknowledgment does
not synchronously fsync audit: a crash can still lose the asynchronous tail, and
persistent `audit_incomplete` remains the signal for that uncertainty.

## Connection setup and retry diagnostics

Owner status includes `scheduler.connection`: dial attempts/successes/currently
in-flight count, total/max dial duration in nanoseconds, application retry
attempts, and a fixed `dial_failures` map. CLI and MCP share this projection.
Stages are `validation`, `control_path`, `probe`, `agent_lookup`, `agent_install`,
`agent_start`, `handshake`, `negotiation` and `canceled`. Cancellation takes
precedence over the stage where teardown occurred. Durations include cleanup
before a dial returns. Stages identify where setup failed, not the underlying
network/OS cause, and never use error strings as labels.

The principal that actually initiates a base/bulk dial owns its counts. A second
project reusing that connection has zero new dials. An application retry is
counted when the client enters its next attempt, even if that attempt then fails
before writing an application frame. Bootstrap SSH subprocesses are phases of a
single dial, not separate attempts. These counters share the scheduler's bounded
recent-owner history and reset on restart/history eviction; concurrent snapshots
can observe individual counters at slightly different instants.

## Shared principal-owned credentials

In shared mode, `rdev secret set HOST NAME < value`, `rdev secret list HOST`, and
`rdev secret delete HOST NAME` use `rdevd`. MCP `rdev_secrets` accepts `action`
(`set`, `list`, `delete`), `host`, `name`, `value`, `approval_token` and
`operation_id`. Values are never returned. CLI stdin is bounded at 64 KiB and
preserves all bytes, including a final newline. Names use ASCII letters, digits,
period, underscore and hyphen, with a 128-byte limit. Values must be valid UTF-8,
6–65536 bytes, without NUL. This route does not read arbitrary daemon-local files.

Each active value belongs to an exact authenticated client/project, configured
host and host/session snapshot. There is no fallback to another project, another
host, or the daemon's legacy declarative secret Store. `secret.list` returns only
this principal's names and opaque versions for that host. `secret.set`,
`secret.delete` and `secret.list` are separately grantable operations in capability
`secret`; exact-host grants are supported. An execution grant alone cannot use
credentials: exec/job requests containing `Env: {"TOKEN":"secret:token"}` also
require `secret.use` for that owner and host in the same policy snapshot. The
initial scope grants use of that principal's own keys on the selected host; it
does not delegate another principal's key.

Every set/delete needs an administrator-issued approval whose request file uses
`secret: {"name":"token","value":"..."}` instead of `wire`. Delete omits the
value. The approval binds principal, operation, target, submitted value and prior
version. Reference-bearing exec/job approvals bind the original request and
random credential versions. These sensitive request digests use a private HMAC
key so audit metadata does not provide an offline credential-guessing oracle.
Rotation before token consumption invalidates that approval; after consumption,
the approved snapshot is frozen through queueing/dispatch. Approval caches retain
digests only. Expansion is capped at 1 MiB and is additionally charged to that
frontend's ingress budget until its request ends.

The daemon persists credentials in `SOCKET.secrets` with mode 0600. This file
contains plaintext values and a private digest key; protect and back it up with
the rest of the broker's private state. Set/delete use durable mutation intents
and support `RDEV_OPERATION_ID`, MCP `operation_id` and `mutation.status` just as
wire mutations do. A repeated ID never silently rotates or reactivates a key.
Atomic file replacement and directory sync precede acknowledgment. Any uncertain
write disables secret resolution until restart. A missing credentials file after
a recorded secret mutation blocks READY, as do malformed/null/duplicate/public/
symlink snapshots. Restore the matching private state before restarting.

Rotation/delete retire injection authority while retaining prior values solely
for redaction. This protects a detached job's historical output across daemon
crashes. Replacement markers contain random version IDs, without other owners'
secret names. Startup durably retires bindings whose configured target changed;
returning to the earlier host definition does not reactivate those bindings.
The archive rejects new versions at 256 versions / 1 MiB per owner and
4096 versions / 16 MiB globally; serialized state is capped at 20 MiB. It never
automatically drops old output protection. Automatic archive retirement and declarative delegation are not implemented;
capacity exhaustion rejects new versions while preserving historical redaction.

## Import a shared credential from a remote file

`rdev secret set_from_file HOST NAME REMOTE_PATH` and MCP `rdev_secrets` with
`action: "set_from_file"` read the selected remote file through the shared bulk
connection. The submitted parameters include a name and path, without a value.
Both `secret.set_from_file` (capability `secret`) and `read_file` (capability
`file.read`) must be authorized for that principal and host in one policy
snapshot. Set `RDEV_APPROVAL_TOKEN` for CLI, or `approval_token` for MCP.

The administrator's approval request uses operation `secret.set_from_file` and
`secret: {"name":"token","path":"~/credential"}`. Approval creation and token
consumption each read a bounded source snapshot with the target principal's
remote identity. The HMAC binds the path, validated value, prior credential
version, owner and host/session target. Changing the path or effective value
invalidates the approval without consuming the original token. After successful
consumption the selected value is frozen before mutation admission. Source reads
are separately scheduled/accounted as bulk work, including decoded payload bytes
that never enter a frontend response, and respond to frontend
cancellation; they do not close the shared base transport.

Reads request at most 64 KiB plus one byte, reject missing, incomplete, oversized,
binary, invalid UTF-8 or short values, and trim surrounding whitespace as the
existing remote-secret loader does. Repeated import reads the raw validated
source even when that value is already registered for output redaction. The value
is never returned to CLI/MCP and never enters audit or mutation records; successful
imports persist in the same private credential archive as inline registrations.
A failed or canceled probe leaves the registry unchanged. As with inline set,
`RDEV_OPERATION_ID`/MCP `operation_id` support crash-outcome queries without
silently repeating an import.

## Shared sync

With `RDEV_BROKER_SOCKET` set, `rdev sync HOST push LOCAL REMOTE -prepare`
retains source content and returns a five-minute `plan_id`, digests and the exact
path/type/content/metadata changes for review. `-prepare` implies `-dry-run`.
The CLI resolves LOCAL against its working directory; MCP `rdev_sync` requires
an absolute local path and uses `prepare: true`. Plain `-dry-run` remains a
disposable rsync preview and cannot authorize execution.

The principal needs exact-host `sync.push` or `sync.pull` authority. `-delete`
additionally requires `sync.delete`, including during preview. An administrator
issues `approval.create` with `approval_spec.sync` containing the execution
options and `plan_id`, with `prepare`/`dry_run` false. Mutating deletes also need
`confirm_delete: true`. The execution keeps the same paths, direction, exclusion,
symlink and conflict policies:

```sh
rdev sync HOST push /absolute/source/ /remote/destination/ -prepare
# Review the returned changes and obtain the matching administrator approval.
RDEV_APPROVAL_TOKEN=APPROVAL rdev sync HOST push /absolute/source/ /remote/destination/ -plan PLAN_ID
```

MCP uses `plan_id`, `approval_token` and optional `operation_id`. CLI callers may
set `RDEV_OPERATION_ID`. Success returns `operation_id`; interrupted callers use
`rdev mutation status ID` / `rdev_mutation_status`. A lost result can be resolved
from the recorded remote/local outcome. Neither reconnect nor restart replays a
commit, and another owner cannot retrieve its plan or outcome. Restart invalidates
unused plans and approval tokens.

Preparation retains regular file bytes without hard links. Later source edits do
not alter that retained content. Rsync evaluates exclusions and directory layout
before approval; execution applies only the fixed change list. Excluded targets
and unrelated siblings are preserved; replacing a directory containing an
excluded descendant is rejected during preparation. A trailing slash names a
directory, including a missing destination for a single-file transfer. Root path
operands and exclusion patterns must be valid UTF-8; retained tree entry names
use a byte-preserving format. Directory sources without a trailing slash retain
their basename under existing and newly created destination directories. The
destination snapshot is checked before
business writes, and competing prepared sync operations are serialized. Files
publish by atomic rename; directories are removed only when empty. A multi-file
plan is not one atomic transaction: a later I/O failure can leave partial work.
External writers should be paused during final application; ordinary filesystem
rename cannot provide compare-and-swap against an uncooperative writer.

Prepared operations have a two-minute deadline and a 256 MiB content / 8192-entry /
2 MiB metadata bound for each scanned tree. Capture currently measures the whole
source before exclusion filtering; scope large sources accordingly. Managed
staging has a ten-minute TTL, reclaimed on subsequent admission, with at most
16 stages globally / 4 per owner; memory admission can reduce those counts.
Retained plans reserve 16 MiB through execution until all workers finish, with an
additional 8 MiB during execution. Expiry releases unused plans. Output remains
bounded: a plan too large to review is rejected,
and `-max-output-bytes` may raise the default 256 KiB up to 512 KiB. Outcome
identities are retained separately with fixed caps; automatic safe retirement
remains follow-up work. State and raw chunks are private and never returned in
frontend or audit responses.

Prepared transfers use bounded chunks on the existing bulk transport and charge
its payload/protocol counters. Plain previews use a separate, cancellable rsync
process with a 30-second deadline; their auxiliary rsync traffic is not included
in agent-protocol counters. Shared preview scans allow 8192 entries, 2 MiB of
metadata and 8 GiB of hashed content. `preserve` keeps source links; `follow` is
confined to the source root, and special files are rejected. The Linux daemon,
CLI/MCP, real SSH execution, cancellation and pre-acknowledgment crash paths are
covered by `make remote-sync-execution`; `make remote-mixed-qos` covers concurrent
exec/wait/status/transfers. macOS runtime remains unverified and was deferred by
the user; see [Phase5 acceptance](phase5-acceptance.md).


## Entry contracts and compatibility

All CLI commands reject unknown flags, missing values, invalid/overflowing
numbers, duplicate single-value flags, extra operands and conflicting modes
before business I/O. Both `-key value` and `--key=value` work. `-exclude` repeats;
`-env`/`-secret` repeat only with distinct keys. Use `-key=-value` for a flag value
beginning with `-`. `--` ends flag parsing: `sync HOST push -- -leading-local REMOTE`
works in both modes. For `exec`/`job start`, everything after the required `--`
is the unchanged command argv. The invoking local shell still requires quoting.
A stdin read error fails the entire write, even if the reader also returned data;
the request is not submitted. The existing input cap remains enforced.

Timeouts are seconds and have the same meaning in both CLI and both MCP modes:

| Budget | Omitted or 0 | Positive | Expiry |
|---|---|---|---|
| Foreground `exec -timeout` / `timeout_sec` | 60 | 1–3600 | Terminates only that foreground process group; returns captured output and `timed_out` |
| `job wait -timeout` / `timeout_sec` | 300 | 1–3600 | Ends this observation; the job and other subscribers survive |
| New `job start -wall-timeout` / `resources.wall_timeout_sec` | 3600 | 1–3600 | Supervisor terminates the job group and records `resource_limit=wall_timeout` |

Negative, overflowing, above-limit and explicit infinite values fail. CLI exec
and job wait return nonzero on expiry; MCP returns structured timeout data.
Connection establishment budgets and request context deadlines are separate;
a shorter caller deadline can end an observation or cancel an attached exec,
but never extends a runtime budget or terminates a detached job. Mutation
cancellation retains the existing possibly-executed outcome and never auto-replays.
CLI/MCP job results include requested/effective resources and the limit reason.

This changes old defaults: standalone CLI/client and broker exec used to allow
unbounded runtime, while standalone MCP used 60 seconds. New jobs previously
had no wall timer when it was omitted. Existing positive values within the hard
maximum keep their meaning;
existing running supervisors keep their recorded envelope. A new job requires
negotiated `job_resource_envelope`; an old agent lacking it is rejected before
launch. Update the embedded agent using `make all` and reconnect. This does not
remove access to existing jobs' status/wait/stop operations.

`rdev support [HOST] [-refresh]` / `rdev_support` use one support matrix.
No-host calls are static. With HOST, broker discovery returns only the authenticated
principal's operation decisions; a denied capability probe opens no SSH connection
and does not look up host inventory. `allowed` is authorization, `callable` excludes
supplementary rights such as `secret.use`, and runtime support is separate. A
positive grant still requires resource admission and approval. Probe results omit
profile/environment/path data; a probe is not platform runtime certification.
macOS shared runtime remains unverified and explicitly deferred.

`rdev compat` / `rdev_compat` expose ranges, features, error registry, actual config
fields and persisted schemas from the same constants used by validators. Standalone
client/agent supports protocol 2–3 common operations; broker/agent requires protocol
3 and owner-preserving features; client/broker currently supports only protocol 1.
N/N-1 means these explicit ranges, not arbitrary release combinations. Unknown
error envelopes fail closed rather than becoming retryable. Host config is
unversioned: standalone tolerates unknown JSON fields, administrative registry
loading rejects them. Broker config rejects unknown fields. Legacy agent records
without a schema can migrate forward with backup; unsupported/future schemas
never silently downgrade. Query existing durable outcomes rather than replaying
old mutation IDs with changed runtime semantics. Complete automated upgrade and
rollback matrices remain Phase8.

## Local release gate

`make GO=/path/to/go release-gate` is an executable local check; it publishes
nothing. It selects the `go.mod` toolchain (Go 1.26.8) and pinned govulncheck
v1.8.0 from `scripts/release-tools.env`, downloads and verifies modules, queries
the module proxy for updates/retractions, audits all four source platforms and
six actual binaries, then generates manifest, CycloneDX SBOM and unsigned SLSA
provenance. A finding, unavailable network, invalid audit report or changing source
fails the gate. Default output is `bin/release`; set `RDEV_RELEASE_OUT` to an
external directory. The default requires a clean tree; development dirty builds
must be explicitly labeled with `RDEV_RELEASE_ALLOW_DIRTY=1`.

`make GO=/path/to/go verify-release` rechecks artifact hashes, embedded agents,
Go build information, audit evidence and metadata binding. This is local evidence,
not a hosted CI run or signed release. Phase8 adds separate signing/verification, linked-module notices, channel policy
and remote transactions. Formal release identity and full production acceptance
remain pending; see [Phase8 acceptance](phase8-acceptance.md). [Phase7 acceptance](phase7-acceptance.md) records the current execution;
[Phase6 acceptance](phase6-acceptance.md) retains the prior source-bound evidence.


### Release policy and agent transactions

Use the administrator's private `~/.config/rdev/release-policy.json`, or set
`RDEV_RELEASE_POLICY` in the trusted daemon environment. Shared clients cannot
supply this path or their own trust root. Existing unsigned installations need an
explicit dev opt-in; build stamped artifacts with `make all daemon`. Policy
examples, signing commands, rotation/revocation rules and precise rollback
permissions are maintained in [Phase8 acceptance](phase8-acceptance.md).

The installed helper reconciles `.rdev-upgrade.json` under the permanent
`.rdev-upgrade.lock` before a new upload. A same-digest connect still reconciles
trust/journal state. Four upload slots and one prior known-good binary bound
staging; a dead PID, ten-minute age and exact safe layout are all necessary for
slot reclamation. Unknown/live/reused-PID objects are preserved. If all slots
were abandoned during the very first bootstrap, no installed trusted helper
exists: inspect the private namespace and remove only confirmed incomplete
upload reservations during maintenance, then retry. Do not remove the active
agent, only fallback, journal, state lease or mutation recovery evidence.

Installation health uses ping/features/platform and a read-only state inspection.
The transaction does not kill serving agents or detached jobs. Unconfirmed
switch/rollback or lost transaction reply is ambiguous; query retained state and
reconcile before assuming success. A committed cleanup warning means the new
binary already took effect. Binary rollback is not state rollback. Drain old
writers before migrating into the shared writer-lease regime; a new running job
holds its lease until exit and blocks exclusive migration.
