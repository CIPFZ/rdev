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
