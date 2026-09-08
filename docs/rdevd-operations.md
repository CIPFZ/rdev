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
{"max_hosts":12,"idle_ttl":300000000000,"owner_weights":{"agent-a\u0000project-a":2}}
```

SIGHUP parses and validates config and key before applying them. Invalid input
retains the running configuration and key; restoring valid files and sending
SIGHUP retries reload. Deleting a previously loaded config is a reload failure.
Review of the existing quota/connection meaning of `max_hosts` remains open;
do not interpret it as proof of the Phase5 host-capacity gate.

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
