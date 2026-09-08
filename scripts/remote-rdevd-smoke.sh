#!/bin/sh
set -eu
remote=${RDEV_REMOTE_SSH:-service-deploy}
go=${RDEV_GO:-go}
ssh_config=${RDEV_SSH_CONFIG:-}
rdev_ssh() {
 if [ -n "$ssh_config" ]; then ssh -F "$ssh_config" "$@"; else ssh "$@"; fi
}
rdev_scp() {
 if [ -n "$ssh_config" ]; then scp -F "$ssh_config" "$@"; else scp "$@"; fi
}
local_tmp=$(mktemp -d "${TMPDIR:-/tmp}/rdev-remote-smoke.XXXXXX")
remote_dir=
cleanup() {
 result=$?
 trap - EXIT HUP INT TERM
 if [ -n "$remote_dir" ]; then
  rdev_ssh "$remote" "if test -f '$remote_dir/run/pid'; then p=\$(cat '$remote_dir/run/pid'); if test -r /proc/\$p/cmdline && tr '\\0' '\\n' </proc/\$p/cmdline | head -1 | cmp -s - '$remote_dir/binary-path'; then kill -KILL \"\$p\" 2>/dev/null || true; fi; fi; rm -rf '$remote_dir'" >/dev/null 2>&1 || true
 fi
 rm -rf "$local_tmp"
 exit "$result"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 "$go" build -trimpath -o "$local_tmp/rdevd" ./cmd/rdevd
printf 'remote validation source: %s\n' "$(git describe --always --dirty)"
printf 'rdevd artifact sha256: %s\n' "$(shasum -a 256 "$local_tmp/rdevd" | cut -d ' ' -f 1)"
if [ "${RDEV_REMOTE_RUNTIME:-0}" = 1 ]; then
 GOOS=linux GOARCH=amd64 CGO_ENABLED=0 "$go" test -c -o "$local_tmp/rdevd.test" ./cmd/rdevd
fi
remote_dir=$(rdev_ssh "$remote" 'mktemp -d /tmp/rdev-remote-smoke.XXXXXX')
case "$remote_dir" in /tmp/rdev-remote-smoke.*) ;; *) echo 'invalid remote temporary directory' >&2; remote_dir=; exit 1;; esac
case "$remote_dir" in *[!a-zA-Z0-9/._-]*) echo 'unsafe remote directory' >&2; remote_dir=; exit 1;; esac
rdev_scp -q "$local_tmp/rdevd" scripts/rdevd-smoke-runtime.sh scripts/systemd-rdevd-smoke.sh deploy/systemd/rdevd.service "$remote:$remote_dir/"
rdev_ssh "$remote" "printf '%s\\n' '$remote_dir/rdevd' >'$remote_dir/binary-path'; sh '$remote_dir/rdevd-smoke-runtime.sh' '$remote_dir/rdevd' '$remote_dir/run'"
if [ "${RDEV_REMOTE_RUNTIME:-0}" = 1 ]; then
 rdev_scp -q "$local_tmp/rdevd.test" "$remote:$remote_dir/"
 rdev_ssh "$remote" "RDEV_TEST_DAEMON_BINARY='$remote_dir/rdevd' '$remote_dir/rdevd.test' -test.run='^TestDaemonRuntimeLifecycle$' -test.v -test.timeout=3m"
fi
if [ "${RDEV_REMOTE_SERVICE:-0}" = 1 ]; then
 rdev_ssh "$remote" "sh '$remote_dir/systemd-rdevd-smoke.sh' '$remote_dir/rdevd' '$remote_dir/rdevd.service' '$remote_dir/service'"
fi
