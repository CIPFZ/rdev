#!/bin/sh
set -eu

remote=${RDEV_REMOTE_SSH:-service-deploy}
go=${RDEV_GO:-go}
local_tmp=$(mktemp -d "${TMPDIR:-/tmp}/rdev-remote-smoke.XXXXXX")
remote_dir="/tmp/rdev-remote-smoke-${USER:-user}-$$"
cleanup() {
	ssh "$remote" "if [ -n \"\${rdev_pid:-}\" ]; then kill -TERM \"\$rdev_pid\" 2>/dev/null || true; fi; rm -rf '$remote_dir'" >/dev/null 2>&1 || true
	rm -rf "$local_tmp"
}
trap cleanup EXIT INT TERM

GOOS=linux GOARCH=amd64 CGO_ENABLED=0 "$go" build -trimpath -o "$local_tmp/rdevd" ./cmd/rdevd
ssh "$remote" "mkdir -p '$remote_dir' && chmod 700 '$remote_dir'"
scp -q "$local_tmp/rdevd" "$remote:$remote_dir/rdevd"
ssh "$remote" "chmod 700 '$remote_dir/rdevd'; '$remote_dir/rdevd' -socket '$remote_dir/rdevd.sock' -ready-file '$remote_dir/ready' -agent-dir '$remote_dir/agents' >'$remote_dir/log' 2>&1 & rdev_pid=\$!; echo \$rdev_pid >'$remote_dir/pid'; for i in \$(seq 1 100); do test -f '$remote_dir/ready' && break; sleep .1; done; test -f '$remote_dir/ready'; test \"\$(stat -c '%a' '$remote_dir' 2>/dev/null || stat -f '%Lp' '$remote_dir')\" = 700; test \"\$(stat -c '%a' '$remote_dir/rdevd.sock' 2>/dev/null || stat -f '%Lp' '$remote_dir/rdevd.sock')\" = 600; kill -TERM \$rdev_pid; for i in \$(seq 1 100); do kill -0 \$rdev_pid 2>/dev/null || break; sleep .1; done; wait \$rdev_pid; test ! -e '$remote_dir/ready'; test ! -e '$remote_dir/rdevd.sock'; echo remote-rdevd-smoke:ok"
