#!/usr/bin/env bash
set -euo pipefail
rdev_go=${1:?Go binary required}
rdev_old_refs=${RDEV_JOB_PREDECESSOR_COMMITS:-e3ac9c2 c5707ee}
rdev_build_dir=$(mktemp -d "${TMPDIR:-/tmp}/rdev-job-predecessor.XXXXXX")
trap 'rm -rf "$rdev_build_dir"' EXIT
for rdev_old_ref in $rdev_old_refs; do
 rdev_old_commit=$(git rev-parse --verify "${rdev_old_ref}^{commit}")
 rdev_old_dir="$rdev_build_dir/$rdev_old_commit"
 mkdir -p "$rdev_old_dir"
 git archive "$rdev_old_commit" | tar -xf - -C "$rdev_old_dir"
 (
  cd "$rdev_old_dir"
  "$rdev_go" build -trimpath -o "$rdev_old_dir/rdevd" ./cmd/rdevd
  mkdir -p "$rdev_old_dir/agents"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 "$rdev_go" build -trimpath -o "$rdev_old_dir/agents/rdev-agent-linux-amd64" ./cmd/rdev-agent
 )
 printf 'job predecessor commit: %s\n' "$rdev_old_commit"
 if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$rdev_old_dir/rdevd" "$rdev_old_dir/agents/rdev-agent-linux-amd64"
 else
  shasum -a 256 "$rdev_old_dir/rdevd" "$rdev_old_dir/agents/rdev-agent-linux-amd64"
 fi
 RDEV_TEST_PREDECESSOR_DAEMON_BINARY="$rdev_old_dir/rdevd" \
 RDEV_TEST_PREDECESSOR_AGENT_DIR="$rdev_old_dir/agents" \
 "$rdev_go" test ./cmd/rdevd -run '^TestRemoteBrokerJobUpgrade$' -count=3 -timeout=3m -v
done
