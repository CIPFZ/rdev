#!/usr/bin/env bash
set -euo pipefail
rdev_go=${1:?Go binary required}
rdev_old_ref=${RDEV_PREDECESSOR_COMMIT:-08ff4fb}
rdev_old_commit=$(git rev-parse --verify "${rdev_old_ref}^{commit}")
rdev_old_dir=$(mktemp -d "${TMPDIR:-/tmp}/rdev-audit-predecessor.XXXXXX")
trap 'rm -rf "$rdev_old_dir"' EXIT
git archive "$rdev_old_commit" | tar -xf - -C "$rdev_old_dir"
(
 cd "$rdev_old_dir"
 "$rdev_go" build -trimpath -o "$rdev_old_dir/rdevd" ./cmd/rdevd
)
printf 'audit predecessor commit: %s\n' "$rdev_old_commit"
if command -v sha256sum >/dev/null 2>&1; then
 sha256sum "$rdev_old_dir/rdevd"
else
 shasum -a 256 "$rdev_old_dir/rdevd"
fi
RDEV_TEST_PREDECESSOR_DAEMON_BINARY="$rdev_old_dir/rdevd" "$rdev_go" test ./cmd/rdevd -run '^TestRemoteBrokerAuditPredecessorRecovery$' -count=3 -timeout=3m -v
