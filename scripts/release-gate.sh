#!/bin/sh
# Local executable gate only. It creates no hosted release and signs nothing.
set -eu
cd "$(dirname "$0")/.."
. scripts/release-tools.env
RDEV_RELEASE_GO=${RDEV_RELEASE_GO:-go}
RDEV_RELEASE_OUT=${RDEV_RELEASE_OUT:-bin/release}
RDEV_RELEASE_PIN=$(awk '$1 == "toolchain" { print $2 }' go.mod)
test -n "$RDEV_RELEASE_PIN"
export GOTOOLCHAIN="$RDEV_RELEASE_PIN"
RDEV_RELEASE_VERSION=$("$RDEV_RELEASE_GO" env GOVERSION)
test "$RDEV_RELEASE_VERSION" = "$RDEV_RELEASE_PIN"
# Put the selected Go on PATH for govulncheck's go list subprocesses.
PATH=$("$RDEV_RELEASE_GO" env GOROOT)/bin:$PATH
export PATH
mkdir -p "$RDEV_RELEASE_OUT"
RDEV_RELEASE_OUT=$(cd "$RDEV_RELEASE_OUT" && pwd)
RDEV_RELEASE_TOOLS=$(mktemp -d)
trap 'rm -rf "$RDEV_RELEASE_TOOLS"' EXIT HUP INT TERM
"$RDEV_RELEASE_GO" build -trimpath -o "$RDEV_RELEASE_TOOLS/releasecheck" ./scripts/releasecheck
"$RDEV_RELEASE_TOOLS/releasecheck" snapshot > "$RDEV_RELEASE_OUT/source.json"
GOBIN="$RDEV_RELEASE_TOOLS" "$RDEV_RELEASE_GO" install "golang.org/x/vuln/cmd/govulncheck@$RDEV_GOVULNCHECK_VERSION"
"$RDEV_RELEASE_TOOLS/govulncheck" -version > "$RDEV_RELEASE_OUT/tools.txt"
# -u queries the module proxy online; available updates are review information,
# while checksum failure or any known vulnerability is a hard gate failure.
"$RDEV_RELEASE_GO" mod download
"$RDEV_RELEASE_GO" mod verify > "$RDEV_RELEASE_OUT/module-verify.txt"
"$RDEV_RELEASE_GO" list -m -u -json all > "$RDEV_RELEASE_OUT/modules.json"
"$RDEV_RELEASE_TOOLS/releasecheck" modules "$RDEV_RELEASE_OUT/modules.json"
make GO="$RDEV_RELEASE_GO" all daemon
cp bin/rdevd "$RDEV_RELEASE_OUT/rdevd"
cp bin/rdev "$RDEV_RELEASE_OUT/rdev"
for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
    cp "cmd/rdev/agents/rdev-agent-$platform" "$RDEV_RELEASE_OUT/rdev-agent-$platform"
    report="$RDEV_RELEASE_OUT/govulncheck-source-$platform.json"
    GOOS=${platform%-*} GOARCH=${platform#*-} CGO_ENABLED=0 "$RDEV_RELEASE_TOOLS/govulncheck" -json ./... > "$report"
    "$RDEV_RELEASE_TOOLS/releasecheck" audit "$report"
done
for artifact in rdev rdevd rdev-agent-linux-amd64 rdev-agent-linux-arm64 rdev-agent-darwin-amd64 rdev-agent-darwin-arm64; do
    report="$RDEV_RELEASE_OUT/govulncheck-binary-$artifact.json"
    "$RDEV_RELEASE_TOOLS/govulncheck" -mode=binary -json "$RDEV_RELEASE_OUT/$artifact" > "$report"
    "$RDEV_RELEASE_TOOLS/releasecheck" audit "$report"
done
"$RDEV_RELEASE_TOOLS/releasecheck" generate "$RDEV_RELEASE_OUT" "$RDEV_RELEASE_PIN"
printf 'release gate passed: %s (unsigned local artifacts and evidence)\n' "$RDEV_RELEASE_OUT"
