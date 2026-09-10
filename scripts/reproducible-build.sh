#!/bin/sh
# Two clean checkouts with separate compiler caches; no signing or publication.
set -eu
cd "$(dirname "$0")/.."
rdev_repro_go=${RDEV_GO:-go}
rdev_repro_out=${RDEV_REPRO_OUT:?set RDEV_REPRO_OUT to a new private external directory}
rdev_repro_version=${RDEV_RELEASE_TAG:-0.1.0-dev.0}
test -z "$(git status --porcelain --untracked-files=all)"
test ! -e "$rdev_repro_out"
umask 077
mkdir -p "$rdev_repro_out"
rdev_repro_out=$(cd "$rdev_repro_out" && pwd)
rdev_repro_source=$(git rev-parse HEAD)
rdev_repro_repo=$(pwd)
case $(uname -s) in Darwin) CGO_ENABLED=1 ;; *) CGO_ENABLED=0 ;; esac
export CGO_ENABLED
rdev_repro_started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
"$rdev_repro_go" mod verify > "$rdev_repro_out/module-verify.txt"
"$rdev_repro_go" env -json GOVERSION GOOS GOARCH GOAMD64 CGO_ENABLED CC GOROOT GOTOOLCHAIN > "$rdev_repro_out/build-environment.json"
for rdev_repro_copy in first second; do
    git clone --quiet --no-hardlinks --no-local "$rdev_repro_repo" "$rdev_repro_out/$rdev_repro_copy"
    git -C "$rdev_repro_out/$rdev_repro_copy" checkout --quiet --detach "$rdev_repro_source"
    GOCACHE="$rdev_repro_out/cache-$rdev_repro_copy" make -C "$rdev_repro_out/$rdev_repro_copy" GO="$rdev_repro_go" VERSION="$rdev_repro_version" all daemon > "$rdev_repro_out/$rdev_repro_copy.log" 2>&1
    "$rdev_repro_go" version > "$rdev_repro_out/$rdev_repro_copy-toolchain.txt"
done
python3 - "$rdev_repro_out" "$rdev_repro_source" "$rdev_repro_version" "$rdev_repro_started" <<'PY'
import hashlib,json,pathlib,sys,datetime
root=pathlib.Path(sys.argv[1]); paths=['bin/rdev','bin/rdevd']+['cmd/rdev/agents/rdev-agent-'+p for p in ['linux-amd64','linux-arm64','darwin-amd64','darwin-arm64','windows-amd64']]
rows=[]
for p in paths:
    digests=[]
    for copy in ['first','second']:
        with (root/copy/p).open('rb') as f: digests.append(hashlib.file_digest(f,'sha256').hexdigest())
    rows.append(dict(path=p,first_sha256=digests[0],second_sha256=digests[1],equal=digests[0]==digests[1]))
result=dict(schema_version=1,source_commit=sys.argv[2],version=sys.argv[3],cgo_enabled=int(json.loads((root/"build-environment.json").read_text())["CGO_ENABLED"]),started_at=sys.argv[4],build_environment=json.loads((root/"build-environment.json").read_text()),separate_checkouts=True,separate_compiler_caches=True,shared_verified_module_cache=True,completed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),artifacts=rows,result='passed' if all(r['equal'] for r in rows) else 'failed',boundary='local reproducibility on this OS/toolchain; no cross-toolchain or hosted builder claim')
(root/'result.json').write_text(json.dumps(result,indent=2)+'\n')
if result['result']!='passed':raise SystemExit('independent build bytes differ')
PY
