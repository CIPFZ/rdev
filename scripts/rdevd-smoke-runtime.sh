#!/bin/sh
# Runs the actual binary. Every assertion is fatal; cleanup owns only this run.
set -eu
bin=$1
run=$2
mkdir -m 700 "$run"
pid=
cleanup() {
 result=$?
 trap - EXIT HUP INT TERM
 if [ -n "$pid" ]; then
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
 fi
 if [ "$result" -ne 0 ] && [ -f "$run/log" ]; then tail -30 "$run/log" >&2; fi
 exit "$result"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
"$bin" principal-keygen -out "$run/principal.key"
"$bin" -socket "$run/broker.sock" -ready-file "$run/ready" -principal-key-file "$run/principal.key" >"$run/log" 2>&1 &
pid=$!
printf '%s\n' "$pid" >"$run/pid"
n=0
while [ ! -f "$run/ready" ]; do
 kill -0 "$pid"
 n=$((n + 1)); test "$n" -lt 100
 sleep .1
done
test "$(cat "$run/ready")" = READY
test -S "$run/broker.sock"
mode() { stat -c '%a' "$1" 2>/dev/null || stat -f '%Lp' "$1"; }
test "$(mode "$run")" = 700
test "$(mode "$run/broker.sock")" = 600
test "$(mode "$run/ready")" = 600
if "$bin" -socket "$run/broker.sock" -ready-file "$run/ready" -principal-key-file "$run/principal.key" >"$run/second.log" 2>&1; then
 echo 'duplicate instance unexpectedly started' >&2; exit 1
fi
test -f "$run/ready"
kill -TERM "$pid"
n=0
while kill -0 "$pid" 2>/dev/null; do n=$((n + 1)); test "$n" -lt 150; sleep .1; done
wait "$pid"
pid=
test ! -e "$run/ready"
test ! -e "$run/broker.sock"
printf '%s\n' 'rdevd authenticated readiness/permissions/single-instance/shutdown smoke: ok'
