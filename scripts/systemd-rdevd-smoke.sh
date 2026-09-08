#!/bin/sh
# Execute on a Linux host with a running systemd user manager. This installs a
# uniquely named test unit from the shipped template and removes it on exit.
set -eu
bin=$1
template=$2
run=$3
mkdir -m 700 "$run"
unit="rdevd-phase5-$(basename "$run")-$$.service"
linked=0
cleanup() {
	result=$?
	trap - EXIT HUP INT TERM
	if [ "$result" -ne 0 ]; then journalctl --user -u "$unit" -n 30 --no-pager >&2 || true; fi
	if [ "$linked" = 1 ]; then
		systemctl --user disable --now "$unit" >/dev/null 2>&1 || true
		systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true
		systemctl --user daemon-reload
	fi
	exit "$result"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
"$bin" principal-keygen -out "$run/principal.key"
python3 - "$template" "$run/$unit" "$bin" "$run" <<'PY'
import pathlib, sys
template, target, binary, run = sys.argv[1:]
text = pathlib.Path(template).read_text()
text = text.replace('%h/bin/rdevd', binary)
text = text.replace('-ready-file %t/rdevd.ready', '-socket '+run+'/broker.sock -ready-file '+run+'/ready')
text = text.replace('%h/.config/rdev/principal.key', run+'/principal.key')
text = text.replace('%h/.local/share/rdev/agents', run+'/agents')
pathlib.Path(target).write_text(text)
PY
systemd-analyze --user verify "$run/$unit"
systemctl --user link "$run/$unit"
linked=1
systemctl --user enable --now "$unit"
test "$(systemctl --user is-enabled "$unit")" = enabled
wait_ready() {
	n=0
	while [ ! -f "$run/ready" ]; do
		n=$((n + 1)); test "$n" -lt 150; sleep .1
	done
	test "$(systemctl --user is-active "$unit")" = active
}
wait_ready
old_pid=$(systemctl --user show -p MainPID --value "$unit")
test "$old_pid" -gt 0
systemctl --user reload "$unit"
systemctl --user kill --kill-whom=main --signal=KILL "$unit"
n=0
while :; do
	new_pid=$(systemctl --user show -p MainPID --value "$unit")
	if [ "$new_pid" -gt 0 ] && [ "$new_pid" != "$old_pid" ]; then break; fi
	n=$((n + 1)); test "$n" -lt 150; sleep .1
done
wait_ready
test "$(systemctl --user show -p NRestarts --value "$unit")" -ge 1
systemctl --user stop "$unit"
test ! -e "$run/ready"
test ! -e "$run/broker.sock"
systemctl --user start "$unit"
wait_ready
printf 'systemd user install/enable/start/reload/SIGKILL recovery/stop/start: ok (pid %s -> %s)\n' "$old_pid" "$new_pid"
