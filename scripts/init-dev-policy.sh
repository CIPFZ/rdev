#!/bin/sh
set -eu

policy_dir=${XDG_CONFIG_HOME:-"$HOME/.config"}/rdev
policy=${RDEV_RELEASE_POLICY:-$policy_dir/release-policy.json}

if [ "${policy#/}" = "$policy" ]; then
	printf '%s\n' 'RDEV_RELEASE_POLICY must be an absolute path' >&2
	exit 2
fi

if [ -e "$policy" ]; then
	printf 'Keeping existing release policy: %s\n' "$policy"
	exit 0
fi

policy_dir=$(dirname "$policy")

case "$(uname -s)" in
	Darwin) valid_until=$(date -u -v+7d '+%Y-%m-%dT%H:%M:%SZ') ;;
	*) valid_until=$(date -u -d '+7 days' '+%Y-%m-%dT%H:%M:%SZ') ;;
esac

umask 077
mkdir -p "$policy_dir"
tmp="$policy.$$"
trap 'rm -f "$tmp"' EXIT HUP INT TERM
cat >"$tmp" <<EOF
{
  "schema_version": 1,
  "valid_until": "$valid_until",
  "channels": ["dev"],
  "allow_unsigned_dev": true,
  "allow_test_roots": false,
  "roots": []
}
EOF
chmod 600 "$tmp"
mv "$tmp" "$policy"
trap - EXIT HUP INT TERM
printf 'Created unsigned-dev policy (expires %s): %s\n' "$valid_until" "$policy"
