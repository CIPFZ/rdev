#!/bin/sh
# Real Go coverage-guided fuzzing; seeds also run with ordinary go test.
set -eu
cd "$(dirname "$0")/.."
rdev_fuzz_go=${RDEV_GO:-go}
rdev_fuzz_time=${RDEV_FUZZ_TIME:-5s}
rdev_fuzz_parallel=${RDEV_FUZZ_PARALLEL:-2}
for entry in \
    './internal/transport FuzzNDJSONAndFrame' \
    './internal/transport FuzzRemotePath' \
    './internal/proto FuzzErrorEnvelope' \
    './internal/session FuzzHostConfig' \
    './internal/artifact FuzzManifest' \
    './internal/artifact FuzzSignaturePolicy' \
    './internal/broker FuzzBrokerErrorResponse' \
    './internal/agentinstall FuzzUpgradeRecord' \
    './internal/state FuzzStateMetadata' \
    './cmd/rdev FuzzCLIArguments'; do
    set -- $entry
    "$rdev_fuzz_go" test "$1" -run='^$' -fuzz="^$2$" -fuzztime="$rdev_fuzz_time" -parallel="$rdev_fuzz_parallel"
done
