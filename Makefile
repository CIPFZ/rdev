GO ?= $(HOME)/sdk/go1.25.0/bin/go
AGENT_DIR := cmd/rdev/agents
PLATFORMS := linux-amd64 linux-arm64 darwin-amd64 darwin-arm64

# Build identity, stamped into both binaries so a running one can say what it is.
#
# COMMIT_TIME is the commit's date, not the build's. A build timestamp would change
# the agent's bytes on every rebuild, defeating the content-hash check that decides
# whether a remote agent needs replacing -- every reconnect would re-upload 9 MB.
# The commit date orders two builds just as well for that purpose, and keeps builds
# reproducible.
#
# A dirty tree inherits its parent commit's date, so COMMIT carries -dirty and
# consumers treat that as "unorderable" rather than trusting the timestamp.
PKG         := github.com/CIPFZ/rdev/internal/buildinfo
COMMIT      := $(shell git describe --tags --always --dirty 2>/dev/null || echo unknown)
COMMIT_TIME := $(shell TZ=UTC0 git show -s --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ 2>/dev/null)
STAMP       := -X $(PKG).Commit=$(COMMIT) -X $(PKG).CommitTime=$(COMMIT_TIME)

.PHONY: all agents build test vet fmt clean install check-agents check smoke-rdevd remote-smoke remote-service-smoke remote-phase5-runtime remote-session-benchmark remote-lifecycle remote-policy remote-routes remote-lane-traffic remote-connection-diagnostics remote-secrets remote-secret-import remote-secret-qos remote-approval remote-qos remote-ingress remote-frontends remote-warm-pool remote-mux-capacity remote-audit-continuity remote-audit-routes remote-audit-soak remote-audit-upgrade stress-broker

all: agents build

# Agent binaries are embedded into the rdev binary, so they must be built first.
# -s -w strips symbols and DWARF: these are uploaded over ssh on first connect,
# and the size reduction is worth more than a remote stack trace.
agents:
	@mkdir -p $(AGENT_DIR)
	@for p in $(PLATFORMS); do \
		os=$${p%-*}; arch=$${p#*-}; \
		printf 'building agent %s/%s\n' $$os $$arch; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 $(GO) build -trimpath -ldflags='-s -w $(STAMP)' \
			-o $(AGENT_DIR)/rdev-agent-$$os-$$arch ./cmd/rdev-agent || exit 1; \
	done

build: agents
	$(GO) build -trimpath -ldflags='$(STAMP)' -o bin/rdev ./cmd/rdev

install: agents
	$(GO) install -trimpath -ldflags='$(STAMP)' ./cmd/rdev

# check-agents verifies the embedded agents were built from the current source.
#
# `go build ./cmd/rdev` does not rebuild cmd/rdev/agents/, so it is possible to
# produce an rdev whose embedded agent is older than its own code -- and from the
# outside that build looks perfectly fine. This target is what detects it.
#
# The comparison is by content, not mtime. git does not preserve mtimes, so a fresh
# clone or a restored CI cache would make a timestamp check cry wolf until someone
# disabled it. Builds are byte-reproducible under -trimpath with fixed ldflags, so
# equal checksums are a real answer rather than a heuristic.
#
# The rebuild uses the same STAMP as `make agents`, so this compares like with like.
# A dirty tree therefore reports STALE until the agents are rebuilt, which is
# correct: source has changed that the embedded binaries do not contain.
check-agents:
	@fail=0; tmp=$$(mktemp -d); \
	trap 'rm -rf $$tmp' EXIT; \
	for p in $(PLATFORMS); do \
		os=$${p%-*}; arch=$${p#*-}; \
		have=$(AGENT_DIR)/rdev-agent-$$os-$$arch; \
		if [ ! -f $$have ]; then \
			printf 'MISSING  rdev-agent-%s-%s (run make agents)\n' $$os $$arch; fail=1; continue; \
		fi; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 $(GO) build -trimpath -ldflags='-s -w $(STAMP)' \
			-o $$tmp/rdev-agent-$$os-$$arch ./cmd/rdev-agent || exit 1; \
		embedded=$$(shasum -a 256 $$have | cut -c1-12); \
		current=$$(shasum -a 256 $$tmp/rdev-agent-$$os-$$arch | cut -c1-12); \
		if [ "$$embedded" = "$$current" ]; then \
			printf 'ok       rdev-agent-%s-%s %s\n' $$os $$arch $$embedded; \
		else \
			printf 'STALE    rdev-agent-%s-%s embedded=%s current=%s\n' \
				$$os $$arch $$embedded $$current; fail=1; \
		fi; \
	done; \
	if [ $$fail -ne 0 ]; then \
		printf '\nThe embedded agents were not built from this source tree.\n'; \
		printf 'Run `make all` (not `go build`) so bin/rdev and its agents agree.\n'; \
		printf 'Compare against a built binary with `rdev version`.\n'; \
		exit 1; \
	fi

# check is what CI and a pre-push run should use: correctness plus build consistency.
check: vet test check-agents

# Start the real broker binary, wait for readiness, then verify signal-driven
# shutdown removes both the readiness marker and private socket.
smoke-rdevd: agents
	@set -eu; tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	$(GO) build -trimpath -o "$$tmp/rdevd" ./cmd/rdevd; \
	sh scripts/rdevd-smoke-runtime.sh "$$tmp/rdevd" "$$tmp/run"

# Build the Linux daemon locally and exercise it on the configured real SSH
# host. The script uses the user's SSH alias/configuration and never handles a
# private key directly. Override RDEV_REMOTE_SSH to test another SSH target.
remote-smoke: agents
	RDEV_REMOTE_SSH='$(RDEV_REMOTE_SSH)' RDEV_SSH_CONFIG='$(RDEV_SSH_CONFIG)' RDEV_GO='$(GO)' sh scripts/remote-rdevd-smoke.sh

remote-service-smoke: agents
	RDEV_REMOTE_SERVICE=1 RDEV_REMOTE_SSH='$(RDEV_REMOTE_SSH)' RDEV_SSH_CONFIG='$(RDEV_SSH_CONFIG)' RDEV_GO='$(GO)' sh scripts/remote-rdevd-smoke.sh

remote-phase5-runtime: agents
	RDEV_REMOTE_RUNTIME=1 RDEV_REMOTE_SERVICE=1 RDEV_REMOTE_SSH='$(RDEV_REMOTE_SSH)' RDEV_SSH_CONFIG='$(RDEV_SSH_CONFIG)' RDEV_GO='$(GO)' sh scripts/remote-rdevd-smoke.sh

remote-session-benchmark: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerProcesses$$' -count=3 -timeout=5m -v

remote-lifecycle: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerRetryCancellation$$' -count=3 -timeout=2m -v
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerLeaseLifecycle$$' -count=1 -timeout=2m -v

remote-policy: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerPolicyIsolation$$' -count=3 -timeout=2m -v

remote-secret-import: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerSecretImport$$' -count=3 -timeout=3m -v

remote-secrets: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerSecrets$$' -count=3 -timeout=3m -v

remote-connection-diagnostics: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerConnectionDiagnostics$$' -count=3 -timeout=2m -v

remote-lane-traffic: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerLaneTraffic$$' -count=3 -timeout=2m -v

remote-routes: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerRoutes$$' -count=3 -timeout=2m -v

remote-approval: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerApprovalIsolation$$' -count=3 -timeout=2m -v

remote-secret-qos: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerQoSWithSecrets$$' -count=3 -timeout=5m -v

remote-qos: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerQoS$$' -count=3 -timeout=4m -v

.PHONY: remote-jobs remote-wait remote-replay-digest remote-mutation remote-events remote-fleet-boundary remote-job-upgrade remote-sync-preview
remote-sync-preview: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerSyncPreview$$' -count=3 -timeout=3m -v

remote-job-upgrade: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' bash scripts/remote-job-upgrade.sh '$(GO)'

remote-fleet-boundary: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerFleetBoundary$$' -count=3 -timeout=2m -v

remote-jobs: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerJobRecovery$$' -count=3 -timeout=2m -v

remote-wait: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerSharedWait$$' -count=3 -timeout=3m -v

remote-replay-digest: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteAgentReplayDigest$$' -count=3 -timeout=2m -v

remote-mutation: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerMutationCrashRecovery$$' -count=3 -timeout=3m -v

remote-events: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerJobEventHistory$$' -count=3 -timeout=3m -v

remote-audit-upgrade: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' scripts/remote-audit-upgrade.sh '$(GO)'

remote-audit-soak: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerAuditSoak$$' -count=1 -timeout=65m -v

remote-audit-routes: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerAuditRoutes$$' -count=3 -timeout=2m -v

remote-audit-continuity: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerAuditContinuity$$' -count=3 -timeout=3m -v

remote-mux-capacity: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerSaturatedControlMaster$$' -count=3 -timeout=5m -v

remote-warm-pool: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerWarmPool$$' -count=1 -timeout=15m -v

remote-frontends: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerFrontendBoundary$$' -count=3 -timeout=3m -v

remote-ingress: agents
	RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE='$(RDEV_REMOTE_SSH)' RDEV_TEST_SSH_CONFIG='$(RDEV_SSH_CONFIG)' $(GO) test ./cmd/rdevd -run '^TestRemoteBrokerIngressIsolation$$' -count=3 -timeout=3m -v

stress-broker: agents
	$(GO) test ./cmd/rdevd -run TestUnixBrokerTwentyClients -count=100 -timeout=5m

# vet and test depend on agents for the same reason check does: cmd/rdev cannot be
# loaded at all until the binaries it embeds exist.
vet: agents
	$(GO) vet ./...

test: agents
	$(GO) test ./... -count=1

fmt:
	$(GO) fmt ./...

clean:
	rm -rf bin $(AGENT_DIR)
