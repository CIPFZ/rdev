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

.PHONY: all agents build test vet fmt clean install check-agents check smoke-rdevd

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
	@tmp=$$(mktemp -d); \
	sock=$$tmp/rdevd.sock; ready=$$tmp/ready; log=$$tmp/log; \
	trap 'kill -TERM $$pid 2>/dev/null || true; wait $$pid 2>/dev/null || true; rm -rf $$tmp' EXIT; \
	bin=$$tmp/rdevd; $(GO) build -trimpath -o "$$bin" ./cmd/rdevd; \
	"$$bin" -socket "$$sock" -ready-file "$$ready" -agent-dir "$$tmp/agents" >"$$log" 2>&1 & pid=$$!; \
	for i in $$(seq 1 100); do test -f "$$ready" && break; sleep 0.1; done; \
	test -f "$$ready"; kill -TERM $$pid; \
	for i in $$(seq 1 100); do kill -0 $$pid 2>/dev/null || break; sleep 0.1; done; \
	wait $$pid; test ! -e "$$ready"; test ! -e "$$sock"; \
	echo 'rdevd readiness/shutdown smoke: ok'

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
