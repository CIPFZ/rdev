GO ?= $(HOME)/sdk/go1.25.0/bin/go
AGENT_DIR := cmd/rdev/agents
PLATFORMS := linux-amd64 linux-arm64 darwin-amd64 darwin-arm64

.PHONY: all agents build test vet fmt clean install

all: agents build

# Agent binaries are embedded into the rdev binary, so they must be built first.
# -s -w strips symbols and DWARF: these are uploaded over ssh on first connect,
# and the size reduction is worth more than a remote stack trace.
agents:
	@mkdir -p $(AGENT_DIR)
	@for p in $(PLATFORMS); do \
		os=$${p%-*}; arch=$${p#*-}; \
		printf 'building agent %s/%s\n' $$os $$arch; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 $(GO) build -trimpath -ldflags='-s -w' \
			-o $(AGENT_DIR)/rdev-agent-$$os-$$arch ./cmd/rdev-agent || exit 1; \
	done

build: agents
	$(GO) build -trimpath -o bin/rdev ./cmd/rdev

install: agents
	$(GO) install ./cmd/rdev

vet:
	$(GO) vet ./...

test:
	$(GO) test ./... -count=1

fmt:
	$(GO) fmt ./...

clean:
	rm -rf bin $(AGENT_DIR)
