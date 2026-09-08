# Phase 5 implementation status

The broker package now contains the first implementation layer for P5-01 through
P5-16: protocol version negotiation, private Unix socket ownership, shared
service state, owner scoping, cancellation isolation, quotas, fair scheduling,
traffic lanes, merged job watches, detached-job registry, leases, policy and
approval checks, rotating audit events, readiness, and validated reload/drain.

Validation currently covers `internal/broker`, `internal/proto`,
`internal/transport`, and `internal/client` with the Go toolchain under
`~/sdk/go1.25.0`. The repository gate `make check` passes after generating the
four embedded agent artifacts; it runs agent consistency checks, `go vet`, and
the complete test suite.
The `cmd/rdevd` entrypoint now performs hello negotiation, peer credential
checks, owner-scoped policy checks, QoS admission, audit recording, and wire
dispatch through the broker-owned `client.Client`; broker shutdown also closes
the shared client pool. Persistent job re-discovery and multi-client QoS
benchmarks remain follow-up validation for the final Phase 5 gate.

User service templates are provided under `deploy/systemd` and `deploy/launchd`;
both rely on the private socket lock and bounded shutdown path. Policy grants,
job ownership, audit events, and config are persisted beside the broker socket.


The 2026-09-08 follow-up adds authenticated-by-default daemon startup, private
key generation and token provisioning, live key rotation with session revocation,
fail-closed startup/config parsing, and real process lifecycle tests. Linux
systemd user installation and automatic restart are exercised by
`make remote-service-smoke`; `make remote-phase5-runtime` additionally executes
the real credential/crash test suite on the remote host without needing Go there.
Consult `phase5-acceptance.md` for the authoritative incomplete items; source
primitives and green unit tests alone are not completion evidence.
