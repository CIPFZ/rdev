# rdev onboarding and diagnostics delivery plan

Status: active; no requirement below is accepted until its implementation, review,
behavioral tests and evidence are recorded. Branch: `codex/onboarding-diagnostics`.
Workspace: `/Users/tonyny/.codex/worktrees/0ed0/rdev`. Starting commit: `d456c8c`
(main in `/Users/tonyny/works/rdev`). Main is not the delivery worktree; do not push.
The previous Phase 9 documentation refresh does not implement this plan.

## Requirements and acceptance matrix

| ID / phase | Required behavior and acceptance | Code | Tests / evidence | Commit / status |
|---|---|---|---|---|
| R1 / M1 P0 | Offline `policy check`: actual source/path, missing vs denied, JSON/schema, owner/mode/ACL including ancestors, expiry, channels, hosts, signature boundary; stable codes/actionable reasons. Broker never consults client-local policy or reveals admin paths. Report only the current process environment. | `internal/artifact/diagnose.go`, `cmd/rdev/main.go` | `go test ./internal/artifact ./cmd/rdev`; missing/valid/public behavior and broker routing review | 3173709 + 24d9574 / complete for standalone read-only diagnosis; broker admin-side diagnosis remains pending |
| R2 / M1 P0 | Hierarchical help including `hosts add --help`; useful unknown argument errors; help matches accepted options without relaxing strict parsing. | `cmd/rdev/main.go`, `cmd/rdev/flags.go` | `make build`; real `hosts add --help` and unknown flag exit 2 | b2c8c4f / complete |
| R3 / M1 P0 | Preserve explicit unsigned dev opt-in; portable Go discovery and default policy path; init never overwrites existing policy; permission/expiry guidance. Normal builds do not expand trust. | `scripts/init-dev-policy.sh`, `Makefile`, `internal/artifact/policy.go` | existing policy tests, `make check`; no user policy mutation performed | pending targeted dev-policy portability review |
| R4 / M1 P0 | Review macOS ACL handling: allow deny-only, reject allow/unknown/error, retain owner/mode/ancestor checks. Real ACL regression; do not alter system ACLs. | `internal/artifact/acl_darwin_cgo.go` | existing ACL regression suite; macOS system ACL not modified | pending real deny-only/allow ACL execution evidence |
| R5 / M2 P1 | Device ID persists; per-host private/public key and metadata under `~/.config/rdev/keys/<device-id>/hosts/<host-id>/`; stable canonical user/address/port/namespace identity, aliases not security identities, explain generation relation. 0700 directories/0600 private keys, atomic/concurrent operations, identity changes and corruption handled. Existing user keys never become owned revocable keys. | `internal/keys` provides persistent device ID, canonical host ID, no-replace ed25519 key creation, fingerprint metadata, pair validation and private permissions; integration with SSH config remains pending | `go test -race ./internal/keys -count=1` covers stability, identity separation, mode, metadata, symlink rejection, no-regeneration and corruption rejection | ebed038, cef3a80, 5e7a3fc, 28e5eda, 58605b8, 1853a74 / partial; owner checks and SSH integration pending |
| R6 / M2 P1 | Use configured target user (including root); no implicit sudo, password or sshd changes. | pending | pending | pending |
| R7 / M2 P1 | Append dedicated key to target user's authorized_keys, preserve original bytes/keys. Match exact locally owned key/fingerprint, never comment alone; device/host comment. Test idempotency, concurrency, failures. | `internal/keys/authorized.go` exact-body append primitive | `go test ./internal/keys -count=1` covers forged comments, idempotency and preservation | ed95de3 / partial; remote user authorized_keys transport and concurrent file lock pending |
| R8 / M3 P1 | Explicit single-host `bootstrap-key` on real interactive terminal. Verify usable dedicated key first. Otherwise show target/user/port, authorized_keys change and fingerprint; explicit confirmation and hidden password through controlled terminal auth, never argv/env/ordinary stdin/log/file/MCP. Agent never receives password; no scripted default yes. | `cmd/rdev/setup.go` adds strict terminal-only entry and rejects noninteractive/MCP/job invocation without reading a password | `go test ./cmd/rdev`; piped stdin real binary test exits 1 with explicit refusal | 1714971 / partial; controlled terminal exchange and isolated sshd still pending |
| R9 / M3 P1 | `setup`: config, project approval, SSH host-key confirmation, dedicated key bootstrap, policy, ping. Unknown host requires fingerprint/target plus full yes; scans not automatically trusted; changed keys not treated as first use. Keep `hosts trust`; distinguish SSH trust and project approval; skip unnecessary steps and report failed stage/next action. | strict `setup` entry exists but deliberately reports unavailable terminal flow | noninteractive refusal tested; no real trust mutation | 1714971 / partial |
| R10 / M4 P2 | Read-only `doctor [host]`: version/mode/config/source/project trust/host key/public-key auth/policy/agent/protocol/cwd. Separate local, network probes and unknown/skipped. Never implicit install/repair/key/trust writes. PASS/WARN/FAIL/SKIP and actions, structured output. | pending | pending | pending |
| R11 / M4 P2 | hosts add scope and global all-projects explanation; list source/scope/overrides/effective item. Display untrusted project entries without activating; broker owner privacy. | `cmd/rdev/main.go` now labels scope and source; existing approval-gated registry loading retained | `go test ./cmd/rdev -count=1`; source label is derived only from effective approved scope | 2af3a1c / partial; override detail and broker owner-scoped listing pending |
| R12 / M5 P2 | Explicit hidden-password repair only after user confirms; no noninteractive/MCP/job fallback, no misclassifying network/policy/host-key errors as damaged keys. Disconnect password session, force dedicated-key-only reauthentication without caches before success; no retry loops/false partial success. | pending | pending | pending |
| R13 / M5 P2 | `bootstrap-key remove`: revoke only exact local device/host key, preserve others, explain possible last-access loss, cache-independent auth verification, recoverable local state and clear outcomes. | `internal/keys/authorized.go` exact-body removal primitive; CLI remains terminal-only boundary | `go test ./internal/keys -count=1` covers unrelated-key preservation and missing-key no-op | ed95de3 / partial; remote cache-independent verification pending |
| R14 / M6 P3 | Read-only `agent status/plan`: current/candidate versions, policy decision, upload need, transaction/lock/actions. Unknown when not observable; never trigger install. | `cmd/rdev/doctor.go` and CLI routing | `go test ./cmd/rdev -count=1`; command uses capability probe only and reports unknown upload/transaction fields | f1971d5 / partial; broker implementation and richer remote release inspection pending |
| R15 / M6 P3 | Conditional `agent repair` only with mature status/plan plus transaction/preview/authorization/recovery contracts. If deferred give technical reasons and exact boundary; no other R1–R14 can be deferred instead. | pending | pending | pending |
| R16 / M6 P3 | Accurate exec/job stdin, noninteractive, no PTY/TUI boundaries; no nonexistent session promises. | README and skill now document inherited stdio and no PTY/TUI/session promise | documentation review; runtime protocol already uses stdio without PTY allocation | current docs commit / partial |
| R17 / M6 P3 | Corresponding CLI/MCP diagnostics where possible. No password-bearing MCP or automated human trust tools; actionable terminal guidance. Respect standalone/broker permission boundaries. | pending | pending | pending |
| R18 / docs | Update README, operations, plan/acceptance, repository skills/rdev. Verify skill against implemented behavior for Codex/Claude/general agents. No .agents/.claude repository directories or symlinks. | README and `skills/rdev` now describe read-only diagnostics and terminal-only password boundaries; repository remains free of `.agents/.claude` | `git diff --check`; references reviewed against implemented behavior | current docs commit / partial; operations updates pending |
| R19 / delivery | Copy physical skill files to existing ~/.codex/skills/rdev and ~/.agents/skills/rdev; fix global relative references with self-contained references; verify usable references, not merely byte equality. | `Makefile install-skill` copies SKILL and references as regular files | `make install-skill`; destination references exist and SKILL files are not symlinks | b11df2c / partial; user-level copies installed and verified |
| R20 / delivery | After acceptance rebuild/install user-local rdev/rdevd; verify executable paths/version/current dev policy and my-hk/dev-env ping. Do not restart unknown services. State worktree branch/main relationship precisely. | `Makefile`, built `bin/rdev`/`bin/rdevd`; installed to `$HOME/.local/bin` | `make all daemon`; `rdev version`; `hosts list`; `dev-env ping` passed; `my-hk ping` failed with remote connection closed (no mutation attempted) | `2a8cb4c` / partial until my-hk connectivity and final acceptance |

## Stage gates and evidence rules

Execute M1 through M6 in order. Each implementation must be followed by code review,
functional review, actual behavioral tests, repairs and reruns. Independent review
agents are allowed. Review cannot merely repeat author claims.

Keys, permissions, ACLs, SSH trust and password handling require negative/regression
tests. Use real binaries and an isolated sshd with a test account/temp directories
and synthetic credentials for password bootstrap, key idempotency, repair, removal,
original-key preservation and authentication cache isolation. Never put real account
passwords in tool arguments/logs. Existing dev-env/my-hk are authorized for ping,
not destructive key/trust exercises without specific authorization.

Record actual environment, command, exit status, artifact path and commit for make
check, targeted race/platform and SSH tests. Unit tests are not SSH acceptance;
unavailable Linux/macOS/Windows evidence stays explicitly untested. Keep the
requirement → code → test → evidence → commit matrix current. Only mark the goal
complete after necessary implementation, review/tests, docs and local delivery all
agree. Time spent, old-code tests and documentation alone are not completion.

## Initial inspection

2026-09-13: both worktrees clean; task detached at 52dbfa2, main at d456c8c.
Created delivery branch at d456c8c. Existing dev-build, skill and ACL changes need
validation rather than being assumed correct. No push or service restart performed.
