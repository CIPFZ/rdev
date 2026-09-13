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
| R5 / M2 P1 | Device ID persists; per-host private/public key and metadata under `~/.config/rdev/keys/<device-id>/hosts/<host-id>/`; stable canonical user/address/port/namespace identity, aliases not security identities, explain generation relation. 0700 directories/0600 private keys, atomic/concurrent operations, identity changes and corruption handled. Existing user keys never become owned revocable keys. | `internal/keys` provides persistent device ID, canonical host ID, no-replace ed25519 key creation, fingerprint metadata and private permissions; integration with SSH config remains pending | `go test ./internal/keys -count=1` covers stability, identity separation, mode, metadata and no-regeneration | ebed038, cef3a80, 5e7a3fc / partial; independent review found crash-atomic publication and corruption validation gaps; authorized_keys/bootstrap pending |
| R6 / M2 P1 | Use configured target user (including root); no implicit sudo, password or sshd changes. | pending | pending | pending |
| R7 / M2 P1 | Append dedicated key to target user's authorized_keys, preserve original bytes/keys. Match exact locally owned key/fingerprint, never comment alone; device/host comment. Test idempotency, concurrency, failures. | pending | pending | pending |
| R8 / M3 P1 | Explicit single-host `bootstrap-key` on real interactive terminal. Verify usable dedicated key first. Otherwise show target/user/port, authorized_keys change and fingerprint; explicit confirmation and hidden password through controlled terminal auth, never argv/env/ordinary stdin/log/file/MCP. Agent never receives password; no scripted default yes. | pending | pending | pending |
| R9 / M3 P1 | `setup`: config, project approval, SSH host-key confirmation, dedicated key bootstrap, policy, ping. Unknown host requires fingerprint/target plus full yes; scans not automatically trusted; changed keys not treated as first use. Keep `hosts trust`; distinguish SSH trust and project approval; skip unnecessary steps and report failed stage/next action. | pending | pending | pending |
| R10 / M4 P2 | Read-only `doctor [host]`: version/mode/config/source/project trust/host key/public-key auth/policy/agent/protocol/cwd. Separate local, network probes and unknown/skipped. Never implicit install/repair/key/trust writes. PASS/WARN/FAIL/SKIP and actions, structured output. | pending | pending | pending |
| R11 / M4 P2 | hosts add scope and global all-projects explanation; list source/scope/overrides/effective item. Display untrusted project entries without activating; broker owner privacy. | pending | pending | pending |
| R12 / M5 P2 | Explicit hidden-password repair only after user confirms; no noninteractive/MCP/job fallback, no misclassifying network/policy/host-key errors as damaged keys. Disconnect password session, force dedicated-key-only reauthentication without caches before success; no retry loops/false partial success. | pending | pending | pending |
| R13 / M5 P2 | `bootstrap-key remove`: revoke only exact local device/host key, preserve others, explain possible last-access loss, cache-independent auth verification, recoverable local state and clear outcomes. | pending | pending | pending |
| R14 / M6 P3 | Read-only `agent status/plan`: current/candidate versions, policy decision, upload need, transaction/lock/actions. Unknown when not observable; never trigger install. | `cmd/rdev/doctor.go` and CLI routing | `go test ./cmd/rdev -count=1`; command uses capability probe only and reports unknown upload/transaction fields | f1971d5 / partial; broker implementation and richer remote release inspection pending |
| R15 / M6 P3 | Conditional `agent repair` only with mature status/plan plus transaction/preview/authorization/recovery contracts. If deferred give technical reasons and exact boundary; no other R1–R14 can be deferred instead. | pending | pending | pending |
| R16 / M6 P3 | Accurate exec/job stdin, noninteractive, no PTY/TUI boundaries; no nonexistent session promises. | pending | pending | pending |
| R17 / M6 P3 | Corresponding CLI/MCP diagnostics where possible. No password-bearing MCP or automated human trust tools; actionable terminal guidance. Respect standalone/broker permission boundaries. | pending | pending | pending |
| R18 / docs | Update README, operations, plan/acceptance, repository skills/rdev. Verify skill against implemented behavior for Codex/Claude/general agents. No .agents/.claude repository directories or symlinks. | pending | pending | pending |
| R19 / delivery | Copy physical skill files to existing ~/.codex/skills/rdev and ~/.agents/skills/rdev; fix global relative references with self-contained references; verify usable references, not merely byte equality. | pending | pending | pending |
| R20 / delivery | After acceptance rebuild/install user-local rdev/rdevd; verify executable paths/version/current dev policy and my-hk/dev-env ping. Do not restart unknown services. State worktree branch/main relationship precisely. | pending | pending | pending |

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
