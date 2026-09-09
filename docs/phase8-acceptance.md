# Phase8 acceptance

Status: **In progress — not Phase8 Complete or Production Certified**.
The frozen product, runtime tests and local release artifacts bind
`047a0f53bb39b637b55c5aedec99c28ff0a0f8da`. Later workflow/closure changes are
recorded separately; CI builds have their own commit stamps and are not the same
bytes as this local candidate. The handoff baseline was `e73a38b`, clean and equal
to `origin/main` after fetch. No original platform target or gate is waived.

| Requirement | Accepted engineering evidence | Remaining acceptance |
|---|---|---|
| P8-01 | Strict SSHSIG manifest/trust policy, exact six-binary and metadata binding, linked-dependency notices, online source/binary vulnerability audits, two independent clean builds with six equal digests, full-bundle test signing and eight tamper refusals | Official authorized signer/root and corresponding final release identity; test root is not official |
| P8-02 | Namespace inode lock, bounded staging, durable journal, real hello/state readiness and verified rollback implementation; subprocess lock/SIGKILL window tests; actual predecessor upgrade retains original supervisors and once-only effects | Full real-agent upgrade/automatic and explicit rollback/fault-window matrix; unit fixture agents do not certify production rollback |
| P8-03 | Shared administrator verification, signed test-root CLI/broker/agent runtime, channel/pin/rollback policy negatives, low-sensitivity audit, typed rejection and uncertainty semantics | Full historical channel/rollback combinations and official identity deployment remain unverified |
| P8-04 | Local check/race, actual ten-target fuzz, real IPv4/IPv6/ProxyJump and isolated auth/host-key negatives; hosted runs actually executed | Hosted SSH step failed on 563/047; diagnostic rerun pending. Full declared DNS/storage/half-close/failure matrix remains incomplete |
| P8-05 | Persistent supervisor, fixed storage/resource budgets, actual real-SSH short engineering fault/idle probes; first frozen 100-instance/20-client run stopped after 25.48s with sync not_sent and no staging; bounded workload admission correction under verification | Passing final short-run result, continuous 24h and full scale fairness/Fleet/in-flight/GC/metric coverage; no physical-100 topology supplied |
| P8-06 | Actual Phase7 predecessor clean build: retained-supervisor upgrade, old CLI/MCP→new broker/agent and new CLI/MCP→old broker/agent shared read/policy cases passed | Formal N-1 release identity; full standalone/state rollback/approval/archive/channel matrix |

| Production Gate | Status and evidence boundary |
|---|---|
| 1. High/Medium findings closed | Passed for independently reviewed product source 047a0f5; no unresolved confirmed High/Medium and no agent-made risk acceptance. Workflow-only delta independently reviewed. |
| 2. All declared Tier 1 runtime | Pending. Linux amd64/OpenSSH/XFS and actual IPv4/IPv6/ProxyJump exercised. Linux arm64 and Darwin runtime not run; macOS remains explicitly deferred. Cross-builds do not pass these targets. |
| 3. Official signature, SBOM, provenance, audits, notices | Blocked on authorized official signing identity/root. Local exact-byte audits/notices/SBOM/provenance and isolated test signatures passed. |
| 4. Real continuous 24h within budgets | Pending; no completed 24h evidence. Starting or supervising a run is not passing it. |
| 5. Complete upgrade/rollback/crash/network/migration drills | Partial. Actual retained-job upgrade, broker/network/mutation recovery passed; full real rollback and historical state matrix still pending. |

The available environment is Linux amd64 / OpenSSH 9.3p2 / XFS, Go 1.26.8,
16 CPUs and about 32 GiB RAM, with an authorized Linux amd64 SSH endpoint.
The isolated-instance topology shares one kernel/filesystem/network fault domain;
physical machine count is unverified. A non-root SSH preflight also passed on
this Linux system; it does not certify the Ubuntu hosted runner. A GitHub runner
actually executed workflows, but anonymous API/log access is rate-limited or
requires authentication. No formal signer, arm64/Darwin runtime or authorized
100-machine topology has been supplied. No production release/deployment,
repository permission/secret change or cloud purchase was performed.

## Contracts and operation

`release.json` schema 1 is the exact SSHSIG payload, purpose `rdev-release`.
It binds release version/channel, full source commit and tree digest, dirty flag,
signer identity, validity window, six binary digests/platforms, and exact bytes of
`manifest.json`, CycloneDX SBOM, local SLSA provenance and distribution notices.
The inner manifest carries actual Go build information and complete protocol,
feature and state contract. Both layers are checked together. Go omits ldflags
under trimpath; a linker-injected version marker, checked against the runtime
version at initialization, provides cross-platform version binding. Plain builds
without this marker cannot become a signed candidate.

Build order is agents → CLI embedding those exact agents + broker → actual
linked-dependency notices/SBOM/provenance → release manifest → detached signature.
No binary embeds its own digest or signature. Signing uses a private immutable
copy of the already validated payload. Testing, signing and distribution use the
same artifact directory; rebuilding or changing any metadata invalidates it.
Native Linux release builds and cross-built agents use CGO_ENABLED=0. Native
Darwin CLI/broker builds require CGO_ENABLED=1 for fd-native ACL validation;
Darwin no-cgo policy admission fails closed and is not a supported runtime. The two-checkout reproducibility command uses
separate compiler caches, the same pinned toolchain and verified module cache.
It proves local byte reproducibility, not cross-toolchain equivalence or a SLSA
level. Provenance continues to identify the actual local release-gate process;
running that script inside CI does not change its builder claim.

The trusted policy is a private owned regular file, by default
`~/.config/rdev/release-policy.json`; a trusted process environment may select an
absolute `RDEV_RELEASE_POLICY`. All ancestors must be protected (root-owned sticky
`/tmp` is permitted). CLI and standalone MCP share the verifier. Shared frontends
cannot pass a policy/root to rdevd; rdevd reads its own administrator environment.
A rejected shared request never falls back to private SSH.

Policy roots are administrator-provisioned Ed25519 OpenSSH public keys with
identity, channels, validity and revocation state. SSHSIG verifies publisher
identity; SSH host keys verify the connection target. Neither replaces the other.
Multiple roots support overlap during rotation; revoke/remove old roots and
refresh `valid_until` through the same trusted administration path. Verification
is offline: it uses the current local revocation snapshot and clock, not a claim
of fresh online revocation. Expired policy/root/release fails closed. Missing
signature or failed validation never enables unsigned fallback.

Host restrictions only narrow global channels/pin. A nonempty `hosts` map also
denies every unlisted configured target, so changing a project/SSH alias cannot
fall back to a broader global policy. Keys are SHA-256 of normalized SSH
destination, NUL, port, NUL, home-relative namespace. They identify configured
connection identity, not physical hardware; aliases require explicit entries.
Projects cannot set roots, bundle paths, unsigned opt-in or rollback grants.

Unsigned migration requires a visible administrator dev opt-in, for example in
an isolated test environment (replace the expiry before using it):

```json
{"schema_version":1,"valid_until":"2026-09-20T00:00:00Z","channels":["dev"],"allow_unsigned_dev":true,"allow_test_roots":false,"roots":[]}
```

Create the parent privately and set the policy to 0600. For signed use, set an
absolute `bundle_dir`, allowed channels, optional `pin_version`, and authorized
`roots`; `test_only` roots additionally require `allow_test_roots:true` and never
count as official identity. Numeric release order replaces commit time for
signed updates. A signed version cannot be reused with different bytes. An
explicit signed rollback needs both force and an administrator
`rollback_digests[target_key]` matching the exact candidate digest; signatures,
channel, pin and state readability are still checked. Unsigned dev retains the
existing clean commit-time guard. Force never relaxes signature or state checks.

The remote transaction locks a permanent `.rdev-upgrade.lock` inode in the actual
installation directory, independent of local aliases. Kernel ownership lasts
until process exit; it is never stolen by age or unlinked. Operations have a
45-second context, and individual hello checks have a 10-second deadline. Under
the lock, the observed installed digest is rechecked before replacement. A
concurrent winner therefore causes an explicit re-probe requirement.

Four private upload reservations bound pre-helper staging; each candidate is at
most 64 MiB. A dead owner plus ten-minute age and exact recognized layout allow
conservative stale-slot cleanup while the transaction lock is held. Unknown
objects and live/reused PIDs are retained. Exhausting slots rejects further
uploads, rather than deleting uncertain evidence. The journal and current release
record are fixed bounded files; one known-good previous binary is retained.

The state sequence is prepare → verify/real hello → durable switch intent →
atomic rename/fsync → installed digest/hello → commit → bounded cleanup. A
shared state writer lease prevents migration during this sequence. Health sends
only ping, checks actual platform/protocol/features, and inspects state without
migration or business mutation. Existing serving processes and detached jobs are
not killed. A replacement whose active-path health fails restores and rechecks
the previous bytes; an unverifiable rollback or lost transaction reply remains
ambiguous. Committed cleanup failure is reported as committed. Recovery examines
journal plus actual active/backup digests and never replays a business operation.
First installation has no prior known-good version; post-switch uncertainty
requires recovery, not a fictitious successful rollback.

New state writers and supervisors share `.migration.lock`; migrate/repair take
it exclusively and inspect again while locked. The inode is permanent and old
O_EXCL migrators refuse it. Future/corrupt state is preserved, and observers can
inspect it. A running new supervisor blocks migration but can still be observed
and stopped. **Old binaries cannot be retroactively fenced:** drain legacy
writers/jobs before the first migration. Binary rollback does not downgrade
state, ledger, Fleet plan/HostID/approval, redaction archive or sync outcome.
Backup and quarantine each allow at most 64 runs, 64 MiB and 16,384 entries;
preflight reserves actual encoded bytes including the existing manifest before
mutation. Exhaustion requires deliberate archival; no automatic retirement
removes recovery evidence. Full historical writer combinations remain pending.
Before first enabling managed signed installation, stop legacy client/broker
upgrade paths: their older same-UID shell installers do not obey the new lock or
signature journal, and old force-upload against managed state is unsupported.

## Executable validation entries

```sh
GOCACHE=/data/tmp/rdev-gocache GOMODCACHE=/data/tmp/rdev-gomodcache \
  make GO=/data/tmp/rdev-toolchain/go/bin/go all daemon check
RDEV_GO=/data/tmp/rdev-toolchain/go/bin/go sh scripts/fuzz-smoke.sh
python3 scripts/isolated-ssh.py --go /data/tmp/rdev-toolchain/go/bin/go --out /tmp/phase8-ssh-NEW
RDEV_RELEASE_OUT=/tmp/phase8-release-NEW make GO=/data/tmp/rdev-toolchain/go/bin/go release-gate
RDEV_GO=/data/tmp/rdev-toolchain/go/bin/go RDEV_REPRO_OUT=/tmp/phase8-repro-NEW sh scripts/reproducible-build.sh
```

Prepare/sign/verify a frozen audited candidate, using a separately authorized
signing identity and explicit RFC3339 dates:

```sh
go run ./scripts/releasecheck prepare DIR VERSION CHANNEL SIGNER ISSUED EXPIRES
go run ./scripts/releasecheck sign DIR AUTHORIZED_KEY_REFERENCE
go run ./scripts/releasecheck verify-signed DIR PRIVATE_POLICY
```

The default candidate is `0.1.0-dev.0`; set `RDEV_RELEASE_TAG` before building to
choose a different release version. Stable has no prerelease suffix; beta/dev use
`-beta.N` / `-dev.N`. Preparation is not publication. The workflow has read-only
repository permission, no signing secrets and no deployment/release step.
Hosted execution must have a real run ID/source/artifact binding before passing.

The scale supervisor's `--help` describes start/status/cancel/cleanup and exact
budgets. Final runs require clean source and matching immutable binaries, fixed
owners/namespaces, persisted run identity and real elapsed time. A smoke flag is
only for short fixture checks. Never clear mutation/job-start/outcome evidence,
retire redaction history, or recreate namespaces to prolong the test. Fixed
ledger limits and workload budgets remain visible; exhausted quota is not
reported as a memory leak or silently bypassed. Strict mixed-load SLO retains
the existing 2× control p95 threshold and runs without competing build/fuzz/load.
No completed 24h run is claimed. The current run identity and exact observed elapsed time are recorded with the final evidence below.


## Compatibility evidence boundary

The primary engineering predecessor is the actual Phase7 source
`e73a38bcd94c6ffb4dbe379578c3d46ff70fa50e` (production code `db9a260`). It is
not an invented N-1 formal release. `scripts/compatibility-matrix.py` clean-builds
that source, records actual binary digests, exercises old CLI/MCP with new
broker/agent, new CLI/MCP with old broker/agent for shared read/permission routes,
and upgrades broker/agent while retaining original supervisors and owner state.
Historical `remote-job-upgrade.sh` and `remote-audit-upgrade.sh` remain narrower
predecessor regressions; none implies all N/N-1 combinations are certified.

```sh
RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE=service-deploy \
RDEV_TEST_SSH_CONFIG=/data/tmp/rdev-validation/ssh/config \
RDEV_RELEASE_POLICY=/PRIVATE/dev-policy.json \
GOCACHE=/data/tmp/rdev-gocache GOMODCACHE=/data/tmp/rdev-gomodcache \
python3 scripts/compatibility-matrix.py --go /data/tmp/rdev-toolchain/go/bin/go \
  --out /tmp/phase8-compat-NEW
```

The old broker has no release-policy authority and is unsupported as a managed
signed deployment controller. Old force upload into a new managed namespace is
unsupported; drain old installers before adoption. New untrusted/unsigned agent
admission fails before business dispatch. A first connection rejection can be
recorded `not_sent`; an earlier dispatched mutation stays `ambiguous`. Broker v1
adds optional strictly validated `error_envelope`; legacy text remains available,
and CLI/MCP share typed projection without private SSH fallback.

Release decisions record version/digest/channel/unsigned/test-root/result in
bounded broker audit events, including Fleet and background connections. They
never record keys, policy contents or raw installer diagnostics. The current
observer-series count describes only the fixed-vocabulary `observe.Registry`,
not every possible process metric collector.
