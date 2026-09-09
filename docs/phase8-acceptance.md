# Phase8 acceptance

Status: **In progress — not Production Certified**. Baseline `e73a38b` matched
`origin/main` after fetch, with a clean workspace. Phase5/6/7 evidence remains
valid for unchanged behavior; new acceptance must bind the final Phase8 source.
macOS runtime is explicitly deferred, and all original Tier 1 targets remain
required. Cross-builds and isolated signing keys are never production evidence.

| Requirement | Current engineering / acceptance boundary |
|---|---|
| P8-01 | SSHSIG release manifest, shared runtime verifier, exact metadata/build binding, third-party notice collection and independent-build entry implemented; final clean build/audit/reproducibility and independent review pending. Official signer/root not supplied. |
| P8-02 | Remote inode lock, bounded staging, journal, real hello/features/state health, atomic switch and verified rollback implemented; final real crash/upgrade drills and review pending. Legacy non-lease writers require maintenance. |
| P8-03 | Administrator stable/beta/dev policy, signer validity/revocation, version pin, host restrictions and exact rollback authorization implemented; final frontend/audit/support validation pending. |
| P8-04 | Isolated IPv4/IPv6/ProxyJump, actual fuzz and read-only CI workflow implemented; final clean runtime and hosted run pending. |
| P8-05 | Real isolated SSH scale/mixed-load supervisor implemented and under review. Short smoke tests are fixture evidence. Final 100-target/20-process run and continuous 24h acceptance not run. |
| P8-06 | Explicit source/schema/protocol matrix in preparation; historical upgrade fixtures retained. Full N/N-1 and state rollback matrix not yet accepted. |

Production Gates remain pending: independent High/Medium finding closure; actual
Tier 1 runtime; official artifact identity/audit/notices; continuous 24h within
budgets; and complete upgrade/rollback/crash/network/migration drills. No finding
has been accepted as a risk by the agent. No release or deployment is authorized.

The available environment is Linux amd64 / OpenSSH 9.3p2, Go 1.26.8, Docker and
sshd, plus the existing authorized `service-deploy` Linux SSH target. `/tmp` has
sufficient space for isolated artifacts; `/data` had only about 5.4 GiB free.
No formal signing identity, hosted-run API authorization, arm64/Darwin machine or
100-machine topology has been supplied. A one-machine isolated-instance test
reports its shared kernel/user/storage/network fault domain explicitly.

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
No final 24h run has yet started.


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
