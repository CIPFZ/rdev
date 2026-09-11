# Phase8 acceptance

Status: **In progress — not Phase8 Complete or Production Certified**.
The frozen product and local release artifacts bind
`047a0f53bb39b637b55c5aedec99c28ff0a0f8da`. Additional runtime test, harness and workflow changes are
recorded separately; CI builds have their own commit stamps and are not the same
bytes as this local candidate. The handoff baseline was `e73a38b`, clean and equal
to `origin/main` after fetch. No original platform target or gate is waived.

| Requirement | Accepted engineering evidence | Remaining acceptance |
|---|---|---|
| P8-01 | Strict SSHSIG manifest/trust policy, exact six-binary and metadata binding, linked-dependency notices, online source/binary vulnerability audits, two independent clean builds with six equal digests, full-bundle test signing and eight tamper refusals | Official authorized signer/root and corresponding final release identity; test root is not official |
| P8-02 | Namespace inode lock/staging/journal; nine actual 047/e73 installer groups, eleven SIGKILL barriers, seventeen record-recovery subcases; real test-root signed SSH upgrade/authorized rollback, state refusal and retained supervisors | Complete historical state/channel and platform combinations; official identity remains Gate 3 |
| P8-03 | Shared administrator verification, signed test-root CLI/broker/agent runtime, actual exact force/target/digest rollback and refusals, low-sensitivity audit and typed uncertainty | Full historical channel/rollback combinations and official identity deployment remain unverified |
| P8-04 | Local check/race, actual ten-target fuzz, real IPv4/IPv6/ProxyJump; actual DNS/master-loss and private tmpfs ENOSPC/EROFS added to hosted validation | Latest completed hosted run is recorded below; SSH application-stream delay/EOF/partial-frame tests added, packet-level fault coverage remains partial |
| P8-05 | Actual 100 independent sshd/agent targets and 20 clients: corrected 0bc1d93 mixed short run passed 308.664s, six fault stages, 220 completed mutations and 160 exact markers; immutable 047 bytes | New actual 24h run started, not passed. Full fairness/Fleet/in-flight/GC/metric coverage and physical-100 topology remain pending; failed earlier attempts are preserved separately |
| P8-06 | Eight actual source-defined cases: shared/standalone CLI/MCP, explicit legacy artifact refusal, retained jobs and Fleet approval/HostID/attempt/audit; an additional historical secret/sync outcome case is recorded separately below | Formal N-1 release identity; full signed-channel, migration and historical agent/state rollback matrix |

| Production Gate | Status and evidence boundary |
|---|---|
| 1. High/Medium findings closed | Passed for independently reviewed product source 047a0f5; no unresolved confirmed High/Medium and no agent-made risk acceptance. Subsequent harness/CI deltas are independently reviewed with their exact source bindings in the evidence. |
| 2. All declared Tier 1 runtime | Pending. Linux amd64/OpenSSH/XFS and macOS arm64 controller → Linux amd64 runtime are exercised. Linux arm64 and macOS remote-agent runtime remain unverified; cross-builds do not pass these targets. |
| 3. Official signature, SBOM, provenance, audits, notices | Blocked on authorized official signing identity/root. Local exact-byte audits/notices/SBOM/provenance and isolated test signatures passed. |
| 4. Real continuous 24h within budgets | Pending; run 5f68aaab657347af8bf3dc6a69589416 started at 2026-09-09 13:29:12.940 UTC. No completed 24h evidence; starting or supervising is not passing. |
| 5. Complete upgrade/rollback/crash/network/migration drills | Partial. Actual test-root signed SSH rollback, record/crash barriers, retained jobs and Fleet/audit recovery passed; complete historical state/migration/channel and platform matrix remains pending. |

The available environment is Linux amd64 / OpenSSH 9.3p2 / XFS, Go 1.26.8,
16 CPUs and about 32 GiB RAM, with an authorized Linux amd64 SSH endpoint.
The isolated-instance topology shares one kernel/filesystem/network fault domain;
physical machine count is unverified. A non-root SSH preflight also passed on
this Linux system; it does not certify the Ubuntu hosted runner. Actual hosted workflow identities and execution results are recorded below.
Anonymous API/log access remains
rate-limited or requires authentication; public run/step metadata is available.
No formal signer, arm64/Darwin runtime or authorized
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
identity, channels, validity and revocation state. The `public_key` value contains
exactly `ssh-ed25519 BASE64_KEY`, without options, a display comment or a newline.
SSHSIG verifies publisher
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
# Clean source and its complete unsigned dev.0 release from release-gate:
python3 scripts/prepare-signed-test-releases.py --go /data/tmp/rdev-toolchain/go/bin/go \
  --out /tmp/phase8-test-signatures-NEW --frozen /tmp/phase8-release-NEW
# The generated ROLE-policy.json files feed the signed ProxyJump command below.
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
No completed 24h run is claimed. The current run identity and exact observed elapsed time are recorded with the evidence below.

## Recorded validation

[Bounded evidence](evidence/phase8/final-validation.json) records exact commands,
UTC times, sources, binary/metadata digests, instrumentation, independent reviews,
failed attempts and pending gates. The [compatibility matrix](evidence/phase8/compatibility-matrix.json)
separates actual component combinations from test release-protocol cases. Raw
logs, samples, artifacts and private state remain outside Git.

The frozen 047 candidate passed `make all daemon check`, whole-repository race,
online source/six-binary vulnerability audits, release verification, two clean
build comparisons, and actual test-root signing with eight tamper refusals.
Signed SSH runtime passed 18 top-level groups without skips. The earlier runtime
race group instruments local CLI, daemon and harness; remote agents remain
uninstrumented release bytes, and the Secrets helper builds an ordinary CLI.
Strict mixed-load control ping/status p95 ratios were 1.069/1.059 against the
unchanged 2× bound. Ten real fuzz targets passed the 10-second-per-target rerun;
the earlier frame-fuzz deadline failure remains recorded.

The ec8c201 extension changes tests, harnesses and CI; product inputs still match
047. Independent non-author reviewers accepted exact source blobs and actual
results. Its local evidence includes:

| Actual verification | Result and boundary |
|---|---|
| Real installer and recovery | Nine groups passed using clean 047/e73 native agents. Eleven named SIGKILL barriers plus seventeen disk-state reconstruction subcases cover decision/journal gaps, same-byte trust activation and partial/unsafe scratch. Active inode, predecessor and recovery evidence remain intact. Reconstruction is not extra SIGKILL or physical power-loss evidence. |
| Disk faults | Four kernel-enforced ENOSPC/EROFS first/replacement cases passed in private tmpfs/bind mounts; failed staging preserved active/state bytes and retry committed. No business filesystem was mounted. |
| DNS and ControlMaster | Actual resolver failure, master SIGKILL, changed serving PID, retained detached supervisor and completed mutation, exact marker and other-project denial passed, including IPv4/IPv6 ProxyJump. |
| SSH stream faults | Barrier-delayed terminal, application-stream one-way EOF and partial terminal passed on the authorized endpoint and both ProxyJump targets. Independent control progresses while bulk is held. Restart retains completed/ambiguous outcomes and single marker `x`; duplicate mutation is refused. This is not TCP packet-loss certification. |
| Signed SSH upgrade/rollback | Clean ec8c201 test source passed both ProxyJump targets without skips. Actual 563/dev.0 → 047/dev.1 → 563/dev.0 test releases exercise force/target/digest authorization, four incorrect/missing grant combinations, future/corrupt state refusal, version-reuse refusal and exact-byte reconcile. Job/mutation/marker and forward-recovery bytes remain intact. |
| Harness regressions | All 28 Python tests passed, including nine new offline-report regressions. The new stream runtime also passed with a race-instrumented local Go harness; its actual CLI/broker/agents remain uninstrumented 047 bytes. |

The signed fixtures are distinct clean source builds with explicit **test**
versions and an isolated root. The separately built 047/dev.1 bundle passed full
release audit, metadata and signing checks; it did not replace the frozen
047/dev.0 or soak inputs. Agent implementation is identical between these signed
fixtures; the actual product difference is two broker files. Their successful
release-protocol test does not certify historical agent/schema migration or a
formal N-1 release. Library-only signed decision reconstruction also remains
separate from actual signature verification.

Hosted run [34365260787](https://github.com/CIPFZ/rdev/actions/runs/34365260787)
on 95bdca0 passed check, real installer/storage, IPv4/IPv6/ProxyJump, full race,
fuzz, Python, online release audit and independent reproducibility. Its public
runner annotation binds six binary and four metadata digests to source/run/attempt;
these are runner-reported identities, not an independently downloaded archive.
The extended ec8c201 run [34368166512](https://github.com/CIPFZ/rdev/actions/runs/34368166512)
failed its new signed preparation/runtime step. The same clean source reproduced
the preparation failure locally: the generated public key's display comment
violated the strict two-field policy contract. The fixture correction in 2245361
preserves that contract. Thirty Python regressions, independent old-refusal/new-pass
verification of the same signature, full clean three-bundle preparation and actual
signed IPv4/IPv6 ProxyJump upgrade/rollback passed. Exact candidate/previous bytes,
test identity, all six negative cases per target and cleanup are recorded.
The corrected hosted run [34369741453](https://github.com/CIPFZ/rdev/actions/runs/34369741453)
passed the complete workflow, including signed SSH and evidence upload. Its six
binary digests also match the independently verified local 224 release. The
public combined signature annotation was truncated at GitHub's message limit;
the preserved partial text is not treated as a complete identity JSON. The
4b6b30c workflow emits four bounded, independently source/run-bound annotations.
Its actual run [34371245934](https://github.com/CIPFZ/rdev/actions/runs/34371245934)
passed all validation and evidence-upload steps. All four public identity records
parse completely; the signed frozen bundle's six binaries and four metadata files
match the audited hosted release. These are public runner records; the uploaded
archive was not independently downloaded. The added historical state case ran
locally at ad32968, separately from this hosted workflow.
CI creates only an ephemeral test root, retains
its public identity/signatures/metadata/binaries, deletes the private key, and
never publishes or deploys.

Earlier failed attempts remain failed in the JSON evidence. In particular,
linked-worktree Go builds lacked predecessor VCS identity; a clean clone fixed
the fixture without weakening checks. OpenSSH's temporary socket suffix exceeded
the long runner path; short private controls fixed it. Extended runner-home ACLs
correctly rejected policy copies; safe private `/tmp` policy locations passed
real `setfacl` regressions with ancestor ACLs unchanged. The new stream fixture
initially confused ledger `ambiguous` with protocol `possibly_executed`; only its
assertion changed. None of these failures is silently counted as passed.

## Compatibility and rerun commands

The engineering predecessor is actual Phase7 e73a38b (product db9a260), not an
invented formal N-1. At clean source 9b6c430 the complete eight-case entry passed
with exact 047/e73 binaries: shared and standalone CLI/MCP, new broker rejection
of a legacy local artifact without release identity, old broker/new agent use,
new standalone authorized upgrade and old standalone refusal before dispatch.
Fleet transitions e73 → 047 → e73 broker-only → 047 preserved six predecessor
audit-event fingerprints, HostIDs, approval policy/expiry/consumption and exact
attempt/operation/job IDs; six markers remained single `x`. Terminal execute
returned its existing receipt. Cases own and clean their transports; standalone
CLI additionally needs a private mount namespace. Historical job/audit scripts
remain narrower valid evidence.

The additional `secret-sync-outcome-upgrade-continuity` case at clean ad32968
uses actual e73/047 artifacts. Its five stages exercise recovery of a remote
completed push whose broker response was held before ACK, new broker restart,
new agent SIGKILL/reconnect, old broker-only rollback and new broker restoration.
Four archive records (two active/two retired), all eight tracked original mutation
identities, two remote push outcomes and one local pull outcome remain intact.
Real historical output stays redacted for both projects; retired/foreign secrets
cannot be injected. Consumed sync replay is refused, and business bytes/inode/mtime
remain unchanged. Additional injection probes create their own mutations; eight
is the tracked set, not the total number of successful operations. Business sync
targets and the agent state tree are separate subtrees. This case does not roll
back the agent or migrate a schema, and is separate from the earlier eight-case
run. Its exact result, policy scope and independent review are in the JSON evidence.
The current runner includes it by default; use
`--case secret-sync-outcome-upgrade-continuity` to run only this addition.

```sh
GOCACHE=/data/tmp/rdev-gocache GOMODCACHE=/data/tmp/rdev-gomodcache \
RDEV_RUN_REMOTE=1 RDEV_TEST_REMOTE=service-deploy \
RDEV_TEST_SSH_CONFIG=/data/tmp/rdev-validation/ssh/config \
RDEV_RELEASE_POLICY=/PRIVATE/valid-dev-policy.json \
python3 scripts/compatibility-matrix.py --go /data/tmp/rdev-toolchain/go/bin/go \
  --out /tmp/phase8-compat-NEW \
  --artifact-source 047a0f53bb39b637b55c5aedec99c28ff0a0f8da \
  --current-artifacts /tmp/rdev-p8-final-047a0f5/release \
  --previous-artifacts /tmp/rdev-p8-compat-047a0f5/previous

# Clean source, still-valid private test policies and a new output directory:
python3 scripts/signed-upgrade-ssh.py --go /data/tmp/rdev-toolchain/go/bin/go \
  --out /tmp/phase8-signed-NEW --remote service-deploy \
  --ssh-config /data/tmp/rdev-validation/ssh/config \
  --previous-policy /tmp/rdev-p8-final-56351b7/signed-test-policy.json \
  --candidate-policy /tmp/rdev-p8-signed-candidate-047-dev1/signed-test-policy.json \
  --frozen-policy /tmp/rdev-p8-final-047a0f5/signed-test-policy.json
# Supply these as --signed-ROLE-policy to scripts/isolated-ssh.py to run both
# real local IPv4/IPv6 ProxyJump targets. No draft flag is used for acceptance.
```

`real-agent-install.py --help` lists required native current/predecessor paths,
full commits and expected digests; compile outside the checkout with `go test -c -o
/tmp/rdev-real-agent-record-recovery.test ./internal/agentinstall`. `storage-faults.py --help` uses the same identities,
computes digests and requires a private mount namespace. Exact complete
invocations are in the JSON evidence. The old broker has no release-policy
authority and cannot control managed signed deployment. Drain legacy installers
before adoption; force is not a trust bypass. Unsupported shared routes never
fall back to private SSH. Legacy broker text errors remain available alongside
strict optional typed envelopes; earlier ambiguous mutations remain ambiguous.

## Current scale and continuous soak

Corrected frozen harness 0bc1d93 passed short run
`45c8ca75371441c69bf46de16e3aa599`: 308.664 seconds, 20 clients completing mixed
cycles, 4,385 pings, 230 expected fault errors, zero unexpected errors, 220 durable
completed mutations and 160 exact markers. Six fault stages recovered; cleanup
left no managed process. Peak RSS was 4,290,998,272 bytes, FD 2,940, processes 494,
managed disk 453,555,387 bytes and goroutines 142, within predeclared budgets.
The 100 targets are separate sshd ports/processes and agent/install/state/business
directories under one account/kernel/filesystem. They are not containers, VMs or
100 physical machines. Preceding failed/draft runs remain separately recorded.

Retained sync and wait share existing 64 MiB global/32 MiB owner ingress limits.
The workload admits one such large response at a time; this proves neither
saturated sync throughput nor fairness. Loggers rotate every two 20-minute cycles,
with at most 2350 seconds logging under a 3500-second wall envelope. There are
nominal 50-second-plus scheduling gaps, not one continuous 24h job. The daily
mutation budget is 7080 (5040 ordinary + 1440 logger + 600 Fleet), below global
8192/per-owner1024 caps. Same-principal credentials renew after 12 hours from
issue time with 24h TTL. No state is cleared or namespace recreated; this daily
budget does not prove indefinitely sustainable deduplication storage.

Actual 24h run `5f68aaab657347af8bf3dc6a69589416` persists at
`/tmp/rdev-p8-soak-0bc1d93`, supervisor PID667709/start ticks192573725. Workload
started **2026-09-09 13:29:12.940 UTC**; the earliest nominal 24h point is
**2026-09-10 13:29:12.940 UTC**, followed by final idle/cleanup evaluation.
The JSON evidence has a timestamped observation, never an advance pass.
`run.json`, `status.json`, `samples.jsonl`, `supervisor.log`, workers and Fleet
results retain progress. Resume observation across sessions; do not restart it
or splice failed/short durations into it.

```sh
python3 scripts/scale-soak.py status --run /tmp/rdev-p8-soak-0bc1d93
python3 scripts/scale-soak.py cancel --run /tmp/rdev-p8-soak-0bc1d93
python3 scripts/soak-report.py --run /tmp/rdev-p8-soak-0bc1d93 \
  --out /tmp/phase8-soak-observation-NEW.json
# A separate run requires fresh private output and the same immutable inputs.
python3 scripts/scale-soak.py start --run /tmp/phase8-short-NEW \
  --artifacts /tmp/rdev-p8-final-047a0f5/scale-artifacts \
  --artifact-source 047a0f53bb39b637b55c5aedec99c28ff0a0f8da \
  --go /data/tmp/rdev-toolchain/go/bin/go \
  --seconds 300 --targets 100 --clients 20 --workload mixed --broker-crash --faults
```

Only after short correctness/capacity acceptance should a separate final run use
`--seconds 86400`. `cancel` preserves state and stops recorded identities;
`cleanup` destructively removes a stopped fixture only after required evidence
is archived. Strict SLO runs must have uncontended resources and keep the 2×
control-p95 bound; do not run another scale load beside the current soak.

The independently checked offline reporter never alters this run or reads
credentials, ledger bodies or business payloads. It streams a fixed sample
prefix, records incomplete appends, reports cumulative histogram intervals and
per-worker counts, and separates dial-counter epochs and phased/idle resources.
These are asynchronous observations. Warm samples remain sparse; CPU misses
exiting-process final ticks; storage slopes are descriptive. No new SLO or
production verdict is inferred. Explicit `job_rm` is not autonomous retention GC;
shared broker has no `storage_gc` route or certified cleanup-interval consumer.
Saturated fairness, full Fleet/in-flight fault interleaving, retention/metric
coverage and complete platform/formal identity requirements remain open.
