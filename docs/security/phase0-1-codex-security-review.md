# Phase 0–1 Codex Security Review

## Review identity

- Reviewed commit: `c396e9932a2757ea7331363dac3888c98bb82405`
- Parent: `11e9c588c857277b768799fbf5d62c223697ff6e`
- Scan ID: `1e735fd4-53ed-455a-816b-89a6b4bdbd7f`
- Snapshot: `codex-security-snapshot/v1:sha256:557594e4102e1d0925d1f3f598e93b16b7a04b25c19458ceb060a17044f642fb`
- Review date: 2026-08-27
- Scope: the complete Phase 0–1 diff, local tests, race tests, vet, and generated-agent consistency checks

This is the repository-maintained projection of the immutable Codex Security
report. It intentionally contains no developer credential or private-key paths.
The original scan reported one medium and three low findings, all high confidence.

## Threat boundaries retained by maintainers

- Repository paths and `.rdev/hosts.json` are untrusted until the exact canonical
  path and exact bytes are approved.
- Global configuration and project trust are authorization inputs owned by the
  current local OS user. Other local users and writable ACL principals are not trusted.
- SSH destination, port, remote state paths, rsync operands, and bootstrap values
  remain data after approval and may not become local options or shell source.
- A selected remote and concurrent processes under the same remote account are
  untrusted with respect to rdev-managed staging objects and local resource use.
- Connection reuse is permitted only for the immutable canonical host identity
  and its current Registry generation.

## Findings and disposition

### CSF-1 — Host alias redefinition reuses the old SSH identity (medium)

The alias-only pool key allowed a warm connection to retain an old destination,
port, or `RemoteDir` after a Registry replacement. The fix binds every pooled
connection to a canonical fingerprint plus a monotonic generation. Registry
publication invalidates the alias, dials recheck identity before and after pool
publication, and a superseded in-flight dial is closed rather than returned.
Regression coverage exercises direct updates, project approval, concurrent
lookup, and exec/read/write/rsync consumers.

### CSF-2 — ApproveProject publishes before trust persistence (low)

Approval previously merged project hosts before reading and durably replacing
the trust store. The fix parses into an immutable staged snapshot, serializes all
Registry mutations through the transaction lock, persists trust first, and then
publishes hosts, scopes, state, approval, generations, and invalidations together.
Injected read, marshal, write, file-fsync, rename, and directory-fsync failures
leave the old live snapshot active. The POSIX writer also rolls the trust file
back if directory fsync fails after rename.

### CSF-3 — Bootstrap staging lacks exclusive no-follow creation (low)

The PID-derived staging pathname and `dd of=...` open did not establish object
identity. Bootstrap now uses a cryptographically random staging directory,
exclusive `mkdir`, a shell noclobber file descriptor kept open through the
upload, and regular-file/owner/link-count/inode checks before and after writing.
The digest is verified before chmod and atomic rename. Cleanup is armed only
after rdev owns the staging directory. Tests cover pre-existing links, regular
and non-regular objects, digest failure cleanup, and concurrent complete installs.

### CSF-4 — Config/trust reads omit owner and mode validation (low)

POSIX reads now `fstat` the already-open directory and file descriptors, require
effective-UID ownership, reject group/other write bits, and reject recognized
POSIX/macOS ACL xattrs. Existing current-user-owned `0755` directories and `0644`
files remain readable for compatibility; all new writes remain `0700`/`0600`.
Windows remains build-only: it rejects visible links but does not claim POSIX
no-follow, owner, mode, or ACL guarantees.

## Independent validation notes

The independent test task passed clean build, formatting, unit tests, race tests,
vet, four-platform agent reproducibility, adversarial destination/path/trust/link
cases, and real Ubuntu amd64 bootstrap/exec/read/write/rsync. A single isolated
Claude Code invocation successfully used the rdev MCP server for a read-only
remote operation. Test state was confined to a unique remote directory and was
verified absent after cleanup.

The same run recorded four non-Phase-1 product gaps for later acceptance:
native `IdentityFile`/`IdentitiesOnly` configuration, CLI expression of a bare
leading-dash local rsync operand, CLI visibility of output truncation, and
protocol propagation of local cancellation. These are tracked in the evolution
plan and were not implemented as Phase 3 work in this batch.

## Residual scope

Phase 2+ items such as host-scoped secrets, pre-publication secret initialization,
non-idempotent replay semantics, response/resource bounds, job identity/ownership,
ControlMaster directory hardening, and broker capability isolation remain open.
They are not accepted risks and must satisfy their later phase gates.

## Second-round remediation addendum

Follow-up verification found that the first remediation still held the Registry
global read lock for an operation lifetime, used localized Darwin/GNU `stat`
type text during bootstrap, left a pathname identity interval between digest and
rename, treated post-commit backup cleanup as an uncommitted approval failure,
and queried Darwin ACLs by pathname. The follow-up patch replaces those controls
with sorted per-alias leases, numeric plus `test -f` regular-file checks,
fd/inode-bound digest and hard-link publication with rollback, an explicit
directory-fsync commit point with typed committed/ambiguous outcomes, and
`acl_get_fd_np` inspection (fail-closed without cgo). Host-secret reads during
connection initialization remain a Phase 2 item; the Phase 1 lease claim covers
the public exec/read/write/sync sinks only. The patch has implementation-task
tests and a fresh Ubuntu first-bootstrap run, but remains pending independent
review and is not recorded here as a completed batch.

## Final targeted-remediation addendum

Final verify-fix found that the bootstrap cleanup trap still treated every
post-publication failure alike: it could overwrite or remove a concurrently
replaced target, ignored rollback/cleanup failures, and had no typed distinction
between an uncertain rollback and a committed install with residue. Bootstrap
now uses an explicit `STAGED → VERIFIED → INSTALLING → COMMITTED` machine. It
retains publication and prior-inode hard links, quarantines only a target that
still matches the publication inode, restores the prior inode with no-replace
linking, verifies its inode and digest, and preserves evidence when any rollback
step is uncertain. Post-commit backup or directory cleanup produces a typed
committed warning. Fault tests cover validation failure, rollback move/remove
failure, restored-inode mismatch, concurrent first-install target replacement,
and post-commit backup/rmdir failure.

The same verification also found that CLI and MCP callers treated
`ConfigWriteCommittedError` as approval failure even though Registry and disk
already contained `Approved=true`. Both frontends now return success with the
approved state and an observable warning; ambiguous and pre-commit errors remain
failures. These changes remain pending final independent targeted verification,
so this archive does not mark Batch A complete.

## Final publication-window addendum

The last targeted verification identified two ordering intervals in the remote
bootstrap transaction. Recovery links (`failed`, `published`, and `old`) were
removed before the restored target's final fd/inode/digest check, and a signal
could arrive after first-install no-replace publication but before the shell
recorded `INSTALLING`. Recovery now reaches its verified rollback commit point
before removing any evidence. A pathname replacement during the final check
therefore returns a typed ambiguous outcome with the old, failed-publication,
and publication-proof inodes retained.

First publication now sets a pending-publication flag before `link(2)`, records
`published_by_us` before changing state, and clears the pending flag only after
`INSTALLING` is recorded. The signal trap reconciles the flag with the published
inode instead of relying on state alone; an interruption anywhere in that
boundary preserves both the visible target and staging proof and reports a typed
ambiguous outcome. HUP, INT, and TERM also carry explicit nonzero statuses into
the trap. Deterministic tests stop the publication link at that boundary and
replace the pathname during the final rollback check. This patch remains pending
the requested final independent targeted verification; Batch A is still not
marked complete.
