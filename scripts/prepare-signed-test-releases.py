#!/usr/bin/env python3
"""Build isolated, actually signed test releases. Never create an official root."""
import argparse
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

PREVIOUS = "56351b738f1f1aaa89aa4d788858af078b82de30"


def policy_public_key(public):
    # ssh-keygen appends a display comment. Policy roots deliberately contain
    # only algorithm and key; never loosen the product parser for this fixture.
    lines = public.strip().splitlines()
    fields = lines[0].split(maxsplit=2) if len(lines) == 1 else []
    if len(fields) < 2 or fields[0] != "ssh-ed25519":
        raise ValueError("expected one generated Ed25519 public key")
    return " ".join(fields[:2])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", required=True)
    parser.add_argument("--out", required=True, type=Path, help="new private directory outside the repository, with safe policy ancestors")
    parser.add_argument("--frozen", required=True, type=Path, help="existing complete audited dev.0 bundle for the current clean source")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=repo):
        parser.error("clean source required")
    out = args.out.resolve()
    frozen = args.frozen.resolve(strict=True)
    if out == repo or repo in out.parents or out == frozen or frozen in out.parents:
        parser.error("output must be separate from source and frozen input")
    os.umask(0o077)
    out.mkdir(mode=0o700)
    (out / "bundles").mkdir(mode=0o700)
    env = dict(os.environ, GOMAXPROCS="2", GOFLAGS="-p=1")
    spec = importlib.util.spec_from_file_location("isolated_ssh", repo / "scripts/isolated-ssh.py")
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)
    report = {"schema_version": 1, "status": "running", "source_commit": source,
              "previous_source_commit": PREVIOUS, "official_identity": False,
              "scope": "isolated test versions and ephemeral test signing root; not a formal N/N-1 release or deployment",
              "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "steps": [], "bundles": {}}
    def save():
        (out / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    def run(name, command, cwd=repo, extra=None):
        row = {"name": name, "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        with (out / (name + ".log")).open("wb") as log:
            try:
                harness.run(command, cwd=cwd, env={**env, **(extra or {})}, stdout=log, stderr=subprocess.STDOUT, timeout=900)
                code = 0
            except subprocess.CalledProcessError as error:
                code = error.returncode
        row.update(exit_code=code, completed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
        report["steps"].append(row)
        save()
        if code:
            raise RuntimeError("test release step failed: " + name)
    save()
    key = out / "ephemeral-test-key"
    try:
        # Copy first: signing this fixture must not modify the audited input.
        shutil.copytree(frozen, out / "bundles/frozen")
        frozen_manifest = json.loads((out / "bundles/frozen/manifest.json").read_text())
        if frozen_manifest["source"]["commit"] != source or frozen_manifest["source"]["dirty"]:
            raise RuntimeError("frozen release does not match clean current source")
        for role, commit, version in (("previous", PREVIOUS, "0.1.0-dev.0"), ("candidate", source, "0.1.0-dev.1")):
            clone = out / (role + "-source")
            run(role + "-clone", ["git", "clone", "--quiet", "--no-hardlinks", "--no-local", str(repo), str(clone)])
            run(role + "-checkout", ["git", "checkout", "--quiet", "--detach", commit], cwd=clone)
            run(role + "-release", ["make", "GO=" + args.go, "release-gate"], cwd=clone,
                extra={"RDEV_RELEASE_OUT": str(out / "bundles" / role), "RDEV_RELEASE_TAG": version})
        run("compile-verifier", [args.go, "build", "-o", str(out / "releasecheck"), "./scripts/releasecheck"])
        verifier = str(out / "releasecheck")
        run("create-test-key", ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "phase8-ephemeral-test-only", "-f", str(key)])
        public = policy_public_key(key.with_suffix(".pub").read_text())
        fingerprint = subprocess.check_output(["ssh-keygen", "-lf", str(key.with_suffix(".pub")), "-E", "sha256"], text=True, timeout=10).split()[1]
        report["signing_identity"] = {"id": "phase8-isolated-test", "test_only": True,
                                      "public_key": public, "fingerprint": fingerprint}
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        issued = (now - datetime.timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        expires = (now + datetime.timedelta(days=2)).isoformat().replace("+00:00", "Z")
        for role, version in (("previous", "0.1.0-dev.0"), ("candidate", "0.1.0-dev.1"), ("frozen", "0.1.0-dev.0")):
            bundle = out / "bundles" / role
            run(role + "-verify", [verifier, "verify", str(bundle)])
            run(role + "-prepare", [verifier, "prepare", str(bundle), version, "dev", "phase8-isolated-test", issued, expires])
            run(role + "-sign", [verifier, "sign", str(bundle), str(key)])
            policy = {"schema_version": 1, "valid_until": expires, "bundle_dir": str(bundle), "channels": ["dev"],
                      "allow_unsigned_dev": False, "allow_test_roots": True,
                      "roots": [{"id": "phase8-isolated-test", "public_key": public, "channels": ["dev"],
                                 "not_before": issued, "not_after": expires, "revoked": False, "test_only": True}]}
            path = out / (role + "-policy.json")
            path.write_text(json.dumps(policy, indent=2) + "\n")
            run(role + "-verify-signed", [verifier, "verify-signed", str(bundle), str(path)])
            manifest = json.loads((bundle / "release.json").read_text())
            report["bundles"][role] = {"policy": str(path), "version": version, "source_commit": manifest["source"]["commit"],
                "digests": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [bundle / item["name"] for item in manifest["binaries"] + manifest["metadata"]] + [bundle / "release.json", bundle / "release.json.sig"]}}
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["failure_type"] = type(error).__name__
        raise
    finally:
        # No signing key is needed by the subsequent runtime or artifact upload.
        key.unlink(missing_ok=True)
        report["private_test_key_removed"] = not key.exists()
        report["completed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
    print(json.dumps({"status": report["status"], "result": str(out / "result.json"), "official_identity": False}))


if __name__ == "__main__":
    main()
