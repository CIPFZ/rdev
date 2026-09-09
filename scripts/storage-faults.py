#!/usr/bin/env python3
"""Real Linux ENOSPC/EROFS installer-entry checks in a private mount namespace.

Requires root/CAP_SYS_ADMIN, util-linux, and two identified native agent builds.
Uses at most 96 MiB tmpfs, no loop device, existing filesystem or business state.
The helper receives an already-authorized unsigned-dev decision: this checks the
real binary installation boundary, not SSH upload or publisher authorization.
"""
import argparse
import datetime
import errno
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, timeout=60, **kwargs)


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def identity(go, path, commit):
    path = path.resolve(strict=True)
    if not re.fullmatch(r"[0-9a-f]{40}", commit) or not path.is_file() or path.stat().st_size > 64 << 20:
        raise ValueError("full source commit and bounded native binary required")
    metadata = subprocess.check_output([go, "version", "-m", str(path)], text=True, timeout=30)
    fields = [line.strip().split("\t") for line in metadata.splitlines()[1:]]
    settings = dict(parts[1].split("=", 1) for parts in fields if len(parts) == 2 and parts[0] == "build" and "=" in parts[1])
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine())
    if ["path", "github.com/CIPFZ/rdev/cmd/rdev-agent"] not in fields or not arch:
        raise ValueError("actual native Go agent required")
    if any(settings.get(key) != value for key, value in {"vcs.revision": commit, "vcs.modified": "false", "GOOS": "linux", "GOARCH": arch}.items()):
        raise ValueError("binary/source/platform identity mismatch")
    return {"path": str(path), "source_commit": commit, "sha256": digest(path), "size": path.stat().st_size}


def invoke(binary, root, old, expected=None):
    decision = json.dumps({"version": "", "channel": "dev", "digest": binary["sha256"], "test_root": False, "unsigned": True, "rollback": False})
    result = subprocess.run([binary["path"], "-install-candidate", str(root), decision, old], capture_output=True, text=True, timeout=50)
    marker = result.stderr.strip()
    if result.stdout or (expected is None and (result.returncode or marker)) or (expected is not None and (result.returncode != 1 or marker != expected)):
        raise RuntimeError("installer result differs from declared outcome: " + repr((result.returncode, marker[:256])))
    return marker or "committed"


def fill_to_errno(path):
    # Buffered streams can move ENOSPC into close(); use bounded direct writes.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    written = 0
    try:
        while written <= 96 << 20:
            try:
                written += os.write(fd, b"x" * (1 << 20))
            except OSError as error:
                if error.errno != errno.ENOSPC:
                    raise
                return written
        raise RuntimeError("private tmpfs capacity bound was not enforced")
    finally:
        os.close(fd)


def worker(config):
    spec = json.loads(config.read_text())
    if os.readlink("/proc/self/ns/mnt") == spec["parent_mount_namespace"]:
        raise RuntimeError("refusing storage faults outside the private mount namespace")
    out = config.parent
    report = spec["report"]
    current, previous = report["binaries"]["current"], report["binaries"]["previous"]
    for binary in (current, previous):
        if digest(binary["path"]) != binary["sha256"]:
            raise RuntimeError("immutable input changed")
    mount = out / "private-tmpfs"
    mount.mkdir(mode=0o700)
    mounted = False
    try:
        run(["mount", "-t", "tmpfs", "-o", "size=96m,mode=0700,nosuid,nodev", "rdev-private-storage-fault", str(mount)])
        mounted = True
        report["worker_mount_namespace"] = os.readlink("/proc/self/ns/mnt")
        for fault in ("disk-full", "read-only"):
            for replacement in (False, True):
                case = {"name": fault + ("-replacement" if replacement else "-first-install"), "status": "running", "started_at": utc()}
                report["cases"].append(case)
                save(out / "result.json", report)
                root = mount / case["name"]
                root.mkdir(mode=0o700)
                manifest = b'{"schema_version":1,"writer_version":"storage-fault-fixture","namespace":"private-tmpfs"}\n'
                (root / "manifest.json").write_bytes(manifest)
                (root / ".rdev-upgrade.lock").touch(mode=0o600)
                old = previous["sha256"] if replacement else ""
                if replacement:
                    shutil.copyfile(previous["path"], root / "rdev-agent")
                    (root / "rdev-agent").chmod(0o700)
                bound = False
                try:
                    if fault == "disk-full":
                        fill = mount / "fill"
                        used = fill_to_errno(fill)
                        if used < 1 << 20:
                            raise RuntimeError("unexpectedly small private capacity")
                        # Leave enough for the journal, but less than candidate bytes.
                        os.truncate(fill, used - (64 << 10))
                        case["kernel_errno"] = "ENOSPC"
                        case["available_before_install"] = os.statvfs(root).f_bavail * os.statvfs(root).f_frsize
                        case["outcome"] = invoke(current, root, old, "RDEV_AGENT_INSTALL_NOT_SENT:stage")
                        staged = root / ".rdev-agent.new"
                        if not 0 < staged.stat().st_size < current["size"]:
                            raise RuntimeError("disk-full did not exercise an actual partial candidate staging write")
                        case["partial_stage_bytes"] = staged.stat().st_size
                    else:
                        run(["mount", "--bind", str(root), str(root)])
                        bound = True
                        run(["mount", "-o", "remount,bind,ro", str(root)])
                        try:
                            (root / "write-probe").write_bytes(b"probe")
                        except OSError as error:
                            if error.errno != errno.EROFS:
                                raise
                        else:
                            raise RuntimeError("mount did not enforce EROFS")
                        case["kernel_errno"] = "EROFS"
                        case["outcome"] = invoke(current, root, old, "RDEV_AGENT_INSTALL_NOT_SENT:lock")
                    actual = digest(root / "rdev-agent") if (root / "rdev-agent").exists() else ""
                    if actual != old or (root / "manifest.json").read_bytes() != manifest:
                        raise RuntimeError("failed install changed active bytes or state")
                    case["active_and_state_preserved"] = True
                finally:
                    if bound:
                        run(["umount", str(root)])
                    if (mount / "fill").exists():
                        (mount / "fill").unlink()
                # Recover the actual incomplete transaction before retrying it.
                run([current["path"], "-recover-upgrades", str(root)], capture_output=True)
                invoke(current, root, old)
                if digest(root / "rdev-agent") != current["sha256"] or (root / "manifest.json").read_bytes() != manifest:
                    raise RuntimeError("retry did not commit exact candidate and preserve state")
                if replacement and digest(root / ".rdev-agent.previous") != old:
                    raise RuntimeError("retry lost predecessor backup")
                if json.loads((root / ".rdev-upgrade.json").read_text())["phase"] != "committed":
                    raise RuntimeError("retry commit journal missing")
                case.update(status="passed", recovery_and_retry="committed", completed_at=utc())
                save(out / "result.json", report)
                shutil.rmtree(root)
        report["status"] = "passed"
    except BaseException:
        report["status"] = "failed"
        if report["cases"] and report["cases"][-1]["status"] == "running":
            report["cases"][-1].update(status="failed", completed_at=utc())
        raise
    finally:
        if mounted:
            run(["umount", str(mount)])
        mount.rmdir()
        report.update(completed_at=utc(), mounts_remaining=0)
        save(out / "result.json", report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go")
    parser.add_argument("--current", type=Path)
    parser.add_argument("--current-commit")
    parser.add_argument("--previous", type=Path)
    parser.add_argument("--previous-commit")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args.worker.resolve(strict=True))
        return
    if platform.system() != "Linux" or os.geteuid() != 0:
        parser.error("Linux root/CAP_SYS_ADMIN required; no simulated fallback")
    if any(getattr(args, name) is None for name in ("go", "current", "current_commit", "previous", "previous_commit", "out")):
        parser.error("all binary/source/go/output arguments are required")
    for tool in ("unshare", "mount", "umount"):
        if shutil.which(tool) is None:
            parser.error("missing required tool: " + tool)
    repo = Path(__file__).resolve().parents[1]
    out = args.out.resolve()
    if out == repo or repo in out.parents:
        parser.error("evidence must be outside the repository")
    os.umask(0o077)
    current = identity(args.go, args.current, args.current_commit)
    previous = identity(args.go, args.previous, args.previous_commit)
    if current["source_commit"] == previous["source_commit"] or current["sha256"] == previous["sha256"]:
        parser.error("distinct actual engineering source and binary identities required")
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    # CI invokes this script with sudo from an explicitly selected runner-owned
    # checkout. Allow that exact directory for this read-only Git command only.
    head = subprocess.check_output(["git", "-c", "safe.directory=" + str(repo), "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    report = {"schema_version": 1, "status": "running", "started_at": utc(), "harness_sha256": digest(__file__), "harness_head": head, "environment": {"kernel": platform.release(), "machine": platform.machine(), "filesystem": "tmpfs; read-only bind remount", "mount_tool": subprocess.check_output(["mount", "--version"], text=True).strip(), "go": subprocess.check_output([args.go, "version"], text=True).strip()}, "binaries": {"current": current, "previous": previous}, "cases": [], "topology": "one Linux kernel, private 96 MiB tmpfs and bind mounts", "boundary": "actual production installer/recovery entry, unsigned dev caller decision; no SSH/publisher authorization or physical disk failure claim", "production_gate": "pending"}
    config = out / "worker.json"
    save(config, {"parent_mount_namespace": os.readlink("/proc/self/ns/mnt"), "report": report})
    save(out / "result.json", report)
    with (out / "worker.log").open("w") as log:
        child = subprocess.Popen(["unshare", "--mount", "--propagation", "private", sys.executable, str(Path(__file__).resolve()), "--worker", str(config)], stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        timed_out = False
        try:
            code = child.wait(timeout=300)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(child.pid, signal.SIGKILL)
            code = child.wait()
        except BaseException:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()
            interrupted = json.loads((out / "result.json").read_text())
            interrupted.update(status="interrupted", completed_at=utc())
            save(out / "result.json", interrupted)
            raise
    result = json.loads((out / "result.json").read_text())
    result.update(worker_exit_code=code, timed_out=timed_out, log_sha256=digest(out / "worker.log"))
    if code or result["status"] != "passed" or len(result["cases"]) != 4 or any(case["status"] != "passed" for case in result["cases"]):
        result.update(status="failed", completed_at=utc())
        save(out / "result.json", result)
        raise SystemExit("real storage fault checks failed; see private worker.log")
    save(out / "result.json", result)
    print(json.dumps({"status": "passed", "cases": 4, "evidence": str(out), "production_gate": "pending"}))


if __name__ == "__main__":
    main()
