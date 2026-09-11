#!/usr/bin/env python3
"""Exercise the shipped LaunchAgent in the current macOS GUI login domain.

Creates one uniquely named LaunchAgent and private test credentials, then removes
both. Does not alter the normal service or SSH configuration. Requires a built
native cgo-enabled daemon. Logout/reboot survival is outside this fixture.
"""
import argparse
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import socket
import stat
import subprocess
import tempfile
import time


def run(args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True,
                          timeout=20, **kwargs).stdout.strip()


def until(check, description, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(.1)
    raise RuntimeError("deadline: " + description)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if os.uname().sysname != "Darwin":
        raise RuntimeError("requires native macOS")
    binary = args.daemon.resolve(strict=True)
    domain = "gui/" + str(os.getuid())
    run(["launchctl", "print", domain])
    root = Path(tempfile.mkdtemp(prefix="rdev-launchd-", dir="/private/tmp"))
    label = "com.cipfz." + root.name
    service = domain + "/" + label
    agents = Path.home() / "Library/LaunchAgents"
    agents.mkdir(exist_ok=True)
    plist = agents / (label + ".plist")
    loaded = False
    installed = False
    report = {"platform": run(["sw_vers", "-productVersion"]),
              "architecture": os.uname().machine, "status": "running", "checks": []}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    try:
        key, ready, sock = root / "principal.key", root / "ready", root / "broker.sock"
        run([str(binary), "principal-keygen", "-out", str(key)])
        token = run([str(binary), "principal-token", "-key-file", str(key),
                     "-client-id", "launchd-test", "-project-id", "macos", "-ttl", "10m"])
        policy = Path(str(sock) + ".policy")
        policy.write_text(json.dumps({"launchd-test\u0000macos": {"status": True}}))
        policy.chmod(0o600)
        template = Path(__file__).resolve().parents[1] / "deploy/launchd/com.cipfz.rdevd.plist"
        with template.open("rb") as stream:
            config = plistlib.load(stream)
        config["Label"] = label
        config["ProgramArguments"] = [str(binary), "-socket", str(sock),
                                      "-principal-key-file", str(key), "-ready-file", str(ready)]
        config["EnvironmentVariables"]["RDEV_AGENT_DIR"] = str(root / "agents")
        config["StandardOutPath"] = str(root / "stdout.log")
        config["StandardErrorPath"] = str(root / "stderr.log")
        with plist.open("xb") as stream:
            installed = True
            plistlib.dump(config, stream)
        plist.chmod(0o600)
        run(["plutil", "-lint", str(plist)])

        def pid():
            text = run(["launchctl", "print", service])
            match = re.search(r"^\s*pid = (\d+)$", text, re.M)
            return int(match.group(1)) if match else 0

        def live():
            if not ready.exists() or ready.read_text() != "READY\n":
                return False
            try:
                with socket.socket(socket.AF_UNIX) as conn:
                    conn.settimeout(1)
                    conn.connect(str(sock))
                    with conn.makefile("rwb", buffering=0) as stream:
                        def call(value):
                            stream.write(json.dumps(value).encode() + b"\n")
                            return json.loads(stream.readline())
                        hello = call({"version": 1, "min_version": 1, "client_id": "launchd-test",
                                      "project_id": "macos", "principal_token": token})
                        status = call({"id": "status", "owner": {"client_id": "launchd-test", "project_id": "macos"},
                                       "operation": "status"}) if hello.get("ok") else {}
                        return hello.get("ok") and status.get("ok")
            except (OSError, ValueError):
                return False

        run(["launchctl", "bootstrap", domain, str(plist)])
        loaded = True
        until(live, "authenticated readiness")
        old = pid()
        assert old > 0
        for path, mode in [(root, 0o700), (key, 0o600), (sock, 0o600), (ready, 0o600)]:
            assert stat.S_IMODE(path.stat().st_mode) == mode
        report["checks"].append("install/bootstrap/private-socket/authenticated-status")
        duplicate = subprocess.run(config["ProgramArguments"], capture_output=True, timeout=10)
        assert duplicate.returncode != 0 and live() and pid() == old
        report["checks"].append("duplicate-instance-preserves-live-service")
        settings = Path(str(sock) + ".json")
        for content, message in [("{", "reload rejected"), ('{"max_hosts":12}', "configuration reloaded")]:
            settings.write_text(content)
            settings.chmod(0o600)
            run(["launchctl", "kill", "HUP", service])
            until(lambda: message in (root / "stderr.log").read_text(), message)
            assert live() and pid() == old
        report["checks"].append("invalid-reload-preserves-state/valid-reload-keeps-pid")
        run(["launchctl", "kill", "KILL", service])
        until(lambda: pid() not in (0, old) and live(), "KeepAlive crash recovery", 30)
        report["checks"].append("SIGKILL/KeepAlive/stale-socket-recovery/authentication")
        run(["launchctl", "bootout", service])
        loaded = False
        until(lambda: not sock.exists() and not ready.exists(), "graceful bootout cleanup")
        run(["launchctl", "bootstrap", domain, str(plist)])
        loaded = True
        until(live, "second bootstrap")
        report["checks"].append("bootout-drain/socket-ready-cleanup/rebootstrap")
        report["status"] = "passed"
    finally:
        try:
            if loaded:
                run(["launchctl", "bootout", service])
                until(lambda: not (root / "broker.sock").exists() and not (root / "ready").exists(), "final cleanup")
            if installed:
                plist.unlink()
            # Keep private signing material only for this run, never in evidence.
            shutil.rmtree(root)
            report["cleanup"] = "passed"
        except Exception:
            report["status"] = "failed"
            report["cleanup"] = "failed; inspect the unique test LaunchAgent"
            raise
        finally:
            if report["status"] == "running":
                report["status"] = "failed"
            args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
