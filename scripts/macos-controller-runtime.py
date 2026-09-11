#!/usr/bin/env python3
"""Run macOS CLI/MCP/broker against an authorized Linux SSH target.

Requires make all daemon, native cgo builds and a host-key-verified SSH alias.
Creates an isolated, expiring unsigned-dev release policy, never changing the
administrator's policy. Go fixtures own random remote test namespaces. Existing
SSH keys/configuration remain under OpenSSH control.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


GROUPS = {
    "sync": ["TestRemoteBrokerPreparedSync", "TestRemoteBrokerSyncPreview",
             "TestRemoteBrokerSyncManifest"],
    "core": ["TestRemotePhase6FrontendContracts", "TestRemoteBrokerProcesses",
             "TestRemoteBrokerPolicyIsolation", "TestRemoteBrokerApprovalIsolation",
             "TestRemoteBrokerRetryCancellation", "TestRemoteBrokerLeaseLifecycle",
             "TestRemoteBrokerJobRecovery", "TestRemoteBrokerSharedWait",
             "TestRemoteBrokerMutationCrashRecovery", "TestRemoteBrokerJobEventHistory",
             "TestRemoteBrokerPreparedSync", "TestRemoteBrokerSyncPreview",
             "TestRemoteBrokerSyncManifest", "TestRemoteBrokerSecrets",
             "TestRemoteBrokerSecretImport", "TestRemoteSupportAndStateFrontends"],
    "fleet": ["TestRemoteFleetFrontendsAndRetry", "TestRemoteFleetCrashRecovery",
              "TestRemoteFleetPauseCancelAndCanary", "TestRemoteFleetMixedWorkload"],
    "qos": ["TestRemoteBrokerMixedWorkload"],
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--ssh-config")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--group", choices=GROUPS, default="core")
    parser.add_argument("--race", action="store_true")
    args = parser.parse_args()
    if os.uname().sysname != "Darwin":
        raise RuntimeError("requires native macOS controller")
    repo = Path(__file__).resolve().parents[1]
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    ssh = ["ssh"] + (["-F", args.ssh_config] if args.ssh_config else [])
    ssh += ["-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
            "-o", "ConnectTimeout=8", args.target, "uname -sm"]
    target = subprocess.check_output(ssh, text=True, timeout=15).strip()
    if not target.startswith("Linux "):
        raise RuntimeError("this fixture requires a Linux target")
    report = {"status": "running", "scope": "macOS controller to Linux remote",
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
              "working_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=repo)).hexdigest(),
              "platform": os.uname().sysname + " " + os.uname().machine,
              "remote_platform": target, "group": args.group, "race": args.race,
              "agent_instrumented": False, "selected": GROUPS[args.group]}
    report["binaries"] = {name: hashlib.sha256((repo / "bin" / name).read_bytes()).hexdigest()
                          for name in ("rdev", "rdevd")}
    with tempfile.TemporaryDirectory(prefix="rdev-mac-policy-", dir="/private/tmp") as private:
        policy = Path(private) / "policy.json"
        policy.write_text(json.dumps({"schema_version": 1,
            "valid_until": (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "channels": ["dev"], "allow_unsigned_dev": True,
            "allow_test_roots": False, "roots": []}))
        policy.chmod(0o600)
        env = dict(os.environ, CGO_ENABLED="1", RDEV_RUN_REMOTE="1",
                   RDEV_TEST_REMOTE=args.target, RDEV_TEST_SSH_CONFIG=args.ssh_config or "",
                   RDEV_RELEASE_POLICY=str(policy), RDEV_TEST_RELEASE_POLICY_PREFLIGHT="1",
                   RDEV_TEST_CLI_BINARY=str(repo / "bin/rdev"),
                   RDEV_TEST_DAEMON_BINARY=str(repo / "bin/rdevd"))
        # -race on the harness alone would leave subprocesses uninstrumented.
        if args.race:
            for name in ("rdev", "rdevd"):
                binary = Path(private) / name
                subprocess.run([args.go, "build", "-race", "-o", str(binary), "./cmd/" + name],
                               env=env, cwd=repo, check=True)
                env["RDEV_TEST_CLI_BINARY" if name == "rdev" else "RDEV_TEST_DAEMON_BINARY"] = str(binary)
                report["binaries"][name] = hashlib.sha256(binary.read_bytes()).hexdigest()
        selection = ["TestIsolatedReleasePolicyReadiness"] + GROUPS[args.group]
        command = [args.go, "test"] + (["-race"] if args.race else [])
        command += ["./cmd/rdevd", "-json", "-count=1", "-timeout=20m", "-run=^(" + "|".join(selection) + ")$"]
        report["command"] = command
        log = out / (args.group + ("-race" if args.race else "") + ".jsonl")
        with log.open("xb") as stream:
            result = subprocess.run(command, cwd=repo, env=env, stdout=stream, stderr=subprocess.STDOUT)
        passed, skipped, failed = [], [], []
        for line in log.read_text().splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            name = event.get("Test", "")
            if event.get("Action") == "skip":
                skipped.append(name)
            if event.get("Action") == "fail":
                failed.append(name or event.get("Package"))
            if name in selection and event.get("Action") == "pass":
                passed.append(name)
        report.update(exit_code=result.returncode, passed=passed, skipped=skipped, failed=failed,
                      log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())
        report["status"] = "passed" if result.returncode == 0 and set(passed) == set(selection) and not skipped else "failed"
    report["policy_cleanup"] = "passed"
    destination = log.with_suffix(".summary.json")
    destination.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
