#!/usr/bin/env python3
"""Run immutable real-agent installation tests; no build, SSH or signing claim."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time


CASES = (
    "ProductionEntry", "HealthAndStateRefusal", "PostSwitchRollback",
    "CommittedCleanupFailure", "ExplicitUnsignedDevRollback",
    "SIGKILLAndRecovery", "CrossProcessLockAndCAS",
    "DecisionJournalRecovery", "RecordScratchRecovery",
)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", required=True, type=Path,
                        help="precompiled go test -c ./internal/agentinstall binary")
    parser.add_argument("--out", required=True, type=Path,
                        help="new private evidence directory; must not already exist")
    for role in ("current", "previous"):
        parser.add_argument("--" + role, required=True, type=Path)
        parser.add_argument("--" + role + "-commit", required=True)
        parser.add_argument("--" + role + "-sha256", required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    agent_env = {}
    agents = {}
    for role in ("current", "previous"):
        path = getattr(args, role).resolve(strict=True)
        commit = getattr(args, role + "_commit")
        expected = getattr(args, role + "_sha256")
        if not re.fullmatch(r"[0-9a-f]{40}", commit):
            parser.error(role + " requires a full source commit")
        if not re.fullmatch(r"[0-9a-f]{64}", expected) or digest(path) != expected:
            parser.error(role + " does not match the externally supplied digest")
        agents[role] = {"path": str(path), "source_commit": commit, "sha256": expected}
        prefix = "RDEV_REAL_AGENT_" + role.upper() + "_"
        agent_env.update({prefix + "PATH": str(path), prefix + "COMMIT": commit,
                          prefix + "SHA256": expected})
    binary = args.test_binary.resolve(strict=True)
    args.out = args.out.absolute()
    args.out.mkdir(mode=0o700)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("RDEV_REAL_AGENT_")}
    env.update(agent_env, GOMAXPROCS="2")
    expression = "^TestRealAgentBinary(" + "|".join(CASES) + ")$"
    command = [str(binary), "-test.run=" + expression, "-test.v", "-test.timeout=3m"]
    source_file = repo / "internal/agentinstall/real_binary_test.go"
    result = {
        "schema_version": 1, "started_at": utc(), "status": "running",
        "command": command, "agents": agents,
        "harness_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "test_source_sha256": digest(source_file),
        "test_binary_sha256": digest(binary),
        "runner_sha256": digest(Path(__file__)),
        "environment": {"gomaxprocs": 2, "nice_increment": 10,
                        "platform": os.uname().sysname, "machine": os.uname().machine},
        "scope": {
            "agent_bytes": "unmodified clean builds; actual native buildinfo checked in tests",
            "production_entry": "actual -install-candidate and -recover-upgrades executables",
            "faults": "test installer hooks with real agent hello; pipe barriers and SIGKILL",
            "rollback": "actual automatic rollback; explicit unsigned dev library rollback",
            "signature_ssh_and_formal_n_minus_one": "not exercised or certified",
            "durable_windows": "prepared, verified, switching, published, committed; replacement also rolled_back",
            "window_count": 11,
            "reconstructed_record_states": "decision before committed journal, same-byte test-root metadata activation, interrupted fixed scratch and unsafe scratch refusal; actual production recovery entry",
        },
    }
    summary = args.out / "result.json"
    summary.write_text(json.dumps(result, indent=2) + "\n")
    started = time.monotonic()
    timed_out = False
    with (args.out / "test.log").open("wb") as log:
        # Only this process group contains these private fixture tests. A runner
        # timeout kills that group, never a caller's shell or another soak run.
        process = subprocess.Popen(["nice", "-n", "10", *command], cwd=repo,
                                   env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            code = process.wait(timeout=210)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            code = process.wait()
    output = (args.out / "test.log").read_text(errors="replace")
    passed = re.findall(r"^--- PASS: (TestRealAgentBinary\w+) \(([0-9.]+)s\)$",
                        output, flags=re.MULTILINE)
    names = {name for name, _ in passed}
    expected = {"TestRealAgentBinary" + case for case in CASES}
    success = code == 0 and names == expected and "--- SKIP:" not in output
    result.update(status="passed" if success else "failed", completed_at=utc(),
                  elapsed_seconds=time.monotonic() - started, exit_code=code,
                  timeout=timed_out, log_sha256=digest(args.out / "test.log"),
                  cases=[{"name": name, "status": "passed", "elapsed_seconds": float(seconds)}
                         for name, seconds in passed])
    summary.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"status": result["status"], "result": str(summary),
                      "elapsed_seconds": result["elapsed_seconds"]}))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
