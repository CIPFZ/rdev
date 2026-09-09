#!/usr/bin/env python3
"""Run real rdev runtime tests through private loopback sshd/ProxyJump servers.

Requires Linux, OpenSSH client/server, rsync, Python 3 and a usable local account.
Both root and ordinary accounts may run the fixture. This creates no user, changes no SSH configuration
and reads no existing key. Reports contain public identities and test output only.
"""
import argparse
import datetime
import hashlib
import json
import os
import pwd
from pathlib import Path
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time


def run(argv, **kwargs):
    timeout = kwargs.pop("timeout", 30)
    capture = kwargs.pop("capture_output", False)
    if capture:
        kwargs.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process = subprocess.Popen(argv, start_new_session=True, **kwargs)
    try:
        out, err = process.communicate(timeout=timeout)
    except BaseException:
        # go test may have built and started test binaries, daemons and clients.
        # Killing only the go parent strands them and skips Go's t.Cleanup.
        # A parent may already have exited while its child holds an inherited
        # stdout pipe. Inventory the private session even after leader exit;
        # signal each exact identity, including TERM-resistant descendants.
        for record in session_processes(process.pid):
            if process_identity(record["pid"]) == record:
                try:
                    os.kill(record["pid"], signal.SIGTERM)
                except ProcessLookupError:
                    pass
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            for record in session_processes(process.pid):
                if process_identity(record["pid"]) == record:
                    try:
                        os.kill(record["pid"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            # Escaped sessions retain the private fixture for explicit recovery;
            # an inherited pipe must not make this timeout handler wait forever.
            process.communicate(timeout=5)
        finally:
            # stdout may be a file, so communicate can return when the parent
            # exits even while a resistant descendant remains in the session.
            for record in session_processes(process.pid):
                if process_identity(record["pid"]) == record:
                    try:
                        os.kill(record["pid"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass
        raise
    result = subprocess.CompletedProcess(argv, process.returncode, out, err)
    result.check_returncode()
    return result


def session_processes(session_id):
    records = []
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                if fields[0] != "Z" and int(fields[3]) == session_id:
                    records.append({"pid": int(entry.name), "start_ticks": fields[19]})
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                pass
    return records


def process_identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        if fields[0] == "Z":
            return None
        return {"pid": pid, "start_ticks": fields[19]}
    except (FileNotFoundError, ProcessLookupError):
        return None


def owned_processes(fixture, namespace):
    records = []
    prefixes = (str(fixture).encode() + b"/", str(namespace).encode() + b"/")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            argv = (entry / "cmdline").read_bytes().split(b"\0")
            matched = any(arg.startswith(prefixes) or arg.startswith(b"ssh: " + prefixes[0]) or arg.startswith(b"ControlPath=" + prefixes[0]) or (arg.startswith(b"sshd: ") and b" -f " + prefixes[0] in arg) for arg in argv)
            if not matched:
                for descriptor in ("1", "2"):
                    try:
                        matched = matched or os.readlink(entry / "fd" / descriptor).startswith(str(fixture) + "/")
                    except (FileNotFoundError, PermissionError):
                        pass
            if matched:
                record = process_identity(int(entry.name))
                if record:
                    records.append(record)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    return records


def release_policy_root(fixture):
    return Path("/tmp") / ("rdev-p8-policy-" + fixture.name.removeprefix(".rdev-p8-ssh-"))


def create_release_policy(fixture, out):
    # OpenSSH accepts a fixture below the account home even when a home
    # ancestor has an extended ACL. Release trust deliberately rejects those
    # ACLs. Keep its isolated test policy below the real root-owned sticky /tmp,
    # without changing the account ACL or weakening the production verifier.
    parent = Path("/tmp")
    info = parent.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or not info.st_mode & stat.S_ISVTX:
        raise RuntimeError("release policy fixture requires a real root-owned sticky /tmp")
    if {"system.posix_acl_access", "system.posix_acl_default"}.intersection(os.listxattr(parent)):
        raise RuntimeError("release policy temporary parent has an unsupported extended ACL")
    root = release_policy_root(fixture)
    root.mkdir(mode=0o700)
    binding = {"fixture": str(fixture), "evidence": str(out)}
    policy = root / "release-policy.json"
    for path, value in ((root / "owner.json", binding), (policy, {"schema_version": 1, "valid_until": (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"), "channels": ["dev"], "allow_unsigned_dev": True, "allow_test_roots": False, "roots": []})):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as output:
            json.dump(value, output)
    return policy


def cleanup_release_policy(fixture, out):
    root = release_policy_root(fixture)
    if not root.exists():
        return
    info = root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise RuntimeError("release policy recovery directory is not private and owned")
    if {entry.name for entry in root.iterdir()} != {"owner.json", "release-policy.json"}:
        raise RuntimeError("unknown release policy recovery entries retained")
    for name in ("owner.json", "release-policy.json"):
        info = (root / name).lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 4096:
            raise RuntimeError("unsafe release policy recovery file retained")
    if json.loads((root / "owner.json").read_text()) != {"fixture": str(fixture), "evidence": str(out)}:
        raise RuntimeError("release policy recovery binding mismatch")
    for name in ("release-policy.json", "owner.json"):
        (root / name).unlink()
    root.rmdir()


def recover(out, action):
    record = json.loads((out / "recovery.json").read_text())
    fixture, namespace = Path(record["fixture"]), Path(record["namespace"])
    if fixture.parent != Path.home() or not fixture.name.startswith(".rdev-p8-ssh-") or namespace.parent != Path.home() / ".cache" or namespace.name != "rdev-phase5-isolated-" + fixture.name.removeprefix(".rdev-p8-ssh-"):
        raise RuntimeError("recovery paths are not the private fixture namespace")
    if not fixture.exists():
        if action == "cleanup":
            cleanup_release_policy(fixture, out)
        print(json.dumps({"status": "already-cleaned", "evidence": str(out)}))
        return
    if json.loads((fixture / "owner.json").read_text())["evidence"] != str(out):
        raise RuntimeError("fixture ownership binding mismatch")
    live = owned_processes(fixture, namespace)
    if action == "status":
        print(json.dumps({"fixture": str(fixture), "namespace": str(namespace), "live_processes": live, "ssh_config": str(fixture / "ssh_config"), "release_policy_root": str(release_policy_root(fixture))}))
        return
    # Explicit recovery cleanup authorizes terminating only this isolated run.
    # Retain the directory whenever process termination cannot be confirmed.
    for control in (fixture / "tmp" / "rdev-ctl").glob("*"):
        if control.is_socket():
            try:
                run(["ssh", "-F", "/dev/null", "-S", str(control), "-O", "exit", "isolated-recovery"], capture_output=True, timeout=5)
            except (subprocess.SubprocessError, OSError):
                pass
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for proc in owned_processes(fixture, namespace):
            if process_identity(proc["pid"]) == proc:
                try:
                    os.kill(proc["pid"], sig)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 5
        while owned_processes(fixture, namespace) and time.monotonic() < deadline:
            time.sleep(.05)
        if not owned_processes(fixture, namespace):
            break
    if owned_processes(fixture, namespace):
        raise RuntimeError("live isolated processes remain; fixture and namespace retained")
    if namespace.exists():
        if json.loads((namespace / ".isolated-owner.json").read_text())["evidence"] != str(out):
            raise RuntimeError("namespace ownership binding mismatch")
        shutil.rmtree(namespace)
    cleanup_release_policy(fixture, out)
    shutil.rmtree(fixture)
    record.update(cleaned_unix=time.time(), status="cleaned", live_processes=[])
    (out / "recovery.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps({"status": "cleaned", "evidence": str(out)}))


def port(address):
    with socket.socket(socket.AF_INET6 if ":" in address else socket.AF_INET) as listener:
        listener.bind((address, 0))
        return listener.getsockname()[1]


def artifact_inputs(repo):
    """Report the same explicit binary selectors the real runtime helpers use."""
    for variable in ("RDEV_TEST_AGENT_DIR", "RDEV_TEST_CLI_BINARY", "RDEV_TEST_DAEMON_BINARY"):
        if os.environ.get(variable) and not Path(os.environ[variable]).is_absolute():
            raise ValueError(variable + " requires an absolute path; runtime working directories differ")
    agents = Path(os.environ.get("RDEV_TEST_AGENT_DIR") or repo / "cmd/rdev/agents").resolve()
    if not agents.is_dir():
        raise ValueError("selected runtime agent directory is missing")
    selected = {"agents": {"directory": str(agents), "sha256": {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(agents.glob("rdev-agent-*")) if path.is_file()}}}
    if not selected["agents"]["sha256"]:
        raise ValueError("selected runtime agent directory has no agents")
    for role, variable in (("cli", "RDEV_TEST_CLI_BINARY"), ("broker", "RDEV_TEST_DAEMON_BINARY")):
        if os.environ.get(variable):
            path = Path(os.environ[variable]).resolve(strict=True)
            selected[role] = {"selection": "explicit immutable input", "path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        else:
            selected[role] = {"selection": "runtime test defaults; per-test build/use, no single binary digest claimed"}
    return selected


class Topology:
    def __init__(self, root):
        self.root = root
        self.servers = []
        self.config = root / "ssh_config"
        self.ports = {name: port(address) for name, address in (("jump", "127.0.0.1"), ("v4", "127.0.0.1"), ("v6", "::1"))}

    def start(self):
        for name in ("identity", "wrong-identity", "jump", "v4", "v6"):
            run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(self.root / name)], stdout=subprocess.DEVNULL)
        # Known host keys come directly from these generated servers. TOFU and
        # user/global known_hosts are deliberately disabled.
        known = []
        for name in ("jump", "v4", "v6"):
            address = "::1" if name == "v6" else "127.0.0.1"
            key = (self.root / (name + ".pub")).read_text().split()
            known.append(f"[{address}]:{self.ports[name]} {' '.join(key[:2])}\n")
            config = self.root / (name + ".sshd_config")
            config.write_text(f"""Port {self.ports[name]}
ListenAddress {address}
HostKey {self.root / name}
PidFile {self.root / (name + '.pid')}
AuthorizedKeysFile {self.root / 'identity.pub'}
StrictModes yes
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
PermitRootLogin prohibit-password
AllowUsers {pwd.getpwuid(os.geteuid()).pw_name}
AllowTcpForwarding {'local' if name == 'jump' else 'no'}
PermitOpen {'127.0.0.1:' + str(self.ports['v4']) + ' [::1]:' + str(self.ports['v6']) if name == 'jump' else 'none'}
PermitTTY no
X11Forwarding no
AllowAgentForwarding no
LogLevel DEBUG1
""")
            server = subprocess.Popen([shutil.which("sshd"), "-D", "-f", str(config), "-E", str(self.root / (name + ".log"))], start_new_session=True)
            self.servers.append(server)
            server.start_ticks = Path(f"/proc/{server.pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
            deadline = time.monotonic() + 10
            while True:
                if server.poll() is not None:
                    raise RuntimeError(f"{name} sshd exited; see its private log")
                try:
                    with socket.create_connection((address, self.ports[name]), timeout=.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"{name} sshd readiness deadline")
                    time.sleep(.02)
        (self.root / "known_hosts").write_text("".join(known))
        self.config.write_text(f"""Host jump
 HostName 127.0.0.1
 Port {self.ports['jump']}
Host target-v4
 HostName 127.0.0.1
 Port {self.ports['v4']}
 ProxyJump jump
Host target-v6
 HostName ::1
 Port {self.ports['v6']}
 ProxyJump jump
Host *
 User {pwd.getpwuid(os.geteuid()).pw_name}
 IdentityFile {self.root / 'identity'}
 IdentitiesOnly yes
 IdentityAgent none
 UserKnownHostsFile {self.root / 'known_hosts'}
 GlobalKnownHostsFile /dev/null
 StrictHostKeyChecking yes
 BatchMode yes
 ConnectTimeout 5
 ServerAliveInterval 3
 ServerAliveCountMax 2
 ControlPath {self.root}/control-%C
""")

    def ssh(self, target, *options):
        return ["ssh", "-F", str(self.config), "-o", "ControlMaster=no", "-o", "ControlPath=none", *options, target, "printf isolated-ssh-ok"]

    def close(self):
        # Only the three groups created here; no process-name/global pkill.
        for server in reversed(self.servers):
            try:
                if server.poll() is None and Path(f"/proc/{server.pid}/stat").read_text().rsplit(")", 1)[1].split()[19] == server.start_ticks:
                    os.killpg(server.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except FileNotFoundError:
                pass
        for server in self.servers:
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid, signal.SIGKILL)
                server.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default=os.environ.get("RDEV_GO", "go"))
    parser.add_argument("--out", required=True, type=Path, help="new private evidence directory outside the repository")
    parser.add_argument("--preflight-only", action="store_true", help="OpenSSH topology/fault checks only; no rdev runtime claim")
    parser.add_argument("--recover", choices=("status", "cleanup"), help="inspect or explicitly clean a retained failed fixture")
    parser.add_argument("--runtime-timeout", type=float, default=1250, help="whole runtime process-tree deadline in seconds")
    parser.add_argument("--run", default="TestRemote(BrokerRoutes|BrokerFrontendBoundary|BrokerJobRecovery|BrokerPreparedSync|BrokerRetryCancellation|BrokerMutationCrashRecovery|FleetFrontendsAndRetry|Phase8MasterDisappearance|Phase8DNSFailure|Phase8StreamFaults)$", help="explicit Go runtime test selection")
    for role in ("previous", "candidate", "frozen"):
        parser.add_argument("--signed-" + role + "-policy", type=Path, help="all three policies select the real test-root signed upgrade/rollback matrix")
    args = parser.parse_args()
    signed_policies = {role: getattr(args, "signed_" + role + "_policy") for role in ("previous", "candidate", "frozen")}
    signed = any(signed_policies.values())
    if signed and (not all(signed_policies.values()) or args.preflight_only):
        parser.error("signed runtime requires all three policies and cannot be preflight-only")
    if signed:
        signed_policies = {role: path.resolve(strict=True) for role, path in signed_policies.items()}
        # The signed runner deliberately selects the frozen bundle's broker.
        # Bind that same selection here, even when the caller supplied unrelated
        # ordinary-runtime overrides. The runner/Go verifier validates trust.
        config = json.loads(signed_policies["frozen"].read_text())
        bundle = Path(config["bundle_dir"])
        if not bundle.is_absolute():
            parser.error("signed frozen bundle requires an absolute path")
        bundle = bundle.resolve(strict=True)
        os.environ.update(RDEV_TEST_AGENT_DIR=str(bundle), RDEV_TEST_DAEMON_BINARY=str(bundle / "rdevd"), RDEV_TEST_CLI_BINARY=str(bundle / "rdev"))
    if args.recover:
        recover(args.out.resolve(), args.recover)
        return
    if args.runtime_timeout <= 0 or args.runtime_timeout > 3600:
        parser.error("runtime-timeout must be positive and at most 3600 seconds")
    repo = Path(__file__).resolve().parent.parent
    for tool in ("sshd", "ssh", "ssh-keygen", "rsync"):
        if shutil.which(tool) is None:
            parser.error(f"required executable missing: {tool}")
    if os.geteuid() == 0 and not Path("/run/sshd").exists():
        Path("/run/sshd").mkdir(mode=0o755)
    args.out = args.out.resolve()
    if args.out == repo or repo in args.out.parents:
        parser.error("--out must be outside the repository")
    args.out.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.umask(0o077)
    report = {"schema": 1, "kind": "isolated-loopback-ssh", "started_unix": time.time(), "status": "running", "shared_host_instances": 1, "physical_machine_count": "unverified", "sshd_instances": 3, "targets": 2, "fault_domain": "one shared Linux kernel and filesystem", "runtime": "not-run", "checks": {}}
    report["harness_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report["artifact_inputs"] = artifact_inputs(repo)
    report["artifacts"] = report["artifact_inputs"]["agents"]["sha256"]
    # sshd StrictModes examines ancestors, so a private leaf below an arbitrary
    # world-writable TMPDIR is insufficient. The account home is its boundary.
    fixture = Path(tempfile.mkdtemp(prefix=".rdev-p8-ssh-", dir=Path.home()))
    namespace = Path.home() / ".cache" / ("rdev-phase5-isolated-" + fixture.name.removeprefix(".rdev-p8-ssh-"))
    namespace.mkdir(mode=0o700, parents=True)
    owner = {"evidence": str(args.out)}
    (fixture / "owner.json").write_text(json.dumps(owner))
    (namespace / ".isolated-owner.json").write_text(json.dumps(owner))
    (fixture / "runtime").mkdir()
    (fixture / "tmp").mkdir()
    recovery = {"schema": 1, "fixture": str(fixture), "namespace": str(namespace), "status": "active", "commands": {"status": [sys.executable, str(Path(__file__).resolve()), "--out", str(args.out), "--recover", "status"], "cleanup": [sys.executable, str(Path(__file__).resolve()), "--out", str(args.out), "--recover", "cleanup"]}}
    (args.out / "recovery.json").write_text(json.dumps(recovery, indent=2) + "\n")
    topology = Topology(fixture)
    try:
        topology.start()
        recovery["servers"] = [process_identity(server.pid) for server in topology.servers]
        (args.out / "recovery.json").write_text(json.dumps(recovery, indent=2) + "\n")
        for target in ("target-v4", "target-v6"):
            result = run(topology.ssh(target), capture_output=True)
            if result.stdout != b"isolated-ssh-ok":
                raise RuntimeError("unexpected SSH response")
            report["checks"][target] = "passed"
        # A denied key and a wrong, pinned host key must fail before commands.
        wrong_config = fixture / "wrong_config"
        wrong_config.write_text(topology.config.read_text().replace(str(fixture / "identity") + "\n", str(fixture / "wrong-identity") + "\n"))
        denied = subprocess.run(["ssh", "-F", str(wrong_config), "jump", "true"], capture_output=True, timeout=15)
        if denied.returncode == 0 or b"Permission denied" not in denied.stderr:
            raise RuntimeError("authentication failure injection was not observed")
        report["checks"]["authentication-denied"] = "passed"
        wrong_known = fixture / "wrong_known_hosts"
        bad_key = (fixture / "wrong-identity.pub").read_text().split()
        wrong_known.write_text(f"[127.0.0.1]:{topology.ports['jump']} {' '.join(bad_key[:2])}\n")
        denied = subprocess.run(topology.ssh("jump", "-o", "UserKnownHostsFile=" + str(wrong_known)), capture_output=True, timeout=15)
        if denied.returncode == 0 or b"Host key verification failed" not in denied.stderr:
            raise RuntimeError("host-key failure injection was not observed")
        report["checks"]["host-key-denied"] = "passed"
        if not args.preflight_only:
            if signed:
                policy = signed_policies["frozen"]
                report["release_trust"] = "actual isolated test-root signed bundles; no official identity or unsigned fallback"
                report["signed_runtime"] = {}
            else:
                policy = create_release_policy(fixture, args.out)
                recovery["release_policy_root"] = str(policy.parent)
                (args.out / "recovery.json").write_text(json.dumps(recovery, indent=2) + "\n")
                report["release_trust"] = "isolated administrator unsigned-dev opt-in; not trusted release certification"
                preflight_env = dict(os.environ, RDEV_RELEASE_POLICY=str(policy), RDEV_TEST_RELEASE_POLICY_PREFLIGHT="1")
                with (args.out / "release-policy-preflight.log").open("wb") as log:
                    run([args.go, "test", "./cmd/rdevd", "-run", "^TestIsolatedReleasePolicyReadiness$", "-count=1", "-v"], env=preflight_env, cwd=repo, stdout=log, stderr=subprocess.STDOUT, timeout=120)
                preflight = (args.out / "release-policy-preflight.log").read_text()
                if "RDEV_SAFE_POLICY reason=accepted explicit=true" not in preflight or "--- PASS:" not in preflight:
                    raise RuntimeError("release policy preflight did not prove explicit fixture admission")
                report["checks"]["release-policy-readiness"] = "passed"
            for target in ("target-v4", "target-v6"):
                env = dict(os.environ, TMPDIR=str(fixture / "tmp"), RDEV_TEST_RUNTIME_ROOT=str(fixture / "runtime"), RDEV_TEST_NAMESPACE_PREFIX=str(namespace.relative_to(Path.home())), RDEV_RUN_REMOTE="1", RDEV_TEST_REMOTE=target, RDEV_TEST_SSH_CONFIG=str(topology.config), RDEV_RELEASE_POLICY=str(policy))
                if signed:
                    output = args.out / (target + "-signed")
                    command = [sys.executable, str(repo / "scripts/signed-upgrade-ssh.py"), "--go", args.go, "--out", str(output), "--remote", target, "--ssh-config", str(topology.config)]
                    for role, path in signed_policies.items():
                        command += ["--" + role + "-policy", str(path)]
                    with (args.out / (target + "-runtime.log")).open("wb") as log:
                        run(command, env=env, cwd=repo, stdout=log, stderr=subprocess.STDOUT, timeout=args.runtime_timeout)
                    result = json.loads((output / "result.json").read_text())
                    if result["status"] != "passed" or result["source_dirty"] or result["draft_fixture"] or not result["inputs_unchanged"]:
                        raise RuntimeError("signed runtime did not pass with clean, immutable inputs")
                    report["signed_runtime"][target] = result
                    report["checks"][target + "-runtime"] = "passed"
                    continue
                with (args.out / (target + "-runtime.log")).open("wb") as log:
                    run([args.go, "test", "./cmd/rdevd", "-run", args.run, "-count=1", "-timeout=20m", "-v"], env=env, cwd=repo, stdout=log, stderr=subprocess.STDOUT, timeout=args.runtime_timeout)
                text = (args.out / (target + "-runtime.log")).read_text()
                if "--- PASS:" not in text or "--- SKIP:" in text or "no tests to run" in text:
                    raise RuntimeError("runtime selection skipped or ran no test")
                report["checks"][target + "-runtime"] = "passed"
            report["runtime"] = "passed"
        # DEBUG1 direct-tcpip channel records prove forwarding by sshd, while
        # killing the jump and forcing fresh connections proves dependency.
        forwarding = (fixture / "jump.log").read_text()
        for name in ("v4", "v6"):
            if f"port {topology.ports[name]}" not in forwarding or "direct-tcpip" not in forwarding:
                raise RuntimeError(f"no jump forwarding evidence for {name}")
        report["checks"]["jump-forwarding-log"] = "passed"
        os.killpg(topology.servers[0].pid, signal.SIGTERM)
        topology.servers[0].wait(timeout=5)
        for target in ("target-v4", "target-v6"):
            failed = subprocess.run(topology.ssh(target), capture_output=True, timeout=15)
            if failed.returncode == 0:
                raise RuntimeError("target succeeded without its required jump")
        report["checks"]["jump-stopped-fresh-connections-fail"] = "passed"
        if artifact_inputs(repo) != report["artifact_inputs"]:
            raise RuntimeError("selected runtime binary inputs changed during validation")
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = str(error)
        raise
    finally:
        if report["status"] == "passed":
            topology.close()
        report["ended_unix"] = time.time()
        report["source_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
        report["source_dirty"] = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repo))
        for name in ("jump.log", "v4.log", "v6.log"):
            if (fixture / name).exists():
                shutil.copyfile(fixture / name, args.out / name)
        (args.out / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        recovery.update(status="retained", live_processes=owned_processes(fixture, namespace))
        (args.out / "recovery.json").write_text(json.dumps(recovery, indent=2) + "\n")
        if report["status"] == "passed":
            recover(args.out, "cleanup")
        print(json.dumps({"status": report["status"], "evidence": str(args.out / "result.json"), "runtime": report["runtime"]}))


if __name__ == "__main__":
    main()
