#!/usr/bin/env python3
"""Persistent, bounded real-SSH scale workload (NOT Production Gate).

100 distinct loopback sshd endpoints and agent state namespaces, 20 processes,
one persistent broker. Ordinary exec/job/wait/status/log/removal and retained sync
run on a fixed, storage-budgeted cadence while ping traffic switches all targets.
The result states exact coverage; running 24h alone cannot pass Production Gate.
Use start/status/cancel/cleanup; never restart a stopped run to add elapsed time.
"""
import argparse
import contextlib
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

BOUNDS = {"rss_bytes": 12 << 30, "fds": 32768, "processes": 2500, "disk_bytes": 8 << 30,
          "managed_observed_cpu_cores": 12,
          "request_deadline_seconds": 30, "sample_seconds": 5, "max_sample_gap_seconds": 45,
          "mutation_operations": 8192, "warm_hosts": 16, "mutation_cycle_seconds": 1200,
          "goroutines": 4096, "idle_rss_growth_bytes": 256 << 20, "idle_goroutine_growth": 128,
          "idle_interval_seconds": 3600, "idle_seconds": 330, "job_log_bytes": 4 << 10}
LATENCY_MS = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000]
MISSING = ["saturated owner/lane fairness and quota SLO", "Fleet threshold/cancel/subset-retry fault stages", "in-flight mutation crash/ambiguity fault stages", "automatic retention GC removal proof", "CPU final ticks for processes exiting between samples", "dial/reconnect/backoff/queue distribution acceptance thresholds", "metric collectors beyond fixed observe.Registry counters", "production topology and cross-machine network", "strict control latency SLO", "all Production Gates"]
CPU_PREVIOUS = None
DIAGNOSTIC_CODES = frozenset("internal.failure object.not_found process.invalid_state process.start_failed protocol.frame_too_large protocol.invalid_event protocol.invalid_frame protocol.unknown_operation protocol.unsupported_feature release.channel_denied release.platform_unsupported release.policy_required release.untrusted release.version_denied request.canceled request.deadline_exceeded request.invalid request.operation_id_conflict resource.limit_exceeded resource.queue_full resource.slow_consumer resource.watcher_limit state.incompatible transport.ambiguous_outcome transport.unavailable".split())
DIAGNOSTIC_STATES = frozenset("prepared not_sent dispatched ambiguous completed possibly_executed committed failed".split())


def write(path, value):
    temporary = path.with_name(path.name + ".tmp-" + str(os.getpid()))
    with temporary.open("w") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def command(argv, **kwargs):
    return subprocess.run([str(x) for x in argv], check=True, timeout=30, **kwargs)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return fields[19] if fields[0] != "Z" else None
    except FileNotFoundError:
        return None


def live(record):
    return record["start_ticks"] is not None and identity(record["pid"]) == record["start_ticks"]


def owned_processes(root, namespace):
    """Find live processes carrying private run paths, including reparented mux."""
    records = []
    prefixes = (str(root).encode() + b"/", str(namespace).encode() + b"/")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            if fields[0] == "Z":
                continue
            argv = (entry / "cmdline").read_bytes().split(b"\0")
            matched = any(arg.startswith(prefixes) or arg.startswith(b"ssh: " + prefixes[0]) or arg.startswith(b"ControlPath=" + prefixes[0]) for arg in argv)
            if not matched:
                for descriptor in ("1", "2"):
                    try:
                        if os.readlink(entry / "fd" / descriptor).startswith(str(root) + "/"):
                            matched = True
                    except (FileNotFoundError, PermissionError):
                        pass
            if matched:
                records.append({"pid": int(entry.name), "start_ticks": fields[19]})
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return records


def close_mux(root):
    for control in (root / "tmp" / "rdev-ctl").glob("*"):
        if control.is_socket():
            try:
                subprocess.run(["ssh", "-F", "/dev/null", "-S", str(control), "-O", "exit", "isolated-scale"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            except subprocess.TimeoutExpired:
                # The caller still inventories remaining live processes and
                # refuses destructive cleanup when graceful mux close failed.
                pass


def process_tree(roots):
    parents = {}
    detached_supervisors = set()
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                parents[int(entry.name)] = int(fields[1])
                if b"-supervise" in (entry / "cmdline").read_bytes().split(b"\0"):
                    detached_supervisors.add(int(entry.name))
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                pass
    selected = set(roots)
    while True:
        # A detached job can still have the serving agent as its Unix parent.
        # Stop only the SSH/serve path; preserve the supervisor and its subtree.
        added = {pid for pid, parent in parents.items() if parent in selected and pid not in detached_supervisors}
        if added <= selected:
            break
        selected |= added
    return [{"pid": pid, "start_ticks": identity(pid)} for pid in selected if identity(pid)]


def resume_records(records):
    for record in records:
        if live(record):
            try:
                os.kill(record["pid"], signal.SIGCONT)
            except ProcessLookupError:
                pass


def logger_progress(root, namespace, clients, targets):
    result = {}
    for index in range(clients):
        logger = json.loads((root / f"worker-{index}.json").read_text()).get("logger")
        if logger and int(logger["host"].split("-")[-1]) in targets:
            directory = namespace / ("agent-" + str(int(logger["host"].split("-")[-1]))) / "jobs" / logger["id"]
            if not (directory / "status.json").exists():
                result[str(directory / "ledger.json")] = json.loads((directory / "ledger.json").read_text())["stdout_ledger"]["original_bytes"]
    return result


def ping_probe(root, target):
    credential = json.loads((root / "credential-0.json").read_text())
    channel = connect(root, credential["owner"], credential["token"])
    try:
        channel.settimeout(2)
        response = checked(channel, {"id": "recovery-probe", "owner": credential["owner"], "operation": "ping", "host": f"target-{target:03d}", "wire": {"op": "ping"}})
        return response["wire"]["ping"]["pid"]
    finally:
        channel.close()


def rpc(sock, message):
    deadline = time.monotonic() + min(sock.gettimeout() or BOUNDS["request_deadline_seconds"], BOUNDS["request_deadline_seconds"])
    sock.sendall(json.dumps(message, separators=(",", ":")).encode() + b"\n")
    # No buffering across reconnects, bounded individual protocol records.
    result = bytearray()
    while len(result) <= 1 << 20:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("broker whole-record deadline")
        sock.settimeout(remaining)
        byte = sock.recv(1)
        if not byte:
            raise RuntimeError("broker EOF")
        if byte == b"\n":
            sock.settimeout(BOUNDS["request_deadline_seconds"])
            return json.loads(result)
        result += byte
    raise RuntimeError("oversized broker record")


def connect(root, owner, token):
    sock = socket.socket(socket.AF_UNIX)
    sock.settimeout(BOUNDS["request_deadline_seconds"])
    sock.connect(str(root / "broker.sock"))
    hello = rpc(sock, dict(version=1, min_version=1, principal_token=token, **owner))
    if not hello.get("ok") or not hello.get("min_version", 0) <= 1 <= hello.get("version", 0):
        sock.close()
        raise RuntimeError("broker negotiation rejected")
    return sock


def checked(sock, request):
    response = rpc(sock, request)
    if not response.get("ok") or ("wire" in response and not response["wire"].get("ok")):
        envelope = response.get("error_envelope") or response.get("wire", {}).get("error") or {}
        code = envelope.get("code") if isinstance(envelope, dict) else None
        if not isinstance(code, str) or code not in DIAGNOSTIC_CODES:
            code = "broker-denied"
        # Match a fixed product constant; never retain arbitrary peer diagnostics.
        if response.get("error") == "broker ingress limit reached":
            code = "broker-ingress-limit"
        mutation = response.get("mutation") or {}
        state = mutation.get("state") if isinstance(mutation, dict) else None
        if not state and isinstance(envelope, dict):
            state = envelope.get("execution_state")
        if not isinstance(state, str) or state not in DIAGNOSTIC_STATES:
            state = "unknown"
        raise RuntimeError("operation " + request["operation"] + " rejected: " + code + "; state=" + state)
    return response


@contextlib.contextmanager
def large_response_admission(root, stats):
    """Test workload budget: serialize retained sync and job wait reservations.

    One sync reserves 16+8+4 MiB; job wait reserves 8 MiB. Two syncs plus
    one wait already exhaust the product's global 64 MiB before request bytes.
    All 20 clients and the daily mutation count remain; this is explicitly
    bounded workload admission, not saturated sync-throughput certification.
    Kernel flock releases on process death; no mutation is retried here.
    """
    started = time.monotonic()
    with (root / "large-response.lock").open("a+b") as lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() - started >= BOUNDS["request_deadline_seconds"]:
                    raise RuntimeError("workload large-response admission deadline")
                time.sleep(.02)
        stats["workload_admission_wait_seconds"] = stats.get("workload_admission_wait_seconds", 0) + time.monotonic() - started
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def approved_wire(root, sock, owner, host, wire, operation_id):
    wire = dict(wire, operation_id=operation_id)
    admin = json.loads((root / "credential-admin.json").read_text())
    control = connect(root, admin["owner"], admin["token"])
    try:
        approval = checked(control, {"id": operation_id + "-approval", "owner": admin["owner"], "operation": "approval.create", "approval_spec": {"owner": owner, "operation": wire["op"], "host": host, "wire": wire, "ttl": 120000000000}})
        return checked(sock, {"id": operation_id, "owner": owner, "host": host, "operation": wire["op"], "operation_id": operation_id, "wire": wire, "approval": approval["approval"]["token"]})
    finally:
        control.close()


def pool_health(root, require_metrics=False):
    admin = json.loads((root / "credential-admin.json").read_text())
    control = connect(root, admin["owner"], admin["token"])
    try:
        health = checked(control, {"id": "pool-health", "owner": admin["owner"], "operation": "pool.health"})["pool"]
        if require_metrics and any(key not in health for key in ("goroutines", "observer_metric_series", "observer_metric_series_bound", "dial_admission")):
            raise RuntimeError("long-run broker lacks required process/observer metrics")
        if health["limit"] != BOUNDS["warm_hosts"] or max(health["reserved_hosts"], health["base_transports"]) > BOUNDS["warm_hosts"]:
            raise RuntimeError("actual warm pool exceeded its fixed cap")
        if health.get("goroutines", 0) > BOUNDS["goroutines"]:
            raise RuntimeError("broker goroutine budget exceeded")
        if "observer_metric_series" in health and health["observer_metric_series"] > health["observer_metric_series_bound"]:
            raise RuntimeError("enumerated observer counter cardinality exceeded its source bound")
        if "dial_admission" in health:
            admission = health["dial_admission"]
            if admission["state_limit"] != 1024 or admission["states"] > 1024 or admission["global_slot_limit"] != 6 or admission["global_slots_in_use"] > 6:
                raise RuntimeError("actual dial state/slot limits exceed their fixed source contracts")
            if len(admission["queue_buckets"]) != 7 or admission["queue_bucket_upper_ns"] != [1000000, 10000000, 100000000, 1000000000, 10000000000, 30000000000, -1]:
                raise RuntimeError("dial queue buckets do not match the fixed finite metric contract")
        return health
    finally:
        control.close()


def observer(root, index):
    credential = json.loads((root / "credential-0.json").read_text())
    logger = json.loads((root / "worker-0.json").read_text())["logger"]
    channel = connect(root, credential["owner"], credential["token"])
    try:
        result = checked(channel, {"id": f"observer-{index}", "owner": credential["owner"], "host": logger["host"], "operation": "job_wait", "wire": {"op": "job_wait", "job": {"id": logger["id"], "wait_timeout_sec": 8}}})
        write(root / f"observer-{index}.json", {"result": result, "ended_unix": time.time()})
    finally:
        channel.close()


def check_idle(samples, baseline=None, full=False):
    tail = samples[-1]
    if tail["pool"]["base_transports"] or tail["pool"]["reserved_hosts"]:
        raise RuntimeError("idle warm transports did not fall to zero")
    if full and tail["process_kinds"].get("ssh_controlmaster", 0):
        raise RuntimeError("ControlMaster did not expire within declared 330-second idle window")
    if baseline:
        if tail["rss_bytes"] > baseline["rss_bytes"] + BOUNDS["idle_rss_growth_bytes"]:
            raise RuntimeError("idle RSS regression exceeds predeclared baseline budget")
        if tail["pool"].get("goroutines", 0) > baseline["pool"].get("goroutines", 0) + BOUNDS["idle_goroutine_growth"]:
            raise RuntimeError("idle goroutine regression exceeds predeclared baseline budget")
    return tail


def mixed_cycle(root, spec, index, cycle, sock, owner, host, target, stats):
    business = Path(spec["namespace"]) / f"business-{target}"
    prefix = "op_scale_" + spec["run_id"][:12] + f"_{index}_{cycle}_"
    admin = json.loads((root / "credential-admin.json").read_text())
    admin_sock = connect(root, admin["owner"], admin["token"])
    def call(operation, wire=None, sync=None, mutation=False):
        request = {"id": prefix + operation, "owner": owner, "operation": operation, "host": host}
        if wire is not None:
            request["wire"] = dict(wire, op=operation)
        if sync is not None:
            request["sync"] = sync
        if mutation:
            request["operation_id"] = prefix + operation.replace(".", "_")
            if wire is not None:
                request["wire"]["operation_id"] = request["operation_id"]
            approval = checked(admin_sock, {"id": prefix + "approve_" + operation, "owner": admin["owner"], "operation": "approval.create", "approval_spec": {"owner": owner, "operation": operation, "host": host, "wire": request.get("wire"), "sync": sync, "ttl": 120000000000}})
            request["approval"] = approval["approval"]["token"]
        result = checked(sock, request)
        stats["operations"][operation] = stats["operations"].get(operation, 0) + 1
        if mutation:
            stats["mutations"] += 1
        stats["updated_unix"] = time.time()
        stats["updated_monotonic"] = time.monotonic()
        write(root / f"worker-{index}.json", stats)
        return result
    try:
        if cycle == 0:
            logger_marker = business / f"logger-{index}"
            duration = max(8, spec["seconds"] - 45 - index)
            program = "import os,sys,time\nwith open(sys.argv[1],'ab') as marker: marker.write(b'x')\nend=time.monotonic()+float(sys.argv[2])\nwhile time.monotonic()<end:\n os.write(1,b'rdev-log'*128+b'\\n');time.sleep(1)\n"
            logger = approved_wire(root, sock, owner, host, {"op": "job_start", "job": {"spec": {"argv": ["python3", "-c", program, str(logger_marker), str(duration)]}, "resources": {"wall_timeout_sec": min(86400, spec["seconds"]), "fds": 128}}}, prefix + "continuous_logger")
            stats["logger"] = {"id": logger["wire"]["job"]["info"]["id"], "host": host, "marker": str(logger_marker)}
            stats["mutations"] += 1
            stats["operations"]["job_start"] = stats["operations"].get("job_start", 0) + 1
        exec_marker = business / f"exec-{index}-{cycle}"
        call("exec", {"exec": {"argv": ["sh", "-c", 'printf x >> "$1"; printf bounded-exec', "rdev-scale", str(exec_marker)], "timeout_sec": 10, "max_output_bytes": 4096}}, mutation=True)
        if exec_marker.read_bytes() != b"x":
            raise RuntimeError("successful exec was duplicated or lost")
        job_marker = business / f"job-{index}-{cycle}"
        log_program = "import os,sys,time\nwith open(sys.argv[1],'ab') as marker: marker.write(b'x')\nfor i in range(16):\n os.write(1,b'rdev-log'*1024+b'\\n');time.sleep(.05)\n"
        started = call("job_start", {"job": {"spec": {"argv": ["python3", "-c", log_program, str(job_marker)]}, "resources": {"wall_timeout_sec": 60, "fds": 128}}}, mutation=True)
        job_id = started["wire"]["job"]["info"]["id"]
        call("job_status", {"job": {"id": job_id}})
        with large_response_admission(root, stats):
            call("job_wait", {"job": {"id": job_id, "wait_timeout_sec": 10}})
        info = call("job_status", {"job": {"id": job_id}})["wire"]["job"]["info"]
        ledger = info["stdout_ledger"]
        if ledger["original_bytes"] < 128 << 10 or not 0 < ledger["retained_bytes"] <= BOUNDS["job_log_bytes"] or ledger["dropped_bytes"] <= 0:
            raise RuntimeError("bounded sustained job log did not rotate with exact ledger accounting")
        stats["log_rotation_proofs"] += 1
        logs = call("job_logs", {"job": {"id": job_id, "tail": 8}})
        if job_marker.read_bytes() != b"x":
            raise RuntimeError("successful detached job was duplicated or lost")
        call("job_rm", {"job": {"id": job_id}}, mutation=True)
        # Job removal exercises production cleanup while keeping its durable
        # start tombstone and mutation ledger. No test directly deletes records.
        if cycle % 2 == 0:
            source = root / "sources" / f"payload-{index}"
            source.write_bytes((f"{index}:{cycle}:".encode() + bytes(range(256))) * 4096)
            destination = business / f"sync-{index}"
            options = {"direction": "push", "local": str(source), "remote": str(destination)}
            with large_response_admission(root, stats):
                prepare = call("sync.push", sync=dict(options, prepare=True, dry_run=True))
                options["plan_id"] = prepare["sync"]["plan_id"]
                call("sync.push", sync=options, mutation=True)
            if sha(source) != sha(destination):
                raise RuntimeError("retained sync byte identity changed")
        stats["cycles"] += 1
        stats["marker_proofs"] += 2
    finally:
        admin_sock.close()


def fleet_loop(root, spec, report, control):
    """Six persistent 100-target plans fit the existing eight-plan owner cap."""
    try:
        cycle = 0
        next_cycle = time.monotonic() + 15
        while not (root / "stop").exists():
            if time.monotonic() < next_cycle:
                time.sleep(.1)
                continue
            with control["lock"]:
                if control["quiesced"]:
                    admitted = False
                else:
                    control["active"] = True
                    admitted = True
            if not admitted:
                time.sleep(.05)
                continue
            credential = json.loads((root / "credential-admin.json").read_text())
            owner = credential["owner"]
            sock = connect(root, owner, credential["token"])
            def call(op, payload, approval=""):
                return checked(sock, {"id": f"fleet-{cycle}-{op}", "owner": owner, "operation": op, "fleet": payload, "approval": approval})
            try:
                # The parent agent supervisor's executable selects its own
                # isolated target namespace; the same exact Fleet argv binds all.
                body = 'agent=$(readlink "/proc/$PPID/exe"); agent=${agent%/*}; target=${agent##*-}; printf x >> "$1/business-$target/fleet-$2"; printf bounded-fleet-log'
                plan = call("fleet.plan", {"spec": {"selector": "all", "operation": "job_start", "job": {"spec": {"argv": ["sh", "-c", body, "rdev-scale-fleet", spec["namespace"], str(cycle)], "cwd": spec["namespace"]}, "resources": {"wall_timeout_sec": 60}}, "rollout": {"strategy": "canary", "canary": 1, "wave_size": min(8, spec["targets"]), "max_parallel": min(4, spec["targets"]), "pause_between_waves_sec": 1}}})["fleet"]
                bound = {"plan_id": plan["plan_id"], "digest": plan["digest"]}
                approval = call("fleet.approve", dict(bound, ttl_seconds=600))["approval"]["token"]
                call("fleet.execute", bound, approval)
                deadline = time.monotonic() + 600
                while True:
                    status = call("fleet.status", {"plan_id": plan["plan_id"], "limit": 1})["fleet"]
                    if status.get("counts", {}).get("success", 0) == spec["targets"]:
                        break
                    if status.get("counts", {}).get("failed", 0) or status.get("counts", {}).get("ambiguous", 0) or time.monotonic() >= deadline:
                        raise RuntimeError("Fleet did not finish all targets within its fixed deadline")
                    if (root / "stop").exists():
                        return
                    time.sleep(.2)
                for target in range(spec["targets"]):
                    if (Path(spec["namespace"]) / f"business-{target}" / f"fleet-{cycle}").read_bytes() != b"x":
                        raise RuntimeError("Fleet marker duplicated or lost")
                report["fleet"].append({"plan_id": plan["plan_id"], "digest": plan["digest"], "targets": spec["targets"], "status": "passed", "markers": "exactly one byte per target", "ended_unix": time.time()})
                write(root / "fleet-results.json", report["fleet"])
                cycle += 1
                next_cycle += 4 * 3600
            finally:
                sock.close()
                with control["lock"]:
                    control["active"] = False
    except BaseException as error:
        report["fleet_error"] = str(error)
        with control["lock"]:
            control["active"] = False


def worker(root, index):
    spec = json.loads((root / "run.json").read_text())
    stats = {"pid": os.getpid(), "calls": 0, "errors": 0, "expected_fault_errors": 0, "max_latency_ms": 0, "targets": [], "cold_ms": [], "warm_ms": [], "latency_histogram_bounds_ms": LATENCY_MS, "latency_histogram": {"cold_ms": [0] * (len(LATENCY_MS) + 1), "warm_ms": [0] * (len(LATENCY_MS) + 1)}, "updated_unix": time.time(), "updated_monotonic": time.monotonic(), "operations": {}, "mutations": 0, "cycles": 0, "marker_proofs": 0, "log_rotation_proofs": 0}
    saved = root / f"worker-{index}.json"
    if saved.exists():
        stats = json.loads(saved.read_text())
        if stats.get("cycle_in_progress"):
            raise RuntimeError("interrupted mutation cycle requires reconciliation; no automatic replay")
        stats["pid"] = os.getpid()
    seen = set(stats["targets"])
    agent_pids = {}
    current_token = None
    sock = None
    turn = stats.get("turn", 0)
    next_cycle = stats.get("next_cycle_monotonic")
    deadline = time.monotonic() + spec["seconds"] + 120
    while not (root / "stop").exists() and time.monotonic() < deadline:
        if (root / "maintenance").exists():
            if sock:
                sock.close()
                sock = None
            stats["parked"] = True
            stats["updated_unix"] = time.time()
            stats["updated_monotonic"] = time.monotonic()
            write(root / f"worker-{index}.json", stats)
            time.sleep(.05)
            continue
        stats["parked"] = False
        if not (root / "start-barrier").exists():
            write(root / f"worker-{index}.json", stats)
            time.sleep(.05)
            continue
        # Five distinct hosts per worker; no alias ever shares another target's
        # sshd or installation namespace. Repeated blocks measure warm access;
        # cycling all 100 targets forces the 16-entry pool to evict.
        target = (index + (turn // 3) * spec["clients"]) % spec["targets"]
        fault_path = root / "network-fault.json"
        fault = json.loads(fault_path.read_text()) if fault_path.exists() else {}
        expected_fault = target in fault.get("targets", []) and time.monotonic() <= fault.get("until_monotonic", 0)
        ping_stage = True
        started = time.monotonic()
        try:
            credential = json.loads((root / f"credential-{index}.json").read_text())
            if sock is not None and credential["token"] != current_token:
                sock.close()
                sock = None
            if sock is None:
                sock = connect(root, credential["owner"], credential["token"])
                current_token = credential["token"]
            if expected_fault:
                sock.settimeout(2)
            result = rpc(sock, {"id": str(turn), "owner": credential["owner"], "operation": "ping", "host": f"target-{target:03d}", "wire": {"op": "ping"}})
            if not result.get("ok") or not result.get("wire", {}).get("ok") or not result.get("wire", {}).get("ping", {}).get("pid"):
                raise RuntimeError("ping rejected")
            latency = (time.monotonic() - started) * 1000
            agent_pid = result["wire"]["ping"]["pid"]
            bucket = "warm_ms" if agent_pids.get(target) == agent_pid else "cold_ms"
            agent_pids[target] = agent_pid
            stats[bucket] = (stats[bucket] + [latency])[-1024:]
            bucket_index = next((i for i, bound in enumerate(LATENCY_MS) if latency <= bound), len(LATENCY_MS))
            stats["latency_histogram"][bucket][bucket_index] += 1
            stats["max_latency_ms"] = max(stats["max_latency_ms"], latency)
            stats["calls"] += 1
            seen.add(target)
            stats["targets"] = sorted(seen)
            ping_stage = False
            if turn % 5 == 0 and not fault:
                status = checked(sock, {"id": f"status-{turn}", "owner": credential["owner"], "operation": "status"})
                stats["scheduler"] = status["scheduler"]
                stats["operations"]["status"] = stats["operations"].get("status", 0) + 1
            if next_cycle is None:
                # A deterministic offset keeps all clients live before their
                # first mixed mutation and leaves initial cold visits observable.
                next_cycle = time.monotonic() + 10 + index
            if spec["workload"] == "mixed" and not fault and time.monotonic() >= next_cycle:
                stats["cycle_in_progress"] = True
                write(saved, stats)
                mixed_cycle(root, spec, index, stats["cycles"], sock, credential["owner"], f"target-{target:03d}", target, stats)
                stats["cycle_in_progress"] = False
                next_cycle += BOUNDS["mutation_cycle_seconds"]
        except (OSError, RuntimeError, ValueError, KeyError) as error:
            if expected_fault and ping_stage:
                stats["expected_fault_errors"] += 1
            else:
                stats["errors"] += 1
                stats["last_error"] = str(error)
            if sock:
                sock.close()
                sock = None
        stats["updated_unix"] = time.time()
        stats["updated_monotonic"] = time.monotonic()
        stats["next_cycle_monotonic"] = next_cycle
        stats["turn"] = turn + 1
        write(root / f"worker-{index}.json", stats)
        if stats.get("cycle_in_progress"):
            # Preserve the interrupted attempt for reconciliation. Never enter
            # the same mutation cycle again merely because a request failed.
            if sock:
                sock.close()
            return
        turn += 1
        time.sleep(max(0, 1 - (time.monotonic() - started)))
    if sock:
        sock.close()


def cpu_interval(process_ticks, host_ticks, now, previous):
    hz = os.sysconf("SC_CLK_TCK")
    state = {"process_ticks": process_ticks, "host_ticks": host_ticks, "monotonic": now}
    result = {"scope": "identity-bound managed processes alive at sample; final ticks of processes exiting between samples are unobserved", "host_scope": "all CPUs of the shared host; other workloads are not charged to managed CPU", "host_cpu_count": os.cpu_count(), "interval_seconds": 0, "managed_observed_cores": 0, "host_busy_cores": 0, "host_total_cores": 0}
    if previous is None:
        return result, state
    elapsed = now - previous["monotonic"]
    if elapsed <= 0:
        raise RuntimeError("CPU sampling clock did not advance")
    # A newly observed identity is charged its current lifetime ticks. PID reuse
    # can never subtract another process's accumulated CPU from the new one.
    managed = sum(max(0, ticks - previous["process_ticks"].get(key, 0)) for key, ticks in process_ticks.items())
    total = host_ticks[0] - previous["host_ticks"][0]
    idle = host_ticks[1] - previous["host_ticks"][1]
    if total < 0 or idle < 0 or idle > total:
        raise RuntimeError("host CPU tick counters moved backwards")
    result.update(interval_seconds=elapsed, managed_observed_cores=managed / hz / elapsed, host_busy_cores=(total - idle) / hz / elapsed, host_total_cores=total / hz / elapsed)
    if result["managed_observed_cores"] > BOUNDS["managed_observed_cpu_cores"]:
        raise RuntimeError("managed observed CPU exceeds predeclared 12-core budget")
    return result, state


def sample(root, namespace):
    global CPU_PREVIOUS
    table = {}
    identities = {}
    for path in Path("/proc").iterdir():
        if not path.name.isdigit():
            continue
        try:
            fields = (path / "stat").read_text().rsplit(")", 1)[1].split()
            table[int(path.name)] = int(fields[1])
            identities[int(path.name)] = fields[19]
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    descendants = {os.getpid()}
    # ControlMaster and detached agent supervisors may be reparented to init.
    # Include processes carrying this private run's paths, not just descendants.
    for pid in table:
        try:
            argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            if any(arg.startswith((str(root).encode() + b"/", str(namespace).encode() + b"/", b"ssh: " + str(root).encode() + b"/")) or arg.startswith(b"ControlPath=" + str(root).encode() + b"/") for arg in argv):
                descendants.add(pid)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    while True:
        more = {pid for pid, parent in table.items() if parent in descendants}
        if more <= descendants:
            break
        descendants |= more
    rss = fds = 0
    kinds = {}
    process_ticks = {}
    for pid in descendants:
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            if fields[0] == "Z" or fields[19] != identities.get(pid):
                continue
            process_ticks[str(pid) + ":" + fields[19]] = int(fields[11]) + int(fields[12])
            rss += int(Path(f"/proc/{pid}/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
            fds += len(list(Path(f"/proc/{pid}/fd").iterdir()))
            argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            name = Path(argv[0].decode(errors="replace")).name if argv else ""
            kind = "ssh_controlmaster" if argv and argv[0].startswith(b"ssh: ") else "agent_supervisor" if b"-supervise" in argv else "agent" if name == "rdev-agent" else "ssh" if name == "ssh" else "sshd" if name.startswith("sshd") else "other"
            kinds[kind] = kinds.get(kind, 0) + 1
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    disk = 0
    for directory in (root, namespace):
        for parent, _, files in os.walk(directory):
            for name in files:
                try:
                    disk += (Path(parent) / name).lstat().st_size
                except FileNotFoundError:
                    pass
    storage = {"job_start_tombstones": len(list(namespace.glob("agent-*/job-start-intents/*.json"))), "retained_job_directories": len(list(namespace.glob("agent-*/jobs/job_*"))), "sync_outcomes": len(list(namespace.glob("agent-*/.sync/.outcomes/*.json")))}
    mutations = root / "broker.sock.mutations"
    if mutations.exists():
        if mutations.stat().st_size > 8 << 20:
            raise RuntimeError("mutation registry exceeded probe inspection bound")
        data = json.loads(mutations.read_text())
        storage["broker_mutation_records"] = len(data.get("records", []))
        storage["broker_ambiguous_records"] = sum(r.get("state") == "ambiguous" for r in data.get("records", []))
        owners = {}
        for record in data.get("records", []):
            owner = record["owner"]
            owners[owner] = owners.get(owner, 0) + 1
        storage["max_owner_mutation_records"] = max(owners.values(), default=0)
        if storage["max_owner_mutation_records"] > 1024:
            raise RuntimeError("actual per-owner mutation registry exceeds 1024 records")
    # Linux guest fields already contribute to user/nice, so sum the first
    # eight fields exactly once; idle includes iowait as non-executing time.
    ticks = [int(value) for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
    cpu, CPU_PREVIOUS = cpu_interval(process_ticks, (sum(ticks), ticks[3] + ticks[4]), time.monotonic(), CPU_PREVIOUS)
    return {"time_unix": time.time(), "rss_bytes": rss, "fds": fds, "processes": len(descendants), "process_kinds": kinds, "cpu": cpu, "disk_bytes": disk, "storage_records": storage}


def supervise(root):
    spec = json.loads((root / "run.json").read_text())
    report = dict(spec, status="starting", supervisor={"pid": os.getpid(), "start_ticks": identity(os.getpid())}, records=[], elapsed_seconds=0, fleet=[], fault_events=[])
    namespace = Path(spec["namespace"])
    processes = []
    servers = {}
    workers = {}
    paused_targets = {}
    network = None
    network_completed = 0
    broker_crashed = False
    worker_crashed = False
    agent_crashed = False
    cancellation_done = False
    idle_baseline = None
    next_idle = 120 if spec["seconds"] >= 7200 or spec.get("idle_smoke") else float("inf")
    expected_stopped = set()
    fleet_control = {"lock": threading.Lock(), "active": False, "quiesced": False}
    fleet_thread = None
    def quiesce():
        with fleet_control["lock"]:
            fleet_control["quiesced"] = True
        write(root / "maintenance", {"started_unix": time.time()})
        barrier_deadline = time.monotonic() + 45
        while True:
            parked = [json.loads((root / f"worker-{i}.json").read_text()).get("parked", False) for i in range(spec["clients"])]
            with fleet_control["lock"]:
                fleet_active = fleet_control["active"]
            if all(parked) and not fleet_active:
                return
            if report.get("fleet_error"):
                raise RuntimeError(report["fleet_error"])
            if time.monotonic() >= barrier_deadline:
                raise RuntimeError("writer/Fleet maintenance barrier deadline")
            time.sleep(.05)
    def unquiesce():
        (root / "maintenance").unlink(missing_ok=True)
        with fleet_control["lock"]:
            fleet_control["quiesced"] = False
    def spawn(argv, log, env=None):
        stream = (root / log).open("ab")
        child = subprocess.Popen([str(x) for x in argv], cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
        stream.close()
        processes.append(child)
        report["records"].append({"pid": child.pid, "start_ticks": identity(child.pid), "kind": log})
        write(root / "status.json", report)
        return child
    def cancellation_probe():
        credential = json.loads((root / "credential-0.json").read_text())
        logger = json.loads((root / "worker-0.json").read_text())["logger"]
        channel = connect(root, credential["owner"], credential["token"])
        def status():
            return checked(channel, {"id": "cancel-status", "owner": credential["owner"], "operation": "status"})["shared_waits"]
        def job():
            return checked(channel, {"id": "cancel-job", "owner": credential["owner"], "host": logger["host"], "operation": "job_status", "wire": {"op": "job_status", "job": {"id": logger["id"]}}})["wire"]["job"]["info"]
        try:
            before_job = job()
            before_calls = [json.loads((root / f"worker-{i}.json").read_text())["calls"] for i in range(spec["clients"])]
            observers = [spawn([sys.executable, root / "harness.py", "_observer", str(root), str(i)], f"observer-{i}.log") for i in range(2)]
            deadline = time.monotonic() + 5
            while status()["subscribers"] < 2:
                if time.monotonic() >= deadline or any(p.poll() is not None for p in observers):
                    raise RuntimeError("two actual job observers were not concurrently admitted")
                time.sleep(.02)
            expected_stopped.add(observers[0].pid)
            observers[0].kill()
            observers[0].wait(timeout=5)
            deadline = time.monotonic() + 3
            while status()["subscribers"] != 1:
                if time.monotonic() >= deadline:
                    raise RuntimeError("observer disconnect did not release exactly its own subscription")
                time.sleep(.02)
            expected_stopped.add(observers[1].pid)
            observers[1].wait(timeout=10)
            survivor = json.loads((root / "observer-1.json").read_text())["result"]
            after_job = job()
            after_calls = [json.loads((root / f"worker-{i}.json").read_text())["calls"] for i in range(spec["clients"])]
            # terminal describes the wait operation's completed response. The
            # detached job's state and timed_out describe its independent life.
            if not survivor["wire"]["job"].get("timed_out") or survivor["wire"]["job"]["info"]["state"] != "running" or after_job["state"] != "running" or after_job["pid"] != before_job["pid"] or Path(logger["marker"]).read_bytes() != b"x":
                raise RuntimeError("one observer's death changed the surviving wait or durable job")
            if spec["clients"] > 1 and not any(after_calls[i] > before_calls[i] for i in range(1, spec["clients"])):
                raise RuntimeError("other owners made no progress during observer cancellation")
            return {"phase": "observer-process-SIGKILL", "killed_pid": observers[0].pid, "surviving_pid": observers[1].pid, "job_supervisor_pid": after_job["pid"], "subscribers_before": 2, "subscribers_after_cancel": 1, "other_owner_call_deltas": [b - a for a, b in zip(before_calls, after_calls)], "result": "independent wait deadline and detached job preserved", "time_unix": time.time()}
        finally:
            channel.close()
    def idle_window(duration, phase):
        quiesce()
        started_idle = time.monotonic()
        samples = []
        while True:
            if (root / "cancel").exists():
                raise RuntimeError("cancellation requested during idle observation")
            metric = sample(root, namespace)
            metric["pool"] = pool_health(root, not spec["allow_dirty_smoke"])
            metric["phase"] = phase
            for name in ("rss_bytes", "fds", "processes", "disk_bytes"):
                if metric[name] > BOUNDS[name]:
                    raise RuntimeError(name + " exceeds fixed idle budget")
            if any(child.pid not in expected_stopped and child.poll() is not None for child in processes):
                raise RuntimeError("managed process exited during idle window")
            if any(time.monotonic() - json.loads((root / f"worker-{i}.json").read_text())["updated_monotonic"] > BOUNDS["max_sample_gap_seconds"] for i in range(spec["clients"])):
                raise RuntimeError("parked worker heartbeat gap during idle window")
            samples.append(metric)
            if len(samples) > 1 and metric["time_unix"] - samples[-2]["time_unix"] > BOUNDS["max_sample_gap_seconds"]:
                raise RuntimeError("periodic idle observation gap exceeds fixed budget")
            with (root / "samples.jsonl").open("a") as stream:
                stream.write(json.dumps(metric) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            report.update(latest_sample=metric, elapsed_seconds=time.monotonic() - start)
            write(root / "status.json", report)
            if time.monotonic() - started_idle >= duration:
                break
            time.sleep(BOUNDS["sample_seconds"])
        unquiesce()
        return samples
    try:
        for name, digest in spec["artifacts"].items():
            if sha(root / name) != digest:
                raise RuntimeError("frozen artifact modified")
        namespace.mkdir(mode=0o700)
        (root / "sources").mkdir()
        write(namespace / "run-owner.json", {"run_id": spec["run_id"]})
        command(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", namespace / "identity"], stdout=subprocess.DEVNULL)
        report["ssh_host_key_digests"] = []
        hosts, known, config = [], [], []
        for index in range(spec["targets"]):
            host_key = namespace / f"host-key-{index}"
            command(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", host_key], stdout=subprocess.DEVNULL)
            key = host_key.with_name(host_key.name + ".pub").read_text().split()
            report["ssh_host_key_digests"].append(hashlib.sha256(" ".join(key[:2]).encode()).hexdigest())
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            server_config = namespace / f"sshd-{index}.conf"
            server_config.write_text(f"Port {port}\nListenAddress 127.0.0.1\nHostKey {host_key}\nPidFile {namespace / ('sshd-' + str(index) + '.pid')}\nAuthorizedKeysFile {namespace / 'identity.pub'}\nStrictModes yes\nPubkeyAuthentication yes\nPasswordAuthentication no\nKbdInteractiveAuthentication no\nUsePAM no\nPermitRootLogin prohibit-password\nAllowUsers {pwd.getpwuid(os.geteuid()).pw_name}\nAllowTcpForwarding no\nX11Forwarding no\nLogLevel ERROR\n")
            server = spawn([shutil.which("sshd"), "-D", "-f", server_config], f"sshd-{index}.log")
            servers[index] = server
            deadline = time.monotonic() + 5
            while True:
                if server.poll() is not None:
                    raise RuntimeError("sshd exited")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("sshd readiness timeout")
                    time.sleep(.02)
            name = f"target-{index:03d}"
            (namespace / f"business-{index}").mkdir(mode=0o700)
            state = namespace / f"agent-{index}"
            state.mkdir(mode=0o700)
            scope = {"max_bytes": 64 << 20, "retention_sec": 60, "keep_last_jobs": 2, "high_watermark": .85, "low_watermark": .7, "min_free_bytes": 0}
            write(state / "storage-policy.json", {"local": scope, "remote_state": scope, "per_job": {"max_stdout_bytes": BOUNDS["job_log_bytes"], "max_stderr_bytes": BOUNDS["job_log_bytes"], "on_log_limit": "truncate_oldest"}, "cleanup": {"interval_sec": 30, "max_delete_bytes": 4 << 20, "max_delete_jobs": 8, "max_scan_jobs": 32, "dry_run": False}})
            hosts.append({"name": name, "addr": name, "remote_dir": namespace.name + "/agent-" + str(index), "login_shell": False})
            known.append(f"[127.0.0.1]:{port} {' '.join(key[:2])}\n")
            config.append(f"Host {name}\n HostName 127.0.0.1\n Port {port}\n")
        (namespace / "known_hosts").write_text("".join(known))
        config.append(f"Host *\n User {pwd.getpwuid(os.geteuid()).pw_name}\n IdentityFile {namespace / 'identity'}\n IdentitiesOnly yes\n IdentityAgent none\n UserKnownHostsFile {namespace / 'known_hosts'}\n GlobalKnownHostsFile /dev/null\n StrictHostKeyChecking yes\n BatchMode yes\n ConnectTimeout 5\n")
        (namespace / "ssh_config").write_text("".join(config))
        tools = root / "tools"
        tools.mkdir()
        (root / "tmp").mkdir()
        (tools / "ssh").write_text('#!/bin/sh\nexec "$RDEV_SCALE_REAL_SSH" -F "$RDEV_SCALE_SSH_CONFIG" "$@"\n')
        (tools / "ssh").chmod(0o700)
        env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"], TMPDIR=str(root / "tmp"), RDEV_SCALE_REAL_SSH=shutil.which("ssh"), RDEV_SCALE_SSH_CONFIG=str(namespace / "ssh_config"), RDEV_RELEASE_POLICY=str(root / "release-policy.json"))
        write(root / "hosts.json", {"hosts": hosts})
        write(root / "broker.sock.json", {"max_hosts": 128, "idle_ttl": 1000000000, "max_warm_hosts": 16, "warm_idle_ttl": 10000000000})
        command([root / "rdevd", "principal-keygen", "-out", root / "principal.key"], stdout=subprocess.DEVNULL)
        grants = {}
        for index in [*range(spec["clients"]), "admin"]:
            owner = {"client_id": f"client-{index % 10}", "project_id": f"project-{index // 10}"} if index != "admin" else {"client_id": "scale-administrator", "project_id": "operations"}
            token = command([root / "rdevd", "principal-token", "-key-file", root / "principal.key", "-client-id", owner["client_id"], "-project-id", owner["project_id"], "-ttl", "24h"], capture_output=True).stdout.decode().strip()
            write(root / f"credential-{index}.json", {"owner": owner, "token": token})
            operations = ["status", "ping", "exec", "job_start", "job_status", "job_wait", "job_logs", "job_rm", "sync.push", "mutation.status"] if index != "admin" else ["pool.health", "approval.create", "policy.grant", "fleet.inventory.list", "fleet.inventory.import", "fleet.plan", "fleet.approve", "fleet.execute", "fleet.status"]
            grants[owner["client_id"] + "\x00" + owner["project_id"]] = dict.fromkeys(operations, True)
        write(root / "broker.sock.policy", grants)
        broker_argv = [root / "rdevd", "-socket", root / "broker.sock", "-ready-file", root / "ready", "-principal-key-file", root / "principal.key", "-hosts-file", root / "hosts.json", "-agent-dir", root / "agents"]
        broker = spawn(broker_argv, "broker.log", env)
        deadline = time.monotonic() + 15
        while not (root / "ready").exists():
            if broker.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError("broker readiness failed")
            time.sleep(.05)
        if spec["workload"] == "mixed":
            admin = json.loads((root / "credential-admin.json").read_text())
            control = connect(root, admin["owner"], admin["token"])
            inventory = checked(control, {"id": "inventory-list", "owner": admin["owner"], "operation": "fleet.inventory.list", "fleet": {}})["inventory"]
            inventory = checked(control, {"id": "inventory-import", "owner": admin["owner"], "operation": "fleet.inventory.import", "fleet": {"revision": inventory["revision"]}})["inventory"]
            if len(inventory["records"]) != spec["targets"]:
                raise RuntimeError("distinct agent target inventory mismatch")
            for host in inventory["records"]:
                for operation in ("job_start", "job_status"):
                    checked(control, {"id": "host-authorize", "owner": admin["owner"], "operation": "policy.grant", "grant_owner": admin["owner"], "grant_host": host["host_id"], "grant_capability": "job", "grant_operation": operation})
            control.close()
            report["host_ids"] = [h["host_id"] for h in inventory["records"]]
        for index in range(spec["clients"]):
            workers[index] = spawn([sys.executable, root / "harness.py", "_worker", str(root), str(index)], f"worker-{index}.log")
        deadline = time.monotonic() + 15
        while len(list(root.glob("worker-*.json"))) < spec["clients"]:
            if time.monotonic() >= deadline:
                raise RuntimeError("client start barrier timeout")
            time.sleep(.05)
        start = last = refreshed = time.monotonic()
        progress_watch = {i: {"count": 0, "at": start} for i in range(spec["clients"])}
        report.update(status="running", started_unix=time.time())
        write(root / "start-barrier", {"started_unix": report["started_unix"]})
        if spec["workload"] == "mixed":
            fleet_thread = threading.Thread(target=fleet_loop, args=(root, spec, report, fleet_control), daemon=True)
            fleet_thread.start()
        while time.monotonic() - start < spec["seconds"]:
            if (root / "cancel").exists():
                report["status"] = "canceled"
                break
            now = time.monotonic()
            if report.get("fleet_error"):
                raise RuntimeError(report["fleet_error"])
            if now - refreshed >= 12 * 3600:
                # Issued tokens have a strict 24-hour maximum. Renew the same
                # owners before expiry; no policy, broker or namespace reset.
                for index in [*range(spec["clients"]), "admin"]:
                    credential = json.loads((root / f"credential-{index}.json").read_text())
                    owner = credential["owner"]
                    credential["token"] = command([root / "rdevd", "principal-token", "-key-file", root / "principal.key", "-client-id", owner["client_id"], "-project-id", owner["project_id"], "-ttl", "24h"], capture_output=True).stdout.decode().strip()
                    write(root / f"credential-{index}.json", credential)
                refreshed = now
            if now - last > BOUNDS["max_sample_gap_seconds"]:
                raise RuntimeError("observation gap exceeds fixed budget")
            if spec["workload"] == "mixed" and not cancellation_done and now - start >= 20:
                if all(json.loads((root / f"worker-{i}.json").read_text()).get("cycles", 0) for i in range(spec["clients"])):
                    report["fault_events"].append(cancellation_probe())
                    cancellation_done = True
            if network is None and now - start >= next_idle:
                samples = idle_window(BOUNDS["idle_seconds"], "periodic-idle")
                tail = check_idle(samples, idle_baseline, full=True)
                if idle_baseline is None:
                    idle_baseline = tail
                report.setdefault("periodic_idle_windows", []).append({"started_unix": samples[0]["time_unix"], "ended_unix": samples[-1]["time_unix"], "sample_count": len(samples), "tail": tail})
                next_idle += BOUNDS["idle_interval_seconds"]
                now = last = time.monotonic()
                for watch in progress_watch.values():
                    watch["at"] = now
            if spec["faults"] and network is None and network_completed < 2 and now - start >= spec["seconds"] * (.2 if network_completed == 0 else .6):
                quiesce()
                indices = list(range(0, spec["targets"], 2)) if network_completed == 0 else list(range(spec["targets"]))
                before = [json.loads((root / f"worker-{i}.json").read_text()) for i in range(spec["clients"])]
                network = {"phase": "partial-sshd-path-stop" if network_completed == 0 else "all-sshd-path-stop", "started_unix": time.time(), "targets": indices, "before_calls": [s["calls"] for s in before], "before_expected_errors": sum(s["expected_fault_errors"] for s in before), "next": time.monotonic() + 10, "step": "stopped", "restored_batches": []}
                network["logger_bytes_before"] = logger_progress(root, namespace, spec["clients"], indices)
                for target in indices:
                    paused_targets[target] = process_tree([servers[target].pid])
                # Write recovery authority before the first stop signal. A
                # killed supervisor must not strand stopped sshd descendants.
                write(root / "network-fault.json", {"targets": indices, "until_monotonic": time.monotonic() + 90, "phase": network["phase"], "paused_process_records": [record for records in paused_targets.values() for record in records]})
                for records in paused_targets.values():
                    for record in records:
                        if live(record):
                            os.kill(record["pid"], signal.SIGSTOP)
                unquiesce()
            if network and time.monotonic() >= network["next"]:
                if paused_targets:
                    if not network["restored_batches"]:
                        network["logger_bytes_during_fault"] = {}
                        for path, before_bytes in network["logger_bytes_before"].items():
                            after_bytes = json.loads(Path(path).read_text())["stdout_ledger"]["original_bytes"]
                            if after_bytes <= before_bytes:
                                raise RuntimeError("detached logger stopped with its SSH path")
                            network["logger_bytes_during_fault"][path] = after_bytes
                    batch = sorted(paused_targets)[:max(1, (len(network["targets"]) + 1) // 2)]
                    for target in batch:
                        resume_records(paused_targets.pop(target))
                    network["restored_batches"].append({"targets": batch, "time_unix": time.time()})
                    network["next"] = time.monotonic() + (5 if paused_targets else 10)
                else:
                    after = [json.loads((root / f"worker-{i}.json").read_text()) for i in range(spec["clients"])]
                    errors = sum(s["expected_fault_errors"] for s in after) - network["before_expected_errors"]
                    if errors == 0:
                        raise RuntimeError("real stopped SSH paths produced no observed failure")
                    if network_completed == 0 and spec["clients"] >= 2 and not any(s["calls"] > network["before_calls"][i] for i, s in enumerate(after) if i % 2 == 1):
                        raise RuntimeError("partial SSH outage starved all unaffected clients")
                    network.update(ended_unix=time.time(), observed_fault_errors=errors, result="recovered")
                    report["fault_events"].append(network)
                    network = None
                    network_completed += 1
                    (root / "network-fault.json").unlink()
            if spec["broker_crash"] and not broker_crashed and network is None and now - start >= spec["seconds"] / 2:
                event = {"phase": "broker-SIGKILL-after-completed-mutations", "started_unix": time.time(), "scope": "quiesced writers, same namespace and persisted ledger; mid-flight ambiguity covered separately by runtime fault harness"}
                quiesce()
                expected_stopped.add(broker.pid)
                broker.kill()
                broker.wait(timeout=5)
                broker = spawn(broker_argv, "broker.log", env)
                credential = json.loads((root / "credential-0.json").read_text())
                readiness = time.monotonic() + 10
                while True:
                    try:
                        probe = connect(root, credential["owner"], credential["token"])
                        probe.close()
                        break
                    except (OSError, RuntimeError):
                        if broker.poll() is not None or time.monotonic() >= readiness:
                            raise RuntimeError("broker crash recovery handshake deadline")
                        time.sleep(.05)
                event.update(ended_unix=time.time(), replacement_pid=broker.pid, result="recovered")
                report["fault_events"].append(event)
                broker_crashed = True
                unquiesce()
            if spec["faults"] and not worker_crashed and network is None and network_completed == 2:
                quiesce()
                old = workers[0]
                expected_stopped.add(old.pid)
                old.kill()
                old.wait(timeout=5)
                workers[0] = spawn([sys.executable, root / "harness.py", "_worker", str(root), "0"], "worker-0-recovered.log")
                deadline = time.monotonic() + 10
                while json.loads((root / "worker-0.json").read_text())["pid"] != workers[0].pid:
                    if time.monotonic() >= deadline or workers[0].poll() is not None:
                        raise RuntimeError("same-owner client recovery barrier failed")
                    time.sleep(.05)
                report["fault_events"].append({"phase": "client-SIGKILL-quiesced-cycle", "old_pid": old.pid, "new_pid": workers[0].pid, "time_unix": time.time(), "result": "restored counters, principal and next cycle without replay"})
                worker_crashed = True
                unquiesce()
            if spec["faults"] and worker_crashed and not agent_crashed and network is None:
                quiesce()
                logger = json.loads((root / "worker-0.json").read_text()).get("logger")
                target = int(logger["host"].split("-")[-1]) if logger else 0
                old_pid = ping_probe(root, target)
                old_identity = identity(old_pid)
                if old_identity is None or not live({"pid": old_pid, "start_ticks": old_identity}):
                    raise RuntimeError("agent vanished before fault identity binding")
                os.kill(old_pid, signal.SIGKILL)
                deadline = time.monotonic() + 15
                new_pid = old_pid
                while time.monotonic() < deadline:
                    try:
                        new_pid = ping_probe(root, target)
                        if new_pid != old_pid:
                            break
                    except (OSError, RuntimeError):
                        pass
                    time.sleep(.05)
                if new_pid == old_pid:
                    raise RuntimeError("agent did not recover a new handshake identity")
                report["fault_events"].append({"phase": "agent-SIGKILL-serve-only", "target": target, "old_pid": old_pid, "old_start_ticks": old_identity, "new_pid": new_pid, "time_unix": time.time(), "result": "reconnected"})
                agent_crashed = True
                unquiesce()
            if any(child.pid not in expected_stopped and child.poll() is not None for child in processes):
                raise RuntimeError("test process exited; run is not restarted or spliced")
            metric = sample(root, namespace)
            metric["pool"] = pool_health(root, not spec["allow_dirty_smoke"])
            if metric["storage_records"].get("broker_mutation_records", 0) > BOUNDS["mutation_operations"]:
                raise RuntimeError("actual broker mutation registry exceeds fixed storage budget")
            stats = [json.loads((root / f"worker-{i}.json").read_text()) for i in range(spec["clients"])]
            for index, worker_stats in enumerate(stats):
                if time.monotonic() - worker_stats["updated_monotonic"] > BOUNDS["max_sample_gap_seconds"]:
                    raise RuntimeError("worker heartbeat observation gap")
                progress = worker_stats["calls"] + worker_stats["expected_fault_errors"] + sum(worker_stats["operations"].values())
                if progress > progress_watch[index]["count"]:
                    progress_watch[index] = {"count": progress, "at": time.monotonic()}
                elif time.monotonic() - progress_watch[index]["at"] > BOUNDS["max_sample_gap_seconds"]:
                    raise RuntimeError("worker made no progress within the fixed window")
            if sum(s["errors"] for s in stats):
                raise RuntimeError("connectivity error; inspect private worker results")
            if sum(s["mutations"] for s in stats) + sum(f["targets"] for f in report["fleet"]) > BOUNDS["mutation_operations"]:
                raise RuntimeError("predeclared mutation storage budget exhausted")
            metric["clients"] = [{k: s.get(k) for k in ("pid", "calls", "errors", "expected_fault_errors", "max_latency_ms", "scheduler")} for s in stats]
            for name in ("rss_bytes", "fds", "processes", "disk_bytes"):
                if metric[name] > BOUNDS[name]:
                    raise RuntimeError(name + " exceeds fixed pre-run budget")
            with (root / "samples.jsonl").open("a") as stream:
                stream.write(json.dumps(metric) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            report.update(elapsed_seconds=now - start, latest_sample=metric)
            write(root / "status.json", report)
            last = now
            time.sleep(BOUNDS["sample_seconds"])
        if report["status"] == "running":
            # Admission is fenced before taking the final snapshot. Waiting on
            # both worker acknowledgments and Fleet's lock closes the window in
            # which a successful mutation could be absent from the final proof.
            quiesce()
            write(root / "stop", {})
            if fleet_thread:
                fleet_thread.join(timeout=5)
                if fleet_thread.is_alive():
                    raise RuntimeError("Fleet observer did not stop after its barrier")
            for child, record in zip(processes, report["records"]):
                if record["kind"].startswith("worker-"):
                    child.wait(timeout=5)
            for name, digest in spec["artifacts"].items():
                if sha(root / name) != digest:
                    raise RuntimeError("frozen input changed during the run")
            stats = [json.loads((root / f"worker-{i}.json").read_text()) for i in range(spec["clients"])]
            for index, entry in enumerate(stats):
                if spec["workload"] != "mixed":
                    continue
                logger = entry["logger"]
                credential = json.loads((root / f"credential-{index}.json").read_text())
                channel = connect(root, credential["owner"], credential["token"])
                try:
                    query = {"id": "final-logger", "owner": credential["owner"], "host": logger["host"], "operation": "job_status", "wire": {"op": "job_status", "job": {"id": logger["id"]}}}
                    info = checked(channel, query)["wire"]["job"]["info"]
                    ledger = info["stdout_ledger"]
                    if info["state"] != "exited" or ledger["original_bytes"] <= BOUNDS["job_log_bytes"] or ledger["retained_bytes"] > BOUNDS["job_log_bytes"] or ledger["dropped_bytes"] <= 0 or Path(logger["marker"]).read_bytes() != b"x":
                        raise RuntimeError("continuous logger lost progress, duplicated, or exceeded its rolling budget")
                    approved_wire(root, channel, credential["owner"], logger["host"], {"op": "job_rm", "job": {"id": logger["id"]}}, "op_scale_" + spec["run_id"][:12] + f"_{index}_logger_remove")
                    entry["mutations"] += 1
                    entry["marker_proofs"] += 1
                    entry["log_rotation_proofs"] += 1
                    entry["logger_ledger"] = ledger
                    write(root / f"worker-{index}.json", entry)
                finally:
                    channel.close()
            final_storage = sample(root, namespace)["storage_records"]
            if final_storage.get("broker_mutation_records", 0) > BOUNDS["mutation_operations"]:
                raise RuntimeError("actual final mutation ledger exceeds budget")
            report["final_storage_records"] = final_storage
            visited = set().union(*(set(s["targets"]) for s in stats))
            if len(visited) != spec["targets"] or any(s["calls"] == 0 for s in stats):
                raise RuntimeError("not every real target/client made progress")
            if spec["workload"] == "mixed" and any(s["cycles"] == 0 for s in stats):
                raise RuntimeError("mixed workload did not complete for every owner")
            if spec["workload"] == "mixed" and not report["fleet"]:
                raise RuntimeError("Fleet workload did not complete")
            if spec["workload"] == "mixed" and not cancellation_done:
                raise RuntimeError("observer cancellation isolation was not exercised")
            if spec["faults"] and (network_completed != 2 or not worker_crashed or not agent_crashed):
                raise RuntimeError("requested fault phases did not all finish")
            if spec["broker_crash"] and not broker_crashed:
                raise RuntimeError("requested broker crash was not exercised")
            if spec["workload"] == "mixed":
                for index, stats_entry in enumerate(stats):
                    for cycle in range(stats_entry["cycles"]):
                        for kind in ("exec", "job"):
                            markers = list(namespace.glob(f"business-*/{kind}-{index}-{cycle}"))
                            if len(markers) != 1 or markers[0].read_bytes() != b"x":
                                raise RuntimeError("retained success marker changed across broker recovery")
            report.update(status="workload-scope-passed", elapsed_seconds=time.monotonic() - start, targets_visited=len(visited), completed_ordinary_mutations=sum(s["mutations"] for s in stats), completed_fleet_targets=sum(f["targets"] for f in report["fleet"]), marker_proofs=sum(s["marker_proofs"] for s in stats) + sum(f["targets"] for f in report["fleet"]))
            report["client_totals"] = [{k: s[k] for k in ("pid", "calls", "errors", "operations", "cycles", "mutations", "marker_proofs", "latency_histogram", "max_latency_ms")} for s in stats]
            report["latency_histogram_bounds_ms"] = LATENCY_MS
            report["throughput_ping_per_second"] = sum(s["calls"] for s in stats) / report["elapsed_seconds"]
            # Stop only workload observers, keep the same broker/state/sshd
            # alive through >2x the configured warm-idle TTL, and retain every
            # resource sample so reclamation can be judged beyond two endpoints.
            write(root / "stop", {})
            report["status"] = "observing-idle"
            write(root / "status.json", report)
            idle_samples = []
            for _ in range(5):
                time.sleep(5)
                metric = sample(root, namespace)
                metric["pool"] = pool_health(root, not spec["allow_dirty_smoke"])
                metric["phase"] = "idle"
                idle_samples.append(metric)
                with (root / "samples.jsonl").open("a") as stream:
                    stream.write(json.dumps(metric) + "\n")
                report["latest_sample"] = metric
                write(root / "status.json", report)
            check_idle(idle_samples)
            report.update(status="workload-scope-passed", idle_samples=idle_samples)
    except BaseException as error:
        report.update(status="canceled" if (root / "cancel").exists() else "failed", error=str(error))
    finally:
        for records in paused_targets.values():
            resume_records(records)
        write(root / "stop", {})
        # Every process record is identity-bound. Parent exit/PID reuse never
        # authorizes a signal to an unrelated process.
        for child, record in reversed(list(zip(processes, report["records"]))):
            if live(record):
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for child in processes:
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        # ControlMaster deliberately detaches. Explicitly close only sockets
        # below this run's private TMPDIR; never touch another test or user mux.
        close_mux(root)
        teardown_deadline = time.monotonic() + 5
        while True:
            report["remaining_managed_processes"] = owned_processes(root, namespace)
            if not report["remaining_managed_processes"] or time.monotonic() >= teardown_deadline:
                break
            time.sleep(.05)
        if report["remaining_managed_processes"] and report["status"] == "workload-scope-passed":
            report.update(status="failed", error="managed processes remain after bounded teardown; preserve namespace")
        missing = list(MISSING)
        if not report.get("periodic_idle_windows"):
            missing.append("330-second periodic idle resource regression windows were not run")
        if not spec["faults"]:
            missing.append("partial/full SSH path and client/agent crash stages were not requested")
        if "goroutines" not in report.get("latest_sample", {}).get("pool", {}):
            missing.append("artifact lacks broker goroutine/fixed observer counter metrics")
        if "dial_admission" not in report.get("latest_sample", {}).get("pool", {}):
            missing.append("artifact lacks aggregate dial/backoff/queue fixed counters and histograms")
        report.update(ended_unix=time.time(), production_gate="not-run", missing_coverage=missing)
        write(root / "status.json", report)


def main():
    os.umask(0o077)
    if len(sys.argv) > 1 and sys.argv[1] in ("_supervise", "_worker", "_observer"):
        root = Path(sys.argv[2])
        {"_supervise": lambda: supervise(root), "_worker": lambda: worker(root, int(sys.argv[3])), "_observer": lambda: observer(root, int(sys.argv[3]))}[sys.argv[1]]()
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "status", "cancel", "cleanup"))
    parser.add_argument("--run", type=Path, required=True, help="new private absolute /tmp directory for start")
    parser.add_argument("--artifacts", type=Path, help="directory containing rdevd and agents/rdev-agent-linux-ARCH")
    parser.add_argument("--artifact-source", help="explicit clean artifact commit; requires identical cmd/internal/go.mod/go.sum/Makefile inputs")
    parser.add_argument("--go", default=os.environ.get("RDEV_GO", "go"), help="Go tool used to inspect immutable binary build metadata")
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--targets", type=int, default=100)
    parser.add_argument("--clients", type=int, default=20)
    parser.add_argument("--allow-dirty-smoke", action="store_true")
    parser.add_argument("--workload", choices=("mixed", "connectivity"), default="mixed")
    parser.add_argument("--broker-crash", action="store_true", help="at midpoint, barrier all writers then SIGKILL/restart the same broker state")
    parser.add_argument("--faults", action="store_true", help="real partial/full SSH path SIGSTOP, staggered resume, client and serve-agent SIGKILL")
    parser.add_argument("--idle-smoke", action="store_true", help="exercise one real 330-second idle window in an explicitly labelled 600-second engineering smoke")
    args = parser.parse_args()
    root = args.run.resolve()
    if args.action == "start":
        if sys.platform != "linux":
            parser.error("the isolated /proc capacity harness requires Linux")
        for tool in ("ssh", "sshd", "ssh-keygen", "rsync"):
            if shutil.which(tool) is None:
                parser.error("missing prerequisite: " + tool)
        if os.geteuid() == 0 and not Path("/run/sshd").exists():
            Path("/run/sshd").mkdir(mode=0o755)
        if not 60 <= args.seconds <= 86400 or not 1 <= args.targets <= 100 or not 1 <= args.clients <= 20 or args.targets % args.clients:
            parser.error("require 60..86400 seconds, 1..100 targets, 1..20 clients, targets divisible by clients")
        if args.broker_crash and args.seconds < 120:
            parser.error("broker crash phase requires at least 120 seconds")
        if args.faults and (args.seconds < 180 or args.targets < 2):
            parser.error("network/client/agent phases require at least 180 seconds and two targets")
        if args.idle_smoke and (not args.allow_dirty_smoke or args.seconds != 600 or args.faults):
            parser.error("--idle-smoke requires --allow-dirty-smoke --seconds 600 and separate fault runs")
        if args.artifacts is None or root.parent != Path("/tmp"):
            parser.error("start requires --artifacts and a new direct /tmp child")
        repo = Path(__file__).resolve().parent.parent
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
        artifact_commit = commit
        if args.artifact_source:
            artifact_commit = subprocess.check_output(["git", "rev-parse", args.artifact_source + "^{commit}"], cwd=repo, text=True).strip()
            if subprocess.run(["git", "diff", "--quiet", artifact_commit, "HEAD", "--", "cmd", "internal", "go.mod", "go.sum", "Makefile", ":(exclude)**/*_test.go"], cwd=repo).returncode:
                parser.error("artifact source differs in product/build inputs")
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repo))
        if args.allow_dirty_smoke and args.seconds > 600:
            parser.error("--allow-dirty-smoke is always limited to 600 seconds, including clean checkouts")
        if dirty and not args.allow_dirty_smoke:
            parser.error("dirty source permitted only for explicitly labelled smoke <=600 seconds")
        root.mkdir(mode=0o700)
        run_id = uuid.uuid4().hex
        namespace = Path.home() / (".rdev-scale-" + run_id)
        (root / "agents").mkdir()
        shutil.copyfile(__file__, root / "harness.py")
        shutil.copyfile(args.artifacts / "rdevd", root / "rdevd")
        (root / "rdevd").chmod(0o700)
        agents = list((args.artifacts / "agents").glob("rdev-agent-*"))
        if not agents:
            parser.error("no frozen agents found")
        for agent in agents:
            shutil.copyfile(agent, root / "agents" / agent.name)
        build_metadata = {}
        for binary in [root / "rdevd", *sorted((root / "agents").iterdir())]:
            metadata = command([args.go, "version", "-m", binary], capture_output=True).stdout.decode()
            build_metadata[str(binary.relative_to(root))] = metadata
            if not args.allow_dirty_smoke and ("vcs.revision=" + artifact_commit not in metadata or "vcs.modified=false" not in metadata):
                parser.error("binary/source identity mismatch; rebuild clean source before starting a long probe")
        write(root / "build-metadata.json", build_metadata)
        artifacts = {str(path.relative_to(root)): sha(path) for path in [root / "harness.py", root / "rdevd", *sorted((root / "agents").iterdir())]}
        spec = {"schema": 1, "kind": "real-ssh-scale", "run_id": run_id, "source_commit": commit, "source_dirty": dirty, "allow_dirty_smoke": args.allow_dirty_smoke, "artifact_build_metadata": "build-metadata.json", "seconds": args.seconds, "targets": args.targets, "clients": args.clients, "shared_host_instances": 1, "physical_machine_count": "unverified", "fault_domain": "one shared Linux kernel and filesystem", "namespace": str(namespace), "artifacts": artifacts, "workload": args.workload, "predeclared_bounds": BOUNDS, "bounds_basis": "conservative 16-CPU/32-GiB shared Linux instance; observed managed CPU <=12 cores (75% of 16) per sampling interval; fixed 20-minute cycles budget 5040 ordinary mutations plus 40 logger starts/removals and six 100-target Fleet plans/24h=5680, below existing 8192, no direct record deletion/reset; not production throughput/SLO", "production_gate": "not-run", "missing_coverage": MISSING}
        spec["broker_crash"] = args.broker_crash
        spec["artifact_source_commit"] = artifact_commit
        spec["workload_admission"] = {"large_response_concurrency": 1, "scope": "retained sync prepare+execute and job_wait; all 20 clients and daily mutation count retained", "basis": "28 MiB per sync plus 8 MiB per wait compete for unchanged global64MiB/owner32MiB ingress; control and ordinary work retain headroom", "saturated_sync_throughput_certification": False}
        spec["faults"] = args.faults
        spec["idle_smoke"] = args.idle_smoke
        write(root / "run.json", spec)
        write(root / "release-policy.json", {"schema_version": 1, "valid_until": (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"), "channels": ["dev"], "allow_unsigned_dev": True, "allow_test_roots": False, "roots": []})
        with (root / "supervisor.log").open("ab") as log:
            child = subprocess.Popen([sys.executable, root / "harness.py", "_supervise", root], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        print(json.dumps({"run": str(root), "supervisor_pid": child.pid, "status": "started; not passed"}))
        return
    spec = json.loads((root / "run.json").read_text())
    status = json.loads((root / "status.json").read_text())
    active = status["status"] in ("starting", "running", "observing-idle") and live(status["supervisor"])
    if args.action == "status":
        print(json.dumps({"run": str(root), "run_id": spec["run_id"], "status": status["status"] if active or status["status"] not in ("starting", "running", "observing-idle") else "interrupted", "elapsed_seconds": status["elapsed_seconds"], "production_gate": "not-run", "latest_sample_unix": status.get("latest_sample", {}).get("time_unix")}, indent=2))
    elif args.action == "cancel":
        write(root / "cancel", {"requested_unix": time.time()})
        if not active:
            write(root / "stop", {})
            fault = root / "network-fault.json"
            if fault.exists():
                resume_records(json.loads(fault.read_text()).get("paused_process_records", []))
            for record in reversed(status["records"]):
                if live(record):
                    try:
                        os.killpg(record["pid"], signal.SIGTERM)
                    except ProcessLookupError:
                        pass
            close_mux(root)
            # Orphan descendants retain this run's private argv/log paths.
            # Signal individual identities, never an unrecorded process group.
            for record in owned_processes(root, Path(spec["namespace"])):
                if live(record):
                    try:
                        os.kill(record["pid"], signal.SIGTERM)
                    except ProcessLookupError:
                        pass
            deadline = time.monotonic() + 5
            while owned_processes(root, Path(spec["namespace"])) and time.monotonic() < deadline:
                time.sleep(.05)
            # A private child can ignore TERM. Re-inventory and check each PID
            # identity immediately before KILL; never broaden to name-based kill.
            for record in owned_processes(root, Path(spec["namespace"])):
                if live(record):
                    try:
                        os.kill(record["pid"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            deadline = time.monotonic() + 5
            while owned_processes(root, Path(spec["namespace"])) and time.monotonic() < deadline:
                time.sleep(.05)
            remaining = owned_processes(root, Path(spec["namespace"]))
            status.update(orphan_cleanup={"ended_unix": time.time(), "remaining_processes": remaining, "result": "passed" if not remaining else "failed"})
            write(root / "status.json", status)
            if remaining:
                parser.error("private processes remain after identity-bound CONT/TERM/KILL; preserve namespace and recovery evidence")
        print("Cancellation requested; query status until stopped. Evidence retained.")
    else:
        if active or any(live(record) for record in status["records"]) or owned_processes(root, Path(spec["namespace"])):
            parser.error("run still has live identity-bound processes; cancel and inspect first")
        namespace = Path(spec["namespace"])
        if namespace.parent != Path.home() or namespace.name != ".rdev-scale-" + spec["run_id"] or json.loads((namespace / "run-owner.json").read_text())["run_id"] != spec["run_id"]:
            parser.error("namespace ownership mismatch")
        for process in Path("/proc").iterdir():
            if process.name.isdigit():
                try:
                    executable = os.readlink(process / "exe")
                    if executable.startswith(str(namespace) + "/"):
                        parser.error("an isolated agent/supervisor is still alive; preserve its state until it exits")
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass
        shutil.rmtree(namespace)
        print("Stopped test namespace removed; immutable inputs, timeline, and results retained.")


if __name__ == "__main__":
    main()
