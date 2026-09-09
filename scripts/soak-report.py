#!/usr/bin/env python3
"""Describe a frozen scale-soak run without writing its state or certifying it.

Usage: python3 scripts/soak-report.py --run /tmp/rdev-p8-soak-RUN --out /tmp/report.json
Only named metadata, samples and allowlisted frozen inputs are read. Credentials,
keys, agent state, marker files and business payloads are never inspected.
"""
import argparse
import contextlib
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time

MAX_JSON = 8 << 20
MAX_LINE = 512 << 10
MAX_SAMPLES = 100000
MAX_SAMPLE_BYTES = 2 << 30
MAX_SEGMENTS = 256
MAX_ARTIFACT = 96 << 20
RESOURCES = ("rss_bytes", "fds", "processes", "disk_bytes")
STORAGE = ("job_start_tombstones", "retained_job_directories", "sync_outcomes",
           "broker_mutation_records", "broker_ambiguous_records", "max_owner_mutation_records")
STAGES = ("agent_install", "agent_lookup", "agent_start", "canceled", "control_path",
          "handshake", "negotiation", "probe", "validation")
FAULTS = frozenset(("observer-process-SIGKILL", "partial-sshd-path-stop",
                   "all-sshd-path-stop", "broker-SIGKILL-after-completed-mutations",
                   "client-SIGKILL-quiesced-cycle", "agent-SIGKILL-serve-only"))
ARTIFACTS = frozenset(("harness.py", "rdev", "rdevd", *(
    "agents/rdev-agent-" + platform for platform in
    ("linux-amd64", "linux-arm64", "darwin-amd64", "darwin-arm64"))))
STATUSES = frozenset(("starting", "running", "observing-idle", "workload-scope-passed",
                      "failed", "canceled", "interrupted"))


class Invalid(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise Invalid(message)


def number(value, integer=False, maximum=2**64 - 1):
    require(type(value) in ((int,) if integer else (int, float)), "invalid numeric field")
    require(0 <= value <= maximum and math.isfinite(value), "numeric field outside bound")
    return value


def obj(value):
    require(type(value) is dict, "expected object")
    return value


def array(value, maximum):
    require(type(value) is list and len(value) <= maximum, "array outside bound")
    return value


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON field")
            result[key] = value
        return result
    try:
        return obj(json.loads(raw, object_pairs_hook=pairs,
                              parse_constant=lambda _: (_ for _ in ()).throw(Invalid("nonfinite JSON"))))
    except Invalid:
        raise
    except (UnicodeError, ValueError, RecursionError) as error:
        raise Invalid("malformed or excessively nested JSON") from error


@contextlib.contextmanager
def regular(root, name, maximum):
    # Relative names come only from fixed metadata names or ARTIFACTS. Open
    # every path component without following symlinks, including the run root.
    require(not Path(name).is_absolute() and all(p not in ("", ".", "..") for p in name.split("/")),
            "unsafe input name")
    descriptors = [os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
    try:
        parts = name.split("/")
        for part in parts[:-1]:
            descriptors.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                       dir_fd=descriptors[-1]))
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                     dir_fd=descriptors[-1])
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_size <= maximum, "input file outside bound")
            yield stream, info
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


def read_json(root, name, identities):
    with regular(root, name, MAX_JSON) as (stream, _):
        raw = stream.read(MAX_JSON + 1)
    require(len(raw) <= MAX_JSON, "JSON input outside bound")
    identities[name] = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    return decode(raw)


def frozen_inputs(root, spec):
    selected = obj(spec["artifacts"])
    require(0 < len(selected) <= len(ARTIFACTS) and set(selected) <= ARTIFACTS,
            "unrecognized frozen artifact")
    result = {}
    for name, expected in selected.items():
        require(type(expected) is str and re.fullmatch("[0-9a-f]{64}", expected), "invalid artifact digest")
        digest = hashlib.sha256()
        with regular(root, name, MAX_ARTIFACT) as (stream, before):
            remaining = before.st_size
            while remaining:
                block = stream.read(min(1 << 20, remaining))
                require(bool(block), "frozen input truncated during observation")
                digest.update(block)
                remaining -= len(block)
            after = os.fstat(stream.fileno())
        unchanged = (before.st_size, before.st_mtime_ns, before.st_ino) == (after.st_size, after.st_mtime_ns, after.st_ino)
        result[name] = {"declared_sha256": expected, "observed_sha256": digest.hexdigest(),
                        "matches": unchanged and digest.hexdigest() == expected}
    return result


def quantile(counts, bounds, percent):
    count = sum(counts)
    if not count:
        return None
    rank, total = (count * percent + 99) // 100, 0
    for index, n in enumerate(counts):
        total += n
        if total >= rank:
            return {"lower_exclusive_ms": 0 if index == 0 else bounds[index - 1],
                    "upper_inclusive_ms": bounds[index] if index < len(bounds) else None}


def workers(root, count, identities):
    result, cumulative, bounds = [], {}, None
    for index in range(count):
        worker = read_json(root, f"worker-{index}.json", identities)
        chosen = array(worker["latency_histogram_bounds_ms"], 64)
        require(chosen and all(number(x) > 0 for x in chosen)
                and all(b > a for a, b in zip(chosen, chosen[1:])), "invalid latency bounds")
        require(bounds is None or bounds == chosen, "worker latency bounds differ")
        bounds = chosen
        histogram = obj(worker["latency_histogram"])
        counts = {}
        for kind in ("cold_ms", "warm_ms"):
            values = array(histogram[kind], 65)
            require(len(values) == len(bounds) + 1, "histogram shape mismatch")
            values = [number(n, True) for n in values]
            counts[kind] = sum(values)
            cumulative[kind] = [a + b for a, b in zip(cumulative.get(kind, [0] * len(values)), values)]
        calls = number(worker["calls"], True)
        require(sum(counts.values()) == calls, "worker calls and cumulative histogram differ")
        result.append({"worker": index, "pid": number(worker["pid"], True),
                       "observed_worker_update_unix": number(worker["updated_unix"]),
                       "calls": calls, "sample_counts": counts,
                       "errors": number(worker["errors"], True),
                       "expected_fault_errors": number(worker["expected_fault_errors"], True),
                       "completed_cycles": number(worker["cycles"], True),
                       "mutations": number(worker["mutations"], True),
                       "workload_admission_wait_seconds": number(worker.get("workload_admission_wait_seconds", 0))})
    return {"workers": result, "bounds_ms": bounds,
            "cumulative": {kind: {"count": sum(values), "buckets": values,
                                   "p95_interval": quantile(values, bounds, 95),
                                   "p99_interval": quantile(values, bounds, 99)}
                           for kind, values in cumulative.items()},
            "limits": "Cumulative histogram intervals only. Rolling cold_ms/warm_ms arrays are ignored. No SLO is inferred; remote ping is not a broker control.status latency measurement."}


def event_windows(status):
    result = []
    for item in array(status.get("fault_events", []), 64):
        item = obj(item)
        require(item.get("phase") in FAULTS, "unrecognized fault phase")
        start = number(item.get("started_unix", item.get("time_unix")))
        end = number(item.get("ended_unix", start))
        require(end >= start, "fault time moved backwards")
        result.append({"phase": item["phase"], "started_unix": start, "ended_unix": end})
    return result


def values(row):
    out = {k: number(row[k], True) for k in RESOURCES}
    out.update({"storage." + k: number(obj(row["storage_records"])[k], True) for k in STORAGE})
    pool = obj(row["pool"])
    for key in ("base_transports", "reserved_hosts", "bulk_transports", "goroutines",
                "observer_metric_series", "observer_metric_series_bound"):
        out["pool." + key] = number(pool[key], True)
    out["ssh_controlmaster"] = number(obj(row["process_kinds"]).get("ssh_controlmaster", 0), True)
    return out


def begin_segment(phase, timestamp, sample_values):
    return {"phase": phase, "first_sample_unix": timestamp, "last_sample_unix": timestamp,
            "samples": 0, "first": sample_values, "last": sample_values,
            "minimum": dict(sample_values), "maximum": dict(sample_values),
            "observed_cpu_core_seconds": 0, "observed_cpu_interval_seconds": 0}


def add_segment(segment, timestamp, sample_values, row):
    segment["samples"] += 1
    segment["last_sample_unix"], segment["last"] = timestamp, sample_values
    for key, value in sample_values.items():
        segment["minimum"][key] = min(segment["minimum"][key], value)
        segment["maximum"][key] = max(segment["maximum"][key], value)
    cpu = obj(row["cpu"])
    interval = number(cpu["interval_seconds"])
    segment["observed_cpu_core_seconds"] += number(cpu["managed_observed_cores"]) * interval
    segment["observed_cpu_interval_seconds"] += interval


def finish_segment(segment):
    elapsed = segment["last_sample_unix"] - segment["first_sample_unix"]
    segment["sample_span_seconds"] = elapsed
    segment["endpoint_growth_per_hour"] = {
        key: (segment["last"][key] - segment["first"][key]) * 3600 / elapsed if elapsed else None
        for key in ("disk_bytes", *("storage." + k for k in STORAGE))}
    interval = segment["observed_cpu_interval_seconds"]
    segment["mean_observed_managed_cpu_cores"] = segment["observed_cpu_core_seconds"] / interval if interval else None
    return segment


def dial_values(row):
    admission = obj(row["pool"]["dial_admission"])
    connection = obj(admission["connection"])
    result = {key: number(admission[key], True) for key in
              ("backoff_waits", "backoff_duration_ns", "queue_waits", "queue_duration_ns")}
    for key in ("dial_started", "dial_succeeded", "dial_failed", "dial_canceled", "dial_duration_ns"):
        result[key] = number(connection[key], True)
    bounds = [1000000, 10000000, 100000000, 1000000000, 10000000000, 30000000000, -1]
    require(admission["queue_bucket_upper_ns"] == bounds, "dial histogram bounds differ")
    buckets = array(admission["queue_buckets"], 7)
    require(len(buckets) == 7, "dial histogram shape mismatch")
    result.update({"queue_bucket_" + str(i): number(n, True) for i, n in enumerate(buckets)})
    failures = obj(connection["dial_failures"])
    require(set(failures) <= set(STAGES), "unrecognized dial failure stage")
    result.update({"failure_" + key: number(failures.get(key, 0), True) for key in STAGES})
    sampled = {key: number(admission[key], True) for key in
               ("waiting", "waiting_peak", "states", "state_limit", "global_slots_in_use", "global_slot_limit")}
    sampled["dial_inflight"] = number(connection["dial_inflight"], True)
    return result, sampled


def samples(root, events, identities):
    segments, epochs, peaks = [], [], {}
    previous_time, previous_values, previous_counters, previous_epoch = None, None, None, None
    count = 0
    maximum_gap = 0
    state_decreases = {key: 0 for key in STORAGE}
    restarts = sorted(e["ended_unix"] for e in events if e["phase"] == "broker-SIGKILL-after-completed-mutations")
    digest = hashlib.sha256()
    tail_bytes = 0
    with regular(root, "samples.jsonl", MAX_SAMPLE_BYTES) as (stream, initial):
        remaining = initial.st_size
        while remaining:
            raw = stream.readline(min(MAX_LINE + 1, remaining))
            require(bool(raw), "sample prefix truncated during observation")
            remaining -= len(raw)
            digest.update(raw)
            require(len(raw) <= MAX_LINE, "sample row outside bound")
            if not raw.endswith(b"\n"):
                require(remaining == 0, "sample row outside bound")
                tail_bytes = len(raw)  # A concurrently appended final row is not invented or parsed.
                break
            count += 1
            require(count <= MAX_SAMPLES, "sample count outside bound")
            row = decode(raw)
            timestamp = number(row["time_unix"])
            require(previous_time is None or timestamp > previous_time, "sample timestamps did not advance")
            if previous_time is not None:
                maximum_gap = max(maximum_gap, timestamp - previous_time)
            current = values(row)
            if previous_values is not None:
                for key in STORAGE:
                    state_decreases[key] += current["storage." + key] < previous_values["storage." + key]
            phase = row.get("phase", "workload")
            require(phase in ("workload", "periodic-idle", "idle"), "unrecognized sample phase")
            if phase == "workload":
                for event in events:
                    if event["started_unix"] <= timestamp < event["ended_unix"]:
                        phase = event["phase"]
                        break
            if not segments or segments[-1]["phase"] != phase:
                require(len(segments) < MAX_SEGMENTS, "phase segment count outside bound")
                segments.append(begin_segment(phase, timestamp, current))
            add_segment(segments[-1], timestamp, current, row)
            counters, sampled = dial_values(row)
            epoch = sum(end <= timestamp for end in restarts)
            reset = previous_counters is not None and any(counters[k] < previous_counters[k] for k in counters)
            if not epochs or epoch != previous_epoch or reset:
                require(len(epochs) < 64, "counter epoch count outside bound")
                reason = "first_observation" if not epochs else "recorded_broker_restart" if epoch != previous_epoch else "unattributed_counter_reset"
                epochs.append({"reason": reason, "recorded_broker_epoch": epoch,
                               "first_sample_unix": timestamp, "last_sample_unix": timestamp,
                               "first_cumulative": counters, "last_cumulative": counters,
                               "observed_increments": {key: 0 for key in counters}, "samples": 0})
            elif previous_counters is not None:
                for key in counters:
                    epochs[-1]["observed_increments"][key] += counters[key] - previous_counters[key]
            epochs[-1]["last_sample_unix"], epochs[-1]["last_cumulative"] = timestamp, counters
            epochs[-1]["samples"] += 1
            for key, value in sampled.items():
                peaks[key] = max(peaks.get(key, 0), value)
            previous_time, previous_values, previous_counters, previous_epoch = timestamp, current, counters, epoch
        final = os.fstat(stream.fileno())
    require(count > 0, "no complete sample available")
    require(final.st_size >= initial.st_size, "sample file shrank during observation")
    identities["samples.jsonl"] = {"prefix_sha256": digest.hexdigest(), "prefix_bytes": initial.st_size,
                                  "complete_rows": count, "incomplete_final_row_bytes_ignored": tail_bytes,
                                  "bytes_at_read_end": final.st_size}
    segments = [finish_segment(item) for item in segments]
    idle = [item for item in segments if item["phase"] == "periodic-idle"]
    span = previous_time - segments[0]["first_sample_unix"]
    return {"count": count, "first_sample_unix": segments[0]["first_sample_unix"],
            "last_sample_unix": previous_time, "maximum_gap_seconds": maximum_gap,
            "sampled_resource_peaks": {key: max(item["maximum"][key] for item in segments)
                                       for key in segments[0]["maximum"]},
            "overall_endpoint_growth_per_hour": {
                key: (segments[-1]["last"][key] - segments[0]["first"][key]) * 3600 / span if span else None
                for key in ("disk_bytes", *("storage." + k for k in STORAGE))},
            "segments": segments, "observed_state_count_decreases": state_decreases,
            "periodic_idle_tails": [{"first_sample_unix": item["first_sample_unix"],
                                      "last_sample_unix": item["last_sample_unix"], "samples": item["samples"],
                                      "sample_span_seconds": item["sample_span_seconds"],
                                      "followed_by_another_phase": item is not segments[-1],
                                      "tail": item["last"],
                                      "rss_delta_from_first_idle": item["last"]["rss_bytes"] - idle[0]["last"]["rss_bytes"],
                                      "goroutine_delta_from_first_idle": item["last"]["pool.goroutines"] - idle[0]["last"]["pool.goroutines"]}
                                     for item in idle],
            "dial": {"epochs": epochs, "sampled_peaks": peaks,
                     "observed_total_increments": {key: sum(epoch["observed_increments"][key] for epoch in epochs)
                                                   for key in epochs[0]["observed_increments"]},
                     "increment_scope": "Only positive deltas between samples in each epoch. First cumulative offsets and restart gaps are excluded, never fabricated. Unattributed resets remain explicit; no complete across-restart count is claimed."}}


def supervisor_live(identity):
    identity = obj(identity)
    pid = number(identity["pid"], True, 2**31 - 1)
    ticks = identity["start_ticks"]
    require(type(ticks) in (str, int) and re.fullmatch("[0-9]{1,24}", str(ticks)), "invalid supervisor identity")
    if not sys.platform.startswith("linux"):
        return None
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return fields[0] != "Z" and fields[19] == str(ticks)
    except (FileNotFoundError, ProcessLookupError):
        return False


def report(root):
    began = time.time()
    identities = {}
    spec, status = (read_json(root, name, identities) for name in ("run.json", "status.json"))
    require(spec["schema"] == 1 and type(spec["schema"]) is int, "unsupported run schema")
    run_id = spec["run_id"]
    require(type(run_id) is str and re.fullmatch("[0-9a-f]{32}", run_id), "invalid run identity")
    require(status["run_id"] == run_id, "status belongs to another run")
    require(status.get("schema") == 1 and type(status.get("schema")) is int, "unsupported status schema")
    for field in ("source_commit", "artifact_source_commit"):
        require(type(spec[field]) is str and re.fullmatch("[0-9a-f]{40}", spec[field]), "invalid source identity")
    clients = number(spec["clients"], True, 20)
    targets = number(spec["targets"], True, 100)
    require(clients > 0 and targets > 0, "empty run topology")
    require(status["status"] in STATUSES, "unrecognized run status")
    events = event_windows(status)
    latency = workers(root, clients, identities)
    sampled = samples(root, events, identities)
    artifacts = frozen_inputs(root, spec)
    live = supervisor_live(status["supervisor"])
    now = time.time()
    started = number(status["started_unix"])
    ended = number(status["ended_unix"]) if "ended_unix" in status else None
    active_status = status["status"] in ("starting", "running", "observing-idle")
    active = live is True and active_status
    elapsed = now - started if active else ended - started if ended is not None else None
    require(elapsed is None or elapsed >= 0, "run time moved backwards")
    bounds = obj(spec["predeclared_bounds"])
    return {"schema": 1, "kind": "read-only-soak-description", "run": str(root), "run_id": run_id,
            "reporter_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "source_commit": spec["source_commit"], "artifact_source_commit": spec["artifact_source_commit"],
            "topology": {"targets": targets, "clients": clients, "physical_machine_count": "unverified"},
            "observed_at_utc": datetime.datetime.fromtimestamp(now, datetime.timezone.utc).isoformat(),
            "snapshot_read_seconds": now - began, "status_snapshot": status["status"],
            "supervisor_identity_live": live, "active": active,
            "effective_status": "interrupted" if active_status and live is False else status["status"],
            "started_unix": started, "ended_unix": ended,
            "recorded_elapsed_seconds": number(status["elapsed_seconds"]),
            "observed_wall_elapsed_seconds": elapsed,
            "latest_sample_age_seconds": now - sampled["last_sample_unix"],
            "frozen_inputs": artifacts, "input_snapshot_identities": identities,
            "client_error_count": sum(w["errors"] for w in latency["workers"]),
            "successful_ping_per_observed_wall_second": sum(w["calls"] for w in latency["workers"]) / elapsed if elapsed else None,
            "latency": latency, "samples": sampled, "recorded_fault_events": events,
            "predeclared_sample_gap": {"maximum_seconds": number(bounds["max_sample_gap_seconds"]),
                                        "observed_within_bound": sampled["maximum_gap_seconds"] <= bounds["max_sample_gap_seconds"]},
            "production_gate": "not-evaluated; this descriptive report cannot certify Phase8",
            "limitations": [
                "run/status/workers/samples are separate asynchronous snapshots; input hashes bind the exact observations, not an atomic checkpoint.",
                "Source commits are declared run identities. Binary buildinfo, signatures and provenance require their independent verification evidence.",
                "Only a fixed prefix of samples.jsonl is streamed; an incomplete final append is ignored and counted. No run file is changed.",
                "Histogram quantiles are bounded intervals, with null upper bound meaning overflow. Sparse warm samples cannot establish full warm coverage.",
                "Per-worker scheduler snapshots are not simultaneous; this report does not reconstruct strict global/host/lane fairness or quota SLO.",
                "Resource extrema are sampled extrema; CPU excludes final ticks of processes exiting between samples. Phase boundary CPU intervals may straddle phases.",
                "Disk counts logical file sizes including evidence/log growth. Endpoint slopes describe observed intervals, not sustained-capacity predictions.",
                "Idle tails may describe an active, unfinished idle window; compare their span with the predeclared duration before using completed-window evidence.",
                "Explicit job_rm is not autonomous retention GC; no credential, key, ledger body, marker or business payload is read.",
                "observe.Registry series count covers that fixed collector only, not all possible metric registries.",
                "Only completed fault events in status are attributed; an ongoing event not yet recorded remains workload/unclassified.",
                "No new SLO threshold, successful24h verdict, platform certification or Production Gate result is inferred."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--out", type=Path, help="new JSON file outside the run; omit for stdout")
    args = parser.parse_args()
    try:
        root = args.run.resolve(strict=True)
        if args.out:
            destination = args.out.absolute()
            require(not destination.resolve().is_relative_to(root), "output must be outside the run")
        data = report(root)
        encoded = json.dumps(data, indent=2, allow_nan=False) + "\n"
        if args.out:
            fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(encoded)
            print(json.dumps({"report": str(destination), "run_id": data["run_id"],
                              "active": data["active"], "production_gate": data["production_gate"]}))
        else:
            print(encoded, end="")
        return 0
    except (Invalid, OSError, KeyError, TypeError, IndexError) as error:
        # Never echo malformed peer values, filesystem payloads or raw JSON.
        reason = str(error) if isinstance(error, Invalid) else "required input unavailable or malformed"
        print("soak-report: " + reason, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
