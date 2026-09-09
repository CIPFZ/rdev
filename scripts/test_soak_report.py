"""Offline reporter regressions. Synthetic metadata is never runtime evidence."""
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("soak_report", Path(__file__).with_name("soak-report.py"))
reporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reporter)


def snapshot(at, count, phase=None):
    row = {"time_unix": at, "rss_bytes": 1000, "fds": 8, "processes": 3,
           "disk_bytes": 2000 + count, "storage_records": {key: count for key in reporter.STORAGE},
           "process_kinds": {"ssh_controlmaster": 0},
           "cpu": {"interval_seconds": 5, "managed_observed_cores": .5},
           "pool": {key: 0 for key in ("base_transports", "reserved_hosts", "bulk_transports", "goroutines")}}
    row["pool"].update(observer_metric_series=37, observer_metric_series_bound=37)
    row["pool"]["dial_admission"] = {
        "backoff_waits": 0, "backoff_duration_ns": 0, "queue_waits": count,
        "queue_duration_ns": count * 1000, "queue_buckets": [count, 0, 0, 0, 0, 0, 0],
        "queue_bucket_upper_ns": [1000000, 10000000, 100000000, 1000000000, 10000000000, 30000000000, -1],
        "waiting": 0, "waiting_peak": 1, "states": 2, "state_limit": 1024,
        "global_slots_in_use": 0, "global_slot_limit": 6,
        "connection": {"dial_started": count, "dial_succeeded": count, "dial_failed": 0,
                       "dial_canceled": 0, "dial_duration_ns": count * 2000,
                       "dial_inflight": 0, "dial_failures": {}}}
    if phase:
        row["phase"] = phase
    return row


class ReportTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rdev-report-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "run"
        self.root.mkdir()
        self.put("harness.py", b"synthetic fixture bytes, not a runtime artifact")
        self.run = {"schema": 1, "run_id": "a" * 32, "source_commit": "b" * 40,
                    "artifact_source_commit": "c" * 40, "clients": 1, "targets": 1,
                    "predeclared_bounds": {"max_sample_gap_seconds": 45},
                    "artifacts": {"harness.py": hashlib.sha256((self.root / "harness.py").read_bytes()).hexdigest()}}
        self.status = dict(self.run, status="failed", started_unix=5, ended_unix=50,
                           elapsed_seconds=45, supervisor={"pid": os.getpid(), "start_ticks": "0"}, fault_events=[])
        self.worker = {"pid": os.getpid(), "updated_unix": 40, "calls": 100,
                       "errors": 0, "expected_fault_errors": 0, "cycles": 1, "mutations": 2,
                       "latency_histogram_bounds_ms": [10, 20],
                       "latency_histogram": {"cold_ms": [90, 5, 5], "warm_ms": [0, 0, 0]},
                       "cold_ms": [1], "warm_ms": [], "last_error": "DO_NOT_REPORT_PRIVATE_SENTINEL"}
        self.save()

    def put(self, name, value):
        (self.root / name).write_bytes(value if isinstance(value, bytes) else (json.dumps(value) + "\n").encode())

    def save(self):
        self.put("run.json", self.run)
        self.put("status.json", self.status)
        self.put("worker-0.json", self.worker)
        self.put("samples.jsonl", b"".join((json.dumps(snapshot(at, count)) + "\n").encode()
                                           for at, count in ((10, 10), (20, 12))))

    def test_cumulative_quantile_overflow_and_no_private_input_reads(self):
        self.put("credential-0.json", b"DO_NOT_REPORT_PRIVATE_SENTINEL")
        self.put("principal.key", b"DO_NOT_REPORT_PRIVATE_SENTINEL")
        original, names = reporter.regular, []

        @contextlib.contextmanager
        def inspected(root, name, maximum):
            names.append(name)
            with original(root, name, maximum) as opened:
                yield opened

        with patch.object(reporter, "regular", inspected):
            output = reporter.report(self.root)
        cold = output["latency"]["cumulative"]["cold_ms"]
        self.assertEqual(cold["p95_interval"], {"lower_exclusive_ms": 10, "upper_inclusive_ms": 20})
        self.assertEqual(cold["p99_interval"], {"lower_exclusive_ms": 20, "upper_inclusive_ms": None})
        self.assertIsNone(output["latency"]["cumulative"]["warm_ms"]["p95_interval"])
        self.assertNotIn("DO_NOT_REPORT_PRIVATE_SENTINEL", json.dumps(output))
        self.assertEqual(set(names), {"run.json", "status.json", "worker-0.json", "samples.jsonl", "harness.py"})
        self.assertIn("cannot certify", output["production_gate"])
        self.assertEqual(output["observed_wall_elapsed_seconds"], 45)

    def test_fixed_prefix_ignores_concurrent_append_and_incomplete_final_row(self):
        prefix = (json.dumps(snapshot(10, 1)) + "\n").encode() + b'{"time_unix": 20'
        self.put("samples.jsonl", prefix)
        original = reporter.regular

        @contextlib.contextmanager
        def append_after_open(root, name, maximum):
            with original(root, name, maximum) as opened:
                if name == "samples.jsonl":
                    with (root / name).open("ab") as stream:
                        stream.write(b'}\n' + (json.dumps(snapshot(30, 3)) + "\n").encode())
                yield opened

        identities = {}
        with patch.object(reporter, "regular", append_after_open):
            output = reporter.samples(self.root, [], identities)
        self.assertEqual(output["count"], 1)
        self.assertEqual(identities["samples.jsonl"]["prefix_sha256"], hashlib.sha256(prefix).hexdigest())
        self.assertEqual(identities["samples.jsonl"]["incomplete_final_row_bytes_ignored"], len(b'{"time_unix": 20'))
        self.assertGreater(identities["samples.jsonl"]["bytes_at_read_end"], len(prefix))

    def test_dial_epochs_do_not_treat_reset_or_first_offsets_as_increments(self):
        self.put("samples.jsonl", b"".join((json.dumps(snapshot(at, count)) + "\n").encode()
                                           for at, count in ((10, 10), (20, 3), (30, 5), (40, 2))))
        events = [{"phase": "broker-SIGKILL-after-completed-mutations", "started_unix": 14, "ended_unix": 15}]
        output = reporter.samples(self.root, events, {})["dial"]
        self.assertEqual([x["reason"] for x in output["epochs"]],
                         ["first_observation", "recorded_broker_restart", "unattributed_counter_reset"])
        self.assertEqual(output["observed_total_increments"]["dial_started"], 2)
        self.assertEqual(output["epochs"][1]["first_cumulative"]["dial_started"], 3)
        self.assertEqual(output["epochs"][2]["last_cumulative"]["dial_started"], 2)

    def test_idle_partial_window_and_resource_slopes_keep_observation_scope(self):
        rows = [snapshot(10, 10), snapshot(20, 12, "periodic-idle"), snapshot(30, 14, "periodic-idle")]
        self.put("samples.jsonl", b"".join((json.dumps(row) + "\n").encode() for row in rows))
        output = reporter.samples(self.root, [], {})
        tail = output["periodic_idle_tails"][0]
        self.assertFalse(tail["followed_by_another_phase"])
        self.assertEqual(tail["sample_span_seconds"], 10)
        self.assertEqual(output["segments"][1]["endpoint_growth_per_hour"]["disk_bytes"], 720)

    def test_duplicate_future_boolean_and_nonfinite_inputs_are_rejected(self):
        for bad in (b'{"schema":1,"schema":1}', b'{"x":NaN}', b'{"x":Infinity}'):
            with self.subTest(bad=bad), self.assertRaises(reporter.Invalid):
                reporter.decode(bad)
        self.run["schema"] = 99
        self.save()
        with self.assertRaisesRegex(reporter.Invalid, "schema"):
            reporter.report(self.root)
        self.run["schema"] = 1
        self.worker["calls"] = True
        self.save()
        with self.assertRaisesRegex(reporter.Invalid, "numeric"):
            reporter.report(self.root)
        with self.assertRaisesRegex(reporter.Invalid, "outside bound"):
            reporter.number(10**1000)
        with self.assertRaises(reporter.Invalid):
            reporter.decode(b'{"x":' + b'9' * 5000 + b'}')

    def test_stale_process_identity_never_adds_elapsed_time_or_hides_changed_input(self):
        self.status["status"] = "running"
        del self.status["ended_unix"]
        self.save()
        self.put("harness.py", b"changed fixture input")
        with patch.object(reporter.sys, "platform", "linux"), patch.object(reporter.Path, "read_text", return_value="1 (test) " + " ".join(["S"] + ["0"] * 18 + ["123"])):
            output = reporter.report(self.root)
        self.assertFalse(output["active"])
        self.assertEqual(output["effective_status"], "interrupted")
        self.assertIsNone(output["observed_wall_elapsed_seconds"])
        self.assertFalse(output["frozen_inputs"]["harness.py"]["matches"])

    def test_path_escape_symlink_and_fifo_never_become_report_inputs(self):
        self.run["artifacts"] = {"../principal.key": "0" * 64}
        with self.assertRaisesRegex(reporter.Invalid, "artifact"):
            reporter.frozen_inputs(self.root, self.run)
        (self.root / "worker-0.json").unlink()
        (self.root / "worker-0.json").symlink_to(self.root / "status.json")
        with self.assertRaises(OSError):
            reporter.workers(self.root, 1, {})
        (self.root / "worker-0.json").unlink()
        os.mkfifo(self.root / "worker-0.json", 0o600)
        with self.assertRaisesRegex(reporter.Invalid, "file outside bound"):
            reporter.workers(self.root, 1, {})

    def test_line_count_size_and_time_limits_are_enforced(self):
        with patch.object(reporter, "MAX_SAMPLES", 1), self.assertRaisesRegex(reporter.Invalid, "sample count"):
            reporter.samples(self.root, [], {})
        with patch.object(reporter, "MAX_LINE", 8), self.assertRaisesRegex(reporter.Invalid, "sample row"):
            reporter.samples(self.root, [], {})
        self.put("samples.jsonl", b"".join((json.dumps(snapshot(at, 1)) + "\n").encode() for at in (20, 10)))
        with self.assertRaisesRegex(reporter.Invalid, "timestamps"):
            reporter.samples(self.root, [], {})

    def test_cli_refuses_writing_into_run_and_creates_private_external_report(self):
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        command = [sys.executable, str(Path(reporter.__file__)), "--run", str(self.root), "--out"]
        refused = subprocess.run(command + [str(self.root / "report.json")], capture_output=True, timeout=5)
        self.assertEqual(refused.returncode, 2)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.iterdir()})
        external = Path(self.temporary.name) / "external.json"
        result = subprocess.run(command + [str(external)], capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(external.stat().st_mode & 0o777, 0o600)
        old = external.read_bytes()
        result = subprocess.run(command + [str(external)], capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(external.read_bytes(), old)


if __name__ == "__main__":
    unittest.main()
