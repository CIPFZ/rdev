"""Regression tests for evidence honesty and bounded harness supervision."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("scale_soak", Path(__file__).with_name("scale-soak.py"))
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


class SupervisorContracts(unittest.TestCase):
    def test_day_budget_counts_all_periodic_logger_start_and_remove_records(self):
        budget = harness.workload_budget(86400, 20, 100)
        self.assertEqual(budget["cycles_per_client_max"], 72)
        self.assertEqual(budget["logger_generations_per_client_max"], 36)
        self.assertEqual(budget["ordinary_mutations_max"], 5040)
        self.assertEqual(budget["logger_mutations_max"], 1440)
        self.assertEqual(budget["fleet_mutations_max"], 600)
        self.assertEqual(budget["total_mutations_max"], 7080)
        self.assertEqual(budget["ordinary_owner_mutations_max"], 324)
        self.assertEqual(budget["admin_owner_mutations_max"], 600)
        self.assertEqual(harness.workload_budget(300, 20, 100)["total_mutations_max"], 220)
        self.assertEqual(harness.logger_duration(86400, 0, 2400), 2350)
        self.assertEqual(harness.logger_duration(300, 20, 2400), 235)
        with self.assertRaisesRegex(RuntimeError, "remaining run time"):
            harness.logger_duration(300, 248, 2400)
        with self.assertRaisesRegex(RuntimeError, "generation interval"):
            harness.logger_duration(86400, 7530, 52)

    def test_mutation_budget_is_rejected_before_creating_a_run(self):
        argv = ["scale-soak.py", "start", "--run", "/tmp/rdev-budget-must-not-create", "--artifacts", "/unused", "--seconds", "86400"]
        for budget in ({"total_mutations_max": 8193, "ordinary_owner_mutations_max": 324, "admin_owner_mutations_max": 600},
                       {"total_mutations_max": 7080, "ordinary_owner_mutations_max": 1025, "admin_owner_mutations_max": 600}):
            with self.subTest(budget=budget), mock.patch.object(sys, "argv", argv), mock.patch.object(harness, "workload_budget", return_value=budget), mock.patch.object(Path, "mkdir") as mkdir, contextlib.redirect_stderr(io.StringIO()) as error:
                with self.assertRaises(SystemExit) as rejected:
                    harness.main()
                self.assertEqual(rejected.exception.code, 2)
                self.assertIn("existing global/owner mutation retention limits", error.getvalue())
                mkdir.assert_not_called()

    def logger_fixture(self, root):
        marker = root / "logger-0-0"
        marker.write_bytes(b"x")
        record = {"id": "job-old", "host": "target-000", "marker": str(marker), "generation": 0,
                  "start_operation_id": "old-start", "remove_operation_id": "old-remove", "removed": False}
        stats = {"logger": record, "logger_history": [record], "mutations": 1, "operations": {"job_start": 1},
                 "marker_proofs": 0, "log_rotation_proofs": 0}
        # Worker recovery/finalization reloads JSON; these are distinct dicts.
        stats = json.loads(json.dumps(stats))
        ledger = {"original_bytes": 8192, "retained_bytes": 4096, "dropped_bytes": 4096}
        return stats, {"state": "exited", "stdout_ledger": ledger}

    def test_logger_rotation_retains_identities_and_uses_bounded_product_envelope(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            root = Path(directory)
            harness.write(root / "start-barrier", {"started_monotonic": 100})
            stats, info = self.logger_fixture(root)
            run = {"seconds": 86400, "mutation_budget": harness.workload_budget(86400, 20, 100)}
            with mock.patch.object(harness, "checked", return_value={"wire": {"job": {"info": info}}}), mock.patch.object(harness, "approved_wire", side_effect=[{}, {"wire": {"job": {"info": {"id": "job-new"}}}}]) as mutate, mock.patch.object(harness.time, "monotonic", return_value=2500):
                harness.start_logger(root, run, None, {"client_id": "owner"}, "target-001", 0, 2, root, "new-", stats, scheduled_cycle_monotonic=2500)
            self.assertEqual([call.args[4]["op"] for call in mutate.call_args_list], ["job_rm", "job_start"])
            self.assertEqual(mutate.call_args_list[0].args[5], "old-remove")
            envelope = mutate.call_args_list[1].args[4]["job"]
            self.assertEqual(envelope["resources"]["wall_timeout_sec"], 3500)
            self.assertLessEqual(envelope["resources"]["wall_timeout_sec"], 3600)
            self.assertEqual(envelope["spec"]["argv"][-1], "2350")
            saved = json.loads((root / "worker-0.json").read_text())
            self.assertEqual(len(saved["logger_history"]), 2)
            old, new = saved["logger_history"]
            self.assertTrue(old["removed"])
            self.assertEqual(old["start_operation_id"], "old-start")
            self.assertEqual(old["remove_operation_id"], "old-remove")
            self.assertEqual(old["ledger"], info["stdout_ledger"])
            self.assertEqual(Path(old["marker"]).read_bytes(), b"x")
            self.assertFalse(new["removed"])
            self.assertEqual(new["generation"], 1)
            self.assertEqual(new["id"], "job-new")
            self.assertEqual(saved["mutations"], 3)
            self.assertEqual(saved["marker_proofs"], 1)
            self.assertEqual(saved["log_rotation_proofs"], 1)

    def test_maintenance_delayed_logger_exits_before_original_next_generation(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            root = Path(directory)
            harness.write(root / "start-barrier", {"started_monotonic": 0})
            run = {"seconds": 86400, "mutation_budget": harness.workload_budget(86400, 20, 100)}
            stats = {"logger": None, "logger_history": [], "mutations": 0, "operations": {}, "marker_proofs": 0, "log_rotation_proofs": 0}
            clock = {"now": 7530}  # cycle6 scheduled7210; hourly maintenance delayed admission.
            deadlines, operations = {}, []
            def mutation(root, sock, owner, host, wire, operation_id):
                operations.append(wire["op"])
                if wire["op"] == "job_start":
                    argv = wire["job"]["spec"]["argv"]
                    Path(argv[3]).write_bytes(b"x")
                    job_id = "job-" + operation_id
                    deadlines[job_id] = clock["now"] + float(argv[4])
                    return {"wire": {"job": {"info": {"id": job_id}}}}
                return {}
            def status(sock, request):
                job_id = request["wire"]["job"]["id"]
                state = "exited" if clock["now"] >= deadlines[job_id] else "running"
                return {"wire": {"job": {"info": {"state": state, "stdout_ledger": {"original_bytes": 8192, "retained_bytes": 4096, "dropped_bytes": 4096}}}}}
            with mock.patch.object(harness.time, "monotonic", side_effect=lambda: clock["now"]), mock.patch.object(harness, "approved_wire", side_effect=mutation), mock.patch.object(harness, "checked", side_effect=status):
                harness.start_logger(root, run, None, {}, "target-000", 0, 6, root, "generation3-", stats, scheduled_cycle_monotonic=7210)
                first = dict(stats["logger"])
                self.assertEqual(first["duration_seconds"], 2035)
                self.assertEqual(first["next_generation_monotonic"], 9610)
                self.assertLess(deadlines[first["id"]], 9610)
                # Reload like worker recovery: elapsed maintenance never rebases the schedule.
                stats = json.loads((root / "worker-0.json").read_text())
                clock["now"] = 9610
                harness.start_logger(root, run, None, {}, "target-001", 0, 8, root, "generation4-", stats, scheduled_cycle_monotonic=9610)
            self.assertEqual(operations, ["job_start", "job_rm", "job_start"])
            self.assertTrue(stats["logger_history"][0]["removed"])
            self.assertEqual(stats["logger"]["scheduled_cycle_monotonic"], 9610)
            self.assertEqual(stats["logger"]["duration_seconds"], 2350)
            self.assertEqual(stats["mutations"], 3)

    def test_unfinished_failed_or_ambiguous_logger_never_starts_another_generation(self):
        for condition in ("running", "failed-exit", "duplicate-history", "ambiguous-removal"):
            with self.subTest(condition=condition), tempfile.TemporaryDirectory(dir="/tmp") as directory:
                root = Path(directory)
                stats, info = self.logger_fixture(root)
                if condition == "running":
                    info["state"] = "running"
                elif condition == "failed-exit":
                    info["exit_code"] = 137
                elif condition == "duplicate-history":
                    stats["logger_history"].append(dict(stats["logger"]))
                run = {"seconds": 86400, "mutation_budget": harness.workload_budget(86400, 20, 100)}
                with mock.patch.object(harness, "checked", return_value={"wire": {"job": {"info": info}}}), mock.patch.object(harness, "approved_wire", side_effect=RuntimeError("transport.ambiguous_outcome")) as mutate:
                    with self.assertRaises(RuntimeError):
                        harness.start_logger(root, run, None, {}, "target-001", 0, 2, root, "new-", stats, scheduled_cycle_monotonic=2500)
                self.assertEqual(mutate.call_count, 1 if condition == "ambiguous-removal" else 0)
                self.assertFalse(stats["logger_history"][0]["removed"])
                self.assertEqual(stats["mutations"], 1)
                self.assertFalse((root / "worker-0.json").exists())

    def test_credentials_renew_same_owner_with_product_max_ttl_from_issue_time(self):
        owner = {"client_id": "scale-0", "project_id": "project-0"}
        with mock.patch.object(harness.time, "monotonic", side_effect=[100, 43300]), mock.patch.object(harness.time, "time", side_effect=[1000, 44200]), mock.patch.object(harness, "command", return_value=mock.Mock(stdout=b"private-test-token\n")) as issue:
            first = harness.issue_credential(Path("/private/run"), owner)
            second = harness.issue_credential(Path("/private/run"), first["owner"])
        self.assertEqual(first["refresh_monotonic"], 43300)
        self.assertEqual(second["refresh_monotonic"], 86500)
        self.assertEqual(first["owner"], second["owner"])
        for call in issue.call_args_list:
            argv = call.args[0]
            self.assertEqual(argv[argv.index("-ttl") + 1], "24h")
            self.assertEqual(argv[argv.index("-client-id") + 1], owner["client_id"])
            self.assertEqual(argv[argv.index("-project-id") + 1], owner["project_id"])

    def test_quota_diagnostic_preserves_state_without_peer_payload(self):
        response = {"ok": False, "error": "broker ingress limit reached", "mutation": {"state": "not_sent"}}
        with mock.patch.object(harness, "rpc", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "broker-ingress-limit; state=not_sent"):
                harness.checked(None, {"operation": "sync.push"})
        response["error"] = "private-peer-payload"
        with mock.patch.object(harness, "rpc", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "broker-denied; state=not_sent") as caught:
                harness.checked(None, {"operation": "sync.push"})
            self.assertNotIn("private-peer", str(caught.exception))
        response.update(error_envelope={"code": "private-peer-code"}, mutation={"state": "private-peer-state"})
        with mock.patch.object(harness, "rpc", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "broker-denied; state=unknown") as caught:
                harness.checked(None, {"operation": "sync.push"})
            self.assertNotIn("private-peer", str(caught.exception))

    def test_large_response_admission_is_cross_process_and_crash_released(self):
        import fcntl
        import select
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            program = "import importlib.util,sys,time;from pathlib import Path;s=importlib.util.spec_from_file_location('h',sys.argv[1]);h=importlib.util.module_from_spec(s);s.loader.exec_module(h)\nwith h.large_response_admission(Path(sys.argv[2]),{}):\n print('held',flush=True);time.sleep(30)\n"
            child = subprocess.Popen([sys.executable, "-c", program, str(Path(harness.__file__)), directory], stdout=subprocess.PIPE, text=True)
            try:
                self.assertTrue(select.select([child.stdout], [], [], 3)[0])
                self.assertEqual(child.stdout.readline(), "held\n")
                with (Path(directory) / "large-response.lock").open("a+b") as lock:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                child.kill()
                child.wait(timeout=3)
                stats = {}
                with harness.large_response_admission(Path(directory), stats):
                    self.assertLess(stats["workload_admission_wait_seconds"], 1)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=3)
                child.stdout.close()

    def test_cpu_sampling_separates_shared_host_and_pid_identity(self):
        import os
        hz = os.sysconf("SC_CLK_TCK")
        _, before = harness.cpu_interval({"123:old": 5 * hz}, (100 * hz, 20 * hz), 10, None)
        interval, _ = harness.cpu_interval({"123:new": hz}, (116 * hz, 27 * hz), 12, before)
        self.assertEqual(interval["managed_observed_cores"], .5)
        self.assertEqual(interval["host_busy_cores"], 4.5)
        self.assertEqual(interval["host_total_cores"], 8)
        with self.assertRaisesRegex(RuntimeError, "12-core"):
            harness.cpu_interval({"123:new": 25 * hz}, (116 * hz, 27 * hz), 12, before)

    def test_persisted_fault_identity_can_resume_only_the_owned_process(self):
        import os
        import signal
        import time
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
        try:
            record = {"pid": child.pid, "start_ticks": harness.identity(child.pid)}
            os.kill(child.pid, signal.SIGSTOP)
            deadline = time.monotonic() + 3
            while Path(f"/proc/{child.pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "T" and time.monotonic() < deadline:
                time.sleep(.01)
            harness.resume_records([dict(record, start_ticks="wrong")])
            self.assertEqual(Path(f"/proc/{child.pid}/stat").read_text().rsplit(")", 1)[1].split()[0], "T")
            harness.resume_records([record])
            deadline = time.monotonic() + 3
            while Path(f"/proc/{child.pid}/stat").read_text().rsplit(")", 1)[1].split()[0] == "T" and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertNotEqual(Path(f"/proc/{child.pid}/stat").read_text().rsplit(")", 1)[1].split()[0], "T")
        finally:
            os.kill(child.pid, signal.SIGCONT)
            child.terminate()
            child.wait(timeout=5)

    def test_ssh_fault_tree_preserves_detached_supervisor_subtree(self):
        with tempfile.TemporaryDirectory(prefix="rdev-fault-tree-", dir="/tmp") as directory:
            root = Path(directory)
            program = root / "parent.py"
            program.write_text("import pathlib,subprocess,sys,time\np=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)','-supervise'])\npathlib.Path(sys.argv[1]).write_text(str(p.pid))\ntime.sleep(30)\n")
            pidfile = root / "child.pid"
            parent = subprocess.Popen([sys.executable, program, pidfile], start_new_session=True)
            try:
                import time
                deadline = time.monotonic() + 5
                while not pidfile.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                child_pid = int(pidfile.read_text())
                selected = [record["pid"] for record in harness.process_tree([parent.pid])]
                self.assertIn(parent.pid, selected)
                self.assertNotIn(child_pid, selected)
            finally:
                import os
                import signal
                os.killpg(parent.pid, signal.SIGTERM)
                parent.wait(timeout=5)

    def test_idle_bounds_reject_retained_transports_and_cumulative_growth(self):
        baseline = {"rss_bytes": 100, "pool": {"base_transports": 0, "reserved_hosts": 0, "goroutines": 20}, "process_kinds": {}}
        sample = {"rss_bytes": 101, "pool": dict(baseline["pool"], base_transports=1), "process_kinds": {}}
        with self.assertRaisesRegex(RuntimeError, "transports"):
            harness.check_idle([sample], baseline, full=True)
        sample["pool"]["base_transports"] = 0
        sample["process_kinds"]["ssh_controlmaster"] = 1
        with self.assertRaisesRegex(RuntimeError, "ControlMaster"):
            harness.check_idle([sample], baseline, full=True)
        sample["process_kinds"] = {}
        sample["rss_bytes"] = baseline["rss_bytes"] + harness.BOUNDS["idle_rss_growth_bytes"] + 1
        with self.assertRaisesRegex(RuntimeError, "RSS"):
            harness.check_idle([sample], baseline, full=True)
        sample["rss_bytes"] = 101
        sample["pool"]["goroutines"] = baseline["pool"]["goroutines"] + harness.BOUNDS["idle_goroutine_growth"] + 1
        with self.assertRaisesRegex(RuntimeError, "goroutine"):
            harness.check_idle([sample], baseline, full=True)

    def test_dirty_opt_in_cannot_bypass_long_run_identity_on_clean_git(self):
        argv = ["scale-soak.py", "start", "--run", "/tmp/rdev-test-must-not-create", "--artifacts", "/unused", "--seconds", "86400", "--allow-dirty-smoke"]
        with mock.patch.object(sys, "argv", argv), mock.patch.object(subprocess, "check_output", side_effect=[b"e" * 40, b""]), contextlib.redirect_stderr(io.StringIO()) as error:
            with self.assertRaises(SystemExit) as rejected:
                harness.main()
        self.assertEqual(rejected.exception.code, 2)
        self.assertIn("always limited to 600 seconds", error.getvalue())

    def test_record_deadline_rejects_continuous_dribble(self):
        left, right = socket.socketpair()
        ended = threading.Event()
        def peer():
            try:
                right.recv(4096)
                for byte in b'{"ok":true}\n':
                    right.sendall(bytes([byte]))
                    if ended.wait(.03):
                        return
            except OSError:
                pass
        thread = threading.Thread(target=peer)
        thread.start()
        left.settimeout(.12)
        try:
            with self.assertRaises((TimeoutError, socket.timeout)):
                harness.rpc(left, {"operation": "ping"})
        finally:
            ended.set()
            left.close()
            right.close()
            thread.join(timeout=1)

    def test_owned_detached_process_is_visible_until_exit(self):
        with tempfile.TemporaryDirectory(prefix="rdev-owner-test-", dir="/tmp") as directory:
            root = Path(directory)
            program = root / "owned.py"
            program.write_text("import time; time.sleep(30)\n")
            child = subprocess.Popen([sys.executable, program], start_new_session=True)
            try:
                record = {"pid": child.pid, "start_ticks": harness.identity(child.pid)}
                self.assertTrue(harness.live(record))
                self.assertIn(child.pid, [p["pid"] for p in harness.owned_processes(root, root / "namespace")])
                self.assertFalse(harness.live(dict(record, start_ticks="wrong-identity")))
            finally:
                child.terminate()
                child.wait(timeout=5)
            self.assertFalse(harness.live(record))
            self.assertNotIn(child.pid, [p["pid"] for p in harness.owned_processes(root, root / "namespace")])


if __name__ == "__main__":
    unittest.main()
