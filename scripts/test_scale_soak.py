"""Regression tests for evidence honesty and bounded harness supervision."""
import contextlib
import importlib.util
import io
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
