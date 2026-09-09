"""Actual process timeout regression; does not claim SSH/runtime acceptance."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

spec = importlib.util.spec_from_file_location("isolated_ssh", Path(__file__).with_name("isolated-ssh.py"))
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


class TimeoutCleanup(unittest.TestCase):
    def test_timeout_kills_descendant_after_parent_exits(self):
        with tempfile.TemporaryDirectory(prefix="rdev-exited-parent-", dir="/tmp") as directory:
            root = Path(directory)
            program = root / "parent.py"
            # Child inherits stdout, installs a TERM handler, and acknowledges
            # readiness before its parent exits. communicate still times out.
            program.write_text("import pathlib,subprocess,sys,time\np=subprocess.Popen([sys.executable,'-c',\"import pathlib,signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); pathlib.Path(sys.argv[1]).write_text('ready'); time.sleep(30)\",sys.argv[2]])\npathlib.Path(sys.argv[1]).write_text(str(p.pid))\nwhile not pathlib.Path(sys.argv[2]).exists(): time.sleep(.01)\n")
            pidfile = root / "child.pid"
            with self.assertRaises(subprocess.TimeoutExpired):
                harness.run([sys.executable, program, pidfile, root / "ready"], timeout=.5, capture_output=True)
            self.assertTrue(pidfile.exists())
            self.assertIsNone(harness.process_identity(int(pidfile.read_text())), "TERM-resistant orphan survived timeout")

    def test_timeout_terminates_parent_and_real_descendant(self):
        with tempfile.TemporaryDirectory(prefix="rdev-timeout-tree-", dir="/tmp") as directory:
            root = Path(directory)
            program = root / "parent.py"
            program.write_text("import pathlib,subprocess,sys,time\np=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])\npathlib.Path(sys.argv[1]).write_text(str(p.pid))\ntime.sleep(30)\n")
            pidfile = root / "child.pid"
            with self.assertRaises(subprocess.TimeoutExpired):
                harness.run([sys.executable, program, pidfile], timeout=.5, capture_output=True)
            self.assertTrue(pidfile.exists(), "test must actually start a descendant")
            pid = int(pidfile.read_text())
            deadline = time.monotonic() + 5
            while harness.process_identity(pid) and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertIsNone(harness.process_identity(pid), "timed-out subprocess descendant survived")


if __name__ == "__main__":
    unittest.main()
