import io
import os
from pathlib import Path
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
import unittest

from npu_lease.client import run_workload


ROOT = Path(__file__).resolve().parents[1]


class ClientTests(unittest.TestCase):
    def test_workload_starts_only_after_registration_ack(self):
        with TemporaryDirectory() as directory:
            marker = Path(directory) / "started"
            command = [sys.executable, "-c", "from pathlib import Path; import sys; Path(sys.argv[1]).touch()",
                       str(marker)]
            for response in (b'{"error":"denied"}\n', b""):
                with self.subTest(response=response):
                    left, right = socket.socketpair()
                    with left, right, self.assertRaises(ValueError):
                        run_workload(command, os.environ, left, io.BytesIO(response))
                    self.assertFalse(marker.exists())
            left, right = socket.socketpair()
            with left, right:
                self.assertEqual(run_workload(command, os.environ, left, io.BytesIO(b'{"bound":true}\n')), 0)
            self.assertTrue(marker.is_file())

    def test_python_startup_hooks_run_only_in_the_acknowledged_workload(self):
        with TemporaryDirectory() as directory:
            marker = Path(directory) / "hook-ran"
            hook = Path(directory) / "sitecustomize.py"
            hook.write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
            env = dict(os.environ, PYTHONPATH=directory)
            argv = [sys.executable, "-c", "pass"]
            left, right = socket.socketpair()
            with left, right, self.assertRaises(ValueError):
                run_workload(argv, env, left, io.BytesIO(b'{"error":"denied"}\n'))
            self.assertFalse(marker.exists())
            left, right = socket.socketpair()
            with left, right:
                self.assertEqual(run_workload(argv, env, left, io.BytesIO(b'{"bound":true}\n')), 0)
            self.assertTrue(marker.is_file())

    def test_wait_timeout_is_enforced_when_old_daemon_ignores_it(self):
        with TemporaryDirectory() as directory, socket.socket(socket.AF_UNIX) as listener:
            path = Path(directory) / "old-daemon.sock"
            listener.bind(str(path))
            listener.listen()
            listener.settimeout(3)
            received = []
            disconnected = threading.Event()

            def old_daemon():
                try:
                    conn, _ = listener.accept()
                    with conn, conn.makefile("rb") as stream:
                        conn.settimeout(3)
                        received.append(stream.readline())
                        if stream.read(1) == b"":
                            disconnected.set()
                except OSError:
                    return

            peer = threading.Thread(target=old_daemon, daemon=True)
            peer.start()
            try:
                result = subprocess.run(
                    [str(ROOT / "npu-lease"), "--socket", str(path), "run", "--ids", "0",
                     "--wait-timeout", "0.1", "--", "true"],
                    timeout=2, capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 1)
                self.assertIn("lease wait timed out", result.stderr)
                self.assertTrue(disconnected.wait(1))
            finally:
                peer.join(3)
            self.assertIn(b'"wait": true', received[0])

    def test_installed_archive_runs_outside_checkout_and_propagates_errors(self):
        with TemporaryDirectory() as directory:
            path = Path(directory)
            subprocess.run([str(ROOT / "install.sh"), str(path)], cwd=path,
                           check=True, capture_output=True)
            binary = path / "bin/npu-lease"
            result = subprocess.run([str(binary), "--help"], cwd=path, check=True,
                                    capture_output=True, text=True)
            self.assertIn("drain", result.stdout)
            result = subprocess.run([str(binary), "--socket", str(path / "absent"), "status"],
                                    cwd=path, capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("No such file", result.stderr)
