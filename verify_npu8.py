"""Short live acceptance test. Run as root on npu8 with the toolkit sourced."""
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time

BIN = "/usr/local/bin/npu-lease"
ACL = """
import ctypes as c
import os
import pathlib
import sys
import time
acl = c.CDLL('libascendcl.so')
assert acl.aclInit(None) == 0
try:
    expected = list(map(int, os.environ['ASCEND_RT_VISIBLE_DEVICES'].split(',')))
    count = c.c_uint32()
    assert acl.aclrtGetDeviceCount(c.byref(count)) == 0
    assert count.value == len(expected), count.value
    actual = []
    for index in range(count.value):
        logic = c.c_int32()
        assert acl.aclrtGetLogicDevIdByUserDevId(index, c.byref(logic)) == 0
        actual.append(logic.value)
    assert actual == expected, (actual, expected)
    if len(sys.argv) > 1:
        result = acl.aclrtSetDevice(0)
        assert result == 0, result
        pathlib.Path(sys.argv[1]).touch()
        try:
            stop = pathlib.Path(sys.argv[1] + '.stop')
            deadline = time.monotonic() + 40
            while not stop.exists() and time.monotonic() < deadline:
                time.sleep(0.1)
        finally:
            assert acl.aclrtResetDevice(0) == 0
    print(f'ACL count={count.value} host logical IDs={actual}', flush=True)
finally:
    assert acl.aclFinalize() == 0
"""


def until(check, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.1)
    raise AssertionError("condition did not become true")


with tempfile.TemporaryDirectory(prefix="npu-lease-") as directory:
    sock = pathlib.Path(directory) / "control.sock"
    cli = [BIN, "--socket", str(sock)]
    daemon = subprocess.Popen([BIN, "deamon", str(sock)])
    children = []
    try:
        until(sock.is_socket)

        def run(*args, **kwargs):
            return subprocess.run(cli + list(args), timeout=15, text=True, **kwargs)

        def status():
            return json.loads(run("status", capture_output=True, check=True).stdout)

        idle = [int(device) for device, row in status().items() if row["state"] == "idle"]
        assert len(idle) >= 3, idle
        first, second = idle[0], idle[-1]
        assert second - first > 1
        run("run", "--ids", f"{second},{first}", "--", sys.executable, "-c", ACL, check=True)
        subprocess.run([BIN, "run", "--socket", str(sock), "--count", "2", "--",
                        sys.executable, "-c", ACL], check=True, timeout=15)
        print("PASS explicit and count allocation with real ACL visibility", flush=True)

        holder = subprocess.Popen(cli + ["run", "--ids", str(first), "--", "sleep", "25"])
        children.append(holder)
        until(lambda: status()[str(first)]["state"] == "leased")
        denied = run("run", "--ids", f"{first},{second}", "--", "true", capture_output=True)
        assert denied.returncode == 1 and "unavailable" in denied.stderr, denied
        assert status()[str(second)]["state"] == "idle"
        waiter = subprocess.Popen(cli + ["run", "--ids", str(first), "--wait", "--", "true"])
        children.append(waiter)
        time.sleep(1.2)
        assert waiter.poll() is None
        holder.send_signal(signal.SIGTERM)
        assert holder.wait(timeout=5) == -signal.SIGTERM
        assert waiter.wait(timeout=8) == 0
        until(lambda: status()[str(first)]["state"] == "idle")
        assert run("run", "--ids", str(first), "--", "sh", "-c", "exit 37").returncode == 37
        print("PASS atomic conflict, wait, SIGTERM release, child exit status", flush=True)

        shell = 'sleep 3 </dev/null >/dev/null 2>&1 &'
        run("run", "--ids", str(first), "--", "sh", "-c", shell, check=True)
        until(lambda: status()[str(first)]["state"] == "idle")
        print("PASS an outliving background process does not retain the lease", flush=True)

        first_ready = pathlib.Path(directory) / "first-ready"
        second_ready = pathlib.Path(directory) / "second-ready"
        holder = subprocess.Popen(cli + ["run", "--ids", str(first), "--wait", "--", "sleep", "30"])
        children.append(holder)
        until(lambda: status()[str(first)]["state"] == "leased")
        older = subprocess.Popen(cli + [
            "run", "--ids", str(first), "--wait", "--",
            "sh", "-c", 'touch "$1"; exec sleep 30', "sh", str(first_ready),
        ])
        children.append(older)
        time.sleep(0.5)
        newer = subprocess.Popen(cli + [
            "run", "--ids", str(first), "--wait", "--", "touch", str(second_ready),
        ])
        children.append(newer)
        run("run", "--ids", str(second), "--wait", "--", "true", check=True)
        holder.terminate()
        assert holder.wait(timeout=5) == -signal.SIGTERM
        until(first_ready.is_file)
        assert newer.poll() is None and not second_ready.exists()
        older.terminate()
        assert older.wait(timeout=5) == -signal.SIGTERM
        assert newer.wait(timeout=8) == 0 and second_ready.exists()
        print("PASS queue preserves order for competing requests and allows disjoint grants", flush=True)

        def verify_recovery(kill_daemon):
            label = "daemon" if kill_daemon else "client"
            ready = pathlib.Path(directory) / f"{label}-ready"
            stop = pathlib.Path(directory) / f"{label}-stop"
            command = (
                "import os, pathlib, sys, time;"
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()));"
                "stop=pathlib.Path(sys.argv[2]);"
                "end=time.monotonic()+45;"
                "exec('while not stop.exists() and time.monotonic() < end: time.sleep(0.05)')"
            )
            client = subprocess.Popen(cli + [
                "run", "--ids", str(first), "--wait", "--",
                sys.executable, "-c", command, str(ready), str(stop),
            ])
            children.append(client)
            replacement = None
            try:
                until(lambda: ready.is_file() and ready.read_text().isdigit())
                workload = int(ready.read_text())
                assert status()[str(first)]["pid"] == workload
                if kill_daemon:
                    daemon.kill()
                    assert daemon.wait(timeout=5) == -signal.SIGKILL
                    assert sock.is_socket(), "expected stale socket after SIGKILL"
                    replacement = subprocess.Popen(cli + ["daemon"])
                    def recovered():
                        return run("status", capture_output=True).returncode == 0
                    until(recovered)
                else:
                    client.kill()
                    assert client.wait(timeout=5) == -signal.SIGKILL
                assert status()[str(first)]["pid"] == workload
                denied = run("run", "--ids", str(first), "--", "true", capture_output=True)
                assert denied.returncode == 1 and "unavailable" in denied.stderr
                stop.touch()
                if kill_daemon:
                    assert client.wait(timeout=5) == 0
                until(lambda: status()[str(first)]["state"] == "idle")
                run("run", "--ids", str(first), "--", "true", check=True)
                print(f"PASS {label} SIGKILL retains lease until workload exit", flush=True)
                return replacement
            except BaseException:
                if replacement is not None:
                    replacement.terminate()
                    replacement.wait(timeout=8)
                raise
            finally:
                stop.touch()

        verify_recovery(kill_daemon=False)
        daemon = verify_recovery(kill_daemon=True)

        marker = pathlib.Path(directory) / "external-ready"
        external = subprocess.Popen(
            [sys.executable, "-c", ACL, str(marker)],
            env=dict(os.environ, ASCEND_RT_VISIBLE_DEVICES=str(second)),
        )
        children.append(external)
        until(lambda: marker.is_file() or external.poll() is not None, timeout=60)
        assert marker.is_file(), external.returncode
        row = status()[str(second)]
        assert row["state"] == "busy", row
        blocked = run("run", "--ids", str(second), "--", "true", capture_output=True)
        assert blocked.returncode == 1 and external.poll() is None, blocked
        pathlib.Path(str(marker) + '.stop').touch()
        assert external.wait(timeout=45) == 0
        until(lambda: status()[str(second)]["state"] == "idle")
        print(f"PASS external ACL workload skipped ({row['state']}) and left running", flush=True)
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
        if daemon.poll() is None:
            daemon.terminate()
            assert daemon.wait(timeout=8) == 0
        assert not sock.exists()
        print("PASS test daemon stopped and socket removed", flush=True)
