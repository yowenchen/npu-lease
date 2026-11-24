"""Request leases and launch registered subprocesses."""

import json
import os
import signal
import socket
import subprocess
import sys
import time

from .leases import TIMEOUT_ERROR

RESPONSE_LIMIT = 65536


def request_server(args):
    request = {"op": args.action}
    timeout = None
    if args.action == "run":
        timeout = args.wait_timeout
        request.update(ids=args.ids, num=args.num, wait=args.wait, wait_timeout=timeout)
    if args.action == "status" and args.queue:
        request["queue"] = True
    message = json.dumps(request).encode() + b"\n"
    with socket.socket(socket.AF_UNIX) as conn:
        response = None
        try:
            deadline = None if timeout is None else time.monotonic() + timeout
            if deadline is not None:
                conn.settimeout(timeout)
            conn.connect(args.socket)
            if deadline is not None:
                set_remaining_timeout(conn, deadline)
            conn.sendall(message)
            if deadline is not None:
                response = receive_by_deadline(conn, deadline)
                conn.settimeout(None)
        except TimeoutError as exc:
            raise ValueError(TIMEOUT_ERROR) from exc
        with conn.makefile("rb") as stream:
            if deadline is None:
                response = json.loads(stream.readline(RESPONSE_LIMIT))
            return handle_response(args, conn, stream, response)


def set_remaining_timeout(conn, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    conn.settimeout(remaining)


def receive_by_deadline(conn, deadline):
    # The only message before registration is the initial lease decision.
    data = bytearray()
    while len(data) < RESPONSE_LIMIT:
        set_remaining_timeout(conn, deadline)
        chunk = conn.recv(min(4096, RESPONSE_LIMIT - len(data)))
        if not chunk:
            break
        line, separator, _ = chunk.partition(b"\n")
        data.extend(line)
        if separator:
            if time.monotonic() >= deadline:
                raise TimeoutError
            return json.loads(data)
    raise ValueError("incomplete or oversized lease response")


def handle_response(args, conn, stream, response):
    if not isinstance(response, dict):
        raise ValueError("invalid daemon response")
    if "error" in response:
        print(f"npu-lease: {response['error']}", file=sys.stderr)
        return 1
    if args.action != "run":
        if args.action == "status" and not args.queue:
            response = response["devices"]
        print(json.dumps(response, indent=2))
        return 0
    env = dict(os.environ, ASCEND_RT_VISIBLE_DEVICES=",".join(map(str, sorted(response["ids"]))))
    return run_workload(args.command, env, conn, stream)


GATED_EXEC = """
import os
import sys
fd = int(sys.argv[1])
permit = os.read(fd, 1)
os.close(fd)
if permit != b"1":
    sys.exit(1)
try:
    os.execvpe(sys.argv[2], sys.argv[2:], os.environ)
except OSError as exc:
    print(f"npu-lease: {exc}", file=sys.stderr)
    sys.exit(127)
"""


def run_workload(command, env, conn, stream):
    child = None
    queued_signals = []

    def forward(signum, _frame):
        if child is None:
            queued_signals.append(signum)
        elif child.returncode is None:
            child.send_signal(signum)

    signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    previous = {signum: signal.signal(signum, forward) for signum in signals}
    try:
        gate_read, gate_write = os.pipe()
        try:
            child = subprocess.Popen(
                [sys.executable, "-I", "-S", "-c", GATED_EXEC, str(gate_read), *command],
                env=env, pass_fds=(gate_read,),
            )
        finally:
            os.close(gate_read)
            if child is None:
                os.close(gate_write)
        try:
            for signum in queued_signals:
                child.send_signal(signum)
            conn.sendall(json.dumps({"pid": child.pid}).encode() + b"\n")
            registration = json.loads(stream.readline(RESPONSE_LIMIT))
            if registration.get("bound") is not True:
                raise ValueError(registration.get("error", "lease registration failed"))
            os.write(gate_write, b"1")
        finally:
            os.close(gate_write)
            child.wait()
        return child.returncode
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
