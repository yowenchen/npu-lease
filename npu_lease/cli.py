"""Command-line entry point."""

import argparse
import asyncio
import os
from pathlib import Path
import signal
import subprocess
import sys

from .client import request_server
from .leases import WAIT_TIMEOUT_ERROR, valid_wait_timeout
from .server import daemon

DEFAULT_SOCKET = "/run/npu-lease/control.sock"


def main():
    parser = argparse.ArgumentParser(description="Coordinate access to Ascend devices via a local Unix socket.")
    parser.add_argument("--socket", default=os.getenv("NPU_LEASE_SOCKET", DEFAULT_SOCKET))
    commands = parser.add_subparsers(dest="action", required=True)
    start = commands.add_parser("daemon", aliases=["deamon"])
    start.add_argument("socket_path", nargs="?")
    start.add_argument("--group", help="allow this Unix group to connect")
    status = commands.add_parser("status")
    status.add_argument("--queue", action="store_true", help="include queue and maintenance state")
    drain = commands.add_parser("drain", help="pause new grants and reject queued requests")
    resume = commands.add_parser("resume", help="resume granting leases")
    run = commands.add_parser("run")
    selection = run.add_mutually_exclusive_group(required=True)
    selection.add_argument("--ids", type=lambda raw: [int(device) for device in raw.split(",")])
    selection.add_argument("--num", "--count", type=int)
    run.add_argument("--wait", action="store_true")
    run.add_argument("--wait-timeout", type=float, metavar="SECONDS", help="bound the wait for a lease")
    run.add_argument("command", nargs=argparse.REMAINDER)
    for subcommand in (start, status, drain, resume, run):
        subcommand.add_argument("--socket", default=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.action == "deamon":
        args.action = "daemon"
    if args.action == "daemon" and args.socket_path is not None:
        args.socket = args.socket_path
    if args.action == "run":
        if args.wait_timeout is not None:
            if not valid_wait_timeout(args.wait_timeout):
                run.error(WAIT_TIMEOUT_ERROR)
            args.wait = True
        if len(args.command) < 2 or args.command[0] != "--":
            run.error("expected -- followed by a command")
        args.command = args.command[1:]
    try:
        if args.action == "daemon":
            asyncio.run(daemon(Path(args.socket), args.group))
            return 0
        status = request_server(args)
        if status < 0:
            signum = -status
            if signum not in (signal.SIGKILL, signal.SIGSTOP):
                signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)
            return 128 + signum
        return status
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        print(f"npu-lease: {exc}", file=sys.stderr)
        return 1
