"""Serve lease requests over a host-local Unix socket."""

import asyncio
import errno
import fcntl
import grp
import json
import os
from pathlib import Path
import signal
import socket
import stat
import struct

from .devices import inventory
from .leases import MAX_PENDING_REQUESTS, Leases, WAIT_TIMEOUT_ERROR, valid_wait_timeout
from .state import LeaseStore

HOST_LOCK = "/run/lock/npu-lease.lock"
LIMIT = 4096
GENERAL_CONNECTIONS = MAX_PENDING_REQUESTS + 16
RESERVED_CONNECTIONS = 8
PEER_FORMAT = "3i"


class Connections:
    def __init__(self, leases, general_limit=GENERAL_CONNECTIONS, reserve_limit=RESERVED_CONNECTIONS):
        self.leases = leases
        self.general_limit = general_limit
        self.reserve_limit = reserve_limit
        self.general = 0
        self.reserved = 0

    async def accept(self, reader, writer):
        try:
            peer = writer.get_extra_info("socket").getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize(PEER_FORMAT),
            )
            pid, uid, _ = struct.unpack(PEER_FORMAT, peer)
            reserved = self.general >= self.general_limit
            if reserved:
                if uid != os.geteuid() or self.reserved >= self.reserve_limit:
                    return
                self.reserved += 1
            else:
                self.general += 1
            try:
                await handle_connection(self.leases, reader, writer, pid, uid, reserved)
            finally:
                if reserved:
                    self.reserved -= 1
                else:
                    self.general -= 1
        finally:
            writer.close()


async def handle_connection(leases, reader, writer, pid, uid, reserved=False):
    allocation = None
    disconnected = None
    try:
        async with asyncio.timeout(5):
            raw = await reader.readline()
        if not raw:
            return
        request = json.loads(raw)
        if not isinstance(request, dict):
            raise ValueError("expected a request object")
        operation = request.get("op")
        if operation == "status":
            if request.get("queue") is True:
                await reply(writer, await leases.inspect(include_queue=True))
            elif reserved:
                raise ValueError("live status unavailable at connection capacity; use status --queue")
            else:
                await live_status(leases, reader, writer)
            return
        if operation in ("drain", "resume"):
            if uid != os.geteuid():
                raise ValueError("only the daemon owner can change maintenance mode")
            await reply(writer, leases.set_draining(operation == "drain"))
            return
        if operation != "run" or type(request.get("wait")) is not bool:
            raise ValueError("invalid request")
        if reserved:
            raise ValueError("connection capacity reserved for maintenance")
        timeout = request.get("wait_timeout")
        if timeout is not None and (not request["wait"] or not valid_wait_timeout(timeout)):
            raise ValueError(WAIT_TIMEOUT_ERROR)
        disconnected = asyncio.create_task(reader.readline())
        allocation = leases.submit(request, pid, uid, disconnected)
        await asyncio.wait({allocation.result, disconnected}, return_when=asyncio.FIRST_COMPLETED)
        if disconnected.done():
            return
        response = allocation.result.result()
        await reply(writer, response)
        if "ids" in response:
            async with asyncio.timeout(15):
                raw_registration = await disconnected
            if not raw_registration:
                return
            leases.register(allocation, json.loads(raw_registration))
            await reply(writer, {"bound": True})
    except (ValueError, TimeoutError) as exc:
        try:
            await reply(writer, {"error": str(exc) or "request timed out"})
        except (ConnectionError, OSError):
            pass
    except (ConnectionError, OSError):
        pass
    finally:
        if disconnected is not None:
            disconnected.cancel()
        if allocation is not None:
            leases.finish(allocation)


async def live_status(leases, reader, writer):
    disconnected = asyncio.create_task(reader.read(1))
    scan = asyncio.create_task(leases.inspect(include_queue=False))
    try:
        await asyncio.wait({disconnected, scan}, return_when=asyncio.FIRST_COMPLETED)
        if not disconnected.done():
            await reply(writer, scan.result())
    finally:
        for task in (disconnected, scan):
            task.cancel()
        await asyncio.gather(disconnected, scan, return_exceptions=True)


async def reply(writer, response):
    writer.write(json.dumps(response).encode() + b"\n")
    await writer.drain()


async def daemon(path, group):
    parent = path.parent.stat()
    if parent.st_uid != os.geteuid() or parent.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ValueError("socket parent must be daemon-owned and writable only by its owner")
    lock_path = path.with_name(path.name + ".lock")
    host_directory = Path(HOST_LOCK).parent
    host_directory.mkdir(mode=0o755, exist_ok=True)
    host_info = host_directory.stat()
    if (host_info.st_uid != os.geteuid()
            or (host_info.st_mode & 0o022 and not host_info.st_mode & stat.S_ISVTX)):
        raise ValueError("host lock directory must be daemon-owned and protected from replacement")
    with open(HOST_LOCK, "a", opener=private_file) as host_lock:
        lock_info = os.fstat(host_lock.fileno())
        if not stat.S_ISREG(lock_info.st_mode) or lock_info.st_uid != os.geteuid():
            raise ValueError("host lock must be a regular file owned by the daemon user")
        try:
            fcntl.flock(host_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("another npu-lease daemon is active on this host") from exc
        with open(lock_path, "a", opener=private_file) as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            await serve(path, group)


def private_file(path, flags):
    return os.open(path, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)


async def serve(path, group):
    leases = Leases(inventory(), LeaseStore(path.with_name(path.name + ".leases")))
    gid = grp.getgrnam(group).gr_gid if group else None
    # A killed daemon leaves a socket inode behind. The lock serializes recovery.
    if path.is_socket() and path.lstat().st_uid == os.geteuid():
        with socket.socket(socket.AF_UNIX) as probe_socket:
            probe_socket.settimeout(1)
            if probe_socket.connect_ex(str(path)) != errno.ECONNREFUSED:
                raise ValueError("socket already in use or its owner cannot be determined")
        path.unlink()
    with socket.socket(socket.AF_UNIX) as listener:
        old_mask = os.umask(0o177)
        try:
            listener.bind(str(path))
        finally:
            os.umask(old_mask)
        try:
            if gid is not None:
                os.chown(path, -1, gid)
                path.chmod(0o660)
            server = await asyncio.start_unix_server(Connections(leases).accept, sock=listener, limit=LIMIT)
            stop = asyncio.Event()
            for sig in (signal.SIGINT, signal.SIGTERM):
                asyncio.get_running_loop().add_signal_handler(sig, stop.set)
            async with server:
                print(f"listening on {path} for NPU IDs {leases.devices}", flush=True)
                scheduler = asyncio.create_task(leases.schedule())
                stopping = asyncio.create_task(stop.wait())
                try:
                    await asyncio.wait({scheduler, stopping}, return_when=asyncio.FIRST_COMPLETED)
                    if scheduler.done():
                        scheduler.result()
                finally:
                    for task in (scheduler, stopping):
                        task.cancel()
                    await asyncio.gather(scheduler, stopping, return_exceptions=True)
        finally:
            path.unlink(missing_ok=True)
