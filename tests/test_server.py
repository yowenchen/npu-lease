import asyncio
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch

from npu_lease import server
from npu_lease.leases import Leases
from npu_lease.state import LeaseStore


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = Path(self.enterContext(TemporaryDirectory()))
        self.socket = self.directory / "control.sock"
        self.store = LeaseStore(self.socket.with_suffix(".leases"))
        self.leases = Leases([0, 1, 2], self.store)
        self.states = {device: {"state": "idle"} for device in self.leases.devices}
        self.enterContext(patch("npu_lease.leases.probe", side_effect=self.states.__getitem__))
        self.connections = server.Connections(self.leases)
        self.listener = await asyncio.start_unix_server(self.connections.accept, path=self.socket)
        self.scheduler = asyncio.create_task(self.leases.schedule())

    async def asyncTearDown(self):
        self.listener.close()
        await self.listener.wait_closed()
        self.scheduler.cancel()
        await asyncio.gather(self.scheduler, return_exceptions=True)

    async def open(self, message):
        reader, writer = await asyncio.open_unix_connection(self.socket)
        self.addAsyncCleanup(self.close, writer)
        await self.send(writer, message)
        return reader, writer

    @staticmethod
    async def close(writer):
        writer.close()
        await writer.wait_closed()

    @staticmethod
    async def send(writer, message):
        writer.write(json.dumps(message).encode() + b"\n")
        await writer.drain()

    @staticmethod
    async def receive(reader):
        return json.loads(await asyncio.wait_for(reader.readline(), timeout=3))

    async def request(self, **message):
        reader, writer = await self.open(message)
        try:
            return await self.receive(reader)
        finally:
            await self.close(writer)

    async def until(self, check):
        async with asyncio.timeout(3):
            while not check():
                await asyncio.sleep(0.01)

    async def test_queue_prefers_older_request_and_skips_blocked_requests(self):
        self.states[0] = {"state": "busy"}
        older, older_writer = await self.open({"op": "run", "ids": [0], "wait": True})
        await self.until(lambda: len(self.leases.pending) == 1)
        newer, _ = await self.open({"op": "run", "ids": [0], "wait": True})
        await self.until(lambda: len(self.leases.pending) == 2)
        disjoint = await self.request(op="run", ids=[1], wait=False)
        self.assertEqual(disjoint, {"ids": [1]})
        self.states[0] = {"state": "idle"}
        self.leases.changed.set()
        self.assertEqual(await self.receive(older), {"ids": [0]})
        self.assertEqual(len(self.leases.queue_status()["queued"]), 1)
        await self.close(older_writer)
        self.assertEqual(await self.receive(newer), {"ids": [0]})

    async def test_timeout_and_drain_do_not_launch_work(self):
        self.states[0] = {"state": "busy"}
        expired = await self.request(op="run", ids=[0], wait=True, wait_timeout=0.1)
        self.assertEqual(expired, {"error": "lease wait timed out"})
        waiting, _ = await self.open({"op": "run", "ids": [0], "wait": True})
        await self.until(lambda: len(self.leases.pending) == 1)
        status = await self.request(op="status", queue=True)
        self.assertFalse(status["draining"])
        self.assertEqual(status["queued"][0]["ids"], [0])
        self.assertGreaterEqual(status["queued"][0]["wait_seconds"], 0)
        self.assertTrue((await self.request(op="drain"))["draining"])
        self.assertEqual(await self.receive(waiting), {"error": "daemon is draining"})
        self.assertEqual(await self.request(op="run", ids=[1], wait=False),
                         {"error": "daemon is draining"})
        self.assertFalse((await self.request(op="resume"))["draining"])
        self.assertEqual(await self.request(op="run", ids=[1], wait=False), {"ids": [1]})

    async def test_full_queue_keeps_status_and_maintenance_accessible(self):
        self.states[0] = {"state": "busy"}
        with patch("npu_lease.leases.MAX_PENDING_REQUESTS", 2):
            first, _ = await self.open({"op": "run", "ids": [0], "wait": True})
            second, _ = await self.open({"op": "run", "ids": [0], "wait": True})
            await self.until(lambda: len(self.leases.pending) == 2)
            self.assertEqual(await self.request(op="run", ids=[0], wait=True),
                             {"error": "lease request queue is full"})
            status = await self.request(op="status", queue=True)
            self.assertEqual(len(status["queued"]), 2)
            self.assertTrue((await self.request(op="drain"))["draining"])
            for reader in (first, second):
                self.assertEqual(await self.receive(reader), {"error": "daemon is draining"})

    async def test_drain_rejects_waiters_before_resume_even_without_scheduler(self):
        self.scheduler.cancel()
        await asyncio.gather(self.scheduler, return_exceptions=True)
        waiting, _ = await self.open({"op": "run", "ids": [0], "wait": True})
        await self.until(lambda: len(self.leases.pending) == 1)
        self.assertTrue((await self.request(op="drain"))["draining"])
        self.assertEqual(await self.request(op="run", ids=[1], wait=True),
                         {"error": "daemon is draining"})
        self.assertFalse((await self.request(op="resume"))["draining"])
        self.assertEqual(await self.receive(waiting), {"error": "daemon is draining"})

    async def test_owner_reserve_remains_available_at_connection_capacity(self):
        self.connections.general_limit = 1
        self.connections.reserve_limit = 1
        _, general = await asyncio.open_unix_connection(self.socket)
        self.addAsyncCleanup(self.close, general)
        await self.until(lambda: self.connections.general == 1)
        self.assertIn("queued", await self.request(op="status", queue=True))
        self.assertIn("use status --queue", (await self.request(op="status"))["error"])
        self.assertTrue((await self.request(op="drain"))["draining"])
        self.assertFalse((await self.request(op="resume"))["draining"])
        self.assertEqual(await self.request(op="run", ids=[0], wait=False),
                         {"error": "connection capacity reserved for maintenance"})

        _, reserved = await asyncio.open_unix_connection(self.socket)
        self.addAsyncCleanup(self.close, reserved)
        await self.until(lambda: self.connections.reserved == 1)
        rejected, writer = await asyncio.open_unix_connection(self.socket)
        self.addAsyncCleanup(self.close, writer)
        self.assertEqual(await asyncio.wait_for(rejected.read(), 1), b"")
        self.assertEqual((self.connections.general, self.connections.reserved), (1, 1))

    async def test_slow_probe_does_not_block_queue_status_drain_or_expiry(self):
        started = threading.Event()
        unblock = threading.Event()

        def slow_probe(_device):
            started.set()
            unblock.wait(5)
            return {"state": "idle"}

        with patch("npu_lease.leases.probe", side_effect=slow_probe):
            try:
                waiting, _ = await self.open({"op": "run", "ids": [0], "wait": True})
                await self.until(started.is_set)
                result = await self.request(op="status", queue=True)
                self.assertEqual(len(result["queued"]), 1)
                self.assertEqual(result["devices"]["0"]["state"], "unknown")
                expired = await self.request(op="run", ids=[1], wait=True, wait_timeout=0.05)
                self.assertEqual(expired, {"error": "lease wait timed out"})
                self.assertFalse(unblock.is_set())
                self.assertTrue((await self.request(op="drain"))["draining"])
                self.assertFalse((await self.request(op="resume"))["draining"])
                self.assertEqual(await self.receive(waiting), {"error": "daemon is draining"})
                self.assertEqual(self.leases.active, {})
            finally:
                unblock.set()
        self.assertEqual(await self.request(op="run", ids=[0], wait=False), {"ids": [0]})

    async def test_disconnected_live_status_releases_the_connection_during_probe(self):
        self.connections.general_limit = 1
        started, unblock = threading.Event(), threading.Event()
        calls = []

        def slow_probe(device):
            calls.append(device)
            started.set()
            unblock.wait(5)
            return {"state": "idle"}

        with patch("npu_lease.leases.probe", side_effect=slow_probe):
            try:
                _, writer = await self.open({"op": "status"})
                await self.until(started.is_set)
                self.assertEqual(self.connections.general, 1)
                self.assertTrue((await self.request(op="drain"))["draining"])
                await self.close(writer)
                await self.until(lambda: self.connections.general == 0)
                self.assertTrue((await self.request(op="status", queue=True))["draining"])
                self.assertEqual(calls, [0])
            finally:
                unblock.set()
            await asyncio.wait_for(self.leases.live_scan, 3)
            self.assertEqual(calls, [0])

    async def test_drained_request_does_not_probe_another_device_after_resume(self):
        started, unblock = threading.Event(), threading.Event()
        calls = []

        def probe(device):
            calls.append(device)
            if device == 0:
                started.set()
                unblock.wait(5)
            return {"state": "idle"}

        with patch("npu_lease.leases.probe", side_effect=probe):
            try:
                waiting, _ = await self.open({"op": "run", "ids": [0, 1], "wait": True})
                await self.until(started.is_set)
                self.assertTrue((await self.request(op="drain"))["draining"])
                self.assertEqual(await self.receive(waiting), {"error": "daemon is draining"})
                self.assertFalse((await self.request(op="resume"))["draining"])
                fresh, _ = await self.open({"op": "run", "ids": [2], "wait": True, "wait_timeout": 1})
                await self.until(lambda: len(self.leases.pending) == 1)
                unblock.set()
                self.assertEqual(await self.receive(fresh), {"ids": [2]})
                self.assertEqual(calls, [0, 2])
            finally:
                unblock.set()

    async def test_grant_awaiting_registration_does_not_fill_request_queue(self):
        with patch("npu_lease.leases.MAX_PENDING_REQUESTS", 1):
            first, _ = await self.open({"op": "run", "ids": [0], "wait": True})
            self.assertEqual(await self.receive(first), {"ids": [0]})
            self.assertEqual(len(self.leases.pending), 0)
            second, _ = await self.open({"op": "run", "ids": [1], "wait": True})
            self.assertEqual(await self.receive(second), {"ids": [1]})

    async def test_scheduler_cancellation_wins_over_concurrent_wakeup(self):
        self.leases.active[0] = {"pid": os.getpid(), "uid": os.getuid()}
        await asyncio.sleep(0)
        self.leases.changed.set()
        await asyncio.sleep(0)
        self.scheduler.cancel()
        async with asyncio.timeout(1):
            await asyncio.gather(self.scheduler, return_exceptions=True)
        self.assertTrue(self.scheduler.cancelled())

    async def test_abandoned_registration_releases_entire_reservation(self):
        reader, writer = await self.open({"op": "run", "ids": [0, 1], "wait": False})
        self.assertEqual(await self.receive(reader), {"ids": [0, 1]})
        await self.send(writer, {"pid": os.getpid()})
        self.assertIn("live child", (await self.receive(reader))["error"])
        await self.until(lambda: not self.leases.active)
        self.assertEqual(self.store.load([0, 1]), {})

    async def test_registered_workload_keeps_lease_after_connection_closes(self):
        process = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(30)")
        try:
            reader, writer = await self.open({"op": "run", "ids": [0], "wait": False})
            self.assertEqual(await self.receive(reader), {"ids": [0]})
            await self.send(writer, {"pid": process.pid})
            self.assertEqual(await self.receive(reader), {"bound": True})
            await self.close(writer)
            self.assertEqual(self.store.load([0, 1])[0]["pid"], process.pid)
            self.assertEqual((await self.request(op="status"))["devices"]["0"]["pid"], process.pid)
            denied = await self.request(op="run", ids=[0, 1], wait=False)
            self.assertIn("unavailable", denied["error"])
            self.assertEqual((await self.request(op="status"))["devices"]["1"]["state"], "idle")
        finally:
            process.terminate()
            await process.wait()
        self.assertEqual((await self.request(op="status"))["devices"]["0"]["state"], "idle")

    async def test_host_lock_covers_different_socket_paths(self):
        started = asyncio.Event()

        async def hold(_path, _group):
            started.set()
            await asyncio.Future()

        runtime_lock = self.directory / "lock" / "host.lock"
        self.assertFalse(runtime_lock.parent.exists())
        with patch.object(server, "HOST_LOCK", str(runtime_lock)), \
             patch.object(server, "serve", side_effect=hold):
            daemon = asyncio.create_task(server.daemon(self.directory / "first.sock", None))
            try:
                await asyncio.wait_for(started.wait(), timeout=3)
                self.assertTrue(runtime_lock.is_file())
                with self.assertRaisesRegex(ValueError, "another npu-lease daemon"):
                    await server.daemon(self.directory / "second.sock", None)
            finally:
                daemon.cancel()
                await asyncio.gather(daemon, return_exceptions=True)

    async def test_daemon_rejects_unprotected_host_lock_directory(self):
        runtime_directory = self.directory / "writable"
        runtime_directory.mkdir()
        runtime_directory.chmod(0o777)
        with patch.object(server, "HOST_LOCK", str(runtime_directory / "host.lock")):
            with self.assertRaisesRegex(ValueError, "protected from replacement"):
                await server.daemon(self.directory / "other.sock", None)
