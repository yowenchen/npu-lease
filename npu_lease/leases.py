"""Queue and allocate device leases."""

import asyncio
from dataclasses import dataclass

from .devices import probe
from .state import process_identity

MAX_PENDING_REQUESTS = 128
MAX_WAIT_SECONDS = 86400
WAIT_TIMEOUT_ERROR = f"wait timeout must be greater than 0 and at most {MAX_WAIT_SECONDS} seconds"
DRAINING_ERROR = "daemon is draining"
TIMEOUT_ERROR = "lease wait timed out"


def valid_wait_timeout(value):
    return type(value) in (int, float) and 0 < value <= MAX_WAIT_SECONDS


@dataclass
class Request:
    candidates: list[int]
    count: int
    explicit: bool
    wait: bool
    queued_at: float
    deadline: float | None
    pid: int
    uid: int
    disconnected: asyncio.Task
    result: asyncio.Future
    timer: asyncio.TimerHandle | None = None
    granted: tuple[int, ...] = ()
    bound: bool = False


class Leases:
    def __init__(self, devices, store):
        self.devices = devices
        self.store = store
        self.active = store.load(devices)
        self.live_scan = None
        self.scan_waiters = 0
        self.last_probes = {}
        self.pending = {}
        self.changed = asyncio.Event()
        self.draining = False

    def submit(self, message, pid, uid, disconnected):
        if self.draining:
            raise ValueError(DRAINING_ERROR)
        if len(self.pending) >= MAX_PENDING_REQUESTS:
            raise ValueError("lease request queue is full")
        candidates, count = self.validate(message)
        loop = asyncio.get_running_loop()
        now = loop.time()
        timeout = message.get("wait_timeout")
        result = loop.create_future()
        request = Request(
            candidates, count, message.get("ids") is not None, message["wait"], now,
            None if timeout is None else now + timeout, pid, uid, disconnected, result,
        )
        self.pending[result] = request
        if request.deadline is not None:
            request.timer = loop.call_at(request.deadline, self.expire, request)
        self.changed.set()
        return request

    def decide(self, request, response):
        self.pending.pop(request.result, None)
        if request.timer is not None:
            request.timer.cancel()
        request.result.set_result(response)

    def expire(self, request):
        if not request.result.done():
            self.decide(request, {"error": TIMEOUT_ERROR})

    def finish(self, request):
        self.pending.pop(request.result, None)
        if request.timer is not None:
            request.timer.cancel()
        if not request.bound:
            for device in request.granted:
                del self.active[device]
        request.result.cancel()
        self.changed.set()

    async def inspect(self, include_queue):
        self.reap()
        if include_queue:
            now = asyncio.get_running_loop().time()
            states = {}
            for device in self.devices:
                if device in self.active:
                    states[device] = {"state": "leased", **self.active[device]}
                elif device in self.last_probes:
                    observed, result = self.last_probes[device]
                    states[device] = {**result, "probe_age_seconds": round(now - observed, 2)}
                else:
                    states[device] = {"state": "unknown", "reason": "no completed device probe"}
            return {"devices": states, **self.queue_status()}
        self.scan_waiters += 1
        try:
            if self.live_scan is None or self.live_scan.done():
                self.live_scan = asyncio.create_task(
                    self.snapshot(self.devices, stop=lambda: self.scan_waiters == 0),
                )
            return {"devices": await asyncio.shield(self.live_scan)}
        finally:
            self.scan_waiters -= 1

    def set_draining(self, draining):
        self.reap()
        self.draining = draining
        if draining:
            for request in list(self.pending.values()):
                if not request.result.done():
                    self.decide(request, {"error": DRAINING_ERROR})
        self.changed.set()
        return {"draining": self.draining, "reserved_devices": len(self.active)}

    def register(self, request, registration):
        self.bind_workload(request.pid, request.uid, request.granted, registration)
        request.bound = True

    def queue_status(self):
        now = asyncio.get_running_loop().time()
        queue = []
        for request in self.pending.values():
            if request.result.done() or request.disconnected.done() or not request.wait:
                continue
            selection = {"ids": request.candidates} if request.explicit else {"num": request.count}
            queue.append({
                **selection, "pid": request.pid, "uid": request.uid,
                "wait_seconds": round(now - request.queued_at, 2),
                "timeout_remaining_seconds": (
                    None if request.deadline is None else round(max(0, request.deadline - now), 2)
                ),
            })
        return {"draining": self.draining, "queued": queue}

    def reap(self):
        expired = []
        for device, holder in self.active.items():
            if "start_time" not in holder:
                continue
            identity = process_identity(holder["pid"])
            if identity is None or identity[1] != holder["start_time"]:
                expired.append(device)
        if expired:
            remaining = {device: holder for device, holder in self.active.items() if device not in expired}
            self.store.save(remaining)
            self.active = remaining

    def bind_workload(self, connect_pid, uid, devices, registration):
        pid = registration.get("pid") if isinstance(registration, dict) else None
        if type(pid) is not int or pid <= 0:
            raise ValueError("expected a workload PID")
        identity = process_identity(pid)
        if identity is None or identity[0] != connect_pid:
            raise ValueError("workload must be a live child of the lease client")
        holder = {"pid": pid, "uid": uid, "start_time": identity[1]}
        updated = dict(self.active)
        updated.update({device: holder for device in devices})
        self.store.save(updated)
        self.active = updated

    async def schedule(self):
        while True:
            if self.pending or self.active:
                try:
                    async with asyncio.timeout(1):
                        await self.changed.wait()
                except TimeoutError:
                    pass
            else:
                await self.changed.wait()
            self.changed.clear()

            self.reap()
            probes = {}
            for request in list(self.pending.values()):
                if request.result.done() or request.disconnected.done():
                    continue
                states = await self.snapshot(
                    request.candidates, request.count, probes,
                    stop=lambda: request.result.done() or request.disconnected.done(),
                )
                # Drain and expiry can complete this request while a driver probe is in flight.
                if request.result.done() or request.disconnected.done():
                    continue
                if request.deadline is not None and asyncio.get_running_loop().time() >= request.deadline:
                    self.expire(request)
                    continue
                chosen = tuple(device for device, state in states.items() if state["state"] == "idle")
                if len(chosen) == request.count:
                    for device in chosen:
                        probes.pop(device, None)
                        self.last_probes.pop(device, None)
                        self.active[device] = {"uid": request.uid, "pid": request.pid}
                    request.granted = chosen
                    self.decide(request, {"ids": chosen})
                elif not request.wait:
                    self.decide(request, {"error": "requested devices unavailable", "devices": states})

    async def snapshot(self, candidates, count=None, probes=None, *, stop=None):
        if probes is None:
            probes = {}
        states = {}
        for device in candidates:
            if stop is not None and stop():
                break
            holder = self.active.get(device)
            if holder is not None:
                states[device] = {"state": "leased", **holder}
            else:
                if device not in probes:
                    probes[device] = await asyncio.to_thread(probe, device)
                    self.last_probes[device] = (asyncio.get_running_loop().time(), probes[device])
                holder = self.active.get(device)
                states[device] = {"state": "leased", **holder} if holder is not None else probes[device]
            if count is not None and sum(s["state"] == "idle" for s in states.values()) == count:
                break
        return states

    def validate(self, request):
        explicit = request.get("ids")
        count = request.get("num")
        if explicit is not None:
            if (count is not None or not isinstance(explicit, list) or not explicit
                    or any(type(device) is not int or device not in self.devices for device in explicit)
                    or len(explicit) != len(set(explicit))):
                raise ValueError("ids must be distinct device IDs from the daemon inventory")
            return explicit, len(explicit)
        if type(count) is not int or not 1 <= count <= len(self.devices):
            raise ValueError("num must be between 1 and the inventory size")
        return self.devices, count
