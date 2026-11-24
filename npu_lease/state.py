"""Persist and recover workload identities."""

import json
import os
import tempfile
from pathlib import Path


def process_identity(pid):
    try:
        raw = Path(f"/proc/{pid}/stat").read_bytes()
    except FileNotFoundError:
        return None
    fields = raw.rsplit(b") ", 1)[1].split()
    if fields[0] in (b"Z", b"X"):
        return None
    return int(fields[1]), int(fields[19])


class LeaseStore:
    def __init__(self, path):
        self.path = path
        self.boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()

    def load(self, devices):
        if not self.path.exists():
            return {}
        data = json.loads(self.path.read_text())
        if not isinstance(data, dict) or set(data) != {"boot_id", "devices"}:
            raise ValueError("invalid lease state")
        if data["boot_id"] != self.boot_id:
            return {}
        active = {}
        for key, holder in data["devices"].items():
            device = int(key)
            if (device not in devices or device in active or not isinstance(holder, dict)
                    or set(holder) != {"pid", "uid", "start_time"}
                    or any(type(value) is not int or value < 0 for value in holder.values())
                    or holder["pid"] == 0):
                raise ValueError("invalid or incompatible lease state")
            active[device] = holder
        return active

    def save(self, active):
        bound = {device: holder for device, holder in active.items() if "start_time" in holder}
        with tempfile.NamedTemporaryFile(mode="w", dir=self.path.parent, delete=False) as stream:
            pending = Path(stream.name)
            try:
                json.dump({"boot_id": self.boot_id, "devices": bound}, stream)
                stream.flush()
                os.fsync(stream.fileno())
                pending.replace(self.path)
                directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                pending.unlink(missing_ok=True)
