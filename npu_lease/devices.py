"""Discover devices and probe for compute processes."""

import os
import re
import subprocess

SMI = "/usr/local/sbin/npu-smi"


def smi(*args):
    env = dict(os.environ, LC_ALL="C")
    return subprocess.run(
        [SMI, "info", *args], env=env, stdin=subprocess.DEVNULL,
        capture_output=True, text=True, timeout=5, check=True,
    ).stdout


def inventory():
    rows = [line.split() for line in smi("-m").splitlines() if line.strip()]
    if not rows or " ".join(rows[0]) != "NPU ID Slot ID Chip ID Chip Phy-ID Chip Name":
        raise ValueError("unrecognized npu-smi device inventory")
    ids = []
    for row in rows[1:]:
        if (len(row) != 5 or not all(field.isdecimal() for field in row[:4])
                or row[2] != "0" or row[0] != row[3]):
            raise ValueError("requires one chip per NPU and matching NPU and physical IDs")
        ids.append(int(row[0]))
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("empty or ambiguous NPU inventory")
    return sorted(ids)


def probe(device):
    try:
        output = smi("-t", "proc-mem", "-i", str(device), "-p", "0")
    except subprocess.TimeoutExpired:
        return {"state": "unknown", "reason": "npu-smi timed out"}
    except subprocess.CalledProcessError as exc:
        return {"state": "unknown", "reason": f"npu-smi exited with status {exc.returncode}"}
    except (OSError, UnicodeError):
        return {"state": "unknown", "reason": "could not execute or decode npu-smi"}
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines or not re.fullmatch(rf"NPU ID\s*:\s*{device}", lines[0]):
        return {"state": "unknown", "reason": "unrecognized npu-smi output"}
    if lines[1:] == ["No process in device."]:
        return {"state": "idle"}
    if any(
        re.match(r"^Process ID\s*:\s*[1-9][0-9]*\b", line, re.IGNORECASE)
        for line in lines[1:]
    ):
        return {"state": "busy"}
    return {"state": "unknown", "reason": "unrecognized npu-smi output"}
