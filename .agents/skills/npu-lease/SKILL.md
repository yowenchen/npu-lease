---
name: npu-lease
description: Use an existing npu-lease daemon before launching local Ascend NPU workloads on a shared Linux host, including training, inference, and benchmarks.
---

# NPU Lease

Run these checks on the host where the workload will execute. Use the host's
approved daemon and run as the intended workload user.

```bash
command -v npu-lease
npu-lease status
```

The default socket is `/run/npu-lease/control.sock`. If the host operator has
provided a different socket, use the same `--socket PATH` for both status and
run. A successful status query confirms the daemon is reachable. Use its
per-device states to diagnose unavailable devices. If `npu-lease status --help`
lists `--queue`, use it to see waiter order and maintenance state. Those fields
require daemon support. Use plain `status` for a fresh scan.

If the CLI is missing or the daemon is stopped or inaccessible, report the
reason. Get explicit authorization before installing or starting the daemon
or proceeding with an unleased NPU workload. For authorized setup and restart,
follow the [repository README](../../../README.md). Restart using the same
socket path and retain the adjacent lease state and lock files.

## Launch a workload

Request the number of cards needed. Use fixed host IDs only when the workload
requires them. Use `--wait` when waiting for cards is acceptable. A request
without `--wait` fails if the devices cannot be granted. To bound the wait, use
`--wait-timeout SECONDS` when it appears in `npu-lease run --help`. On versions
without that flag, use a non-waiting request if a bounded wait is required.

```bash
npu-lease run --count 2 --wait -- torchrun --nproc-per-node=2 train.py
npu-lease run --ids 0,2 -- python benchmark.py
```

Let `npu-lease run` set `ASCEND_RT_VISIBLE_DEVICES`. Within the workload, use
process-local device indices starting at zero. For example, a lease for host
IDs `0,2` exposes local indices `0,1`. Keep the launcher in the foreground and
ensure it waits for its workers. The lease tracks the launcher's lifetime.

The daemon grants devices only after a recognized idle report and leaves
existing compute processes running. Socket permissions control who can
request a lease. Jobs launched outside the daemon can still access the devices,
so treat the lease as coordination among participating workloads.
