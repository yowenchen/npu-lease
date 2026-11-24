# npu-lease

A Linux host tool inspired by [gpu-lease](https://github.com/reyoung/gpu-lease/tree/fce454b9bee4b2a32ddabc8d33af7b205b56adff).
The tool needs Linux, `/usr/bin/python3` 3.11 or later, and the host's
`/usr/local/sbin/npu-smi`. It uses the Python standard library. The wrapped
workload can run in its own Python or virtual environment.
The supported topology is one chip per device with matching NPU and physical IDs.

## Install

From a checkout, install on the host that runs the workload:

```sh
git clone https://github.com/yowenchen/npu-lease.git
cd npu-lease
sudo ./install.sh
npu-lease --help
```

The script checks the system Python and installs the executable at
`/usr/local/bin/npu-lease`. It accepts an absolute prefix as its first argument.
The daemon remains stopped after installation. Before upgrading a running
daemon, drain and stop it, then restart with the original socket path. If
the installed version predates `drain`, pause new submissions through your
team's launch process and let running leases finish before stopping it.

## Manual startup

As root, after the team agrees to use the daemon:

```sh
install -d -m 0755 /run/npu-lease
npu-lease daemon --group HwHiAiUser
```

Omit `--group` for an owner-only socket. The host-wide lock is
`/run/lock/npu-lease.lock`. The daemon creates `/run/lock` if it is missing.
Daemons must share the host's `/run/lock` mount for this lock to apply. Run
the daemon as a host administrator.

The daemon runs in the foreground. Restart using the same socket path.
The adjacent `control.sock.leases` file tracks running workloads, and the
adjacent `control.sock.lock` file serializes daemon startup. Keep both files
and the socket directory intact across daemon restarts.
For isolated testing, use `--socket PATH` on both daemon and clients, with a
private owner-controlled parent directory. On restart the daemon removes its
stale socket after checking that no listener is active.

## Client

```sh
npu-lease status
npu-lease status --queue
npu-lease run --ids 0,2 -- python train.py
npu-lease run --num 2 --wait-timeout 120 -- python train.py
```

Requests reserve the whole set atomically among clients of the same daemon.
A recognized idle report is required for each granted device. Busy devices and
probe failures are skipped. Waiting requests are considered in arrival order on release and on periodic
device checks. A request that cannot be fulfilled with the currently idle
devices may be bypassed by a later request that can. `--wait-timeout SECONDS`
implies `--wait` and bounds lease acquisition. The client enforces this
deadline even with an older daemon. It does not limit the workload's runtime. `status --queue` reports queue order, wait durations, maintenance state
and the last completed device probes with their ages. Use plain `status` for a
fresh device scan. An unknown state includes the reason the probe could not be
used. Queue inspection and maintenance commands can run during a slow probe.

The lease request queue holds up to 128 requests. The connection limit leaves
room for status and maintenance when the queue is full. When general
connections fill up, additional slots are reserved for the daemon owner's
`status --queue`, `drain`, and `resume`. A live `status` request in a reserved
slot is directed to `status --queue`.

## Maintenance

The daemon owner can run `npu-lease drain` to pause grants and reject queued
requests while existing leases continue. Check `npu-lease status --queue` until
there are no leased devices, then stop the daemon for maintenance. Use
`npu-lease resume` to resume admission without restarting it. Drain mode lasts
for the current daemon process.

## Workload behavior

The command runs as the invoking user with `ASCEND_RT_VISIBLE_DEVICES` set to the
granted IDs in ascending order. The daemon records its PID, process start time
and host boot ID before the command can run. The reservation follows that
process across a daemon restart or client crash. The client forwards SIGINT,
SIGTERM and SIGHUP to its direct child. Keep the workload in the foreground and
have it wait for its workers. Workers that outlive their launcher are outside
this lease lifetime.

`--count` is an alias for `--num`. The socket can be specified before or after the
subcommand or as a positional argument to `daemon`. The upstream spelling
`deamon` is accepted as an alias.

Device masks and leases coordinate participating workloads. Jobs launched
directly may bypass them and can race with a grant. The daemon never terminates
compute processes or changes device permissions.

## Development

The `npu_lease/` package separates the CLI and client, Unix socket server,
lease scheduling, persistent state, and NPU discovery. The checkout's
`./npu-lease` launches the package directly. The installer bundles it into a
single executable Python archive that can run outside the checkout.

Run the hardware-independent suite on Linux with Python 3.11 or later:

```sh
python3 -m unittest discover -s tests -v
```

The suite uses simulated device responses, temporary sockets, and ordinary
subprocesses. On npu8, an administrator can run the integration suite with
the CANN toolkit environment loaded and the shared daemon stopped:

```sh
python3 tests/verify_npu8.py
```

Set `NPU_LEASE_TEST_BIN` to verify a binary installed in a temporary prefix.
The integration test uses a private socket and stops its daemon on exit.
