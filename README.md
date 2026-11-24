# npu-lease

A Linux host tool inspired by [gpu-lease](https://github.com/reyoung/gpu-lease/tree/fce454b9bee4b2a32ddabc8d33af7b205b56adff).
It uses Python 3.11+ and the host's `/usr/local/sbin/npu-smi`.
The supported topology is one chip per device with matching NPU and physical IDs.

## Manual startup

As root, after the team agrees to use the daemon:

```sh
install -d -m 0755 /run/npu-lease
npu-lease daemon --group HwHiAiUser
```

Omit `--group` for an owner-only socket. Run one daemon per host.
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
npu-lease run --ids 0,2 -- python train.py
npu-lease run --num 2 --wait -- python train.py
```

Requests reserve the whole set atomically among clients of the same daemon.
A recognized idle report is required for each granted device. Busy devices and
probe failures are skipped. Waiting requests are considered in arrival order on release and on periodic
device checks. A request that cannot be fulfilled with the currently idle
devices may be bypassed by a later request that can.

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
