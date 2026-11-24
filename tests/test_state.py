import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from npu_lease.leases import Leases
from npu_lease.state import LeaseStore, process_identity


class StateTests(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.store = LeaseStore(Path(directory) / "control.sock.leases")
        self.holder = {
            "pid": os.getpid(), "uid": os.getuid(),
            "start_time": process_identity(os.getpid())[1],
        }

    def test_restart_recovers_bound_leases_only(self):
        self.store.save({0: self.holder, 1: {"pid": os.getpid(), "uid": os.getuid()}})
        leases = Leases([0, 1], self.store)
        leases.reap()
        self.assertEqual(leases.active, {0: self.holder})

    def test_reap_releases_dead_or_reused_pid(self):
        for identity in (None, (os.getppid(), self.holder["start_time"] + 1)):
            with self.subTest(identity=identity):
                self.store.save({0: self.holder})
                leases = Leases([0], self.store)
                with patch("npu_lease.leases.process_identity", return_value=identity):
                    leases.reap()
                self.assertEqual(leases.active, {})
                self.assertEqual(self.store.load([0]), {})

    def test_previous_boot_does_not_restore_leases(self):
        self.store.path.write_text(json.dumps({"boot_id": "other", "devices": {"0": self.holder}}))
        self.assertEqual(self.store.load([0]), {})

    def test_corrupt_or_incompatible_state_fails_closed(self):
        self.store.path.write_text("truncated")
        with self.assertRaises(ValueError):
            Leases([0], self.store)
        self.store.save({1: self.holder})
        with self.assertRaises(ValueError):
            Leases([0], self.store)

    def test_failed_persistence_does_not_bind_workload(self):
        leases = Leases([0], self.store)
        client = os.getppid()
        reservation = {"pid": client, "uid": os.getuid()}
        leases.active[0] = reservation
        with patch.object(self.store, "save", side_effect=OSError("full")):
            with self.assertRaises(OSError):
                leases.bind_workload(client, os.getuid(), [0], {"pid": os.getpid()})
        self.assertEqual(leases.active[0], reservation)
