import subprocess
import unittest
from unittest.mock import patch

from npu_lease import devices


class DeviceTests(unittest.TestCase):
    def test_inventory_sorts_single_chip_devices(self):
        output = (
            "NPU ID  Slot ID  Chip ID  Chip Phy-ID  Chip Name\n"
            "7  42  0  7  Ascend950PR\n"
            "0  41  0  0  Ascend950PR\n"
        )
        with patch.object(devices, "smi", return_value=output):
            self.assertEqual(devices.inventory(), [0, 7])

    def test_inventory_rejects_incompatible_mappings(self):
        header = "NPU ID Slot ID Chip ID Chip Phy-ID Chip Name\n"
        for row in ("0 41 0 2 Ascend950PR", "0 41 1 0 Ascend950PR", "???"):
            with self.subTest(row=row), patch.object(devices, "smi", return_value=header + row):
                with self.assertRaises(ValueError):
                    devices.inventory()

    def test_probe_requires_explicit_idle_report_for_the_requested_device(self):
        for output, expected in (
            ("NPU ID : 2\nNo process in device.\n", "idle"),
            ("NPU ID : 0\nNo process in device.\n", "unknown"),
            ("NPU ID : 2\nProcess id:42 Process name:python3 Process memory(MB):1", "busy"),
            ("NPU ID : 2\nNo process in device.\nUnrecognized report", "unknown"),
        ):
            with self.subTest(output=output), patch.object(devices, "smi", return_value=output):
                self.assertEqual(devices.probe(2)["state"], expected)

    def test_probe_errors_include_a_diagnostic(self):
        for error, reason in (
            (subprocess.TimeoutExpired("npu-smi", 5), "timed out"),
            (subprocess.CalledProcessError(4, "npu-smi"), "status 4"),
            (FileNotFoundError(), "could not execute"),
        ):
            with self.subTest(reason=reason), patch.object(devices, "smi", side_effect=error):
                result = devices.probe(2)
                self.assertEqual(result["state"], "unknown")
                self.assertIn(reason, result["reason"])
