import unittest
from unittest.mock import patch
import AE_RenderSlave

class TestAERenderSlaveHelpers(unittest.TestCase):

    def test_get_ae_version(self):
        # Extract correctly when format is "After Effects YYYY"
        self.assertEqual(AE_RenderSlave.get_ae_version("C:\\Program Files\\Adobe\\Adobe After Effects 2024\\Support Files\\aerender.exe"), "2024")
        self.assertEqual(AE_RenderSlave.get_ae_version("/Applications/Adobe After Effects 2021/aerender"), "2021")
        # Handle lowercase and other cases
        self.assertEqual(AE_RenderSlave.get_ae_version("after effects 2025 path"), "2025")
        # Handle missing version
        self.assertEqual(AE_RenderSlave.get_ae_version("C:\\Some\\Path\\aerender.exe"), "Unknown")

    def test_check_plugins(self):
        installed_set = {"particular", "optical flares", "rs_motionblur"}

        # Test built-in ADBE effects always returning True
        required_adbe = ["ADBE Gaussian Blur", "adbe some effect"]
        result_adbe = AE_RenderSlave.check_plugins(required_adbe, installed_set)
        self.assertTrue(result_adbe["ADBE Gaussian Blur"])
        self.assertTrue(result_adbe["adbe some effect"])

        # Test finding plugins by exact match or substring
        required_mixed = ["Particular", "flares", "missing_plugin"]
        result_mixed = AE_RenderSlave.check_plugins(required_mixed, installed_set)
        self.assertTrue(result_mixed["Particular"]) # exact match case insensitive
        self.assertTrue(result_mixed["flares"]) # substring match
        self.assertFalse(result_mixed["missing_plugin"]) # not in set


class TestAERenderSlaveState(unittest.TestCase):

    @patch('AE_RenderSlave.get_local_ip')
    @patch('AE_RenderSlave.find_aerender')
    @patch('AE_RenderSlave.get_ae_version')
    @patch('AE_RenderSlave.scan_installed_plugins')
    @patch('socket.gethostname')
    def test_slave_state_init(self, mock_hostname, mock_scan_plugins, mock_ae_version, mock_find_aerender, mock_get_ip):
        # Setup mocks
        mock_hostname.return_value = "Test-Host"
        mock_get_ip.return_value = "192.168.1.100"
        mock_find_aerender.return_value = "/path/to/aerender"
        mock_ae_version.return_value = "2024"
        mock_scan_plugins.return_value = ["particular", "optical flares"]

        # Initialize
        slave = AE_RenderSlave.SlaveState("192.168.1.10", "Custom-Name", 9877)

        # Assertions for arguments taking precedence over defaults
        self.assertEqual(slave.manager_ip, "192.168.1.10")
        self.assertEqual(slave.hostname, "Custom-Name") # "name" argument
        self.assertEqual(slave.listen_port, 9877)

        # Assertions for properties populated from methods
        self.assertEqual(slave.local_ip, "192.168.1.100")
        self.assertEqual(slave.aerender, "/path/to/aerender")
        self.assertEqual(slave.ae_version, "2024")
        self.assertEqual(slave.installed_plugins, {"particular", "optical flares"})

        # Test defaults
        slave_defaults = AE_RenderSlave.SlaveState("192.168.1.10", "", 9877)
        self.assertEqual(slave_defaults.hostname, "Test-Host") # default fallback

    @patch('AE_RenderSlave.get_local_ip', return_value="127.0.0.1")
    @patch('AE_RenderSlave.find_aerender', return_value="/path")
    @patch('AE_RenderSlave.scan_installed_plugins', return_value=[])
    def test_build_payload(self, mock_scan, mock_find, mock_ip):
        slave = AE_RenderSlave.SlaveState("10.0.0.1", "Node1", 9000)

        # Test basic payload without extras
        payload = slave._build_payload()
        self.assertEqual(payload["type"], "SLAVE_STATUS")
        self.assertEqual(payload["hostname"], "Node1")
        self.assertEqual(payload["ip"], "127.0.0.1")
        self.assertEqual(payload["port"], 9000)
        self.assertEqual(payload["status"], "Idle")
        self.assertEqual(payload["current_job"], "--")
        self.assertEqual(payload["progress"], 0)
        self.assertTrue("cpu_pct" in payload)
        self.assertTrue("ram_gb" in payload)

        # Test with extra dictionary updates
        payload_extra = slave._build_payload({"type": "SLAVE_CONNECT", "custom": True})
        self.assertEqual(payload_extra["type"], "SLAVE_CONNECT")
        self.assertTrue(payload_extra["custom"])


    @patch('AE_RenderSlave.get_local_ip', return_value="127.0.0.1")
    @patch('AE_RenderSlave.find_aerender', return_value="/path")
    @patch('AE_RenderSlave.scan_installed_plugins', return_value=["particular", "element"])
    def test_handle_preflight(self, mock_scan, mock_find, mock_ip):
        slave = AE_RenderSlave.SlaveState("10.0.0.1", "Node1", 9000)

        required_plugins = ["Particular", "missing", "Element"]
        result = slave.handle_preflight(required_plugins)

        # Result is a dictionary matching input plugin to a boolean availability
        self.assertEqual(len(result), 3)
        self.assertTrue(result["Particular"]) # "particular" is in the mock set
        self.assertFalse(result["missing"])   # "missing" is not in the set
        self.assertTrue(result["Element"])    # "element" is in the set

        # Test built-in ADBE pass-through correctly inside handle_preflight
        result_adbe = slave.handle_preflight(["ADBE Transform"])
        self.assertTrue(result_adbe["ADBE Transform"])


if __name__ == '__main__':
    unittest.main()
