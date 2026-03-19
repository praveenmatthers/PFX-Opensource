import unittest
from unittest.mock import patch
import json
import os

# We need to mock QApplication before importing AE_RenderManager
# because it instantiates GUI elements at module level if not careful.
# However, AE_RenderManager uses PyQt5. Let's see if we can import just what we need,
# or mock sys.modules to avoid Qt import issues.

import sys
from unittest.mock import MagicMock

# Create a mock for PyQt5 modules so we don't need a real X11 display
sys.modules['PyQt5'] = MagicMock()
sys.modules['PyQt5.QtWidgets'] = MagicMock()
sys.modules['PyQt5.QtCore'] = MagicMock()
sys.modules['PyQt5.QtGui'] = MagicMock()

import AE_RenderManager

class TestAERenderManager(unittest.TestCase):

    @patch('AE_RenderManager.log')
    @patch('builtins.open')
    def test_save_history_error_path(self, mock_open, mock_log):
        # Configure the mock open to raise an exception when called
        error_msg = "Permission denied"
        mock_open.side_effect = Exception(error_msg)

        # Create some dummy jobs to pass to save_history
        class DummyJob:
            def to_dict(self):
                return {"status": "Active", "submitted_epoch": 12345}

        dummy_jobs = {"job1": DummyJob()}

        # Call the function, which should hit the except block
        AE_RenderManager.save_history(dummy_jobs)

        # Verify that builtins.open was called
        mock_open.assert_called_once_with(AE_RenderManager.HISTORY_FILE, "w")

        # Verify that the exception was caught and logged appropriately
        mock_log.error.assert_called_once()

        # Check that the logged message contains the expected text
        logged_arg = mock_log.error.call_args[0][0]
        self.assertIn("Save history failed:", logged_arg)
        self.assertIn(error_msg, logged_arg)

if __name__ == '__main__':
    unittest.main()
