import pytest
from unittest.mock import patch, MagicMock
import AE_RenderSlave
import sys

def test_ram_used_gb_happy_path():
    with patch('AE_RenderSlave.HAS_PSUTIL', True):
        mock_psutil = MagicMock()
        mock_mem_info = MagicMock()
        mock_mem_info.used = 16_123_456_789
        mock_psutil.virtual_memory.return_value = mock_mem_info

        with patch.dict(sys.modules, {'psutil': mock_psutil}):
            with patch('AE_RenderSlave.psutil', mock_psutil, create=True):
                result = AE_RenderSlave.ram_used_gb()
                assert result == 16.1

def test_ram_used_gb_exception_fallback():
    with patch('AE_RenderSlave.HAS_PSUTIL', True):
        mock_psutil = MagicMock()
        mock_psutil.virtual_memory.side_effect = Exception("mocked error")

        with patch.dict(sys.modules, {'psutil': mock_psutil}):
            with patch('AE_RenderSlave.psutil', mock_psutil, create=True):
                result = AE_RenderSlave.ram_used_gb()
                assert result == 0.0

def test_ram_used_gb_no_psutil_fallback():
    with patch('AE_RenderSlave.HAS_PSUTIL', False):
        result = AE_RenderSlave.ram_used_gb()
        assert result == 0.0
