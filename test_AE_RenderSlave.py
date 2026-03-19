import pytest
from unittest.mock import patch, MagicMock

import AE_RenderSlave

def test_cpu_pct_fallback_exception():
    with patch.object(AE_RenderSlave, 'HAS_PSUTIL', True):
        with patch('AE_RenderSlave.psutil', create=True) as mock_psutil:
            mock_psutil.cpu_percent.side_effect = Exception("mocked exception")
            assert AE_RenderSlave.cpu_pct() == 0.0

def test_cpu_pct_no_psutil():
    with patch.object(AE_RenderSlave, 'HAS_PSUTIL', False):
        assert AE_RenderSlave.cpu_pct() == 0.0
