import pytest
import os
import json
from unittest.mock import patch, mock_open

import AE_RenderManager

def test_load_history_no_file(mocker):
    mocker.patch('os.path.exists', return_value=False)
    assert AE_RenderManager.load_history() == []

def test_load_history_success(mocker):
    mocker.patch('os.path.exists', return_value=True)
    mock_data = [{"id": "1", "status": "Completed"}]
    mocker.patch('builtins.open', mock_open(read_data=json.dumps(mock_data)))
    assert AE_RenderManager.load_history() == mock_data

def test_load_history_exception(mocker):
    mocker.patch('os.path.exists', return_value=True)
    mocker.patch('builtins.open', side_effect=Exception("Test exception"))
    mock_logger = mocker.patch('AE_RenderManager.log')
    assert AE_RenderManager.load_history() == []
    mock_logger.error.assert_called_once()
