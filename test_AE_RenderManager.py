import pytest
from AE_RenderManager import check_slave_plugins

def test_check_slave_plugins_empty_required():
    """
    Test that check_slave_plugins returns an empty dict without attempting
    a network connection when the required list is empty.
    """
    # Using an invalid IP/port that would normally raise an exception or timeout
    # if a network connection was actually attempted.
    result = check_slave_plugins("256.256.256.256", 9999, [])
    assert result == {}

    # Also verify behavior with None, which evaluates to False
    result = check_slave_plugins("256.256.256.256", 9999, None)
    assert result == {}
