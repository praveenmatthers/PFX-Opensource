import pytest
import os
import sys

# Add the parent directory to the path so we can import AE_RenderSlave
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from AE_RenderSlave import check_plugins

def test_check_plugins_exact_match():
    required = ["sapphire"]
    installed = {"sapphire", "particular"}
    result = check_plugins(required, installed)
    assert result == {"sapphire": True}

def test_check_plugins_substring_match():
    required = ["sapphire"]
    installed = {"genarts_sapphire", "particular"}
    result = check_plugins(required, installed)
    assert result == {"sapphire": True}

    # Reverse substring match (required contains installed)
    required2 = ["sapphire_edge"]
    installed2 = {"sapphire", "particular"}
    result2 = check_plugins(required2, installed2)
    assert result2 == {"sapphire_edge": True}

def test_check_plugins_case_insensitive():
    required = ["Sapphire"]
    installed = {"sapphire", "particular"}
    result = check_plugins(required, installed)
    assert result == {"Sapphire": True}

def test_check_plugins_adbe_builtin():
    required = ["ADBE Fast Blur", "adbe Gaussian Blur", "regular_plugin"]
    installed = {"regular_plugin"}
    result = check_plugins(required, installed)
    assert result == {
        "ADBE Fast Blur": True,
        "adbe Gaussian Blur": True,
        "regular_plugin": True
    }

def test_check_plugins_empty_inputs():
    assert check_plugins([], {"plugin"}) == {}
    assert check_plugins(["plugin"], set()) == {"plugin": False}
    assert check_plugins([], set()) == {}

def test_check_plugins_non_string_edge_case():
    # When eff is not a string, name = ""
    # "" in any string is True, so if installed is not empty, it returns True.
    # If installed is empty, it returns False.
    required = [123, None]
    installed = {"some_plugin"}
    result = check_plugins(required, installed)
    assert result == {123: True, None: True}

    installed_empty = set()
    result_empty = check_plugins(required, installed_empty)
    assert result_empty == {123: False, None: False}

def test_check_plugins_missing_plugin():
    required = ["missing_plugin"]
    installed = {"sapphire", "particular"}
    result = check_plugins(required, installed)
    assert result == {"missing_plugin": False}
