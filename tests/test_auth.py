import pytest
import json
import tempfile
import os
from unittest.mock import patch, MagicMock
from ys2wl.core.auth import (
    get_client_config, credentials_status,
)


def test_get_client_config_returns_none_for_missing_file():
    result = get_client_config("/nonexistent/credentials.json")
    assert result is None


def test_get_client_config_parses_installed():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"installed": {"client_id": "abc", "client_secret": "def"}}, f)
        path = f.name
    try:
        result = get_client_config(path)
        assert result == {"client_id": "abc", "client_secret": "def"}
    finally:
        os.unlink(path)


def test_get_client_config_parses_web():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"web": {"client_id": "xyz", "client_secret": "123"}}, f)
        path = f.name
    try:
        result = get_client_config(path)
        assert result == {"client_id": "xyz", "client_secret": "123"}
    finally:
        os.unlink(path)


def test_credentials_status_none():
    assert credentials_status(None) == {"authenticated": False}
