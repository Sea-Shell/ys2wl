import json
import tempfile
import os
from ys2wl.core.auth import (
    get_client_config,
    credentials_status,
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


def test_oauth2_credentials_constructor():
    from google.oauth2.credentials import Credentials

    creds = Credentials(
        token="ya29.test_token",
        refresh_token="1//test_refresh",
        token_uri="https://oauth2.googleapis.com/token",
        client_id="123456.apps.googleusercontent.com",
        client_secret="G0ogleS3cret",
        scopes=["https://www.googleapis.com/auth/youtube"],
    )
    assert creds.token == "ya29.test_token"
    assert creds.refresh_token == "1//test_refresh"
    assert creds.valid
    assert creds.expired is False


def test_oauth2_credentials_pickle_roundtrip():
    import pickle
    from google.oauth2.credentials import Credentials

    creds = Credentials(
        token="ya29.test_token",
        refresh_token="1//test_refresh",
        token_uri="https://oauth2.googleapis.com/token",
        client_id="123456.apps.googleusercontent.com",
        client_secret="G0ogleS3cret",
        scopes=["https://www.googleapis.com/auth/youtube"],
    )
    data = pickle.dumps(creds)
    loaded = pickle.loads(data)
    assert loaded.token == "ya29.test_token"
    assert loaded.refresh_token == "1//test_refresh"


def test_credentials_status_with_valid_creds():
    from google.oauth2.credentials import Credentials
    from ys2wl.core.auth import credentials_status

    creds = Credentials(
        token="ya29.test_token",
        token_uri="https://oauth2.googleapis.com/token",
        client_id="123456.apps.googleusercontent.com",
        client_secret="G0ogleS3cret",
    )
    status = credentials_status(creds)
    assert status["authenticated"] is True
