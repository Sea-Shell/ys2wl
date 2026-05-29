import json
import logging
import os
import pickle
from typing import Optional
from google.auth.credentials import Credentials
from google.oauth2.credentials import Credentials as OAuth2Credentials
from google.auth.transport.requests import Request
import httpx

log = logging.getLogger("ys2wl.auth")

SCOPES = [
    "https://www.googleapis.com/auth/youtubepartner",
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.readonly",
]

DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
TOKEN_URL = "https://oauth2.googleapis.com/token"


def get_client_config(credentials_file: str) -> Optional[dict]:
    """Extract client_id and client_secret from Google OAuth credentials JSON."""
    try:
        with open(credentials_file) as f:
            data = json.load(f)
        installed = data.get("installed") or data.get("web", {})
        return {
            "client_id": installed.get("client_id"),
            "client_secret": installed.get("client_secret"),
        }
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        log.error("Failed to read credentials file: %s", e)
        return None


def start_device_flow(client_id: str) -> dict:
    """Start OAuth 2.0 Device Flow. Returns device_code, user_code, verification_url, interval."""
    with httpx.Client() as client:
        resp = client.post(
            DEVICE_CODE_URL,
            data={
                "client_id": client_id,
                "scope": " ".join(SCOPES),
            },
        )
        resp.raise_for_status()
        return resp.json()


def poll_device_flow(
    client_id: str, client_secret: str, device_code: str
) -> tuple[Optional[Credentials], Optional[str]]:
    """Poll Google for token. Returns (credentials, None) on success, (None, status) if pending/failed."""
    with httpx.Client() as client:
        resp = client.post(
            TOKEN_URL,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "device_code": device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            },
        )
        data = resp.json()
    if "access_token" in data:
        creds = OAuth2Credentials(
            token=data["access_token"],
            refresh_token=data.get("refresh_token"),
            token_uri=TOKEN_URL,
            client_id=client_id,
            client_secret=client_secret,
            scopes=SCOPES,
        )
        return creds, None
    error = data.get("error", "unknown")
    if error in ("authorization_pending",):
        return None, "pending"
    if error == "slow_down":
        return None, "slow_down"
    return None, error


def save_credentials(credentials: Credentials, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(credentials, f)


def load_credentials(path: str) -> Optional[Credentials]:
    try:
        with open(path, "rb") as f:
            creds = pickle.load(f)
            log.debug("Loaded credentials from %s", path)
            return creds
    except FileNotFoundError:
        log.debug("No credentials file at %s", path)
        return None
    except (pickle.UnpicklingError, EOFError) as e:
        log.warning("Failed to unpickle credentials from %s: %s", path, e)
        return None


def credentials_status(credentials: Optional[Credentials]) -> dict:
    if credentials is None:
        return {"authenticated": False}
    if not credentials.valid:
        if credentials.expired and credentials.refresh_token:
            try:
                credentials.refresh(Request())
                return {
                    "authenticated": True,
                    "expires_at": str(credentials.expiry)
                    if credentials.expiry
                    else None,
                }
            except Exception:
                return {"authenticated": False}
        return {"authenticated": False}
    return {
        "authenticated": True,
        "expires_at": str(credentials.expiry) if credentials.expiry else None,
    }
