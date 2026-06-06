import base64
import json
import logging
import pickle
import sqlite3
from typing import Optional
from urllib.parse import urlencode
from google.auth.credentials import Credentials
from google.oauth2.credentials import Credentials as OAuth2Credentials
from google.auth.transport.requests import Request
import httpx
from sortarr.db.repository import config as repo

log = logging.getLogger("sortarr.auth")

SCOPES = [
    "https://www.googleapis.com/auth/youtubepartner",
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.readonly",
]

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"


def get_client_config(db_con: sqlite3.Connection) -> Optional[dict]:
    """Extract client_id and client_secret from Google OAuth credentials JSON in DB."""
    try:
        raw = repo.get_config(db_con, "credentials_file")
        if not raw:
            log.error("No credentials_file in app_config")
            return None
        data = json.loads(raw)
        installed = data.get("installed") or data.get("web", {})
        return {
            "client_id": installed.get("client_id"),
            "client_secret": installed.get("client_secret"),
        }
    except (KeyError, json.JSONDecodeError) as e:
        log.error("Failed to read credentials from DB: %s", e)
        return None


def get_authorization_url(client_config: dict, redirect_uri: str) -> str:
    """Build Google OAuth authorization URL for browser redirect."""
    params = {
        "client_id": client_config["client_id"],
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
    }
    return f"{AUTH_URL}?{urlencode(params)}"


def exchange_code_for_tokens(
    client_config: dict, code: str, redirect_uri: str
) -> Optional[Credentials]:
    """Exchange authorization code for OAuth credentials."""
    with httpx.Client() as client:
        resp = client.post(
            TOKEN_URL,
            data={
                "client_id": client_config["client_id"],
                "client_secret": client_config["client_secret"],
                "code": code,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        data = resp.json()
    if "access_token" not in data:
        log.error("Token exchange failed: %s", data.get("error", "unknown"))
        return None
    return OAuth2Credentials(
        token=data["access_token"],
        refresh_token=data.get("refresh_token"),
        token_uri=TOKEN_URL,
        client_id=client_config["client_id"],
        client_secret=client_config["client_secret"],
        scopes=SCOPES,
    )


def save_credentials(db_con: sqlite3.Connection, credentials: Credentials) -> None:
    blob = base64.b64encode(pickle.dumps(credentials)).decode("ascii")
    repo.set_config(db_con, "credentials_pickle", blob)


def load_credentials(db_con: sqlite3.Connection) -> Optional[Credentials]:
    try:
        raw = repo.get_config(db_con, "credentials_pickle")
        if not raw:
            log.debug("No credentials_pickle in app_config")
            return None
        creds = pickle.loads(base64.b64decode(raw))
        log.debug("Loaded credentials from DB")
        return creds
    except (pickle.UnpicklingError, EOFError, ValueError) as e:
        log.warning("Failed to unpickle credentials from DB: %s", e)
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
