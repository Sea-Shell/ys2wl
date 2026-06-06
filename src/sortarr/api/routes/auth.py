import logging
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from fastapi.responses import RedirectResponse

from sortarr.core.auth import (
    get_client_config,
    get_authorization_url,
    exchange_code_for_tokens,
    save_credentials,
    credentials_status,
)
from sortarr.core.youtube import YouTubeAPIClient

log = logging.getLogger("sortarr.auth_routes")

router = APIRouter()


class AuthLoginResponse(BaseModel):
    authorization_url: str


class StatusResponse(BaseModel):
    authenticated: bool
    expires_at: str | None = None


def _get_state(request: Request):
    return request.app.state.sortarr


@router.get("/auth/status", response_model=StatusResponse)
async def auth_status(request: Request):
    state = _get_state(request)
    return credentials_status(state.credentials)


@router.get("/auth/login", response_model=AuthLoginResponse)
async def auth_login(request: Request):
    state = _get_state(request)
    config = get_client_config(state.db_con)
    if not config or not config.get("client_id"):
        raise HTTPException(
            status_code=400, detail="credentials.json not found or invalid"
        )
    redirect_uri = f"{state.settings.public_url}/api/auth/callback"
    url = get_authorization_url(config, redirect_uri)
    return AuthLoginResponse(authorization_url=url)


@router.get("/auth/callback")
async def auth_callback(
    request: Request, code: str | None = None, error: str | None = None
):
    state = _get_state(request)
    if error:
        log.warning("OAuth callback error: %s", error)
        return RedirectResponse(url=f"{state.settings.public_url}/ui/index.html#auth")
    if not code:
        raise HTTPException(status_code=400, detail="Missing authorisation code")
    config = get_client_config(state.db_con)
    if not config:
        raise HTTPException(status_code=400, detail="No client config found")
    redirect_uri = f"{state.settings.public_url}/api/auth/callback"
    creds = exchange_code_for_tokens(config, code, redirect_uri)
    if not creds:
        raise HTTPException(
            status_code=400, detail="Failed to exchange authorisation code"
        )
    save_credentials(state.db_con, creds)
    state.credentials = creds
    state.youtube = YouTubeAPIClient(credentials=creds)
    return RedirectResponse(url=f"{state.settings.public_url}/ui/index.html#auth")
