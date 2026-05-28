from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from ys2wl.core.auth import (
    get_client_config,
    start_device_flow,
    poll_device_flow,
    save_credentials,
    credentials_status,
)
from ys2wl.core.youtube import YouTubeAPIClient

router = APIRouter()


class DeviceFlowResponse(BaseModel):
    user_code: str
    verification_url: str
    device_code: str
    interval: int


class PollResponse(BaseModel):
    status: str
    error: str | None = None


class StatusResponse(BaseModel):
    authenticated: bool
    expires_at: str | None = None


def _get_state(request: Request):
    return request.app.state.ys2wl


@router.get("/auth/status", response_model=StatusResponse)
async def auth_status(request: Request):
    state = _get_state(request)
    return credentials_status(state.credentials)


@router.post("/auth/device", response_model=DeviceFlowResponse)
async def auth_device(request: Request):
    state = _get_state(request)
    config = get_client_config(state.settings.credentials_file)
    if not config or not config.get("client_id"):
        raise HTTPException(
            status_code=400, detail="credentials.json not found or invalid"
        )
    data = start_device_flow(config["client_id"])
    state.device_flow = {
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
        "device_code": data["device_code"],
    }
    return DeviceFlowResponse(
        user_code=data["user_code"],
        verification_url=data["verification_url"],
        device_code=data["device_code"],
        interval=data.get("interval", 5),
    )


@router.post("/auth/poll", response_model=PollResponse)
async def auth_poll(request: Request):
    state = _get_state(request)
    if not state.device_flow:
        raise HTTPException(
            status_code=400, detail="No active device flow. POST /auth/device first."
        )
    creds, error = poll_device_flow(
        state.device_flow["client_id"],
        state.device_flow["client_secret"],
        state.device_flow["device_code"],
    )
    if creds:
        save_credentials(creds, state.settings.pickle_file)
        state.credentials = creds
        state.youtube = YouTubeAPIClient(credentials=creds)
        state.device_flow = None
        return PollResponse(status="success")
    return PollResponse(status=error or "pending", error=error)
