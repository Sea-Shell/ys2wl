from fastapi import APIRouter, Request
from ys2wl.api.models import ConfigResponse, ConfigUpdate
from ys2wl.db import repository as repo

router = APIRouter()


def _get_state(request: Request):
    return request.app.state.ys2wl


@router.get("/config", response_model=ConfigResponse)
async def get_config(request: Request):
    state = _get_state(request)
    s = state.settings
    return ConfigResponse(
        schedule=s.schedule,
        compare_distance=s.compare_distance,
        reprocess_days=s.reprocess_days,
        playlist_sleep=s.playlist_sleep,
        subscription_sleep=s.subscription_sleep,
        pipeline_concurrency=s.pipeline_concurrency,
        activity_limit=s.activity_limit,
        subscription_limit=s.subscription_limit,
        log_level=s.log_level,
    )


@router.put("/config", response_model=ConfigResponse)
async def update_config(update: ConfigUpdate, request: Request):
    state = _get_state(request)
    s = state.settings
    for k, v in update.model_dump(exclude_none=True).items():
        if hasattr(s, k):
            setattr(s, k, v)
            repo.set_config(state.db_con, k, str(v))
    return await get_config(request)
