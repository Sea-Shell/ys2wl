from typing import List
from fastapi import APIRouter, HTTPException, Request
from sortarr.api.models import (
    ConfigResponse,
    ConfigUpdate,
    IgnoreEntryCreate,
    IgnoreEntryUpdate,
    IgnoreEntryResponse,
)
from sortarr.db.repository import config as repo

router = APIRouter()


def _get_state(request: Request):
    return request.app.state.sortarr


def _get_db_val(state, key: str, fallback):
    db_val = repo.get_config(state.db_con, key)
    return db_val if db_val is not None else fallback


@router.get("/config", response_model=ConfigResponse)
async def get_config(request: Request):
    state = _get_state(request)
    s = state.settings
    return ConfigResponse(
        schedule=_get_db_val(state, "schedule", s.schedule),
        compare_distance=int(
            _get_db_val(state, "compare_distance", s.compare_distance)
        ),
        reprocess_days=int(_get_db_val(state, "reprocess_days", s.reprocess_days)),
        playlist_sleep=int(_get_db_val(state, "playlist_sleep", s.playlist_sleep)),
        subscription_sleep=int(
            _get_db_val(state, "subscription_sleep", s.subscription_sleep)
        ),
        pipeline_concurrency=int(
            _get_db_val(state, "pipeline_concurrency", s.pipeline_concurrency)
        ),
        activity_limit=int(_get_db_val(state, "activity_limit", s.activity_limit)),
        subscription_limit=int(
            _get_db_val(state, "subscription_limit", s.subscription_limit)
        ),
        log_level=_get_db_val(state, "log_level", s.log_level),
        published_after=_get_db_val(state, "published_after", s.published_after),
        no_webbrowser=_get_db_val(state, "no_webbrowser", str(s.no_webbrowser))
        == "True",
        credentials_file=_get_db_val(state, "credentials_file", s.credentials_file),
        public_url=_get_db_val(state, "public_url", s.public_url),
    )


@router.put("/config", response_model=ConfigResponse)
async def update_config(update: ConfigUpdate, request: Request):
    state = _get_state(request)
    s = state.settings
    for k, v in update.model_dump(exclude_none=True).items():
        # Migrate legacy key
        if k == "client_secret_json":
            k = "credentials_file"
        if hasattr(s, k):
            setattr(s, k, v)
            repo.set_config(state.db_con, k, str(v))
    return await get_config(request)


@router.get("/config/ignores", response_model=List[IgnoreEntryResponse])
async def list_ignores(type: str, request: Request):
    state = _get_state(request)
    if type not in ("subscription", "video", "words"):
        raise HTTPException(status_code=400, detail="Invalid type")
    return repo.get_ignore_entries(state.db_con, type)


@router.post("/config/ignores", response_model=IgnoreEntryResponse, status_code=201)
async def create_ignore(entry: IgnoreEntryCreate, request: Request):
    state = _get_state(request)
    if entry.type not in ("subscription", "video", "words"):
        raise HTTPException(status_code=400, detail="Invalid type")
    entry_id = repo.add_ignore_entry(state.db_con, entry.type, entry.pattern)
    if entry_id is None:
        raise HTTPException(status_code=500, detail="Failed to create entry")
    entries = repo.get_ignore_entries(state.db_con, entry.type)
    return next(e for e in entries if e["id"] == entry_id)


@router.put("/config/ignores/{entry_id:int}", response_model=IgnoreEntryResponse)
async def update_ignore(entry_id: int, update: IgnoreEntryUpdate, request: Request):
    state = _get_state(request)
    if not repo.update_ignore_entry(state.db_con, entry_id, update.pattern):
        raise HTTPException(status_code=404, detail="Entry not found")
    for t in ("subscription", "video", "words"):
        entries = repo.get_ignore_entries(state.db_con, t)
        for e in entries:
            if e["id"] == entry_id:
                return e
    raise HTTPException(status_code=404, detail="Entry not found")


@router.delete("/config/ignores/{entry_id:int}", status_code=204)
async def delete_ignore(entry_id: int, request: Request):
    state = _get_state(request)
    if not repo.delete_ignore_entry(state.db_con, entry_id):
        raise HTTPException(status_code=404, detail="Entry not found")
