import logging
import uuid
from typing import List
from fastapi import APIRouter, HTTPException, Request
from sortarr.api.models import (
    PipelineCreate,
    PipelineUpdate,
    PipelineResponse,
    IgnoreListResponse,
    IgnoreListCreate,
    IgnoreListEntryCreate,
    PlaylistResponse,
)
from sortarr.api.deps import get_state, require_youtube
from sortarr.db.repository import pipeline as pl, ignore_lists as il, videos as v

log = logging.getLogger("sortarr.api.pipelines")
router = APIRouter()


def _get_state(request: Request):
    return request.app.state.sortarr


# ── Pipelines ─────────────────────────────────────────────────────


def _enrich_pipeline(state, db_pipeline: dict) -> dict:
    data = dict(db_pipeline)
    data["ignore_list_ids"] = pl.get_pipeline_ignore_list_ids(
        state.db_con, db_pipeline["id"]
    )
    data["selectors"] = pl.get_pipeline_selectors(state.db_con, db_pipeline["id"])
    return data


@router.get("/pipelines", response_model=List[PipelineResponse])
async def list_pipelines(request: Request):
    state = _get_state(request)
    pipelines = pl.get_pipelines(state.db_con)
    return [_enrich_pipeline(state, p) for p in pipelines]


@router.get("/pipelines/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(pipeline_id: str, request: Request):
    state = _get_state(request)
    pipelines = pl.get_pipelines(state.db_con)
    match = [p for p in pipelines if p["id"] == pipeline_id]
    if not match:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return _enrich_pipeline(state, match[0])


@router.post("/pipelines", response_model=PipelineResponse, status_code=201)
async def create_pipeline(pipeline: PipelineCreate, request: Request):
    state = _get_state(request)
    pid = str(uuid.uuid4())
    ok = pl.create_pipeline(
        state.db_con,
        pid,
        pipeline.name,
        pipeline.destination_playlist_id,
        pipeline.destination_playlist_title,
        selector_mode=pipeline.selector_mode,
        duration_min_seconds=pipeline.duration_min_seconds,
        duration_max_seconds=pipeline.duration_max_seconds,
        check_db_exists=pipeline.check_db_exists,
        check_title_similarity=pipeline.check_title_similarity,
        compare_distance=pipeline.compare_distance,
        subscription_scope=pipeline.subscription_scope,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create pipeline")
    pipelines = pl.get_pipelines(state.db_con)
    match = [p for p in pipelines if p["id"] == pid]
    return _enrich_pipeline(state, match[0])


@router.put("/pipelines/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(pipeline_id: str, update: PipelineUpdate, request: Request):
    state = _get_state(request)
    updates = {k: v for k, v in update.model_dump(exclude_none=True).items()}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    pl.update_pipeline(state.db_con, pipeline_id, **updates)
    pipelines = pl.get_pipelines(state.db_con)
    match = [p for p in pipelines if p["id"] == pipeline_id]
    if not match:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return _enrich_pipeline(state, match[0])


@router.delete("/pipelines/{pipeline_id}", status_code=204)
async def delete_pipeline(pipeline_id: str, request: Request):
    state = _get_state(request)
    pl.delete_pipeline(state.db_con, pipeline_id)


# ── Pipeline ignore lists ─────────────────────────────────────────


@router.put("/pipelines/{pipeline_id}/ignore-lists", status_code=204)
async def set_pipeline_ignore_lists(pipeline_id: str, body: dict, request: Request):
    state = _get_state(request)
    list_ids = body.get("ignore_list_ids", [])
    pl.set_pipeline_ignore_lists(state.db_con, pipeline_id, list_ids)


# ── Pipeline selectors ────────────────────────────────────────────


@router.put("/pipelines/{pipeline_id}/selectors", status_code=204)
async def set_pipeline_selectors(pipeline_id: str, body: dict, request: Request):
    state = _get_state(request)
    selectors = body.get("selectors", [])
    pl.set_pipeline_selectors(state.db_con, pipeline_id, selectors)


# ── Pipeline subscriptions (scope=selected) ────────────────────────


@router.put("/pipelines/{pipeline_id}/subscriptions", status_code=204)
async def set_pipeline_subscriptions(pipeline_id: str, body: dict, request: Request):
    state = _get_state(request)
    sub_ids = body.get("subscription_ids", [])
    pl.set_pipeline_subscriptions(state.db_con, pipeline_id, sub_ids)


# ── Ignore Lists ──────────────────────────────────────────────────


@router.get("/ignore-lists", response_model=List[IgnoreListResponse])
async def list_ignore_lists(request: Request):
    state = _get_state(request)
    lists = il.get_ignore_lists(state.db_con)
    result = []
    for lst in lists:
        entries = il.get_ignore_list_entries(state.db_con, lst["id"])
        result.append({**lst, "entries": entries})  # list of str values
    return result


@router.get("/ignore-lists/{list_id}/entries", status_code=200)
async def get_ignore_list_entries(list_id: str, request: Request):
    """Return entries with IDs for UI deletion."""
    state = _get_state(request)
    cursor = state.db_con.execute(
        "SELECT id, value FROM ignore_list_entries WHERE ignore_list_id = ? ORDER BY id",
        (list_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


@router.post("/ignore-lists", response_model=IgnoreListResponse, status_code=201)
async def create_ignore_list(body: IgnoreListCreate, request: Request):
    state = _get_state(request)
    if body.list_type not in ("word", "video", "subscription"):
        raise HTTPException(status_code=400, detail="Invalid list_type")
    lid = str(uuid.uuid4())
    ok = il.create_ignore_list(state.db_con, lid, body.name, body.list_type)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create ignore list")
    lst = il.get_ignore_list(state.db_con, lid)
    if not lst:
        raise HTTPException(
            status_code=500, detail="Ignore list not found after create"
        )
    entries = il.get_ignore_list_entries(state.db_con, lid)
    return {**lst, "entries": entries}


@router.put("/ignore-lists/{list_id}", response_model=IgnoreListResponse)
async def update_ignore_list(list_id: str, body: IgnoreListCreate, request: Request):
    state = _get_state(request)
    if body.list_type not in ("word", "video", "subscription"):
        raise HTTPException(status_code=400, detail="Invalid list_type")
    ok = il.update_ignore_list(state.db_con, list_id, body.name)
    if not ok:
        raise HTTPException(status_code=404, detail="Ignore list not found")
    lst = il.get_ignore_list(state.db_con, list_id)
    if not lst:
        raise HTTPException(status_code=404, detail="Ignore list not found")
    entries = il.get_ignore_list_entries(state.db_con, list_id)
    return {**lst, "entries": entries}


@router.delete("/ignore-lists/{list_id}", status_code=204)
async def delete_ignore_list(list_id: str, request: Request):
    state = _get_state(request)
    il.delete_ignore_list(state.db_con, list_id)


@router.post("/ignore-lists/{list_id}/entries", status_code=201)
async def add_ignore_list_entry(
    list_id: str, body: IgnoreListEntryCreate, request: Request
):
    state = _get_state(request)
    lst = il.get_ignore_list(state.db_con, list_id)
    if not lst:
        raise HTTPException(status_code=404, detail="Ignore list not found")
    eid = str(uuid.uuid4())
    ok = il.add_ignore_list_entry(state.db_con, eid, list_id, body.value)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to add entry")
    return {"id": eid, "value": body.value}


@router.delete("/ignore-lists/{list_id}/entries/{entry_id}", status_code=204)
async def remove_ignore_list_entry(list_id: str, entry_id: str, request: Request):
    state = _get_state(request)
    il.remove_ignore_list_entry(state.db_con, entry_id)


@router.get("/playlists", response_model=List[PlaylistResponse])
async def list_playlists(request: Request):
    """Return user's YouTube playlists for pipeline config."""
    state = get_state(request)
    youtube = require_youtube(state)
    channel = v.get_channel(state.db_con)
    channel_id = None
    if channel:
        channel_id = channel["id"]
    else:
        channels = youtube.get_channel_id()
        if channels:
            channel_id = channels[0].id
    if not channel_id:
        raise HTTPException(status_code=404, detail="No channel found")
    playlists = youtube.get_user_playlists(channel_id)
    return [PlaylistResponse(id=p.id, title=p.title) for p in playlists]


# ── Video Lookup ───────────────────────────────────────────────────────


@router.get("/videos/{video_id}")
async def get_video_by_id(video_id: str, request: Request):
    """Look up video details by YouTube video ID across all pipelines."""
    state = _get_state(request)
    result = v.get_video_by_id(state.db_con, video_id)
    if not result:
        raise HTTPException(status_code=404, detail="Video not found in database")
    return {
        "video_id": video_id,
        "title": result.get("title"),
        "timestamp": result.get("timestamp"),
        "subscription_id": result.get("subscriptionId"),
        "playlist_id": result.get("playlistId"),
        "duration_seconds": result.get("duration_seconds"),
        "route_rule": result.get("route_rule"),
        "pipeline_id": result.get("pipeline_id"),
    }
