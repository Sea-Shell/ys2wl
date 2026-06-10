import logging
from typing import List
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sortarr.api.deps import get_state, require_youtube

log = logging.getLogger("sortarr.api.subscriptions")
router = APIRouter()


class SubscriptionResponse(BaseModel):
    id: str
    title: str
    channel_id: str
    added_to_playlist_count: int = 0


class ActivityResponse(BaseModel):
    video_id: str
    title: str
    published_at: str
    video_type: str


def _get_added_count(con, sub_id: str) -> int:
    row = con.execute(
        "SELECT added_to_playlist_count FROM subscription WHERE id = ?", (sub_id,)
    ).fetchone()
    return (
        row["added_to_playlist_count"] if row and row["added_to_playlist_count"] else 0
    )


@router.get("/subscriptions", response_model=List[SubscriptionResponse])
async def list_subscriptions(request: Request):
    state = get_state(request)
    youtube = require_youtube(state)
    try:
        subs = youtube.get_subscriptions()
    except Exception as e:
        log.error("Failed to list subscriptions: %s", e)
        raise HTTPException(status_code=502, detail=str(e))
    return [
        SubscriptionResponse(
            id=s.id,
            title=s.title,
            channel_id=s.channel_id,
            added_to_playlist_count=_get_added_count(state.db_con, s.id),
        )
        for s in subs
    ]


@router.get(
    "/subscriptions/{channel_id}/activity", response_model=List[ActivityResponse]
)
async def get_subscription_activity(channel_id: str, request: Request):
    state = get_state(request)
    youtube = require_youtube(state)
    try:
        activities = youtube.get_subscription_activity(channel_id)
    except Exception as e:
        log.error("Failed to get activity for channel %s: %s", channel_id, e)
        raise HTTPException(status_code=502, detail=str(e))
    return [
        ActivityResponse(
            video_id=a.video_id,
            title=a.title,
            published_at=a.published_at,
            video_type=a.video_type,
        )
        for a in activities
    ]
