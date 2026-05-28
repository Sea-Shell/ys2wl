from typing import List
from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter()


class SubscriptionResponse(BaseModel):
    id: str
    title: str
    channel_id: str


class ActivityResponse(BaseModel):
    video_id: str
    title: str
    published_at: str
    video_type: str


def _get_state(request: Request):
    return request.app.state.ys2wl


@router.get("/subscriptions", response_model=List[SubscriptionResponse])
async def list_subscriptions(request: Request):
    state = _get_state(request)
    subs = state.youtube.get_subscriptions()
    return [
        SubscriptionResponse(id=s.id, title=s.title, channel_id=s.channel_id)
        for s in subs
    ]


@router.get(
    "/subscriptions/{channel_id}/activity", response_model=List[ActivityResponse]
)
async def get_subscription_activity(channel_id: str, request: Request):
    state = _get_state(request)
    activities = state.youtube.get_subscription_activity(channel_id)
    return [
        ActivityResponse(
            video_id=a.video_id,
            title=a.title,
            published_at=a.published_at,
            video_type=a.video_type,
        )
        for a in activities
    ]
