from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Channel:
    id: str
    title: str


@dataclass
class Playlist:
    id: str
    title: str


@dataclass
class Subscription:
    id: str
    title: str
    channel_id: str = field(repr=False)


@dataclass
class Activity:
    video_id: str
    title: str
    published_at: str
    video_type: str  # "upload" or "playlistItem"


@dataclass
class Video:
    video_id: str
    title: str
    duration_seconds: int = 0
    subscription_id: str = ""
    playlist_id: str = ""
    route_rule: str = ""


@dataclass
class RoutingRule:
    id: int = 0
    name: str = ""
    priority: int = 0
    field: Optional[str] = None
    operator: str = "contains"
    pattern: Optional[str] = None
    destination_playlist_id: str = ""
    destination_playlist_title: str = ""
    enabled: bool = True
