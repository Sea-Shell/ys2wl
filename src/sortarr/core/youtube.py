import json
import logging
import os
import pickle
import time
from typing import Any, Optional
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sortarr.models.youtube import Channel, Playlist, Subscription, Activity

log = logging.getLogger("sortarr.youtube")
CRITICAL_STATUSES = {
    400,
    401,
    402,
    403,
    404,
    405,
    409,
    410,
    412,
    413,
    416,
    417,
    428,
    429,
    500,
    501,
    503,
}
MAX_RETRIES = 3
RETRY_DELAYS = [1, 2, 4]


class YouTubeAPIClient:
    def __init__(
        self,
        credentials: Credentials,
        use_local: bool = False,
        debug_dir: str = "debug",
    ):
        self.credentials = credentials
        self.use_local = use_local
        self.debug_dir = debug_dir
        self.api_calls: list[int] = [0]
        self._service: Any = None

    @property
    def service(self) -> Any:
        if self._service is None and not self.use_local:
            self._service = build("youtube", "v3", credentials=self.credentials)
        return self._service

    def close(self) -> None:
        if self._service is not None:
            self._service.close()
            self._service = None

    def _local_json(self, filename: str) -> Any:
        path = os.path.join(self.debug_dir, filename)
        with open(path) as f:
            return json.loads(f.read().strip())

    def _execute_with_retry(self, request: Any) -> Any:
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                response = request.execute()
                self.api_calls[0] += 1
                return response
            except HttpError as err:
                last_error = err
                if err.resp.status in CRITICAL_STATUSES:
                    log.critical("Critical HTTP error: %s", err)
                    raise
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAYS[attempt] + (time.time() % 1)
                    log.warning(
                        "HTTP %d, retrying in %.1fs (attempt %d/%d)",
                        err.resp.status,
                        delay,
                        attempt + 1,
                        MAX_RETRIES,
                    )
                    time.sleep(delay)
        log.error("Max retries exhausted: %s", last_error)
        raise last_error

    def get_subscriptions(self) -> list[Subscription]:
        if self.use_local:
            data = self._local_json("subscriptions_list.json")
        else:
            items = []
            next_page: Optional[str] = None
            while True:
                req = self.service.subscriptions().list(
                    part="snippet",
                    maxResults=50,
                    mine=True,
                    order="alphabetical",
                    pageToken=next_page,
                )
                resp = self._execute_with_retry(req)
                items.extend(resp.get("items", []))
                next_page = resp.get("nextPageToken")
                if not next_page:
                    break
            data = {"items": items}
        return [
            Subscription(
                id=item["snippet"]["resourceId"]["channelId"],
                title=item["snippet"]["title"],
                channel_id=item["snippet"]["resourceId"]["channelId"],
            )
            for item in data.get("items", [])
        ]

    def get_channel_id(self) -> list[Channel]:
        if self.use_local:
            data = self._local_json("channels_list.json")
        else:
            req = self.service.channels().list(part="snippet", mine=True)
            resp = self._execute_with_retry(req)
            data = resp
        return [
            Channel(id=item["id"], title=item["snippet"]["title"])
            for item in data.get("items", [])
        ]

    def get_subscription_activity(
        self, channel_id: str, published_after: Optional[str] = None, limit: int = 50
    ) -> list[Activity]:
        if self.use_local:
            data = self._local_json("subscription_activity_list.json")
        else:
            items = []
            next_page: Optional[str] = None
            while True:
                req = self.service.activities().list(
                    part="snippet,contentDetails",
                    maxResults=limit,
                    publishedAfter=published_after,
                    uploadType="upload",
                    channelId=channel_id,
                    pageToken=next_page,
                )
                resp = self._execute_with_retry(req)
                items.extend(resp.get("items", []))
                next_page = resp.get("nextPageToken")
                if not next_page:
                    break
            data = {"items": items}
        activities = []
        for item in data.get("items", []):
            video_type = item["snippet"]["type"]
            if video_type not in ("upload", "playlistItem"):
                continue
            video_id = ""
            if video_type == "upload":
                video_id = (
                    item.get("contentDetails", {}).get("upload", {}).get("videoId", "")
                )
            else:
                video_id = (
                    item.get("contentDetails", {})
                    .get("playlistItem", {})
                    .get("resourceId", {})
                    .get("videoId", "")
                )
            if not video_id:
                continue
            activities.append(
                Activity(
                    video_id=video_id,
                    title=item["snippet"]["title"],
                    published_at=item["snippet"]["publishedAt"],
                    video_type=video_type,
                )
            )
        return activities

    def get_video_duration(self, video_id: str) -> int:
        if self.use_local:
            data = self._local_json("video.json")
        else:
            req = self.service.videos().list(part="contentDetails", id=video_id)
            resp = self._execute_with_retry(req)
            data = resp
        items = data.get("items", [])
        if not items:
            return 0
        duration = items[0].get("contentDetails", {}).get("duration", "PT0S")
        return self._iso8601_to_seconds(duration)

    def get_user_playlists(self, channel_id: str) -> list[Playlist]:
        if self.use_local:
            data = self._local_json("user_playlists_list.json")
        else:
            items = []
            next_page: Optional[str] = None
            while True:
                req = self.service.playlists().list(
                    part="snippet",
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page,
                )
                resp = self._execute_with_retry(req)
                items.extend(resp.get("items", []))
                next_page = resp.get("nextPageToken")
                if not next_page:
                    break
            data = {"items": items}
        return [
            Playlist(id=item["id"], title=item["snippet"]["title"])
            for item in data.get("items", [])
        ]

    def get_playlist(self, playlist_id: str) -> list[dict]:
        if self.use_local:
            data = self._local_json("user_playlist.json")
        else:
            items = []
            next_page: Optional[str] = None
            while True:
                req = self.service.playlistItems().list(
                    part="snippet",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page,
                )
                resp = self._execute_with_retry(req)
                items.extend(resp.get("items", []))
                next_page = resp.get("nextPageToken")
                if not next_page:
                    break
            data = {"items": items}
        return data.get("items", [])

    def add_to_playlist(self, playlist_id: str, video_id: str) -> bool:
        if self.use_local:
            return True
        body = {
            "kind": "youtube#playlistItem",
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {"kind": "youtube#video", "videoId": video_id},
            },
        }
        req = self.service.playlistItems().insert(part="snippet", body=body)
        try:
            self._execute_with_retry(req)
            return True
        except HttpError as err:
            log.error(
                "Failed to add video %s to playlist %s: %s", video_id, playlist_id, err
            )
            return False

    @staticmethod
    def _iso8601_to_seconds(duration_str: str) -> int:
        import re

        match = re.match(
            r"P(?:(?P<days>\d+)D)?T?(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?",
            duration_str,
        )
        if not match:
            return 0
        days = int(match.group("days")) if match.group("days") else 0
        hours = int(match.group("hours")) if match.group("hours") else 0
        minutes = int(match.group("minutes")) if match.group("minutes") else 0
        seconds = int(match.group("seconds")) if match.group("seconds") else 0
        return days * 86400 + hours * 3600 + minutes * 60 + seconds


def authenticate(
    credentials_file: str,
    pickle_credentials: str,
    scopes_list: list[str],
    no_webbrowser: bool = False,
) -> Credentials:
    credentials = None
    if os.path.exists(pickle_credentials):
        log.debug("Loading credentials from %s", pickle_credentials)
        with open(pickle_credentials, "rb") as token:
            credentials = pickle.load(token)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            log.debug("Refreshing access token")
            credentials.refresh(Request())
        else:
            log.debug("Fetching new tokens")
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, scopes=scopes_list
            )
            flow.run_local_server(
                port=8080, prompt="consent", open_browser=not no_webbrowser
            )
            credentials = flow.credentials
            with open(pickle_credentials, "wb") as f:
                log.debug("Saving credentials to pickle file")
                pickle.dump(credentials, f)
    return credentials
