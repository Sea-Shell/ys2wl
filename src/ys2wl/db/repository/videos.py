import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ys2wl.db.repository.videos")

__all__ = [
    "video_exists",
    "insert_video",
    "get_all_video_titles",
    "insert_channel",
    "get_channel",
    "insert_playlist",
    "get_playlist",
    "insert_subscription",
    "get_subscription_timestamp",
    "get_last_run",
    "set_last_run",
    "cache_activity",
    "get_cached_activities",
    "clear_activity_cache",
    "video_exists_for_pipeline",
    "get_all_video_titles_for_pipeline",
]


def video_exists(con: sqlite3.Connection, video_id: str) -> bool:
    cursor = con.execute("SELECT 1 FROM videos WHERE videoId = ? LIMIT 1", (video_id,))
    return cursor.fetchone() is not None


def insert_video(
    con: sqlite3.Connection,
    video_id: str,
    timestamp: str,
    title: str,
    subscription_id: str,
    playlist_id: str = "",
    duration_seconds: int = 0,
    route_rule: str = "",
    pipeline_id: str = "",
) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO videos (videoId, timestamp, title, subscriptionId, playlistId, duration_seconds, route_rule, pipeline_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                video_id,
                timestamp,
                title,
                subscription_id,
                playlist_id,
                duration_seconds,
                route_rule,
                pipeline_id,
            ),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to insert video: %s", err)
        return False


def get_all_video_titles(con: sqlite3.Connection) -> list[tuple[str, str]]:
    cursor = con.execute("SELECT videoId, title FROM videos")
    return cursor.fetchall()


def insert_channel(con: sqlite3.Connection, channel_id: str, title: str) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO channel (id, title) VALUES (?, ?)",
            (channel_id, title),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to insert channel: %s", err)
        return False


def get_channel(con: sqlite3.Connection) -> Optional[dict]:
    cursor = con.execute("SELECT id, title FROM channel LIMIT 1")
    row = cursor.fetchone()
    if row:
        return {"id": row["id"], "title": row["title"]}
    return None


def insert_playlist(con: sqlite3.Connection, playlist_id: str, title: str) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO playlist (id, title) VALUES (?, ?)",
            (playlist_id, title),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to insert playlist: %s", err)
        return False


def get_playlist(con: sqlite3.Connection) -> Optional[dict]:
    cursor = con.execute("SELECT id, title FROM playlist LIMIT 1")
    row = cursor.fetchone()
    if row:
        return {"id": row["id"], "title": row["title"]}
    return None


def insert_subscription(
    con: sqlite3.Connection, sub_id: str, title: str, timestamp: str
) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO subscription (id, title, timestamp) VALUES (?, ?, ?)",
            (sub_id, title, timestamp),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to insert subscription: %s", err)
        return False


def get_subscription_timestamp(con: sqlite3.Connection, sub_id: str) -> Optional[str]:
    cursor = con.execute(
        "SELECT timestamp FROM subscription WHERE id = ? LIMIT 1", (sub_id,)
    )
    row = cursor.fetchone()
    return row["timestamp"] if row else None


def get_last_run(con: sqlite3.Connection) -> Optional[str]:
    cursor = con.execute("SELECT timestamp FROM last_run WHERE id = 1 LIMIT 1")
    row = cursor.fetchone()
    return row["timestamp"] if row else None


def set_last_run(con: sqlite3.Connection, timestamp: str) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO last_run (id, timestamp) VALUES (1, ?)",
            (timestamp,),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to set last_run: %s", err)
        return False


def cache_activity(
    con: sqlite3.Connection,
    sub_id: str,
    video_id: str,
    title: str,
    channel_id: str,
    channel_title: str,
    video_type: str = "",
    description: str = "",
    published_at: str = "",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    con.execute(
        "INSERT OR REPLACE INTO activity_cache (subscription_id, video_id, title, channel_id, channel_title, "
        "video_type, description, published_at, cached_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            sub_id,
            video_id,
            title,
            channel_id,
            channel_title,
            video_type,
            description,
            published_at,
            now,
        ),
    )
    con.commit()


def get_cached_activities(con: sqlite3.Connection, sub_id: str) -> list[dict]:
    cursor = con.execute(
        "SELECT subscription_id, video_id, title, channel_id, channel_title, video_type, description, published_at "
        "FROM activity_cache WHERE subscription_id = ? ORDER BY published_at ASC",
        (sub_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def clear_activity_cache(con: sqlite3.Connection) -> None:
    try:
        con.execute("DELETE FROM activity_cache")
        con.commit()
    except sqlite3.Error as err:
        log.error("Failed to clear activity cache: %s", err)


def video_exists_for_pipeline(
    con: sqlite3.Connection, video_id: str, pipeline_id: str
) -> bool:
    cursor = con.execute(
        "SELECT 1 FROM videos WHERE videoId = ? AND pipeline_id = ? LIMIT 1",
        (video_id, pipeline_id),
    )
    return cursor.fetchone() is not None


def get_all_video_titles_for_pipeline(
    con: sqlite3.Connection, pipeline_id: str
) -> list[tuple[str, str]]:
    cursor = con.execute(
        "SELECT videoId, title FROM videos WHERE pipeline_id = ?", (pipeline_id,)
    )
    return cursor.fetchall()
