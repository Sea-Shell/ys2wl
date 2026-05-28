import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ys2wl.db.repository")


# -- videos --
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
) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO videos (videoId, timestamp, title, subscriptionId, playlistId, duration_seconds, route_rule) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                video_id,
                timestamp,
                title,
                subscription_id,
                playlist_id,
                duration_seconds,
                route_rule,
            ),
        )
        return True
    except sqlite3.Error as err:
        log.error("Failed to insert video: %s", err)
        return False


def get_all_video_titles(con: sqlite3.Connection) -> list[tuple[str, str]]:
    cursor = con.execute("SELECT videoId, title FROM videos")
    return cursor.fetchall()


# -- channel --
def insert_channel(con: sqlite3.Connection, channel_id: str, title: str) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO channel (id, title) VALUES (?, ?)",
            (channel_id, title),
        )
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


# -- playlist --
def insert_playlist(con: sqlite3.Connection, playlist_id: str, title: str) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO playlist (id, title) VALUES (?, ?)",
            (playlist_id, title),
        )
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


# -- subscription --
def insert_subscription(
    con: sqlite3.Connection, sub_id: str, title: str, timestamp: str
) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO subscription (id, title, timestamp) VALUES (?, ?, ?)",
            (sub_id, title, timestamp),
        )
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


# -- last_run --
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
        return True
    except sqlite3.Error as err:
        log.error("Failed to set last_run: %s", err)
        return False


# -- routing_rules --
def get_routing_rules(con: sqlite3.Connection) -> list[dict]:
    cursor = con.execute(
        "SELECT id, name, priority, field, operator, pattern, destination_playlist_id, destination_playlist_title, enabled "
        "FROM routing_rules WHERE enabled = 1 ORDER BY priority DESC"
    )
    return [dict(row) for row in cursor.fetchall()]


def create_routing_rule(
    con: sqlite3.Connection,
    name: str,
    priority: int,
    field: Optional[str],
    operator: str,
    pattern: Optional[str],
    dest_playlist_id: str,
    dest_playlist_title: str,
) -> Optional[int]:
    now = datetime.now(timezone.utc).isoformat()
    try:
        cursor = con.execute(
            "INSERT INTO routing_rules (name, priority, field, operator, pattern, destination_playlist_id, destination_playlist_title, enabled, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
            (
                name,
                priority,
                field,
                operator,
                pattern,
                dest_playlist_id,
                dest_playlist_title,
                now,
                now,
            ),
        )
        return cursor.lastrowid
    except sqlite3.Error as err:
        log.error("Failed to create routing rule: %s", err)
        return None


def update_routing_rule(con: sqlite3.Connection, rule_id: int, **kwargs) -> bool:
    allowed = {
        "name",
        "priority",
        "field",
        "operator",
        "pattern",
        "destination_playlist_id",
        "destination_playlist_title",
        "enabled",
    }
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False
    updates["updated_at"] = datetime.now(timezone.utc).isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [rule_id]
    try:
        con.execute(f"UPDATE routing_rules SET {set_clause} WHERE id = ?", values)
        return True
    except sqlite3.Error as err:
        log.error("Failed to update routing rule: %s", err)
        return False


def delete_routing_rule(con: sqlite3.Connection, rule_id: int) -> bool:
    try:
        con.execute("DELETE FROM routing_rules WHERE id = ?", (rule_id,))
        return True
    except sqlite3.Error as err:
        log.error("Failed to delete routing rule: %s", err)
        return False


# -- pipeline_runs --
def create_pipeline_run(
    con: sqlite3.Connection, trigger: str = "scheduled"
) -> Optional[int]:
    now = datetime.now(timezone.utc).isoformat()
    try:
        cursor = con.execute(
            "INSERT INTO pipeline_runs (started_at, status, trigger) VALUES (?, 'running', ?)",
            (now, trigger),
        )
        return cursor.lastrowid
    except sqlite3.Error as err:
        log.error("Failed to create pipeline run: %s", err)
        return None


def finish_pipeline_run(con: sqlite3.Connection, run_id: int, summary: dict) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    try:
        con.execute(
            "UPDATE pipeline_runs SET finished_at = ?, status = ?, subscriptions_processed = ?, subscriptions_skipped = ?, "
            "videos_added = ?, videos_skipped = ?, errors = ?, error_message = ? WHERE id = ?",
            (
                now,
                summary.get("status", "completed"),
                summary.get("subscriptions_processed", 0),
                summary.get("subscriptions_skipped", 0),
                summary.get("videos_added", 0),
                summary.get("videos_skipped", 0),
                summary.get("errors", 0),
                summary.get("error_message", ""),
                run_id,
            ),
        )
        return True
    except sqlite3.Error as err:
        log.error("Failed to finish pipeline run: %s", err)
        return False


def get_pipeline_runs(con: sqlite3.Connection, limit: int = 20) -> list[dict]:
    cursor = con.execute(
        "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT ?", (limit,)
    )
    return [dict(row) for row in cursor.fetchall()]


def get_pipeline_run(con: sqlite3.Connection, run_id: int) -> Optional[dict]:
    cursor = con.execute("SELECT * FROM pipeline_runs WHERE id = ?", (run_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


# -- app_config --
def get_config(con: sqlite3.Connection, key: str) -> Optional[str]:
    cursor = con.execute("SELECT value FROM app_config WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row["value"] if row else None


def set_config(con: sqlite3.Connection, key: str, value: str) -> bool:
    try:
        con.execute(
            "INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)", (key, value)
        )
        return True
    except sqlite3.Error as err:
        log.error("Failed to set config '%s': %s", key, err)
        return False
