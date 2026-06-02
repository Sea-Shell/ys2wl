import sqlite3
import logging
from datetime import datetime, timedelta, timezone
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


# -- channel --
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


# -- playlist --
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


# -- subscription --
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
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to set last_run: %s", err)
        return False


# -- routing_rules --
def get_routing_rules(con: sqlite3.Connection) -> list[dict]:
    cursor = con.execute(
        "SELECT id, name, priority, field, operator, pattern, destination_playlist_id, destination_playlist_title, enabled, minimum_length, maximum_length, catch_all "
        "FROM routing_rules ORDER BY priority DESC"
    )
    results = [dict(row) for row in cursor.fetchall()]
    log.info("get_routing_rules: %d enabled rules", len(results))
    return results


def create_routing_rule(
    con: sqlite3.Connection,
    name: str,
    priority: int,
    field: Optional[str],
    operator: str,
    pattern: Optional[str],
    dest_playlist_id: str,
    dest_playlist_title: str,
    minimum_length: str = "0s",
    maximum_length: str = "0s",
    catch_all: bool = False,
) -> Optional[int]:
    now = datetime.now(timezone.utc).isoformat()
    try:
        cursor = con.execute(
            "INSERT INTO routing_rules (name, priority, field, operator, pattern, destination_playlist_id, destination_playlist_title, enabled, minimum_length, maximum_length, catch_all, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)",
            (
                name,
                priority,
                field,
                operator,
                pattern,
                dest_playlist_id,
                dest_playlist_title,
                minimum_length,
                maximum_length,
                int(catch_all),
                now,
                now,
            ),
        )
        con.commit()
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
        "minimum_length",
        "maximum_length",
        "catch_all",
    }
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False
    updates["updated_at"] = datetime.now(timezone.utc).isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [rule_id]
    try:
        con.execute(f"UPDATE routing_rules SET {set_clause} WHERE id = ?", values)
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to update routing rule: %s", err)
        return False


def delete_routing_rule(con: sqlite3.Connection, rule_id: int) -> bool:
    try:
        con.execute("DELETE FROM routing_rules WHERE id = ?", (rule_id,))
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to delete routing rule: %s", err)
        return False


# -- pipeline_runs --
def create_pipeline_run(
    con: sqlite3.Connection, trigger: str = "scheduled", dry_run: bool = False
) -> Optional[int]:
    now = datetime.now(timezone.utc).isoformat()
    try:
        cursor = con.execute(
            "INSERT INTO pipeline_runs (started_at, status, trigger, dry_run) VALUES (?, 'running', ?, ?)",
            (now, trigger, int(dry_run)),
        )
        con.commit()
        return cursor.lastrowid
    except sqlite3.Error as err:
        log.error("Failed to create pipeline run: %s", err)
        return None


def finish_pipeline_run(con: sqlite3.Connection, run_id: int, summary: dict) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    try:
        con.execute(
            "UPDATE pipeline_runs SET finished_at = ?, status = ?, subscriptions_processed = ?, subscriptions_skipped = ?, "
            "videos_added = ?, videos_skipped = ?, errors = ?, error_message = ?, "
            "pipelines_invoked = ?, pipelines_with_errors = ? WHERE id = ?",
            (
                now,
                summary.get("status", "completed"),
                summary.get("subscriptions_processed", 0),
                summary.get("subscriptions_skipped", 0),
                summary.get("videos_added", 0),
                summary.get("videos_skipped", 0),
                summary.get("errors", 0),
                summary.get("error_message", ""),
                summary.get("pipelines_invoked", 0),
                summary.get("pipelines_with_errors", 0),
                run_id,
            ),
        )
        con.commit()
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


# -- pipeline_run_decisions --
def insert_run_decisions(
    con: sqlite3.Connection, run_id: int, decisions: list[dict]
) -> bool:
    try:
        now = datetime.now(timezone.utc).isoformat()
        con.executemany(
            "INSERT INTO pipeline_run_decisions (run_id, video_id, title, subscription_title, action, reason, reason_detail, routed_to, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    run_id,
                    d.get("video_id"),
                    d.get("title"),
                    d.get("subscription_title"),
                    d.get("action"),
                    d.get("reason"),
                    d.get("reason_detail"),
                    d.get("routed_to"),
                    now,
                )
                for d in decisions
            ],
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to insert run decisions: %s", err)
        return False


def get_run_decisions(
    con: sqlite3.Connection, run_id: int, limit: int = 1000
) -> list[dict]:
    cursor = con.execute(
        "SELECT id, video_id, title, subscription_title, action, reason, reason_detail, routed_to, created_at "
        "FROM pipeline_run_decisions WHERE run_id = ? ORDER BY id ASC LIMIT ?",
        (run_id, limit),
    )
    return [dict(row) for row in cursor.fetchall()]


def insert_run_decision(con: sqlite3.Connection, run_id: int, decision: dict) -> bool:
    """Insert a single decision row and commit. Used for incremental progress updates."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        con.execute(
            "INSERT INTO pipeline_run_decisions (run_id, video_id, title, subscription_title, action, reason, reason_detail, routed_to, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                decision.get("video_id"),
                decision.get("title"),
                decision.get("subscription_title"),
                decision.get("action"),
                decision.get("reason"),
                decision.get("reason_detail"),
                decision.get("routed_to"),
                now,
            ),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to insert run decision: %s", err)
        return False


def update_pipeline_run_progress(
    con: sqlite3.Connection, run_id: int, summary: dict
) -> bool:
    """Update run counters without changing status/finished_at."""
    try:
        con.execute(
            "UPDATE pipeline_runs SET subscriptions_processed = ?, subscriptions_skipped = ?, "
            "videos_added = ?, videos_skipped = ?, errors = ?, "
            "pipelines_invoked = ?, pipelines_with_errors = ? WHERE id = ?",
            (
                summary.get("subscriptions_processed", 0),
                summary.get("subscriptions_skipped", 0),
                summary.get("videos_added", 0),
                summary.get("videos_skipped", 0),
                summary.get("errors", 0),
                summary.get("pipelines_invoked", 0),
                summary.get("pipelines_with_errors", 0),
                run_id,
            ),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to update pipeline run progress: %s", err)
        return False


def cleanup_old_decisions(con: sqlite3.Connection, days: int = 10) -> bool:
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cursor = con.execute(
            "DELETE FROM pipeline_run_decisions WHERE created_at < ?", (cutoff,)
        )
        con.commit()
        deleted = cursor.rowcount
        if deleted > 0:
            log.info("Cleaned up %d old pipeline run decisions", deleted)
        return True
    except sqlite3.Error as err:
        log.error("Failed to cleanup old decisions: %s", err)
        return False


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
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to set config '%s': %s", key, err)
        return False


# -- ignore_entries --
def get_ignore_entries(con: sqlite3.Connection, ignore_type: str) -> list[dict]:
    cursor = con.execute(
        "SELECT id, type, pattern, created_at FROM ignore_entries WHERE type = ? ORDER BY id",
        (ignore_type,),
    )
    return [dict(row) for row in cursor.fetchall()]


def add_ignore_entry(
    con: sqlite3.Connection, ignore_type: str, pattern: str
) -> Optional[int]:
    now = datetime.now(timezone.utc).isoformat()
    try:
        cursor = con.execute(
            "INSERT INTO ignore_entries (type, pattern, created_at) VALUES (?, ?, ?)",
            (ignore_type, pattern, now),
        )
        con.commit()
        return cursor.lastrowid
    except sqlite3.Error as err:
        log.error("Failed to add ignore entry: %s", err)
        return None


def update_ignore_entry(con: sqlite3.Connection, entry_id: int, pattern: str) -> bool:
    try:
        con.execute(
            "UPDATE ignore_entries SET pattern = ? WHERE id = ?",
            (pattern, entry_id),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to update ignore entry: %s", err)
        return False


def delete_ignore_entry(con: sqlite3.Connection, entry_id: int) -> bool:
    try:
        con.execute("DELETE FROM ignore_entries WHERE id = ?", (entry_id,))
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to delete ignore entry: %s", err)
        return False


# -- pipelines --
def get_pipelines(con: sqlite3.Connection) -> list[dict]:
    cursor = con.execute(
        "SELECT id, name, enabled, selector_mode, duration_min_seconds, duration_max_seconds, "
        "check_db_exists, check_title_similarity, compare_distance, subscription_scope, "
        "destination_playlist_id, destination_playlist_title, created_at, updated_at "
        "FROM pipelines ORDER BY name"
    )
    return [dict(row) for row in cursor.fetchall()]


def create_pipeline(
    con: sqlite3.Connection,
    pipeline_id: str,
    name: str,
    destination_playlist_id: str,
    destination_playlist_title: str,
    **kwargs,
) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    selector_mode = kwargs.get("selector_mode", "AND")
    duration_min = kwargs.get("duration_min_seconds", 0)
    duration_max = kwargs.get("duration_max_seconds", 0)
    check_db = kwargs.get("check_db_exists", 0)
    check_sim = kwargs.get("check_title_similarity", 0)
    distance = kwargs.get("compare_distance", 80)
    scope = kwargs.get("subscription_scope", "all")
    try:
        con.execute(
            "INSERT INTO pipelines (id, name, enabled, selector_mode, duration_min_seconds, duration_max_seconds, "
            "check_db_exists, check_title_similarity, compare_distance, subscription_scope, "
            "destination_playlist_id, destination_playlist_title, created_at, updated_at) "
            "VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                pipeline_id,
                name,
                selector_mode,
                duration_min,
                duration_max,
                int(check_db),
                int(check_sim),
                distance,
                scope,
                destination_playlist_id,
                destination_playlist_title,
                now,
                now,
            ),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to create pipeline: %s", err)
        return False


def update_pipeline(con: sqlite3.Connection, pipeline_id: str, **kwargs) -> bool:
    allowed = {
        "name",
        "enabled",
        "selector_mode",
        "duration_min_seconds",
        "duration_max_seconds",
        "check_db_exists",
        "check_title_similarity",
        "compare_distance",
        "subscription_scope",
        "destination_playlist_id",
        "destination_playlist_title",
    }
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False
    updates["updated_at"] = datetime.now(timezone.utc).isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [pipeline_id]
    try:
        con.execute(f"UPDATE pipelines SET {set_clause} WHERE id = ?", values)
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to update pipeline: %s", err)
        return False


def delete_pipeline(con: sqlite3.Connection, pipeline_id: str) -> bool:
    try:
        con.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to delete pipeline: %s", err)
        return False


# -- ignore_lists --
def get_ignore_lists(con: sqlite3.Connection) -> list[dict]:
    cursor = con.execute(
        "SELECT id, name, list_type, created_at FROM ignore_lists ORDER BY name"
    )
    return [dict(row) for row in cursor.fetchall()]


def get_ignore_list(con: sqlite3.Connection, list_id: str) -> Optional[dict]:
    cursor = con.execute(
        "SELECT id, name, list_type, created_at FROM ignore_lists WHERE id = ?",
        (list_id,),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def create_ignore_list(
    con: sqlite3.Connection, list_id: str, name: str, list_type: str
) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    try:
        con.execute(
            "INSERT INTO ignore_lists (id, name, list_type, created_at) VALUES (?, ?, ?, ?)",
            (list_id, name, list_type, now),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to create ignore list: %s", err)
        return False


def update_ignore_list(con: sqlite3.Connection, list_id: str, name: str) -> bool:
    try:
        con.execute("UPDATE ignore_lists SET name = ? WHERE id = ?", (name, list_id))
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to update ignore list: %s", err)
        return False


def delete_ignore_list(con: sqlite3.Connection, list_id: str) -> bool:
    try:
        con.execute("DELETE FROM ignore_lists WHERE id = ?", (list_id,))
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to delete ignore list: %s", err)
        return False


# -- ignore_list_entries --
def get_ignore_list_entries(con: sqlite3.Connection, list_id: str) -> list[str]:
    cursor = con.execute(
        "SELECT value FROM ignore_list_entries WHERE ignore_list_id = ? ORDER BY id",
        (list_id,),
    )
    return [row["value"] for row in cursor.fetchall()]


def add_ignore_list_entry(
    con: sqlite3.Connection, entry_id: str, list_id: str, value: str
) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    try:
        con.execute(
            "INSERT INTO ignore_list_entries (id, ignore_list_id, value, created_at) VALUES (?, ?, ?, ?)",
            (entry_id, list_id, value, now),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to add ignore list entry: %s", err)
        return False


def remove_ignore_list_entry(con: sqlite3.Connection, entry_id: str) -> bool:
    try:
        con.execute("DELETE FROM ignore_list_entries WHERE id = ?", (entry_id,))
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to remove ignore list entry: %s", err)
        return False


# -- pipeline_ignore_lists --
def get_pipeline_ignore_list_ids(
    con: sqlite3.Connection, pipeline_id: str
) -> list[str]:
    cursor = con.execute(
        "SELECT ignore_list_id FROM pipeline_ignore_lists WHERE pipeline_id = ?",
        (pipeline_id,),
    )
    return [row["ignore_list_id"] for row in cursor.fetchall()]


def set_pipeline_ignore_lists(
    con: sqlite3.Connection, pipeline_id: str, list_ids: list[str]
) -> None:
    con.execute(
        "DELETE FROM pipeline_ignore_lists WHERE pipeline_id = ?", (pipeline_id,)
    )
    for lid in list_ids:
        con.execute(
            "INSERT OR IGNORE INTO pipeline_ignore_lists (pipeline_id, ignore_list_id) VALUES (?, ?)",
            (pipeline_id, lid),
        )
    con.commit()


# -- pipeline_selectors --
def get_pipeline_selectors(con: sqlite3.Connection, pipeline_id: str) -> list[dict]:
    cursor = con.execute(
        "SELECT id, pipeline_id, field, operator, pattern, combine_operator, created_at FROM pipeline_selectors WHERE pipeline_id = ? ORDER BY id",
        (pipeline_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def set_pipeline_selectors(
    con: sqlite3.Connection, pipeline_id: str, selectors: list[dict]
) -> None:
    con.execute("DELETE FROM pipeline_selectors WHERE pipeline_id = ?", (pipeline_id,))
    for sel in selectors:
        sid = sel.get("id") or f"{pipeline_id}_sel_{abs(hash(str(sel)))}"
        con.execute(
            "INSERT INTO pipeline_selectors (id, pipeline_id, field, operator, pattern, combine_operator, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                sid,
                pipeline_id,
                sel["field"],
                sel["operator"],
                sel["pattern"],
                sel.get("combine_operator", "AND"),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
    con.commit()


# -- pipeline_subscription_tracking --
def get_pipeline_tracking(
    con: sqlite3.Connection, pipeline_id: str, subscription_id: str
) -> Optional[str]:
    cursor = con.execute(
        "SELECT last_processed FROM pipeline_subscription_tracking WHERE pipeline_id = ? AND subscription_id = ?",
        (pipeline_id, subscription_id),
    )
    row = cursor.fetchone()
    return row["last_processed"] if row else None


def upsert_pipeline_tracking(
    con: sqlite3.Connection, pipeline_id: str, subscription_id: str, last_processed: str
) -> None:
    con.execute(
        "INSERT OR REPLACE INTO pipeline_subscription_tracking (pipeline_id, subscription_id, last_processed) VALUES (?, ?, ?)",
        (pipeline_id, subscription_id, last_processed),
    )
    con.commit()


def get_min_tracking_for_subscription(
    con: sqlite3.Connection, subscription_id: str
) -> Optional[str]:
    cursor = con.execute(
        "SELECT MIN(last_processed) as min_ts FROM pipeline_subscription_tracking WHERE subscription_id = ?",
        (subscription_id,),
    )
    row = cursor.fetchone()
    return row["min_ts"] if row and row["min_ts"] else None


# -- pipeline_subscriptions (selected scope) --
def get_pipeline_subscription_ids(
    con: sqlite3.Connection, pipeline_id: str
) -> list[str]:
    cursor = con.execute(
        "SELECT subscription_id FROM pipeline_subscriptions WHERE pipeline_id = ?",
        (pipeline_id,),
    )
    return [row["subscription_id"] for row in cursor.fetchall()]


def set_pipeline_subscriptions(
    con: sqlite3.Connection, pipeline_id: str, sub_ids: list[str]
) -> None:
    con.execute(
        "DELETE FROM pipeline_subscriptions WHERE pipeline_id = ?", (pipeline_id,)
    )
    for sid in sub_ids:
        con.execute(
            "INSERT OR IGNORE INTO pipeline_subscriptions (pipeline_id, subscription_id) VALUES (?, ?)",
            (pipeline_id, sid),
        )
    con.commit()


# -- activity_cache --
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


# -- per-pipeline video queries --
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


def clear_activity_cache(con: sqlite3.Connection) -> None:
    try:
        con.execute("DELETE FROM activity_cache")
        con.commit()
    except sqlite3.Error as err:
        log.error("Failed to clear activity cache: %s", err)
