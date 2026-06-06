import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ys2wl.db.repository.pipeline")

__all__ = [
    "get_pipelines",
    "create_pipeline",
    "update_pipeline",
    "delete_pipeline",
    "get_pipeline_selectors",
    "set_pipeline_selectors",
    "get_pipeline_ignore_list_ids",
    "set_pipeline_ignore_lists",
    "get_pipeline_subscription_ids",
    "set_pipeline_subscriptions",
    "get_pipeline_tracking",
    "upsert_pipeline_tracking",
    "get_min_tracking_for_subscription",
    "get_routing_rules",
    "create_routing_rule",
    "update_routing_rule",
    "delete_routing_rule",
]


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
