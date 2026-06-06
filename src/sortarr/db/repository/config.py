import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("sortarr.db.repository.config")

__all__ = [
    "get_config",
    "set_config",
    "get_ignore_entries",
    "add_ignore_entry",
    "update_ignore_entry",
    "delete_ignore_entry",
]


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
