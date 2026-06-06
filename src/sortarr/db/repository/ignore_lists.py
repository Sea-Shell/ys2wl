import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("sortarr.db.repository.ignore_lists")

__all__ = [
    "get_ignore_lists",
    "get_ignore_list",
    "create_ignore_list",
    "update_ignore_list",
    "delete_ignore_list",
    "get_ignore_list_entries",
    "add_ignore_list_entry",
    "remove_ignore_list_entry",
]


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
