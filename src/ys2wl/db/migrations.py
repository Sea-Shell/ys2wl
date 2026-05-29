import sqlite3
import logging

log = logging.getLogger("ys2wl.db.migrations")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS videos (
    videoId TEXT NOT NULL PRIMARY KEY,
    timestamp TEXT,
    title TEXT,
    subscriptionId TEXT,
    playlistId TEXT,
    duration_seconds INTEGER,
    route_rule TEXT
);
CREATE TABLE IF NOT EXISTS channel (
    id TEXT NOT NULL PRIMARY KEY,
    title TEXT
);
CREATE TABLE IF NOT EXISTS playlist (
    id TEXT NOT NULL PRIMARY KEY,
    title TEXT
);
CREATE TABLE IF NOT EXISTS subscription (
    id TEXT NOT NULL PRIMARY KEY,
    title TEXT,
    timestamp TEXT
);
CREATE TABLE IF NOT EXISTS last_run (
    id NUMBER NOT NULL PRIMARY KEY,
    timestamp TEXT
);
CREATE TABLE IF NOT EXISTS routing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    field TEXT,
    operator TEXT NOT NULL DEFAULT 'contains',
    pattern TEXT,
    destination_playlist_id TEXT NOT NULL,
    destination_playlist_title TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    subscriptions_processed INTEGER DEFAULT 0,
    subscriptions_skipped INTEGER DEFAULT 0,
    videos_added INTEGER DEFAULT 0,
    videos_skipped INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    error_message TEXT,
    trigger TEXT DEFAULT 'scheduled'
);
CREATE TABLE IF NOT EXISTS pipeline_run_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES pipeline_runs(id),
    video_id TEXT,
    title TEXT,
    subscription_title TEXT,
    action TEXT NOT NULL,
    reason TEXT,
    reason_detail TEXT,
    routed_to TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS app_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ignore_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL CHECK(type IN ('subscription', 'video', 'words')),
    pattern TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ignore_type ON ignore_entries(type);
"""


def init_db(db_path: str) -> bool:
    try:
        con = sqlite3.connect(db_path)
        con.executescript(SCHEMA_SQL)
        con.close()
        return True
    except sqlite3.Error as err:
        log.error("Failed to initialize database: %s", err)
        return False
