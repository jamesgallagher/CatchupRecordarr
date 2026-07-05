"""Minimal plugin-owned key-value store (Section 6 - a small piece pulled
forward early). Section 6 will grow this into the full job/segment
schema; for now it durably persists scheduler state across process
restarts and across the several separate processes (uWSGI workers,
Celery workers, beat, daphne) that all load this plugin independently,
since none of them share Python memory.

The database lives OUTSIDE the plugin's own folder, in a sibling
directory under the plugins root. Verified in Dispatcharr's installer
(apps/plugins/api_views.py, _install_plugin_from_zip): a repo-based
plugin update atomically swaps the plugin directory and deletes the old
one, so anything stored inside the plugin folder that isn't in the
release zip is destroyed on every update. A sibling directory survives
updates, still sits on the persisted /data volume, and the plugin loader
ignores it because it contains no plugin.py/__init__.py.
"""

import os
import sqlite3
import threading
from datetime import datetime, timezone

_PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.join(os.path.dirname(_PLUGIN_DIR), "catchup_recordarr_data")
_DB_PATH = os.path.join(_DATA_DIR, "state.db")

_lock = threading.Lock()


def _connect():
    os.makedirs(_DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, timeout=10)
    # Autocommit mode - transactions are managed explicitly where needed
    # (claim()); standalone statements commit immediately.
    conn.isolation_level = None
    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)"
    )
    return conn


def get(key, default=None):
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT value FROM kv WHERE key = ?", (key,)
            ).fetchone()
            return row[0] if row else default
        finally:
            conn.close()


def set(key, value):
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                "INSERT INTO kv (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        finally:
            conn.close()


def claim(key, stale_after):
    """Atomically claim `key` if unclaimed or stale; returns True if won.

    BEGIN IMMEDIATE takes SQLite's write lock before the read, making the
    check-then-set atomic across every process that loads this plugin. A
    plain get()+set() pair is not a rare race here: all processes start
    their scheduler threads at container boot and wake on the same
    interval, so simultaneous claims are the expected case, not an edge
    case. Losers either see the fresh claim or block briefly on the lock
    (busy timeout) and then see it.
    """
    now = datetime.now(timezone.utc)
    with _lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT value FROM kv WHERE key = ?", (key,)
                ).fetchone()
                if row:
                    try:
                        held = datetime.fromisoformat(row[0])
                        if now - held < stale_after:
                            conn.execute("ROLLBACK")
                            return False
                    except ValueError:
                        pass  # unparseable claim = treat as stale
                conn.execute(
                    "INSERT INTO kv (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, now.isoformat()),
                )
                conn.execute("COMMIT")
                return True
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        finally:
            conn.close()
