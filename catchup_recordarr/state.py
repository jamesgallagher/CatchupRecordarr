"""Minimal plugin-owned key-value store (Section 6 - a small piece pulled
forward early). Section 6 will grow this into the full job/segment
schema; for now it just needs to durably persist "when did we last run
the archive-flag refresh" across process restarts and across the several
separate processes (gunicorn workers, Celery workers) that all load this
plugin independently, since none of them share Python memory.
"""

import os
import sqlite3
import threading

_STATE_DIR = os.path.dirname(os.path.abspath(__file__))
_DB_PATH = os.path.join(_STATE_DIR, "state.db")

_lock = threading.Lock()


def _connect():
    conn = sqlite3.connect(_DB_PATH, timeout=10)
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
            conn.commit()
        finally:
            conn.close()
