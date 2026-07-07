"""Plugin-owned SQLite state store (Section 6).

Durably persists job/segment/dialect state across process restarts and
across the several separate processes (uWSGI workers, Celery workers,
beat, daphne) that all load this plugin independently, since none of
them share Python memory.

The database lives OUTSIDE the plugin's own folder, in a sibling
directory under the plugins root. Verified in Dispatcharr's installer
(apps/plugins/api_views.py, _install_plugin_from_zip): a repo-based
plugin update atomically swaps the plugin directory and deletes the old
one, so anything stored inside the plugin folder that isn't in the
release zip is destroyed on every update. A sibling directory survives
updates, still sits on the persisted /data volume, and the plugin loader
ignores it because it contains no plugin.py/__init__.py.

Refactored in v0.27.0 (Section 16 R3/R10): schema DDL now runs once per
process instead of on every call (it previously re-executed four
CREATE TABLE IF NOT EXISTS statements per state access, across ~8
processes), and the 22 hand-rolled lock/connect/close blocks collapsed
into one _db() context manager. Schema v2 (first real use of the
migration machinery reserved at step 3): jobs gained an output_path
column, replacing the stitched_output_path:{rid} kv rows that were
load-bearing data hiding in a scratch table (R2).
"""

import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

_PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.join(os.path.dirname(_PLUGIN_DIR), "catchup_recordarr_data")
_DB_PATH = os.path.join(_DATA_DIR, "state.db")

# Public alias - other modules need this to place files somewhere that
# survives a plugin update, same reasoning as state.db itself living
# outside the plugin folder.
DATA_DIR = _DATA_DIR

_lock = threading.Lock()
_schema_ready = False  # per-process; guarded by _lock

SCHEMA_VERSION = "2"

# Full Section 6 schema (v2 shape - fresh installs get this directly;
# existing v1 databases are migrated in _init_schema). Job identity is
# the native Recording.id (the plugin acts on existing native rows, it
# never invents its own job ids).
# Statuses are plain strings matching the design's state machines:
#   jobs:     pending / in_progress / stitched / validated / completed / failed
#             ('stitched': every segment fetched and concatenated, not
#             yet validated. 'validated': post-stitch ffprobe checks
#             passed. Both non-terminal. 'completed': the native
#             Recording row has been updated in place and is playable in
#             Dispatcharr; terminal, same as 'failed'.)
#   segments: pending / in_progress / completed  (failure -> back to
#             pending, Section 9 - deliberately no dead-end state)
# account_dialects holds Section 8's per-M3UAccount timeshift dialect.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
    recording_id INTEGER PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'pending',
    retry_count  INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    output_path  TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    recording_id     INTEGER NOT NULL REFERENCES jobs(recording_id) ON DELETE CASCADE,
    idx              INTEGER NOT NULL,
    start_utc        TEXT NOT NULL,
    duration_minutes INTEGER NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    retry_count      INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT,
    file_path        TEXT,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (recording_id, idx)
);

CREATE TABLE IF NOT EXISTS account_dialects (
    m3u_account_id       INTEGER PRIMARY KEY,
    dialect              TEXT NOT NULL DEFAULT 'unknown',
    confirmed_at         TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0
);
"""

# Terminal job statuses - set_job_status refuses to overwrite these
# (Section 16 R10's lightweight transition guard: the full state machine
# lives in call-site discipline, but "never resurrect a terminal job" is
# cheap to enforce mechanically and catches the worst class of future
# call-site bug).
_TERMINAL_JOB_STATUSES = ("completed", "failed")


def segment_path(recording_id, idx):
    """The one place a segment's on-disk path is defined (Section 16
    R5). Orphan recovery's disk-existence check only works if it
    reconstructs exactly the path the fetch used - two independent
    constructions of this string was a silent-drift bug waiting.
    """
    return os.path.join(
        DATA_DIR, "segments", str(recording_id), f"segment_{idx:04d}.ts"
    )


def job_output_path(recording_id):
    """The one place a job's final stitched output path is defined."""
    return os.path.join(DATA_DIR, "output", f"{recording_id}.mkv")


def _init_schema(conn):
    """Idempotent DDL + one-time v1 -> v2 migration. Runs once per
    process (R3) - previously executed on every single connection.
    """
    conn.executescript(_SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO kv (key, value) VALUES ('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    row = conn.execute(
        "SELECT value FROM kv WHERE key = 'schema_version'"
    ).fetchone()
    if row and row[0] == "1":
        # v1 -> v2: output_path moves from kv scratch rows to a real
        # jobs column. ALTER guarded because another process may have
        # already migrated between our executescript and here.
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN output_path TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists (concurrent/prior migration)
        conn.execute(
            "UPDATE jobs SET output_path = ("
            "  SELECT value FROM kv WHERE kv.key = 'stitched_output_path:' || jobs.recording_id"
            ") WHERE output_path IS NULL AND EXISTS ("
            "  SELECT 1 FROM kv WHERE kv.key = 'stitched_output_path:' || jobs.recording_id"
            ")"
        )
        # Migrated rows, plus the retention-warned flags obsolete since
        # v0.21.0 (step 15 replaced warn-once dedup with real failure).
        conn.execute("DELETE FROM kv WHERE key LIKE 'stitched_output_path:%'")
        conn.execute("DELETE FROM kv WHERE key LIKE 'post_air_retention_warned:%'")
        conn.execute("UPDATE kv SET value = '2' WHERE key = 'schema_version'")


@contextmanager
def _db():
    """One connection per call, schema guaranteed once per process.
    Serialized by _lock within the process; cross-process safety comes
    from SQLite's own locking (10s busy timeout) plus BEGIN IMMEDIATE
    where check-then-set atomicity matters (the claim functions).
    """
    global _schema_ready
    with _lock:
        os.makedirs(_DATA_DIR, exist_ok=True)
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        # Autocommit mode - transactions are managed explicitly where
        # needed; standalone statements commit immediately.
        conn.isolation_level = None
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            if not _schema_ready:
                _init_schema(conn)
                _schema_ready = True
            yield conn
        finally:
            conn.close()


def get(key, default=None):
    with _db() as conn:
        row = conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default


def set(key, value):
    with _db() as conn:
        conn.execute(
            "INSERT INTO kv (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def job_exists(recording_id):
    with _db() as conn:
        row = conn.execute(
            "SELECT 1 FROM jobs WHERE recording_id = ?", (recording_id,)
        ).fetchone()
        return row is not None


def non_terminal_job_recording_ids():
    """recording_ids for every job we've taken over that hasn't reached
    a terminal state yet.
    """
    with _db() as conn:
        rows = conn.execute(
            "SELECT recording_id FROM jobs WHERE status NOT IN ('completed', 'failed')"
        ).fetchall()
        return [r[0] for r in rows]


def failed_jobs():
    """Every job in a terminal 'failed' state, with its reason - so a job
    hitting the retry cap doesn't just silently vanish from every list
    action once non_terminal_job_recording_ids() stops returning it
    (Section 14's observability philosophy applies to terminal failures
    as much as to in-flight state).
    """
    with _db() as conn:
        rows = conn.execute(
            "SELECT recording_id, last_error, updated_at FROM jobs WHERE status = 'failed'"
        ).fetchall()
        return [{"recording_id": r[0], "last_error": r[1], "updated_at": r[2]} for r in rows]


def create_job(recording_id):
    """Record a taken-over Recording as a pending catchup job. Idempotent -
    INSERT OR IGNORE preserves existing status/retries if the takeover
    receiver fires more than once for the same row (it fires at least
    twice per new Recording: the original save plus core's nested
    task_id-assignment save).
    """
    now = datetime.now(timezone.utc).isoformat()
    with _db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO jobs "
            "(recording_id, status, created_at, updated_at) "
            "VALUES (?, 'pending', ?, ?)",
            (recording_id, now, now),
        )


def delete_job(recording_id):
    """Remove a job, its segments (ON DELETE CASCADE), and its
    per-recording kv flags - the flags previously leaked forever
    (Section 16 R2), one stale row per deleted recording.

    For when the Recording itself has been deleted out from under a
    still-open job (Session 39) - distinct from Section 9's segment-level
    orphan recovery, which assumes the Recording still exists.
    """
    with _db() as conn:
        conn.execute("DELETE FROM jobs WHERE recording_id = ?", (recording_id,))
        conn.execute(
            "DELETE FROM kv WHERE key IN (?, ?, ?)",
            (
                f"post_air_ready_logged:{recording_id}",
                f"stitched_output_path:{recording_id}",
                f"post_air_retention_warned:{recording_id}",
            ),
        )


def get_job(recording_id):
    with _db() as conn:
        row = conn.execute(
            "SELECT recording_id, status, retry_count, last_error, "
            "output_path, created_at, updated_at FROM jobs WHERE recording_id = ?",
            (recording_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "recording_id": row[0],
            "status": row[1],
            "retry_count": row[2],
            "last_error": row[3],
            "output_path": row[4],
            "created_at": row[5],
            "updated_at": row[6],
        }


def set_job_status(recording_id, status, last_error=None):
    """Move a job to a new status. Refuses to overwrite a terminal
    status (completed/failed) - returns False and leaves the row alone,
    so a future call-site bug can't silently resurrect or corrupt a
    finished job (R10's lightweight transition guard).
    """
    now = datetime.now(timezone.utc).isoformat()
    with _db() as conn:
        cur = conn.execute(
            "UPDATE jobs SET status = ?, last_error = ?, updated_at = ? "
            "WHERE recording_id = ? AND status NOT IN (?, ?)",
            (status, last_error, now, recording_id, *_TERMINAL_JOB_STATUSES),
        )
        return cur.rowcount > 0


def set_job_output_path(recording_id, output_path):
    now = datetime.now(timezone.utc).isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE jobs SET output_path = ?, updated_at = ? WHERE recording_id = ?",
            (output_path, now, recording_id),
        )


def get_account_dialect(m3u_account_id):
    """Section 8's per-account dialect memory. Returns None if never
    recorded (cold start - caller defaults to 'path' per Sportarr's own
    default, not this module's job to decide).
    """
    with _db() as conn:
        row = conn.execute(
            "SELECT dialect, confirmed_at, consecutive_failures "
            "FROM account_dialects WHERE m3u_account_id = ?",
            (m3u_account_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "dialect": row[0],
            "confirmed_at": row[1],
            "consecutive_failures": row[2],
        }


def set_account_dialect(m3u_account_id, dialect, confirmed_at):
    """Record a confirmed-working dialect and reset consecutive_failures -
    only ever called after an actual successful fetch with this dialect
    (self-healing on a stale/wrong prior detection), never on a guess.
    """
    with _db() as conn:
        conn.execute(
            "INSERT INTO account_dialects "
            "(m3u_account_id, dialect, confirmed_at, consecutive_failures) "
            "VALUES (?, ?, ?, 0) "
            "ON CONFLICT(m3u_account_id) DO UPDATE SET "
            "dialect = excluded.dialect, confirmed_at = excluded.confirmed_at, "
            "consecutive_failures = 0",
            (m3u_account_id, dialect, confirmed_at),
        )


def increment_account_dialect_failures(m3u_account_id):
    """Both dialects failed for this account. Deliberately does not touch
    which dialect is preferred (Section 8 - don't thrash the setting on a
    transient provider outage; only a confirmed successful fetch with
    the other dialect should flip it).
    """
    with _db() as conn:
        conn.execute(
            "INSERT INTO account_dialects "
            "(m3u_account_id, dialect, consecutive_failures) "
            "VALUES (?, 'unknown', 1) "
            "ON CONFLICT(m3u_account_id) DO UPDATE SET "
            "consecutive_failures = consecutive_failures + 1",
            (m3u_account_id,),
        )


def segments_exist(recording_id):
    with _db() as conn:
        row = conn.execute(
            "SELECT 1 FROM segments WHERE recording_id = ? LIMIT 1",
            (recording_id,),
        ).fetchone()
        return row is not None


def create_segments(recording_id, segments):
    """Idempotent - INSERT OR IGNORE per segment, so re-planning (e.g. if
    the tick that triggers it fires more than once before segments_exist
    is checked) never duplicates rows or resets progress on a segment
    already claimed.

    segments: iterable of (idx, start_utc_iso, duration_minutes).
    """
    now = datetime.now(timezone.utc).isoformat()
    with _db() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO segments "
            "(recording_id, idx, start_utc, duration_minutes, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [(recording_id, idx, start, dur, now) for idx, start, dur in segments],
        )


def get_segments(recording_id):
    with _db() as conn:
        rows = conn.execute(
            "SELECT idx, start_utc, duration_minutes, status, retry_count, "
            "last_error, file_path FROM segments WHERE recording_id = ? "
            "ORDER BY idx",
            (recording_id,),
        ).fetchall()
        return [
            {
                "idx": r[0],
                "start_utc": r[1],
                "duration_minutes": r[2],
                "status": r[3],
                "retry_count": r[4],
                "last_error": r[5],
                "file_path": r[6],
            }
            for r in rows
        ]


def all_segments_completed(recording_id):
    """True once every planned segment for recording_id is 'completed' -
    the stitching trigger. False if no segments are planned yet (never
    true for a job that hasn't reached planning) or any segment is still
    pending/in_progress.
    """
    with _db() as conn:
        row = conn.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) "
            "FROM segments WHERE recording_id = ?",
            (recording_id,),
        ).fetchone()
        total, completed = row[0], row[1] or 0
        return total > 0 and total == completed


def claim_next_pending_segment(recording_id, min_retry_age):
    """Atomically claim the lowest-idx pending segment for recording_id,
    marking it in_progress and returning its row, or None if nothing
    claimable right now.

    Conditions beyond plain "status = pending":
    - A never-attempted segment (retry_count = 0) is claimable
      immediately - the job's own post-air grace period has already
      done the "don't rush the archive" waiting once, at the job level.
    - A previously-failed segment (retry_count > 0) is only claimable
      once min_retry_age (a timedelta) has passed since its last
      attempt (tracked via its own updated_at) - spaces retries out so
      the 5-attempt cap can't exhaust itself in minutes against a
      provider that genuinely needs longer (Sessions 40-42's real-world
      finding), without needing a separate backoff-timestamp column.
    - Refuses to claim while ANY segment of ANY job is in_progress -
      one in-flight provider fetch globally, not per job (v0.26.0,
      Section 16 R1): Section 9's rationale for rejecting concurrency
      is a per-account/global concern, not a per-job one.

    BEGIN IMMEDIATE (same pattern as claim() below) makes the whole
    check-then-set atomic across every process that runs its own copy
    of the tick thread (uWSGI/Celery/beat/daphne) - Section 9's "never
    concurrent" guarantee has to hold across processes sharing this one
    state.db, not just within a single one.
    """
    now = datetime.now(timezone.utc)
    cutoff = (now - min_retry_age).isoformat()
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT idx, start_utc, duration_minutes, retry_count "
                "FROM segments "
                "WHERE recording_id = ? AND status = 'pending' "
                "AND (retry_count = 0 OR updated_at <= ?) "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM segments s2 WHERE s2.status = 'in_progress'"
                ") "
                "ORDER BY idx LIMIT 1",
                (recording_id, cutoff),
            ).fetchone()
            if not row:
                conn.execute("ROLLBACK")
                return None
            idx = row[0]
            conn.execute(
                "UPDATE segments SET status = 'in_progress', updated_at = ? "
                "WHERE recording_id = ? AND idx = ?",
                (now.isoformat(), recording_id, idx),
            )
            conn.execute("COMMIT")
            return {
                "idx": idx,
                "start_utc": row[1],
                "duration_minutes": row[2],
                "retry_count": row[3],
            }
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise


def mark_segment_completed(recording_id, idx, file_path):
    now = datetime.now(timezone.utc).isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE segments SET status = 'completed', file_path = ?, "
            "updated_at = ? WHERE recording_id = ? AND idx = ?",
            (file_path, now, recording_id, idx),
        )


def record_segment_attempt_failure(recording_id, idx, error, retry_count):
    """A real fetch attempt failed (both dialects, Section 8) - back to
    'pending' per Section 9's no-dead-end-state rule, with retry_count
    bumped and last_error recorded. The caller decides whether this
    retry_count has hit the cap and, if so, marks the whole *job*
    failed - the segment itself has no separate terminal state.
    """
    now = datetime.now(timezone.utc).isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE segments SET status = 'pending', retry_count = ?, "
            "last_error = ?, updated_at = ? WHERE recording_id = ? AND idx = ?",
            (retry_count, error, now, recording_id, idx),
        )


def reset_segment_to_pending(recording_id, idx):
    """Orphan recovery only (Section 9) - a segment found in_progress at
    the start of a new tick, with no completed file on disk (checked by
    the caller first), can only be a leftover from a crash/restart, not a
    real fetch failure - so this deliberately does not touch retry_count
    or last_error the way record_segment_attempt_failure does.
    """
    now = datetime.now(timezone.utc).isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE segments SET status = 'pending', updated_at = ? "
            "WHERE recording_id = ? AND idx = ?",
            (now, recording_id, idx),
        )


def in_progress_segments():
    """Every segment currently in_progress, across every job - the
    candidate set for Section 9's orphan recovery at the start of a tick.
    """
    with _db() as conn:
        rows = conn.execute(
            "SELECT recording_id, idx FROM segments WHERE status = 'in_progress'"
        ).fetchall()
        return [{"recording_id": r[0], "idx": r[1]} for r in rows]


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
    with _db() as conn:
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
