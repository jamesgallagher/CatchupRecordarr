"""Scheduling: the plugin's single background thread and its per-tick
sweeps (Sections 3, 5, 9 - the *when*; pipeline.py owns the *what*).

One 60s-interval thread per process, but only one process actually runs
each pass: a cross-process leader claim (v0.27.0, Section 16 R7) gates
the pass, since all ~8 processes (uWSGI workers under lazy-apps, two
Celery workers, beat, daphne) load this module and would otherwise each
run identical sweeps every minute - the per-segment claims kept that
*correct*, but 7/8 of the DB polling was pure waste, and leader-gating
also makes cross-process concurrent fetches structurally impossible
rather than merely guarded against (R1's fix, defense in depth). The
daily archive-flag refresh (Section 3) is one more check inside this
same pass - it was previously a second thread in plugin.py with its own
duplicated lifecycle plumbing.

The pass, in order:
1. Take over any future scheduled recordings the post_save signal never
   saw (sweep_missed_takeovers - scheduled while disabled/before install).
2. Reap jobs whose Recording was deleted out from under them.
3. Flip taken-over recordings inside their air window to "interrupted"
   (+ a friendly reason) so the native UI stops showing a misleading
   "Recording" badge (purely time-based, RecordingCard.jsx - it has no
   way to know a plugin cancelled the capture).
4. Detect closed windows (+ grace), plan segments, or fail jobs whose
   window aged out of the provider's archive retention.
5. Recover segments left in_progress by a crashed process, and resume
   jobs left mid-finalize.
6. Claim and fetch the next pending segment (real network calls - at
   most one in flight globally, Section 9).
7. Daily archive-flag refresh, when due.
"""

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone as dt_timezone

from django.db import close_old_connections
from django.utils import timezone

from apps.channels.models import Recording

from . import pipeline
from . import state
from . import takeover
from ._version import LOG_TAG
from .archive import channel_archive_retention_days, refresh_archive_flags
from .planning import SEGMENT_MINUTES, plan_segments
from .settings import get_int_setting, plugin_enabled

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 60

# Slightly under the interval: at each wakeup the first process to
# attempt wins the pass, everyone else's claim finds it fresh and skips.
LEADER_CLAIM_KEY = "tick_leader_claimed_at"
LEADER_CLAIM_STALE = timedelta(seconds=CHECK_INTERVAL_SECONDS - 5)

INTERRUPTED_REASON = (
    "This channel supports catchup/timeshift - Catchup Recordarr will fetch "
    "the finished recording from the provider's archive shortly after it "
    "airs, instead of capturing it live."
)

DEFAULT_GRACE_PERIOD_MINUTES = 15

# Daily archive-flag refresh (Section 3) - moved here from plugin.py's
# own scheduler thread (R7). Keeps its own claim: the manual "Refresh
# Now" action updates the same last-completed timestamp, and the claim
# also guards the boot case where several processes race the very first
# leader window.
REFRESH_INTERVAL = timedelta(hours=24)
REFRESH_CLAIM_STALE = timedelta(minutes=10)

_tick_started = False
_tick_lock = threading.Lock()


def _grace_period():
    return timedelta(minutes=get_int_setting("grace_period_minutes", DEFAULT_GRACE_PERIOD_MINUTES))


def _due_for_refresh():
    last_completed = state.get("archive_refresh_last_completed_at")
    if not last_completed:
        return True
    try:
        last_dt = datetime.fromisoformat(last_completed)
    except ValueError:
        return True
    return datetime.now(dt_timezone.utc) - last_dt >= REFRESH_INTERVAL


def _run_refresh_if_due():
    if not _due_for_refresh():
        return
    if not state.claim("archive_refresh_claimed_at", REFRESH_CLAIM_STALE):
        return
    logger.info("%s daily archive-flag refresh starting", LOG_TAG)
    refresh_archive_flags()
    state.set(
        "archive_refresh_last_completed_at",
        datetime.now(dt_timezone.utc).isoformat(),
    )


def _reap_orphaned_jobs():
    """A taken-over job's Recording can be deleted out from under it -
    nothing reacts to a Recording deletion (only post_save is wired,
    Section 5 Part A), and a stale recording_id in the jobs table breaks
    any code assuming every non-terminal job has a live Recording
    (confirmed in practice, Session 39). Distinct from segment-level
    orphan recovery, which assumes the Recording still exists.
    """
    recording_ids = state.non_terminal_job_recording_ids()
    if not recording_ids:
        return
    existing_ids = set(
        Recording.objects.filter(id__in=recording_ids).values_list("id", flat=True)
    )
    for rid in recording_ids:
        if rid not in existing_ids:
            logger.warning(
                "%s recording %s: underlying Recording no longer exists "
                "(deleted) - removing its catchup job/segment state",
                LOG_TAG, rid,
            )
            state.delete_job(rid)


def _mark_interrupted_if_due():
    now = timezone.now()
    recording_ids = state.non_terminal_job_recording_ids()
    if not recording_ids:
        return

    # Only rows whose window has actually started but not yet ended -
    # this is the exact span the native badge would otherwise show as
    # "Recording".
    recordings = Recording.objects.filter(
        id__in=recording_ids,
        start_time__lte=now,
        end_time__gt=now,
    )
    for rec in recordings:
        cp = rec.custom_properties or {}
        if cp.get("status") == "interrupted" and cp.get("interrupted_reason") == INTERRUPTED_REASON:
            continue  # already flipped on a previous tick
        cp["status"] = "interrupted"
        cp["interrupted_reason"] = INTERRUPTED_REASON
        rec.custom_properties = cp
        rec.save(update_fields=["custom_properties"])
        logger.info(
            "%s recording %s: flipped to 'interrupted' now that its window "
            "has started, so the UI stops showing a live 'Recording' badge",
            LOG_TAG, rec.id,
        )


def _check_post_air_ready():
    now = timezone.now()
    recording_ids = state.non_terminal_job_recording_ids()
    if not recording_ids:
        return

    candidates = Recording.objects.filter(
        id__in=recording_ids,
        end_time__lte=now - _grace_period(),
    ).select_related("channel")

    for rec in candidates:
        job = state.get_job(rec.id)
        if job and job["status"] in ("stitched", "validated"):
            # Already has a real deliverable file - never retroactively
            # fail it as "too old" just because its (already-fetched)
            # window has since aged past retention.
            continue

        retention_days = (
            channel_archive_retention_days(rec.channel) if rec.channel else 0
        )
        if retention_days and rec.end_time < now - timedelta(days=retention_days):
            # Step 15 - Section 10's job-level cap IS this retention
            # cutoff. Failing the job removes it from
            # non_terminal_job_recording_ids() on the next query, so it
            # naturally stops being re-checked.
            pipeline.fail_job(
                rec.id,
                f"window aged out of the provider's {retention_days}d archive "
                f"retention before catchup fetch completed (window closed "
                f"{now - rec.end_time} ago)",
            )
            continue

        # Plan segments the first time a job is seen as ready -
        # idempotent regardless of how many ticks see it.
        if not state.segments_exist(rec.id):
            segments = plan_segments(rec.start_time, rec.end_time)
            state.create_segments(
                rec.id, [(idx, s.isoformat(), dur) for idx, s, dur in segments]
            )
            logger.info(
                "%s recording %s: planned %d segment(s) (%d min each, "
                "window %s to %s)",
                LOG_TAG, rec.id, len(segments), SEGMENT_MINUTES,
                rec.start_time, rec.end_time,
            )

        # Log "ready" once per job, not every tick - without this a job
        # would re-log every 60s for as long as it sits pending (e.g.
        # while segment fetching works through its retry backoff).
        if state.get(f"post_air_ready_logged:{rec.id}"):
            continue
        program = (rec.custom_properties or {}).get("program") or {}
        logger.info(
            "%s recording %s ('%s' on channel '%s') ready for catchup fetch "
            "(window closed %s ago) - segments planned, fetching will begin "
            "this tick or next",
            LOG_TAG, rec.id, program.get("title", "?"),
            getattr(rec.channel, "name", "?"), now - rec.end_time,
        )
        state.set(f"post_air_ready_logged:{rec.id}", "1")


def _recover_orphaned_segments():
    """Section 9's segment-level orphan recovery (Session 38's Mustarrd
    refinement included). A segment found in_progress at the start of a
    NEW pass can only be a leftover from a crash/restart mid-fetch -
    processing is strictly sequential, so nothing is ever legitimately
    still "being worked on" when a new pass begins.

    Before resetting to pending (and re-fetching from scratch), check
    whether the segment's file already exists on disk - download.py's
    atomic .part-then-rename means the final path only ever exists for a
    fetch that fully completed, so existence alone proves it finished
    pre-crash and just never got marked complete in the state store.
    """
    for seg in state.in_progress_segments():
        rid, idx = seg["recording_id"], seg["idx"]
        expected_path = state.segment_path(rid, idx)
        if os.path.exists(expected_path):
            state.mark_segment_completed(rid, idx, expected_path)
            logger.info(
                "%s recording %s segment #%s: found already completed on "
                "disk after a restart - marking completed, not re-fetching",
                LOG_TAG, rid, idx,
            )
        else:
            state.reset_segment_to_pending(rid, idx)
            logger.warning(
                "%s recording %s segment #%s: was in_progress at tick "
                "start with no file on disk - crash/restart mid-fetch, "
                "resetting to pending",
                LOG_TAG, rid, idx,
            )


def _process_segments():
    """One claim attempt per non-terminal job per pass, strictly
    sequential (Section 9) - and at most one segment in flight globally,
    enforced by the claim itself (R1) and by leader-gating the pass.
    """
    backoff = pipeline.segment_retry_backoff()  # one setting read per pass
    for rid in state.non_terminal_job_recording_ids():
        segment = state.claim_next_pending_segment(rid, backoff)
        if segment is not None:
            pipeline.fetch_claimed_segment(rid, segment)


def run_once():
    """One full tick pass - shared verbatim by the background loop and
    the manual "Run Status Tick Now" action (public as of v0.27.0,
    Section 16 R9 - the action previously reached into four private
    functions), so a manual click can never drift from the real tick.
    """
    takeover.sweep_missed_takeovers()
    _reap_orphaned_jobs()
    _mark_interrupted_if_due()
    _check_post_air_ready()
    _recover_orphaned_segments()
    pipeline.resume_stalled_finalizes()
    _process_segments()


def fetch_one_segment_now():
    """On-demand single claim+fetch for the manual "Fetch One Pending
    Segment Now" action - the exact same claim/pipeline path the
    automatic tick uses, reported from the pipeline's own outcome
    (Section 16 R6) instead of re-querying state and guessing what
    happened. Returns a human-readable status string.
    """
    _reap_orphaned_jobs()
    # Recover orphans before claiming: the claim guard is global (one
    # in-flight segment across ALL jobs), so a stale in_progress row
    # left by a crash would otherwise block every manual fetch until the
    # next background pass cleaned it up.
    _recover_orphaned_segments()
    backoff = pipeline.segment_retry_backoff()
    for rid in state.non_terminal_job_recording_ids():
        segment = state.claim_next_pending_segment(rid, backoff)
        if segment is None:
            continue
        result = pipeline.fetch_claimed_segment(rid, segment)
        outcome = result["outcome"]
        prefix = f"recording {rid} segment #{segment['idx']}"
        if outcome == "fetched":
            return f"{prefix}: fetched successfully -> {result['path']}"
        if outcome == "finalized":
            return (
                f"{prefix}: fetched successfully - that was the last segment; "
                f"stitching, validation, and the Recording-row update all "
                f"succeeded -> {result['output_path']} (now playable in Dispatcharr)"
            )
        if outcome == "row_update_failed":
            return (
                f"{prefix}: fetched, stitched, and validated successfully "
                f"({result['output_path']}), but updating the native Recording "
                f"row failed - it will be retried automatically on a later tick"
            )
        if outcome == "job_failed":
            return f"{prefix}: job is now permanently failed - {result['reason']}"
        if outcome == "fetch_failed":
            return (
                f"{prefix}: fetch failed (attempt {result['retry_count']}/"
                f"{pipeline.MAX_SEGMENT_ATTEMPTS}) - {result['error']}"
            )
        if outcome == "recording_deleted":
            return f"recording {rid} no longer exists - its stale catchup job has been removed."
        return f"{prefix}: {outcome}"  # future-proof fallthrough
    return "No claimable segments right now (none pending, or all mid-retry-backoff)."


def _tick_loop():
    time.sleep(45)
    while True:
        try:
            close_old_connections()
            # Disabling the plugin in the UI doesn't unload this module
            # or stop this thread - it must gate itself (the Session 25
            # rule, extended to threads in v0.26.0).
            if plugin_enabled() and state.claim(LEADER_CLAIM_KEY, LEADER_CLAIM_STALE):
                run_once()
                _run_refresh_if_due()
        except Exception:
            logger.exception("%s tick pass error", LOG_TAG)
        finally:
            close_old_connections()
        time.sleep(CHECK_INTERVAL_SECONDS)


def start():
    global _tick_started
    with _tick_lock:
        if _tick_started:
            return
        _tick_started = True
        thread = threading.Thread(
            target=_tick_loop, daemon=True, name="catchup-recordarr-tick"
        )
        thread.start()
        logger.info("%s background tick thread started", LOG_TAG)
