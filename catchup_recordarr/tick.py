"""Status-transition tick (#13) + post-air detection (Section 5 Part B)
+ segment orchestration (Section 9, step 12).

Several checks on one shared schedule, folded into a single thread
deliberately (noted as a TODO when step 5 was built standalone, done now
that step 6 exists) rather than one scheduler per concern:

1. Flip a taken-over Recording's status to "interrupted" (+ a friendly
   reason) once start_time has passed, so the native UI stops showing a
   misleading "Recording" badge. Confirmed on the real deployment that
   the badge is purely time-based (start <= now < end, RecordingCard.jsx)
   - it has no way to know a plugin cancelled the native capture.

2. Detect when a taken-over recording's window has closed (+ grace) and
   plan its segments (Section 9, step 10) if not already planned.

3. Recover segments left in_progress by a crashed/restarted process, and
   claim + fetch the next pending segment for each non-terminal job
   (step 12) - real network calls, one claim attempt per job per tick
   pass, never concurrent (Section 9).
"""

import logging
import os
import threading
import time
from datetime import datetime, timedelta

from django.db import close_old_connections
from django.utils import timezone

from apps.channels.models import Recording

from . import state
from ._version import LOG_TAG
from .archive import catchup_capable_stream_for_channel, channel_archive_retention_days
from .download import fetch_segment
from .planning import SEGMENT_MINUTES, plan_segments
from .provider import resolve_provider_timezone
from .recording import mark_recording_completed
from .stitch import stitch_segments
from .validate import validate_output

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 60

INTERRUPTED_REASON = (
    "This channel supports catchup/timeshift - Catchup Recordarr will fetch "
    "the finished recording from the provider's archive shortly after it "
    "airs, instead of capturing it live."
)

# Sportarr-matching default (Section 5) - buffer after end_time before
# treating a window as ready, since the provider's archive needs time to
# finalize the tail of the broadcast. Hardcoded for now, same reasoning
# as Section 8's "not ready" threshold: add a setting only if real-world
# testing shows it needs tuning.
#
# TEMPORARILY 5 minutes (Session 40) for faster iteration while debugging
# step 11/12 - real value is 15 minutes, revert once the live->timeshift
# lag is actually known. See design.md Session 40.
GRACE_PERIOD = timedelta(minutes=5)

# Section 9 - hardcoded, 5 attempts, no separate backoff timer was the
# original reasoning ("the existing 5-15 min poll cadence already spaces
# retries out naturally"). That assumption didn't match what actually got
# built: the tick runs every 60 seconds, not every 5-15 minutes, so
# without an explicit backoff the cap would exhaust itself in 5 *minutes*
# against a provider that (per Sessions 40-42's real-world testing) can
# genuinely take 20-30+ minutes to finalize a window. SEGMENT_RETRY_BACKOFF
# below fixes that gap - deliberately its own constant, not tied to
# GRACE_PERIOD above, since GRACE_PERIOD is currently an intentionally-
# shortened debug value (Session 40) and this retry spacing needs a safer,
# more conservative number until the real provider lag is known. At 15
# minutes x 5 attempts, a job gets up to 75 minutes of retrying before its
# segment is marked permanently failed - matching Section 9's originally
# intended "25-75 minutes" range.
MAX_SEGMENT_ATTEMPTS = 5
SEGMENT_RETRY_BACKOFF = timedelta(minutes=15)

_tick_started = False
_tick_lock = threading.Lock()


def _reap_orphaned_jobs():
    """A taken-over job's Recording can be deleted out from under it -
    the user removing a recording in Dispatcharr's UI, or an old
    throwaway test recording being cleaned up - and the plugin has no
    signal to react to that (only post_save is wired, Section 5 Part A).
    Left unreaped, a stale recording_id sitting in the jobs table breaks
    any code that assumes "every non-terminal job has a live Recording" -
    confirmed in practice (Session 39): fetch_test_segment's
    `Recording.objects.get(id=...)` raised DoesNotExist against a real
    deployment. This is a different case from Section 9's segment-level
    orphan recovery, which assumes the Recording still exists and only a
    fetch stalled - here the job's whole subject is gone. Runs at the
    start of every tick, and is also called defensively by any one-off
    action that's about to pick a "next" job, so it's never more than one
    tick stale either way.
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
        end_time__lte=now - GRACE_PERIOD,
    ).select_related("channel")

    for rec in candidates:
        job = state.get_job(rec.id)
        if job and job["status"] in ("stitched", "validated"):
            # Already has a real deliverable file waiting on step 16 -
            # never retroactively fail it as "too old" just because its
            # (already-fetched) window has since aged past retention.
            # Only relevant for jobs still working toward that point.
            continue

        retention_days = (
            channel_archive_retention_days(rec.channel) if rec.channel else 0
        )
        if retention_days and rec.end_time < now - timedelta(days=retention_days):
            # Step 15 - Section 10's job-level cap IS this retention
            # cutoff, not a separate counter: "once the requested window
            # falls out of the channel's archive retention, stop
            # retrying and mark the job permanently failed." Marking
            # 'failed' here (rather than just warning, as before step 15)
            # removes the job from non_terminal_job_recording_ids() on
            # the very next query, so this naturally stops it being
            # re-checked - no separate dedup flag needed the way the old
            # warn-only version required, since a terminal job just never
            # matches this query again.
            reason = (
                f"window aged out of the provider's {retention_days}d archive "
                f"retention before catchup fetch completed (window closed "
                f"{now - rec.end_time} ago)"
            )
            state.set_job_status(rec.id, "failed", reason)
            logger.error(
                "%s recording %s: %s - job marked permanently failed",
                LOG_TAG, rec.id, reason,
            )
            continue

        # Plan segments (step 10) the first time a job is seen as ready -
        # idempotent regardless of how many ticks see it before the
        # actual fetch (step 11) exists to consume them.
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
        # would otherwise re-log every 60s for as long as it sits pending
        # (e.g. while _process_segments below is still working through
        # its retry backoff).
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
    """Section 9's segment-level orphan recovery, refined per Session 38's
    Mustarrd comparison. A segment found in_progress at the start of a
    NEW tick can only be a leftover from a crash/restart mid-fetch -
    Section 9 already commits to strictly sequential (non-concurrent)
    processing, so a segment can never legitimately still be "being
    worked on" once a new tick pass begins; that work would have already
    finished, synchronously, within the tick that claimed it.

    Before blindly resetting to pending (and re-fetching from scratch),
    check whether the segment's file already exists on disk. Unlike
    Mustarrd's byte-count bookkeeping, our own atomic rename in
    download.py means this check is just "does the final path exist" -
    the .part-file-then-rename pattern guarantees the final path is only
    ever created once a fetch fully completed and passed the not-ready
    threshold, so existence alone proves it finished pre-crash and just
    never got marked complete in the state store.
    """
    for seg in state.in_progress_segments():
        rid, idx = seg["recording_id"], seg["idx"]
        expected_path = os.path.join(
            state.DATA_DIR, "segments", str(rid), f"segment_{idx:04d}.ts"
        )
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


def _fail_segment_and_maybe_job(rid, segment, error):
    """A real fetch attempt failed (both dialects). Bump the segment's
    retry_count; once it hits the cap, mark the whole *job* permanently
    failed with a specific reason (Section 9) rather than waiting on the
    retention-based cutoff (step 15) to eventually notice. The segment
    itself has no separate dead-end state either way - it goes back to
    'pending', but a failed job is filtered out of
    non_terminal_job_recording_ids() so it's never claimed again.
    """
    retry_count = segment["retry_count"] + 1
    state.record_segment_attempt_failure(rid, segment["idx"], error, retry_count)
    if retry_count >= MAX_SEGMENT_ATTEMPTS:
        reason = f"segment {segment['idx']} failed after {retry_count} attempts: {error}"
        state.set_job_status(rid, "failed", reason)
        logger.error(
            "%s recording %s: %s - job marked permanently failed",
            LOG_TAG, rid, reason,
        )
    else:
        logger.warning(
            "%s recording %s segment #%s: attempt %d/%d failed (%s), will "
            "retry no sooner than %s from now",
            LOG_TAG, rid, segment["idx"], retry_count, MAX_SEGMENT_ATTEMPTS,
            error, SEGMENT_RETRY_BACKOFF,
        )


def _fetch_claimed_segment(rid, segment):
    """Resolve everything fetch_segment() needs for an already-claimed
    segment, run the real fetch, and persist the result. Shared by the
    automatic per-tick orchestration below and the manual "Fetch One
    Pending Segment Now" action (plugin.py), so there's exactly one code
    path that actually talks to a provider for a segment, not two
    diverging implementations.
    """
    try:
        rec = Recording.objects.select_related("channel").get(id=rid)
    except Recording.DoesNotExist:
        # _reap_orphaned_jobs() normally catches this before we ever get
        # here, but the Recording could vanish in the gap between that
        # check and this one - don't leave the segment stuck in_progress.
        state.reset_segment_to_pending(rid, segment["idx"])
        state.delete_job(rid)
        return

    stream = catchup_capable_stream_for_channel(rec.channel) if rec.channel else None
    if not stream or not stream.m3u_account or stream.stream_id is None:
        _fail_segment_and_maybe_job(
            rid, segment, "could not resolve a catchup-capable stream/account for this recording"
        )
        return

    tz = resolve_provider_timezone(stream.m3u_account)
    start_utc = datetime.fromisoformat(segment["start_utc"])
    start_local = start_utc.astimezone(tz)
    dest_path = os.path.join(
        state.DATA_DIR, "segments", str(rid), f"segment_{segment['idx']:04d}.ts"
    )

    success, error = fetch_segment(
        stream.m3u_account, stream.stream_id, start_local,
        segment["duration_minutes"], dest_path,
    )

    if success:
        state.mark_segment_completed(rid, segment["idx"], dest_path)
        logger.info(
            "%s recording %s segment #%s: fetched successfully (%d bytes)",
            LOG_TAG, rid, segment["idx"], os.path.getsize(dest_path),
        )
        if state.all_segments_completed(rid):
            _stitch_job(rid)
    else:
        _fail_segment_and_maybe_job(rid, segment, error)


def _stitch_job(rid):
    """Step 13 - every segment for this job just finished; concatenate
    them into the final MKV (Section 9/11). The claim mechanism already
    guarantees only one process can ever complete a job's *last* segment
    (each segment claim is mutually exclusive), so only that one process
    ever observes all_segments_completed() flip to True - no separate
    lock needed to stop two processes stitching the same job twice.
    """
    segments = sorted(state.get_segments(rid), key=lambda s: s["idx"])
    segment_paths = [s["file_path"] for s in segments]
    if any(p is None for p in segment_paths):
        # Shouldn't happen - all_segments_completed() only returns True
        # when every segment's status is 'completed', and mark_segment_completed()
        # always sets file_path in the same call. Defensive, not expected.
        logger.error(
            "%s recording %s: all segments marked completed but at least "
            "one has no file_path recorded - cannot stitch",
            LOG_TAG, rid,
        )
        state.set_job_status(rid, "failed", "internal error: a completed segment has no file_path")
        return

    output_path = os.path.join(state.DATA_DIR, "output", f"{rid}.mkv")
    logger.info(
        "%s recording %s: all %d segment(s) fetched - stitching into %s",
        LOG_TAG, rid, len(segment_paths), output_path,
    )
    success, error = stitch_segments(segment_paths, output_path)
    if not success:
        state.set_job_status(rid, "failed", f"stitching failed: {error}")
        logger.error(
            "%s recording %s: stitching failed - %s - job marked "
            "permanently failed",
            LOG_TAG, rid, error,
        )
        return

    state.set(f"stitched_output_path:{rid}", output_path)
    logger.info(
        "%s recording %s: stitched successfully -> %s (%d bytes)",
        LOG_TAG, rid, output_path, os.path.getsize(output_path),
    )

    # Step 14 - validate before this is ever allowed to look finished
    # (Section 7's ordering rule: never mark something completed until
    # it's fully downloaded, stitched, AND verified).
    rec = Recording.objects.select_related("channel").filter(id=rid).first()
    if rec is None:
        # _reap_orphaned_jobs() normally catches a deleted Recording
        # before we ever get this far, but handle the gap defensively
        # rather than crash on rec.end_time below.
        state.delete_job(rid)
        return

    valid, verror = validate_output(output_path, rec.end_time - rec.start_time)
    if not valid:
        # Step 15 will turn a validation failure into a proper job-level
        # retry policy tied to archive retention; until it exists, an
        # immediate permanent failure with a clear reason is the correct
        # v1 behavior - same as step 12's segment-retry-cap exhaustion -
        # rather than silently leaving a bad file marked 'stitched'.
        state.set_job_status(rid, "failed", f"post-stitch validation failed: {verror}")
        logger.error(
            "%s recording %s: post-stitch validation failed - %s - job "
            "marked permanently failed",
            LOG_TAG, rid, verror,
        )
        return

    state.set_job_status(rid, "validated")
    logger.info("%s recording %s: post-stitch validation passed", LOG_TAG, rid)

    # Step 16 - update the native Recording row now that everything is
    # fully downloaded, stitched, AND verified (Section 7's ordering
    # rule). Moves our own internal job status to a genuinely terminal
    # 'completed' - the first real use of that status value since it was
    # reserved in the schema back in step 3.
    mark_recording_completed(rec, output_path)
    state.set_job_status(rid, "completed")


def _process_segments():
    """Step 12 - the real claim -> fetch -> mark orchestration, one
    claim attempt per non-terminal job per tick pass. Deliberately a
    plain sequential for-loop (not threads/async): Section 9 committed
    to never fetching concurrently, and that has to hold across every
    process running its own copy of this thread too, which is what
    state.claim_next_pending_segment()'s atomic claim guarantees - only
    one process can ever be mid-fetch for a given segment (or, via the
    NOT EXISTS check inside that claim, a given job) at a time.
    """
    for rid in state.non_terminal_job_recording_ids():
        segment = state.claim_next_pending_segment(rid, SEGMENT_RETRY_BACKOFF)
        if segment is not None:
            _fetch_claimed_segment(rid, segment)


def fetch_one_segment_now():
    """On-demand version of _process_segments() for the manual "Fetch One
    Pending Segment Now" action (plugin.py) - claims and fetches the
    first available segment across any non-terminal job, through the
    exact same state.claim_next_pending_segment()/_fetch_claimed_segment()
    path the automatic tick uses, so a manual click can never race or
    diverge from what the tick itself would have done. Returns a short
    human-readable status string for the action's response.
    """
    _reap_orphaned_jobs()
    for rid in state.non_terminal_job_recording_ids():
        segment = state.claim_next_pending_segment(rid, SEGMENT_RETRY_BACKOFF)
        if segment is None:
            continue
        _fetch_claimed_segment(rid, segment)
        updated = next(
            (s for s in state.get_segments(rid) if s["idx"] == segment["idx"]), None
        )
        job = state.get_job(rid)
        if updated and updated["status"] == "completed":
            if job and job["status"] == "completed":
                path = state.get(f"stitched_output_path:{rid}")
                return (
                    f"recording {rid} segment #{segment['idx']}: fetched successfully "
                    f"-> {updated['file_path']} - that was the last segment; stitching, "
                    f"validation, and the Recording-row update all succeeded -> {path} "
                    f"(now playable in Dispatcharr)"
                )
            if job and job["status"] == "failed":
                return (
                    f"recording {rid} segment #{segment['idx']}: fetched successfully, "
                    f"but that was the last segment and stitching or validation "
                    f"failed - {job['last_error']}"
                )
            return f"recording {rid} segment #{segment['idx']}: fetched successfully -> {updated['file_path']}"
        if job and job["status"] == "failed":
            return f"recording {rid} segment #{segment['idx']}: failed and job is now permanently failed - {job['last_error']}"
        return (
            f"recording {rid} segment #{segment['idx']}: fetch failed "
            f"(attempt {updated['retry_count'] if updated else '?'}/{MAX_SEGMENT_ATTEMPTS}) - "
            f"{updated['last_error'] if updated else 'see logs'}"
        )
    return "No claimable segments right now (none pending, or all mid-retry-backoff)."


def _tick_loop():
    time.sleep(45)
    while True:
        try:
            close_old_connections()
            _reap_orphaned_jobs()
            _mark_interrupted_if_due()
            _check_post_air_ready()
            _recover_orphaned_segments()
            _process_segments()
        except Exception:
            logger.exception("%s status-transition tick error", LOG_TAG)
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
            target=_tick_loop, daemon=True, name="catchup-recordarr-status-tick"
        )
        thread.start()
        logger.info("%s status-transition tick thread started", LOG_TAG)
