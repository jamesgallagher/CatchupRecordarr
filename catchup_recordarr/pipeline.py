"""The download pipeline: fetch -> stitch -> validate -> finalize
(build steps 11-17, Sections 8-11 + 7).

Extracted from tick.py in v0.27.0 (Section 16 R4) - the tick had grown
into a god module owning both *when* things run (scheduling) and *what*
runs (this pipeline). tick.py now owns only scheduling and the per-tick
sweeps; every function here is a pipeline stage operating on one job.

Every public function returns an outcome dict ({"outcome": ..., ...},
Section 16 R6) instead of nothing - so callers (the tick loop, the
manual fetch action) report what actually happened rather than
re-querying state afterwards and guessing, which had already produced
one blind spot (a last-segment fetch whose Recording-row save failed
reported as a plain success with no mention the stitch ever ran).
"""

import logging
import os
from datetime import datetime, timedelta

from apps.channels.models import Recording

from . import state
from ._version import LOG_TAG
from .archive import catchup_capable_stream_for_channel
from .download import fetch_segment
from .fsutil import try_delete
from .provider import resolve_provider_timezone
from .recording import mark_recording_completed, mark_recording_failed, maybe_queue_comskip
from .settings import get_int_setting
from .stitch import stitch_segments
from .validate import validate_output

logger = logging.getLogger(__name__)

# Section 9 - 5 attempts, hardcoded (nothing has suggested the *count*
# needs tuning, only the spacing, which is the setting below). A segment
# failure only counts once both dialects have failed (Section 8). When
# the cap is hit the whole job goes permanently failed with a specific
# reason, rather than waiting on the retention cutoff to notice.
MAX_SEGMENT_ATTEMPTS = 5

DEFAULT_SEGMENT_RETRY_BACKOFF_MINUTES = 15


def segment_retry_backoff():
    """Minimum spacing between retry attempts on a failed segment - a
    live setting (step 18), read fresh at each point of use. Exists
    because the tick runs every 60s: without explicit spacing the
    5-attempt cap would exhaust itself in 5 minutes against a provider
    that (Sessions 40-42) can genuinely take 20-30+ minutes to finalize
    a window. 5 x 15min default = up to 75 minutes of retrying, inside
    Section 9's intended 25-75 minute range.
    """
    return timedelta(
        minutes=get_int_setting(
            "segment_retry_backoff_minutes", DEFAULT_SEGMENT_RETRY_BACKOFF_MINUTES
        )
    )


def fail_job(rid, reason):
    """The one place a job goes permanently failed - both halves
    together, always: the plugin's own state store AND the native
    Recording row (Section 10's "failure surfaced by setting
    custom_properties['status'] = 'failed' with a reason").
    """
    state.set_job_status(rid, "failed", reason)
    mark_recording_failed(rid, reason)
    logger.error(
        "%s recording %s: %s - job marked permanently failed",
        LOG_TAG, rid, reason,
    )
    return {"outcome": "job_failed", "reason": reason}


def _fail_segment_attempt(rid, segment, error):
    """A real fetch attempt failed (both dialects). Bump the segment's
    retry_count; at the cap, the whole job fails (Section 9). The
    segment itself has no dead-end state - it returns to 'pending', but
    a failed job is filtered out of non_terminal_job_recording_ids() so
    it's never claimed again.
    """
    retry_count = segment["retry_count"] + 1
    state.record_segment_attempt_failure(rid, segment["idx"], error, retry_count)
    if retry_count >= MAX_SEGMENT_ATTEMPTS:
        return fail_job(
            rid,
            f"segment {segment['idx']} failed after {retry_count} attempts: {error}",
        )
    logger.warning(
        "%s recording %s segment #%s: attempt %d/%d failed (%s), will "
        "retry no sooner than %s from now",
        LOG_TAG, rid, segment["idx"], retry_count, MAX_SEGMENT_ATTEMPTS,
        error, segment_retry_backoff(),
    )
    return {"outcome": "fetch_failed", "error": error, "retry_count": retry_count}


def fetch_claimed_segment(rid, segment):
    """Fetch one already-claimed segment and persist the result; if it
    was the job's last segment, run the whole finish pipeline. The single
    provider-facing code path shared by the automatic tick and the
    manual "Fetch One Pending Segment Now" action.
    """
    try:
        rec = Recording.objects.select_related("channel").get(id=rid)
    except Recording.DoesNotExist:
        # The reaper normally catches this before we ever get here, but
        # the Recording could vanish in the gap between that check and
        # this one - don't leave the segment stuck in_progress.
        state.reset_segment_to_pending(rid, segment["idx"])
        state.delete_job(rid)
        return {"outcome": "recording_deleted"}

    stream = catchup_capable_stream_for_channel(rec.channel) if rec.channel else None
    if not stream or not stream.m3u_account or stream.stream_id is None:
        return _fail_segment_attempt(
            rid, segment, "could not resolve a catchup-capable stream/account for this recording"
        )

    tz = resolve_provider_timezone(stream.m3u_account)
    start_utc = datetime.fromisoformat(segment["start_utc"])
    start_local = start_utc.astimezone(tz)
    dest_path = state.segment_path(rid, segment["idx"])

    success, error = fetch_segment(
        stream.m3u_account, stream.stream_id, start_local,
        segment["duration_minutes"], dest_path,
    )

    if not success:
        return _fail_segment_attempt(rid, segment, error)

    state.mark_segment_completed(rid, segment["idx"], dest_path)
    logger.info(
        "%s recording %s segment #%s: fetched successfully (%d bytes)",
        LOG_TAG, rid, segment["idx"], os.path.getsize(dest_path),
    )
    if state.all_segments_completed(rid):
        return finalize_job(rid)
    return {"outcome": "fetched", "path": dest_path}


def finalize_job(rid):
    """The finish pipeline for a job whose segments are all fetched:
    stitch (step 13) -> validate (step 14) -> update the native
    Recording row (step 16) -> comskip (step 17). Renamed from
    _stitch_job (Section 16 R4) - the old name claimed the first stage
    of four.

    Resumable: also called by resume_stalled_finalizes() for a job left
    at 'stitched'/'validated' by a crash mid-finalize - stages that
    already completed are skipped (the stitch via an output-file
    existence check; download.py/stitch.py's atomic renames mean the
    final paths only ever exist for fully-completed work). Previously a
    crash between stitching and the Recording-row update left the job
    stuck in a non-terminal state forever, with nothing to re-trigger
    the remaining stages - found while extracting this module.

    The claim mechanism guarantees only one process ever completes a
    job's *last* segment, so only that process enters this from the
    fetch path; the resume path is leader-gated by the tick (v0.27.0),
    so the two can't race in practice, and every stage is idempotent
    anyway.
    """
    output_path = state.job_output_path(rid)
    job = state.get_job(rid)
    already_validated = bool(job and job["status"] == "validated")

    if not (os.path.exists(output_path) and os.path.getsize(output_path) > 0):
        segments = sorted(state.get_segments(rid), key=lambda s: s["idx"])
        segment_paths = [s["file_path"] for s in segments]
        if any(p is None for p in segment_paths):
            # Shouldn't happen - all_segments_completed() only returns
            # True when every segment is 'completed', and
            # mark_segment_completed() always sets file_path in the same
            # call. Defensive, not expected.
            return fail_job(rid, "internal error: a completed segment has no file_path recorded - cannot stitch")

        logger.info(
            "%s recording %s: all %d segment(s) fetched - stitching into %s",
            LOG_TAG, rid, len(segment_paths), output_path,
        )
        success, error = stitch_segments(segment_paths, output_path)
        if not success:
            return fail_job(rid, f"stitching failed: {error}")
        already_validated = False  # fresh file, must (re-)validate

        logger.info(
            "%s recording %s: stitched successfully -> %s (%d bytes)",
            LOG_TAG, rid, output_path, os.path.getsize(output_path),
        )

    state.set_job_output_path(rid, output_path)
    state.set_job_status(rid, "stitched")

    # Never allowed to look finished until fully downloaded, stitched,
    # AND verified (Section 7's ordering rule).
    rec = Recording.objects.select_related("channel").filter(id=rid).first()
    if rec is None:
        state.delete_job(rid)
        return {"outcome": "recording_deleted"}

    if not already_validated:
        valid, verror = validate_output(output_path, rec.end_time - rec.start_time)
        if not valid:
            # Immediate permanent failure with a clear reason - never
            # silently leave a bad file marked 'stitched'.
            return fail_job(rid, f"post-stitch validation failed: {verror}")
        logger.info("%s recording %s: post-stitch validation passed", LOG_TAG, rid)
    state.set_job_status(rid, "validated")

    # Step 16 - left at 'validated' (not advanced, not failed) if the
    # save itself fails: the underlying work all genuinely succeeded,
    # only the DB write didn't, so there's no reason to burn the job's
    # retry machinery over it - and resume_stalled_finalizes() will now
    # retry this stage on a later tick.
    if not mark_recording_completed(rec, output_path):
        return {"outcome": "row_update_failed", "output_path": output_path}
    state.set_job_status(rid, "completed")

    # Step 17 - global CoreSettings switch AND the plugin's own setting,
    # never the plugin flag acting alone (Section 7).
    maybe_queue_comskip(rec)

    # Found while extracting this module (Session 45): segment .ts files
    # were never cleaned up after a successful stitch - every completed
    # recording permanently kept a full second copy of itself on disk.
    # Only on success: a failed job keeps its segments for diagnosis and
    # potential re-stitching.
    _cleanup_segment_files(rid)
    return {"outcome": "finalized", "output_path": output_path}


def _cleanup_segment_files(rid):
    for s in state.get_segments(rid):
        if s["file_path"]:
            try_delete(s["file_path"])
    try:
        os.rmdir(os.path.join(state.DATA_DIR, "segments", str(rid)))
    except OSError:
        pass  # not empty or already gone - fine either way


def resume_stalled_finalizes():
    """Job-level analog of segment orphan recovery, at the finalize
    stage: a job sitting at 'stitched' or 'validated' when a tick pass
    starts can only be a leftover from a crash (or a failed
    Recording-row save) mid-finalize - the fetch path runs finalize
    synchronously after the last segment, so nothing legitimately parks
    there between ticks. Re-enter finalize_job(), which skips the
    already-completed stages.
    """
    for rid in state.non_terminal_job_recording_ids():
        job = state.get_job(rid)
        if job and job["status"] in ("stitched", "validated"):
            logger.warning(
                "%s recording %s: found parked at '%s' at tick start - "
                "resuming the finish pipeline from where it stopped",
                LOG_TAG, rid, job["status"],
            )
            finalize_job(rid)
