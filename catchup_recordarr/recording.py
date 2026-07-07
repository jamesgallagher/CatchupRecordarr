"""Section 7 - update the taken-over Recording row in place on success.

The plugin never creates its own Recording - it takes over a native row
(Section 4/5) and, once the file is fully downloaded, stitched, AND
validated (Section 9/10/14 - steps 12-14), updates that same row's
custom_properties to the shape the native live-capture pipeline already
produces, so playback and comskip work exactly as they do for any other
recording with zero plugin-specific handling needed on the read side.

Critical ordering rule (Section 7): do NOT call mark_recording_completed()
until the file is fully downloaded, stitched, and validated - the
playback endpoint only checks os.path.exists + non-zero size, not
validity, so exposing a partial/corrupt file as "completed" early would
serve a broken recording to the user. tick.py only reaches this once a
job's status is already 'validated' (step 14), never earlier.
"""

import logging
import os

from django.utils import timezone

from apps.channels.tasks import comskip_process_recording
from core.models import CoreSettings

from ._version import LOG_TAG
from .settings import get_bool_setting

logger = logging.getLogger(__name__)

# Session 11 (resolved): a small title prefix is the agreed visual
# distinction for a catchup-fetched recording, plugin-only, no frontend
# change - checked both RecordingCard.jsx and RecordingDetailsModal.jsx
# at the time and found no generic property-rendering surface, so a real
# badge would have needed editing frontend source (off the table).
CATCHUP_TITLE_PREFIX = "[Catchup] "


def mark_recording_completed(rec, output_path):
    """Update rec (a Recording instance) in place to look exactly like a
    finished native recording. Merges custom_properties rather than
    overwriting it wholesale - Session 8 (#14) found RecurringRecordingRule
    recordings carry a 'rule' marker the frontend reads for its own
    "Recurring" badge, which a naive overwrite would silently strip.

    Returns True on success, False if the save itself failed - logged
    with the recording's identity here rather than left to raise up into
    tick.py's tick-level try/except, whose generic "status-transition
    tick error" catch-all (Section 14) would otherwise be the only trace
    of a failure that's actually specific to one recording. Matches
    every other provider/mechanic function in this codebase
    (download.fetch_segment(), stitch.stitch_segments(),
    validate.validate_output()) - return a result, never raise.
    """
    cp = dict(rec.custom_properties or {})

    program = dict(cp.get("program") or {})
    if not program.get("title"):
        # Section 5's designed fallback for a Recording with no EPG tie
        # at all (Session 10, #2) - every prior read of this field was
        # just a "?" log placeholder, so this fallback was specified but
        # never actually had anywhere that wrote user-visible data until
        # now.
        channel_name = rec.channel.name if rec.channel else "Recording"
        program.setdefault(
            "title", f"{channel_name} — {rec.start_time:%Y-%m-%d %H:%M}"
        )
        program.setdefault("description", "Catchup recording")

    base_title = program["title"]
    if not base_title.startswith(CATCHUP_TITLE_PREFIX):
        program["title"] = f"{CATCHUP_TITLE_PREFIX}{base_title}"
    cp["program"] = program

    cp["status"] = "completed"
    cp["file_path"] = output_path
    cp["file_name"] = os.path.basename(output_path)
    cp["file_url"] = f"/api/channels/recordings/{rec.id}/file/"
    cp["output_file_url"] = cp["file_url"]
    cp["ended_at"] = timezone.now().isoformat()
    cp["bytes_written"] = os.path.getsize(output_path)
    cp["remux_success"] = True
    cp.pop("interrupted_reason", None)  # no longer meaningful once completed

    rec.custom_properties = cp
    try:
        rec.save(update_fields=["custom_properties"])
    except Exception:
        # Local DB write, not a provider-facing call - safe to log the
        # full exception directly (Section 14's safe_error_string() rule
        # is specifically about calls that can embed provider
        # credentials in their own str(), which this isn't).
        logger.exception(
            "%s recording %s: failed to save the Recording row as "
            "completed - the file itself is fully downloaded, stitched, "
            "and validated, but Dispatcharr won't show it as finished "
            "until this save succeeds",
            LOG_TAG, rec.id,
        )
        return False

    logger.info(
        "%s recording %s: Recording row updated to completed - %s (%d bytes)",
        LOG_TAG, rec.id, output_path, cp["bytes_written"],
    )
    return True


def mark_recording_failed(recording_id, reason):
    """Section 10: "Failure surfaced by setting
    custom_properties['status'] = 'failed' with a reason string on the
    taken-over Recording row - renders correctly through the existing
    native UI with zero UI changes." Specified from the start, but
    Session 45's requirements cross-check found no failure path ever
    actually called anything like this - a permanently-failed job only
    updated the plugin's own SQLite, leaving the native row saying
    "Interrupted - will be fetched from the provider's catchup archive
    shortly" forever, which becomes a lie the moment the job gives up.

    Takes the id (not an instance) and loads fresh - failure sites often
    don't have a Recording loaded, and one that was loaded earlier in
    the pass may be stale. Merges custom_properties (same #14 rule as
    mark_recording_completed). Returns True on success; never raises
    (same convention as everything else in this module).
    """
    from apps.channels.models import Recording

    rec = Recording.objects.filter(id=recording_id).first()
    if rec is None:
        return False  # deleted out from under us - reaper's problem, not an error

    cp = dict(rec.custom_properties or {})
    cp["status"] = "failed"
    cp["failure_reason"] = reason
    # Replace (never leave) the takeover-time "will be fetched shortly"
    # text - the details modal has a display slot for this field
    # (RecordingCard.jsx:488, #13), so this is the human-visible line.
    cp["interrupted_reason"] = f"Catchup fetch failed permanently: {reason}"

    rec.custom_properties = cp
    try:
        rec.save(update_fields=["custom_properties"])
    except Exception:
        logger.exception(
            "%s recording %s: failed to save the Recording row as failed "
            "(reason was: %s)",
            LOG_TAG, recording_id, reason,
        )
        return False

    logger.info(
        "%s recording %s: Recording row updated to failed - %s",
        LOG_TAG, recording_id, reason,
    )
    return True


def maybe_queue_comskip(rec):
    """Queue the native comskip_process_recording task - unmodified, the
    same Celery task the live-capture pipeline calls - only when BOTH the
    existing global Dispatcharr DVR-comskip switch and this plugin's own
    setting are true (Section 7). Deliberately not letting the plugin's
    own flag act alone: overriding an operator's system-wide comskip-off
    choice would be a surprising override of stated intent.

    Section 7 also specifies an optional per-channel override
    (`custom_properties["catchup_recordarr"]["comskip_enabled"]`) that
    would take precedence over the `comskip_enabled_default` setting -
    not implemented here. Unlike every other custom_properties reference in
    this codebase (Stream, Recording), it was never actually confirmed
    against Dispatcharr's source whether Channel even has a
    custom_properties field to hold it - the design doc itself flags
    this as speculative ("kept as an easy follow-up, not designed
    further"). Flagged rather than guessed at - verify against
    Dispatcharr's real Channel model before building this, don't assume
    a field exists.
    """
    if not CoreSettings.get_dvr_comskip_enabled():
        return
    if not get_bool_setting("comskip_enabled_default", False):
        return

    try:
        comskip_process_recording.delay(rec.id)
    except Exception:
        # Non-fatal either way: the recording itself is already marked
        # completed and playable (mark_recording_completed() already
        # succeeded by the time this is called) - comskip is a nice-to-
        # have on top, not a precondition for the recording being usable.
        logger.exception(
            "%s recording %s: failed to queue comskip processing (non-fatal "
            "- the recording is still marked completed and playable)",
            LOG_TAG, rec.id,
        )
        return

    logger.info("%s recording %s: queued for comskip processing", LOG_TAG, rec.id)
