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

from ._version import LOG_TAG

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
    rec.save(update_fields=["custom_properties"])
    logger.info(
        "%s recording %s: Recording row updated to completed - %s (%d bytes)",
        LOG_TAG, rec.id, output_path, cp["bytes_written"],
    )
