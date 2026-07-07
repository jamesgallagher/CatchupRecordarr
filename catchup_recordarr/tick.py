"""Status-transition tick (#13) + post-air detection (Section 5 Part B).

Two checks on one shared schedule, folded into a single thread
deliberately (noted as a TODO when step 5 was built standalone, done now
that step 6 exists):

1. Flip a taken-over Recording's status to "interrupted" (+ a friendly
   reason) once start_time has passed, so the native UI stops showing a
   misleading "Recording" badge. Confirmed on the real deployment that
   the badge is purely time-based (start <= now < end, RecordingCard.jsx)
   - it has no way to know a plugin cancelled the native capture.

2. Detect when a taken-over recording's window has closed (+ grace),
   plan its segments (Section 9, step 10) if not already planned, and
   log that it's ready for a catchup fetch. No actual fetching yet -
   that's step 11.
"""

import logging
import threading
import time
from datetime import timedelta

from django.db import close_old_connections
from django.utils import timezone

from apps.channels.models import Recording

from . import state
from ._version import LOG_TAG
from .archive import channel_archive_retention_days
from .planning import SEGMENT_MINUTES, plan_segments

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
GRACE_PERIOD = timedelta(minutes=15)

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
        retention_days = (
            channel_archive_retention_days(rec.channel) if rec.channel else 0
        )
        if retention_days and rec.end_time < now - timedelta(days=retention_days):
            # Job-level retention cutoff/permanent-failure handling is
            # step 15 - not built yet. Log once, don't spam every tick.
            if not state.get(f"post_air_retention_warned:{rec.id}"):
                logger.warning(
                    "%s recording %s: window has aged out of the provider's "
                    "%sd archive retention before being fetched - job-level "
                    "retention cutoff/failure handling not built yet (step 15)",
                    LOG_TAG, rec.id, retention_days,
                )
                state.set(f"post_air_retention_warned:{rec.id}", "1")
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

        # Log "ready" once per job, not every tick, since nothing fetches
        # a planned segment yet (step 11 isn't built) - without this a
        # job would otherwise re-log every 60s for as long as its
        # retention window stays open, potentially days.
        if state.get(f"post_air_ready_logged:{rec.id}"):
            continue
        program = (rec.custom_properties or {}).get("program") or {}
        logger.info(
            "%s recording %s ('%s' on channel '%s') ready for catchup fetch "
            "(window closed %s ago) - segments planned, no fetch yet "
            "(step 11 not built)",
            LOG_TAG, rec.id, program.get("title", "?"),
            getattr(rec.channel, "name", "?"), now - rec.end_time,
        )
        state.set(f"post_air_ready_logged:{rec.id}", "1")


def _tick_loop():
    time.sleep(45)
    while True:
        try:
            close_old_connections()
            _reap_orphaned_jobs()
            _mark_interrupted_if_due()
            _check_post_air_ready()
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
