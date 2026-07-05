"""Status-transition tick - resolves open question #13.

Flips a taken-over Recording's status to "interrupted" (+ a friendly
reason) once start_time has passed, so the native UI stops showing a
misleading "Recording" badge. Confirmed on the real deployment
(recording 24) that the badge is purely time-based
(start <= now < end, RecordingCard.jsx) - it has no way to know a
plugin cancelled the native capture, so without this it looks
indistinguishable from an actual live recording in progress.

Standalone thread for now (step 5). Step 6 (Section 5 Part B, the
post-air poll) should fold this into the same tick that detects
"program finished" rather than run two separate schedulers.
"""

import logging
import threading
import time

from django.db import close_old_connections
from django.utils import timezone

from apps.channels.models import Recording

from . import state
from ._version import LOG_TAG

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 60

INTERRUPTED_REASON = (
    "This channel supports catchup/timeshift - Catchup Recordarr will fetch "
    "the finished recording from the provider's archive shortly after it "
    "airs, instead of capturing it live."
)

_tick_started = False
_tick_lock = threading.Lock()


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


def _tick_loop():
    time.sleep(45)
    while True:
        try:
            close_old_connections()
            _mark_interrupted_if_due()
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
