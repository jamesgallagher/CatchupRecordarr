import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from . import state
from ._version import LOG_TAG, VERSION
from .archive import refresh_archive_flags

logger = logging.getLogger(__name__)

# Superseded, Session 21: originally registered as a Celery task via
# core.scheduling.create_or_update_periodic_task, matching Section 2's
# "plugin can register its own periodic Celery task" claim. Verified on a
# real deployment that this doesn't reliably work: Dispatcharr's own
# plugin-discovery-on-worker_ready hook (dispatcharr/celery.py) fires
# after the Celery Consumer has already built its dispatch table from
# app.tasks, so a task registered by a plugin at worker_ready time is
# invisible to that table for the lifetime of the worker process -
# confirmed on a genuine restart, and confirmed it wasn't about *how* the
# task was bound (tried both @shared_task and binding directly to
# dispatcharr.celery.app - identical failure either way). Since this
# would affect every future Celery task this plugin might register, not
# just this one, replaced with a self-contained background thread that
# doesn't touch Celery's task registry at all.
CHECK_INTERVAL_SECONDS = 30 * 60
REFRESH_INTERVAL = timedelta(hours=24)
CLAIM_STALE_AFTER = timedelta(minutes=10)

_scheduler_started = False
_scheduler_lock = threading.Lock()


def _due_for_refresh():
    last_completed = state.get("archive_refresh_last_completed_at")
    if not last_completed:
        return True
    try:
        last_dt = datetime.fromisoformat(last_completed)
    except ValueError:
        return True
    return datetime.now(timezone.utc) - last_dt >= REFRESH_INTERVAL


def _claim_refresh():
    """Best-effort claim, not a strict distributed lock - separate
    check-then-set across processes/threads, so a tiny race window exists
    where two processes could both start a refresh around the same
    moment. Accepted: worst case is a harmless redundant API call, not
    worth a heavier locking scheme for.
    """
    claimed_at = state.get("archive_refresh_claimed_at")
    if claimed_at:
        try:
            claimed_dt = datetime.fromisoformat(claimed_at)
            if datetime.now(timezone.utc) - claimed_dt < CLAIM_STALE_AFTER:
                return False
        except ValueError:
            pass
    state.set("archive_refresh_claimed_at", datetime.now(timezone.utc).isoformat())
    return True


def _run_refresh_if_due():
    if not _due_for_refresh():
        return
    if not _claim_refresh():
        return
    try:
        logger.info("%s daily archive-flag refresh starting", LOG_TAG)
        refresh_archive_flags()
        state.set(
            "archive_refresh_last_completed_at",
            datetime.now(timezone.utc).isoformat(),
        )
    except Exception:
        logger.exception("%s daily archive-flag refresh failed", LOG_TAG)


def _scheduler_loop():
    # Let Django's app-loading sequence fully settle before touching the ORM -
    # this thread starts during PluginsConfig.ready() (transitively), before
    # Django guarantees every app is finished loading. Same reasoning as
    # Sportarr's own CatchupDownloadService, which delays 30s before its
    # first tick for the same reason.
    time.sleep(30)
    while True:
        try:
            _run_refresh_if_due()
        except Exception:
            logger.exception("%s scheduler loop error", LOG_TAG)
        time.sleep(CHECK_INTERVAL_SECONDS)


def _start_scheduler():
    global _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        _scheduler_started = True
        thread = threading.Thread(
            target=_scheduler_loop, daemon=True, name="catchup-recordarr-scheduler"
        )
        thread.start()
        logger.info("%s background scheduler thread started", LOG_TAG)


_start_scheduler()


class Plugin:
    """Entry point Dispatcharr's plugin loader discovers (apps/plugins/loader.py).

    Instantiated with no args; name/version/description/author/help_url/
    fields/actions are read via getattr with defaults, so plain class
    attributes are enough.
    """

    name = "Catchup Recordarr"
    version = VERSION
    description = (
        "Detects catchup/timeshift-capable channels and fulfills scheduled "
        "recordings from the provider's archive instead of live capture."
    )
    author = "James"
    help_url = "https://github.com/jamesgallagher/CatchupRecordarr"

    fields = []

    actions = [
        {
            "id": "ping",
            "label": "Ping",
            "description": "Verify the plugin is loaded and responding.",
            "button_label": "Ping",
        },
        {
            "id": "refresh_archive_flags",
            "label": "Refresh Archive Flags Now",
            "description": (
                "Immediately re-check which channels support catchup/timeshift, "
                "instead of waiting for the daily job. Runs synchronously - the "
                "button will wait for it to finish."
            ),
            "button_label": "Refresh Now",
        },
    ]

    def run(self, action_id, params, context):
        log = context.get("logger", logger)

        if action_id == "ping":
            log.info("%s ping action invoked - plugin is loaded and responding", LOG_TAG)
            return {"status": "ok", "message": f"Catchup Recordarr v{VERSION} is loaded and responding."}

        if action_id == "refresh_archive_flags":
            log.info("%s archive flag refresh starting (manual action, synchronous)", LOG_TAG)
            refresh_archive_flags()
            state.set(
                "archive_refresh_last_completed_at",
                datetime.now(timezone.utc).isoformat(),
            )
            log.info("%s archive flag refresh complete (manual action)", LOG_TAG)
            return {
                "status": "ok",
                "message": "Archive flag refresh complete - check logs for details.",
            }

        raise ValueError(f"Unknown action '{action_id}'")
