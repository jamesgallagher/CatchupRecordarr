import logging
import random
import threading
import time
from datetime import datetime, timedelta, timezone

from django.db import close_old_connections

from . import state
from . import takeover  # noqa: F401 - connects the Recording post_save receiver (Section 5 Part A)
from . import tick
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


def _run_refresh_if_due():
    if not _due_for_refresh():
        return
    # Atomic cross-process claim (state.claim uses BEGIN IMMEDIATE) - every
    # process that loads this plugin runs its own copy of this thread, and
    # they all wake on the same interval, so simultaneous attempts are the
    # expected case, not an edge case.
    if not state.claim("archive_refresh_claimed_at", CLAIM_STALE_AFTER):
        return
    try:
        # This thread's DB connection sits idle for ~24h between runs and
        # may have been closed server-side; drop stale handles so the ORM
        # opens a fresh one (the same pattern Dispatcharr itself uses
        # around plugin actions and discovery).
        close_old_connections()
        logger.info("%s daily archive-flag refresh starting", LOG_TAG)
        refresh_archive_flags()
        state.set(
            "archive_refresh_last_completed_at",
            datetime.now(timezone.utc).isoformat(),
        )
    except Exception:
        logger.exception("%s daily archive-flag refresh failed", LOG_TAG)
    finally:
        close_old_connections()


def _scheduler_loop():
    # Two reasons for the delay: (1) let Django's app-loading sequence fully
    # settle before touching the ORM - this thread starts during
    # PluginsConfig.ready() (transitively), before Django guarantees every
    # app is finished loading (same reasoning as Sportarr's own 30s startup
    # delay); (2) the random jitter staggers the many independent copies of
    # this thread (4 uWSGI workers under lazy-apps, two Celery workers,
    # beat, daphne - all load this module at boot) so they don't all hit
    # the claim lock at the same instant. The atomic claim is the actual
    # guard; jitter just avoids pointless contention.
    time.sleep(30 + random.uniform(0, 90))
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
tick.start()


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
        {
            "id": "list_catchup_channels",
            "label": "List Catchup Channels",
            "description": (
                "Show which channels support catchup/timeshift and how many days "
                "of archive each provides. Full list is also written to the logs."
            ),
            "button_label": "List",
        },
        {
            "id": "run_status_tick",
            "label": "Run Status Tick Now",
            "description": (
                "Immediately flip any taken-over recording within its air window "
                "to 'interrupted', and log any recording whose window has closed "
                "as ready for a catchup fetch (detection only - no download "
                "pipeline yet), instead of waiting up to 60 seconds."
            ),
            "button_label": "Run Now",
        },
    ]

    def run(self, action_id, params, context):
        log = context.get("logger", logger)

        if action_id == "ping":
            log.info("%s ping action invoked - plugin is loaded and responding", LOG_TAG)
            try:
                schema_version = state.get("schema_version", "unknown")
                store_status = f"state store OK (schema v{schema_version})"
            except Exception as exc:
                store_status = f"state store ERROR: {exc}"
            return {
                "status": "ok",
                "message": f"Catchup Recordarr v{VERSION} is loaded and responding; {store_status}.",
            }

        if action_id == "run_status_tick":
            tick._mark_interrupted_if_due()
            tick._check_post_air_ready()
            log.info("%s status tick run manually", LOG_TAG)
            return {"status": "ok", "message": "Status tick complete - check logs for details."}

        if action_id == "list_catchup_channels":
            from .archive import list_catchup_channels

            channels = list_catchup_channels()
            if not channels:
                return {
                    "status": "ok",
                    "message": (
                        "No catchup-capable channels found. Run 'Refresh Archive "
                        "Flags Now' first, and check the provider is configured "
                        "as an Xtream Codes account."
                    ),
                }
            for num, name, days in channels:
                log.info(
                    "%s catchup-capable: [%s] %s (%sd retention)",
                    LOG_TAG, num, name, days,
                )
            preview = ", ".join(f"{name} ({days}d)" for _, name, days in channels[:12])
            more = (
                f" ... and {len(channels) - 12} more (full list in logs)"
                if len(channels) > 12
                else ""
            )
            return {
                "status": "ok",
                "message": f"{len(channels)} catchup-capable channel(s): {preview}{more}",
            }

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
