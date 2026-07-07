import logging
import random
import threading
import time
from datetime import datetime, timedelta, timezone

from django.db import close_old_connections

from . import settings
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

# Name used by the abandoned Celery-PeriodicTask approach above (pre-v0.5.0,
# via core.scheduling.create_or_update_periodic_task). Confirmed on a real
# deployment (Session 41): removing the *code* in Session 22 never removed
# the *database row* itself, so Celery Beat has kept dispatching it against
# an empty registry ever since - "Received unregistered task ... KeyError"
# every time it fires. Harmless (the message is just discarded, and the
# real refresh runs fine via the background thread below), but it's a
# genuine loose end left in the DB, not a code bug to route around.
LEGACY_PERIODIC_TASK_NAME = "catchup_recordarr-archive-refresh"


def _cleanup_legacy_periodic_task():
    """One-time, idempotent: delete the leftover PeriodicTask row from the
    pre-v0.5.0 Celery-based refresh mechanism, if still present. Safe to
    run on every plugin load - a no-op once the row's gone. Also protects
    any other install still carrying the same leftover from an old
    version, not just this deployment.
    """
    try:
        from django_celery_beat.models import PeriodicTask

        deleted, _ = PeriodicTask.objects.filter(name=LEGACY_PERIODIC_TASK_NAME).delete()
        if deleted:
            logger.info(
                "%s removed leftover Celery PeriodicTask '%s' from the "
                "pre-v0.5.0 refresh mechanism (Session 22 replaced it with "
                "a background thread, but never deleted the DB row itself)",
                LOG_TAG, LEGACY_PERIODIC_TASK_NAME,
            )
    except Exception:
        logger.exception("%s legacy PeriodicTask cleanup failed (non-fatal)", LOG_TAG)


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
    if not settings.plugin_enabled():
        # Same Session 25 rule as the takeover receiver and (as of
        # v0.26.0) the tick thread: disabling the plugin doesn't stop
        # this thread, so it must gate itself or a disabled plugin keeps
        # hitting the provider daily.
        return
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


_cleanup_legacy_periodic_task()
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

    # Schema confirmed against Dispatcharr's actual PluginFieldSerializer
    # (apps/plugins/serializers.py) before writing this, not assumed -
    # id/label/type/default/help_text, type one of
    # string/number/boolean/select/text/info. Values persist in
    # PluginConfig.settings (settings.py reads them back for this
    # plugin's own background threads, which never receive an action's
    # context["settings"]).
    fields = [
        {
            "id": "comskip_enabled_default",
            "label": "Run comskip on catchup recordings",
            "type": "boolean",
            "default": False,
            "help_text": (
                "Also requires Dispatcharr's own global DVR comskip setting "
                "(Settings -> DVR) to be enabled - this plugin never "
                "overrides that system-wide switch on its own, only adds "
                "an extra gate on top of it (Section 7)."
            ),
        },
        {
            "id": "grace_period_minutes",
            "label": "Post-air grace period (minutes)",
            "type": "number",
            "default": 15,
            "min": 1,
            "help_text": (
                "How long after a recording's scheduled end time to wait "
                "before treating its catchup window as ready to fetch - "
                "gives the provider's archive time to finalize the "
                "broadcast (Section 5)."
            ),
        },
        {
            "id": "segment_retry_backoff_minutes",
            "label": "Segment retry backoff (minutes)",
            "type": "number",
            "default": 15,
            "min": 1,
            "help_text": (
                "Minimum time between retry attempts on a failed segment "
                "fetch (5 attempts total, hardcoded) - spaces retries out "
                "so the cap doesn't exhaust itself before a slow provider "
                "finishes archiving the window (Section 9)."
            ),
        },
    ]

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
                "Immediately run everything the background tick does: take "
                "over any future scheduled recordings the signal missed, flip "
                "any taken-over recording within its air window to "
                "'interrupted', plan segments for any recording whose window "
                "has closed, recover any orphaned in-progress segments, and "
                "attempt one real segment fetch per ready job - instead of "
                "waiting up to 60 seconds."
            ),
            "button_label": "Run Now",
        },
        {
            "id": "list_pending_segments",
            "label": "List Pending Segments",
            "description": (
                "Show planned segments for every taken-over job that hasn't "
                "reached a terminal state yet - which windows were split into "
                "which chunks, each segment's current status and retry count "
                "- plus any job that has hit the retry cap and been marked "
                "permanently failed."
            ),
            "button_label": "List",
        },
        {
            "id": "test_timeshift_url",
            "label": "Test Timeshift URL Builder",
            "description": (
                "Build both timeshift URL dialects with placeholder values "
                "(no real provider involved) to visually confirm the format "
                "matches Section 8's spec."
            ),
            "button_label": "Test",
        },
        {
            "id": "test_provider_timezone",
            "label": "Test Provider Timezone Resolution",
            "description": (
                "Authenticate to each active Xtream Codes account, resolve its "
                "reported local timezone, and check its reported clock against "
                "ours (Section 10). Makes a real request to your provider(s)."
            ),
            "button_label": "Test",
        },
        {
            "id": "test_dialect_fallback",
            "label": "Test Dialect Fallback Logic",
            "description": (
                "Self-test with mock fetch results (no real provider, no real "
                "account touched) verifying the cold-start default, the "
                "self-healing flip on a successful fallback, and that a "
                "double failure leaves the preference untouched."
            ),
            "button_label": "Test",
        },
        {
            "id": "fetch_test_segment",
            "label": "Fetch One Pending Segment Now",
            "description": (
                "REAL download: claims and fetches the next available segment "
                "across all taken-over jobs from your actual provider (dialect "
                "fallback, resolved UA/timezone), through the same persisted "
                "claim/retry-cap path the automatic background tick uses - "
                "runs it on demand instead of waiting for the next tick pass."
            ),
            "button_label": "Fetch",
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
            # The exact same pass the background loop runs (v0.26.0:
            # includes the missed-takeover sweep) - one shared function,
            # so this action can never drift from the real tick.
            tick._tick_pass()
            log.info("%s status tick run manually", LOG_TAG)
            return {"status": "ok", "message": "Status tick complete (including the missed-recording sweep and a real segment-fetch attempt, if one was claimable) - check logs for details."}

        if action_id == "list_pending_segments":
            recording_ids = state.non_terminal_job_recording_ids()
            lines = []
            if not recording_ids:
                lines.append("No non-terminal jobs found.")
            for rid in recording_ids:
                job = state.get_job(rid)
                job_status = job["status"] if job else "?"
                segments = state.get_segments(rid)
                if not segments:
                    lines.append(f"recording {rid} [{job_status}]: no segments planned yet")
                    continue
                seg_summary = ", ".join(
                    f"#{s['idx']} {s['start_utc']} ({s['duration_minutes']}m) [{s['status']}"
                    + (f", {s['retry_count']} failed attempt(s)]" if s["retry_count"] else "]")
                    for s in segments
                )
                extra = ""
                if job_status in ("stitched", "validated"):
                    extra = f" -> {state.get(f'stitched_output_path:{rid}')}"
                    if job_status == "validated":
                        extra += " (post-stitch validation passed)"
                lines.append(f"recording {rid} [{job_status}] ({len(segments)} segment(s)): {seg_summary}{extra}")

            # Terminal failures don't show up above once
            # non_terminal_job_recording_ids() stops returning them
            # (step 12) - surface them separately so a job hitting the
            # retry cap doesn't just silently vanish (Section 14).
            failed = state.failed_jobs()
            if failed:
                lines.append("")
                lines.append(f"{len(failed)} permanently failed job(s):")
                for job in failed:
                    lines.append(f"  recording {job['recording_id']}: {job['last_error']}")

            return {"status": "ok", "message": "\n".join(lines)}

        if action_id == "test_timeshift_url":
            from .timeshift import build_timeshift_url

            example_start = datetime(2026, 6, 11, 19, 55, 0)
            path_url = build_timeshift_url(
                "http://provider.example:8080", "user", "pass", 12345, example_start, 215, dialect="path"
            )
            php_url = build_timeshift_url(
                "http://provider.example:8080", "user", "pass", 12345, example_start, 215, dialect="php"
            )
            log.info("%s timeshift URL builder tested with placeholder values (no real provider)", LOG_TAG)
            return {
                "status": "ok",
                "message": f"path: {path_url}\nphp: {php_url}",
            }

        if action_id == "test_provider_timezone":
            from apps.m3u.models import M3UAccount

            from .provider import resolve_provider_timezone

            accounts = list(
                M3UAccount.objects.filter(account_type=M3UAccount.Types.XC, is_active=True)
            )
            if not accounts:
                return {
                    "status": "ok",
                    "message": "No active Xtream Codes accounts found.",
                }
            lines = []
            for account in accounts:
                tz = resolve_provider_timezone(account)
                lines.append(f"{account.name}: {tz}")
                log.info("%s account '%s': resolved timezone %s", LOG_TAG, account.name, tz)
            return {
                "status": "ok",
                "message": (
                    "\n".join(lines)
                    + "\n\nCheck logs for a clock-skew warning if the provider's "
                    "reported clock is unexpectedly far from ours."
                ),
            }

        if action_id == "test_dialect_fallback":
            from . import state as _state
            from .dialect import fetch_with_fallback, get_preferred_dialect

            test_id = -1  # synthetic - never a real M3UAccount.id (always positive)
            _state.set_account_dialect(test_id, "unknown", None)  # clean slate

            results = []

            cold = get_preferred_dialect(test_id)
            results.append(
                f"cold-start default: '{cold}' "
                f"[{'PASS' if cold == 'path' else 'FAIL, expected path'}]"
            )

            def fail_all(url):
                return (False, "simulated failure")

            def php_only(url):
                return ("php" in url, "simulated success")

            ok, used, _ = fetch_with_fallback(
                test_id, "test-account", lambda d: f"http://x/{d}", php_only
            )
            after_flip = get_preferred_dialect(test_id)
            results.append(
                f"path fails, php succeeds: success={ok}, used='{used}', "
                f"new preference='{after_flip}' "
                f"[{'PASS' if ok and used == 'php' and after_flip == 'php' else 'FAIL'}]"
            )

            ok2, used2, _ = fetch_with_fallback(
                test_id, "test-account", lambda d: f"http://x/{d}", fail_all
            )
            still_pref = get_preferred_dialect(test_id)
            row = _state.get_account_dialect(test_id)
            results.append(
                f"both dialects fail: success={ok2}, preference unchanged="
                f"{still_pref == 'php'}, consecutive_failures={row['consecutive_failures']} "
                f"[{'PASS' if not ok2 and used2 is None and still_pref == 'php' and row['consecutive_failures'] >= 1 else 'FAIL'}]"
            )

            log.info("%s dialect fallback self-test: %s", LOG_TAG, " | ".join(results))
            return {"status": "ok", "message": "\n".join(results)}

        if action_id == "fetch_test_segment":
            # Step 12 (v0.18.0): this now goes through the exact same
            # claim -> fetch -> mark path the automatic tick uses
            # (tick.fetch_one_segment_now()), rather than its own
            # separate ad-hoc implementation that never persisted a
            # result. Keeping two diverging "fetch a segment" code paths
            # around would have meant they could race each other's claims
            # or disagree about what "pending" means - now there's one.
            log.info("%s manual fetch-one-segment action invoked (REAL download against your provider)", LOG_TAG)
            result = tick.fetch_one_segment_now()
            log.info("%s manual fetch-one-segment result: %s", LOG_TAG, result)
            return {"status": "ok", "message": result}

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
