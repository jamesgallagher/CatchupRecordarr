"""Plugin entry point: the class Dispatcharr's loader discovers, its
settings fields and action definitions, and thin action handlers.

Refactored in v0.27.0 (Section 16 R7/R9): the action if-chain became a
dispatch table with one small handler per action; the ~90 lines of
inlined self-test code moved to selftest.py; and this module's own
background scheduler thread is gone - the daily archive-flag refresh is
now one more check inside tick.py's single leader-gated tick thread,
instead of a second thread with duplicated lifecycle plumbing. (The
original Celery-PeriodicTask approach was abandoned back in Session 22:
Dispatcharr's plugin discovery fires after the Celery Consumer snapshots
its dispatch table, so plugin-registered tasks are structurally
invisible to the worker - hence self-contained threads at all.)
"""

import logging
from datetime import datetime, timezone

from . import selftest
from . import state
from . import takeover  # noqa: F401 - connects the Recording post_save receiver (Section 5 Part A)
from . import tick
from ._version import LOG_TAG, VERSION
from .archive import list_catchup_channels, refresh_archive_flags

logger = logging.getLogger(__name__)

# Name used by the abandoned Celery-PeriodicTask approach (pre-v0.5.0,
# via core.scheduling.create_or_update_periodic_task). Confirmed on a real
# deployment (Session 41): removing the *code* in Session 22 never removed
# the *database row* itself, so Celery Beat kept dispatching it against
# an empty registry - "Received unregistered task ... KeyError" on every
# fire. Harmless but noisy, and permanent without an explicit delete.
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


_cleanup_legacy_periodic_task()
tick.start()


def _handle_ping(params, context, log):
    log.info("%s ping action invoked - plugin is loaded and responding", LOG_TAG)
    try:
        schema_version = state.get("schema_version", "unknown")
        store_status = f"state store OK (schema v{schema_version})"
    except Exception as exc:
        store_status = f"state store ERROR: {exc}"
    return f"Catchup Recordarr v{VERSION} is loaded and responding; {store_status}."


def _handle_refresh_archive_flags(params, context, log):
    log.info("%s archive flag refresh starting (manual action, synchronous)", LOG_TAG)
    refresh_archive_flags()
    state.set(
        "archive_refresh_last_completed_at",
        datetime.now(timezone.utc).isoformat(),
    )
    log.info("%s archive flag refresh complete (manual action)", LOG_TAG)
    return "Archive flag refresh complete - check logs for details."


def _handle_list_catchup_channels(params, context, log):
    channels = list_catchup_channels()
    if not channels:
        return (
            "No catchup-capable channels found. Run 'Refresh Archive "
            "Flags Now' first, and check the provider is configured "
            "as an Xtream Codes account."
        )
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
    return f"{len(channels)} catchup-capable channel(s): {preview}{more}"


def _handle_run_status_tick(params, context, log):
    # The exact same pass the background loop runs - one shared public
    # function (tick.run_once), so this action can never drift from the
    # real tick.
    tick.run_once()
    log.info("%s status tick run manually", LOG_TAG)
    return (
        "Status tick complete (including the missed-recording sweep and a "
        "real segment-fetch attempt, if one was claimable) - check logs "
        "for details."
    )


def _handle_list_pending_segments(params, context, log):
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
        if job_status in ("stitched", "validated") and job:
            extra = f" -> {job['output_path']}"
            if job_status == "validated":
                extra += " (post-stitch validation passed)"
        lines.append(f"recording {rid} [{job_status}] ({len(segments)} segment(s)): {seg_summary}{extra}")

    # Terminal failures don't show up above once
    # non_terminal_job_recording_ids() stops returning them - surface
    # them separately so a job hitting the retry cap doesn't just
    # silently vanish (Section 14).
    failed = state.failed_jobs()
    if failed:
        lines.append("")
        lines.append(f"{len(failed)} permanently failed job(s):")
        for job in failed:
            lines.append(f"  recording {job['recording_id']}: {job['last_error']}")

    return "\n".join(lines)


def _handle_fetch_test_segment(params, context, log):
    log.info("%s manual fetch-one-segment action invoked (REAL download against your provider)", LOG_TAG)
    result = tick.fetch_one_segment_now()
    log.info("%s manual fetch-one-segment result: %s", LOG_TAG, result)
    return result


def _handle_test_timeshift_url(params, context, log):
    return selftest.timeshift_url_selftest()


def _handle_test_provider_timezone(params, context, log):
    return selftest.provider_timezone_selftest()


def _handle_test_dialect_fallback(params, context, log):
    return selftest.dialect_fallback_selftest()


_HANDLERS = {
    "ping": _handle_ping,
    "refresh_archive_flags": _handle_refresh_archive_flags,
    "list_catchup_channels": _handle_list_catchup_channels,
    "run_status_tick": _handle_run_status_tick,
    "list_pending_segments": _handle_list_pending_segments,
    "fetch_test_segment": _handle_fetch_test_segment,
    "test_timeshift_url": _handle_test_timeshift_url,
    "test_provider_timezone": _handle_test_provider_timezone,
    "test_dialect_fallback": _handle_test_dialect_fallback,
}


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
    # plugin's own background thread, which never receives an action's
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
        handler = _HANDLERS.get(action_id)
        if handler is None:
            raise ValueError(f"Unknown action '{action_id}'")
        return {"status": "ok", "message": handler(params, context, log)}
