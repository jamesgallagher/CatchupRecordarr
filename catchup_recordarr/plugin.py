import logging

from core.scheduling import create_or_update_periodic_task

from .tasks import ARCHIVE_REFRESH_TASK_PATH

logger = logging.getLogger(__name__)

ARCHIVE_REFRESH_TASK_NAME = "catchup_recordarr-archive-refresh"


def _register_periodic_tasks():
    """Register our own periodic tasks (Section 2/3) - idempotent, safe to
    call on every plugin (re)load, matching how core Dispatcharr registers
    its own plugin-repo-refresh periodic task at app startup.
    """
    try:
        create_or_update_periodic_task(
            task_name=ARCHIVE_REFRESH_TASK_NAME,
            celery_task_path=ARCHIVE_REFRESH_TASK_PATH,
            interval_hours=24,
            enabled=True,
        )
    except Exception:
        logger.exception("[Catchup] failed to register the daily archive-flag refresh task")


_register_periodic_tasks()


class Plugin:
    """Entry point Dispatcharr's plugin loader discovers (apps/plugins/loader.py).

    Instantiated with no args; name/version/description/author/help_url/
    fields/actions are read via getattr with defaults, so plain class
    attributes are enough.
    """

    name = "Catchup Recordarr"
    version = "0.4.0"
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
                "instead of waiting for the daily job."
            ),
            "button_label": "Refresh Now",
        },
    ]

    def run(self, action_id, params, context):
        log = context.get("logger", logger)

        if action_id == "ping":
            log.info("[Catchup] ping action invoked - plugin is loaded and responding")
            return {"status": "ok", "message": "Catchup Recordarr is loaded and responding."}

        if action_id == "refresh_archive_flags":
            from .tasks import refresh_archive_flags
            refresh_archive_flags.delay()
            log.info("[Catchup] archive flag refresh queued via manual action")
            return {
                "status": "ok",
                "message": "Archive flag refresh queued - check logs shortly for results.",
            }

        raise ValueError(f"Unknown action '{action_id}'")
