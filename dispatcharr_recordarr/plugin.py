import logging

logger = logging.getLogger(__name__)


class Plugin:
    """Entry point Dispatcharr's plugin loader discovers (apps/plugins/loader.py).

    Instantiated with no args; name/version/description/author/help_url/
    fields/actions are read via getattr with defaults, so plain class
    attributes are enough.
    """

    name = "DispatcharrRecordarr"
    version = "0.1.0"
    description = (
        "Detects catchup/timeshift-capable channels and fulfills scheduled "
        "recordings from the provider's archive instead of live capture."
    )
    author = "James"
    help_url = "https://github.com/jamesgallagher/DispatcharrRecordarr"

    fields = []

    actions = [
        {
            "id": "ping",
            "label": "Ping",
            "description": "Verify the plugin is loaded and responding.",
            "button_label": "Ping",
        },
    ]

    def run(self, action_id, params, context):
        log = context.get("logger", logger)

        if action_id == "ping":
            log.info("[Catchup] ping action invoked - plugin is loaded and responding")
            return {"status": "ok", "message": "DispatcharrRecordarr is loaded and responding."}

        raise ValueError(f"Unknown action '{action_id}'")
