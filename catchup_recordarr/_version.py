VERSION = "0.24.0"
LOG_TAG = f"[Catchup v{VERSION}]"

# The key this plugin is registered under (apps.plugins.models.PluginConfig.key)
# - the plugin folder/slug name, confirmed against Dispatcharr's actual
# source (Session 43, step 18) rather than assumed. Centralized here
# since it's needed both by takeover.py (checking PluginConfig.enabled,
# built step 4) and settings.py (reading PluginConfig.settings, step 18).
PLUGIN_KEY = "catchup_recordarr"
