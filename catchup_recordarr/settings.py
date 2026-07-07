"""Section 15/18 - read this plugin's own configurable settings.

Verified against Dispatcharr's actual source (apps/plugins/models.py,
apps/plugins/loader.py, apps/plugins/serializers.py) before building this,
not assumed - the exact mistake this project got burned by once already
(Session 21/22's Celery PeriodicTask registration failure):

- `Plugin.fields` (plugin.py) is validated against a real
  `PluginFieldSerializer` schema (id/label/type/default/help_text/...).
- The actual persisted values live in `PluginConfig.settings`, a plain
  `JSONField` keyed by field id - no schema enforcement on the stored
  values themselves.
- `context["settings"]` (the version with field defaults merged in via
  `_merge_settings_with_defaults()`) is only ever built inside
  `_build_context()`, which runs for **action invocations** - this
  plugin's own background threads (plugin.py's scheduler, tick.py's
  tick) run independently of any action call and never receive that
  context. They read `PluginConfig` directly here instead, applying the
  same default-fallback logic `_merge_settings_with_defaults()` uses for
  actions.
"""

import logging

from ._version import LOG_TAG, PLUGIN_KEY

logger = logging.getLogger(__name__)


def get_setting(field_id, default):
    """Raw setting value for this plugin, by field id - `default` if
    unset, or if the plugin's config row doesn't exist yet (e.g. before
    it has ever been saved/enabled).
    """
    try:
        from apps.plugins.models import PluginConfig

        cfg = PluginConfig.objects.filter(key=PLUGIN_KEY).only("settings").first()
    except Exception:
        logger.exception(
            "%s could not read plugin settings, using default for '%s'",
            LOG_TAG, field_id,
        )
        return default
    if not cfg or not isinstance(cfg.settings, dict):
        return default
    return cfg.settings.get(field_id, default)


def get_int_setting(field_id, default):
    """Defensive on top of get_setting() - a "number" field's stored
    value isn't schema-enforced (PluginConfig.settings has no type
    validation on the values themselves, only Plugin.fields' own
    definition is validated), so coerce rather than assume int/float.
    """
    value = get_setting(field_id, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning(
            "%s setting '%s' has a non-numeric value (%r) - using default %s",
            LOG_TAG, field_id, value, default,
        )
        return default


def get_bool_setting(field_id, default):
    """Defensive on top of get_setting() - guards against a stored
    string like "false" (truthy in plain Python) being misread as
    enabled, the same class of bug _parse_bool_ish() in archive.py
    exists to prevent for Xtream's own string-typed archive flags.
    """
    value = get_setting(field_id, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)
