"""Section 5 Part A - real-time takeover of scheduled recordings on
catchup-capable channels.

A post_save receiver on Recording revokes the native live-capture task
the moment one is scheduled on a channel whose stream has an archive
(tv_archive > 0), and records it as a pending catchup job in the state
store. Core's own schedule_task_on_save receiver is always connected
before ours (INSTALLED_APPS order, verified - Section 12 #11), so
task_id exists by the time we look for it.
"""

import logging

from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone

from apps.channels.models import Recording
from apps.channels.signals import revoke_task

from . import state
from ._version import LOG_TAG
from .archive import stream_is_catchup_capable

logger = logging.getLogger(__name__)

# dispatch_uid makes signal connection idempotent - the plugin loader can
# re-import this module on a forced reload, and without a uid each import
# would stack another live receiver.
DISPATCH_UID = "catchup_recordarr_takeover"

PLUGIN_KEY = "catchup_recordarr"


def _plugin_enabled():
    """Signals stay connected even when the plugin is disabled in the UI
    (the module remains imported) - a disabled plugin must not keep
    cancelling captures, so check the real config row before acting.
    """
    try:
        from apps.plugins.models import PluginConfig

        cfg = PluginConfig.objects.filter(key=PLUGIN_KEY).only("enabled").first()
        return bool(cfg and cfg.enabled)
    except Exception:
        return False


def _channel_catchup_capable(channel):
    """True if any of the channel's streams belongs to an active XC account
    and carries an archive flag. Checked in Python rather than a JSON
    filter: values are stored as strings in custom_properties and the
    per-channel stream count is small.
    """
    if channel is None:
        return False
    try:
        streams = channel.streams.filter(
            m3u_account__account_type="XC",
            m3u_account__is_active=True,
        )
        return any(stream_is_catchup_capable(s) for s in streams)
    except Exception:
        logger.exception(
            "%s takeover: capability check failed for channel %s",
            LOG_TAG, getattr(channel, "id", "?"),
        )
        return False


@receiver(post_save, sender=Recording, dispatch_uid=DISPATCH_UID)
def takeover_on_save(sender, instance, created, **kwargs):
    # Guards ordered cheapest-first: this fires on EVERY Recording save,
    # including the frequent progress updates the native recorder writes
    # while a live capture runs - those must exit without any DB query.
    if not instance.task_id:
        # Core hasn't scheduled this row yet; we fire again on its nested
        # task_id-assignment save.
        return
    if instance.start_time is None or instance.start_time <= timezone.now():
        # Already started (or starting this instant): revoking the
        # schedule can't stop a capture Beat may have already dispatched.
        # Only future recordings are taken over; in-progress ones keep
        # their native live path untouched.
        return
    if state.job_exists(instance.id):
        return
    if not _plugin_enabled():
        return
    if not _channel_catchup_capable(instance.channel):
        return

    try:
        revoke_task(instance.task_id)
    except Exception:
        logger.exception(
            "%s takeover: revoke_task failed for recording %s (task_id=%s) - "
            "native live capture remains scheduled",
            LOG_TAG, instance.id, instance.task_id,
        )
        return

    # Deliberately do NOT clear instance.task_id: schedule_task_on_save
    # only (re-)schedules when task_id is empty, so the stale value is
    # exactly what stops later saves from re-arming native capture
    # (Section 5 Part A).
    state.create_job(instance.id)

    program = (instance.custom_properties or {}).get("program") or {}
    logger.info(
        "%s took over recording %s ('%s' on channel '%s', starts %s): "
        "native live capture cancelled, will fetch from catchup archive after it airs",
        LOG_TAG, instance.id,
        program.get("title", "?"),
        getattr(instance.channel, "name", "?"),
        instance.start_time,
    )
