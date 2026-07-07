"""Section 5 Part A - takeover of scheduled recordings on
catchup-capable channels.

Two entry points share one core (attempt_takeover):

1. A post_save receiver on Recording - fires the moment core schedules a
   recording, revokes the native live-capture task while it's still in
   the future. Core's own schedule_task_on_save receiver is always
   connected before ours (INSTALLED_APPS order, verified - Section 12
   #11), so task_id exists by the time we look for it.

2. A periodic sweep (sweep_missed_takeovers, called from tick.py) for
   recordings the signal never saw - scheduled while the plugin was
   disabled, or before it was installed. Flagged as a known gap in
   Session 25 and deferred to step 6, but step 6 only built post-air
   detection; Session 45's requirements cross-check found the sweep
   itself was never actually built (and Session 41 hit the gap for real
   on a live recording, initially mistaken for a takeover bug).
"""

import logging

from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone

from apps.channels.models import Recording
from apps.channels.signals import revoke_task

from . import state
from ._version import LOG_TAG
from .archive import channel_catchup_info
from .settings import plugin_enabled

logger = logging.getLogger(__name__)

# dispatch_uid makes signal connection idempotent - the plugin loader can
# re-import this module on a forced reload, and without a uid each import
# would stack another live receiver.
DISPATCH_UID = "catchup_recordarr_takeover"


def _channel_catchup_capable(channel):
    """True if any of the channel's streams belongs to an active XC account
    and carries an archive flag (archive.channel_catchup_info - the one
    shared capability check, Section 16 R8). The try/except stays here:
    the receiver path fires on every Recording save and must never let a
    capability-check error propagate into core's save flow.
    """
    try:
        return channel_catchup_info(channel)[0] is not None
    except Exception:
        logger.exception(
            "%s takeover: capability check failed for channel %s",
            LOG_TAG, getattr(channel, "id", "?"),
        )
        return False


def attempt_takeover(instance, source="signal"):
    """The takeover core shared by the receiver and the sweep. Guards
    ordered cheapest-first: the receiver path fires on EVERY Recording
    save, including the frequent progress updates the native recorder
    writes while a live capture runs - those must exit without any DB
    query. Returns True only if a takeover actually happened.
    """
    if not instance.task_id:
        # Core hasn't scheduled this row yet; the receiver fires again on
        # its nested task_id-assignment save. (For the sweep: a future
        # recording with no task_id has nothing to revoke - leave it for
        # a later pass once core has scheduled it.)
        return False
    if instance.start_time is None or instance.start_time <= timezone.now():
        # Already started (or starting this instant): revoking the
        # schedule can't stop a capture Beat may have already dispatched.
        # Only future recordings are taken over; in-progress ones keep
        # their native live path untouched.
        return False
    if state.job_exists(instance.id):
        return False
    if not plugin_enabled():
        return False
    if not _channel_catchup_capable(instance.channel):
        return False

    try:
        revoke_task(instance.task_id)
    except Exception:
        logger.exception(
            "%s takeover: revoke_task failed for recording %s (task_id=%s) - "
            "native live capture remains scheduled",
            LOG_TAG, instance.id, instance.task_id,
        )
        return False

    # Deliberately do NOT clear instance.task_id: schedule_task_on_save
    # only (re-)schedules when task_id is empty, so the stale value is
    # exactly what stops later saves from re-arming native capture
    # (Section 5 Part A).
    state.create_job(instance.id)

    program = (instance.custom_properties or {}).get("program") or {}
    via = "" if source == "signal" else " (caught by the periodic sweep - scheduled while the plugin was disabled or before install, the signal never saw it)"
    logger.info(
        "%s took over recording %s ('%s' on channel '%s', starts %s): "
        "native live capture cancelled, will fetch from catchup archive "
        "after it airs%s",
        LOG_TAG, instance.id,
        program.get("title", "?"),
        getattr(instance.channel, "name", "?"),
        instance.start_time,
        via,
    )
    return True


@receiver(post_save, sender=Recording, dispatch_uid=DISPATCH_UID)
def takeover_on_save(sender, instance, created, **kwargs):
    attempt_takeover(instance, source="signal")


def sweep_missed_takeovers():
    """Section 5 Part A's catch-up path (Session 25's known gap, built
    Session 45): find future-scheduled recordings that aren't in the
    jobs table - the signal never saw them - and run them through the
    same attempt_takeover() core, which re-applies every guard
    (capability, task_id, enabled) per recording. Candidate volume is
    tiny (only future-scheduled recordings), so this runs every tick
    pass rather than on its own slower cadence.
    """
    known_ids = set(state.non_terminal_job_recording_ids())
    candidates = Recording.objects.filter(
        start_time__gt=timezone.now()
    ).select_related("channel")
    for rec in candidates:
        if rec.id in known_ids:
            continue
        attempt_takeover(rec, source="sweep")
