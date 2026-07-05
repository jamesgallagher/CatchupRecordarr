"""Section 3 - Archive Detection.

Daily job: re-fetch each active Xtream Codes (XC) M3UAccount's live-stream
list and surgically update tv_archive/tv_archive_duration on each Stream's
custom_properties, without touching anything else the normal M3U sync
already populated there.
"""

import logging

from apps.channels.models import Stream
from apps.m3u.models import M3UAccount
from core.xtream_codes import Client as XtreamClient

from ._version import LOG_TAG
from .errors import safe_error_string

logger = logging.getLogger(__name__)


def _parse_bool_ish(value):
    """XC archive values land in custom_properties as strings (e.g. "1"/"0"),
    stringified during the normal M3U sync - a bare truthy check on the
    string would treat "0" as True, so parse it as an int explicitly.
    """
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def stream_is_catchup_capable(stream):
    """Section 3's only eligibility gate: tv_archive > 0 on the stream."""
    cp = stream.custom_properties or {}
    return _parse_bool_ish(cp.get("tv_archive", 0))


def catchup_capable_stream_for_channel(channel):
    """The channel's first catchup-capable stream on an active XC account,
    or None. Same eligibility gate as stream_is_catchup_capable, resolved
    down to the actual Stream object (not just a bool) - needed once
    something has to actually fetch from it (stream_id, m3u_account).
    """
    for s in channel.streams.filter(
        m3u_account__account_type="XC", m3u_account__is_active=True
    ):
        if stream_is_catchup_capable(s):
            return s
    return None


def channel_archive_retention_days(channel):
    """Max tv_archive_duration across the channel's catchup-capable
    streams, 0 if none - the outer edge of Section 5 Part B's lookback
    window (no point treating a window as fetchable once it's aged out
    of the provider's own archive retention).
    """
    days = 0
    for s in channel.streams.all():
        cp = s.custom_properties or {}
        if _parse_bool_ish(cp.get("tv_archive", 0)):
            try:
                days = max(days, int(cp.get("tv_archive_duration", 0)))
            except (TypeError, ValueError):
                pass
    return days


def list_catchup_channels():
    """Every channel with at least one catchup-capable stream on an active
    XC account, as [(channel_number, name, retention_days), ...] ordered by
    channel number. Retention is the max across the channel's streams.
    """
    from apps.channels.models import Channel

    channels = (
        Channel.objects.filter(
            streams__m3u_account__account_type="XC",
            streams__m3u_account__is_active=True,
        )
        .distinct()
        .prefetch_related("streams")
        .order_by("channel_number")
    )

    results = []
    for channel in channels:
        capable = False
        days = 0
        for s in channel.streams.all():
            cp = s.custom_properties or {}
            if _parse_bool_ish(cp.get("tv_archive", 0)):
                capable = True
                try:
                    days = max(days, int(cp.get("tv_archive_duration", 0)))
                except (TypeError, ValueError):
                    pass
        if capable:
            results.append((channel.channel_number, channel.name, days))
    return results


def refresh_archive_flags():
    """Refresh tv_archive/tv_archive_duration for every active XC account's
    streams. Empty/failed fetch never clears existing flags - a provider
    hiccup shouldn't silently downgrade catchup capability (Section 3).
    """
    accounts = list(
        M3UAccount.objects.filter(
            account_type=M3UAccount.Types.XC,
            is_active=True,
        )
    )

    # Log the empty case explicitly - a silent instant no-op here cost a
    # real debugging round (looked identical to a failure from the outside).
    if not accounts:
        logger.info(
            "%s no active Xtream Codes (XC) accounts found - nothing to refresh. "
            "Catchup requires the provider to be configured as an Xtream Codes "
            "account type, not a plain M3U URL.",
            LOG_TAG,
        )
        return

    for account in accounts:
        try:
            with XtreamClient(
                account.server_url,
                account.username,
                account.password,
                account.get_user_agent(),
            ) as client:
                streams = client.get_all_live_streams()
        except Exception as exc:
            # safe_error_string(), never str(exc) - XtreamClient wraps
            # requests exceptions that can embed the account's credentials
            # in the request URL (Section 14).
            logger.warning(
                "%s account '%s': archive flag refresh failed, keeping existing flags: %s",
                LOG_TAG, account.name, safe_error_string(exc),
            )
            continue

        if not streams:
            logger.warning(
                "%s account '%s': archive flag refresh returned no streams, keeping existing flags",
                LOG_TAG, account.name,
            )
            continue

        archive_by_stream_id = {}
        for s in streams:
            try:
                sid = int(s.get("stream_id"))
            except (TypeError, ValueError):
                continue
            archive_by_stream_id[sid] = (
                _parse_bool_ish(s.get("tv_archive", 0)),
                s.get("tv_archive_duration", 0),
            )

        updated = 0
        db_streams = Stream.objects.filter(m3u_account=account, stream_id__isnull=False)
        for stream in db_streams:
            info = archive_by_stream_id.get(stream.stream_id)
            if info is None:
                continue
            has_archive, duration = info
            cp = stream.custom_properties or {}
            new_archive_str = "1" if has_archive else "0"
            new_duration_str = str(duration)
            if cp.get("tv_archive") != new_archive_str or str(cp.get("tv_archive_duration")) != new_duration_str:
                cp["tv_archive"] = new_archive_str
                cp["tv_archive_duration"] = new_duration_str
                stream.custom_properties = cp
                stream.save(update_fields=["custom_properties"])
                updated += 1

        catchup_capable = sum(1 for v in archive_by_stream_id.values() if v[0])
        logger.info(
            "%s account '%s': refreshed archive flags, %d stream(s) updated, "
            "%d catchup-capable of %d total",
            LOG_TAG, account.name, updated, catchup_capable, len(archive_by_stream_id),
        )
