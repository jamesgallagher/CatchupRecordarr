"""Section 8 - User-Agent + provider timezone resolution.

Also implements Section 10's clock/timezone sanity check at the same
point, since both come from the same Xtream auth response - no reason to
authenticate twice.
"""

import logging
from datetime import datetime
from datetime import timezone as dt_timezone
from zoneinfo import ZoneInfo

from django.utils import timezone as django_timezone

from core.xtream_codes import Client as XtreamClient

from ._version import LOG_TAG

logger = logging.getLogger(__name__)

# Section 10 - normal clock drift is seconds, maybe a couple minutes
# without NTP; a genuine timezone-resolution bug shows up as tens of
# minutes to hours. This tolerance is deliberately generous so we only
# warn on the latter, not routine drift.
CLOCK_SKEW_WARN_THRESHOLD_SECONDS = 15 * 60

# Cached in memory for the process lifetime, same approach Sportarr uses
# (keyed by server URL there; by M3UAccount.id here) - resolving a
# timezone costs an authenticate round-trip, and it doesn't change
# between segments of the same job or between jobs on the same account.
_timezone_cache = {}


def resolve_user_agent(m3u_account):
    """The same call Dispatcharr's own live-viewing proxy path uses
    (apps/proxy/live_proxy/url_utils.py), so this plugin presents itself
    to the provider identically to native playback.
    """
    return m3u_account.get_user_agent().user_agent


def resolve_provider_timezone(m3u_account):
    """Resolve the provider's local timezone from its Xtream auth
    response (server_info.timezone, an IANA name). Falls back to UTC if
    the provider doesn't report one or it doesn't resolve - many
    providers run their panels on UTC anyway, so this is often exact.
    """
    cached = _timezone_cache.get(m3u_account.id)
    if cached is not None:
        return cached

    tz = ZoneInfo("UTC")
    try:
        with XtreamClient(
            m3u_account.server_url,
            m3u_account.username,
            m3u_account.password,
            m3u_account.get_user_agent(),
        ) as client:
            auth = client.authenticate()
    except Exception as exc:
        logger.warning(
            "%s account '%s': could not authenticate to resolve timezone, "
            "assuming UTC: %s",
            LOG_TAG, m3u_account.name, exc,
        )
        _timezone_cache[m3u_account.id] = tz
        return tz

    server_info = (auth or {}).get("server_info") or {}
    tz_name = server_info.get("timezone")
    if tz_name:
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            logger.warning(
                "%s account '%s': provider reported unrecognized timezone "
                "'%s', assuming UTC",
                LOG_TAG, m3u_account.name, tz_name,
            )

    _check_clock_skew(m3u_account, server_info)

    _timezone_cache[m3u_account.id] = tz
    return tz


def _check_clock_skew(m3u_account, server_info):
    """Section 10 - the one check that targets the actual worrying failure
    mode directly (a timezone bug producing a technically-successful
    download of the wrong window) rather than a proxy for it.
    """
    raw_ts = server_info.get("timestamp_now")
    if not raw_ts:
        return  # best-effort - not every panel reports this reliably
    try:
        provider_now = datetime.fromtimestamp(int(raw_ts), tz=dt_timezone.utc)
    except (TypeError, ValueError, OSError):
        return

    our_now = django_timezone.now()
    skew_seconds = abs((provider_now - our_now).total_seconds())
    if skew_seconds > CLOCK_SKEW_WARN_THRESHOLD_SECONDS:
        logger.warning(
            "%s account '%s': provider's reported clock is %.0f minutes off "
            "from ours (provider=%s, us=%s) - this is exactly the failure "
            "mode that can silently corrupt a catchup window's timing; every "
            "download for this account is suspect until this is resolved "
            "(check NTP sync on both ends)",
            LOG_TAG, m3u_account.name, skew_seconds / 60, provider_now, our_now,
        )
