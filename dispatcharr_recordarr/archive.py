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


def refresh_archive_flags():
    """Refresh tv_archive/tv_archive_duration for every active XC account's
    streams. Empty/failed fetch never clears existing flags - a provider
    hiccup shouldn't silently downgrade catchup capability (Section 3).
    """
    accounts = M3UAccount.objects.filter(
        account_type=M3UAccount.Types.XC,
        is_active=True,
    )

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
            logger.warning(
                "[Catchup] account '%s': archive flag refresh failed, keeping existing flags: %s",
                account.name, exc,
            )
            continue

        if not streams:
            logger.warning(
                "[Catchup] account '%s': archive flag refresh returned no streams, keeping existing flags",
                account.name,
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
            "[Catchup] account '%s': refreshed archive flags, %d stream(s) updated, "
            "%d catchup-capable of %d total",
            account.name, updated, catchup_capable, len(archive_by_stream_id),
        )
