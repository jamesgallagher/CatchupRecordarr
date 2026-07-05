"""Section 8 - per-account timeshift dialect memory + fallback logic.

Xtream Codes panels serve timeshift/catchup URLs in one of two dialects
(timeshift.py) and don't advertise which one anywhere - the plugin has
to try one, and if it fails, try the other, remembering which one
worked so future requests go straight to it.

This module owns the decision + the plugin's persisted memory of it.
It knows nothing about HTTP - the actual fetch (step 11) supplies the
fetch_fn callback below, so this module's branching logic is fully
testable without a real network call.
"""

import logging

from datetime import datetime, timezone

from . import state
from ._version import LOG_TAG

logger = logging.getLogger(__name__)

DIALECTS = ("path", "php")

# Sportarr's own default when nothing has been detected yet - "most
# panels use it." Soft default, self-corrects on first success.
COLD_START_DEFAULT = "path"


def get_preferred_dialect(m3u_account_id):
    row = state.get_account_dialect(m3u_account_id)
    dialect = row["dialect"] if row else None
    return dialect if dialect in DIALECTS else COLD_START_DEFAULT


def _other_dialect(dialect):
    return "php" if dialect == "path" else "path"


def record_success(m3u_account_id, dialect):
    state.set_account_dialect(
        m3u_account_id, dialect, datetime.now(timezone.utc).isoformat()
    )


def record_failure(m3u_account_id):
    state.increment_account_dialect_failures(m3u_account_id)


def fetch_with_fallback(m3u_account_id, account_name, url_for_dialect, fetch_fn):
    """Try the account's preferred dialect; on failure, try the other
    once. Returns (success, dialect_used_or_None, result).

    url_for_dialect(dialect: str) -> url: str
    fetch_fn(url: str) -> (success: bool, result)

    Flips the preferred dialect and resets consecutive_failures on a
    successful fallback (self-healing from a stale/wrong detection);
    leaves the preference untouched if both fail (Section 8 - don't
    thrash the setting on what might just be a transient outage).
    """
    preferred = get_preferred_dialect(m3u_account_id)
    success, result = fetch_fn(url_for_dialect(preferred))
    if success:
        record_success(m3u_account_id, preferred)
        return True, preferred, result

    fallback = _other_dialect(preferred)
    logger.info(
        "%s account '%s': '%s' timeshift dialect failed, trying '%s'",
        LOG_TAG, account_name, preferred, fallback,
    )
    success, result = fetch_fn(url_for_dialect(fallback))
    if success:
        logger.info(
            "%s account '%s': '%s' dialect worked - switching preference "
            "from '%s'",
            LOG_TAG, account_name, fallback, preferred,
        )
        record_success(m3u_account_id, fallback)
        return True, fallback, result

    record_failure(m3u_account_id)
    return False, None, result
