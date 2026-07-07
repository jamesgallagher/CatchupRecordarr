"""Self-test actions (Section 16 R9 - previously ~90 lines inlined in
plugin.py's action if-chain). Each returns the message string for the
action's UI response; plugin.py's handlers are one-liners over these.
"""

import logging
from datetime import datetime

from ._version import LOG_TAG

logger = logging.getLogger(__name__)


def timeshift_url_selftest():
    """Build both dialects with placeholder values (no real provider,
    never real credentials) so the format can be visually confirmed.
    Returned, never logged - consistent with Section 14's "never log a
    constructed timeshift URL" rule even though these are placeholders.
    """
    from .timeshift import build_timeshift_url

    example_start = datetime(2026, 6, 11, 19, 55, 0)
    path_url = build_timeshift_url(
        "http://provider.example:8080", "user", "pass", 12345, example_start, 215, dialect="path"
    )
    php_url = build_timeshift_url(
        "http://provider.example:8080", "user", "pass", 12345, example_start, 215, dialect="php"
    )
    logger.info("%s timeshift URL builder tested with placeholder values (no real provider)", LOG_TAG)
    return f"path: {path_url}\nphp: {php_url}"


def provider_timezone_selftest():
    """Authenticate to each active XC account for real and report the
    resolved timezone (the clock-skew check runs inside resolution,
    Section 10 - a warning lands in the logs if it trips).
    """
    from apps.m3u.models import M3UAccount

    from .provider import resolve_provider_timezone

    accounts = list(
        M3UAccount.objects.filter(account_type=M3UAccount.Types.XC, is_active=True)
    )
    if not accounts:
        return "No active Xtream Codes accounts found."
    lines = []
    for account in accounts:
        tz = resolve_provider_timezone(account)
        lines.append(f"{account.name}: {tz}")
        logger.info("%s account '%s': resolved timezone %s", LOG_TAG, account.name, tz)
    return (
        "\n".join(lines)
        + "\n\nCheck logs for a clock-skew warning if the provider's "
        "reported clock is unexpectedly far from ours."
    )


def dialect_fallback_selftest():
    """Scripted mock scenarios against a synthetic account id (-1, never
    a real M3UAccount.id - those are always positive) with real
    pass/fail assertions: cold-start default, self-healing flip on a
    successful fallback, preference untouched on a double failure.
    """
    from . import state
    from .dialect import fetch_with_fallback, get_preferred_dialect

    test_id = -1
    state.set_account_dialect(test_id, "unknown", None)  # clean slate

    results = []

    cold = get_preferred_dialect(test_id)
    results.append(
        f"cold-start default: '{cold}' "
        f"[{'PASS' if cold == 'path' else 'FAIL, expected path'}]"
    )

    def fail_all(url):
        return (False, "simulated failure")

    def php_only(url):
        return ("php" in url, "simulated success")

    ok, used, _ = fetch_with_fallback(
        test_id, "test-account", lambda d: f"http://x/{d}", php_only
    )
    after_flip = get_preferred_dialect(test_id)
    results.append(
        f"path fails, php succeeds: success={ok}, used='{used}', "
        f"new preference='{after_flip}' "
        f"[{'PASS' if ok and used == 'php' and after_flip == 'php' else 'FAIL'}]"
    )

    ok2, used2, _ = fetch_with_fallback(
        test_id, "test-account", lambda d: f"http://x/{d}", fail_all
    )
    still_pref = get_preferred_dialect(test_id)
    row = state.get_account_dialect(test_id)
    results.append(
        f"both dialects fail: success={ok2}, preference unchanged="
        f"{still_pref == 'php'}, consecutive_failures={row['consecutive_failures']} "
        f"[{'PASS' if not ok2 and used2 is None and still_pref == 'php' and row['consecutive_failures'] >= 1 else 'FAIL'}]"
    )

    logger.info("%s dialect fallback self-test: %s", LOG_TAG, " | ".join(results))
    return "\n".join(results)
