"""Section 8 - Timeshift URL Construction.

Two Xtream Codes timeshift/catchup URL dialects exist in the wild
(verified byte-for-byte against Sportarr's own shipped unit tests,
itself ported from timeshifter by scottrobertson):

    path: {server}/timeshift/{user}/{pass}/{duration}/{start}/{streamId}.ts
    php:  {server}/streaming/timeshift.php?username=...&password=...&stream=...&start=...&duration=...

`start` is `yyyy-MM-dd:HH-mm`, always in the PROVIDER's local time (never
Dispatcharr's or the user's) - this module does no timezone conversion
itself, that's step 8. Per-account dialect memory and the fallback/retry
logic between the two styles is step 9 - this module only builds a URL
for a dialect the caller already picked.
"""

from urllib.parse import quote


def _format_start(start_local):
    """yyyy-MM-dd:HH-mm, built with zero-padded integers rather than
    strftime. Sportarr's own code comments flag ':' as the culture-
    sensitive time-separator specifier in .NET custom format strings,
    corrupted by a non-invariant host locale - the equivalent risk here
    would be an implicit locale-aware formatter, so this stays a plain
    f-string of integer fields, never locale-sensitive by construction.
    """
    return (
        f"{start_local.year:04d}-{start_local.month:02d}-{start_local.day:02d}:"
        f"{start_local.hour:02d}-{start_local.minute:02d}"
    )


def build_timeshift_url(server_url, username, password, stream_id, start_local, duration_minutes, dialect="path"):
    """Build a timeshift URL for the given dialect ('path' or 'php').

    start_local must already be in the provider's local time.
    Credentials are percent-escaped with quote(safe='') - equivalent to
    .NET's Uri.EscapeDataString, escapes everything outside the
    unreserved character set, matching Sportarr's own escaping tests.
    """
    server_url = server_url.rstrip("/")
    start_str = _format_start(start_local)
    user = quote(str(username), safe="")
    pw = quote(str(password), safe="")

    if dialect == "php":
        start_q = quote(start_str, safe="")
        return (
            f"{server_url}/streaming/timeshift.php"
            f"?username={user}&password={pw}&stream={stream_id}"
            f"&start={start_q}&duration={duration_minutes}"
        )

    return f"{server_url}/timeshift/{user}/{pw}/{duration_minutes}/{start_str}/{stream_id}.ts"


def redact_timeshift_url(url):
    """Never log a constructed timeshift URL as-is (Section 14) - it
    embeds the provider username/password directly in the path or query
    string. Used only for log lines; real requests use the real URL.
    """
    return "<redacted - contains provider credentials>"
