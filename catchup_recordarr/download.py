"""Section 8/9 - single segment fetch.

Downloads one timeshift segment using dialect fallback (dialect.py) and
the account's resolved User-Agent (provider.py). This is the step that
finally puts fetch_with_fallback()'s injected callback to real use - no
mock fetch functions past this point.

Orchestration (claiming the next pending segment for a job, updating
segment status, retry caps, orphan recovery) is step 12 - this module
only knows how to fetch ONE already-chosen segment.
"""

import logging
import os

import requests

from ._version import LOG_TAG
from .dialect import fetch_with_fallback
from .errors import describe_redirect_chain, safe_error_string
from .provider import resolve_user_agent
from .timeshift import build_timeshift_url

logger = logging.getLogger(__name__)

# Sportarr's own hardcoded constant (Section 8): "A 2xx response with a
# tiny body is how panels signal 'window not in archive' without an HTTP
# error." Not exposed as a setting for v1 - same reasoning as elsewhere
# in this project (Section 8's own "not ready" threshold, Section 9's
# per-segment retry cap): hardcode a value with real precedent, add a
# setting only if real-world testing shows it needs tuning.
NOT_READY_THRESHOLD_BYTES = 1024 * 1024

# Per-segment timeout, not the whole job's - archive pulls are usually
# faster than realtime, but a throttled provider can drip-feed.
REQUEST_TIMEOUT_SECONDS = 60


def fetch_segment(m3u_account, stream_id, start_local, duration_minutes, dest_path):
    """Download one timeshift segment to dest_path, trying the account's
    preferred dialect first and falling back to the other on failure
    (dialect.py owns that decision entirely).

    Returns (success: bool, error: str | None). On success, dest_path
    holds the downloaded data. On failure, nothing is left at dest_path.
    """
    user_agent = resolve_user_agent(m3u_account)

    def url_for_dialect(dialect):
        return build_timeshift_url(
            m3u_account.server_url, m3u_account.username, m3u_account.password,
            stream_id, start_local, duration_minutes, dialect=dialect,
        )

    def do_fetch(url):
        return _download(url, user_agent, dest_path)

    success, _dialect_used, error = fetch_with_fallback(
        m3u_account.id, m3u_account.name, url_for_dialect, do_fetch,
    )
    return success, error


def _download(url, user_agent, dest_path):
    """Stream url to dest_path via a .part file, atomic rename on
    success so nothing ever sees a partial segment as complete. Never
    logs the url itself, and never returns str(exc) for a request
    failure (Section 14) - both the url and a requests exception's own
    string form can embed real provider credentials. A real leak of
    exactly this kind happened during development (Session 35): the
    exception's default __str__ included the full credentialed URL, and
    that string was returned all the way to the action's UI response and
    Dispatcharr's own logs before this fix.
    """
    part_path = dest_path + ".part"
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    except OSError as exc:
        return False, f"could not create destination directory: {exc}"

    try:
        with requests.get(
            url,
            headers={"User-Agent": user_agent},
            stream=True,
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as resp:
            resp.raise_for_status()
            with open(part_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        f.write(chunk)
    except requests.exceptions.RequestException as exc:
        # Never str(exc) here - requests exceptions commonly embed the
        # full request URL, including credentials.
        _try_delete(part_path)
        message = safe_error_string(exc)
        chain = describe_redirect_chain(getattr(exc, "response", None))
        if chain:
            # host+path only (query stripped, Section 14) - diagnostic
            # aid for telling "we built the wrong URL" apart from "the
            # provider redirected us somewhere that then failed".
            message = f"{message}: {chain}"
        return False, message
    except OSError as exc:
        # Local I/O error (disk full, permission denied) - safe to show
        # directly, it never carries the provider URL.
        _try_delete(part_path)
        return False, f"local I/O error writing segment: {exc}"

    size = os.path.getsize(part_path) if os.path.exists(part_path) else 0
    if size < NOT_READY_THRESHOLD_BYTES:
        _try_delete(part_path)
        return False, f"archive returned only {size} bytes - window likely not available yet"

    os.replace(part_path, dest_path)
    return True, None


def _try_delete(path):
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass
