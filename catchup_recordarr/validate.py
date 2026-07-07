"""Section 10 - post-stitch validation (step 14), checks 2 and 3.

Two mechanically-detectable checks that catch the realistic failure
symptoms of a segmented catchup download, using ffprobe (already a
required tool in this container - Dispatcharr's own native DVR/comskip
pipeline depends on the same ffmpeg/ffprobe toolchain):

1. Duration check - the stitched file's measured duration should match
   the recording's own expected window (end_time - start_time), within a
   tolerance generous enough to absorb segment-boundary rounding.
   Catches truncation and silent mid-download data loss.
2. Playability check - ffprobe must successfully parse the file and
   report at least one valid video stream. A garbled/corrupt result
   fails this even if size and duration both look fine.

Genuine content validation ("is this really the right broadcast") needs
video/audio understanding and is out of scope (Section 10) - these are
proxies for realistic failure modes, not a content-correctness guarantee.
Check 1 (account-level clock/timezone sanity) already runs in provider.py
at timezone-resolution time, not here - it targets a different failure
window (the request being built wrong) than these two (the download
coming back damaged).
"""

import json
import logging
import subprocess

logger = logging.getLogger(__name__)

FFPROBE_TIMEOUT_SECONDS = 60

# Section 10: "tolerance +-5% or +-2 minutes (whichever is larger, to
# absorb segment-boundary rounding)."
DURATION_TOLERANCE_PERCENT = 0.05
DURATION_TOLERANCE_MIN_SECONDS = 2 * 60


def _ffprobe_json(path):
    """Run ffprobe over a local file and return (parsed_json, None) or
    (None, error). Local subprocess over an already-downloaded file - no
    provider URL/credentials ever appear in its output, unlike the
    provider-facing exceptions Section 14 is about.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-print_format", "json",
        "-show_format", "-show_streams",
        path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=FFPROBE_TIMEOUT_SECONDS)
    except FileNotFoundError:
        return None, "ffprobe binary not found on PATH"
    except subprocess.TimeoutExpired:
        return None, f"ffprobe timed out after {FFPROBE_TIMEOUT_SECONDS}s"

    if result.returncode != 0:
        stderr_tail = result.stderr.decode("utf-8", errors="replace")[-1000:]
        return None, f"ffprobe failed (exit {result.returncode}): {stderr_tail}"

    try:
        return json.loads(result.stdout), None
    except (ValueError, UnicodeDecodeError) as exc:
        return None, f"ffprobe returned unparseable output: {exc}"


def validate_output(path, expected_duration):
    """Returns (success: bool, error: str | None).

    expected_duration is a timedelta - the recording's own
    end_time - start_time (Section 10's literal spec), not the sum of
    planned segment durations, though the two should normally agree
    since planning.py covers exactly [start_time, end_time).
    """
    probe, error = _ffprobe_json(path)
    if probe is None:
        return False, f"playability check failed - {error}"

    streams = probe.get("streams") or []
    if not any(s.get("codec_type") == "video" for s in streams):
        return False, "playability check failed - no valid video stream found"

    fmt = probe.get("format") or {}
    try:
        actual_seconds = float(fmt.get("duration"))
    except (TypeError, ValueError):
        return False, "duration check failed - ffprobe reported no duration"

    expected_seconds = expected_duration.total_seconds()
    tolerance = max(
        expected_seconds * DURATION_TOLERANCE_PERCENT,
        DURATION_TOLERANCE_MIN_SECONDS,
    )
    diff = abs(actual_seconds - expected_seconds)
    if diff > tolerance:
        return False, (
            f"duration check failed - expected ~{expected_seconds:.0f}s, "
            f"got {actual_seconds:.0f}s (diff {diff:.0f}s, tolerance {tolerance:.0f}s)"
        )

    return True, None
