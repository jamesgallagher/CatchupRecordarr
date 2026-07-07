"""Section 9/11 - stitch completed segments into the final output MKV.

Concatenates a job's segment .ts files (in index order) via ffmpeg's
concat demuxer, applying the same -fflags +genpts -avoid_negative_ts
make_zero treatment Sportarr applies at remux time (Section 9) - fixes
monotonic PTS/timestamp discontinuity at segment boundaries, though (per
Section 9/13's parked stitch-boundary risk) this does not by itself
guarantee no dropped frames at a cut that doesn't land on a provider
keyframe boundary. Each catchup segment is its own independently-
requested archive window, not a provider-delivered keyframe-aligned
chunk - that risk can only be resolved by testing concatenation against
a real provider, not more design work, so this module implements the
accepted mitigation, not a full fix.

Output is always MKV (Section 11), matching what native Dispatcharr
recordings already produce (a direct precedent for stitching, not just a
compatibility guess).

A plain remux (-c copy), never a re-encode - matches native Dispatcharr's
own HLS-concat approach for live recordings (_dvr_build_hls_concat_cmd,
Section 2) and keeps this fast regardless of recording length.
"""

import logging
import os
import subprocess

from ._version import LOG_TAG
from .fsutil import try_delete

logger = logging.getLogger(__name__)

# Generous - concat is a stream copy (I/O-bound remux), not a re-encode,
# so even a multi-hour recording should finish well under this, but a
# slow/network-mounted data volume could still take a while.
FFMPEG_TIMEOUT_SECONDS = 30 * 60


def stitch_segments(segment_paths, output_path):
    """Concatenate segment_paths (already in index order) into
    output_path via ffmpeg's concat demuxer. Returns (success: bool,
    error: str | None). Atomic: a .part.mkv is renamed into place only
    on success, so nothing ever sees a partial stitch as complete (same
    pattern as download.py's segment fetch).
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    list_path = output_path + ".concat_list.txt"
    part_output = output_path + ".part.mkv"

    try:
        with open(list_path, "w", encoding="utf-8") as f:
            for path in segment_paths:
                # ffmpeg's concat demuxer file format: single-quoted,
                # with embedded single quotes escaped. Segment paths are
                # ours (predictable, plugin-controlled), but escape
                # anyway rather than assume.
                escaped = path.replace("'", "'\\''")
                f.write(f"file '{escaped}'\n")

        cmd = [
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0", "-i", list_path,
            "-c", "copy",
            "-fflags", "+genpts",
            "-avoid_negative_ts", "make_zero",
            part_output,
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, timeout=FFMPEG_TIMEOUT_SECONDS,
            )
        except FileNotFoundError:
            return False, "ffmpeg binary not found on PATH"
        except subprocess.TimeoutExpired:
            return False, f"ffmpeg concat timed out after {FFMPEG_TIMEOUT_SECONDS}s"

        if result.returncode != 0:
            # ffmpeg's stderr here only ever references local segment file
            # paths (this is a local subprocess over already-downloaded
            # files, no network call) - safe to include directly, unlike
            # the provider-facing exceptions Section 14 is about.
            stderr_tail = result.stderr.decode("utf-8", errors="replace")[-2000:]
            try_delete(part_output)
            return False, f"ffmpeg concat failed (exit {result.returncode}): {stderr_tail}"

        if not os.path.exists(part_output) or os.path.getsize(part_output) == 0:
            try_delete(part_output)
            return False, "ffmpeg concat produced no output"

        os.replace(part_output, output_path)
        return True, None
    except OSError as exc:
        try_delete(part_output)
        return False, f"local I/O error during stitching: {exc}"
    finally:
        try_delete(list_path)
