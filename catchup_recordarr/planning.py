"""Section 9 - segment planning.

Splits a job's full window (the taken-over Recording's own start_time to
end_time - already padding-adjusted by core's own serializer at creation
time, nothing further to add here) into fixed-size chunks, fetched
sequentially (Section 9 - never concurrently, see design.md's turbo-mode
discussion). Planning only for this step - no fetching yet (step 11).
"""

import math
from datetime import timedelta

# Section 9: "15 or 30 min". 15 chosen as the default - matches the
# reasoning behind the per-segment retry cap (Section 9/12 #5): smaller
# segments mean a bad chunk costs less to retry.
SEGMENT_MINUTES = 15


def plan_segments(window_start, window_end, segment_minutes=SEGMENT_MINUTES):
    """Return [(index, start, duration_minutes), ...] covering
    [window_start, window_end) in fixed segment_minutes chunks. The
    final segment is shortened to fit, rounded UP to whole minutes
    (timeshift duration granularity is minutes) - so a fractional-minute
    window's last segment may request up to 59s past window_end rather
    than dropping up to 59s of content (Section 16 R14: the previous
    round() sat ambiguously between the two and the docstring claimed
    "never past window_end", which minute granularity can't guarantee;
    over-requesting is the right side to land on - the tail overshoot is
    absorbed by validation's own ±2min tolerance and mirrors the design's
    padding philosophy of preferring a little extra over truncation).
    """
    if window_end <= window_start:
        return []

    segments = []
    idx = 0
    cursor = window_start
    delta = timedelta(minutes=segment_minutes)
    while cursor < window_end:
        remaining = window_end - cursor
        this_delta = min(delta, remaining)
        duration_minutes = max(1, math.ceil(this_delta.total_seconds() / 60))
        segments.append((idx, cursor, duration_minutes))
        cursor = cursor + delta
        idx += 1
    return segments
