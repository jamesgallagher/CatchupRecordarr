"""Section 9 - segment planning.

Splits a job's full window (the taken-over Recording's own start_time to
end_time - already padding-adjusted by core's own serializer at creation
time, nothing further to add here) into fixed-size chunks, fetched
sequentially (Section 9 - never concurrently, see design.md's turbo-mode
discussion). Planning only for this step - no fetching yet (step 11).
"""

from datetime import timedelta

# Section 9: "15 or 30 min". 15 chosen as the default - matches the
# reasoning behind the per-segment retry cap (Section 9/12 #5): smaller
# segments mean a bad chunk costs less to retry.
SEGMENT_MINUTES = 15


def plan_segments(window_start, window_end, segment_minutes=SEGMENT_MINUTES):
    """Return [(index, start, duration_minutes), ...] covering
    [window_start, window_end) in fixed segment_minutes chunks. The
    final segment is shortened to fit exactly - it never requests past
    window_end.
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
        duration_minutes = max(1, round(this_delta.total_seconds() / 60))
        segments.append((idx, cursor, duration_minutes))
        cursor = cursor + delta
        idx += 1
    return segments
