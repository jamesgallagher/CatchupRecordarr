"""Tiny shared filesystem helpers (Section 16 R11 - previously
copy-pasted into download.py and stitch.py)."""

import os


def try_delete(path):
    """Best-effort delete - cleanup of .part/scratch files where failure
    to delete must never mask the real error being handled.
    """
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass
