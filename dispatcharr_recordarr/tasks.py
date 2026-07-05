from celery import shared_task

from .archive import refresh_archive_flags as _refresh_archive_flags

ARCHIVE_REFRESH_TASK_PATH = "dispatcharr_recordarr.tasks.refresh_archive_flags"


@shared_task(name=ARCHIVE_REFRESH_TASK_PATH)
def refresh_archive_flags():
    return _refresh_archive_flags()
