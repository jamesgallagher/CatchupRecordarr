from dispatcharr.celery import app as celery_app

from .archive import refresh_archive_flags as _refresh_archive_flags

ARCHIVE_REFRESH_TASK_PATH = "catchup_recordarr.tasks.refresh_archive_flags"


# Bind directly to Dispatcharr's own concrete Celery app rather than using
# @shared_task's deferred multi-app mechanism. shared_task lazily attaches
# to whichever app is "current" at decoration time - designed for reusable
# library code that might run under several different Celery apps. There's
# only ever one app in this system, and this plugin is imported through the
# dynamic plugin-loading mechanism (importlib.util.spec_from_file_location +
# manual sys.modules insertion), not a normal package import via
# INSTALLED_APPS/autodiscover_tasks() - binding directly removes an
# indirection layer that behaved differently for a dynamically-loaded module.
@celery_app.task(name=ARCHIVE_REFRESH_TASK_PATH)
def refresh_archive_flags():
    return _refresh_archive_flags()
