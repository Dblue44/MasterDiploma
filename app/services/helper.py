import os

from app.core import settings


def get_filename_by_task_id(task_id: str) -> str | None:
    """
    Get filename by taskId

    :param task_id:
    :return:
    """
    filename = next((f for f in os.listdir(settings.RESULT_DIR) if f.startswith(task_id)), None)
    return filename
