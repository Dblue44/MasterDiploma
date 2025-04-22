from .state import kafka_producer, redis_client
from .helper import get_filename_by_task_id
from .image_worker import PhotoTask, consume_photos
