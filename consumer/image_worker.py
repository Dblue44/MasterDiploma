import asyncio
import json
from typing import List, Tuple
import torch
import cv2
import numpy as np
import random
from aiokafka.errors import KafkaConnectionError
from pydantic import ValidationError
from aiokafka import AIOKafkaConsumer
from consumer.conf import logger, settings
from consumer.services import RedisService, TritonService
from consumer.utils import TaskStatus, PhotoTask


async def consume_photos(redis_service: RedisService, triton_service: TritonService):
    semaphore = asyncio.Semaphore(settings.MAX_CONSUMER_THREADS)

    while True:
        consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="photo-workers",
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_interval_ms=300000,
            enable_auto_commit=False
        )
        try:
            logger.info("[Consumer] Trying connect to Kafka.")
            await consumer.start()
            logger.info(f"[Consumer] Kafka consumer started for topic: {settings.KAFKA_TOPIC}")

            try:
                async for msg in consumer:
                    asyncio.create_task(handle_message(msg.value, redis_service, triton_service, semaphore))
            except Exception as e:
                logger.exception(f"[Consumer] Unexpected error while consuming messages: {e}")
            # finally:
            #     await consumer.stop()
            #     logger.info("[Consumer] Kafka consumer stopped.")
            #     break
        except KafkaConnectionError as e:
            logger.warning(f"[Consumer] Kafka unavailable: {e}")
            await asyncio.sleep(settings.KAFKA_CONNECTION_DELAY)
        except Exception as e:
            logger.exception(f"[Consumer] Unexpected error starting Kafka consumer: {e}")
            await asyncio.sleep(settings.KAFKA_CONNECTION_DELAY)


async def handle_message(
    value: bytes,
    redis_service: RedisService,
    triton_service: TritonService,
    semaphore: asyncio.Semaphore
) -> None:
    """
    Processes a single Kafka message: parses JSON, creates a Photo Task, and starts the processing process.

    Args:
        value (bytes): Raw bytes of the message from Kafka.
        redis_service (RedisService): A service for updating statuses in Redis.
        triton_service (TritonService): Service for calling Triton Inference Server.
        semaphore (asyncio.Semaphore): Semaphore for awaiting free thread.
    Returns:
        None
    """
    async with semaphore:
        try:
            await asyncio.wait_for(
                _process_message_internal(value, redis_service, triton_service),
                timeout=settings.TASK_TIMEOUT_SECONDS
            )
        except (json.JSONDecodeError, ValidationError) as e:
            logger.exception(f"Failed to parse incoming message: {e}")
        except asyncio.TimeoutError:
            logger.error(f"[Consumer] Task processing timeout after {settings.TASK_TIMEOUT_SECONDS}s")
        except Exception as e:
            logger.exception(f"[ImageWorker] Error: {e}")

async def _process_message_internal(
    value: bytes,
    redis_service: RedisService,
    triton_service: TritonService
) -> None:
    data = json.loads(value.decode("utf-8"))
    task = PhotoTask(**data)
    logger.info(f"Received task: {task.task_id}")
    logger.info(f"Simulating processing for task {task.task_id}, waiting 20 seconds...")

    await asyncio.sleep(20)

    if random.random() < 0.5:
        logger.info(f"Task {task.task_id} simulated completed.")
        await redis_service.set_task_status(task.task_id, {"status": TaskStatus.COMPLETED})
        logger.info(f"Task {task.task_id} completed successfully.")
    else:
        logger.info(f"Task {task.task_id} simulated failure.")
        await redis_service.set_task_status(task.task_id, {"status": TaskStatus.FAILED})
        logger.warning(f"Task {task.task_id} failed successfully.")
    # success, error_message = await _process_task(task, redis_service, triton_service)
    # if success is False:
    #     await redis_service.set_task_status(task.task_id, {
    #         "status": TaskStatus.FAILED,
    #         "error": error_message
    #     })
    #     logger.exception(error_message)

async def _process_task(task: PhotoTask, redis_service: RedisService, triton_service: TritonService) -> (bool, str):
    """
        Reads the original photo, splits it into overlapping tiles,
        sends tile batches to Triton, and collects the output tiles back.
        and that makes it great in a "large-scale" map.

        Args:
            task (PhotoTask): Task data containing task_id, filename, and scale.
            redis_service (RedisService): A service for updating statuses in Redis.
            triton_service (TritonService): Service for calling Triton Inference Server.

        Returns:
            Success parameter ans error message
        """
    input_path = settings.UPLOAD_DIR / task.filename
    output_path = settings.RESULT_DIR / task.filename
    window_size = 128
    logger.info(f"Start processing task: {task.task_id}")
    try:
        await redis_service.set_task_status(task.task_id, {"status": TaskStatus.RUNNING})

        img = cv2.imread(str(input_path), cv2.IMREAD_COLOR).astype(np.float32) / 255.
        if img is None:
            return False, f"[ImageWorker] Cannot read image at {task.task_id}"

        img = torch.from_numpy(np.transpose(img[:, :, [2, 1, 0]], (2, 0, 1))).float()
        img = img.unsqueeze(0)
        _, _, h_old, w_old = img.size()
        h_pad = (h_old // window_size + 1) * window_size - h_old
        w_pad = (w_old // window_size + 1) * window_size - w_old
        img = torch.cat([img, torch.flip(img, [2])], 2)[:, :, :h_old + h_pad, :]
        img = torch.cat([img, torch.flip(img, [3])], 3)[:, :, :, :w_old + w_pad]

        success, scaled = await triton_service.image_infer(
            img.numpy(),
            model_name=task.model_name,
            model_version=task.model_version,
            request_id=task.task_id
        )

        if not success or scaled is None:
            return False, f"[ImageWorker] Triton return None"

        if scaled.ndim != 4 or scaled.shape[0] != 1:
            return False, "[ImageWorker] Unexpected output shape from Triton"

        _, _, h_pad_out, w_pad_out = scaled.shape

        scaled = scaled[..., :h_old * task.scale, :w_old * task.scale]
        scaled = scaled[0]
        scaled = np.transpose(scaled, (1, 2, 0))  # RGB
        scaled = scaled[:, :, [2, 1, 0]]  # BGR
        out_uint8 = np.clip(scaled * 255.0, 0, 255).astype(np.uint8)

        success = cv2.imwrite(str(output_path), out_uint8)

        if not success:
            return False, f"[ImageWorker] Failed to write final image to {output_path}"

        await redis_service.set_task_status(task.task_id, {"status": TaskStatus.COMPLETED})
        logger.info(f"[ImageWorker] Task {task.task_id} completed successfully.")
        return True, None
    except Exception as e:
        await redis_service.set_task_status(task.task_id, {
            "status": TaskStatus.FAILED,
            "error": str(e)
        })
        logger.exception(f"[ImageWorker] Failed to process task {task.task_id}: {e}")


async def process_task_tiles(task: PhotoTask, redis_service: RedisService, triton_service: TritonService) -> (bool, str):
    """
    Reads the original photo, splits it into overlapping tiles,
    sends tile batches to Triton, and collects the output tiles back.
    and that makes it great in a "large-scale" map.

    Args:
        task (PhotoTask): Task data containing task_id, filename, and scale.
        redis_service (RedisService): A service for updating statuses in Redis.
        triton_service (TritonService): Service for calling Triton Inference Server.

    Returns:
        Success parameter ans error message
    """
    input_path = settings.UPLOAD_DIR / task.filename
    output_path = settings.RESULT_DIR / task.filename
    logger.info(f"Start processing task: {task.task_id}")
    try:
        await redis_service.set_task_status(task.task_id, {"status": TaskStatus.RUNNING})

        img_bgr = cv2.imread(str(input_path), cv2.IMREAD_COLOR)

        if img_bgr is None:
            return False, f"[ImageWorker] Cannot read image at {task.task_id}"

        tile = 64
        scale = task.scale
        h_old, w_old = img_bgr.shape[:2]
        img_padded, E, W_mask, h_idx_list, w_idx_list = preprocess_image(img_bgr=img_bgr, tile=tile, scale=scale)

        batch_patches: list[np.ndarray] = []
        batch_positions: list[tuple[int, int]] = []

        for h_idx in h_idx_list:
            for w_idx in w_idx_list:
                tile_bgr = img_padded[h_idx: h_idx + tile, w_idx: w_idx + tile]

                patch_tensor = preprocess_image_tile(tile_bgr=tile_bgr)

                batch_patches.append(patch_tensor)
                batch_positions.append((h_idx, w_idx))

                if len(batch_patches) == settings.TRITON_BATCH_SIZE:
                    processed = await flush_batch(task, triton_service, batch_patches, batch_positions)
                    if not processed:
                        return False, f"[ImageWorker] Triton return empty list"
                    E, W_mask = accumulate(processed=processed, scale=scale, tile=tile, E=E, W_mask=W_mask)
                    batch_patches = []
                    batch_positions = []

        if len(batch_patches) > 0:
            processed = await flush_batch(task, triton_service, batch_patches, batch_positions)
            if not processed:
                return False, f"[ImageWorker] Triton return empty list"
            E, W_mask = accumulate(processed=processed, scale=scale, tile=tile, E=E, W_mask=W_mask)

        output_acc = E / W_mask

        # Trim off the tail "extra" areas to return the size (h_old*scale, w_old*scale)
        output_acc = output_acc[:, : h_old * scale, : w_old * scale]

        # Return to uint8 BGR: CHW→HWC, RGB→BGR, [0..1]→[0..255]
        output_hwc = np.transpose(output_acc[[2, 1, 0], :, :], (1, 2, 0))
        output_uint8 = (np.clip(output_hwc, 0.0, 1.0) * 255.0).round().astype(np.uint8)

        success = cv2.imwrite(str(output_path), output_uint8)

        if not success:
            return False, f"[ImageWorker] Failed to write final image to {output_path}"

        await redis_service.set_task_status(task.task_id, {"status": TaskStatus.COMPLETED})
        logger.info(f"[ImageWorker] Task {task.task_id} completed successfully.")
        return True, None
    except Exception as e:
        await redis_service.set_task_status(task.task_id, {
            "status": TaskStatus.FAILED,
            "error": str(e)
        })
        logger.exception(f"[ImageWorker] Failed to process task {task.task_id}: {e}")


def accumulate(processed: List[Tuple[Tuple[int, int], np.ndarray]],
               scale: int,
               tile: int, E, W_mask):
    """
    Accumulates the output tiles in E and increments the W_mask counter.

    Args:
        :param processed: List of tuples ((h_id, w_idx), scaled_chw).
        :param scale: Degree of magnification
        :param tile: How many pixels should the tiles overlap by
        :param E:
        :param W_mask:
    """
    for (h0, w0), scaled_chw in processed:
        h_out_start = h0 * scale
        h_out_end = h_out_start + tile * scale
        w_out_start = w0 * scale
        w_out_end = w_out_start + tile * scale
        E[:, h_out_start:h_out_end, w_out_start:w_out_end] += scaled_chw
        W_mask[:, h_out_start:h_out_end, w_out_start:w_out_end] += 1.0
    return E, W_mask


async def flush_batch(task: PhotoTask,
                      triton_service: TritonService,
                      batch_patches: List[np.ndarray],
                      batch_positions: List[Tuple[int, int]]):
    """
    Sends accumulated tiles (batch_patches) to Triton as a single batch
    and returns a list of pairs (position, scaled_chw).

    Returns:
        List of maps: ((h_id, w_id), scaled_chw), where scaled_php has size (3, tile*scale, tile*zoom), dtype float32.
    """
    if not batch_patches:
        return []

    batched = np.stack(batch_patches, axis=0)  # shape=(n,3,64,64)
    success, scaled = await triton_service.image_infer(
        batched,
        model_name=task.model_name,
        model_version=task.model_version,
        request_id=task.task_id
    )
    if success is False:
        return []

    results: List[np.ndarray]
    if scaled.ndim == 4:
        results = [scaled[i] for i in range(scaled.shape[0])]
    elif scaled.ndim == 3:
        results = [scaled]
    else:
        raise RuntimeError(f"Unexpected output shape from Triton: {scaled.shape}")

    positions: List[Tuple[int, int]] = batch_positions.copy()
    return list(zip(positions, results))


def preprocess_image_padding(img_bgr: np.ndarray, tile: int, scale: int):
    h_old, w_old = img_bgr.shape[:2]
    overlap = settings.TRITON_OVERLAP_SIZE
    stride = tile - overlap

    h_pad = 0 if (h_old % tile == 0) else ((h_old // tile + 1) * tile - h_old)
    w_pad = 0 if (w_old % tile == 0) else ((w_old // tile + 1) * tile - w_old)

    img_padded = cv2.copyMakeBorder(
        img_bgr,
        top=0, bottom=h_pad,
        left=0, right=w_pad,
        borderType=cv2.BORDER_REFLECT
    )

    h_pad_total = h_old + h_pad
    w_pad_total = w_old + w_pad

    # 4) Подготовка для аккумуляции выходных данных:
    #    - E: сумма выходных тайлов (3, H_pad*scale, W_pad*scale), float32
    #    - W_mask: счётчик попаданий (3, H_pad*scale, W_pad*scale), float32
    H_out = h_pad_total * scale
    W_out = w_pad_total * scale
    E = np.zeros((3, H_out, W_out), dtype=np.float32)
    W_mask = np.zeros((3, H_out, W_out), dtype=np.float32)

    h_idx_list = list(range(0, h_pad_total - tile + 1, stride))
    if h_idx_list[-1] != h_pad_total - tile:
        h_idx_list.append(h_pad_total - tile)
    w_idx_list = list(range(0, w_pad_total - tile + 1, stride))
    if w_idx_list[-1] != w_pad_total - tile:
        w_idx_list.append(w_pad_total - tile)

    return img_padded, E, W_mask, h_idx_list, w_idx_list


def preprocess_image(img_bgr: np.ndarray, tile: int, scale: int):
    h_old, w_old = img_bgr.shape[:2]
    overlap = settings.TRITON_OVERLAP_SIZE
    stride = tile - overlap

    if h_old < tile or w_old < tile:
        h_idx_list = [0]
        w_idx_list = [0]

        E = np.zeros((3, h_old * scale, w_old * scale), dtype=np.float32)
        W_mask = np.zeros((3, h_old * scale, w_old * scale), dtype=np.float32)
        return img_bgr, E, W_mask, h_idx_list, w_idx_list

    h_idx_list = list(range(0, h_old - tile + 1, stride))
    w_idx_list = list(range(0, w_old - tile + 1, stride))

    if h_idx_list[-1] != (h_old - tile):
        h_idx_list.append(h_old - tile)
    if w_idx_list[-1] != (w_old - tile):
        w_idx_list.append(w_old - tile)

    H_out = h_old * scale
    W_out = w_old * scale
    E = np.zeros((3, H_out, W_out), dtype=np.float32)
    W_mask = np.zeros((3, H_out, W_out), dtype=np.float32)

    return img_bgr, E, W_mask, h_idx_list, w_idx_list


def preprocess_image_tile(tile_bgr: np.ndarray) -> np.ndarray:
    # BGR -> RGB and normalization
    tile_rgb = (cv2.cvtColor(tile_bgr, cv2.COLOR_BGR2RGB).astype(np.float32)) / 255.0
    # HWC -> CHW
    tile_chw = np.transpose(tile_rgb, (2, 0, 1))
    return tile_chw
