import asyncio
from typing import Tuple, Any

import grpc
import numpy as np
from tritonclient.grpc.aio import InferenceServerClient, InferInput, InferRequestedOutput
from tritonclient.utils import InferenceServerException
from consumer.conf import logger, settings

class TritonService:
    def __init__(self):
        self.tritonClient = None
        self.connected = False
        self._lock = asyncio.Lock()
        self._infer_semaphore = asyncio.Semaphore(1)

    async def start(self):
        async with self._lock:
            if self.connected:
                return
            try:
                self.tritonClient = InferenceServerClient(url=settings.TRITON_SERVER_URL, verbose=False)
                self.connected = True
            except InferenceServerException as e:
                await self.stop()
                logger.warning(f"[Triton] Triton not connected: {e}")
                self.tritonClient = None

    async def stop(self):
        if self.tritonClient:
            await self.tritonClient.close()
            self.connected = False

    async def image_infer(
            self,
            batch: np.ndarray,
            *,
            model_name: str,
            model_version: str,
            request_id: str,
            cancel_event: asyncio.Event | None = None
    ) -> Tuple[bool, None] | Tuple[bool, Any]:
        if not self.connected or self.tritonClient is None:
            logger.error("[Triton] Client is not connected")
            return False, None
        async with self._infer_semaphore:
            try:
                infer_input = InferInput(
                    name="x",
                    shape=batch.shape,
                    datatype="FP32"
                )
                infer_input.set_data_from_numpy(batch)
                infer_output = InferRequestedOutput(name="OUTPUT__0")

                ctx = await self.tritonClient.async_infer(
                    model_name=model_name,
                    inputs=[infer_input],
                    outputs=[infer_output],
                    model_version=model_version,
                    request_id=request_id,
                )

                if cancel_event is None:
                    resp = await ctx.get_result()
                    return True, resp.as_numpy("OUTPUT__0")

                get_result_task = asyncio.create_task(ctx.get_result())
                cancel_task = asyncio.create_task(cancel_event.wait())

                done, pending = await asyncio.wait(
                    {get_result_task, cancel_task},
                    return_when=asyncio.FIRST_COMPLETED
                )

                if cancel_task in done:
                    try:
                        ctx.cancel()
                    except Exception as e:
                        logger.warning(f"[Triton] ctx.cancel() raised: {e}")

                    try:
                        await get_result_task
                    except (grpc.aio.AioRpcError, asyncio.CancelledError, InferenceServerException) as e:
                        logger.warning(f"[Triton] get_result_task raised: {e}")
                    except Exception as e:
                        logger.warning(f"[Triton] get_result_task raised: {e}")

                if get_result_task in done:
                    try:
                        resp = get_result_task.result()
                        return True, resp.as_numpy("OUTPUT__0")
                    finally:
                        if not cancel_task.done():
                            cancel_task.cancel()
                        for t in pending:
                            t.cancel()

                for t in pending:
                    t.cancel()

                return False, None
                # response = self.tritonClient.infer(
                #     model_name=model_name,
                #     inputs=[infer_input],
                #     outputs=[infer_output],
                #     model_version=model_version,
                #     request_id=request_id
                # )
                # return True, response.as_numpy("OUTPUT__0")
            except InferenceServerException as e:
                logger.error(f"[Triton] InferenceServerException: {e}")
                return False, None
            except Exception as e:
                logger.exception(f"[Triton] Unexpected exception in image_infer: {e}")
                return False, None
