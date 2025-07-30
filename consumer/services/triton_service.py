import asyncio
from typing import Tuple
import numpy as np
from tritonclient.http import InferenceServerClient, InferInput, InferRequestedOutput
from tritonclient.utils import InferenceServerException
from consumer.conf import logger, settings

class TritonService:
    def __init__(self):
        self.tritonClient = None
        self.connected = False
        self._lock = asyncio.Lock()

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
            model_name: str,
            model_version: str,
            request_id: str
    ) -> Tuple[bool, np.ndarray]:
        if not self.connected or self.tritonClient is None:
            logger.error("[Triton] Client is not connected")
            return False, None
        try:
            infer_input = InferInput(
                name="x",
                shape=batch.shape,
                datatype="FP32"
            )
            infer_input.set_data_from_numpy(batch)
            infer_output = InferRequestedOutput(name="OUTPUT__0")
            response = self.tritonClient.infer(
                model_name=model_name,
                inputs=[infer_input],
                outputs=[infer_output],
                model_version=model_version,
                request_id=request_id
            )
            return True, response.as_numpy("OUTPUT__0")
        except InferenceServerException as e:
            logger.error(f"[Triton] InferenceServerException: {e}")
            return False, None
        except Exception as e:
            logger.exception(f"[Triton] Unexpected exception in image_infer: {e}")
            return False, None
