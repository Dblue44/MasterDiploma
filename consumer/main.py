import asyncio

from consumer.conf import logger
from consumer.services import RedisService, TritonService
from consumer.image_worker import consume_photos

async def main():
    redis_service = RedisService()
    triton_service = TritonService()

    await redis_service.start()
    await triton_service.start()
    if not triton_service.connected:
        raise Exception("Triton service is not connected")

    await consume_photos(redis_service, triton_service)

if __name__ == '__main__':
    try:
        logger.info("Start Consumer")
        asyncio.run(main())
    except Exception as e:
        logger.error(f'Consumer end with error: {e}')
