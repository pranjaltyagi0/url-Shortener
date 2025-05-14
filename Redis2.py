from typing import Optional
from redis.asyncio import Redis
from config import settings
from logs import get_logger

logger = get_logger(__name__)


class RedisClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(RedisClient, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self._client: Optional[Redis] = None

    async def init(self):
        """
        Initialize Redis connection (call this once manually).
        """
        if self._client is None:
            client = Redis(
                host=settings.REDIS_URI,
                port=int(settings.REDIS_PORT),
                db=int(settings.REDIS_DATABASE),
                decode_responses=True
            )
            try:
                await client.ping()
                self._client = client
                logger.info("Connected to Redis.")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                raise

    def get_client(self) -> Redis:
        if not self._client:
            raise RuntimeError("RedisClient is not initialized. Call await init() first.")
        return self._client

    async def close(self):
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("Redis connection closed.")
