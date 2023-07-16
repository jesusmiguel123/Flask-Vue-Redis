import datetime
import random
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        key = self.key_schema.sliding_window_rate_limiter_key(
            name,
            self.window_size_ms,
            self.max_hits
        )
        p = self.redis.pipeline()

        dt = datetime.datetime.now()
        timestamp = (dt.hour * 3600 + dt.minute * 60 + dt.second)*1000 \
                    + dt.microsecond // 1000

        p.zadd(key, {
            f"{timestamp}-{random.random()}": timestamp
        })
        p.zremrangebyscore(key, min='-inf', max=timestamp-self.window_size_ms)
        p.zcard(key)

        _, _, hits = p.execute()

        if hits > self.max_hits:
            raise RateLimitExceededException()