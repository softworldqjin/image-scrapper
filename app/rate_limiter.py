import asyncio

from redis.asyncio import Redis
from app.config import REDIS_URL

class RedisRateLimiter:
    def __init__(self):
        self.client = Redis.from_url(
            REDIS_URL,
            socket_connect_timeout=3,
            socket_timeout=3,
        )

        # 모든 작업과 worker가 같은 키를 사용합니다.
        self.key = "rate_limit:naver:image"

        # 호출 허용 간격: 500밀리초 = 0.5초
        self.interval_ms = 500

    async def acquire(self):
        while True:
            allowed = await self.client.set(
                self.key,
                "busy",
                nx=True,
                px=self.interval_ms,
            )

            if allowed: # 함수 종료
                return

            remaining_ms = await self.client.pttl(self.key) # 남은 시간 만큼 대기

            await asyncio.sleep(
                max(remaining_ms, 10) / 1000
            )

    async def close(self):
        await self.client.aclose()