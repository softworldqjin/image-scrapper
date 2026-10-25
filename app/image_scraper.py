import asyncio
import aiohttp
from app.config import get_secret

import logging
from app.rate_limiter import RedisRateLimiter

logger = logging.getLogger(__name__) # 현재 파일 기반 전용 logger 객체 생성


class NaverImageScrapper:
    NAVER_API_IMAGE = "https://naverapihub.apigw.ntruss.com/search/v1/image"
    NAVER_API_ID = get_secret("NAVER_API_ID")
    NAVER_API_SECRET = get_secret("NAVER_API_SECRET")

    @staticmethod
    async def fetch(session, url, headers, limiter):
        await limiter.acquire() # 대기

        logger.info("API 호출 허용: %s", url)

        async with session.get(url, headers=headers) as response: # 허용된 뒤 요청
            # if response.status == 200:
            response.raise_for_status() # HTTP 400 401 403 500 오류 응답에서 예외 발생
            
            result = await response.json(content_type=None)
            return result["items"]

                

    def unit_url(self, keyword, start):
        return {
            "url": f"{self.NAVER_API_IMAGE}?query={keyword}&display=10&start={start}",
            "headers": {
                "X-NCP-APIGW-API-KEY-ID": self.NAVER_API_ID,
                "X-NCP-APIGW-API-KEY": self.NAVER_API_SECRET
            }
        }
    # total_page에 10을,, 0,1,2,3,4,5,6,7,8,9
    async def search(self, keyword, total_page):
        apis = [self.unit_url(keyword, 1 + i*10) for i in range(total_page)]

        limiter = RedisRateLimiter()

        try:
            async with aiohttp.ClientSession() as session:
                tasks = [
                    asyncio.create_task(
                        self.fetch(
                            session,
                            api["url"],
                            api["headers"],
                            limiter,
                        )
                    )
                    for api in apis
                ]

                try:
                    all_data = await asyncio.gather(*tasks)
                finally:
                    # 실패 시간 초과 시 남은 요청 종료
                    for task in tasks:
                        if not task.done():
                            task.cancel() # 이 작업 취소해라

                        await asyncio.gather( # 모든 작업이 취소될 때까지 기다려
                            *tasks,
                            return_exceptions=True # 에러 던지지말고, 에러 객체로 담아둬라
                        )
            result = []

            for page_images in all_data:
                result.extend(page_images) # 요소들을 하나씩 풀어서 리스트에 넣음

            return result

        finally:
            await limiter.close()