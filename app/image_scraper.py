import asyncio
import aiohttp
from app.config import get_secret


class NaverImageScrapper:
    NAVER_API_IMAGE = "https://naverapihub.apigw.ntruss.com/search/v1/image"
    NAVER_API_ID = get_secret("NAVER_API_ID")
    NAVER_API_SECRET = get_secret("NAVER_API_SECRET")

    @staticmethod
    async def fetch(session, url, headers):
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
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
        async with aiohttp.ClientSession() as session:
            all_data = await asyncio.gather(
                *[NaverImageScrapper.fetch(session, api["url"], api["headers"]) for api in apis]
            )
        print(all_data)

        result = []
        for data in all_data:
            if data is not None:
                for image in data:
                    result.append(image)

        # print(result)
        return result

    
naverImageScrapper = NaverImageScrapper()

if __name__ == "__main__":
    asyncio.run(naverImageScrapper.search("cat", 10))

    