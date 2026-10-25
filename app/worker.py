import asyncio
import json
import logging
from datetime import datetime, timezone

import aiohttp
from aiokafka import AIOKafkaConsumer, TopicPartition

from app.image_scraper import NaverImageScrapper
from app.models import mongodb
from app.models.image import ImageModel
from app.models.job import JobModel

from app.config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)


def utc_now(): # 현재 시간을 utc기준으로 가져옴
    return datetime.now(timezone.utc)


async def process_job(job_id: str):
    job = await mongodb.engine.find_one( # DB에 job_id가 존재하지 않은 None반환
        JobModel,
        JobModel.job_id == job_id,
    )

    if job is None:
        # DB에 해당 작업이 없으면 오류를 내며 worker 멈춤
        raise RuntimeError(f"작업을 찾을 수 없습니다: {job_id}")

    if job.status in {"completed", "failed"}:
        logger.info("이미 처리된 작업: %s (%s)", job_id, job.status)
        return
    # 이미 성공했거나 실패한 작업이면 로그를 남김. 중복 실행을 막기 위해 종료(return)

    job.status = "running"
    job.error = None
    job.updated_at = utc_now()
    await mongodb.engine.save(job)

    logger.info("수집 시작: %s / %s", job_id, job.keyword)

    scraper = NaverImageScrapper()

    try:
        images = await asyncio.wait_for(
            scraper.search(job.keyword, job.total_pages),
            timeout=120,
        )
    except (aiohttp.ClientError, TimeoutError, ValueError, KeyError) as exc:
        job.status = "failed"
        job.error = f"{type(exc).__name__}: {exc}"
        job.updated_at = utc_now()
        await mongodb.engine.save(job)

        logger.exception("수집 실패: %s", job_id)
        return

    images_collection = mongodb.engine.get_collection(ImageModel)

    for image in images:
        await images_collection.update_one(
            {"job_id": job_id, "image": image["link"]},
            {"$set": {"keyword": job.keyword, "title": image["title"]}},
            upsert=True,
        )

        logger.info("이미지 저장: %s", image["link"])
        # await asyncio.sleep(2) # 중간 종료 테스트

    job.saved_images = await images_collection.count_documents(
        {"job_id": job.job_id}
    )
    job.status = "completed"
    job.updated_at = utc_now()
    await mongodb.engine.save(job)

    logger.info(
        "수집 완료: %s / %s건 저장",
        job_id,
        job.saved_images,
    )


async def main():
    mongodb.connect() # Mongodb연결하고 

    consumer = AIOKafkaConsumer(
        "crawl-jobs",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="image-workers",
        enable_auto_commit=False, # 메시지 처리하기 전에 자동으로 "읽음 완료" 처리 방지
        auto_offset_reset="earliest", # 처음 연결 또는 기록없으면, 맨 처음 메시지부터 읽기
    )

    try:
        await mongodb.client.admin.command("ping") # 정상 작동 테스트(ping) 수행

        images_collection = mongodb.engine.get_collection(ImageModel)

        await images_collection.create_index(
            [("job_id", 1), ("image", 1)], # job_id 와 image로 인덱스 만들고, db에 저장요청 오면 중복여부 확인
            unique=True,
            partialFilterExpression={ # job_id 값이 문자열인 문서만 이 인덱스에 포함시키겠다
                "job_id": {"$type": "string"},
            },
            name="unique_job_image",
        )

        await consumer.start() # 카프카 consumer 네트워크 세션 시작(메시지 수신 대기)

        logger.info("worker 시작: Kafka 메시지 대기 중")

        async for message in consumer: # 메시지 하나씩 꺼내기
            payload = json.loads(message.value.decode("utf-8")) # 바이트 형태 메시지를 파이썬 딕셔너리로 변환

            await process_job(payload["job_id"])

            # 메시지가 들어온 kafka의 토픽과 파티션 위치 정보를 객체로 생성
            partition = TopicPartition(
                message.topic,
                message.partition,
            )

            # 크롤링 및 DB 저장이 완료되었으므로 이 메시지 "offset + 1" 부터 시작
            await consumer.commit({
                partition: message.offset + 1,
            })
    finally:
        try:
            await consumer.stop() 
            # 프로그램 종료 시, 개발자가 ctrl+c 강제 종료 / error 발생 / 더 이상 메시지 안받을때
            # kafka 연결 종료
        finally:
            mongodb.close()
            # Mongodb 연결 종료


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s PID=%(process)d %(levelname)s %(message)s",
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt: # ctrl c 빨간색 에러 Traceback 숨기기
        pass