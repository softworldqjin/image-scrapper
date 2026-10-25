import json

from aiokafka import AIOKafkaProducer

from app.config import KAFKA_BOOTSTRAP_SERVERS

class KafkaProducer:
    def __init__(self):
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps( # 앞으로 데이터가 들어오면 이렇게 변환해라.
                value, # job_id, keyword, total_pages
                ensure_ascii=False, # 16진수 코드로 변환하지않고, 원본 한글 그대로 유지
            ).encode("utf-8"),
            acks="all", # TCP 핸드셰이크(작성한 메시지 바이트를 카프카 브로커로 전송하고, "받았다"는 응답(ACK)을 받을 때까지 대기
        )
        await self.producer.start() # AIOKafkaProducer 클래스의 start()함수 / kafka 연결

    async def stop(self):
        if self.producer is not None:
            await self.producer.stop()

    async def publish_job(self, job):
        return await self.producer.send_and_wait(
            topic="crawl-jobs",
            key=job.job_id.encode("utf-8"),
            value={
                "job_id": job.job_id,
                "keyword": job.keyword,
                "total_pages": job.total_pages,
            },
        )


kafka_producer = KafkaProducer()