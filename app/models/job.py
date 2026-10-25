from __future__ import annotations
from datetime import datetime, timezone
from uuid import uuid4

from odmantic import Field, Model


def utc_now():
    return datetime.now(timezone.utc)

def generate_uuid():
    return str(uuid4()) #128비트 8,4,4,4,12 무작위 숫자 생성

class JobModel(Model):
    job_id: str = Field(
        default_factory=generate_uuid, # 함수() 실행이 아닌, 함수 이름 자체를 전달
        unique=True,
    )

    keyword: str
    total_pages: int

    status: str = "queued"
    saved_images: int = 0
    error: str | None = None

    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    model_config = {"collection": "jobs"}