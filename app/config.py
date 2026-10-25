import json
import os
from pathlib import Path
from typing import Optional


BASE_DIR = Path(__file__).resolve().parent.parent


def get_secret(
    key: str,
    default_value: Optional[str] = None,
    json_path: str = str(BASE_DIR / "secrets.json"),
):

    value = os.getenv(key)
    if value is not None:
        return value

    path = Path(json_path)

    if path.exists():
        with path.open(encoding="utf-8") as f:
            secrets = json.load(f)

        if key in secrets:
            return secrets[key]

    if default_value is not None:
        return default_value

    raise EnvironmentError(f"설정이 필요합니다: {key}")


MONGO_DB_NAME = get_secret("MONGO_DB_NAME")
MONGO_URL = get_secret("MONGO_URL")

NAVER_API_ID = get_secret("NAVER_API_ID")
NAVER_API_SECRET = get_secret("NAVER_API_SECRET")

KAFKA_BOOTSTRAP_SERVERS = get_secret("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

REDIS_URL = get_secret("REDIS_URL", "redis://localhost:6379/0")