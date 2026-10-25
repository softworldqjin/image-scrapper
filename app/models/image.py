from __future__ import annotations #무조건 첫줄

from odmantic import Model

class ImageModel(Model):
    keyword: str
    title: str
    image: str
    job_id: str | None = None

    model_config = {"collection": "images"}
