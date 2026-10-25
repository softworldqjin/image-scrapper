from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from app.models import mongodb
from app.models.image import ImageModel
from app.models.job import JobModel
from app.image_scraper import NaverImageScrapper
from app.schemas.job import JobCreate

from fastapi import HTTPException

from app.kafka import kafka_producer

from datetime import datetime, timezone
from aiokafka.errors import KafkaError

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent
## Path(__file__).resolve()는 현재 파일, .parent 써주면 이것의 부모폴더를 가리킴

# app.mount("/static", StaticFiles(directory="static"), name="static") ##정적파일


templates = Jinja2Templates(directory=BASE_DIR / "templates")


@app.get("/", response_class=HTMLResponse)
async def read_item(request: Request): #request는 요청하는 주체에 대한 정보를 담고있는 객체
    # print(request["headers"])
    # book = BookModel(keyword="파이썬", price=1000, image="aaa.jpg")
    # print(book)
    # await mongodb.engine.save(book)
    
    return templates.TemplateResponse(request=request, name="./index.html", context={"title": "이미지 콜렉터"})


@app.get("/search", response_class=HTMLResponse)
async def search(request: Request, q: str): #request는 요청하는 주체에 대한 정보를 담고있는 객체
    # print(request["headers"])
    
    keyword = q

    if not keyword:
        return templates.TemplateResponse(
            request=request, name="./index.html", context={"title": "이미지 콜렉터"}
        )

    if await mongodb.engine.find_one(ImageModel, ImageModel.keyword == keyword):
        images = await mongodb.engine.find(ImageModel, ImageModel.keyword == keyword)
        return templates.TemplateResponse(
            request=request, name="./index.html", context={"title": "이미지 콜렉터", "images": images}
        )

    naverImageScrapper = NaverImageScrapper()
    images = await naverImageScrapper.search(keyword, 10)

    image_models = []
    for image in images:
        image_model = ImageModel(
            keyword = keyword,
            title = image["title"],
            image = image["link"]
        )
        image_models.append(image_model)

    # print(image_models)
    await mongodb.engine.save_all(image_models)
    # gather(*[]) 로 되어있음. 비동기처리
    # await mongodb.engine.save(image_model1)
    # await mongodb.engine.save(image_model2)
    # await mongodb.engine.save(image_model3)

    return templates.TemplateResponse(
        request=request, name="./index.html", context={"title": "이미지 콜렉터", "images": image_models})



@app.post("/jobs", status_code=202)
async def create_job(body: JobCreate):
    job = JobModel(
        keyword=body.keyword,
        total_pages=body.total_pages
    )

    await mongodb.engine.save(job)

    try:
        await kafka_producer.publish_job(job)
    except KafkaError as exc:
        job.status = "enqueue_failed"
        job.error = f"Kafka 발행 확인 실패: {type(exc).__name__}"
        job.updated_at = datetime.now(timezone.utc)
        await mongodb.engine.save(job)

        raise HTTPException(
            status_code=503,
            detail={
                "message": "작업 발행을 확인하지 못했습니다.",
                "job_id": job.job_id
            }
        ) from exc # 서버 터미널 로그에 원인 에러와 최종 에러가 모두 출력되어 원인 파악 가능

    return {
        "job_id": job.job_id,
        "status": job.status,
    }


    return {
        "job_id": job.job_id,
        "status": job.status,
    }


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    job = await mongodb.engine.find_one(
        JobModel, # 클래스 이름을 넘겨주면, 클래스 안에 model_config={"collection":"jobs"} 을 보고 jobs컬렉션에서 찾게함
        JobModel.job_id == job_id,
    )

    if job is None:
        raise HTTPException(
            status_code=404,
            detail="작업을 찾을 수 없습니다.",
        )

    return {
        "job_id": job.job_id,
        "keyword": job.keyword,
        "total_pages": job.total_pages,
        "status": job.status,
        "saved_images": job.saved_images,
        "error": job.error,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }


@app.on_event("startup")
async def on_app_start():
    """before app starts"""
    mongodb.connect() # await는 비동기 함수 호출처럼 기다릴 수 있는 객체를 반환할 때만 붙인다.
    await mongodb.engine.configure_database([ImageModel, JobModel])
    await kafka_producer.start()


@app.on_event("shutdown")
async def on_app_shutdown():
    await kafka_producer.stop()
    mongodb.close()
    """after app shutdown"""
    # await mongodb.close() 