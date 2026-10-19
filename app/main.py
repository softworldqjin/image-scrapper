from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from app.models import mongodb
from app.models.image import ImageModel
from app.image_scraper import NaverImageScrapper

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

@app.on_event("startup")
async def on_app_start():
    """before app starts"""
    mongodb.connect()

@app.on_event("shutdown")
async def on_app_shutdown():
    mongodb.close()
    """after app shutdown"""
    # await mongodb.close() 