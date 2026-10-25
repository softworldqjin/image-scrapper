from pydantic import BaseModel, Field, field_validator


class JobCreate(BaseModel):
    keyword: str = Field(min_length=1, max_length=100)
    total_pages: int = Field(default=1, ge=1, le=100)

    @field_validator("keyword") # 데이터가 입력될 때 실행할 커스텀 전처리/검증 함수
    @classmethod
    def validate_keyword(cls, value: str) -> str: # value:str 인자타입, ->str 반환하는 데이터 타입이 str이라 명시
        value = value.strip() # 앞뒤 공백 제거 (예: "  파이썬  " -> "파이썬")
        if not value:
            raise ValueError("검색어를 입력하세요.")
        return value