from fastapi import FastAPI
from fastapi.responses import JSONResponse
from generator import generate_final_data
from pydantic import BaseModel
from typing import Optional
from config import API_KEY
app = FastAPI()

class RequestData(BaseModel):
    days: int = 30
    period_analiz: int = 30
    b7: int = 0
    f7: int = 0
    f8: int = 0
    price_min: int = 0
    price_max: int = 5000
    sklad_max: int = 4
    token: Optional[str] = None  # ← Добавили поле token

@app.post("/api/final-data/")
def get_final_data(payload: RequestData):
    
    token = payload.token or API_KEY

    data = generate_final_data(
        days=payload.days,
        period_analiz=payload.period_analiz,
        B7=payload.b7,
        min_price=payload.price_min,
        max_price=payload.price_max,
        sklad_max=payload.sklad_max,
        API_KEY=token,
        F7 = payload.f7,
        F8 = payload.f8
        
    )
    if not data:
        return JSONResponse(status_code=500, content={"error": "Не удалось сгенерировать данные"})
    return data

