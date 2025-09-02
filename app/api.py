import json
from typing import Any, Dict

from fastapi import APIRouter, Request

# from models.impact.predict import PredictImpactsRequest
# from services.impact.predict import execute_predict_impact
# from services.sales.predict import execute_predict

router = APIRouter()

@router.post(path='/predict/1', response_model=Dict[str, Any])
async def get_predict_sales(request: Request):
    request_json = await request.json()
    #response = execute_predict(request_json=json.dumps(request_json))
    return {'data': "test"}

# @router.post(path='/predict/2', response_model=Dict[str, Any])
# def get_predict_impacts(request: PredictImpactsRequest):
#     response = execute_predict_impact(request)

#     return {'data': response}
