import json
from typing import Any, Dict
from app.model import request
from app.service import test
from fastapi import APIRouter, Request

router = APIRouter()

@router.post(path='/predict/test1', response_model=Dict[str, Any])
async def get_predict_sales(request: request.SampleRequest):    
    data = test.get_test(request = request)
    #response = execute_predict(request_json=json.dumps(request_json))
    return {'data': "test"}