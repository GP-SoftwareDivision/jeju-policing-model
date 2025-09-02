from app.model.request import *
from sqlalchemy import text
from app.config.database import engine

import xml.etree.ElementTree as ET
import requests

def get_test(request: SampleRequest):
    query = text("select * from info_rwis where reg_date is not null and latitude = '33.38483693' order by reg_date desc limit 10")

    with engine.connect() as conn:
        result = conn.execute(query)
        users = [dict(row) for row in result.mappings()]  # 결과를 딕셔너리로 변환        
        freezing, controlL, controlS = get_add_data()

    return "test"

def get_add_data():
    # RSS 데이터 가져오기
    response = requests.get("https://www.jjpolice.go.kr/jjpolice/notice/traffic.htm?act=rss")
    response.raise_for_status()  # 요청 에러 체크

    # XML 파싱
    root = ET.fromstring(response.content)

    # 'item' 태그 찾기
    items = root.findall(".//item")

    # 원하는 데이터 찾기
    for item in items:
        title = item.find("title").text if item.find("title") is not None else ""
        
        if "5.16도로(1131)" in title:  # 제목 필터링
            freezing = item.find("freezing").text if item.find("freezing") is not None else "N/A"
            controlL = item.find("controlL").text if item.find("controlL") is not None else "N/A"
            controlS = item.find("controlS").text if item.find("controlS") is not None else "N/A"

            return freezing, controlL, controlS  # 데이터 반환
    
    return "", "", ""  # 해당 title이 없을 경우