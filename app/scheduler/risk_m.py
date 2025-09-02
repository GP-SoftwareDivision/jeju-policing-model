import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

from sqlalchemy import text
import psycopg2
from app.config.database import engine
from sqlalchemy.exc import SQLAlchemyError

import pickle

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV, KFold
from sklearn.metrics import accuracy_score

#lightgbm
# import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error

from app.scheduler.common import *

import os
"""
0. 필요함수 정의
"""
# 모델 경로 지정
#model_path = '/app/pkl/rf_risk_model.pkl'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # scheduler.py의 디렉터리
MODEL_PATH = os.path.join(BASE_DIR, '..', 'pkl', 'rf_risk_model_4050153100_4050156700_250227.pkl')
# 모델 pkl파일 로드 (2025-02-19 15:00 까지 학습된 모델)
# 모델 각 link_id마다 생성되면 for문 안으로 


def get_traffic_data(conn, link_ids):
    
    query = text(f"""
        SELECT 
            A.link_id
            ,A.prcn_dt as prcn_dt                
            ,date(prcn_dt) as date
            ,extract(month from prcn_dt) as month
            ,extract(dow from prcn_dt) as day
            ,extract(hour from prcn_dt) as hour
            ,extract(min from prcn_dt) as min
            ,cast(tfvl as integer) as tfvl
            ,cast(sped_avg as integer) as sped_avg
        FROM public.info_traffic_realtime_stat A
        JOIN public.tbm_info_link_grouped B ON A.link_id = B.link_id
        WHERE 1=1
        and A.link_id IN ({link_ids})
        and prcn_dt = (select max(prcn_dt) from public.info_traffic_realtime_stat)
        -- and prcn_dt <= '2025-03-11 14:00:00.000'
    """)

    #traffic_df = pd.read_sql(query, conn)
    result = conn.execute(query)
    traffic_df = pd.DataFrame(result.fetchall(), columns=result.keys())        
    return traffic_df

def get_risk_m():
    import warnings
    warnings.filterwarnings('ignore')
    
    # with open(MODEL_PATH, 'rb') as f:
    #     model = pickle.load(f)
    
    # 모델 경로 지정
    model_path = os.path.join(BASE_DIR, '..', 'pkl', '2025-03-11') #'./model/2025-03-11/'    
    model_files = {m.replace('|', '_'): m for m in os.listdir(model_path)}  # 모델 파일을 딕셔너리로 저장

    with engine.begin() as conn:       
          
        # 최종 결과를 저장할 데이터프레임
        result_df = pd.DataFrame()
        link_data = get_link_data(conn)
        for link_cd in tqdm(link_data['link_code'].unique()):
    
            # 모델 로드
            link_cd_t = link_cd.replace('|', '_')
            model_nm = next((m for m in model_files if link_cd_t in m), None)
            
            with open(os.path.join(model_path, model_nm), 'rb') as f:
                model = pickle.load(f)
                        
            # 데이터 로드
            link_id_list = link_data[link_data['link_code']==link_cd].link_id
            link_ids = ",".join(f"'{link_id}'" for link_id in link_id_list)
        
            traffic_df = get_traffic_data(conn, link_ids)
            link_df = link_data[link_data.link_code==link_cd].reset_index(drop=True)
        
            # 데이터 전처리
            processed_data = preprocessing(traffic_df, link_df)
        
            # inference data
            X_test = processed_data.drop(['link_code', 'date', 'min'], axis=1)
        
            # 모델 예측
            pred_df = pd.DataFrame({'pred': model.predict(X_test)})
            
            # 기존 데이터 + 예측 결과 합치기
            merged_result_df = pd.concat([processed_data, pred_df], axis=1)
        
            # 최종 데이터프레임에 누적 저장
            result_df = pd.concat([result_df, merged_result_df])
        
            # 최종 데이터 컬럼명 변경 (한글)
            final_output_df = result_df[['date', 'hour', 'min', 'tfvl', 'sped_avg', 'tpi', 'pred', 'link_code']]\
                .rename(columns={'tpi': 'tp'})\
                .assign(hour = lambda df: df['hour'].astype(int)
                    ,min = lambda df: df['min'].astype(int)
                    ,tp = lambda df: df['tp'].astype(float)
                    ,tfvl = lambda df: df['tfvl'].astype(str)
                    ,cat_tp = lambda df: df['tp'].apply(categorize_value)
                    ,cat_pred = lambda df: df['pred'].apply(categorize_value))
            final_output_df['date'] = traffic_df['prcn_dt'][0]
        insert_risk_data(conn, final_output_df, 'info_risk_pred_m')
        print(final_output_df,)


