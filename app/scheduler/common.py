import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

"""
2. 모델 추론(Inference)을 위한 데이터 전처리
"""

def preprocessing(traffic_df, link_df):
    # object -> datetime
    traffic_df['date'] = pd.to_datetime(traffic_df['date'])
    """
    결측치 채우는 로직 필요?
    """
    # 교통량/평균속도 + MAX_SPD_MN, MAX_SPD_MX
    traffic_spd_df = pd.merge(traffic_df, link_df, how='left', on='link_id')
    # TPI 변수 추가
    traffic_spd_df['tpi'] = traffic_spd_df[['max_spd', 'sped_avg']].apply(lambda df : tpi(df['max_spd'] , df['sped_avg']), axis = 1)
    
    # 출퇴근 시간 가중치 컬럼 추가
    traffic_spd_df['rush_hour'] = traffic_spd_df['hour'].isin([16,17,18]).astype('int')
    
    # 돌발상황 컬럼 추가
    traffic_spd_df['unexp_event'] = 0

    # 불필요 column 제거
    traffic_spd_df.drop(columns=['link_id', 'month','prcn_dt'], inplace=True)
    
    # 구간 groupby (mean)
    input_df = traffic_spd_df.groupby(['link_code','date','day','hour','min']).agg('mean').reset_index()
    
    return input_df

# @title Function : tpi(Vf, Vi) #tpi지수 변환 TRAFFIC PERFORMANCE INDEX
def tpi(MAX_SPD, Vi):
  Vf = MAX_SPD * 1.25
  if Vi == 0:
    return 0
  else:
    if Vf > Vi : #자유속도(Vf)가 측정된 평균속도(Vi)보다 값이 클 때
      return (Vf-Vi) / Vf
    elif Vf <= Vi: #자유속도(Vf)가 측정된 평균속도(Vi)보다 값이 작거나 같을 때(제한속도의 120%이상 측정될 경우 혼잡도 0으로 책정)
      return 0
        
#@title ##### function: 러쉬아워 가중치
def add_rushhour_weight(df, target_column):
  # 러시아워 시간대 정의 (오전 7시 ~ 9시, 오후 6시 ~ 8시)
  # rush_hours_am = range(7, 10)
  rush_hours_pm = range(16, 19)

  # 러시아워 시간대에 해당하는 행의 인덱스 찾기
  # rush_hour_indices = df[(df['시간'].isin(rush_hours_am)) | (df['시간'].isin(rush_hours_pm))].index
  rush_hour_indices = df[(df['hour'].isin(rush_hours_pm))].index

  # 타겟 컬럼 값에 0.2 더하기
  df.loc[rush_hour_indices, target_column] += 0.2

  return df

#@title ##### function: 구간화
def categorize_value(value):
  if value < 0.3: # 0 ~ 0.3
    return 0 # 원활
  elif (value >= 0.3) & (value < 0.6): # 0.3 ~ 0.6
    return 1 # 보통
  else: # 0.6 ~ 1
    return 2 # 혼잡
  
"""
1. 데이터 load
"""

def get_link_data(conn):
    
    # 표준노드링크(평화로) 추출 (16개 구간)
    query = text(f"""
        SELECT 
            link_code, link_id, max_spd
        FROM public.tbm_info_link_grouped
    """)    
    #link_df = pd.read_sql(query, conn)

    result = conn.execute(query)
    link_df = pd.DataFrame(result.fetchall(), columns=result.keys())        

    return link_df

def insert_risk_data(conn, df, tb):    
    try:
        df.to_sql(tb,con=conn, if_exists='append', index=False, method='multi')
    except SQLAlchemyError as e:
        print(f"❌ 데이터 삽입 오류 발생: {e}")
