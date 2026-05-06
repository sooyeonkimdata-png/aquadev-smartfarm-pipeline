# Airflow 통해 데이터 수집 ~ 저장까지 단계별 자동화 설계

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# AD_pipeline.py 경로 추가 (AD_pipeline.py 내 함수 & api 키 가져오기)
from AD_pipeline import (
    generate_sensor_timeseries,
    validate_data,
    fetch_nifs_realtime,
    fetch_nifs_environment,
    save_to_db,
    export_csvs,
    NIFS_API_KEY,
    NIFS_ENV_API_KEY,
)

# DAG 기본 설정
default_args = {
    "owner":            "aquadev",
    "retries":          1,                     # 실패 시 1회 재시도
    "retry_delay":      timedelta(minutes=5),  # 재시도 간격 5분
    "start_date":       datetime(2026, 1, 1),
}

with DAG(
    dag_id="aquafarm_pipeline",
    default_args=default_args,
    schedule_interval="*/30 * * * *",  # 30분마다 실행
    catchup=False,                     # DAG 활성화 시점 기준 과거 데이터 건너뜀
    tags=["aquadev", "smartfarm"],      
) as dag:

    # 더미 생성 + API 수집 → XCom에 저장
    def task_collect(**context):
        end   = datetime.now()            # 끝: 현재 시간 기준
        start = end - timedelta(days=90)  # 시작: 현재 기준 90일 전

        sensor_df = generate_sensor_timeseries(start, end)     # 시계열 더미 생성
        coastal_df = fetch_nifs_realtime(NIFS_API_KEY)         # 실시간 어장 정보 데이터 수집
        env_df     = fetch_nifs_environment(NIFS_ENV_API_KEY)  # 어장 환경 관측 데이터 수집 (모두 df로 반환)

        # XCom으로 다음 태스크에 전달 (건수만 전달, df는 파일로)
        context["ti"].xcom_push(key="sensor_rows",  value=len(sensor_df))
        context["ti"].xcom_push(key="coastal_rows", value=len(coastal_df))
        context["ti"].xcom_push(key="env_rows",     value=len(env_df))

        # 로컬에 데이터 임시 저장 (XCom은 메타데이터 전송에 최적이므로, parquet 처리)
        sensor_df.to_parquet("/tmp/sensor_df.parquet",   index=False)
        coastal_df.to_parquet("/tmp/coastal_df.parquet", index=False) if not coastal_df.empty else None
        env_df.to_parquet("/tmp/env_df.parquet",         index=False) if not env_df.empty else None


    # 데이터 품질 검증
    def task_validate(**context):
        import pandas as pd
        sensor_df = pd.read_parquet("/tmp/sensor_df.parquet")
        result    = validate_data(sensor_df)                    # 결측치, 범위 이탈 여부, 중복 타임스탬프 등
        if not result:
            raise ValueError("데이터 품질 검증 실패 — 파이프라인 중단")  # 결과 False인 경우, 파이프라인에 강제 에러 발생시키기


    # SQLite에 데이터 저장
    def task_save(**context):
        import pandas as pd
        sensor_df  = pd.read_parquet("/tmp/sensor_df.parquet")
        coastal_df = pd.read_parquet("/tmp/coastal_df.parquet") if os.path.exists("/tmp/coastal_df.parquet") else pd.DataFrame()  # 경로에 파일이 없으면 빈 df생성 = 파이프라인 중단 방지
        env_df     = pd.read_parquet("/tmp/env_df.parquet")     if os.path.exists("/tmp/env_df.parquet")     else pd.DataFrame()
        save_to_db(sensor_df, coastal_df, env_df)  # 위 3개 df를 SQLite에 저장


    # CSV 내보내기
    def task_export(**context):
        import pandas as pd
        sensor_df = pd.read_parquet("/tmp/sensor_df.parquet")
        export_csvs(sensor_df)

    # 태스크 생성
    t1 = PythonOperator(task_id="collect",  python_callable=task_collect)
    t2 = PythonOperator(task_id="validate", python_callable=task_validate)
    t3 = PythonOperator(task_id="save",     python_callable=task_save)
    t4 = PythonOperator(task_id="export",   python_callable=task_export)

    t1 >> t2 >> t3 >> t4