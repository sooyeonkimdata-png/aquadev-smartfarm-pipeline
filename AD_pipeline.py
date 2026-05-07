import sqlite3  # 파이썬에 기본으로 내장된 경량 db
import logging  # 프로그램이 실행되는 동안 발생하는 일들(접속 성공, 에러 등)을 기록하는 도구
import os
import sys  # 파일 경로, 시스템 제어
from datetime import datetime, timedelta  # 날짜, 시간 계산
from pathlib import Path  # 파일이나 폴더의 경로를 객체 지향적으로 다루는 도구

import pandas as pd
import numpy as np
from dotenv import load_dotenv  # API 키, DB 비번을 프로그램으로 불러올 때 사용


load_dotenv()  # .env 파일에서 환경변수 읽기 *보안

# 로깅 규칙
logging.basicConfig(
    level=logging.INFO,  # INFO 등급 이상 메시지만 기록
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# 주요 상수 설정
NIFS_API_KEY     = os.getenv("NIFS_API_KEY",     "YOUR_API_KEY_HERE")
NIFS_ENV_API_KEY = os.getenv("NIFS_ENV_API_KEY", "YOUR_API_KEY_HERE")
DAYS = 90  # 시뮬레이션 전체 기간
FREQ_MIN = 30  # 수집 단위시간(분)
TANKS = [f"T{i:02d}" for i in range(1, 9)]  # 수조 리스트


# 출력 경로 설정
OUTPUT_DIR = Path("./output")
DB_PATH = OUTPUT_DIR / "aquafarm.db"
OUTPUT_DIR.mkdir(exist_ok=True)  # exist_ok=True: 폴더가 이미 있어도 오류 내지 않고 그대로 진행


# 경보 기준값
# alert_lo / alert_hi 범위를 벗어날 시, 즉시 알림 보내야 하는 위험 임계값 
# lo, hi: 정상적인 관리 범위
THRESHOLDS = {
    "do_mg_l":         {"lo": 5.0,  "hi": 8.0,   "alert_lo": 4.0},
    "nh4_ppm":         {"lo": 0.0,  "hi": 0.10,  "alert_hi": 0.15},
    "no2_ppm":         {"lo": 0.0,  "hi": 0.05,  "alert_hi": 0.08},
    "no3_ppm":         {"lo": 0.0,  "hi": 50.0,  "alert_hi": 40.0},
    "water_temp_c":    {"lo": 26.0, "hi": 30.0,  "alert_hi": 31.5},
    "ph":              {"lo": 7.5,  "hi": 8.5,   "alert_lo": 7.2},
    "alkalinity_mg_l": {"lo": 80.0, "hi": 150.0, "alert_lo": 70.0},
    "salinity_ppt":    {"lo": 15.0, "hi": 25.0,  "alert_lo": 12.0},
}


# 랜덤 워크 생성 함수 (연속적으로 변하는 센서 데이터 생성(갑자기 튀지 x))
def _randwalk(rng, lo, hi, n, noise=0.03):
    steps  = rng.normal(0, noise * (hi - lo), n)  # 정규분포로 0 근처의 랜덤 변화량 생성, 노이즈로 변화 폭 조절
    series = np.cumsum(steps) + (lo + hi) / 2   # 변화량 누적합
    margin = (hi - lo) * 0.15  # 정상 범위에서 15% 여유공간 계산 -> 데이터가 너무 벗어나지 않게 설정
    return np.clip(series, lo - margin, hi + margin)  # 값 범위 제한. 넘어가면 끝값으로 고정

# 질소 전환 속도 계산 함수
def _nitrogen_conversion_speed(nh4, no2, no3, window=4):
    nh4_delta = -np.diff(nh4, prepend=nh4[0])  # 암모니아 감소량 계산
    no_sum = no2 + no3 / 50  # NO2와 NO3 비중합산
    no_delta = np.diff(no_sum, prepend=no_sum[0])  # NO2와 NO3 합산치 증가량 계산
    
    with np.errstate(divide='ignore', invalid='ignore'):  # 0으로 나누는 경우 방지
        speed = np.where(no_delta > 0.001, nh4_delta / no_delta, 1.0)  # 감소 증가 비율 계산. no_delta가 거의 0일 때 오류 방지 차 기본값 1 설정
        
    speed = np.clip(speed, 0.0, 3.0)  # 비율을 0~3 사이로 제한
    kernel = np.ones(window) / window  # 이동 평균 커널 
    return np.round(np.convolve(speed, kernel, mode='same'), 4)  # 이동 평균 적용 (데이터 부드럽게 만들기))

# 수조별 시계열 + 이상 구간 삽입 + 경보 판정
def generate_sensor_timeseries(start, end):  # 데이터 시작 & 종료일 날짜를 받아 센서 데이터 생성
    timestamps = pd.date_range(start, end, freq=f"{FREQ_MIN}min") # 시작일 ~ 종료일까지 30분 간격 시간표 생성 
    n    = len(timestamps)
    slot = max(1, int(60 / FREQ_MIN))  # 30분 간격에 따른 1시간 당 2슬롯 계산
    rows = []

    for tank_id in TANKS:  # 수조 하나씩 데이터 생성
        seed = 42 + sum(ord(c) for c in tank_id)
        rng  = np.random.default_rng(seed)  # 난수 생성기 준비

        # 정상 데이터 생성
        base_do   = rng.uniform(6.0, 7.0)
        base_nh4  = rng.uniform(0.03, 0.06)
        base_no2  = rng.uniform(0.01, 0.03)
        base_temp = rng.uniform(27.5, 29.0)
        base_ph   = rng.uniform(7.7, 8.1)
        base_alk  = rng.uniform(100.0, 130.0)
        base_sal  = rng.uniform(19.0, 21.0)
        
        # 약간의 노이즈 추가
        do_arr   = base_do   + rng.normal(0, 0.15, n)
        nh4_arr  = base_nh4  + rng.normal(0, 0.008, n)
        no2_arr  = base_no2  + rng.normal(0, 0.004, n)
        no3_arr  = np.linspace(10, 45, n) + rng.normal(0, 0.3, n)  # 서서히 쌓이는 구조로
        temp_arr = base_temp + rng.normal(0, 0.2, n)
        ph_arr   = base_ph   + rng.normal(0, 0.05, n)
        alk_arr  = base_alk  + rng.normal(0, 3.0, n)
        sal_arr  = base_sal  + rng.normal(0, 0.2, n)

        # 값 범위 제한 (물리적 한계 설정)
        do_arr  = np.clip(do_arr,  4.0, 9.0)
        nh4_arr = np.clip(nh4_arr, 0.0, 0.5)
        no2_arr = np.clip(no2_arr, 0.0, 0.2)
        no3_arr = np.clip(no3_arr, 0.0, 70.0)
        ph_arr  = np.clip(ph_arr,  6.5, 9.0)
        alk_arr = np.clip(alk_arr, 50.0, 200.0)
        sal_arr = np.clip(sal_arr, 15.0, 25.0)
        
        # 최근 7일 이상 구간 — 날짜 기준으로 직접 삽입
        one_day = int(24 * 60 / FREQ_MIN)  # 하루 = 48포인트
        recent  = n - (7 * one_day)

        # DO 급락 — 5일 전 반나절
        do_arr[recent + one_day * 2 : recent + one_day * 2 + one_day // 4] = 3.2

        # NH4 급증 — 3일 전 반나절
        nh4_arr[recent + one_day * 4 : recent + one_day * 4 + one_day // 3] = 0.22

        # NO2 급증 — 2일 전 반나절
        no2_arr[recent + one_day * 5 : recent + one_day * 5 + one_day // 4] = 0.12

        # pH 하락 — 1일 전 반나절
        ph_arr[recent + one_day * 6 : recent + one_day * 6 + one_day // 4] = 7.0

        for i, ts in enumerate(timestamps):  # 시간표와 수치 배열을 하나씩 꺼내 실제 데이터로 변환
            do_v   = round(float(do_arr[i]),   3)  # 소수점 자릿수 맞추기
            nh4_v  = round(float(nh4_arr[i]),  4)
            no2_v  = round(float(no2_arr[i]),  4)
            no3_v  = round(float(no3_arr[i]),  2)
            temp_v = round(float(temp_arr[i]), 2)
            ph_v   = round(float(ph_arr[i]),   2)
            alk_v  = round(float(alk_arr[i]),  1)
            sal_v  = round(float(sal_arr[i]),  2)
            cs_v   = round(float(_nitrogen_conversion_speed(    # 질소 전환 속도 계산
                np.array([nh4_v]), np.array([no2_v]), np.array([no3_v]))[0]), 4)

            t = THRESHOLDS
            alerts = []
            if do_v   < t["do_mg_l"]["alert_lo"]:          alerts.append("DO_LOW")  # 기준 치 밖을 갈 경우 alerts 리스트 삽입
            if nh4_v  > t["nh4_ppm"]["alert_hi"]:          alerts.append("NH3_HIGH")
            if no2_v  > t["no2_ppm"]["alert_hi"]:          alerts.append("NO2_HIGH")
            if no3_v  > t["no3_ppm"]["alert_hi"]:          alerts.append("NO3_HIGH")
            if temp_v > t["water_temp_c"]["alert_hi"]:      alerts.append("TEMP_HIGH")
            if ph_v   < t["ph"]["alert_lo"]:               alerts.append("PH_LOW")
            if alk_v  < t["alkalinity_mg_l"]["alert_lo"]:  alerts.append("ALK_LOW")
            if cs_v   < 0.4:                               alerts.append("NITRO_SLOW")

            rows.append({
                "timestamp":       ts,      "tank_id":         tank_id,
                "do_mg_l":         do_v,    "nh4_ppm":         nh4_v,
                "no2_ppm":         no2_v,   "no3_ppm":         no3_v,
                "water_temp_c":    temp_v,  "ph":              ph_v,
                "alkalinity_mg_l": alk_v,   "salinity_ppt":    sal_v,
                "n_conv_speed":    cs_v,
                "alert":           int(bool(alerts)),
                "alert_types":     ",".join(alerts) if alerts else None,
                "data_source":     "dummy_sensor",
            })

    df = pd.DataFrame(rows)
    log.info(f"[더미 생성] {len(df):,}행 | {start.date()}~{end.date()}")
    return df


# QA - 초기화 및 결측치 검사
def validate_data(df):
    log.info("[검증] 데이터 품질 검사 시작")
    issues = []

    # 결측치 확인 & issues 리스트에 추가
    null_counts = df.drop(columns=["alert_types"]).isnull().sum()  # alert_types는 경보 없을 때 None이 정상으로 제외
    null_cols   = null_counts[null_counts > 0]
    if not null_cols.empty:
        issues.append(f"결측치 발견: {null_cols.to_dict()}")  # .to_dict: 시리즈 -> 딕셔너리로

    # 값 범위 이탈 확인 (물리적 한계 설정으로 센서 오류 탐지)
    range_checks = {
        "do_mg_l":      (0, 20),
        "nh4_ppm":      (0, 5),
        "no2_ppm":      (0, 5),
        "no3_ppm":      (0, 200),
        "water_temp_c": (0, 45),
        "ph":           (0, 14),
    }
    
    for col, (lo, hi) in range_checks.items():
        if col not in df.columns:
            continue
        out = df[(df[col] < lo) | (df[col] > hi)]  # 정상 범위 밖 데이터 필터링, out에 저장
        if not out.empty:
            issues.append(f"{col} 물리적 범위 이탈: {len(out)}건")  # 범위 밖 데이터 있는 경우 issues에 기록

    # 중복 타임스탬프 확인
    dupes = df.duplicated(subset=["timestamp", "tank_id"]).sum()
    if dupes > 0:
        issues.append(f"중복 타임스탬프: {dupes}건")

    # 결과 출력
    if issues:
        for iss in issues:
            log.warning(f"[검증] {iss}")  # 문제가 있는 경우 경고 로그 기록
    else:
        log.info("[검증] 이상 없음 ✓")      # 문제가 없는 경우 완료 로그 기록

    return len(issues) == 0  # True면 통과, False면 DAG에서 경보 가능

    
# NIFS 실시간 어장 정보 수집
def fetch_nifs_realtime(api_key, obs_code="DT_0016"):  # DT_0016 = 완도권. 고창과 가장 근접한 관측소
    if api_key == "YOUR_API_KEY_HERE":
        log.warning("[NIFS 실시간] 키 없음 → 건너뜁니다")    # 키 없을 시 오류 방지
        return pd.DataFrame()
    try:
        import requests
        res = requests.get(
            "https://www.nifs.go.kr/OpenAPI_json?id=risaList",
            params={"key": api_key},
            timeout=10,
        )
        res.raise_for_status()  # 오류 응답을 받을 시, 즉시 에러 발생 및 except로 넘기기

        # JSON 데이터 파싱 & 정제
        items = (res.json()
            .get("body", {})
            .get("item", []))

        if isinstance(items, dict):
            items = [items]  # 결과 1건일 때 딕셔너리로 오는 경우 리스트로 통일

        df = pd.DataFrame(items)
        if df.empty:
            return df  # 데이터가 하나도 없을 시, 그대로 종료

        # 컬럼명을 DB 규칙에 맞게 변경
        df = df.rename(columns={
            "obs_dat":     "timestamp",
            "wtr_tmp":     "coastal_temp_c",
            "sta_nam_kor": "obs_station",
            "sta_cde":     "obs_code",
        })
        df["timestamp"]   = pd.to_datetime(df["timestamp"])
        df["data_source"] = "nifs_realtime_api"   # 출처 기록으로 나중에 섞인 데이터 구분
        log.info(f"[NIFS 실시간] {len(df)}건 수신")  # 수집된 데이터 건수 기록
        return df

    # 예외 발생 시 내용 기록, 빈 df 반환
    except Exception as e:
        log.error(f"[NIFS 실시간] 실패: {e}")
        return pd.DataFrame() 
    

# NIFS 어장 환경 관측 데이터 수집 
def fetch_nifs_environment(api_key):
    from datetime import datetime, timedelta
    if api_key == "YOUR_API_KEY_HERE":
        log.warning("[NIFS 어장환경] 키 없음 → 건너뜁니다")
        return pd.DataFrame()  # 키 없을 시 오류 방지
    try:
        import requests
        res = requests.get(
            "https://www.nifs.go.kr/OpenAPI_json?id=femoSeaList",
            params={
                "key":   api_key,
                "sdate": (datetime.now() - timedelta(days=730)).strftime("%Y%m%d"),  # 90 → 730 (90일은 데이터 집계 안 되어 2년으로 변경)
                "edate": datetime.now().strftime("%Y%m%d"),
            },
            timeout=10,
        )
        res.raise_for_status()  # 오류 응답을 받을 시, 즉시 에러 발생 및 except로 넘기기

        items = (res.json()
            .get("body", {})
            .get("item", []))

        if isinstance(items, dict):
            items = [items]  # 결과 1건일 때 딕셔너리로 오는 경우 리스트로 통일

        df = pd.DataFrame(items)
        
        if not df.empty:
            if "FISHERY" in df.columns:
                df = df[df["FISHERY"] == "고창"]  # 고창 어장만 필터링 
                log.info(f"[NIFS 어장환경] 고창 필터링 후 {len(df)}건")
            df["data_source"] = "nifs_env_api"
            log.info(f"[NIFS 어장환경] {len(df)}건 수신")
        return df  # 데이터가 있는 경우, 출처 컬럼 추가 및 수신 건수를 로그에 남기기 & df 반환

    # 예외 발생 시 내용 기록, 빈 df 반환
    except Exception as e:
        log.error(f"[NIFS 어장환경] 실패: {e}")
        return pd.DataFrame()


# SQLite DB에 데이터 저장 및 뷰 생성
def save_to_db(sensor_df, coastal_df, env_df):
    con = sqlite3.connect(DB_PATH)  # aquafarm.db와 SQLite DB 연결

    # 테이블 저장
    sensor_df.to_sql("sensor_timeseries", con, if_exists="replace", index=False)  # 생성된 센서 데이터를 sensor_timeseries 테이블로 DB에 저장
    if not coastal_df.empty:  # api로 받아온 데이터가 있을 때만 테이블을 DB에 저장 
        coastal_df.to_sql("coastal_api", con, if_exists="replace", index=False)
    if not env_df.empty:
        env_df.to_sql("env_api", con, if_exists="replace", index=False)

    # 뷰 생성 (여러 개 SQL 쿼리 한 번에 실행)
    con.executescript("""
        -- 수조별 가장 최신 측정값 (KPI 카드용)
        DROP VIEW IF EXISTS v_latest;   -- v_latest: 최신 값 
        CREATE VIEW v_latest AS
        SELECT s.* FROM sensor_timeseries s
        INNER JOIN (
            SELECT tank_id, MAX(timestamp) AS max_ts
            FROM sensor_timeseries GROUP BY tank_id
        ) m ON s.tank_id = m.tank_id AND s.timestamp = m.max_ts;

        -- 이상 발생 전체 이력 (알림 패널용)
        DROP VIEW IF EXISTS v_alert_log;
        CREATE VIEW v_alert_log AS
        SELECT date(timestamp) AS date, tank_id, timestamp,
               alert_types, do_mg_l, nh4_ppm, no2_ppm, no3_ppm, ph
        FROM sensor_timeseries WHERE alert = 1    -- 이상 발생: alert=1
        ORDER BY timestamp DESC;

        -- 일간 평균 (주간 트렌드 차트용)
        DROP VIEW IF EXISTS v_daily_avg;
        CREATE VIEW v_daily_avg AS
        SELECT date(timestamp) AS date, tank_id,
               ROUND(AVG(do_mg_l),3)          AS avg_do,
               ROUND(AVG(nh4_ppm),4)          AS avg_nh4,
               ROUND(AVG(no2_ppm),4)          AS avg_no2,
               ROUND(AVG(no3_ppm),2)          AS avg_no3,
               ROUND(AVG(water_temp_c),2)     AS avg_temp,
               ROUND(AVG(ph),2)               AS avg_ph,
               ROUND(AVG(alkalinity_mg_l),1)  AS avg_alk,
               ROUND(AVG(salinity_ppt),2)     AS avg_sal,
               ROUND(AVG(n_conv_speed),4)     AS avg_conv_speed,
               SUM(alert)                     AS total_alerts
        FROM sensor_timeseries
        GROUP BY date(timestamp), tank_id;

        -- NO3 일간 추이 + 환수 신호 플래그 (환수 타이밍 차트용)
        DROP VIEW IF EXISTS v_no3_trend;
        CREATE VIEW v_no3_trend AS
        SELECT date(timestamp) AS date, tank_id,
               ROUND(AVG(no3_ppm),2) AS avg_no3,
               CASE WHEN AVG(no3_ppm) > 40 THEN 1 ELSE 0 END AS water_change_flag
        FROM sensor_timeseries
        GROUP BY date(timestamp), tank_id;
    """)

    con.commit()  # 저장/생성된 데이터 커밋
    con.close()   # DB 안전하게 종료
    log.info(f"[DB] {DB_PATH} 저장 완료 (뷰 4개)")  # 작업 완료 로그 기록
    

# csv 내보내기
def export_csvs(sensor_df):
    # 원본 전체 — 30분 간격 raw 데이터
    sensor_df.to_csv(
        OUTPUT_DIR / "sensor_timeseries.csv", index=False, encoding="utf-8-sig")

    # Power BI / Tableau 연동용 피벗
    # 수조 ID가 컬럼으로 펼쳐지는 형태 (예: do_mg_l_T01, do_mg_l_T02 ...)
    pivot = sensor_df.pivot_table(
        index="timestamp", columns="tank_id",
        values=["do_mg_l","nh4_ppm","no2_ppm","no3_ppm",
                "water_temp_c","ph","alkalinity_mg_l","salinity_ppt","n_conv_speed"],
        aggfunc="mean",
    )
    pivot.columns = ["_".join(c) for c in pivot.columns]
    pivot.reset_index().to_csv(
        OUTPUT_DIR / "pivot_for_bi.csv", index=False, encoding="utf-8-sig")

    # 일간 평균 — 주간 트렌드 차트용
    daily = sensor_df.copy()
    daily["date"] = pd.to_datetime(daily["timestamp"]).dt.date
    (daily.groupby(["date","tank_id"])[
        ["do_mg_l","nh4_ppm","no2_ppm","no3_ppm",
         "water_temp_c","ph","alkalinity_mg_l","salinity_ppt","n_conv_speed","alert"]
    ].mean().round(4).reset_index()
     .to_csv(OUTPUT_DIR / "daily_avg.csv", index=False, encoding="utf-8-sig"))

    log.info(f"[CSV] 3개 파일 → {OUTPUT_DIR}/")
    

# 테스트 코드 
if __name__ == "__main__":
    from datetime import datetime, timedelta
    end   = datetime.now()
    start = end - timedelta(days=DAYS)

    df    = generate_sensor_timeseries(start, end)
    valid = validate_data(df)
    c_df  = fetch_nifs_realtime(NIFS_API_KEY)
    e_df  = fetch_nifs_environment(NIFS_ENV_API_KEY)

    try:
        save_to_db(df, c_df, e_df)
    except Exception as e:
        print(f"DB 저장 실패: {e}")

    export_csvs(df)

    print(f"검증 통과: {valid}")
    print(f"연안 실시간: {len(c_df)}건")
    print(f"어장환경: {len(e_df)}건")