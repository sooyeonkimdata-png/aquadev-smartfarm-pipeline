import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px        # 인터랙티브 차트 생성 라이브러리
import plotly.graph_objects as go  # plotly.express 심화 차트 설정도구
from pathlib import Path           # OS 구애없는 경로 조작 모듈
from datetime import datetime
from PIL import Image              # 이미지 처리 라이브러리(Pillow)


# 페이지 기본 설정
st.set_page_config(
    page_title="AD Eyes: 실시간 스마트양식 관리 시스템",
    layout="wide",
)

DB_PATH = Path("./output/aquafarm.db")  # db 경로를 변수에 미리 저장


st.markdown("""
    <style>
    [data-testid="stMetricLabel"] p {
        font-size: 20px !important;
        font-weight: 350 !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 32px !important;
        font-weight: 600 !important;
    }
    </style>
""", unsafe_allow_html=True)


# 로고 이미지 삽입
logo = Image.open("AD_logo_image.png")
st.image(logo, width=120)   # 이미지를 웹 브라우저에 삽입 & 가로 길이 고정


# 경보 기준값 (AD_pipeline.py와 동일)
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


# (Streamlit 대시보드로 효율적으로 불러오기 위한) db 조회 함수
def load_data(query):     
    con = sqlite3.connect(DB_PATH)  # sqlite3로 db에 연결
    df  = pd.read_sql(query, con)   # 쿼리 결과를 df로 반환
    con.close()                     # 접속 종료 (자원 누수 방지)
    return df

# 가장 최근 수집된 데이터 조회 -> KPI 카드에 배치
def load_latest():
    return load_data("SELECT * FROM v_latest")

# 일일 평균 데이터 조회 (날짜 & 수조 순) -> 7일 트렌드 차트에 적용
def load_daily():
    return load_data("SELECT * FROM v_daily_avg ORDER BY date, tank_id")

# 경보 로그 데이터 중 최근 50건 조회 -> 알림 패널 배치
def load_alerts():
    return load_data("""
        SELECT * FROM v_alert_log
        ORDER BY timestamp DESC
        LIMIT 50
    """)

# 특정 수조 선택 or 조회 기간 변경 시, 동적 필터링 적용 (캐시 없이 실시간 조회)
def load_timeseries(tank_id, days=7):
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"""
        SELECT * FROM sensor_timeseries
        WHERE tank_id = '{tank_id}'
        ORDER BY timestamp DESC
        LIMIT {days * 24 * 2}
    """, con)
    con.close()
    return df.sort_values("timestamp")
    
# 헤더
col1, col2 = st.columns([3, 1])  # 화면 비율 나누기 -> 제목, 현재 시간 배치
with col1:
    st.title("AD Eyes: 실시간 스마트양식 관리 시스템")
with col2:
    st.markdown(f"**실시간**  \n{datetime.now().strftime('%Y-%m-%d %H:%M')}")

st.divider()  # 구분선

# 수조 선택 슬라이서
latest_df = load_latest()  # 가장 최신 상태 데이터
tanks     = sorted(latest_df["tank_id"].unique().tolist())
tank_id   = st.selectbox("수조 선택", tanks, index=0)  # 수조 선택할 수 있는 드롭다운 메뉴

# 선택된 수조 최신값 추출
row = latest_df[latest_df["tank_id"] == tank_id].iloc[0]  # 변수에 해당되는 수조의 모든 센서 데이터 접근 가능


# KPI 카드
st.subheader("현재 수질 현황")  # 소제목

# KPI 생성 함수
def kpi_card(col, label, value, unit, lo=None, hi=None, alert_lo=None, alert_hi=None):  # 가로 영역, 지표명, 수치, 단위, 경보 기준값 입력
    # 경보 판정
    is_alert = False                   
    if alert_lo and value < alert_lo:  # 설정된 최저 기준치 존재 & 현재값이 더 작으면 alert를 True로 변경
        is_alert = True
    if alert_hi and value > alert_hi:  # 설정된 최고 기준치 존재 & 현재값이 더 크면 alert를 True로 변경
        is_alert = True

    status = "🔴 경보" if is_alert else "🔵 정상"  # 위 is_alert 값에 따라 경보/정상 변수 처리
    col.metric(                                 # UI 출력
        label=f"{label}   {status}",
        value=f"{value}   {unit}",
    )

c1, c2, c3, c4 = st.columns(4)  # 4개 영역 생성
t = THRESHOLDS                  # 상단 THRESHOLDS 딕셔너리 변수 참조

kpi_card(c1, "DO",  row["do_mg_l"],  "mg/L", alert_lo=t["do_mg_l"]["alert_lo"])  # 최저치보다 낮으면 경보
kpi_card(c2, "NH4", row["nh4_ppm"],  "ppm",  alert_hi=t["nh4_ppm"]["alert_hi"])  # 최고치보다 높으면 경보
kpi_card(c3, "NO2", row["no2_ppm"],  "ppm",  alert_hi=t["no2_ppm"]["alert_hi"])
kpi_card(c4, "pH",             row["ph"],       "",     alert_lo=t["ph"]["alert_lo"])

c5, c6, c7, c8 = st.columns(4)
kpi_card(c5, "수온",    row["water_temp_c"],    "℃",    alert_hi=t["water_temp_c"]["alert_hi"])
kpi_card(c6, "알칼리도", row["alkalinity_mg_l"], "mg/L", alert_lo=t["alkalinity_mg_l"]["alert_lo"])
kpi_card(c7, "염도",    row["salinity_ppt"],    "ppt",  alert_lo=t["salinity_ppt"]["alert_lo"])
kpi_card(c8, "질소전환속도", row["n_conv_speed"], "",    alert_lo=0.4)

st.divider()


# 질소 순환 + 수질 트렌드 차트
ts_df = load_timeseries(tank_id, days=7)  # 선택한 tank_id의 최근 7일간 센서 데이터 df로 반환

st.subheader("질소 순환 모니터링 (7일)")

fig1 = go.Figure()  # 차트 생성 준비

# 질소 화합물 데이터 시각화
fig1.add_trace(go.Scatter(x=ts_df["timestamp"], y=ts_df["nh4_ppm"],
                           name="NH4", line=dict(color="#0066d3", width=2)))  # NH4 라인 그래프 
fig1.add_trace(go.Scatter(x=ts_df["timestamp"], y=ts_df["no2_ppm"],
                           name="NO2", line=dict(color="#41abfc", width=2)))  # NO2 라인 그래프 
fig1.add_trace(go.Scatter(x=ts_df["timestamp"], y=ts_df["no3_ppm"],
                           name="NO3", yaxis="y2", line=dict(color="#85dbe0", width=2)))  # NO3 보조축 (위 2개 지표와 단위가 다를 수 있으므로)
fig1.update_layout(
    yaxis=dict(title="NH4 / NO2 (ppm)"),                           # 왼쪽 축 제목
    yaxis2=dict(title="NO3 (ppm)", overlaying="y", side="right"),  # 오른쪽 축 제목
    legend=dict(orientation="h"),                                  # 범례 가로 배치
    height=400,                                                    # 차트 높이 고정
)

# NO3 환수 기준선 (NO3 농도가 40ppm을 넘어갈 시 환수 타이밍으로 설정)
fig1.add_hline(y=40,
               line=dict(
                    dash="dot", 
                    color="red",
                    width=1.5,
               ),
               annotation_text="NO3 환수 기준 40ppm", 
               yref="y2")
st.plotly_chart(fig1, use_container_width=True)                 # 화면 너비에 맞춰 차트 출력

st.subheader("수질 트렌드 (7일)")

col_l, col_r = st.columns(2)  # DO, pH 7일간 변화를 화면 좌우에 각각 배치 (환경 변화 흐름 파악)

with col_l:  # DO 트렌드
    fig2 = px.line(ts_df, x="timestamp", y="do_mg_l",
                   title="DO", color_discrete_sequence=["#185FA5"])
    fig2.add_hline(y=4.0,
                    line=dict(
                            dash="dot", 
                            color="red",
                            width=1.5,
                    ),
                   annotation_text="경보 기준")                          # 임계치(4.0mg/L) 점선 표시
    fig2.update_layout(height=350)                                     # 그래프 높이 고정
    st.plotly_chart(fig2, use_container_width=True)                    # 화면 너비에 맞춰 차트 출력

with col_r:  # pH 트렌드
    fig3 = px.line(ts_df, x="timestamp", y="ph",
                   title="pH", color_discrete_sequence=["#185FA5"])
    fig3.add_hline(y=7.2,
                    line=dict(
                            dash="dot", 
                            color="red",
                            width=1.5,
                    ),
                   annotation_text="경보 기준")                          # 임계치(7.2) 점선 표시
    fig3.update_layout(height=350)                                     # 그래프 높이 고정
    st.plotly_chart(fig3, use_container_width=True)                    # 화면 너비에 맞춰 차트 출력

st.divider()


# 알림 이력 테이블
st.subheader("알림 이력 (최근 50건)")
alert_df = load_alerts()  # 위에서 정의한 load_alerts 함수로 db에서 최근 50건 경보 데이터 호출

if alert_df.empty:  # 데이터가 없을 때는 예외 처리
    st.info("이상 없음 ✓")
else:  # 데이터가 있는 경우 표 생성
    st.dataframe(
        alert_df,
        column_config={
            "timestamp":   st.column_config.DatetimeColumn("시간"),
            "tank_id":     st.column_config.TextColumn("수조"),
            "alert_types": st.column_config.TextColumn("경보 유형"),
            "do_mg_l":     st.column_config.NumberColumn("DO", format="%.3f"),
            "nh4_ppm":     st.column_config.NumberColumn("NH4", format="%.4f"),
            "ph":          st.column_config.NumberColumn("pH", format="%.2f"),
        },
        use_container_width=True,  # 화면 너비에 맞춤
        hide_index=True,           # 인덱스 숨기기
    )

st.divider()

st.caption("30분마다 자동 갱신 | 에이디수산 AD Eyes 대시보드 자동화 프로젝트")  # 캡션 