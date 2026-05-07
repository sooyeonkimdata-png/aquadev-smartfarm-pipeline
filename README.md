# AquaDev 스마트양식 데이터 파이프라인

에이디수산(Aqua Development Ltd.) AD Eyes 플랫폼을 참고해 구현한
스마트양식 수질 모니터링 데이터 파이프라인 토이 프로젝트입니다.


## 프로젝트 구조
```
AD_project/
├── AD_pipeline.py       # 데이터 수집·검증·저장·내보내기
├── dags/
│   ├── AD_pipeline.py   # Airflow에서 import용 복사본
│   └── aquafarm_dag.py  # Airflow DAG (30분 자동 실행)
├── dashboard.py         # Streamlit 대시보드
├── docker-compose.yaml  # Airflow 실행 환경
└── output/              # 생성 파일 (gitignore)
```

## 파이프라인 구조

수집 (NIFS API + 더미 시뮬레이터)
↓
검증 (결측치·범위·중복 체크)
↓
저장 (SQLite + 뷰 4개)
↓
내보내기 (CSV 3종)
↓
시각화 (Streamlit 대시보드)


## 모니터링 지표

| 지표 | 단위 | 경보 기준 |
|---|---|---|
| DO (용존산소) | mg/L | < 4.0 |
| NH4 (암모니아) | ppm | > 0.15 |
| NO2 (아질산염) | ppm | > 0.08 |
| NO3 (질산염) | ppm | > 40 |
| 수온 | ℃ | > 31.5 |
| pH | — | < 7.2 |
| 알칼리도 | mg/L | < 70 |
| 염도 | ppt | < 12 |


## 데이터 출처

- 국립수산과학원 실시간어장정보 API (서비스 3번) — 수온·DO 30분 단위
- 국립수산과학원 어장환경관측자료 API (서비스 8번) — 고창 어장 수질
- 더미 시뮬레이터 — 흰다리새우 생육 조건 기반 랜덤워크 시뮬레이션


## 실행 방법

### 파이프라인 실행
```bash
pip install -r requirements.txt
python AD_pipeline.py
```

### 대시보드 실행
```bash
streamlit run dashboard.py
```

### Airflow 실행
```bash
docker compose up -d
# localhost:8080 접속 (ID: airflow / PW: airflow)
```

## 향후 확장 계획

- AWS S3 연동으로 클라우드 적재 확장
- Power BI 연동 리포트 자동 생성
- 수온·사료량·DO 간 상관관계 분석 및 시각화
- **이상 감지 알고리즘 고도화** (단순 임계값 → 통계적 이상치 탐지)
