# 액티브 주식 배분 파이프라인

**트렌딩 기반 포트폴리오 자동 구성 시스템**

yfinance + Kafka + Spark + PostgreSQL + Airflow로 구성된 **스케줄 배치 전용** 데이터 파이프라인입니다. 실시간 수집은 금지하며, 배치 수집으로만 파이프라인을 운영합니다.

이 문서는 프로젝트 개요/목적, 아키텍처 요약, 설치·실행, 컴포넌트, 기술 의사결정, 코드 구조, 산출물 문서화를 정리했습니다.

---

## ✅ 프로젝트 개요/목적

**목적**
- ETF 성과를 기준으로 **트렌딩 섹터/벤치마크 ETF**를 자동 식별하고
- 해당 ETF의 보유 종목을 수집해 **멀티 기간(5/10/20일) 포트폴리오**를 자동 구성합니다.

**핵심 가치**
- *자동화*: Airflow 기반 스케줄 배치 실행
- *재현성*: Docker 기반 표준 실행 환경
- *확장성*: Kafka + Spark 분리형 구조
- *정책 준수*: yfinance rate limit 대응을 위한 배치 전용 수집

---

## 🧱 아키텍처 요약

```
Controller DAG (21:30 UTC)
      ├─ Stage 1: 벤치마크 ETF 수집 (Kafka → PostgreSQL)
      ├─ Stage 2: 섹터 ETF 수집 (Kafka → PostgreSQL)
      ├─ Stage 3: 트렌딩 ETF 분석 (Spark)
      ├─ Stage 4: 트렌딩 ETF 보유종목 수집 (Kafka → PostgreSQL)
      └─ Stage 5: 멀티기간 포트폴리오 배분 (Spark)

API/Dashboard
      └─ analytics 결과 조회
```

상세 설계 및 전체 구성도는 [ARCHITECTURE.md](ARCHITECTURE.md)에서 확인할 수 있습니다.

---

## ⚙️ 설치·실행

### 1) 필수 조건
- Docker Desktop
- Git

### 2) 실행
```bash
# Linux/Mac
./start.sh

# Windows
start.bat

# 상태 확인
docker compose ps
```

### 3) Airflow 컨트롤러 실행
```bash
# Airflow UI 접속 후 daily_pipeline_controller DAG 활성화
# http://localhost:8080 (admin/admin)

# 또는 CLI로 직접 트리거
docker compose exec airflow airflow dags trigger daily_pipeline_controller
```

### 4) 서비스 접속
| 서비스 | URL |
|---|---|
| 대시보드 | http://localhost:8050 |
| Airflow | http://localhost:8080 |
| API 문서 | http://localhost:8000/docs |
| Spark Master | http://localhost:8081 |

---

## 🧩 컴포넌트

| 컴포넌트 | 역할 | 주요 위치 |
|---|---|---|
| Airflow | 스케줄/오케스트레이션 | [airflow/dags](airflow/dags) |
| Kafka | 수집 파이프라인 버퍼 | [collector](collector), [consumer](consumer) |
| Spark | 분석/배치 연산 | [batch](batch) |
| PostgreSQL | 저장소 (Collected/Analytics) | [database](database) |
| FastAPI | API 제공 | [api](api) |
| Dashboard | 결과 시각화 | [dashboard](dashboard) |

---

## 🧠 기술 의사결정

1. **배치 전용 수집**: yfinance rate limit 회피 및 안정적 운영을 위해 실시간/스트리밍 수집을 금지합니다.
2. **Kafka 사용**: 수집 로직과 저장 로직을 분리하여 장애 복구, 재처리, 모니터링을 단순화합니다.
3. **Spark 배치 분석**: 다중 기간 수익률 계산, 가중치 산정 등 데이터 처리 비용을 분리합니다.
4. **PostgreSQL 단일 저장소**: 분석/조회/시각화가 동일 스키마를 참조하도록 설계했습니다.
5. **Airflow Controller DAG**: 단계 간 지연과 조건부 실행을 중앙 제어하기 위해 Controller 패턴을 채택했습니다.

---

## 🗂️ 코드 구조

```
airflow/       # DAG 정의
collector/     # Kafka Producer
consumer/      # Kafka Consumer
batch/         # Spark 분석
api/           # FastAPI
dashboard/     # 시각화 앱
database/      # 스키마/마이그레이션
```

관련 파일과 디렉터리 설명은 아래 문서를 참고하세요.
- [ARCHITECTURE.md](ARCHITECTURE.md)
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md)
- [test/README.md](test/README.md)
- [database/NAMING_CONVENTION.md](database/NAMING_CONVENTION.md)

---

## 📌 산출물 문서화

| 테이블 | 용도 |
|---|---|
| `collected_01_daily_etf_ohlc` | 일별 ETF OHLC |
| `collected_04_etf_holdings` | 트렌딩 ETF 보유종목 |
| `collected_06_daily_stock_history` | 보유 종목 OHLC |
| `analytics_03_trending_etfs` | 트렌딩 ETF 식별 결과 |
| `analytics_05_portfolio_allocation` | 멀티기간 포트폴리오 |

스키마는 [database/schema.sql](database/schema.sql)에서 확인할 수 있습니다.

---

## 📚 설계 문서

- 최종 파이프라인 구성도 및 설계 요약: [ARCHITECTURE.md](ARCHITECTURE.md)
- 실행/운영 퀵 레퍼런스: [QUICK_REFERENCE.md](QUICK_REFERENCE.md)

---

**마지막 업데이트**: 2026-01-28
