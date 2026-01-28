# 데이터 플로우 기반 명명 규칙

⚠️ **중요**: NO REAL-TIME/STREAMING COLLECTION (yfinance rate limit 회피)

## 시스템 아키텍처
```
yfinance API (스케줄 배치 전용)
    ↓
Kafka Producer → Kafka Topic → Kafka Consumer → PostgreSQL (수집 데이터)
(Airflow 스케줄 기반)
                                                      ↓
                         Airflow (스케줄러) → Spark Batch Jobs → PostgreSQL (분석 결과)
                                                                ↓
                                                          API → Dashboard
```

## 원칙
- 테이블명은 **데이터 특성**으로 구분합니다
- **배치 수집만 사용** (NO real-time, NO streaming, NO hourly auto-collection)
- Kafka는 Airflow 스케줄 파이프라인에서만 사용

## 테이블 명명: `[category]_[frequency]_[entity]`

### 카테고리별 분류

#### Collected (수집 데이터) - Kafka Consumer가 DB에 저장 (Airflow 스케줄 기반)
Kafka Producer → Topic → Consumer → PostgreSQL

- `collected_06_daily_stock_history` - 일별 주식 히스토리 (Stage 4)
  - 수집: Airflow 5-Stage Pipeline (월-금 12:00 UTC)
  - 저장: Kafka Consumer (`kafka_03_consumer_stock_daily.py`)
  - 읽기: Spark, API

- `collected_00_meta_etf` - ETF 메타데이터 + 보유종목
  - 수집: Airflow `06_etf_holdings_daily_dag.py` (월-금 09:00 UTC)
  - 저장: Kafka Consumer (스케줄 배치)
  - 읽기: Spark, API

- `collected_01_daily_etf_ohlc` - ETF 일별 OHLC (Stage 1, 2)
  - 수집: Airflow 5-Stage Pipeline (월-금 09:00, 10:00 UTC)
  - 저장: Kafka Consumer (`kafka_01_consumer_etf_daily.py`)
  - 읽기: Spark, API

- `collected_04_etf_holdings` - ETF 보유종목 (Stage 4)
  - 수집: Airflow `04_daily_trending_etf_holdings_collection_dag.py` (월-금 12:00 UTC)
  - 저장: Kafka Consumer (`kafka_02_consumer_etf_holdings.py`)
  - 읽기: Spark, API

#### Analytics (분석 결과) - Spark Batch Jobs가 계산하여 저장
Airflow → Spark → PostgreSQL

- `analytics_05_portfolio_allocation` - 액티브 포트폴리오 (5d/10d/20d)
  - 계산: `spark_02_active_stock_allocator.py` (Stage 5, 월-금 13:00 UTC)
  - 저장: Spark Batch Job
  - 읽기: API, Dashboard

- `analytics_08_monthly_portfolio` - 월별 리밸런싱 포트폴리오
  - 계산: `spark_04_monthly_portfolio_rebalancer.py` (매월 마지막 일요일 14:00 UTC)
  - 저장: Spark Batch Job
  - 읽기: API, Dashboard

- `analytics_03_trending_etfs` - 트렌딩 ETF 분석 (Stage 3)
  - 계산: `spark_01_trending_etf_identifier.py` (매일 11:00 UTC)
  - 저장: Spark Batch Job
  - 읽기: Stage 4 Producer, API

#### Logs (로그/에러)
- `logs_consumer_error` - Kafka Consumer 파싱 에러
  - 저장: Kafka Consumer
  - 읽기: Monitoring

## Python 파일 명명: `[tech]_[component]_[workname].py`

### ⚠️ 수집 제약사항
- ❌ 실시간(real-time) 수집 금지
- ❌ 스트리밍(streaming) 수집 금지
- ❌ 시간별(hourly) 자동 수집 금지
- ✅ Airflow 스케줄 배치 수집만 사용

### Airflow DAGs (스케줄 파이프라인)
**5-Stage Daily Pipeline (월-금 09:00-13:00 UTC)**
- `00_daily_pipeline_controller_dag.py` - 최상위 컨트롤러
- `01_daily_benchmark_etf_collection_dag.py` - Stage 1 (Benchmark)
- `02_daily_sector_etf_collection_dag.py` - Stage 2 (Sector)
- `03_daily_trending_etf_analysis_dag.py` - Stage 3 (Analysis)
- `04_daily_trending_etf_holdings_collection_dag.py` - Stage 4 (Holdings)
- `05_daily_portfolio_allocation_dag.py` - Stage 5 (Allocation)

**Monthly & Others**
- `08_monthly_portfolio_rebalance_dag.py` - 월별 리밸런싱
- `06_etf_holdings_daily_dag.py` - ETF 보유종목 일별 수집
- `07_monthly_etf_collection_last_weekend_dag.py` - 월말 ETF 수집
- `09_etf_top_holdings_analysis_dag.py` - 상위 보유종목 분석

### Spark (배치 분석 전용 - NO streaming)
**Batch Jobs (Airflow가 스케줄링)**
- `spark_01_trending_etf_identifier.py` - 트렌딩 ETF 식별
- `spark_02_active_stock_allocator.py` - 포트폴리오 분석
- `spark_04_monthly_portfolio_rebalancer.py` - 월별 리밸런싱
- `spark_03_etf_top_holdings.py` - ETF 보유종목 분석

### Kafka Producers & Consumers
- `kafka_01_producer_etf_daily.py` / `kafka_01_consumer_etf_daily.py`
- `kafka_02_producer_etf_holdings.py` / `kafka_02_consumer_etf_holdings.py`
- `kafka_03_producer_trending_etf_holdings.py`
- `kafka_04_producer_stock_daily.py` / `kafka_03_consumer_stock_daily.py`

### API & Dashboard
- `api/main.py`
- `dashboard/dashboard_finviz_app.py`

## Kafka Topics: `topic_[name]`
- `etf-daily-data` - 일별 ETF 데이터
- `etf-holdings-data` - ETF 보유종목 데이터
- `stock-daily-data` - 일별 주식 데이터

## Docker Containers: `actstock_[service]`
- `actstock_postgres`
- `actstock_kafka`
- `actstock_spark_master`
- `actstock_airflow_webserver`
- `actstock_api`
- `actstock_dashboard`

