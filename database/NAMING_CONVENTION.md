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

- `collected_daily_stock_history` - 일별 주식 히스토리 (Stage 4)
  - 수집: Airflow 5-Stage Pipeline (월-금 12:00 UTC)
  - 저장: Kafka Consumer (스케줄 배치)
  - 읽기: Spark, API

- `collected_meta_etf` - ETF 메타데이터 + 보유종목
  - 수집: Airflow etf_holdings_daily_dag (월-금 09:00 UTC)
  - 저장: Kafka Consumer (스케줄 배치)
  - 읽기: Spark, API

- `collected_daily_etf_ohlc` - ETF 일별 OHLC (Stage 1)
  - 수집: Airflow 5-Stage Pipeline (월-금 09:00 UTC)
  - 저장: Kafka Consumer (스케줄 배치)
  - 읽기: Spark, API

- `collected_daily_benchmark_ohlc` - 벤치마크 OHLC (SPY/QQQ)
  - 수집: Airflow benchmark_data_daily_dag (월-금 09:00 UTC)
  - 저장: Kafka Consumer (스케줄 배치)
  - 읽기: Spark

- `collected_monthly_benchmark_holdings` - 벤치마크 보유종목 (SPY/QQQ)
  - 수집: Airflow benchmark_holdings_monthly_dag (매월 1일 09:00 UTC)
  - 저장: Kafka Consumer (스케줄 배치)
  - 읽기: Spark

#### Analytics (분석 결과) - Spark Batch Jobs가 계산하여 저장
Airflow → Spark → PostgreSQL

- `analytics_portfolio_allocation` - 액티브 포트폴리오 (5d/10d/20d)
  - 계산: spark_active_stock_allocator.py (Airflow 5-Stage Pipeline Stage 5, 월-금 13:00 UTC)
  - 저장: Spark Batch Job
  - 읽기: API, Dashboard

- `analytics_monthly_portfolio` - 월별 리밸런싱 포트폴리오 (5d/10d/20d)
  - 계산: spark_monthly_portfolio_rebalancer.py (Airflow 매월 마지막 일요일 14:00 UTC)
  - 저장: Spark Batch Job
  - 읽기: API, Dashboard

- `analytics_sector_trending` - 월간 섹터 트렌딩
  - 계산: batch_monthly_sector_analyzer.py (Airflow monthly_sector_trending_dag, 매월 1일 10:00 UTC)
  - 저장: Spark Batch Job
  - 읽기: API

- `analytics_stock_trending` - 월간 종목 트렌딩
  - 계산: batch_monthly_sector_analyzer.py (Airflow monthly_sector_trending_dag, 매월 1일 10:00 UTC)
  - 저장: Spark Batch Job
  - 읽기: API

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
- `stock_market_pipeline.py` - 5-Stage 통합 파이프라인

**Daily Collectors (월-금)**
- `etf_holdings_daily_dag.py` - ETF 메타 + 보유종목 (09:00 UTC)
- `benchmark_data_daily_dag.py` - SPY/QQQ OHLC (09:00 UTC)

**Monthly Collectors**
- `benchmark_holdings_monthly_dag.py` - SPY/QQQ 보유종목 (매월 1일 09:00 UTC)
- `monthly_sector_trending_dag.py` - 섹터/종목 트렌딩 (매월 1일 10:00 UTC)
- `monthly_portfolio_rebalance_dag.py` - 월별 리밸런싱 (매월 마지막 일요일 14:00 UTC)

### Spark (배치 분석 전용 - NO streaming)
**Batch Jobs (Airflow가 스케줄링)**
- `spark_active_stock_allocator.py` - 포트폴리오 분석 (5d/10d/20d)
- `spark_monthly_portfolio_rebalancer.py` - 월별 리밸런싱 (5d/10d/20d)
- `batch_monthly_sector_analyzer.py` - 섹터 트렌딩 분석

### API & Dashboard
- `api_main.py`
- `api_routes_stocks.py` - Stock 관련 API
- `api_routes_sectors.py` - Sector 관련 API
- `dashboard_finviz_app.py` - Dash 대시보드

## Kafka Topics: `topic_[name]`
- `stock-market-data` - 5-Stage Pipeline 통합 토픽 (스케줄 배치)
- `etf-holdings-data` - ETF 보유종목 토픽 (스케줄 배치)
- `benchmark-ohlc-data` - 벤치마크 OHLC 토픽 (스케줄 배치)
- `benchmark-holdings-data` - 벤치마크 보유종목 토픽 (스케줄 배치)

## Airflow DAG IDs
- `stock_market_data_pipeline` - 5-Stage 일별 파이프라인
- `etf_holdings_daily_collection` - ETF 보유종목 일별 수집
- `benchmark_data_daily_collection` - 벤치마크 일별 수집
- `benchmark_holdings_monthly_collection` - 벤치마크 월별 수집
- `monthly_sector_trending_analysis` - 월간 섹터 분석
- `monthly_portfolio_rebalance` - 월별 리밸런싱

## Docker Containers: `actstock_[service]`
- `actstock_postgres`
- `actstock_kafka`
- `actstock_zookeeper`
- `actstock_spark_master`
- `actstock_spark_worker`
- `actstock_airflow_webserver`
- `actstock_airflow_scheduler`
- `actstock_api`
- `actstock_dashboard`

