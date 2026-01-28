# 시스템 아키텍처

**트렌딩 기반 포트폴리오 자동 구성 시스템**

---

## 📐 아키텍처 다이어그램

### 전체 시스템 구조 (5-Stage Pipeline + Monthly Rebalance)

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           yfinance API                                     │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
              ┌──────────────────────────────────────────┐
              │           Controller DAG                 │
              │      Daily 21:30 UTC (Market Close)      │
              └───────────────┬──────────────────────────┘
                              │
                              │ [Sequential Trigger]
                              ▼
  ┌──────────┐ (1h delay) ┌──────────┐ (Immediate)  ┌──────────┐ (1h delay)
  │ Stage 1  │ ─────────► │ Stage 2  │ ──────────► │ Stage 3  │ ──────────► ...
  │ Benchmark│            │  Sector  │             │ Trending │
  └──────────┘            └──────────┘             └──────────┘
       │                    │                     │
   [DAG] daily_benchmark_etf_collection_dag.py    │
   [Producer] kafka_producer_etf_daily.py         │
   [Topic] etf-daily-data                         │
   [Consumer] kafka_consumer_etf_daily.py         │
       │                    │                     │
       │    [DAG] daily_sector_etf_collection_dag.py
       │    [Producer] kafka_producer_etf_daily.py
       │    [Topic] etf-daily-data              [DAG] daily_trending_etf_holdings_collection_dag.py
       │    [Consumer] kafka_consumer_etf_daily.py  [Producer] kafka_producer_trending_etf_holdings.py
       │                    │                     [Topic] etf-holdings-data, stock-daily-data
       │                    │                     [Consumer] kafka_consumer_etf_holdings.py
       │                    │                     [Consumer] kafka_consumer_stock_daily.py
       │                    │                     │
       └────────────────────┴─────────────────────┘
                            │
                            ▼
    ┌───────────────────────────────────────────────────────────────────┐
    │               PostgreSQL (Collected Layer)                        │
    │  • collected_daily_etf_ohlc          ← Stage 1, 2                 │
    │  • collected_etf_holdings            ← Stage 4 (Trending only)    │
    │  • collected_daily_stock_history     ← Stage 4 (Trending only)    │
    │  • collected_meta_etf                ← Static metadata            │
    └───────────────────────┬───────────────────────────────────────────┘
                            │
                 ┌──────────┴──────────┐
                 │                     │
                 ▼                     ▼
          ┌───────────┐         ┌───────────┐
          │  Stage 3  │         │  Stage 5  │
          │11:00 UTC  │         │13:00 UTC  │
          │ Trending  │         │ Portfolio │
          │   ETF     │         │(5d/10d/20d)│
          │ Analysis  │         │Allocation │
          └─────┬─────┘         └─────┬─────┘
                │                     │
       [DAG] daily_trending_etf_analysis_dag.py
       [Spark] spark_trending_etf_identifier.py   [DAG] daily_portfolio_allocation_dag.py
                │                     [Spark] spark_active_stock_allocator.py
                │                     │
                └─────────┬───────────┘
                          ▼
    ┌───────────────────────────────────────────────────────────────────┐
    │            PostgreSQL (Analytics Layer)                           │
    │  • analytics_trending_etfs           ← Stage 3 output             │
    │  • analytics_portfolio_allocation    ← Stage 5 output             │
    │    └─ period_days: 5, 10, 20         (3 portfolios per day)       │
    └───────────────────────┬───────────────────────────────────────────┘
                            │
                            ├──────────────────────┐
                            │                      │
                            ▼                      ▼
                    ┌───────────┐          ┌─────────────┐
                    │  FastAPI  │          │   Monthly   │
                    │Port: 8000 │          │  Rebalance  │
                    │Multi-period│         │(Last Sunday)│
                    └─────┬─────┘          │  14:00 UTC  │
                          │                └──────┬──────┘
                          │                       │
                          │         [DAG] monthly_portfolio_rebalance_dag.py
                          │         [Spark] spark_monthly_portfolio_rebalancer.py
                          │         [Logic] Compare 5d/10d/20d → Score → Merge
                          │                       │
                          │                       ▼
                          │         ┌────────────────────────────────────┐
                          │         │ analytics_monthly_portfolio        │
                          │         │ (Final portfolio for next 20 days) │
                          │         └────────────────┬───────────────────┘
                          │                          │
                          └──────────────────────────┘
                                     │
                                     ▼
                              ┌──────────────┐
                              │  Dashboard   │
                              │ Port: 8050   │
                              │Period Selector│
                              │5d/10d/20d/月 │
                              └──────────────┘
```

### 데이터 수집 전략

**핵심 원칙**: ETF 데이터는 **일일 5-Stage 스케줄 배치 수집만 사용** 

⚠️ **중요**: NO REAL-TIME/STREAMING COLLECTION
- yfinance API rate limit 회피를 위해 실시간 수집 절대 금지
- 모든 데이터는 Airflow 스케줄러에 의한 배치 수집만 사용
- Kafka는 배치 데이터 파이프라인 용도로만 사용 (스트리밍 아님)

**수집 방식**: **Airflow DAG → Kafka Producer → Kafka Topic → Consumer → PostgreSQL**
- 수집 이력 로그 보관
- 재처리 가능
- 모니터링 용이
- 스케줄: 월-금 09:00-13:00 (5 stages) + 매월 마지막 일요일 14:00 (rebalance)

### 5-Stage Pipeline 상세 (월-금 Daily)

```
09:00 UTC → Stage 1: 벤치마크 ETF OHLC (6개)
              ├─ SPY, QQQ, IWM, EWY, DIA, SCHD
              ├─ Note: QQQ는 Technology 섹터 대표로도 사용 (총 15 unique)
              └─ Kafka: Airflow → Producer → etf-daily-data → Consumer → PostgreSQL
              
10:00 UTC → Stage 2: 섹터 ETF OHLC (10개)
              ├─ QQQ, XLF, XLV, XLY, XLC, XLI, XLP, XLU, XLRE, XLB
              ├─ Technology(QQQ), Financial, Healthcare, Consumer Cyclical,
              │  Communication, Industrial, Consumer Defensive, Utilities,
              │  Real Estate, Basic Materials
              └─ Kafka: Airflow → Producer → etf-daily-data → Consumer → PostgreSQL
              
11:00 UTC → Stage 3: 트렌딩 ETF 식별 (Spark)
              ├─ Read: collected_daily_etf_ohlc
              ├─ Logic: return_20d > SPY AND > 0%
              └─ Write: analytics_trending_etfs
              
12:00 UTC → Stage 4: 조건부 Holdings 수집 (Kafka)
              ├─ Read: analytics_trending_etfs (trending만 선택)
              ├─ Collect: 트렌딩 ETF의 top 5 holdings만
              ├─ Write: collected_etf_holdings
              └─ Write: collected_daily_stock_history

13:00 UTC → Stage 5: 멀티기간 포트폴리오 배분 (Spark)
              ├─ Read: analytics_trending_etfs, collected_etf_holdings
              ├─ Logic: TOP 1 performer per ETF, Weight = Perf × (1/MCap)
              ├─ Periods: 5d, 10d, 20d (3개 독립 포트폴리오)
              └─ Write: analytics_portfolio_allocation
```

**📝 상세**: [SCHEDULING_STRATEGY.md](SCHEDULING_STRATEGY.md)

---

## 🔄 데이터 수집 계층 (Collected)

### 핵심 원칙

**5-Stage 스케줄 배치 파이프라인**
- Stage 1-2: ETF OHLC 수집 (09:00, 10:00)
- Stage 3: 트렌딩 ETF 분석 (11:00 Spark)
- Stage 4: 조건부 Holdings 수집 (12:00)
- Stage 5: 포트폴리오 배분 (13:00 Spark)

### 수집 방식

**스케줄 Kafka 파이프라인** (Stage 1, 2, 4)
```
Airflow DAG → Kafka Producer → Kafka Topic → Consumer → PostgreSQL
```
- **장점**: 
  - 수집 이력 로그 보관
  - 재처리 가능
  - 모니터링 용이
  - 데이터 파이프라인 표준 패턴
- **스케줄**: 월-금 5-stage workflow

**⚠️ 절대 금지**: 
- ❌ 실시간(real-time) 수집
- ❌ 스트리밍(streaming) 수집
- ❌ 시간별(hourly) 자동 수집
- ✅ Airflow 스케줄러에 의한 배치 수집만 사용 (yfinance rate limit 회피)

### Stage 1 & 2: ETF OHLC 수집 (09:00, 10:00 UTC)

#### Stage 1: 벤치마크 ETF (09:00 UTC)
**DAG**: `daily_benchmark_etf_collection_dag.py`
- **대상**: SPY, QQQ, IWM, EWY, DIA, SCHD (6개)
- **실행**: 월-금 09:00 UTC (주 5회)
- **방식**: Airflow → Kafka Producer (`kafka_producer_etf_daily.py`) → `etf-daily-data` → Consumer (`kafka_consumer_etf_daily.py`) → PostgreSQL
- **Rate Limit**: 5초 간격
- **저장**: `collected_daily_etf_ohlc`

#### Stage 2: 섹터 ETF (10:00 UTC)
**DAG**: `daily_sector_etf_collection_dag.py`
- **대상**: QQQ, XLV, XLF, XLY, XLC, XLI, XLP, XLU, XLRE, XLB (10개)
- **섹터**: Technology, Healthcare, Financial, Consumer Cyclical, Communication, Industrial, Consumer Defensive, Utilities, Real Estate, Basic Materials
- **실행**: 월-금 10:00 UTC (주 5회)
- **방식**: Airflow → Kafka Producer (`kafka_producer_etf_daily.py`) → `etf-daily-data` → Consumer (`kafka_consumer_etf_daily.py`) → PostgreSQL
- **Rate Limit**: 5초 간격
- **저장**: `collected_daily_etf_ohlc`
- **Note**: QQQ는 Stage 1 벤치마크에도 포함되어 총 15개 unique ETF

### Stage 3: 트렌딩 ETF 분석 (11:00 UTC)

**Spark Job**: `spark_trending_etf_identifier.py`
- **입력**: `collected_daily_etf_ohlc` (Stage 1, 2 결과)
- **로직**: 
  - 20일 수익률 계산
  - SPY 대비 outperformance
  - is_trending = (return_20d > spy_return_20d) AND (return_20d > 0)
- **출력**: `analytics_trending_etfs`
- **용도**: Stage 4에서 수집할 ETF 결정

### Stage 4: 조건부 Holdings 수집 (12:00 UTC)

**DAG**: `daily_trending_etf_holdings_collection_dag.py`
- **조건**: `analytics_trending_etfs`에서 `is_trending = TRUE`인 ETF만
- **수집**: 각 trending ETF의 top 5 holdings
- **방식**: Airflow → Kafka Producer (`kafka_producer_trending_etf_holdings.py`) → Topics: `etf-holdings-data`, `stock-daily-data` → Consumers → PostgreSQL
- **저장**: 
  - `collected_etf_holdings` (ETF-종목 매핑) via `kafka_consumer_etf_holdings.py`
  - `collected_daily_stock_history` (종목 OHLC) via `kafka_consumer_stock_daily.py`
- **효율성**: 전체 수집 대비 ~97% API 호출 감소

### Stage 5: 포트폴리오 배분 (13:00 UTC)

**Spark Job**: `spark_active_stock_allocator.py`
- **입력**: 
**📝 상세 스케줄**: [SCHEDULING_STRATEGY.md](SCHEDULING_STRATEGY.md)
**📊 DAG 참조**: [DAG_SCHEDULE_REFERENCE.md](DAG_SCHEDULE_REFERENCE.md)

---

## 🎯 분석 계층 (Analytics)

### 역할
Spark가 수집 데이터를 읽어 분석 결과를 생성
- Airflow가 Spark Job을 스케줄링
- Spark는 PostgreSQL에서 읽고 쓰기만 수행
- **데이터 수집은 하지 않음** (Kafka Producer가 담당)

### Spark Batch Jobs

#### 1. Trending ETF Identifier (매일 11:00 UTC - Stage 3)
**파일**: `batch/spark_trending_etf_identifier.py`  
**트리거**: `airflow/dags/daily_trending_etf_analysis_dag.py`

**기능**: SPY 대비 outperforming ETF 식별

**로직**:
```
1. Load ETF OHLC data (collected_daily_etf_ohlc)
   └─ Last 20 trading days

2. Calculate returns for each period (5d, 10d, 20d)
   ├─ SPY baseline return
   ├─ Each ETF return
   └─ Outperformance = ETF_return - SPY_return

3. Identify trending ETFs
   └─ Condition: outperformance > 0 AND is_trending = TRUE

4. Save results
   └─ analytics_trending_etfs (etf_ticker, return_Xd, spy_return_Xd, outperformance, is_trending)
```

#### 2. Portfolio Allocator (매일 13:00 UTC - Stage 5)
**파일**: `batch/spark_active_stock_allocator.py`  
**트리거**: `airflow/dags/daily_portfolio_allocation_dag.py`

**기능**: 멀티 기간 포트폴리오 생성 (5일/10일/20일)

**로직**:
```
1. Load data
   ├─ analytics_trending_etfs (Stage 3 output)
   ├─ collected_etf_holdings (Stage 4 output)
   └─ collected_daily_stock_history (Stage 4 output)

2. For each period (5d, 10d, 20d):
   ├─ Filter trending ETFs for this period
   ├─ Calculate stock returns
   ├─ Select TOP 1 best performer per trending ETF
   ├─ Weight = Performance × (1 / Market Cap)
   │   └─ Normalize to sum = 1.0 (100%)
   └─ Assign allocation_reason

3. Save results
   └─ analytics_portfolio_allocation (3 portfolios with period_days: 5, 10, 20)
```

#### 3. Monthly Portfolio Rebalancer (매월 마지막 일요일 14:00 UTC)
**파일**: `batch/spark_monthly_portfolio_rebalancer.py`  
**트리거**: `airflow/dags/monthly_portfolio_rebalance_dag.py`

**기능**: 5일/10일/20일 포트폴리오 통합 → 최종 월간 포트폴리오

**로직**:
```
1. Load multi-period portfolios
   └─ analytics_portfolio_allocation (period_days: 5, 10, 20)

2. Compare and score
   ├─ 20일 base: score = 1.0
   ├─ 20일 ∩ 10일: score +1.0
   ├─ 20일 ∩ 5일: score +1.0
   ├─ 10일만 (20일 없음): score = 0.5, rank 20부터
   ├─ 10일 ∩ 5일만 (20일 없음): score = 2.0 (재차 가중)
   └─ 5일만 (10일, 20일 없음): score = 0.3, rank 20부터

3. Rank assignment
   ├─ Sort by score DESC
   ├─ Existing stocks (in 20d): higher ranks
   └─ New stocks: rank 20부터 배치 (2개 이상이면 score 순)

4. Weight calculation
   ├─ Weight = score / sum(scores)
   └─ Normalize to sum = 1.0 (100%)

5. Save final monthly portfolio
   └─ analytics_monthly_portfolio (rebalance_date, valid_until, final_rank, final_weight, score, source_periods)
```
- 결과: `analytics_sector_trending`

---

## 🗄️ 데이터베이스 스키마

⚠️ **NO REAL-TIME COLLECTION** - Airflow 스케줄 배치만 사용

### Collected (수집 계층 - Kafka Consumer가 저장, Airflow 스케줄 기반)

```sql
-- 일별 주식 히스토리 (Stage 4, 월-금 12:00 UTC)
collected_daily_stock_history (
    ticker VARCHAR,
    trade_date DATE,
    close_price NUMERIC,
    sector VARCHAR,
    market_cap BIGINT,
    UNIQUE(ticker, trade_date)
)

-- ETF 메타데이터 + 보유종목 (etf_holdings_daily_dag, 월-금 09:00 UTC)
collected_meta_etf (
    ticker VARCHAR PRIMARY KEY,
    etf_type VARCHAR,
    sector_name VARCHAR,
    description TEXT
)

-- 일별 ETF OHLC
collected_daily_etf_ohlc (
    ticker VARCHAR,
    trade_date DATE,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    volume BIGINT,
    price_change_percent NUMERIC,
    UNIQUE(ticker, trade_date)
)

-- ETF 보유종목
collected_etf_holdings (
    etf_ticker VARCHAR,
    stock_ticker VARCHAR,
    stock_name VARCHAR,
    holding_percent NUMERIC,
    shares BIGINT,
    market_value BIGINT,
    collected_date DATE,
    UNIQUE(etf_ticker, stock_ticker, collected_date)
)

-- 벤치마크 보유종목
collected_monthly_benchmark_holdings (
    etf_ticker VARCHAR,
    holding_ticker VARCHAR,
    holding_name VARCHAR,
    holding_percent NUMERIC,
    as_of_date DATE,
    UNIQUE(etf_ticker, holding_ticker, as_of_date)
)
```

### Analytics (분석 계층 - Spark가 계산)

```sql
-- 포트폴리오 배분 (트렌딩 기반)
analytics_portfolio_allocation (
    as_of_date DATE,
    ticker VARCHAR,
    company_name VARCHAR,
    sector VARCHAR,
    market_cap BIGINT,
    -- 5일 기준
    return_5d NUMERIC,
    sector_avg_5d NUMERIC,
    is_trending_5d BOOLEAN,
    rank_5d INT,
    -- 10일 기준
    return_10d NUMERIC,
    sector_avg_10d NUMERIC,
    is_trending_10d BOOLEAN,
    rank_10d INT,
    -- 20일 기준 (포트폴리오 구성)
    return_20d NUMERIC,
    sector_avg_20d NUMERIC,
    is_trending_20d BOOLEAN,
    rank_20d INT,
    -- 포트폴리오 비중
    portfolio_weight NUMERIC,
    allocation_reason VARCHAR,
    UNIQUE(as_of_date, ticker)
)

-- ETF Top Holdings
analytics_etf_top_holdings (
    etf_ticker VARCHAR,
    stock_ticker VARCHAR,
    stock_name VARCHAR,
    period_days INT,           -- 5, 10, 20
    rank_position INT,         -- 1-5
    avg_return NUMERIC,
    total_return NUMERIC,
    volatility NUMERIC,
    sharpe_ratio NUMERIC,
    analysis_date DATE,
    UNIQUE(etf_ticker, stock_ticker, period_days, analysis_date)
)

-- 트렌딩 ETF (Stage 3 output)
analytics_trending_etfs (
    as_of_date DATE,
    etf_ticker VARCHAR,
    return_20d NUMERIC,
    spy_return_20d NUMERIC,
    outperformance NUMERIC,
    rank_by_outperformance INT,
    is_trending BOOLEAN,
    sector_name VARCHAR,
    UNIQUE(as_of_date, etf_ticker)
)

-- 포트폴리오 배분 (Stage 5 output - 멀티기간)
analytics_portfolio_allocation (
    as_of_date DATE,
    ticker VARCHAR,
    period_days INT,           -- 5, 10, 20 (멀티기간 지원)
    portfolio_weight NUMERIC,
    return_20d NUMERIC,
    market_cap BIGINT,
    allocation_reason VARCHAR,
    rank_20d INT,
    UNIQUE(as_of_date, ticker, period_days)
)
```

### Logs (모니터링 계층)

```sql
-- Consumer 에러 로그
logs_consumer_error (
    sector VARCHAR,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    avg_price NUMERIC,
    total_volume BIGINT,
    min_price NUMERIC,
    max_price NUMERIC,
    record_count INT,
    UNIQUE(symbol, window_start, window_end)
)
```

### Logs (로그 계층)

```sql
-- Consumer 에러 로그
logs_consumer_error (
    raw_value TEXT,
    error_msg TEXT,
    received_at TIMESTAMPTZ
)
```

---

## 🔌 서비스 포트

| 서비스 | 포트 | 용도 |
|--------|------|------|
| PostgreSQL | 5432 | 메인 데이터베이스 |
| Kafka | 9092 | 메시지 브로커 |
| Zookeeper | 2181 | Kafka 코디네이션 |
| Spark Master | 7077, 8081 | Spark 클러스터 마스터 |
| Spark Worker | 8082 | Spark 워커 |
| Airflow Webserver | 8080 | Airflow UI |
| Airflow DB | 5433 | Airflow 메타데이터 |
| Dashboard | 8050 | Plotly Dash UI |
| API | 8000 | FastAPI 엔드포인트 |

---

## 📦 Docker 서비스 구성

```yaml
services:
  # 인프라
  postgres        # 메인 데이터베이스
  kafka           # 메시지 브로커
  zookeeper       # Kafka 코디네이션
  
  # Spark 클러스터
  spark-master    # Spark 마스터
  spark-worker    # Spark 워커
  
  # Airflow (워크플로우 스케줄링)
  airflow-db        # Airflow 메타데이터
  airflow-webserver # Airflow UI
  airflow-scheduler # DAG 스케줄러
  
  # 데이터 파이프라인
  collector       # Kafka Producer (실시간 수집)
  consumer        # Kafka Consumer (DB 저장)
  
  # 사용자 인터페이스
  dashboard       # Plotly Dash (시각화)
  api             # FastAPI (백엔드)
```

---

## 🔐 환경 변수

```bash
# PostgreSQL
DB_HOST=postgres
DB_PORT=5432
DB_NAME=stockdb
DB_USER=postgres
DB_PASSWORD=postgres

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Spark
SPARK_MASTER_URL=spark://spark-master:7077

# API
API_KEY=your_api_key_here
```

---

## 📈 확장성 고려사항

### 수평 확장
- **Kafka**: 파티션 증가로 처리량 향상
- **Spark**: 워커 추가로 병렬 처리 증대
- **Consumer**: 컨슈머 그룹 확장

### 성능 최적화
- **Spark**: 셔플 파티션 수 조정 (`spark.sql.shuffle.partitions`)
- **Kafka**: 배치 크기 및 압축 설정
- **PostgreSQL**: 인덱스 최적화, 파티셔닝

### 모니터링
- **Spark UI** (http://localhost:8081): Job 실행 상태
- **Airflow UI** (http://localhost:8080): DAG 스케줄링
- **Dashboard** (http://localhost:8050): 비즈니스 메트릭

---

## 📁 프로젝트 구조

```
actstock_pipeline/
├── airflow/              # Airflow DAG 정의
│   ├── dags/
│   │   ├── daily_benchmark_etf_collection_dag.py           (Stage 1)
│   │   ├── daily_sector_etf_collection_dag.py              (Stage 2)
│   │   ├── daily_trending_etf_analysis_dag.py              (Stage 3)
│   │   ├── daily_trending_etf_holdings_collection_dag.py   (Stage 4)
│   │   ├── daily_portfolio_allocation_dag.py               (Stage 5)
│   │   ├── monthly_portfolio_rebalance_dag.py              (Monthly)
│   │   ├── etf_top_holdings_analysis_dag.py                (추가 분석)
│   │   ├── etf_holdings_daily_dag.py                       (ETF 보유종목 수집)
│   │   └── monthly_etf_collection_last_weekend_dag.py      (월말 ETF 수집)
│   └── logs/
├── api/                  # FastAPI 백엔드
│   ├── routes/
│   │   ├── stocks.py                 # /stocks/portfolio, /stocks/monthly-portfolio
│   │   ├── sectors.py                # /sectors/* endpoints
│   │   ├── dashboard.py              # /dashboard/* endpoints
│   │   └── etf_holdings.py           # /etf-holdings/* endpoints
│   ├── models/
│   │   └── schemas.py
│   ├── tests/
│   │   └── test_main.py
│   ├── auth.py
│   ├── main.py
│   └── __init__.py
├── batch/                # Spark 배치 Jobs
│   ├── spark_trending_etf_identifier.py       (Stage 3)
│   ├── spark_active_stock_allocator.py        (Stage 5)
│   ├── spark_monthly_portfolio_rebalancer.py  (Monthly)
│   ├── spark_etf_top_holdings.py              (추가 분석)
│   └── batch_monthly_sector_analyzer.py       (섹터 트렌딩 분석)
├── collector/            # Kafka Producers
│   ├── kafka_producer_etf_daily.py                 (Stage 1, 2)
│   ├── kafka_producer_trending_etf_holdings.py     (Stage 4)
│   ├── kafka_producer_etf_holdings.py              (보유종목 수집)
│   └── kafka_producer_stock_daily.py               (주식 OHLC 수집)
├── consumer/             # Kafka Consumers
│   ├── kafka_consumer_etf_daily.py
│   ├── kafka_consumer_etf_holdings.py
│   └── kafka_consumer_stock_daily.py
├── config/               # 설정 파일
│   └── config.py
├── dashboard/            # Plotly Dash 대시보드
│   └── dashboard_finviz_app.py
├── database/             # 스키마 및 헬퍼
│   ├── schema.sql
│   ├── db_helper.py
│   └── NAMING_CONVENTION.md
├── utils/                # 공통 유틸리티
│   ├── kafka_utils.py
│   └── logging_utils.py
├── logs/                 # Runtime logs (생성됨)
├── data/                 # Data directory (생성됨)
├── checkpoints/          # Spark checkpoints (생성됨)
├── test/                 # 테스트 및 백필 스크립트
│   ├── unit_tests/
│   │   └── test_spark_jobs.py
│   ├── 1_backfill_etf_benchmarks.py
│   ├── 2_staggered_sector_backfill.py
│   └── README.md
├── docker-compose.yml    # 서비스 정의 (14 컨테이너)
├── Dockerfile            # 컨테이너 이미지
├── Dockerfile.airflow    # Airflow 전용 이미지
├── Dockerfile.api        # API 전용 이미지
├── start.sh/start.bat    # 시스템 시작 스크립트
├── deploy_staggered_dags.bat  # DAG 배포 스크립트
├── setup_etf_holdings.bat     # ETF Holdings 설정
├── requirements.txt      # Python 의존성
├── requirements-airflow.txt  # Airflow 의존성
└── README.md             # 프로젝트 개요
```

---

## 🔗 관련 문서

### 시스템 아키텍처
- **[README.md](README.md)**: 빠른 시작 가이드 및 시스템 개요
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: 전체 시스템 아키텍처 (현재 문서)
- **[SCHEDULING_STRATEGY.md](SCHEDULING_STRATEGY.md)**: 5-Stage Pipeline 스케줄 전략
- **[DAG_SCHEDULE_REFERENCE.md](DAG_SCHEDULE_REFERENCE.md)**: Active/Disabled DAG 빠른 참조
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)**: 자주 사용하는 명령어

### 최신 기능
- **[MULTI_PERIOD_IMPLEMENTATION.md](MULTI_PERIOD_IMPLEMENTATION.md)**: 멀티기간 포트폴리오 구현 상세
- **[TESTING_MULTI_PERIOD.md](TESTING_MULTI_PERIOD.md)**: 멀티기간 기능 테스트 가이드
- **[CLEANUP_SUMMARY.md](CLEANUP_SUMMARY.md)**: 문서/DB 정리 요약

### 데이터베이스
- **[database/schema.sql](database/schema.sql)**: PostgreSQL 스키마 정의
- **[database/NAMING_CONVENTION.md](database/NAMING_CONVENTION.md)**: 테이블 명명 규칙

---

**마지막 업데이트**: 2026-01-27  
**버전**: 5-Stage Pipeline + Monthly Rebalance  
**명명 규칙**: 데이터 플로우 기반 (collected/analytics/logs)

## 🐳 Docker 컨테이너 구성

### Infrastructure

| 컨테이너 | 역할 | 포트 |
|----------|------|------|
| `postgres` | PostgreSQL 데이터베이스 | 5432 |
| `zookeeper` | Kafka 코디네이션 | 2181 |
| `kafka` | 메시지 브로커 | 9092 |

### Spark Cluster

| 컨테이너 | 역할 | 포트 |
|----------|------|------|
| `spark-master` | Spark 마스터 노드 | 8081, 7077 |
| `spark-worker` | Spark 워커 노드 | 8082 |

### Airflow

| 컨테이너 | 역할 | 포트 |
|----------|------|------|
| `airflow-webserver` | Airflow UI | 8080 |
| `airflow-scheduler` | DAG 스케줄러 | - |

### Application

| 컨테이너 | 역할 | 포트 |
|----------|------|------|
| `api` | FastAPI 백엔드 | 8000 |
| `dashboard` | Plotly Dash 대시보드 | 8050 |

### Data Pipeline (Kafka Consumers)

| 컨테이너 | 역할 | 토픽 | 테이블 |
|----------|------|------|--------|
| `consumer-etf-daily` | ETF OHLC 데이터 저장 | `etf-daily-data` | `collected_daily_etf_ohlc` |
| `consumer-etf-holdings` | ETF Holdings 저장 | `etf-holdings-data` | `collected_etf_holdings` |
| `consumer-stock-daily` | 주식 OHLC 데이터 저장 | `stock-daily-data` | `collected_daily_stock_history` |
