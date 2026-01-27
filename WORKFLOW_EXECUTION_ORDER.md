# ActStock Pipeline - Workflow Execution Order

This document explains the numbered workflow files and their execution sequence.

## 📋 Numbering Convention

All workflow files are numbered with a 2-digit prefix (01, 02, 03, etc.) to indicate:
- **Execution order** within the pipeline
- **Dependencies** between components
- **Logical flow** of data processing

---

## 🔄 Daily Workflow (Weekdays - Monday to Friday)

### Stage 1: Data Collection (09:00-10:00 UTC / 18:00-19:00 KST)

**01. Benchmark ETF Collection** - `09:00 UTC`
- **DAG**: `01_daily_benchmark_etf_collection_dag.py`
- **Producer**: `kafka_01_producer_etf_daily.py`
- **Consumer**: `kafka_01_consumer_etf_daily.py`
- **Purpose**: Collect 6 benchmark ETFs (SPY, QQQ, IWM, EWY, DIA, SCHD)
- **Output**: `collected_daily_etf_ohlc` table

**02. Sector ETF Collection** - `10:00 UTC`
- **DAG**: `02_daily_sector_etf_collection_dag.py`
- **Producer**: `kafka_01_producer_etf_daily.py` (reused)
- **Consumer**: `kafka_01_consumer_etf_daily.py` (reused)
- **Purpose**: Collect 10 sector ETFs (QQQ, XLF, XLV, XLY, XLC, XLI, XLP, XLU, XLRE, XLB)
- **Output**: `collected_daily_etf_ohlc` table

---

### Stage 2: Trending Analysis (11:00 UTC / 20:00 KST)

**03. Trending ETF Identification** - `11:00 UTC`
- **DAG**: `03_daily_trending_etf_analysis_dag.py`
- **Spark Job**: `spark_01_trending_etf_identifier.py`
- **Purpose**: Identify ETFs outperforming SPY benchmark
- **Algorithm**: Compare 20-day returns vs SPY
- **Output**: `analytics_trending_etfs` table

---

### Stage 3: Holdings Collection (12:00 UTC / 21:00 KST)

**04. Trending ETF Holdings Collection** - `12:00 UTC`
- **DAG**: `04_daily_trending_etf_holdings_collection_dag.py`
- **Producer**: `kafka_03_producer_trending_etf_holdings.py`
- **Consumer**: `kafka_02_consumer_etf_holdings.py`
- **Purpose**: Collect holdings ONLY for trending ETFs (conditional)
- **Input**: Reads from `analytics_trending_etfs` (Stage 2 output)
- **Output**: `collected_etf_holdings` table

---

### Stage 4: Portfolio Allocation (13:00 UTC / 22:00 KST)

**05. Daily Portfolio Allocation** - `13:00 UTC`
- **DAG**: `05_daily_portfolio_allocation_dag.py`
- **Spark Job**: `spark_02_active_stock_allocator.py`
- **Purpose**: Generate multi-period portfolios (5/10/20-day)
- **Algorithm**: 
  - Extract top holdings from trending ETFs
  - Calculate stock weights across portfolios
  - Generate separate allocations for each period
- **Input**: 
  - `analytics_trending_etfs` (from Stage 2)
  - `collected_etf_holdings` (from Stage 3)
- **Output**: 
  - `analytics_portfolio_5d`
  - `analytics_portfolio_10d`
  - `analytics_portfolio_20d`

---

### Stage 5: ETF Holdings Stock Price Collection (18:00 UTC / 03:00 KST+1)

**06. ETF Holdings Daily Collection** - `18:00 UTC`
- **DAG**: `06_etf_holdings_daily_dag.py`
- **Producer**: `kafka_04_producer_stock_daily.py`
- **Consumer**: `kafka_03_consumer_stock_daily.py`
- **Purpose**: Collect daily close prices for all stocks in ETF holdings
- **Input**: Reads stock symbols from `collected_etf_holdings`
- **Output**: `collected_daily_stock_history` table

---

## 📅 Weekly Workflow (Sundays Only)

### Stage 1: Monthly ETF Collection (08:00 UTC / 17:00 KST)

**07. Monthly ETF Collection (Last Weekend)** - `08:00 UTC`
- **DAG**: `07_monthly_etf_collection_last_weekend_dag.py`
- **Producer**: `kafka_02_producer_etf_holdings.py`
- **Consumer**: `kafka_02_consumer_etf_holdings.py`
- **Purpose**: Collect all 15 ETF holdings on last Sunday of month
- **Trigger**: Runs every Sunday, but checks if it's the last Sunday
- **Output**: `collected_etf_holdings` table (full refresh)

---

### Stage 2: Monthly Portfolio Rebalancing (14:00 UTC / 23:00 KST)

**08. Monthly Portfolio Rebalance** - `14:00 UTC`
- **DAG**: `08_monthly_portfolio_rebalance_dag.py`
- **Spark Job**: `spark_04_monthly_portfolio_rebalancer.py`
- **Purpose**: Consolidate 5d/10d/20d portfolios into final monthly portfolio
- **Trigger**: Runs every Sunday, but only executes on last Sunday
- **Algorithm**:
  - Merge stocks from all three periods
  - Calculate unified weights
  - Generate single portfolio for next 20 business days
- **Input**:
  - `analytics_portfolio_5d`
  - `analytics_portfolio_10d`
  - `analytics_portfolio_20d`
- **Output**: `analytics_monthly_portfolio` table

---

### Stage 3: Top Holdings Analysis (18:00 UTC / 03:00 KST+1)

**09. ETF Top Holdings Analysis** - `18:00 UTC`
- **DAG**: `09_etf_top_holdings_analysis_dag.py`
- **Spark Job**: `spark_03_etf_top_holdings.py`
- **Purpose**: Analyze and extract top holdings from all ETFs
- **Algorithm**:
  - Identify top N holdings per ETF
  - Calculate concentration metrics
  - Detect overlapping holdings across ETFs
- **Input**: `collected_etf_holdings` (from Stage 1)
- **Output**: `analytics_etf_top_holdings` table

---

## 📊 Component Summary

### Airflow DAGs (9 total)
```
01_daily_benchmark_etf_collection_dag.py      - Weekdays 09:00 UTC
02_daily_sector_etf_collection_dag.py         - Weekdays 10:00 UTC
03_daily_trending_etf_analysis_dag.py         - Weekdays 11:00 UTC
04_daily_trending_etf_holdings_collection_dag.py - Weekdays 12:00 UTC
05_daily_portfolio_allocation_dag.py          - Weekdays 13:00 UTC
06_etf_holdings_daily_dag.py                  - Weekdays 18:00 UTC
07_monthly_etf_collection_last_weekend_dag.py - Sundays 08:00 UTC
08_monthly_portfolio_rebalance_dag.py         - Sundays 14:00 UTC (last Sunday only)
09_etf_top_holdings_analysis_dag.py           - Sundays 18:00 UTC
```

### Kafka Producers (4 total)
```
kafka_01_producer_etf_daily.py               - ETF OHLC data (benchmarks + sectors)
kafka_02_producer_etf_holdings.py            - ETF holdings (monthly full collection)
kafka_03_producer_trending_etf_holdings.py   - ETF holdings (daily conditional)
kafka_04_producer_stock_daily.py             - Stock daily prices
```

### Kafka Consumers (3 total)
```
kafka_01_consumer_etf_daily.py    - Stores ETF OHLC → collected_daily_etf_ohlc
kafka_02_consumer_etf_holdings.py - Stores ETF holdings → collected_etf_holdings
kafka_03_consumer_stock_daily.py  - Stores stock prices → collected_daily_stock_history
```

### Spark Batch Jobs (4 total)
```
spark_01_trending_etf_identifier.py      - Identify trending ETFs
spark_02_active_stock_allocator.py       - Generate multi-period portfolios
spark_03_etf_top_holdings.py             - Analyze top holdings
spark_04_monthly_portfolio_rebalancer.py - Consolidate monthly portfolio
```

---

## 🔗 Data Flow Dependencies

```
Stage 1-2: ETF Collection (09:00-10:00)
    ↓
    collected_daily_etf_ohlc (table)
    ↓
Stage 3: Trending Analysis (11:00) [Spark 01]
    ↓
    analytics_trending_etfs (table)
    ↓
Stage 4: Conditional Holdings (12:00)
    ↓
    collected_etf_holdings (table)
    ↓
Stage 5: Portfolio Allocation (13:00) [Spark 02]
    ↓
    analytics_portfolio_{5d,10d,20d} (tables)
    ↓
Stage 6: Stock Prices (18:00)
    ↓
    collected_daily_stock_history (table)

---

Weekly (Sundays):
    Stage 1: Monthly ETF Holdings (08:00)
        ↓
        collected_etf_holdings (table - full refresh)
        ↓
    Stage 2: Monthly Rebalance (14:00) [Spark 04]
        ↓
        analytics_monthly_portfolio (table)
        ↓
    Stage 3: Top Holdings Analysis (18:00) [Spark 03]
        ↓
        analytics_etf_top_holdings (table)
```

---

## 🎯 Key Design Principles

1. **Sequential Execution**: Each stage depends on the previous stage's output
2. **Conditional Logic**: Holdings collection only for trending ETFs (saves API calls)
3. **Time-Based Separation**: Sufficient gaps between stages for processing
4. **Weekend vs Weekday**: Different workflows for daily operations vs monthly maintenance
5. **Multi-Period Strategy**: Generate portfolios for 5, 10, and 20-day periods
6. **Monthly Consolidation**: Unified monthly portfolio from multi-period analysis

---

## 🚀 Running the Pipeline

### Start All Services
```bash
docker compose up -d
```

### Check DAG Status
```bash
# Access Airflow UI
http://localhost:8080
# Username: admin
# Password: admin
```

### Monitor Workflow Execution
```bash
# View all container logs
docker compose logs -f

# View specific service
docker compose logs -f airflow-scheduler
docker compose logs -f consumer-etf-daily
```

### Test System Health
```bash
cd test
bash test.sh
```

---

## 📝 Notes

- **Timezone**: All times in UTC (add 9 hours for KST)
- **Weekday Filter**: Most DAGs run Monday-Friday only (`1-5` in cron)
- **Sunday Special**: Monthly operations run on Sundays (`0` in cron)
- **Last Sunday Check**: Monthly rebalance includes runtime validation
- **Catchup**: All DAGs have `catchup=False` to prevent backfilling

---

## 🔧 Maintenance

If you need to rerun a specific stage:
1. Go to Airflow UI (http://localhost:8080)
2. Find the DAG (01-09)
3. Click "Trigger DAG" for manual execution
4. Monitor in "Graph View" or "Log" tab

For emergency stop:
```bash
docker compose stop
```

For complete restart:
```bash
docker compose down
docker compose up -d --build
```
