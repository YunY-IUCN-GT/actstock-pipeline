# ActStock Pipeline - Workflow Execution Order (Controller Version)

This document explains the numbered workflow files and their execution sequence.

## 📋 Numbering Convention

All workflow files are numbered with a 2-digit prefix (00, 01, 02, etc.) to indicate logical flow.
**Note**: With the introduction of the Controller DAG, **00** is the master trigger for **01-05**.

---

## 🔄 Daily Workflow (Weekdays - Monday to Friday)

### Master Controller (Daily 21:30 UTC / 06:30 KST+1)

**00. Pipeline Controller** - `21:30 UTC`
- **DAG**: `00_daily_pipeline_controller_dag.py`
- **Purpose**: Master orchestrator that triggers Stages 1-5 sequentially.
- **Schedule**: `30 21 * * 1-5` (The ONLY scheduled daily DAG)
- **Logic**: Enforces delays (e.g., 1 hour) between stages to prevent API rate limits.

---

### Stage 1: Data Collection (Triggered by Controller)

**01. Benchmark ETF Collection**
- **DAG**: `01_daily_benchmark_etf_collection_dag.py` (Triggered)
- **Producer**: `kafka_01_producer_etf_daily.py`
- **Purpose**: Collect 6 benchmark ETFs (SPY, QQQ, IWM, EWY, DIA, SCHD)
- **Output**: `collected_daily_etf_ohlc`

**02. Sector ETF Collection**
- **DAG**: `02_daily_sector_etf_collection_dag.py` (Triggered +1h after Stage 1)
- **Producer**: `kafka_01_producer_etf_daily.py`
- **Purpose**: Collect 10 sector ETFs
- **Output**: `collected_daily_etf_ohlc`

---

### Stage 2: Trending Analysis (Triggered by Controller)

**03. Trending ETF Identification**
- **DAG**: `03_daily_trending_etf_analysis_dag.py` (Triggered immediate after Stage 2)
- **Spark Job**: `spark_01_trending_etf_identifier.py`
- **Purpose**: Identify ETFs outperforming SPY
- **Output**: `analytics_trending_etfs`

---

### Stage 3: Holdings Collection (Triggered by Controller)

**04. Trending ETF Holdings Collection**
- **DAG**: `04_daily_trending_etf_holdings_collection_dag.py` (Triggered +1h after Stage 3)
- **Producer**: `kafka_03_producer_trending_etf_holdings.py`
- **Purpose**: Collect holdings ONLY for trending ETFs
- **Output**: `collected_etf_holdings`

---

### Stage 4: Portfolio Allocation (Triggered by Controller)

**05. Daily Portfolio Allocation**
- **DAG**: `05_daily_portfolio_allocation_dag.py` (Triggered immediate after Stage 4)
- **Spark Job**: `spark_02_active_stock_allocator.py`
- **Purpose**: Generate multi-period portfolios (5/10/20-day)
- **Output**: `analytics_portfolio_allocation`

---

### Stage 5: ETF Holdings Stock Price Collection (18:00 UTC)

**06. ETF Holdings Daily Collection** - `18:00 UTC`
- **DAG**: `06_etf_holdings_daily_dag.py` (Still independent or can be added to controller)
- **Note**: Currently maintained as independent schedule for broader collection.
- **Output**: `collected_daily_stock_history`

---

## 📅 Weekly Workflow (Sundays Only) - Unchanged

### Stage 1: Monthly ETF Collection (08:00 UTC)
**07. Monthly ETF Collection (Last Weekend)**

### Stage 2: Monthly Portfolio Rebalancing (14:00 UTC)
**08. Monthly Portfolio Rebalance**

### Stage 3: Top Holdings Analysis (18:00 UTC)
**09. ETF Top Holdings Analysis**

---

## 📊 Summary

### Active Controller
```
daily_pipeline_controller (00)
  ├── Triggers 01 (Benchmark)
  ├── Waits 1 hour
  ├── Triggers 02 (Sector)
  ├── Triggers 03 (Analysis)
  ├── Waits 1 hour
  ├── Triggers 04 (Holdings)
  └── Triggers 05 (Portfolio)
```

### Manual Execution
To run the pipeline manually, **only trigger DAG 00 (`daily_pipeline_controller`)**.
