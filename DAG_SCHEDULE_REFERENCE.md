# DAG Schedule Reference - Quick Lookup

## Active DAGs (5-Stage Pipeline)

| Time (UTC) | DAG Name | Schedule | Status | Purpose |
|------------|----------|----------|--------|---------|
| **09:00** | `daily_benchmark_etf_collection` | `0 9 * * 1-5` | ✅ Active | Collect 6 benchmark ETFs OHLC |
| **10:00** | `daily_sector_etf_collection` | `0 10 * * 1-5` | ✅ Active | Collect 11 sector ETFs OHLC |
| **11:00** | `daily_trending_etf_analysis` | `0 11 * * 1-5` | ✅ Active | Spark: Identify trending ETFs vs SPY |
| **12:00** | `daily_trending_etf_holdings_collection` | `0 12 * * 1-5` | ✅ Active | Collect holdings for trending ETFs only |
| **13:00** | `daily_portfolio_allocation` | `0 13 * * 1-5` | ✅ Active | Spark: Build portfolio (1 stock per ETF) |

## Disabled DAGs (Legacy)

| DAG Name | Old Schedule | Disabled | Reason |
|----------|--------------|----------|--------|
| `daily_benchmark_top_holdings_dag` | `0 11 * * 1-5` | ❌ Yes | Replaced by conditional holdings |
| `daily_sector_top_holdings_dag` | `0 12 * * 1-5` | ❌ Yes | Replaced by conditional holdings |
| `daily_stock_collection_dag` | `0 9,10,11,12 * * 1-5` | ❌ Yes | Integrated into conditional holdings |
| `stock_market_pipeline` | `0 * * * *` | ❌ Yes | Hourly collection no longer needed |

## Dependencies

```
09:00 → daily_benchmark_etf_collection
          ↓
10:00 → daily_sector_etf_collection
          ↓
11:00 → daily_trending_etf_analysis (requires 09:00 + 10:00 complete)
          ↓
12:00 → daily_trending_etf_holdings_collection (requires 11:00 complete)
          ↓
13:00 → daily_portfolio_allocation (requires 12:00 complete)
```

## Component Files

### Producers (Kafka)
- `kafka_producer_etf_daily.py` - Used by 09:00 and 10:00
- `kafka_producer_conditional_holdings.py` - Used by 12:00

### Consumers (Kafka)
- `kafka_consumer_etf_daily.py` - Stores to `collected_daily_etf_ohlc`
- `kafka_consumer_etf_holdings.py` - Stores to `collected_etf_holdings`
- `kafka_consumer_stock_daily.py` - Stores to `collected_daily_stock_history`

### Spark Jobs
- `spark_trending_etf_identifier.py` - Used by 11:00
- `spark_active_stock_allocator.py` - Used by 13:00

## Data Flow Summary

1. **09:00 + 10:00:** ETF OHLC → `collected_daily_etf_ohlc`
2. **11:00:** Spark reads ETF OHLC → `analytics_trending_etfs`
3. **12:00:** Kafka reads trending flags → `collected_etf_holdings` + `collected_daily_stock_history`
4. **13:00:** Spark reads holdings + stock data → `analytics_portfolio_allocation`

## Verification Commands

### Check DAG schedules in Airflow UI
```bash
# Open Airflow web UI
http://localhost:8080

# Filter by tags
- Tag: scheduled (should show 5 active DAGs)
- Tag: disabled (should show 4 disabled DAGs)
```

### Check DAG files directly
```bash
cd actstock_pipeline/airflow/dags

# Active DAGs
grep "schedule_interval='0 9" daily_benchmark_etf_collection_dag.py
grep "schedule_interval='0 10" daily_sector_etf_collection_dag.py
grep "schedule_interval='0 11" daily_trending_etf_analysis_dag.py
grep "schedule_interval='0 12" daily_trending_etf_holdings_collection_dag.py
grep "schedule_interval='0 13" daily_portfolio_allocation_dag.py

# Disabled DAGs
grep "schedule_interval=None" daily_benchmark_top_holdings_dag.py
grep "schedule_interval=None" daily_sector_top_holdings_dag.py
grep "schedule_interval=None" daily_stock_collection_dag.py
grep "schedule_interval=None" stock_market_pipeline.py
```

### Check database tables
```sql
-- Verify trending ETF results
SELECT COUNT(*), MAX(as_of_date) 
FROM analytics_trending_etfs 
WHERE is_trending = TRUE;

-- Verify portfolio
SELECT COUNT(*), SUM(weight), MAX(as_of_date) 
FROM analytics_portfolio_allocation;

-- Check holdings for trending ETFs only
SELECT COUNT(DISTINCT eh.etf_ticker) as trending_etf_count,
       COUNT(DISTINCT eh.ticker) as holding_count
FROM collected_etf_holdings eh
INNER JOIN analytics_trending_etfs te ON eh.etf_ticker = te.etf_ticker
WHERE te.as_of_date = CURRENT_DATE
  AND te.is_trending = TRUE;
```

## Rate Limit Monitor

### Check yfinance API usage
```bash
# View producer logs
docker logs actstock_pipeline-producer-etf-daily-1
docker logs actstock_pipeline-producer-conditional-holdings-1

# Count API calls by hour
grep "Fetching" /path/to/logs | grep "2024-01-XX" | cut -d' ' -f2 | cut -d: -f1 | sort | uniq -c
```

### Expected API call distribution
```
Hour 09: ~6 calls
Hour 10: ~11 calls
Hour 11: 0 calls (Spark only)
Hour 12: ~30-90 calls (varies by trending count)
Hour 13: 0 calls (Spark only)
```

## Troubleshooting

### DAG not showing in Airflow UI
```bash
# Check for syntax errors
cd actstock_pipeline/airflow/dags
python -m py_compile daily_*.py

# Check Airflow scheduler logs
docker logs actstock_pipeline-airflow-scheduler-1 --tail 100
```

### DAG stuck at "running"
```bash
# Check task logs in Airflow UI
# Or check container logs
docker logs actstock_pipeline-airflow-worker-1 --tail 100
```

### Spark job fails
```bash
# Check Spark logs
docker logs actstock_pipeline-spark-master-1 --tail 100

# Check database connectivity
docker exec -it actstock_pipeline-postgres-1 psql -U stock_user -d stock_db -c "SELECT 1;"
```

## Updates Made (2024-01-XX)

1. ✅ Updated `daily_portfolio_allocation_dag.py`:
   - Schedule: 18:15 → 13:00 UTC
   - Updated description and data dependency checks
   - Removed old sector/stock trending logic

2. ✅ Disabled old holdings DAGs:
   - `daily_benchmark_top_holdings_dag.py` → schedule_interval=None
   - `daily_sector_top_holdings_dag.py` → schedule_interval=None

3. ✅ Verified active DAGs:
   - `daily_benchmark_etf_collection` (09:00 UTC)
   - `daily_sector_etf_collection` (10:00 UTC)
   - `daily_trending_etf_analysis` (11:00 UTC)
   - `daily_trending_etf_holdings_collection` (12:00 UTC)
   - `daily_portfolio_allocation` (13:00 UTC)

4. ✅ Updated Spark job logic:
   - `spark_active_stock_allocator.py` → Top 1 performer per ETF
   - Weighting: performance × inverse market cap

## Next Steps

1. Deploy updates to Airflow:
   ```bash
   docker-compose restart airflow-webserver airflow-scheduler
   ```

2. Wait for next scheduled run (next weekday)

3. Monitor execution:
   - Check Airflow UI for DAG runs
   - Verify data in analytics tables
   - Monitor API rate limits

4. Validate results:
   - Trending ETF count reasonable (5-15)
   - Portfolio stock count = trending ETF count
   - Weights sum to 100%
