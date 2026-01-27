# Multi-Period Portfolio Testing Guide

## Changes Summary

### 1. API Updates ✅
**File**: `api/routes/stocks.py`
- Added `/stocks/portfolio` endpoint
- Supports `period_days` parameter (5, 10, or 20)
- Optional `as_of_date` parameter
- Returns portfolio allocation with weight, returns, market cap, etc.

**File**: `api/routes/dashboard.py`
- Updated `/dashboard/active-allocations` endpoint
- Added `period_days` parameter (5, 10, or 20)
- Added `as_of_date` parameter

### 2. Dashboard Updates ✅
**File**: `dashboard/dashboard_finviz_app.py`
- Added period selector dropdown (5일, 10일, 20일)
- Updated portfolio allocation callback to accept period input
- Dynamic column headers based on selected period
- Shows period information in summary bar

### 3. Containers Restarted ✅
- API container: actstock-api (running, healthy)
- Dashboard container: actstock-dashboard (running)

## Testing Steps

### 1. Access Dashboard
Open browser: http://localhost:8050

**Expected**:
- New dropdown selector: "📊 포트폴리오 분석 기간"
- Options: 5일 단기, 10일 중기, 20일 장기 (default: 20일)

### 2. Test Period Selector
**Steps**:
1. Select "5일 단기 (빠른 반응)"
2. Wait for table to refresh
3. Check column header changes to "5일 수익률"
4. Verify summary bar shows "분석기간: 5일"

**Expected Results**:
- Table updates automatically
- Different stocks may appear (based on 5-day performance)
- Summary shows correct period

### 3. Switch Between Periods
**Steps**:
1. Select "10일 중기 (균형)"
2. Select "20일 장기 (안정성)"
3. Compare results

**Expected**:
- Each period shows different portfolio allocations
- Short-term (5d) may have more volatile stocks
- Long-term (20d) may have more stable allocations

### 4. Verify API Responses

#### Test 5-Day Portfolio
```bash
curl -H "X-API-Key: dev-secret-key-12345" \
  "http://localhost:8000/stocks/portfolio?period_days=5"
```

#### Test 10-Day Portfolio
```bash
curl -H "X-API-Key: dev-secret-key-12345" \
  "http://localhost:8000/stocks/portfolio?period_days=10"
```

#### Test 20-Day Portfolio
```bash
curl -H "X-API-Key: dev-secret-key-12345" \
  "http://localhost:8000/stocks/portfolio?period_days=20"
```

**Expected Response**:
```json
[
  {
    "as_of_date": "2026-01-26",
    "ticker": "AAPL",
    "company_name": "Apple Inc.",
    "sector": "Technology",
    "market_cap": 3000000000000,
    "return_pct": 5.23,
    "weight": 12.5,
    "allocation_reason": "top1_perf_per_etf_5d_score_perf_x_inv_mcap",
    "rank": 1,
    "period_days": 5,
    "created_at": "2026-01-26T13:15:00"
  },
  ...
]
```

### 5. Test Dashboard API Endpoint

```bash
curl -H "X-API-Key: dev-secret-key-12345" \
  "http://localhost:8000/dashboard/active-allocations?period_days=10"
```

## Troubleshooting

### Issue: Empty Portfolio Data
**Symptoms**: Dashboard shows "⏳ 포트폴리오 배분 대기 중..."

**Solutions**:
1. Check if Spark job has run today (13:00 UTC)
2. Verify database has data:
   ```sql
   SELECT COUNT(*), period_days 
   FROM analytics_portfolio_allocation 
   GROUP BY period_days;
   ```
3. Check latest date:
   ```sql
   SELECT MAX(as_of_date), period_days 
   FROM analytics_portfolio_allocation 
   GROUP BY period_days;
   ```

### Issue: Wrong Period Shown
**Symptoms**: Selecting 5일 but seeing 20일 data

**Solutions**:
1. Clear browser cache
2. Check browser console for errors
3. Verify API response:
   ```bash
   curl -H "X-API-Key: dev-secret-key-12345" \
     "http://localhost:8000/stocks/portfolio?period_days=5" | jq '.[] | .period_days'
   ```

### Issue: Column Header Not Updating
**Symptoms**: Header still says "20일 수익률"

**Solution**: 
- Refresh dashboard (Ctrl+F5)
- Check dashboard logs: `docker logs actstock-dashboard --tail 50`

## Manual Spark Job Execution

If you need to generate portfolio data now:

```bash
docker compose exec spark-master \
  spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark-apps/batch/spark_active_stock_allocator.py
```

**This will**:
1. Load trending ETFs from Stage 3
2. Generate 5d, 10d, 20d portfolios
3. Save all 3 to database

## Verification Queries

### Check All Periods Available
```sql
SELECT 
  period_days,
  COUNT(*) as stock_count,
  MAX(as_of_date) as latest_date,
  SUM(portfolio_weight) as total_weight
FROM analytics_portfolio_allocation
GROUP BY period_days
ORDER BY period_days;
```

Expected:
```
period_days | stock_count | latest_date | total_weight
------------|-------------|-------------|-------------
5           | 8-12        | 2026-01-26  | 1.0000
10          | 8-12        | 2026-01-26  | 1.0000
20          | 8-12        | 2026-01-26  | 1.0000
```

### Compare Top 3 Stocks Across Periods
```sql
SELECT 
  period_days,
  ticker,
  ROUND(portfolio_weight * 100, 2) as weight_pct,
  ROUND(return_20d, 2) as return_pct,
  rank_20d
FROM analytics_portfolio_allocation
WHERE as_of_date = (SELECT MAX(as_of_date) FROM analytics_portfolio_allocation)
  AND rank_20d <= 3
ORDER BY period_days, rank_20d;
```

## Success Criteria

✅ Dashboard loads with period selector dropdown  
✅ Selecting different periods updates the table  
✅ API returns correct data for each period  
✅ Column headers update based on selected period  
✅ Summary bar shows correct period information  
✅ No errors in API or Dashboard logs  
✅ Database contains all 3 periods (5, 10, 20)  

## Next Steps

1. ✅ API endpoints updated
2. ✅ Dashboard UI updated
3. ⚠️ **Wait for Spark job to run** (13:00 UTC / 08:00 EST)
4. ⚠️ **Test with real data** after Spark execution
5. ⚠️ Verify all 3 periods show different allocations
6. ⚠️ Compare period strategies (short vs long term)

## Dashboard Access
- URL: http://localhost:8050
- Auto-refresh: Every 60 seconds
- Period selector: Top of page (below title)

## API Documentation
- Swagger UI: http://localhost:8000/docs
- Health check: http://localhost:8000/health
- Test portfolio: http://localhost:8000/stocks/portfolio?period_days=20
