# DAG Schedule Reference - Controller Based

## Active DAGs (Controlled by Master)

The following DAGs are now strictly orchestrated by `daily_pipeline_controller`. They generally **do not** have independent schedules.

| Order | Stage | DAG Name | Trigger Mechanism | Dependencies |
|-------|-------|----------|-------------------|--------------|
| **Master** | **Controller** | `daily_pipeline_controller` | **21:30 UTC** (Mon-Fri) | None |
| 1 | Stage 1 | `daily_benchmark_etf_collection` | Triggered by Master | None |
| 2 | Stage 2 | `daily_sector_etf_collection` | Triggered by Master | 1h delay after Stage 1 |
| 3 | Stage 3 | `daily_trending_etf_analysis` | Triggered by Master | Immediate after Stage 2 |
| 4 | Stage 4 | `daily_trending_etf_holdings_collection` | Triggered by Master | 1h delay after Stage 3 |
| 5 | Stage 5 | `daily_portfolio_allocation` | Triggered by Master | Immediate after Stage 4 |

## Disabled / Legacy DAGs

| DAG Name | Status | Reason |
|----------|--------|--------|
| `daily_benchmark_top_holdings_dag` | ❌ Disabled | Replaced by pipeline |
| `daily_sector_top_holdings_dag` | ❌ Disabled | Replaced by pipeline |
| `daily_stock_collection_dag` | ❌ Disabled | Replaced by pipeline |
| `stock_market_pipeline` | ❌ Disabled | Replaced by pipeline |

## Execution Protocol

### How to Run (Manual)
Trigger **ONLY** the `daily_pipeline_controller` DAG.
> Do **NOT** trigger individual stage DAGs manually unless you are debugging a specific step.

### Backfill
To backfill data, trigger `daily_pipeline_controller` with the desired logical date (execution_date). The controller will propagate this date to all sub-DAGs.

## Verification
Check the `daily_pipeline_controller` graph view in Airflow to see the progress of the entire pipeline.
