#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - Monthly ETF Data Collection (All Groups)
Schedule: Last weekend of every month (Saturday)
Collects: All benchmark + sector ETFs for monthly analysis
Avoids: Market opening days (Mon-Fri)
"""

from datetime import datetime, timedelta
import logging
import sys
import time
from calendar import monthrange

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, '/opt/airflow/project')

from config.config import BENCHMARK_TICKERS, SECTOR_ETF_TICKERS
from database.db_helper import DatabaseHelper

logger = logging.getLogger(__name__)


def is_last_weekend(**context):
    """
    Check if today is the last Saturday or Sunday of the month
    This avoids collecting on market opening days (weekdays)
    """
    execution_date = context.get('execution_date')
    if execution_date:
        day = execution_date.day
        month = execution_date.month
        year = execution_date.year
        weekday = execution_date.weekday()  # Monday=0, Sunday=6
        
        # Get last day of month
        last_day = monthrange(year, month)[1]
        
        # Check if it's a weekend (Sat=5 or Sun=6)
        is_weekend = weekday in [5, 6]
        
        # Check if it's in the last week (within 7 days of month end)
        is_last_week = day > (last_day - 7)
        
        is_eligible = is_weekend and is_last_week
        
        logger.info("Date: %s, Day: %d/%d, Weekday: %d, Is last weekend: %s",
                   execution_date.date(), day, last_day, weekday, is_eligible)
        
        if not is_eligible:
            raise ValueError("Not last weekend of month - skipping")
        
        return True
    return True


def collect_all_etf_monthly_data(**context):
    """
    Collect monthly snapshot of all ETFs (benchmark + sector)
    Rate-limited: 8 seconds between tickers (more conservative for monthly batch)
    """
    import yfinance as yf
    
    all_tickers = list(BENCHMARK_TICKERS) + list(SECTOR_ETF_TICKERS)
    
    db = DatabaseHelper()
    logger.info("=" * 70)
    logger.info("Monthly ETF Collection - Last Weekend of Month")
    logger.info("Total ETFs: %d (6 benchmark + 11 sector)", len(all_tickers))
    logger.info("=" * 70)
    
    successful = []
    failed = []
    
    for idx, ticker in enumerate(all_tickers, 1):
        logger.info("[%d/%d] Fetching %s...", idx, len(all_tickers), ticker)
        
        try:
            etf = yf.Ticker(ticker)
            # Get last 10 days to ensure we have recent data
            hist = etf.history(period='10d')
            
            if hist.empty:
                logger.warning("No data returned for %s", ticker)
                failed.append(ticker)
                continue
            
            # Get last 5 trading days
            recent_data = hist.tail(5)
            
            for date_idx in recent_data.index:
                trade_date = date_idx.date()
                row = recent_data.loc[date_idx]
                
                # Calculate price change
                row_position = recent_data.index.get_loc(date_idx)
                if row_position > 0:
                    prev_close = recent_data.iloc[row_position - 1]['Close']
                    price_change_pct = ((row['Close'] - prev_close) / prev_close) * 100
                else:
                    price_change_pct = 0.0
                
                insert_query = """
                    INSERT INTO collected_daily_etf_ohlc (
                        ticker, trade_date, open_price, high_price, low_price,
                        close_price, volume, price_change_percent
                    ) VALUES (
                        %(ticker)s, %(trade_date)s, %(open_price)s, %(high_price)s,
                        %(low_price)s, %(close_price)s, %(volume)s, %(price_change_percent)s
                    )
                    ON CONFLICT (ticker, trade_date) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        price_change_percent = EXCLUDED.price_change_percent,
                        created_at = NOW()
                """
                
                row_data = {
                    'ticker': ticker,
                    'trade_date': trade_date,
                    'open_price': float(row['Open']),
                    'high_price': float(row['High']),
                    'low_price': float(row['Low']),
                    'close_price': float(row['Close']),
                    'volume': int(row['Volume']),
                    'price_change_percent': float(price_change_pct)
                }
                
                db.execute_query(insert_query, row_data)
            
            logger.info("✓ Saved %s: %d days, latest=%.2f",
                       ticker, len(recent_data), recent_data.iloc[-1]['Close'])
            successful.append(ticker)
            
        except Exception as exc:
            logger.error("✗ Error fetching %s: %s", ticker, exc)
            failed.append(ticker)
        
        # Rate limiting: 8 seconds (more conservative for monthly batch)
        if idx < len(all_tickers):
            logger.debug("Waiting 8 seconds...")
            time.sleep(8)
    
    db.disconnect()
    
    logger.info("=" * 70)
    logger.info("Monthly Collection Summary")
    logger.info("Successful: %d - %s", len(successful), ', '.join(successful))
    logger.info("Failed: %d - %s", len(failed), ', '.join(failed) if failed else 'None')
    logger.info("=" * 70)
    
    if len(failed) > 5:  # Allow some failures
        raise ValueError(f"Too many failures: {len(failed)}/{len(all_tickers)}")


def run_monthly_top_holdings_analysis(**context):
    """
    Run comprehensive top holdings analysis for all ETFs
    Calculates 5/10/20 day windows for monthly report
    """
    import subprocess
    
    logger.info("Running monthly top holdings analysis for all ETFs")
    
    try:
        result = subprocess.run(
            [
                "/opt/spark/bin/spark-submit",
                "--master", "local[2]",
                "--driver-memory", "2g",
                "--executor-memory", "2g",
                "--packages", "org.postgresql:postgresql:42.6.0",
                "/opt/spark-apps/batch/spark_etf_top_holdings.py"
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=900  # 15 minutes for full analysis
        )
        
        logger.info("Monthly analysis completed")
        logger.info("Output: %s", result.stdout[-500:])
        return "success"
        
    except subprocess.TimeoutExpired:
        logger.error("Monthly analysis timed out")
        raise
    except subprocess.CalledProcessError as exc:
        logger.error("Monthly analysis failed: %s", exc.stderr[-1000:] if exc.stderr else "")
        raise


def verify_monthly_results(**context):
    """Verify monthly results"""
    db = DatabaseHelper()
    
    try:
        query = """
            SELECT 
                etf_type,
                COUNT(DISTINCT etf_ticker) as etf_count,
                COUNT(*) as total_records
            FROM analytics_etf_top_holdings
            WHERE as_of_date >= CURRENT_DATE - INTERVAL '2 days'
            GROUP BY etf_type
        """
        results = db.fetch_all(query)
        
        logger.info("Monthly results verification:")
        for row in results:
            logger.info("  %s: %d ETFs, %d records",
                       row['etf_type'], row['etf_count'], row['total_records'])
        
        total_etfs = sum(r['etf_count'] for r in results)
        if total_etfs < 15:  # Expect at least 15 out of 17 ETFs
            raise ValueError(f"Insufficient ETFs in results: {total_etfs}/17")
            
    finally:
        db.disconnect()


default_args = {
    'owner': 'actstock',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(hours=2),
}

with DAG(
    dag_id='monthly_etf_collection_last_weekend',
    default_args=default_args,
    description='Monthly ETF collection on last weekend (Sat/Sun) - Avoids weekdays',
    schedule_interval='0 8 * * 6',  # Every Saturday at 8 AM UTC (will check if it's last weekend)
    start_date=days_ago(1),
    catchup=False,
    tags=['monthly', 'etf', 'all-groups', 'weekend-only'],
) as dag:
    
    check_last_weekend = PythonOperator(
        task_id='check_if_last_weekend',
        python_callable=is_last_weekend,
        provide_context=True,
    )
    
    collect_etf_data = PythonOperator(
        task_id='collect_all_etf_monthly_data',
        python_callable=collect_all_etf_monthly_data,
        provide_context=True,
        execution_timeout=timedelta(minutes=45),
    )
    
    run_analysis = PythonOperator(
        task_id='run_monthly_top_holdings_analysis',
        python_callable=run_monthly_top_holdings_analysis,
        provide_context=True,
        execution_timeout=timedelta(minutes=20),
    )
    
    verify = PythonOperator(
        task_id='verify_monthly_results',
        python_callable=verify_monthly_results,
        provide_context=True,
    )
    
    check_last_weekend >> collect_etf_data >> run_analysis >> verify
