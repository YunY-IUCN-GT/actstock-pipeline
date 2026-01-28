#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - Daily Trending ETF Analysis
Schedule: Weekdays only, 22:30 UTC (after ETF OHLC collections)
Depends on: Group 1 and Group 2 ETF data
Triggers Spark job to identify which ETFs are trending vs SPY
"""

from datetime import datetime, timedelta
import logging
import subprocess
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, '/opt/airflow/project')

from database.db_helper import DatabaseHelper

logger = logging.getLogger(__name__)


def is_weekday(**context):
    """Check if today is a weekday"""
    execution_date = context.get('execution_date')
    if execution_date:
        weekday = execution_date.weekday()
        if weekday >= 5:
            logger.info("Skipping - Weekend (not a trading day)")
            return 'skip_analysis'
    return 'check_etf_data_ready'


def check_etf_data_ready(**context):
    """Verify ETF OHLC data is available from 09:00 and 10:00 collections"""
    db = DatabaseHelper()
    
    try:
        # Check if we have recent ETF data
        query = """
            SELECT COUNT(DISTINCT ticker) as etf_count,
                   MAX(trade_date) as latest_date
            FROM collected_01_daily_etf_ohlc
            WHERE trade_date >= CURRENT_DATE - INTERVAL '2 days'
        """
        result = db.fetch_one(query)
        
        etf_count = result['etf_count'] if result else 0
        latest_date = result['latest_date'] if result else None
        
        logger.info("ETF data check:")
        logger.info("  - ETF tickers with recent data: %d", etf_count)
        logger.info("  - Latest trade date: %s", latest_date)
        
        # Need at least 15 ETFs (6 benchmark + 11 sector, with some tolerance)
        if etf_count < 15:
            raise ValueError(f"Insufficient ETF data: only {etf_count} tickers (need 15+)")
        
        # Check if we have SPY data (critical for trending analysis)
        spy_query = """
            SELECT COUNT(*) as spy_count
            FROM collected_01_daily_etf_ohlc
            WHERE ticker = 'SPY'
              AND trade_date >= CURRENT_DATE - INTERVAL '25 days'
        """
        spy_result = db.fetch_one(spy_query)
        spy_count = spy_result['spy_count'] if spy_result else 0
        
        logger.info("  - SPY data points (last 25 days): %d", spy_count)
        
        if spy_count < 15:
            raise ValueError(f"Insufficient SPY benchmark data: {spy_count} days (need 15+)")
        
        logger.info("ETF data ready for trending analysis")
        return True
        
    finally:
        db.disconnect()


def run_trending_etf_spark(**context):
    """
    Execute Spark job to identify trending ETFs
    """
    logger.info("Starting Spark trending ETF identifier job")
    
    try:
        result = subprocess.run(
            [
                "/opt/spark/bin/spark-submit",
                "--master", "local[2]",
                "--driver-memory", "1g",
                "--packages", "org.postgresql:postgresql:42.6.0",
                "/opt/spark-apps/batch/spark_01_trending_etf_identifier.py"
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minutes
        )
        
        logger.info("Trending ETF identifier completed successfully")
        logger.info("Output (last 500 chars): %s", result.stdout[-500:] if result.stdout else "")
        return "success"
        
    except subprocess.TimeoutExpired:
        logger.error("Trending ETF identifier timed out after 10 minutes")
        raise
    except subprocess.CalledProcessError as exc:
        logger.error("Trending ETF identifier failed")
        logger.error("stderr (last 1000 chars): %s", exc.stderr[-1000:] if exc.stderr else "")
        raise


def verify_trending_results(**context):
    """Verify trending ETF results were generated"""
    db = DatabaseHelper()
    
    try:
        query = """
            SELECT 
                COUNT(*) as total_etfs,
                SUM(CASE WHEN is_trending THEN 1 ELSE 0 END) as trending_count,
                MAX(as_of_date) as latest_date
            FROM analytics_03_trending_etfs
            WHERE as_of_date = CURRENT_DATE
        """
        result = db.fetch_one(query)
        
        total_etfs = result['total_etfs'] if result else 0
        trending_count = result['trending_count'] if result else 0
        latest_date = result['latest_date'] if result else None
        
        logger.info("Trending ETF verification:")
        logger.info("  - Total ETFs analyzed: %d", total_etfs)
        logger.info("  - Trending ETFs identified: %d", trending_count)
        logger.info("  - Analysis date: %s", latest_date)
        
        if total_etfs == 0:
            raise ValueError("No trending ETF results generated")
        
        # It's OK if there are 0 trending ETFs (market conditions)
        if trending_count == 0:
            logger.warning("No ETFs are trending today (all underperforming SPY or negative)")
        else:
            # Get list of trending ETFs
            # Get list of trending ETFs
            # Joins with metadata table to get context
            trending_query = """
                SELECT t.etf_ticker as ticker, m.etf_type, m.sector_name, t.return_pct
                FROM analytics_03_trending_etfs t
                LEFT JOIN collected_00_meta_etf m ON t.etf_ticker = m.ticker
                WHERE t.as_of_date = CURRENT_DATE
                  AND t.is_trending = TRUE
                ORDER BY t.return_pct DESC
            """
            trending_etfs = db.fetch_all(trending_query)
            
            logger.info("Trending ETFs:")
            for etf in trending_etfs:
                logger.info("  %s (%s): %.2f%%",
                           etf['ticker'],
                           etf['sector_name'] if etf['sector_name'] else 'Benchmark',
                           etf['return_pct'])
        
        logger.info("Trending ETF analysis verification passed")
        return True
        
    finally:
        db.disconnect()


# DAG Definition
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_trending_etf_analysis',
    default_args=default_args,
    description='Identify trending ETFs - Triggered by Controller',
    schedule_interval=None,  # Controlled by master DAG
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['analytics', 'trending', 'etf', 'spark', 'controller-managed'],
    max_active_runs=1,
)

# ExternalTaskSensor removed - dependency managed by Controller DAG

# Task 1: Check if weekday
branch_weekday = BranchPythonOperator(
    task_id='branch_weekday',
    python_callable=is_weekday,
    provide_context=True,
    dag=dag,
)

# Task 2: Skip path
skip_analysis = EmptyOperator(
    task_id='skip_analysis',
    dag=dag,
)

# Task 3: Check ETF data ready
check_data = PythonOperator(
    task_id='check_etf_data_ready',
    python_callable=check_etf_data_ready,
    provide_context=True,
    dag=dag,
)

# Task 4: Run Spark job
run_spark = PythonOperator(
    task_id='run_trending_etf_identifier',
    python_callable=run_trending_etf_spark,
    provide_context=True,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

# Task 5: Verify results
verify_results = PythonOperator(
    task_id='verify_trending_results',
    python_callable=verify_trending_results,
    provide_context=True,
    dag=dag,
)

# Task flow
branch_weekday >> [skip_analysis, check_data]
check_data >> run_spark >> verify_results
