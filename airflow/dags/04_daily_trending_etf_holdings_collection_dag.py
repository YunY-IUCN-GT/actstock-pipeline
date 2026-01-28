#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - Daily Trending ETF Holdings Collection
Schedule: Weekdays only, 23:00 UTC (after trending ETF identification)
Depends on: daily_trending_etf_analysis
Only collects holdings for ETFs identified as trending
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
            return 'skip_collection'
    return 'check_trending_results_ready'


def check_trending_results_ready(**context):
    """Verify trending ETF analysis results are available"""
    db = DatabaseHelper()
    
    try:
        query = """
            SELECT 
                COUNT(*) as total_etfs,
                SUM(CASE WHEN is_trending THEN 1 ELSE 0 END) as trending_count
            FROM analytics_03_trending_etfs
            WHERE as_of_date = CURRENT_DATE
        """
        result = db.fetch_one(query)
        
        total_etfs = result['total_etfs'] if result else 0
        trending_count = result['trending_count'] if result else 0
        
        logger.info("Trending ETF check:")
        logger.info("  - Total ETFs analyzed: %d", total_etfs)
        logger.info("  - Trending ETFs: %d", trending_count)
        
        if total_etfs == 0:
            raise ValueError("No trending analysis results found - run daily_trending_etf_analysis first")
        
        if trending_count == 0:
            logger.warning("No trending ETFs today - skipping holdings collection")
            # This is OK - just means no ETFs are trending
            return 'skip_collection'
        
        logger.info("Trending ETF results ready for holdings collection")
        return 'run_holdings_collection'
        
    finally:
        db.disconnect()


def run_holdings_collection(**context):
    """
    Execute holdings collection for trending ETFs only
    """
    logger.info("Starting trending ETF holdings collection")
    
    try:
        result = subprocess.run(
            ["python3", "/opt/airflow/project/collector/kafka_03_producer_trending_etf_holdings.py"],
            check=True,
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minutes
        )
        
        logger.info("Holdings collection completed")
        logger.info("Output (last 500 chars): %s", result.stdout[-500:] if result.stdout else "")
        return "success"
        
    except subprocess.TimeoutExpired:
        logger.error("Holdings collection timed out after 30 minutes")
        raise
    except subprocess.CalledProcessError as exc:
        logger.error("Holdings collection failed")
        logger.error("stderr (last 1000 chars): %s", exc.stderr[-1000:] if exc.stderr else "")
        raise


def verify_holdings_collected(**context):
    """Verify holdings and stock data were collected"""
    db = DatabaseHelper()
    
    try:
        # Check holdings collected
        holdings_query = """
            SELECT 
                COUNT(DISTINCT etf_ticker) as etf_count,
                COUNT(DISTINCT ticker) as holding_count
            FROM collected_04_etf_holdings
            WHERE as_of_date = CURRENT_DATE
        """
        holdings_result = db.fetch_one(holdings_query)
        
        etf_count = holdings_result['etf_count'] if holdings_result else 0
        holding_count = holdings_result['holding_count'] if holdings_result else 0
        
        # Check stock data collected
        stock_query = """
            SELECT 
                COUNT(DISTINCT ticker) as stock_count,
                MAX(trade_date) as latest_date
            FROM collected_06_daily_stock_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '1 day'
        """
        stock_result = db.fetch_one(stock_query)
        
        stock_count = stock_result['stock_count'] if stock_result else 0
        latest_date = stock_result['latest_date'] if stock_result else None
        
        logger.info("Holdings collection verification:")
        logger.info("  - ETFs with holdings: %d", etf_count)
        logger.info("  - Total holdings collected: %d", holding_count)
        logger.info("  - Stocks collected: %d", stock_count)
        logger.info("  - Latest stock data date: %s", latest_date)
        
        # Get trending count for comparison
        trending_query = """
            SELECT COUNT(*) as trending_count
            FROM analytics_03_trending_etfs
            WHERE as_of_date = CURRENT_DATE
              AND is_trending = TRUE
        """
        trending_result = db.fetch_one(trending_query)
        trending_count = trending_result['trending_count'] if trending_result else 0
        
        logger.info("  - Trending ETFs identified: %d", trending_count)
        
        if etf_count == 0 and trending_count > 0:
            logger.warning("No holdings collected despite %d trending ETFs", trending_count)
        
        logger.info("Holdings collection verification passed")
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
    'daily_trending_etf_holdings_collection',
    default_args=default_args,
    description='Collect holdings + stock data - Triggered by Controller',
    schedule_interval=None,  # Controlled by master DAG
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['collection', 'holdings', 'conditional', 'trending', 'controller-managed'],
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
skip_collection = EmptyOperator(
    task_id='skip_collection',
    dag=dag,
)

# Task 3: Check trending results (also branches if no trending ETFs)
check_trending = BranchPythonOperator(
    task_id='check_trending_results_ready',
    python_callable=check_trending_results_ready,
    provide_context=True,
    dag=dag,
)

# Task 4: Run holdings collection
collect_holdings = PythonOperator(
    task_id='run_holdings_collection',
    python_callable=run_holdings_collection,
    provide_context=True,
    execution_timeout=timedelta(minutes=30),
    pool='etf_collection',
    dag=dag,
)

# Task 5: Verify results
verify_results = PythonOperator(
    task_id='verify_holdings_collected',
    python_callable=verify_holdings_collected,
    provide_context=True,
    dag=dag,
)

# Task flow
branch_weekday >> [skip_collection, check_trending]
check_trending >> [skip_collection, collect_holdings]
collect_holdings >> verify_results
