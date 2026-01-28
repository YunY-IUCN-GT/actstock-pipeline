#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - Daily Portfolio Allocation Analysis
Schedule: Weekdays only, 23:30 UTC (after trending ETF holdings collection)
Triggers Spark job to:
  1. Select top 1 best-performing stock from each trending ETF (from top 5 market cap holdings)
  2. Weight by: performance × inverse market cap
  3. Normalize weights to sum to 100%
Writes to: 05_analytics_portfolio_allocation
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
    return 'check_data_ready'


def check_data_ready(**context):
    """Verify trending ETF holdings data is available"""
    db = DatabaseHelper()
    
    try:
        # Check trending ETFs
        trending_query = """
            SELECT COUNT(*) as trending_count
            FROM analytics_03_trending_etfs
            WHERE as_of_date = CURRENT_DATE
              AND is_trending = TRUE
        """
        trending_result = db.fetch_one(trending_query)
        trending_count = trending_result['trending_count'] if trending_result else 0
        
        # Check holdings for trending ETFs
        holdings_query = """
            SELECT COUNT(DISTINCT eh.holding_ticker) as holdings_count
            FROM collected_04_etf_holdings eh
            INNER JOIN analytics_03_trending_etfs te 
                ON eh.etf_ticker = te.etf_ticker
            WHERE te.as_of_date = CURRENT_DATE
              AND te.is_trending = TRUE
              AND eh.as_of_date >= CURRENT_DATE - INTERVAL '2 days'
        """
        holdings_result = db.fetch_one(holdings_query)
        holdings_count = holdings_result['holdings_count'] if holdings_result else 0
        
        # Check stock data for those holdings
        stock_query = """
            SELECT COUNT(DISTINCT sh.ticker) as stock_count
            FROM collected_06_daily_stock_history sh
            INNER JOIN collected_04_etf_holdings eh 
                ON sh.ticker = eh.holding_ticker
            INNER JOIN analytics_03_trending_etfs te 
                ON eh.etf_ticker = te.etf_ticker
            WHERE te.as_of_date = CURRENT_DATE
              AND te.is_trending = TRUE
              AND sh.trade_date >= CURRENT_DATE - INTERVAL '25 days'
        """
        stock_result = db.fetch_one(stock_query)
        stock_count = stock_result['stock_count'] if stock_result else 0
        
        logger.info("Data availability check:")
        logger.info("  - Trending ETFs today: %d", trending_count)
        logger.info("  - Holdings tracked: %d", holdings_count)
        logger.info("  - Stock tickers with history: %d", stock_count)
        
        if trending_count == 0:
            raise ValueError("No trending ETFs identified - check daily_trending_etf_analysis")
        
        if holdings_count < trending_count:
            raise ValueError(f"Incomplete holdings data: {holdings_count}/{trending_count} trending ETFs")
        
        if stock_count < trending_count:
            raise ValueError(f"Insufficient stock data: only {stock_count} tickers (need at least {trending_count})")
        
        logger.info("Data ready for portfolio allocation")
        return True
        
    finally:
        db.disconnect()


def run_portfolio_allocator_spark(**context):
    """
    Execute Spark job to calculate portfolio allocation
    Runs trending analysis for 5d, 10d, 20d periods and builds portfolio
    """
    logger.info("Starting Spark portfolio allocator job")
    
    try:
        result = subprocess.run(
            [
                "/opt/spark/bin/spark-submit",
                "--master", "local[2]",
                "--driver-memory", "2g",
                "--executor-memory", "2g",
                "--packages", "org.postgresql:postgresql:42.6.0",
                "/opt/spark-apps/batch/spark_02_active_stock_allocator.py"
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=900,  # 15 minutes
        )
        
        logger.info("Portfolio allocator completed successfully")
        logger.info("Output (last 500 chars): %s", result.stdout[-500:] if result.stdout else "")
        return "success"
        
    except subprocess.TimeoutExpired:
        logger.error("Portfolio allocator timed out after 15 minutes")
        raise
    except subprocess.CalledProcessError as exc:
        logger.error("Portfolio allocator failed")
        logger.error("stderr (last 1000 chars): %s", exc.stderr[-1000:] if exc.stderr else "")
        raise


def verify_portfolio_results(**context):
    """Verify portfolio allocation results were generated"""
    db = DatabaseHelper()
    
    try:
        # Check portfolio allocation
        portfolio_query = """
            SELECT 
                COUNT(*) as stock_count,
                SUM(portfolio_weight) as total_weight,
                MAX(as_of_date) as latest_date
            FROM analytics_05_portfolio_allocation
            WHERE as_of_date = CURRENT_DATE
        """
        portfolio_result = db.fetch_one(portfolio_query)
        
        stock_count = portfolio_result['stock_count'] if portfolio_result else 0
        total_weight = portfolio_result['total_weight'] if portfolio_result else 0
        latest_date = portfolio_result['latest_date'] if portfolio_result else None
        
        logger.info("Portfolio verification:")
        logger.info("  - Stocks in portfolio: %d", stock_count)
        logger.info("  - Total weight: %.4f", total_weight if total_weight else 0)
        logger.info("  - Latest date: %s", latest_date)
        
        if stock_count == 0:
            raise ValueError("No portfolio allocation generated")
        
        if stock_count > 20:
            logger.warning("Portfolio has more than 20 stocks (%d)", stock_count)
        
        if total_weight and abs(total_weight - 1.0) > 0.01:
            logger.warning("Portfolio weights don't sum to 1.0: %.4f", total_weight)
        
        # Check sector trending
        sector_query = """
            SELECT COUNT(DISTINCT sector) as sector_count
            FROM analytics_sector_trending
            WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
              AND month = EXTRACT(MONTH FROM CURRENT_DATE)
        """
        sector_result = db.fetch_one(sector_query)
        sector_count = sector_result['sector_count'] if sector_result else 0
        
        logger.info("  - Sectors analyzed: %d", sector_count)
        
        # Check stock trending
        stock_query = """
            SELECT COUNT(DISTINCT ticker) as trending_count
            FROM analytics_stock_trending
            WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
              AND month = EXTRACT(MONTH FROM CURRENT_DATE)
              AND is_trending = TRUE
        """
        stock_result = db.fetch_one(stock_query)
        trending_count = stock_result['trending_count'] if stock_result else 0
        
        logger.info("  - Trending stocks: %d", trending_count)
        
        logger.info("Portfolio analysis verification passed")
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
    'daily_portfolio_allocation',
    default_args=default_args,
    description='Daily portfolio allocation analysis - Triggered by Controller',
    schedule_interval=None,  # Controlled by master DAG
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['analytics', 'portfolio', 'spark', 'controller-managed'],
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

# Task 3: Check data ready
check_data = PythonOperator(
    task_id='check_data_ready',
    python_callable=check_data_ready,
    provide_context=True,
    dag=dag,
)

# Task 4: Run Spark job
run_spark = PythonOperator(
    task_id='run_portfolio_allocator',
    python_callable=run_portfolio_allocator_spark,
    provide_context=True,
    execution_timeout=timedelta(minutes=20),
    dag=dag,
)

# Task 5: Verify results
verify_results = PythonOperator(
    task_id='verify_results',
    python_callable=verify_portfolio_results,
    provide_context=True,
    dag=dag,
)

# Task flow
branch_weekday >> [skip_analysis, check_data]
check_data >> run_spark >> verify_results
