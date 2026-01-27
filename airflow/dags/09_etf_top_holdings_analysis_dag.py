#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - ETF Top Holdings Analysis
Daily calculation of top 5 performing holdings for each ETF over 5/10/20 day windows
"""

from datetime import datetime, timedelta
import logging
import subprocess
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, '/opt/airflow/project')

from database.db_helper import DatabaseHelper

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'actstock',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_data_availability(**context):
    """Check if we have sufficient data to run analysis"""
    db = DatabaseHelper()
    
    try:
        # Check ETF metadata
        etf_count_query = "SELECT COUNT(*) as cnt FROM 00_collected_meta_etf"
        etf_result = db.fetch_one(etf_count_query)
        etf_count = etf_result['cnt'] if etf_result else 0
        
        logger.info("Found %d ETFs in database", etf_count)
        
        # Check stock history (need at least 30 days)
        history_query = """
            SELECT COUNT(DISTINCT ticker) as tickers,
                   COUNT(DISTINCT trade_date) as days,
                   MAX(trade_date) as latest_date
            FROM 06_collected_daily_stock_history
            WHERE trade_date >= NOW() - INTERVAL '40 days'
        """
        history_result = db.fetch_one(history_query)
        
        if history_result:
            logger.info(
                "Stock history: %d tickers, %d days, latest: %s",
                history_result['tickers'],
                history_result['days'],
                history_result['latest_date']
            )
            
            if history_result['days'] < 20:
                raise ValueError(f"Insufficient history: only {history_result['days']} days (need >= 20)")
        
        # Check ETF holdings data
        holdings_query = "SELECT COUNT(*) as cnt FROM 04_collected_etf_holdings"
        holdings_result = db.fetch_one(holdings_query)
        holdings_count = holdings_result['cnt'] if holdings_result else 0
        
        logger.info("Found %d ETF holdings records", holdings_count)
        
        if etf_count == 0:
            raise ValueError("No ETF metadata found")
        
        if holdings_count == 0:
            logger.warning("No ETF holdings data - analysis will use stock performance only")
        
        logger.info("Data availability check passed")
        
    except Exception as exc:
        logger.error("Data availability check failed: %s", exc)
        raise
    finally:
        db.disconnect()


def run_spark_top_holdings_job(**context):
    """Execute Spark job to calculate top holdings"""
    logger.info("Starting Spark ETF Top Holdings job")
    
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "local[2]",
        "--driver-memory", "2g",
        "--executor-memory", "2g",
        "--packages", "org.postgresql:postgresql:42.6.0",
        "/opt/spark-apps/batch/spark_03_etf_top_holdings.py"
    ]
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        logger.info("Spark job completed successfully")
        logger.info("STDOUT:\n%s", result.stdout[-2000:])  # Last 2000 chars
        
        if result.stderr:
            logger.warning("STDERR:\n%s", result.stderr[-1000:])
        
        return "success"
        
    except subprocess.TimeoutExpired:
        logger.error("Spark job timed out after 10 minutes")
        raise
    except subprocess.CalledProcessError as exc:
        logger.error("Spark job failed with exit code %d", exc.returncode)
        logger.error("STDOUT:\n%s", exc.stdout[-2000:] if exc.stdout else "")
        logger.error("STDERR:\n%s", exc.stderr[-2000:] if exc.stderr else "")
        raise


def verify_results(**context):
    """Verify that results were generated"""
    db = DatabaseHelper()
    
    try:
        query = """
            SELECT 
                time_period,
                COUNT(*) as record_count,
                COUNT(DISTINCT etf_ticker) as etf_count,
                MAX(as_of_date) as latest_date
            FROM 09_analytics_etf_top_holdings
            WHERE as_of_date >= CURRENT_DATE - INTERVAL '2 days'
            GROUP BY time_period
            ORDER BY time_period
        """
        
        results = db.fetch_all(query)
        
        if not results:
            raise ValueError("No results found in 09_analytics_etf_top_holdings")
        
        logger.info("Results verification:")
        for row in results:
            logger.info(
                "  %d-day period: %d records, %d ETFs, latest: %s",
                row['time_period'],
                row['record_count'],
                row['etf_count'],
                row['latest_date']
            )
        
        # Check that we have all 3 periods
        periods = {row['time_period'] for row in results}
        expected_periods = {5, 10, 20}
        
        if periods != expected_periods:
            missing = expected_periods - periods
            logger.warning("Missing periods: %s", missing)
        
        logger.info("Results verification passed")
        
    except Exception as exc:
        logger.error("Results verification failed: %s", exc)
        raise
    finally:
        db.disconnect()


# DAG definition
with DAG(
    dag_id='etf_top_holdings_analysis',
    default_args=default_args,
    description='Calculate top 5 ETF holdings by performance (5/10/20 day windows)',
    schedule_interval='0 20 * * *',  # Daily at 8 PM UTC (after market close)
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['analytics', 'etf', 'holdings', 'spark'],
) as dag:
    
    check_data = PythonOperator(
        task_id='check_data_availability',
        python_callable=check_data_availability,
        provide_context=True,
    )
    
    run_spark_job = PythonOperator(
        task_id='run_spark_top_holdings',
        python_callable=run_spark_top_holdings_job,
        provide_context=True,
        execution_timeout=timedelta(minutes=15),
    )
    
    verify = PythonOperator(
        task_id='verify_results',
        python_callable=verify_results,
        provide_context=True,
    )
    
    # Task dependencies
    check_data >> run_spark_job >> verify
