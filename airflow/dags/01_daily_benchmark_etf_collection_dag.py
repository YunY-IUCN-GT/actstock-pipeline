#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - Daily Benchmark ETF Data Collection (Group 1)
Schedule: Weekdays only, 9:00 AM UTC (after US market open)
Collects: SPY, QQQ, IWM, EWY, DIA, SCHD (6 benchmark ETFs)
Note: QQQ also serves as Technology sector representative (total 15 unique ETFs)
"""

from datetime import datetime, timedelta
import logging
import sys
import time
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum

sys.path.insert(0, '/opt/airflow/project')

from config.config import BENCHMARK_TICKERS
from database.db_helper import DatabaseHelper

logger = logging.getLogger(__name__)

# Weekday check - Monday=0, Sunday=6
def is_weekday(**context):
    """Check if today is a weekday (Mon-Fri)"""
    execution_date = context.get('execution_date')
    if execution_date:
        weekday = execution_date.weekday()
        is_trading_day = weekday < 5  # Monday=0 to Friday=4
        logger.info("Execution date: %s, Weekday: %d, Is trading day: %s", 
                    execution_date, weekday, is_trading_day)
        
        if not is_trading_day:
            logger.info("Skipping - Weekend detected")
            raise ValueError("Not a trading day - skipping execution")
        
        return True
    return True


def collect_benchmark_etf_data(**context):
    """
    Collect daily OHLC data for benchmark ETFs via Kafka
    Kafka Pipeline: Producer → etf-daily-data topic → Consumer → PostgreSQL
    """
    from collector.kafka_01_producer_etf_daily import ETFDailyDataProducer
    
    logger.info("Starting benchmark ETF data collection (Group 1) via Kafka")
    logger.info("Using Kafka pipeline: Producer → etf-daily-data → Consumer → PostgreSQL")
    
    producer = ETFDailyDataProducer()
    
    try:
        successful_count, failed_count = producer.produce_benchmark_group(delay_seconds=5)
        
        logger.info("=" * 70)
        logger.info("Collection Summary - Benchmark ETFs (Group 1)")
        logger.info("Successful: %d", successful_count)
        logger.info("Failed: %d", failed_count)
        logger.info("Messages sent to Kafka topic: etf-daily-data")
        logger.info("=" * 70)
        
        if failed_count > successful_count:
            raise ValueError(f"Too many failures: {failed_count}/{successful_count + failed_count}")
    
    finally:
        producer.close()


# DAG definition
default_args = {
    'owner': 'actstock',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='daily_benchmark_etf_collection',
    default_args=default_args,
    description='Daily collection of benchmark ETF data (Group 1) - Weekdays only',
    schedule_interval='0 9 * * 1-5',  # 9 AM UTC, Monday-Friday
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['daily', 'etf', 'benchmark', 'group1', 'weekday-only'],
) as dag:
    
    check_weekday = PythonOperator(
        task_id='check_if_weekday',
        python_callable=is_weekday,
        provide_context=True,
    )
    
    collect_data = PythonOperator(
        task_id='collect_benchmark_etf_data',
        python_callable=collect_benchmark_etf_data,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )
    
    check_weekday >> collect_data
