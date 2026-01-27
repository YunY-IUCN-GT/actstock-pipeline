#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - Daily Sector ETF Data Collection (Group 2)
Schedule: Weekdays only, 10:00 AM UTC (1 hour after benchmark ETFs)
Collects: QQQ, XLV, XLF, XLY, XLC, XLI, XLP, XLU, XLRE, XLB (10 sector ETFs)
Note: QQQ is Technology sector representative (also counted in benchmarks)
"""

from datetime import datetime, timedelta
import logging
import sys
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, '/opt/airflow/project')

from config.config import SECTOR_ETF_TICKERS
from database.db_helper import DatabaseHelper

logger = logging.getLogger(__name__)


def is_weekday(**context):
    """Check if today is a weekday (Mon-Fri)"""
    execution_date = context.get('execution_date')
    if execution_date:
        weekday = execution_date.weekday()
        is_trading_day = weekday < 5
        logger.info("Weekday: %d, Is trading day: %s", weekday, is_trading_day)
        
        if not is_trading_day:
            logger.info("Skipping - Weekend detected")
            raise ValueError("Not a trading day")
        
        return True
    return True


def collect_sector_etf_data(**context):
    """
    Collect daily OHLC data for sector ETFs via Kafka
    Kafka Pipeline: Producer → etf-daily-data topic → Consumer → PostgreSQL
    """
    from collector.kafka_01_producer_etf_daily import ETFDailyDataProducer
    
    logger.info("Starting sector ETF data collection (Group 2) via Kafka")
    logger.info("Using Kafka pipeline: Producer → etf-daily-data → Consumer → PostgreSQL")
    
    producer = ETFDailyDataProducer()
    
    try:
        successful_count, failed_count = producer.produce_sector_group(delay_seconds=5)
        
        logger.info("=" * 70)
        logger.info("Collection Summary - Sector ETFs (Group 2)")
        logger.info("Successful: %d", successful_count)
        logger.info("Failed: %d", failed_count)
        logger.info("Messages sent to Kafka topic: etf-daily-data")
        logger.info("=" * 70)
        
        if failed_count > successful_count:
            raise ValueError(f"Too many failures: {failed_count}/{successful_count + failed_count}")
    
    finally:
        producer.close()


default_args = {
    'owner': 'actstock',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='daily_sector_etf_collection',
    default_args=default_args,
    description='Daily collection of sector ETF data (Group 2) - Weekdays only',
    schedule_interval='0 10 * * 1-5',  # 10 AM UTC, Monday-Friday (1 hour after benchmark)
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['daily', 'etf', 'sector', 'group2', 'weekday-only'],
) as dag:
    
    check_weekday = PythonOperator(
        task_id='check_if_weekday',
        python_callable=is_weekday,
        provide_context=True,
    )
    
    collect_data = PythonOperator(
        task_id='collect_sector_etf_data',
        python_callable=collect_sector_etf_data,
        provide_context=True,
        execution_timeout=timedelta(minutes=15),
    )
    
    check_weekday >> collect_data
