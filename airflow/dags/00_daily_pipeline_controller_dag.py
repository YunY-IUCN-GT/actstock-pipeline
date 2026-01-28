#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - Daily Pipeline Controller
Schedule: Weekdays, 21:30 UTC (Market Close + 30 mins)
Description:
    Master Controller that orchestrates the 5-Stage Pipeline sequentially.
    Ensures strict order and delays between stages.

Workflow:
    1. Trigger Stage 1 (Benchmark Collection) -> Wait for completion
    2. Wait 1 Hour
    3. Trigger Stage 2 (Sector Collection) -> Wait for completion
    4. Trigger Stage 3 (Trending Analysis) -> Wait for completion
    5. Wait 1 Hour
    6. Trigger Stage 4 (Holdings Collection) -> Wait for completion
    7. Trigger Stage 5 (Portfolio Allocation) -> Wait for completion
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor

# DAG Definition
default_args = {
    'owner': 'actstock',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_pipeline_controller',
    default_args=default_args,
    description='Master Controller for 5-Stage Daily Pipeline',
    schedule_interval='30 21 * * 1-5',  # 21:30 UTC
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['controller', 'daily', 'pipeline'],
    max_active_runs=1,
) as dag:

    # 1. Trigger Stage 1: Benchmark ETF Collection
    trigger_stage_1 = TriggerDagRunOperator(
        task_id='trigger_stage_1_benchmark',
        trigger_dag_id='daily_benchmark_etf_collection',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True, # Allows re-running if needed
    )

    # 2. Wait 1 Hour after Stage 1
    # Note: TimeDeltaSensor waits X time from the *execution_date* (dag run start time)
    # Ideally, we want to wait relative to the previous task, but Airflow standard sensors utilize execution_date.
    # To implement "Wait 1 hour AFTER Stage 1 finishes", it's simpler to use a Python sleep or
    # just rely on the fact that if Stage 1 takes ~1 min, waiting 1 hour from start is effectively the same.
    # However, for strict "1 hr gap between stages", we will simply sleep or use TimeTimeDeltaSensor with delta.
    # Given requirements: "1 hour interval (1 hour after Stage 1 completion)"
    # A cleaner approach in Airflow for "wait after completion" is simply a sleep task or
    # just chaining dependencies if we accept "1 hour from start of wait task".
    # We will use a precise PythonOperator sleep for semantic correctness of "After Completion".
    
    def wait_one_hour(**context):
        import time
        logging.info("Waiting 1 hour before next stage...")
        time.sleep(3600) # 3600 seconds = 1 hour
        logging.info("Wait complete.")

    wait_after_stage_1 = PythonOperator(
        task_id='wait_1_hour_after_stage_1',
        python_callable=wait_one_hour,
    )

    # 3. Trigger Stage 2: Sector ETF Collection
    trigger_stage_2 = TriggerDagRunOperator(
        task_id='trigger_stage_2_sector',
        trigger_dag_id='daily_sector_etf_collection',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # 4. Trigger Stage 3: Trending Analysis (Immediate after Stage 2)
    trigger_stage_3 = TriggerDagRunOperator(
        task_id='trigger_stage_3_analysis',
        trigger_dag_id='daily_trending_etf_analysis',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # 5. Wait 1 Hour after Stage 3 (Analyzer Confirmation)
    wait_after_stage_3 = PythonOperator(
        task_id='wait_1_hour_after_stage_3',
        python_callable=wait_one_hour,
    )

    # 6. Trigger Stage 4: Holdings Collection
    trigger_stage_4 = TriggerDagRunOperator(
        task_id='trigger_stage_4_holdings',
        trigger_dag_id='daily_trending_etf_holdings_collection',
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
    )

    # 7. Trigger Stage 5: Portfolio Allocation (Immediate after Stage 4)
    trigger_stage_5 = TriggerDagRunOperator(
        task_id='trigger_stage_5_portfolio',
        trigger_dag_id='daily_portfolio_allocation',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Set Dependencies
    trigger_stage_1 >> wait_after_stage_1 >> trigger_stage_2 >> trigger_stage_3 >> wait_after_stage_3 >> trigger_stage_4 >> trigger_stage_5
