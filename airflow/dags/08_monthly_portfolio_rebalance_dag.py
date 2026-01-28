"""
Airflow DAG: Monthly Portfolio Rebalance
=========================================
실행 스케줄: 매주 일요일 14:00 UTC (23:00 KST) - 마지막 일요일만 실행
목적: 5일/10일/20일 포트폴리오를 통합하여 다음 20영업일 동안 유지할 최종 월간 포트폴리오 생성

Output:
- 08_analytics_monthly_portfolio 테이블
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

logger = logging.getLogger(__name__)

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def is_last_sunday_of_month(**context):
    """
    오늘이 해당 월의 마지막 일요일인지 확인
    
    Raises:
        Exception: 마지막 일요일이 아니면 예외 발생하여 DAG 스킵
    """
    execution_date = context['execution_date']
    target_date = execution_date.date()
    
    # 일요일인지 확인 (weekday: 0=월요일, 6=일요일)
    if target_date.weekday() != 6:
        raise Exception(f"{target_date}는 일요일이 아닙니다. DAG 실행을 건너뜁니다.")
    
    # 다음 일요일이 다음 달인지 확인
    next_sunday = target_date + timedelta(days=7)
    if next_sunday.month == target_date.month:
        raise Exception(f"{target_date}는 해당 월의 마지막 일요일이 아닙니다. DAG 실행을 건너뜁니다.")
    
    logger.info(f"✅ {target_date}는 {target_date.strftime('%Y년 %m월')}의 마지막 일요일입니다.")
    return True


def run_spark_monthly_rebalance(**context):
    """Spark 월간 포트폴리오 리밸런싱 실행"""
    logger.info("Starting Spark monthly portfolio rebalancer")
    
    try:
        result = subprocess.run(
            [
                "/opt/spark/bin/spark-submit",
                "--master", "local[2]",
                "--driver-memory", "2g",
                "--executor-memory", "2g",
                "--packages", "org.postgresql:postgresql:42.6.0",
                "/opt/spark-apps/batch/spark_04_monthly_portfolio_rebalancer.py"
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=900,  # 15 minutes
        )
        
        logger.info("Monthly rebalancer completed successfully")
        logger.info("Output (last 500 chars): %s", result.stdout[-500:] if result.stdout else "")
        return "success"
        
    except subprocess.TimeoutExpired:
        logger.error("Monthly rebalancer timed out after 15 minutes")
        raise
    except subprocess.CalledProcessError as exc:
        logger.error("Monthly rebalancer failed")
        logger.error("stderr (last 1000 chars): %s", exc.stderr[-1000:] if exc.stderr else "")
        raise


def validate_monthly_portfolio(**context):
    """월간 포트폴리오 생성 결과 검증"""
    import psycopg2
    import os
    
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        dbname=os.getenv('POSTGRES_DB', 'stockdb'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
    )
    
    cursor = conn.cursor()
    execution_date = context['ds']
    
    # 생성된 포트폴리오 개수 확인
    cursor.execute("""
        SELECT 
            COUNT(*) as stock_count,
            ROUND(SUM(final_weight) * 100, 2) as total_weight
        FROM analytics_08_monthly_portfolio
        WHERE rebalance_date = %s
    """, (execution_date,))
    
    result = cursor.fetchone()
    stock_count, total_weight = result if result else (0, 0)
    
    logger.info(f"📊 월간 포트폴리오 검증 결과:")
    logger.info(f"  - 종목 수: {stock_count}")
    logger.info(f"  - 총 가중치: {total_weight}%")
    
    cursor.close()
    conn.close()
    
    # 경고만 출력 (실패시키지 않음)
    if stock_count == 0:
        logger.warning("⚠️ 포트폴리오가 비어있습니다. Spark job 확인 필요.")
    else:
        logger.info("✅ 월간 포트폴리오 검증 완료")


# DAG 정의
with DAG(
    dag_id='monthly_portfolio_rebalance',
    default_args=default_args,
    description='매월 마지막 일요일 포트폴리오 리밸런싱 (5d/10d/20d 통합)',
    schedule_interval='0 14 * * 0',  # 매주 일요일 14:00 UTC
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=['monthly', 'portfolio', 'spark', 'rebalance'],
    max_active_runs=1,
) as dag:

    # Task 1: 마지막 일요일 체크
    check_last_sunday = PythonOperator(
        task_id='check_last_sunday',
        python_callable=is_last_sunday_of_month,
        provide_context=True,
    )

    # Task 2: Spark 월간 포트폴리오 리밸런싱
    spark_rebalance = PythonOperator(
        task_id='spark_monthly_portfolio_rebalance',
        python_callable=run_spark_monthly_rebalance,
        provide_context=True,
        execution_timeout=timedelta(minutes=20),
    )

    # Task 3: 결과 검증
    validate_portfolio = PythonOperator(
        task_id='validate_monthly_portfolio',
        python_callable=validate_monthly_portfolio,
        provide_context=True,
    )

    # Task Dependencies
    check_last_sunday >> spark_rebalance >> validate_portfolio
