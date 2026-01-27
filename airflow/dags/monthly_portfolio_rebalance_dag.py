"""
Airflow DAG: Monthly Portfolio Rebalance
=========================================
실행 스케줄: 매월 마지막 일요일 14:00 UTC (23:00 KST)
목적: 5일/10일/20일 포트폴리오를 통합하여 다음 20영업일 동안 유지할 최종 월간 포트폴리오 생성

Dependencies:
- benchmark_data_daily_dag (09:00, 10:00 UTC)
- monthly_sector_trending_dag (11:00 UTC)
- etf_holdings_daily_dag (12:00 UTC)
- Spark job (13:00 UTC)
→ 모든 일일 파이프라인 완료 후 실행

Output:
- analytics_monthly_portfolio 테이블
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import calendar

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
    
    Returns:
        bool: True면 계속 진행, False면 DAG 중단
    
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
    
    print(f"✅ {target_date}는 {target_date.strftime('%Y년 %m월')}의 마지막 일요일입니다. 리밸런싱을 진행합니다.")
    return True


# DAG 정의
with DAG(
    dag_id='monthly_portfolio_rebalance',
    default_args=default_args,
    description='매월 마지막 일요일 포트폴리오 리밸런싱 (5d/10d/20d 통합)',
    schedule_interval='0 14 * * 0',  # 매주 일요일 14:00 UTC (23:00 KST) - 실제로는 마지막 일요일만 실행
    start_date=days_ago(1),
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
    spark_monthly_rebalance = SparkSubmitOperator(
        task_id='spark_monthly_portfolio_rebalance',
        application='/opt/spark-apps/batch/spark_monthly_portfolio_rebalancer.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
            'spark.executor.cores': '2',
            'spark.sql.adaptive.enabled': 'true',
            'spark.jars': '/opt/spark/jars/postgresql-42.7.4.jar',
        },
        application_args=['{{ ds }}'],  # as_of_date 전달
        verbose=True,
        driver_class_path='/opt/spark/jars/postgresql-42.7.4.jar',
    )

    # Task 3: 결과 검증 (옵션)
    def validate_monthly_portfolio(**context):
        """월간 포트폴리오 생성 결과 검증"""
        import psycopg2
        import os
        
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            dbname=os.getenv('POSTGRES_DB', 'finviz_stock_db'),
            user=os.getenv('POSTGRES_USER', 'finvizuser'),
            password=os.getenv('POSTGRES_PASSWORD', 'finvizpass')
        )
        
        cursor = conn.cursor()
        execution_date = context['ds']
        
        # 생성된 포트폴리오 개수 확인
        cursor.execute("""
            SELECT 
                COUNT(*) as stock_count,
                ROUND(SUM(final_weight) * 100, 2) as total_weight,
                MIN(final_rank) as min_rank,
                MAX(final_rank) as max_rank
            FROM analytics_monthly_portfolio
            WHERE rebalance_date = %s
        """, (execution_date,))
        
        result = cursor.fetchone()
        stock_count, total_weight, min_rank, max_rank = result
        
        print(f"📊 월간 포트폴리오 검증 결과:")
        print(f"  - 종목 수: {stock_count}")
        print(f"  - 총 가중치: {total_weight}%")
        print(f"  - 순위 범위: {min_rank} ~ {max_rank}")
        
        # 검증: 종목 수가 10개 이상, 총 가중치 95% 이상
        if stock_count < 10:
            raise Exception(f"❌ 종목 수 부족: {stock_count}개 (최소 10개 필요)")
        
        if total_weight < 95.0:
            raise Exception(f"❌ 총 가중치 부족: {total_weight}% (최소 95% 필요)")
        
        # 상위 10개 종목 출력
        cursor.execute("""
            SELECT final_rank, ticker, company_name, 
                   ROUND(final_weight * 100, 2) as weight_pct,
                   score, source_periods
            FROM analytics_monthly_portfolio
            WHERE rebalance_date = %s
            ORDER BY final_rank
            LIMIT 10
        """, (execution_date,))
        
        print("\n📈 상위 10개 종목:")
        for row in cursor.fetchall():
            rank, ticker, name, weight, score, periods = row
            print(f"  {rank:2d}. {ticker:5s} | {name:30s} | {weight:5.2f}% | score={score:.1f} | {periods}")
        
        cursor.close()
        conn.close()
        
        print("\n✅ 월간 포트폴리오 검증 완료")
    
    validate_portfolio = PythonOperator(
        task_id='validate_monthly_portfolio',
        python_callable=validate_monthly_portfolio,
        provide_context=True,
    )

    # Task Dependencies
    check_last_sunday >> spark_monthly_rebalance >> validate_portfolio
