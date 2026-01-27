"""
Spark Job: Monthly Portfolio Rebalancer
========================================
실행 스케줄: 매월 마지막 일요일
목적: 5일/10일/20일 포트폴리오를 비교·통합하여 다음 20영업일 동안 유지할 최종 월간 포트폴리오 생성

비교 로직:
1. 20일 vs 10일: 동일 종목은 가중치+1, 신규(10일만)는 rank=20부터
2. 20일 vs 5일: 동일 종목은 가중치+1, 신규는 다시 10일 vs 5일 비교
   - 10일∩5일: 가중치*2 (재차 가중)
   - 5일만: rank=20부터
3. 신규 종목이 2개 이상이면 기존 리스트 가중치 순으로 배치 (20위→19위→18위...)

입력: analytics_portfolio_allocation (period_days: 5, 10, 20)
출력: analytics_monthly_portfolio
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
import calendar
import logging

# Logging 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MonthlyPortfolioRebalancer:
    """매월 마지막 일요일 포트폴리오 리밸런싱"""
    
    def __init__(self, jdbc_url, db_user, db_password):
        self.jdbc_url = jdbc_url
        self.db_user = db_user
        self.db_password = db_password
        
        # Spark Session 생성
        self.spark = SparkSession.builder \
            .appName("MonthlyPortfolioRebalancer") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Session 초기화 완료")
    
    def is_last_sunday_of_month(self, check_date: datetime) -> bool:
        """주어진 날짜가 해당 월의 마지막 일요일인지 확인"""
        # 일요일인지 확인 (weekday: 0=월요일, 6=일요일)
        if check_date.weekday() != 6:
            return False
        
        # 다음 일요일이 다음 달인지 확인
        next_sunday = check_date + timedelta(days=7)
        return next_sunday.month != check_date.month
    
    def load_multi_period_portfolios(self, as_of_date: str):
        """
        5일/10일/20일 포트폴리오를 로드
        
        Args:
            as_of_date: 기준 날짜 (YYYY-MM-DD)
        
        Returns:
            dict: {5: df_5d, 10: df_10d, 20: df_20d}
        """
        logger.info(f"포트폴리오 로드 시작: {as_of_date}")
        
        portfolios = {}
        for period in [5, 10, 20]:
            query = f"""
                (SELECT 
                    ticker,
                    company_name,
                    sector,
                    return_20d,
                    portfolio_weight,
                    market_cap,
                    allocation_reason,
                    period_days,
                    as_of_date
                FROM analytics_portfolio_allocation
                WHERE period_days = {period}
                  AND as_of_date = '{as_of_date}'
                ) AS portfolio_{period}d
            """
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", query) \
                .option("user", self.db_user) \
                .option("password", self.db_password) \
                .load()
            
            count = df.count()
            logger.info(f"{period}일 포트폴리오: {count}개 종목")
            portfolios[period] = df
        
        return portfolios
    
    def compare_and_score(self, portfolios: dict) -> 'DataFrame':
        """
        포트폴리오 비교 및 가중치 점수 계산
        
        로직:
        1. 20일 리스트를 base로 시작 (base_weight = portfolio_weight)
        2. 20일 ∩ 10일: score +1
        3. 20일 ∩ 5일: score +1
        4. 10일만 존재 (20일 없음): score = 0.5, rank 20부터
        5. 5일만 존재:
           - 5일 ∩ 10일: score = 2.0 (재차 가중)
           - 5일만: score = 0.3, rank 20부터
        
        Returns:
            DataFrame: ticker, company_name, sector, final_score, final_weight, source_periods
        """
        logger.info("포트폴리오 비교 및 점수 계산 시작")
        
        df_20 = portfolios[20].withColumnRenamed("portfolio_weight", "weight_20") \
            .withColumnRenamed("return_20d", "return_20") \
            .withColumnRenamed("allocation_reason", "reason_20") \
            .select("ticker", "company_name", "sector", "market_cap", "weight_20", "return_20", "reason_20", "as_of_date")
        
        df_10 = portfolios[10].withColumnRenamed("portfolio_weight", "weight_10") \
            .select("ticker", "weight_10")
        
        df_5 = portfolios[5].withColumnRenamed("portfolio_weight", "weight_5") \
            .select("ticker", "weight_5")
        
        # Step 1: 20일 기준으로 10일, 5일 조인
        result = df_20.alias("d20") \
            .join(df_10.alias("d10"), "ticker", "left") \
            .join(df_5.alias("d5"), "ticker", "left")
        
        # Step 2: 점수 계산
        result = result.withColumn("score",
            F.when(F.col("weight_20").isNotNull(), 1.0)  # 20일 기본 점수
            .otherwise(0.0)
        )
        
        # 20일 ∩ 10일: +1
        result = result.withColumn("score",
            F.when((F.col("weight_20").isNotNull()) & (F.col("weight_10").isNotNull()),
                   F.col("score") + 1.0)
            .otherwise(F.col("score"))
        )
        
        # 20일 ∩ 5일: +1
        result = result.withColumn("score",
            F.when((F.col("weight_20").isNotNull()) & (F.col("weight_5").isNotNull()),
                   F.col("score") + 1.0)
            .otherwise(F.col("score"))
        )
        
        # Step 3: 10일만 존재 (20일 없음) - 별도 처리 필요
        df_10_only = df_10.join(df_20.select("ticker"), "ticker", "left_anti")
        df_10_only = df_10_only.join(
            portfolios[10].select("ticker", "company_name", "sector", "market_cap", "return_20d", "allocation_reason", "as_of_date"),
            "ticker"
        )
        df_10_only = df_10_only.withColumn("score", F.lit(0.5)) \
            .withColumn("weight_20", F.lit(None).cast("double")) \
            .withColumn("weight_5", F.lit(None).cast("double")) \
            .withColumn("return_20", F.col("return_20d")) \
            .withColumn("reason_20", F.col("allocation_reason")) \
            .select("ticker", "company_name", "sector", "market_cap", "weight_20", "weight_10", "weight_5", "return_20", "reason_20", "as_of_date", "score")
        
        # Step 4: 5일만 존재 처리
        df_5_only = df_5.join(df_20.select("ticker"), "ticker", "left_anti")
        
        # 5일 ∩ 10일 체크
        df_5_and_10 = df_5_only.join(df_10.select("ticker", "weight_10"), "ticker", "inner")
        df_5_and_10 = df_5_and_10.join(
            portfolios[5].select("ticker", "company_name", "sector", "market_cap", "return_20d", "allocation_reason", "as_of_date"),
            "ticker"
        )
        df_5_and_10 = df_5_and_10.withColumn("score", F.lit(2.0)) \
            .withColumn("weight_20", F.lit(None).cast("double")) \
            .withColumn("return_20", F.col("return_20d")) \
            .withColumn("reason_20", F.col("allocation_reason")) \
            .select("ticker", "company_name", "sector", "market_cap", "weight_20", "weight_10", "weight_5", "return_20", "reason_20", "as_of_date", "score")
        
        # 5일만 (10일, 20일 모두 없음)
        df_5_only_pure = df_5_only.join(df_10.select("ticker"), "ticker", "left_anti")
        df_5_only_pure = df_5_only_pure.join(
            portfolios[5].select("ticker", "company_name", "sector", "market_cap", "return_20d", "allocation_reason", "as_of_date"),
            "ticker"
        )
        df_5_only_pure = df_5_only_pure.withColumn("score", F.lit(0.3)) \
            .withColumn("weight_20", F.lit(None).cast("double")) \
            .withColumn("weight_10", F.lit(None).cast("double")) \
            .withColumn("return_20", F.col("return_20d")) \
            .withColumn("reason_20", F.col("allocation_reason")) \
            .select("ticker", "company_name", "sector", "market_cap", "weight_20", "weight_10", "weight_5", "return_20", "reason_20", "as_of_date", "score")
        
        # 모든 결과 합치기
        final_result = result.unionByName(df_10_only) \
            .unionByName(df_5_and_10) \
            .unionByName(df_5_only_pure)
        
        # source_periods 생성
        final_result = final_result.withColumn("source_periods",
            F.concat_ws(",",
                F.when(F.col("weight_20").isNotNull(), F.lit("20d")).otherwise(F.lit(None)),
                F.when(F.col("weight_10").isNotNull(), F.lit("10d")).otherwise(F.lit(None)),
                F.when(F.col("weight_5").isNotNull(), F.lit("5d")).otherwise(F.lit(None))
            )
        )
        
        logger.info(f"비교 완료: 총 {final_result.count()}개 종목")
        return final_result
    
    def assign_ranks_and_weights(self, scored_df: 'DataFrame') -> 'DataFrame':
        """
        점수 기반 순위 배정 및 최종 가중치 계산
        
        로직:
        1. score 내림차순 정렬
        2. 신규 종목(weight_20 = null)은 score 순으로 20위부터 배치
        3. 최종 가중치 정규화 (합계 = 1.0)
        
        Returns:
            DataFrame: ticker, final_rank, final_weight, score
        """
        logger.info("순위 배정 및 가중치 계산 시작")
        
        # Window for ranking
        window_spec = Window.orderBy(F.col("score").desc(), F.col("return_20").desc())
        
        ranked = scored_df.withColumn("overall_rank", F.row_number().over(window_spec))
        
        # 신규 종목 처리: weight_20이 null이면 최소 20위부터
        ranked = ranked.withColumn("is_new_stock",
            F.when(F.col("weight_20").isNull(), F.lit(1)).otherwise(F.lit(0))
        )
        
        # 기존 종목과 신규 종목 분리
        existing_stocks = ranked.filter(F.col("is_new_stock") == 0)
        new_stocks = ranked.filter(F.col("is_new_stock") == 1)
        
        # 신규 종목은 20위 이하로 재배치
        new_window = Window.orderBy(F.col("score").desc(), F.col("return_20").desc())
        new_stocks = new_stocks.withColumn("adjusted_rank",
            F.row_number().over(new_window) + 19  # 20위부터 시작
        )
        
        existing_stocks = existing_stocks.withColumn("adjusted_rank", F.col("overall_rank"))
        
        # 재합치기
        final_ranked = existing_stocks.unionByName(new_stocks)
        
        # 최종 순위 재정렬
        final_window = Window.orderBy(F.col("adjusted_rank"))
        final_ranked = final_ranked.withColumn("final_rank", F.row_number().over(final_window))
        
        # 가중치 계산: score 기반 정규화
        total_score = final_ranked.agg(F.sum("score")).collect()[0][0]
        
        final_ranked = final_ranked.withColumn("final_weight",
            F.col("score") / F.lit(total_score)
        )
        
        # 상위 20개만 선택 (또는 전체)
        final_portfolio = final_ranked.orderBy("final_rank").limit(20)
        
        # 가중치 재정규화
        actual_total = final_portfolio.agg(F.sum("final_weight")).collect()[0][0]
        final_portfolio = final_portfolio.withColumn("final_weight",
            F.col("final_weight") / F.lit(actual_total)
        )
        
        logger.info(f"최종 포트폴리오: {final_portfolio.count()}개 종목")
        return final_portfolio
    
    def save_monthly_portfolio(self, portfolio_df: 'DataFrame'):
        """
        최종 월간 포트폴리오를 analytics_monthly_portfolio 테이블에 저장
        
        Schema:
        - ticker, company_name, sector
        - final_rank, final_weight
        - score, source_periods
        - return_20d, market_cap
        - rebalance_date, valid_until
        """
        logger.info("월간 포트폴리오 저장 시작")
        
        # rebalance_date = as_of_date
        # valid_until = rebalance_date + 20 영업일 (약 28일)
        portfolio_df = portfolio_df.withColumn("rebalance_date", F.col("as_of_date"))
        portfolio_df = portfolio_df.withColumn("valid_until",
            F.date_add(F.col("as_of_date"), 28)  # 20 영업일 ≈ 28 달력일
        )
        
        # 최종 컬럼 선택
        final_columns = [
            "ticker", "company_name", "sector",
            "final_rank", "final_weight", "score", "source_periods",
            F.col("return_20").alias("return_20d"),
            "market_cap",
            F.col("reason_20").alias("allocation_reason"),
            "rebalance_date", "valid_until"
        ]
        
        output_df = portfolio_df.select(*final_columns)
        
        # 기존 데이터 삭제 (같은 rebalance_date)
        rebalance_date = output_df.select("rebalance_date").first()[0]
        
        with self.spark._jvm.java.sql.DriverManager.getConnection(
            self.jdbc_url, self.db_user, self.db_password
        ) as conn:
            stmt = conn.createStatement()
            delete_query = f"DELETE FROM analytics_monthly_portfolio WHERE rebalance_date = '{rebalance_date}'"
            deleted = stmt.executeUpdate(delete_query)
            logger.info(f"기존 데이터 삭제: {deleted}건")
        
        # 새 데이터 저장
        output_df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", "analytics_monthly_portfolio") \
            .option("user", self.db_user) \
            .option("password", self.db_password) \
            .mode("append") \
            .save()
        
        logger.info(f"월간 포트폴리오 저장 완료: {output_df.count()}건")
        
        # 결과 출력
        output_df.orderBy("final_rank").show(20, truncate=False)
    
    def run(self, as_of_date: str = None):
        """
        메인 실행 함수
        
        Args:
            as_of_date: 기준 날짜 (YYYY-MM-DD). None이면 오늘 날짜 사용
        """
        try:
            # 날짜 검증
            if as_of_date is None:
                target_date = datetime.now()
            else:
                target_date = datetime.strptime(as_of_date, "%Y-%m-%d")
            
            logger.info(f"=== 월간 포트폴리오 리밸런싱 시작: {target_date.strftime('%Y-%m-%d')} ===")
            
            # 마지막 일요일 체크
            if not self.is_last_sunday_of_month(target_date):
                logger.warning(f"{target_date.strftime('%Y-%m-%d')}는 해당 월의 마지막 일요일이 아닙니다. 실행을 건너뜁니다.")
                return
            
            as_of_str = target_date.strftime("%Y-%m-%d")
            
            # 1. 포트폴리오 로드
            portfolios = self.load_multi_period_portfolios(as_of_str)
            
            if any(df.count() == 0 for df in portfolios.values()):
                logger.error("하나 이상의 포트폴리오가 비어있습니다. 실행을 중단합니다.")
                return
            
            # 2. 비교 및 점수 계산
            scored_portfolio = self.compare_and_score(portfolios)
            
            # 3. 순위 배정 및 가중치 계산
            final_portfolio = self.assign_ranks_and_weights(scored_portfolio)
            
            # 4. 저장
            self.save_monthly_portfolio(final_portfolio)
            
            logger.info("=== 월간 포트폴리오 리밸런싱 완료 ===")
            
        except Exception as e:
            logger.error(f"실행 중 오류 발생: {str(e)}", exc_info=True)
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    import os
    
    # 환경 변수
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "finviz_stock_db")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "finvizuser")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "finvizpass")
    
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    # 실행
    rebalancer = MonthlyPortfolioRebalancer(
        jdbc_url=jdbc_url,
        db_user=POSTGRES_USER,
        db_password=POSTGRES_PASSWORD
    )
    
    # 오늘 날짜 또는 명령행 인자로 날짜 지정
    import sys
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    
    rebalancer.run(as_of_date=target_date)
