#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Spark Batch Job - Trending ETF Identifier
Identifies which ETFs are trending (outperforming SPY) for conditional holdings collection
Runs at 11:00 UTC after ETF OHLC data collection
Stores results in 03_analytics_trending_etfs table
"""

import os
import sys
from datetime import datetime, timedelta, date
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, '/opt/spark-apps')

from database.db_helper import DatabaseHelper
from utils.logging_utils import setup_logger

LOG = setup_logger(__name__, "spark-trending-etf-identifier.log")


class TrendingETFIdentifier:
    """Identify trending ETFs that outperform SPY"""
    
    def __init__(self):
        """Initialize Spark session and database connection"""
        self.spark = SparkSession.builder \
            .appName("Trending-ETF-Identifier") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.db = DatabaseHelper()
        
        # JDBC connection properties
        self.jdbc_url = self._build_jdbc_url()
        self.jdbc_properties = {
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "postgres"),
            "driver": "org.postgresql.Driver"
        }
        
        LOG.info("Trending ETF Identifier initialized")
    
    def _build_jdbc_url(self) -> str:
        """Build JDBC URL from environment"""
        host = os.getenv("DB_HOST", "postgres")
        port = os.getenv("DB_PORT", "5432")
        dbname = os.getenv("DB_NAME", "stockdb")
        return f"jdbc:postgresql://{host}:{port}/{dbname}"
    
    def load_etf_data(self, days: int = 20) -> DataFrame:
        """
        Load ETF OHLC data for SPY and all other ETFs
        
        Args:
            days: Number of days to load (default 20)
            
        Returns:
            DataFrame with ETF data
        """
        cutoff_date = (date.today() - timedelta(days=days + 5)).strftime('%Y-%m-%d')
        
        query = f"""(
            SELECT 
                e.ticker,
                e.trade_date,
                e.close_price,
                m.etf_type,
                m.sector_name
            FROM 01_collected_daily_etf_ohlc e
            LEFT JOIN 00_collected_meta_etf m ON e.ticker = m.ticker
            WHERE e.trade_date >= '{cutoff_date}'
            ORDER BY e.ticker, e.trade_date
        ) as etf_data"""
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.jdbc_properties
        )
        
        LOG.info("Loaded ETF data: %d records for %d-day period", df.count(), days)
        return df
    
    def calculate_etf_returns(self, df: DataFrame, period_days: int) -> DataFrame:
        """
        Calculate period returns for each ETF
        
        Args:
            df: DataFrame with ticker, trade_date, close_price
            period_days: Period length (5, 10, or 20)
            
        Returns:
            DataFrame with ticker, return_pct, etf_type, sector_name
        """
        # Window to order by date descending
        w_desc = Window.partitionBy("ticker").orderBy(F.col("trade_date").desc())
        
        # Limit to most recent N days per ticker
        limited = df.withColumn("row_num", F.row_number().over(w_desc)) \
                    .filter(F.col("row_num") <= period_days)
        
        # Get first and last close prices
        returns = limited.groupBy("ticker", "etf_type", "sector_name").agg(
            F.max("close_price").alias("last_close"),
            F.min("close_price").alias("first_close"),
            F.max("trade_date").alias("last_date"),
            F.min("trade_date").alias("first_date"),
            F.count("*").alias("day_count")
        )
        
        # Calculate return percentage
        returns = returns.withColumn(
            "return_pct",
            ((F.col("last_close") - F.col("first_close")) / F.col("first_close") * 100.0)
        )
        
        # Filter out tickers with insufficient data
        returns = returns.filter(F.col("day_count") >= (period_days * 0.7))  # At least 70% of days
        
        LOG.info("Calculated %d-day returns for %d ETFs", period_days, returns.count())
        return returns.select("ticker", "etf_type", "sector_name", "return_pct", "last_date")
    
    def identify_trending_etfs(self, etf_returns: DataFrame, as_of_date: date) -> DataFrame:
        """
        Identify ETFs that are trending (outperforming SPY and positive)
        
        Args:
            etf_returns: DataFrame with ETF returns
            as_of_date: Analysis date
            
        Returns:
            DataFrame with trending ETF flags
        """
        # Get SPY return
        spy_return = etf_returns.filter(F.col("ticker") == "SPY") \
                                .select("return_pct") \
                                .first()
        
        if not spy_return:
            LOG.error("SPY data not found!")
            raise ValueError("SPY benchmark data is required")
        
        spy_return_val = spy_return["return_pct"]
        LOG.info("SPY return: %.2f%%", spy_return_val)
        
        # Filter out SPY itself
        other_etfs = etf_returns.filter(F.col("ticker") != "SPY")
        
        # Mark trending: return > SPY AND return > 0
        trending = other_etfs.withColumn(
            "is_trending",
            (F.col("return_pct") > spy_return_val) & (F.col("return_pct") > 0)
        )
        
        # Add SPY return and date for reference
        trending = trending.withColumn("spy_return", F.lit(spy_return_val)) \
                          .withColumn("as_of_date", F.lit(as_of_date)) \
                          .withColumn("analysis_period_days", F.lit(20))
        
        trending_count = trending.filter(F.col("is_trending")).count()
        total_count = trending.count()
        
        LOG.info("=" * 70)
        LOG.info("Trending ETF Analysis Results:")
        LOG.info("  Total ETFs analyzed: %d", total_count)
        LOG.info("  Trending ETFs (> SPY & > 0%%): %d", trending_count)
        LOG.info("  SPY benchmark: %.2f%%", spy_return_val)
        LOG.info("=" * 70)
        
        # Show trending ETFs
        trending_etfs = trending.filter(F.col("is_trending")) \
                               .orderBy(F.col("return_pct").desc())
        
        if trending_count > 0:
            LOG.info("Trending ETFs (top performers):")
            for row in trending_etfs.collect():
                LOG.info("  %s (%s): %.2f%% (vs SPY: %.2f%%)",
                        row["ticker"],
                        row["sector_name"] if row["sector_name"] else "Benchmark",
                        row["return_pct"],
                        spy_return_val)
        else:
            LOG.warning("No trending ETFs found!")
        
        return trending.select(
            "as_of_date", "ticker", "etf_type", "sector_name",
            "return_pct", "spy_return", "is_trending", "analysis_period_days"
        )
    
    def save_trending_results(self, trending_df: DataFrame):
        """
        Save trending ETF results to database
        
        Args:
            trending_df: DataFrame with trending flags
        """
        if trending_df.count() == 0:
            LOG.warning("No trending results to save")
            return
        
        LOG.info("Saving trending ETF results to database...")
        
        # Convert to pandas for easier upsert
        trending_pd = trending_df.toPandas()
        
        for _, row in trending_pd.iterrows():
            upsert_query = """
                INSERT INTO 03_analytics_trending_etfs (
                    as_of_date, ticker, etf_type, sector_name,
                    return_pct, spy_return, is_trending, analysis_period_days
                ) VALUES (
                    %(as_of_date)s, %(ticker)s, %(etf_type)s, %(sector_name)s,
                    %(return_pct)s, %(spy_return)s, %(is_trending)s, %(analysis_period_days)s
                )
                ON CONFLICT (as_of_date, ticker)
                DO UPDATE SET
                    etf_type = EXCLUDED.etf_type,
                    sector_name = EXCLUDED.sector_name,
                    return_pct = EXCLUDED.return_pct,
                    spy_return = EXCLUDED.spy_return,
                    is_trending = EXCLUDED.is_trending,
                    analysis_period_days = EXCLUDED.analysis_period_days,
                    created_at = NOW()
            """
            
            self.db.execute_query(upsert_query, row.to_dict())
        
        trending_count = len(trending_pd[trending_pd['is_trending'] == True])
        LOG.info("Saved %d ETF results (%d trending)", len(trending_pd), trending_count)
    
    def run(self, as_of_date: Optional[date] = None):
        """
        Main execution logic
        
        Args:
            as_of_date: Analysis date (default: today)
        """
        if as_of_date is None:
            as_of_date = date.today()
        
        LOG.info("=" * 70)
        LOG.info("Trending ETF Identification")
        LOG.info("Analysis Date: %s", as_of_date)
        LOG.info("=" * 70)
        
        try:
            # Load ETF data (20-day period)
            etf_data = self.load_etf_data(days=20)
            
            if etf_data.count() == 0:
                LOG.error("No ETF data available!")
                raise ValueError("No ETF data found")
            
            # Calculate 20-day returns
            etf_returns = self.calculate_etf_returns(etf_data, period_days=20)
            
            # Identify trending ETFs
            trending_etfs = self.identify_trending_etfs(etf_returns, as_of_date)
            
            # Save results
            self.save_trending_results(trending_etfs)
            
            LOG.info("=" * 70)
            LOG.info("Trending ETF Identification Complete")
            LOG.info("=" * 70)
            
        except Exception as exc:
            LOG.error("Error in trending ETF identification: %s", exc, exc_info=True)
            raise
        finally:
            self.close()
    
    def close(self):
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
        if self.db:
            self.db.disconnect()
        LOG.info("Resources closed")


def main():
    """Main entry point"""
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    identifier = None
    try:
        identifier = TrendingETFIdentifier()
        identifier.run()
    except Exception as exc:
        LOG.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if identifier:
            identifier.close()


if __name__ == "__main__":
    main()
