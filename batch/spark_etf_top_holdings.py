#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Spark Batch Job - Calculate Top 5 ETF Holdings by Performance
Analyzes ETF holdings performance over 5, 10, and 20-day windows
Stores results in analytics_etf_top_holdings table
"""

import os
import sys
from datetime import datetime, timedelta, date
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, '/opt/spark-apps')

from database.db_helper import DatabaseHelper
from utils.logging_utils import setup_logger

LOG = setup_logger(__name__, "spark-etf-top-holdings.log")


class ETFTopHoldingsAnalyzer:
    """Calculate top 5 performing holdings for each ETF over different time windows"""
    
    def __init__(self):
        """Initialize Spark session and database connection"""
        self.spark = SparkSession.builder \
            .appName("ETF-Top-Holdings-Analyzer") \
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
        
        LOG.info("Spark ETF Top Holdings Analyzer initialized")
    
    def _build_jdbc_url(self) -> str:
        """Build JDBC URL from environment"""
        host = os.getenv("DB_HOST", "postgres")
        port = os.getenv("DB_PORT", "5432")
        dbname = os.getenv("DB_NAME", "stockdb")
        return f"jdbc:postgresql://{host}:{port}/{dbname}"
    
    def load_etf_metadata(self, etf_type_filter: str = None) -> DataFrame:
        """
        Load ETF metadata with optional type filtering
        
        Args:
            etf_type_filter: Optional filter for 'benchmark' or 'sector'
        """
        if etf_type_filter:
            query = f"(SELECT ticker, etf_type, sector_name FROM collected_meta_etf WHERE etf_type = '{etf_type_filter}') as etf_meta"
        else:
            query = "(SELECT ticker, etf_type, sector_name FROM collected_meta_etf) as etf_meta"
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.jdbc_properties
        )
        LOG.info("Loaded %d ETF metadata records (filter: %s)", df.count(), etf_type_filter or 'none')
        return df
    
    def load_stock_history(self, days: int) -> DataFrame:
        """
        Load stock history for the past N days
        
        Args:
            days: Number of days to load
            
        Returns:
            DataFrame with stock history
        """
        cutoff_date = (date.today() - timedelta(days=days + 5)).strftime('%Y-%m-%d')
        
        query = f"""(
            SELECT 
                ticker,
                company_name,
                sector,
                trade_date,
                close_price,
                volume,
                price_change_percent,
                market_cap
            FROM collected_daily_stock_history
            WHERE trade_date >= '{cutoff_date}'
        ) as stock_history"""
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.jdbc_properties
        )
        
        LOG.info("Loaded %d stock history records since %s", df.count(), cutoff_date)
        return df
    
    def load_etf_holdings(self) -> DataFrame:
        """
        Load ETF holdings from collected_etf_holdings
        Uses the most recent holdings data available
        """
        query = """(
            SELECT DISTINCT ON (etf_ticker, holding_ticker)
                etf_ticker,
                holding_ticker,
                holding_name,
                holding_percent,
                as_of_date
            FROM collected_etf_holdings
            ORDER BY etf_ticker, holding_ticker, as_of_date DESC
        ) as holdings"""
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.jdbc_properties
        )
        
        LOG.info("Loaded %d ETF holdings records", df.count())
        return df
    
    def calculate_stock_metrics(self, stock_df: DataFrame, period_days: int, as_of_date: date) -> DataFrame:
        """
        Calculate performance metrics for each stock over a period
        
        Args:
            stock_df: Stock history DataFrame
            period_days: Number of days to analyze (5, 10, or 20)
            as_of_date: Calculation date
            
        Returns:
            DataFrame with stock metrics
        """
        # Filter to period window
        start_date = (as_of_date - timedelta(days=period_days + 5)).strftime('%Y-%m-%d')
        end_date = as_of_date.strftime('%Y-%m-%d')
        
        period_df = stock_df.filter(
            (F.col("trade_date") >= start_date) &
            (F.col("trade_date") <= end_date)
        )
        
        # Calculate metrics per ticker
        window_spec = Window.partitionBy("ticker").orderBy("trade_date")
        
        metrics_df = period_df.groupBy("ticker").agg(
            F.first("company_name").alias("holding_name"),
            F.first("sector").alias("holding_sector"),
            F.last("close_price").alias("current_price"),
            F.last("market_cap").alias("market_cap"),
            F.avg("price_change_percent").alias("avg_return"),
            F.stddev("price_change_percent").alias("volatility"),
            F.avg("volume").alias("avg_volume"),
            F.count("*").alias("trading_days"),
            # Total return calculation
            F.expr("(last(close_price) - first(close_price)) / first(close_price) * 100").alias("total_return")
        ).filter(F.col("trading_days") >= period_days * 0.7)  # At least 70% of trading days
        
        LOG.info("Calculated metrics for %d stocks over %d days", metrics_df.count(), period_days)
        return metrics_df
    
    def rank_holdings_per_etf(
        self,
        etf_meta_df: DataFrame,
        holdings_df: DataFrame,
        metrics_df: DataFrame,
        period_days: int,
        as_of_date: date
    ) -> DataFrame:
        """
        Rank top 5 holdings for each ETF based on performance
        
        Args:
            etf_meta_df: ETF metadata
            holdings_df: ETF holdings
            metrics_df: Stock performance metrics
            period_days: Time period (5, 10, or 20 days)
            as_of_date: Calculation date
            
        Returns:
            DataFrame with top 5 ranked holdings per ETF
        """
        # Join holdings with ETF metadata
        etf_holdings_df = holdings_df.join(
            etf_meta_df,
            holdings_df.etf_ticker == etf_meta_df.ticker,
            "inner"
        ).select(
            holdings_df.etf_ticker,
            etf_meta_df.etf_type,
            etf_meta_df.sector_name,
            holdings_df.holding_ticker,
            holdings_df.holding_name.alias("holdings_name"),
            holdings_df.holding_percent
        )
        
        # Join with stock metrics
        combined_df = etf_holdings_df.join(
            metrics_df,
            etf_holdings_df.holding_ticker == metrics_df.ticker,
            "inner"
        )
        
        # Rank by total return within each ETF
        window_rank = Window.partitionBy("etf_ticker").orderBy(F.desc("total_return"))
        
        ranked_df = combined_df.withColumn("rank_position", F.row_number().over(window_rank))
        
        # Keep only top 5
        top5_df = ranked_df.filter(F.col("rank_position") <= 5)
        
        # Add metadata
        result_df = top5_df.select(
            F.col("etf_ticker"),
            F.col("etf_type"),
            F.col("sector_name"),
            F.col("holding_ticker"),
            F.coalesce(F.col("holding_name"), F.col("holdings_name")).alias("holding_name"),
            F.col("holding_sector"),
            F.lit(period_days).alias("time_period"),
            F.col("rank_position"),
            F.col("avg_return"),
            F.col("total_return"),
            F.col("volatility"),
            F.col("avg_volume").cast("long"),
            F.col("current_price"),
            F.col("market_cap").cast("long"),
            F.lit(as_of_date.strftime('%Y-%m-%d')).cast("date").alias("as_of_date")
        )
        
        LOG.info("Ranked top 5 holdings for period %d days: %d records", period_days, result_df.count())
        return result_df
    
    def save_results(self, results_df: DataFrame):
        """Save results to analytics_etf_top_holdings table"""
        try:
            results_df.write.jdbc(
                url=self.jdbc_url,
                table="analytics_etf_top_holdings",
                mode="append",
                properties=self.jdbc_properties
            )
            LOG.info("Saved %d top holdings records to database", results_df.count())
        except Exception as exc:
            LOG.error("Error saving results: %s", exc)
            raise
    
    def cleanup_old_data(self, as_of_date: date):
        """Remove duplicate entries for the same date before inserting new ones"""
        delete_query = f"""
            DELETE FROM analytics_etf_top_holdings
            WHERE as_of_date = '{as_of_date.strftime('%Y-%m-%d')}'
        """
        try:
            self.db.execute_query(delete_query)
            LOG.info("Cleaned up old data for date %s", as_of_date)
        except Exception as exc:
            LOG.warning("Error cleaning up old data: %s", exc)
    
    def run(self, as_of_date: date = None, etf_type_filter: str = None):
        """
        Main execution: Calculate top holdings for all time periods
        
        Args:
            as_of_date: Date for calculation (default: today)
            etf_type_filter: Optional ETF type filter ('benchmark' or 'sector')
        """
        if as_of_date is None:
            as_of_date = date.today()
        
        LOG.info("=" * 70)
        LOG.info("ETF Top Holdings Analysis - %s", as_of_date)
        if etf_type_filter:
            LOG.info("ETF Type Filter: %s", etf_type_filter)
        LOG.info("=" * 70)
        
        try:
            # Load data
            LOG.info("Loading data...")
            etf_meta_df = self.load_etf_metadata(etf_type_filter)
            holdings_df = self.load_etf_holdings()
            
            # Cleanup old data for this date
            self.cleanup_old_data(as_of_date)
            
            # Process each time period
            time_periods = [5, 10, 20]
            
            for period_days in time_periods:
                LOG.info("-" * 70)
                LOG.info("Processing %d-day period", period_days)
                LOG.info("-" * 70)
                
                # Load stock history for this period
                stock_df = self.load_stock_history(period_days)
                
                # Calculate metrics
                metrics_df = self.calculate_stock_metrics(stock_df, period_days, as_of_date)
                
                # Rank holdings
                top5_df = self.rank_holdings_per_etf(
                    etf_meta_df,
                    holdings_df,
                    metrics_df,
                    period_days,
                    as_of_date
                )
                
                # Save results
                self.save_results(top5_df)
                
                LOG.info("Completed %d-day analysis", period_days)
            
            LOG.info("=" * 70)
            LOG.info("ETF Top Holdings Analysis Complete")
            LOG.info("=" * 70)
            
        except Exception as exc:
            LOG.error("Error in analysis: %s", exc, exc_info=True)
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
    # Check for environment variable to filter by ETF type
    etf_type_filter = os.getenv('ETF_TYPE_FILTER')  # 'benchmark' or 'sector'
    
    analyzer = None
    try:
        analyzer = ETFTopHoldingsAnalyzer()
        analyzer.run(etf_type_filter=etf_type_filter
    try:
        analyzer = ETFTopHoldingsAnalyzer()
        analyzer.run()
    except Exception as exc:
        LOG.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if analyzer:
            analyzer.close()


if __name__ == "__main__":
    main()
