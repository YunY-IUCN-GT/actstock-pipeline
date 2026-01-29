#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Spark Batch Job - Active Stock Portfolio Allocator (5-Stage Workflow)
Uses trending ETFs identified in Stage 3 (11:00 UTC) to build portfolio allocations.
Calculates multi-period allocations (5d, 10d, 20d):
  - Reads trending ETFs from 03_analytics_trending_etfs (Stage 3 output)
  - For each trending ETF: selects TOP 1 best-performing stock from holdings
  - Weights by: Performance × Inverse Market Cap
  - Produces 3 separate portfolios: 5-day, 10-day, 20-day windows
Only uses existing PostgreSQL data - NO API calls to yfinance
"""

import os
import sys
from datetime import datetime, timedelta, date
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, '/opt/spark-apps')

from database.db_helper import DatabaseHelper
from utils.logging_utils import setup_logger

LOG = setup_logger(__name__, "spark-portfolio-allocator.log")

# Exclude known placeholder tickers from mock data
EXCLUDE_TICKER_REGEX = os.getenv("EXCLUDE_TICKER_REGEX", r"(?i)^STOCK\\d+$")
EXCLUDE_COMPANY_PREFIXES = [
    p.strip().upper()
    for p in os.getenv("EXCLUDE_COMPANY_PREFIXES", "STOCK ").split(",")
    if p.strip()
]


class ActiveStockAllocator:
    """Calculate trending sectors, stocks, and portfolio allocation"""
    
    def __init__(self):
        """Initialize Spark session and database connection"""
        self.spark = SparkSession.builder \
            .appName("Active-Stock-Portfolio-Allocator") \
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
        
        LOG.info("Active Stock Allocator initialized")
    
    def _build_jdbc_url(self) -> str:
        """Build JDBC URL from environment"""
        host = os.getenv("DB_HOST", "postgres")
        port = os.getenv("DB_PORT", "5432")
        dbname = os.getenv("DB_NAME", "stockdb")
        return f"jdbc:postgresql://{host}:{port}/{dbname}"
    
    def load_trending_etfs(self, as_of_date: date) -> DataFrame:
        """
        Load trending ETFs from Stage 3 analysis (11:00 UTC)
        
        Args:
            as_of_date: Analysis date
            
        Returns:
            DataFrame with trending ETF tickers
        """
        query = f"""(
            SELECT 
                etf_ticker,
                return_pct as return_20d
            FROM analytics_03_trending_etfs
            WHERE as_of_date = '{as_of_date}'
              AND is_trending = TRUE
            ORDER BY return_pct DESC
        ) as trending_etfs"""
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.jdbc_properties
        )
        
        trending_count = df.count()
        LOG.info("Loaded %d trending ETFs for %s", trending_count, as_of_date)
        
        if trending_count == 0:
            LOG.warning("No trending ETFs found - check Stage 3 execution (11:00 UTC)")
        
        return df
    
    def load_etf_data(self, days: int) -> DataFrame:
        """
        Load ETF OHLC data for SPY and sector ETFs
        
        Args:
            days: Number of days to load
            
        Returns:
            DataFrame with ETF data
        """
        cutoff_date = (date.today() - timedelta(days=days + 5)).strftime('%Y-%m-%d')
        
        query = f"""(
            SELECT 
                ticker,
                trade_date,
                close_price
            FROM collected_01_daily_etf_ohlc
            WHERE trade_date >= '{cutoff_date}'
            AND ticker IN ('SPY', 'XLK', 'XLV', 'XLF', 'XLY', 'XLC', 'XLI', 'XLP', 'XLE', 'XLU', 'XLRE', 'XLB')
            ORDER BY ticker, trade_date
        ) as etf_data"""
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.jdbc_properties
        )
        
        LOG.info("Loaded ETF data: %d records for %d-day period", df.count(), days)
        return df
    
    def load_stock_data(self, days: int) -> DataFrame:
        """
        Load stock history data
        
        Args:
            days: Number of days to load
            
        Returns:
            DataFrame with stock data
        """
        cutoff_date = (date.today() - timedelta(days=days + 5)).strftime('%Y-%m-%d')
        
        query = f"""(
            SELECT 
                ticker,
                company_name,
                sector,
                trade_date,
                close_price,
                market_cap
            FROM collected_06_daily_stock_history
            WHERE trade_date >= '{cutoff_date}'
            AND sector IS NOT NULL
            AND sector != 'Unknown'
            ORDER BY ticker, trade_date
        ) as stock_data"""
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.jdbc_properties
        )

        df = self._filter_invalid_tickers(df, ticker_col="ticker", name_col="company_name")

        LOG.info("Loaded stock data: %d records for %d-day period", df.count(), days)
        return df

    def _filter_invalid_tickers(self, df: DataFrame, ticker_col: str, name_col: Optional[str] = None) -> DataFrame:
        """Filter out placeholder/mock tickers and company names."""
        filtered = df.filter(~F.col(ticker_col).rlike(EXCLUDE_TICKER_REGEX))
        if name_col and name_col in df.columns and EXCLUDE_COMPANY_PREFIXES:
            for prefix in EXCLUDE_COMPANY_PREFIXES:
                filtered = filtered.filter(~F.upper(F.col(name_col)).startswith(prefix))
        return filtered
    
    def calculate_returns(self, df: DataFrame, period_days: int) -> DataFrame:
        """
        Calculate period returns for each ticker
        
        Args:
            df: DataFrame with ticker, trade_date, close_price
            period_days: Period length (5, 10, or 20)
            
        Returns:
            DataFrame with ticker and return_pct
        """
        # Window to order by date descending
        w_desc = Window.partitionBy("ticker").orderBy(F.col("trade_date").desc())
        
        # Limit to most recent N days per ticker
        limited = df.withColumn("row_num", F.row_number().over(w_desc)) \
                    .filter(F.col("row_num") <= period_days)
        
        # Get first and last close prices
        w_part = Window.partitionBy("ticker")
        
        returns = limited.groupBy("ticker").agg(
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
        
        LOG.info("Calculated %d-day returns for %d tickers", period_days, returns.count())
        return returns.select("ticker", "return_pct", "last_date")
    
    def identify_trending_sectors(self, etf_returns: DataFrame, period_days: int, as_of_date: date) -> DataFrame:
        """
        Identify trending sectors that outperform SPY and are positive
        
        Args:
            etf_returns: DataFrame with ETF returns
            period_days: Period length
            as_of_date: Analysis date
            
        Returns:
            DataFrame with sector trending data
        """
        # Get SPY return
        spy_return = etf_returns.filter(F.col("ticker") == "SPY") \
                                .select("return_pct") \
                                .first()
        
        spy_return_val = spy_return["return_pct"] if spy_return else 0.0
        LOG.info("SPY %d-day return: %.2f%%", period_days, spy_return_val)
        
        # Map sector ETFs to sector names
        sector_map = {
            'XLK': 'Technology',
            'XLV': 'Healthcare',
            'XLF': 'Financial',
            'XLY': 'Consumer Cyclical',
            'XLC': 'Communication',
            'XLI': 'Industrial',
            'XLP': 'Consumer Defensive',
            'XLE': 'Energy',
            'XLU': 'Utilities',
            'XLRE': 'Real Estate',
            'XLB': 'Basic Materials'
        }
        
        # Filter sector ETFs only
        sector_etfs = etf_returns.filter(F.col("ticker") != "SPY")
        
        # Add sector names
        mapping_expr = F.create_map([F.lit(x) for pair in sector_map.items() for x in pair])
        sector_etfs = sector_etfs.withColumn("sector", mapping_expr[F.col("ticker")])
        
        # Mark trending sectors (return > SPY and return > 0)
        sector_etfs = sector_etfs.withColumn(
            "is_trending",
            (F.col("return_pct") > spy_return_val) & (F.col("return_pct") > 0)
        )
        
        # Add period and date info
        sector_etfs = sector_etfs.withColumn("period_days", F.lit(period_days)) \
                                 .withColumn("as_of_date", F.lit(as_of_date)) \
                                 .withColumn("spy_return", F.lit(spy_return_val))
        
        trending_count = sector_etfs.filter(F.col("is_trending")).count()
        LOG.info("Trending sectors (%d-day): %d out of %d", 
                period_days, trending_count, sector_etfs.count())
        
        return sector_etfs.select(
            "as_of_date", "sector", "return_pct", "spy_return", 
            "period_days", "is_trending"
        )
    
    def identify_trending_stocks(self, stock_returns: DataFrame, sector_trending: DataFrame, 
                                 stock_data: DataFrame, period_days: int, as_of_date: date) -> DataFrame:
        """
        Identify trending stocks that outperform their sector average
        
        Args:
            stock_returns: DataFrame with stock returns
            sector_trending: DataFrame with trending sectors
            stock_data: Original stock data with sector info
            period_days: Period length
            as_of_date: Analysis date
            
        Returns:
            DataFrame with stock trending data
        """
        # Get trending sectors only
        trending_sectors = sector_trending.filter(F.col("is_trending")) \
                                          .select("sector", F.col("return_pct").alias("sector_avg"))
        
        if trending_sectors.count() == 0:
            LOG.warning("No trending sectors found for %d-day period", period_days)
            return self.spark.createDataFrame([], schema="ticker string, sector string")
        
        # Get latest sector for each stock
        w_latest = Window.partitionBy("ticker").orderBy(F.col("trade_date").desc())
        stock_sectors = stock_data.withColumn("rn", F.row_number().over(w_latest)) \
                                  .filter(F.col("rn") == 1) \
                                  .select("ticker", "company_name", "sector", "market_cap")
        
        # Join stock returns with sector info
        stock_with_sector = stock_returns.join(stock_sectors, "ticker", "inner")
        
        # Join with trending sectors
        stock_with_trending = stock_with_sector.join(trending_sectors, "sector", "inner")
        
        # Mark stocks trending above their sector average
        stock_with_trending = stock_with_trending.withColumn(
            "is_trending",
            F.col("return_pct") > F.col("sector_avg")
        )
        
        # Add period and date info
        stock_with_trending = stock_with_trending.withColumn("period_days", F.lit(period_days)) \
                                                 .withColumn("as_of_date", F.lit(as_of_date))
        
        trending_count = stock_with_trending.filter(F.col("is_trending")).count()
        LOG.info("Trending stocks (%d-day): %d", period_days, trending_count)
        
        return stock_with_trending.select(
            "as_of_date", "ticker", "company_name", "sector", "market_cap",
            F.col("return_pct").alias("return_pct"), 
            "sector_avg", "period_days", "is_trending"
        )
    
    def build_portfolio(self, trending_etfs: DataFrame, stock_returns: DataFrame, 
                       holdings_df: DataFrame, stock_data: DataFrame, 
                       period_days: int, as_of_date: date) -> DataFrame:
        """
        Build portfolio allocation for a specific time period
        
        Logic:
          1. For each trending ETF, get its top 5 holdings (from Stage 4)
          2. Pick the TOP 1 BEST PERFORMER (highest return) per ETF
          3. Weight by: Performance × Inverse Market Cap
          4. Normalize weights to sum to 100%
        
        Args:
            trending_etfs: DataFrame with trending ETF tickers from Stage 3
            stock_returns: DataFrame with stock returns for the period
            holdings_df: DataFrame with ETF holdings from Stage 4
            stock_data: DataFrame with stock metadata (sector, market cap)
            period_days: Period length (5, 10, or 20)
            as_of_date: Analysis date
            
        Returns:
            DataFrame with portfolio allocation
        """
        LOG.info("=" * 70)
        LOG.info("Building %d-day Portfolio Allocation for %s", period_days, as_of_date)
        LOG.info("=" * 70)
        
        if trending_etfs.count() == 0:
            LOG.warning("No trending ETFs, cannot build %d-day portfolio", period_days)
            return self.spark.createDataFrame([], schema="ticker string")
        
        # Get list of trending ETF tickers
        trending_tickers = [row['etf_ticker'] for row in trending_etfs.select('etf_ticker').collect()]
        LOG.info("Trending ETFs: %s", trending_tickers)
        
        # Filter holdings to only trending ETFs
        holdings_for_trending = holdings_df.filter(F.col("etf_ticker").isin(trending_tickers))
        
        LOG.info("Holdings for trending ETFs: %d records", holdings_for_trending.count())
        
        # Join stock returns with holdings
        stocks_with_returns = stock_returns.join(
            holdings_for_trending,
            stock_returns["ticker"] == holdings_for_trending["holding_ticker"],
            "inner"
        )
        
        # Add stock metadata (sector, market cap)
        w_latest = Window.partitionBy("ticker").orderBy(F.col("trade_date").desc())
        stock_metadata = stock_data.withColumn("rn", F.row_number().over(w_latest)) \
                                   .filter((F.col("rn") == 1) & (F.col("market_cap") > 0)) \
                                   .select("ticker", "company_name", "sector", "market_cap")
        
        stocks_full = stocks_with_returns.join(stock_metadata, "ticker", "inner")
        
        LOG.info("Stocks with full data: %d records", stocks_full.count())
        
        # For each ETF, pick the BEST PERFORMER (highest return) - just 1 stock per ETF
        w_perf = Window.partitionBy("etf_ticker").orderBy(F.col("return_pct").desc())
        stocks_ranked = stocks_full.withColumn("perf_rank", F.row_number().over(w_perf))
        
        # Select TOP 1 performer from each trending ETF
        best_per_etf = stocks_ranked.filter(F.col("perf_rank") == 1)
        
        portfolio_count = best_per_etf.count()
        LOG.info("Selected best performer from each trending ETF: %d stocks", portfolio_count)
        
        if portfolio_count == 0:
            LOG.warning("No stocks selected for %d-day portfolio", period_days)
            return self.spark.createDataFrame([], schema="ticker string")
        
        # Calculate allocation score: Performance × Inverse Market Cap
        portfolio_with_score = best_per_etf.withColumn(
            "allocation_score",
            F.col("return_pct") * (F.lit(1.0) / F.col("market_cap"))
        )
        
        total_score = portfolio_with_score.agg(F.sum("allocation_score")).first()[0]
        
        if not total_score or total_score == 0:
            LOG.warning("Total allocation score is zero for %d-day, using equal weight", period_days)
            portfolio = portfolio_with_score.withColumn("weight", F.lit(100.0 / portfolio_count))
        else:
            # Normalize to percentage (0-100)
            portfolio = portfolio_with_score.withColumn(
                "weight",
                (F.col("allocation_score") / F.lit(total_score)) * F.lit(100.0)
            )
        
        # Add metadata
        portfolio = portfolio.withColumn("as_of_date", F.lit(as_of_date)) \
                            .withColumn("period_days", F.lit(period_days)) \
                            .withColumn("allocation_reason", 
                                       F.lit(f"top1_perf_per_etf_{period_days}d_score_perf_x_inv_mcap"))
        
        # Add rank by weight
        w_rank = Window.orderBy(F.col("weight").desc())
        portfolio = portfolio.withColumn("rank", F.row_number().over(w_rank))
        
        # Final selection
        portfolio_final = portfolio.select(
            "as_of_date",
            "ticker",
            "company_name",
            "sector",
            "etf_ticker",
            "weight",
            "return_pct",
            "market_cap",
            "allocation_score",
            "allocation_reason",
            "period_days",
            "rank"
        )
        
        # Log portfolio summary
        LOG.info("%d-day Portfolio Summary:", period_days)
        LOG.info("Total weight: %.2f%%", portfolio_final.agg(F.sum("weight")).first()[0])
        LOG.info("Top 5 allocations:")
        for row in portfolio_final.orderBy(F.col("weight").desc()).limit(5).collect():
            mcap_val = (row["market_cap"] or 0) / 1e9
            ret_val = row["return_pct"] or 0.0
            score_val = row["allocation_score"] or 0.0
            LOG.info("  %s (from %s): weight=%.2f%%, return=%.2f%%, mcap=$%.2fB (score=%.2e)",
                    row["ticker"], row["etf_ticker"], row["weight"] or 0.0, 
                    ret_val, mcap_val, score_val)
        
        return portfolio_final
    
    def save_portfolio_allocation(self, portfolio: DataFrame):
        """
        Save portfolio allocation to database
        Supports multiple periods (5d, 10d, 20d)
        """
        if portfolio.count() == 0:
            LOG.warning("No portfolio data to save")
            return
        
        LOG.info("Saving portfolio allocation to database...")
        
        # Convert to pandas for easier upsert
        portfolio_pd = portfolio.toPandas()

        as_of_date_val = portfolio_pd['as_of_date'].iloc[0]
        period_days_val = int(portfolio_pd['period_days'].iloc[0])

        delete_query = """
            DELETE FROM analytics_05_portfolio_allocation
            WHERE as_of_date = %s AND period_days = %s
        """
        self.db.execute_query(delete_query, (as_of_date_val, period_days_val))
        
        for _, row in portfolio_pd.iterrows():
            period_days = row['period_days']
            
            upsert_query = """
                INSERT INTO analytics_05_portfolio_allocation (
                    as_of_date, ticker, company_name, sector, market_cap,
                    return_pct, rank, portfolio_weight, 
                    allocation_reason, period_days
                ) VALUES (
                    %(as_of_date)s, %(ticker)s, %(company_name)s, %(sector)s, %(market_cap)s,
                    %(return_pct)s, %(rank)s, %(portfolio_weight)s,
                    %(allocation_reason)s, %(period_days)s
                )
                ON CONFLICT (as_of_date, ticker, period_days)
                DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    sector = EXCLUDED.sector,
                    market_cap = EXCLUDED.market_cap,
                    return_pct = EXCLUDED.return_pct,
                    rank = EXCLUDED.rank,
                    portfolio_weight = EXCLUDED.portfolio_weight,
                    allocation_reason = EXCLUDED.allocation_reason,
                    created_at = NOW()
            """
            
            data = {
                'as_of_date': row['as_of_date'],
                'ticker': row['ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'market_cap': row['market_cap'],
                'return_pct': row['return_pct'],
                'rank': row['rank'],
                'portfolio_weight': row['weight'] / 100.0,
                'allocation_reason': row['allocation_reason'],
                'period_days': period_days
            }
            
            self.db.execute_query(upsert_query, data)
        
        LOG.info("Saved %d portfolio allocations for %dd period", 
                len(portfolio_pd), portfolio_pd['period_days'].iloc[0])
    
    def save_sector_trending(self, sector_trending: DataFrame):
        """Save sector trending to database (for all periods)"""
        if sector_trending.count() == 0:
            LOG.warning("No sector trending data to save")
            return
        
        LOG.info("Saving sector trending data to database...")
        
        sector_pd = sector_trending.toPandas()
        
        for _, row in sector_pd.iterrows():
            upsert_query = """
                INSERT INTO analytics_sector_trending (
                    year, month, sector, avg_monthly_return, is_trending
                ) VALUES (
                    %(year)s, %(month)s, %(sector)s, %(avg_monthly_return)s, %(is_trending)s
                )
                ON CONFLICT (year, month, sector)
                DO UPDATE SET
                    avg_monthly_return = EXCLUDED.avg_monthly_return,
                    is_trending = EXCLUDED.is_trending,
                    created_at = NOW()
            """
            
            data = {
                'year': row['as_of_date'].year,
                'month': row['as_of_date'].month,
                'sector': row['sector'],
                'avg_monthly_return': row['return_pct'],
                'is_trending': row['is_trending']
            }
            
            self.db.execute_query(upsert_query, data)
        
        LOG.info("Saved %d sector trending records", len(sector_pd))
    
    def save_stock_trending(self, stock_trending: DataFrame):
        """Save stock trending to database (for all periods)"""
        if stock_trending.count() == 0:
            LOG.warning("No stock trending data to save")
            return
        
        LOG.info("Saving stock trending data to database...")
        
        stock_pd = stock_trending.toPandas()
        
        for _, row in stock_pd.iterrows():
            upsert_query = """
                INSERT INTO analytics_stock_trending (
                    year, month, ticker, company_name, sector, 
                    avg_monthly_return, vs_sector, is_trending
                ) VALUES (
                    %(year)s, %(month)s, %(ticker)s, %(company_name)s, %(sector)s,
                    %(avg_monthly_return)s, %(vs_sector)s, %(is_trending)s
                )
                ON CONFLICT (year, month, ticker)
                DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    sector = EXCLUDED.sector,
                    avg_monthly_return = EXCLUDED.avg_monthly_return,
                    vs_sector = EXCLUDED.vs_sector,
                    is_trending = EXCLUDED.is_trending,
                    created_at = NOW()
            """
            
            data = {
                'year': row['as_of_date'].year,
                'month': row['as_of_date'].month,
                'ticker': row['ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'avg_monthly_return': row['return_pct'],
                'vs_sector': row['return_pct'] - row['sector_avg'],
                'is_trending': row['is_trending']
            }
            
            self.db.execute_query(upsert_query, data)
        
        LOG.info("Saved %d stock trending records", len(stock_pd))
    
    def run(self, as_of_date: Optional[date] = None):
        """
        Main execution logic - Stage 5 of 5-Stage Pipeline (13:00 UTC)
        
        Workflow:
          1. Load trending ETFs from Stage 3 (03_analytics_trending_etfs)
          2. Load stock returns and holdings from Stage 4 
          3. For each period (5d, 10d, 20d):
             - Build portfolio allocation 
             - Top 1 performer per trending ETF
             - Weight by performance × inverse market cap
          4. Save all 3 portfolio allocations to database
        
        Args:
            as_of_date: Analysis date (default: today)
        """
        if as_of_date is None:
            as_of_date = date.today()
        
        LOG.info("=" * 70)
        LOG.info("Active Stock Portfolio Allocator - Stage 5")
        LOG.info("Analysis Date: %s", as_of_date)
        LOG.info("=" * 70)
        
        try:
            # STEP 1: Load trending ETFs from Stage 3
            LOG.info("Loading trending ETFs from Stage 3...")
            trending_etfs = self.load_trending_etfs(as_of_date)
            
            if trending_etfs.count() == 0:
                LOG.warning("No trending ETFs found for %s, cannot build portfolio", as_of_date)
                LOG.warning("Make sure Stage 3 (11:00 UTC) has completed successfully")
                return
            
            LOG.info("Found %d trending ETFs", trending_etfs.count())
            
            # STEP 2: Load holdings from Stage 4 (conditional holdings collection)
            LOG.info("Loading ETF holdings from Stage 4...")
            holdings_query = f"""(
                SELECT DISTINCT 
                    etf_ticker,
                    holding_ticker
                FROM collected_04_etf_holdings
                WHERE as_of_date >= '{as_of_date - timedelta(days=30)}'
            ) as holdings"""
            
            holdings_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=holdings_query,
                properties=self.jdbc_properties
            )

            holdings_df = self._filter_invalid_tickers(holdings_df, ticker_col="holding_ticker")

            LOG.info("Loaded %d holdings records", holdings_df.count())
            
            # STEP 3: Load stock data (for metadata: sector, market cap)
            LOG.info("Loading stock data...")
            stock_data = self.load_stock_data(days=20)  # Load sufficient history
            
            # STEP 4: Build portfolios for each period (5d, 10d, 20d)
            periods = [5, 10, 20]
            all_portfolios = []
            
            for period_days in periods:
                LOG.info("-" * 70)
                LOG.info("Building %d-day Portfolio", period_days)
                LOG.info("-" * 70)
                
                # Calculate stock returns for this period
                stock_returns = self.calculate_returns(stock_data, period_days)
                
                # Build portfolio
                portfolio = self.build_portfolio(
                    trending_etfs=trending_etfs,
                    stock_returns=stock_returns,
                    holdings_df=holdings_df,
                    stock_data=stock_data,
                    period_days=period_days,
                    as_of_date=as_of_date
                )
                
                if portfolio.count() > 0:
                    all_portfolios.append(portfolio)
                else:
                    LOG.warning("No portfolio generated for %d-day period", period_days)
            
            # STEP 5: Save all portfolios
            if all_portfolios:
                LOG.info("=" * 70)
                LOG.info("Saving %d portfolio allocations", len(all_portfolios))
                LOG.info("=" * 70)
                
                for portfolio in all_portfolios:
                    self.save_portfolio_allocation(portfolio)
            else:
                LOG.warning("No portfolios to save")
            
            LOG.info("=" * 70)
            LOG.info("Portfolio Allocation Complete")
            LOG.info("=" * 70)
            
        except Exception as exc:
            LOG.error("Error in portfolio allocation: %s", exc, exc_info=True)
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
    
    allocator = None
    try:
        allocator = ActiveStockAllocator()
        as_of_date_str = sys.argv[1] if len(sys.argv) > 1 else os.getenv("AS_OF_DATE")
        as_of_date = None
        if as_of_date_str:
            as_of_date = datetime.strptime(as_of_date_str, "%Y-%m-%d").date()
        allocator.run(as_of_date=as_of_date)
    except Exception as exc:
        LOG.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if allocator:
            allocator.close()


if __name__ == "__main__":
    main()
