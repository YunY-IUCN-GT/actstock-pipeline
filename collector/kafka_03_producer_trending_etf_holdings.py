#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Producer - Trending ETF Holdings + Stock Data (Conditional Collection)
Only collects holdings for ETFs identified as trending by Spark job
Collects TOP 5 holdings + stock data for those holdings
Runs at 12:00 UTC after trending ETF identification
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Set

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import KAFKA_CONFIG
from database.db_helper import DatabaseHelper
from utils.logging_utils import setup_logger

LOG = setup_logger(__name__, "kafka-producer-trending-etf-holdings.log")


class TrendingETFHoldingsProducer:
    """Conditional holdings collector - only for trending ETFs"""
    
    def __init__(self):
        """Initialize Kafka producer and database connection"""
        self.holdings_topic = "etf-holdings-data"
        self.stock_topic = "stock-daily-data"
        self.producer = None
        self.db = DatabaseHelper()
        self.collected_stocks = set()  # Track collected stock tickers
        self._connect_kafka()
    
    def _connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks='all',
                retries=3,
            )
            LOG.info("Connected to Kafka: %s", KAFKA_CONFIG["bootstrap_servers"])
        except KafkaError as exc:
            LOG.error("Failed to connect to Kafka: %s", exc)
            raise
    
    def get_trending_etfs(self) -> List[Dict]:
        """
        Get list of trending ETFs from today's analysis
        
        Returns:
            List of trending ETF dictionaries
        """
        try:
            query = """
                SELECT ticker, etf_type, sector_name, return_pct, spy_return
                FROM 03_analytics_trending_etfs
                WHERE as_of_date = CURRENT_DATE
                  AND is_trending = TRUE
                ORDER BY return_pct DESC
            """
            
            results = self.db.fetch_all(query)
            
            if not results:
                LOG.warning("No trending ETFs found for today!")
                return []
            
            LOG.info("=" * 70)
            LOG.info("Found %d trending ETFs:", len(results))
            for etf in results:
                LOG.info("  %s (%s): %.2f%% (vs SPY: %.2f%%)",
                        etf['ticker'],
                        etf['sector_name'] if etf['sector_name'] else 'Benchmark',
                        etf['return_pct'],
                        etf['spy_return'])
            LOG.info("=" * 70)
            
            return results
            
        except Exception as exc:
            LOG.error("Error fetching trending ETFs: %s", exc)
            return []
    
    def fetch_etf_holdings_top5(self, ticker: str) -> Optional[List[Dict]]:
        """
        Fetch TOP 5 holdings for an ETF using yfinance
        
        Args:
            ticker: ETF ticker symbol
            
        Returns:
            List of top 5 holding dictionaries or None
        """
        try:
            LOG.info("Fetching top 5 holdings for %s...", ticker)
            etf = yf.Ticker(ticker)
            
            # Get holdings data
            holdings = []
            
            # Try to get institutional holders as proxy for holdings
            try:
                inst_holders = etf.institutional_holders
                if inst_holders is not None and not inst_holders.empty:
                    LOG.info("Found institutional holders for %s", ticker)
                    
                    # Take top 5 by shares
                    for idx, row in inst_holders.head(5).iterrows():
                        holding = {
                            'etf_ticker': ticker,
                            'holding_ticker': None,  # Will try to extract from name
                            'holding_name': str(row.get('Holder', 'Unknown')),
                            'holding_shares': int(row.get('Shares', 0)),
                            'holding_value': float(row.get('Value', 0)) if 'Value' in row else None,
                            'holding_percent': float(row.get('% Out', 0)) if '% Out' in row else None,
                            'date_reported': row.get('Date Reported', date.today()).strftime('%Y-%m-%d') if 'Date Reported' in row else date.today().strftime('%Y-%m-%d'),
                        }
                        holdings.append(holding)
                    
                    LOG.info("Extracted %d holdings for %s", len(holdings), ticker)
                    return holdings
            except Exception as e:
                LOG.debug("No institutional holders for %s: %s", ticker, e)
            
            # Fallback: try to get major holders
            try:
                info = etf.info
                if info and 'holdings' in info:
                    LOG.info("Using info.holdings for %s", ticker)
                    # This is rare but worth trying
                    pass
            except Exception as e:
                LOG.debug("No info holdings for %s: %s", ticker, e)
            
            # If no holdings found, return empty
            if not holdings:
                LOG.warning("Could not fetch holdings for %s", ticker)
                return None
            
            return holdings
                
        except Exception as exc:
            LOG.error("Error fetching holdings for %s: %s", ticker, exc)
            return None
    
    def fetch_stock_data(self, ticker: str) -> Optional[Dict]:
        """
        Fetch stock daily data
        
        Args:
            ticker: Stock ticker
            
        Returns:
            Dictionary with stock data or None
        """
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period='5d')
            
            if hist.empty:
                LOG.warning("No stock data for %s", ticker)
                return None
            
            info = stock.info
            latest_date = hist.index[-1].date()
            latest_row = hist.iloc[-1]
            
            # Calculate price change
            if len(hist) >= 2:
                prev_close = hist.iloc[-2]['Close']
                price_change_pct = ((latest_row['Close'] - prev_close) / prev_close) * 100
            else:
                price_change_pct = 0.0
            
            return {
                'ticker': ticker,
                'company_name': info.get('longName', ticker),
                'sector': info.get('sector', 'Unknown'),
                'trade_date': latest_date.strftime('%Y-%m-%d'),
                'open_price': float(latest_row['Open']),
                'high_price': float(latest_row['High']),
                'low_price': float(latest_row['Low']),
                'close_price': float(latest_row['Close']),
                'volume': int(latest_row['Volume']),
                'price_change_percent': float(price_change_pct),
                'market_cap': info.get('marketCap', 0),
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as exc:
            LOG.warning("Error fetching stock data for %s: %s", ticker, exc)
            return None
    
    def produce_holdings_and_stocks(self, etf_ticker: str, etf_type: str, sector_name: Optional[str] = None):
        """
        Collect holdings and stock data for a trending ETF
        
        Args:
            etf_ticker: ETF ticker
            etf_type: 'benchmark' or 'sector'
            sector_name: Sector name if sector ETF
        """
        # Get top 5 holdings
        holdings = self.fetch_etf_holdings_top5(etf_ticker)
        
        if not holdings:
            LOG.warning("Skipping %s - no holdings data", etf_ticker)
            return 0
        
        # Enrich holdings with metadata
        for holding in holdings:
            holding['etf_type'] = etf_type
            holding['sector_name'] = sector_name
            holding['timestamp'] = datetime.utcnow().isoformat()
            holding['as_of_date'] = date.today().strftime('%Y-%m-%d')
        
        # Produce holdings to Kafka
        message = {
            'etf_ticker': etf_ticker,
            'etf_type': etf_type,
            'sector_name': sector_name,
            'holdings_count': len(holdings),
            'holdings': holdings,
            'timestamp': datetime.utcnow().isoformat(),
        }
        
        try:
            future = self.producer.send(
                self.holdings_topic,
                key=etf_ticker,
                value=message
            )
            future.get(timeout=10)
            LOG.info("Published %d holdings for %s", len(holdings), etf_ticker)
        except KafkaError as exc:
            LOG.error("Failed to publish holdings for %s: %s", etf_ticker, exc)
            return 0
        
        # Collect stock data for each holding (with ticker)
        stocks_collected = 0
        for holding in holdings:
            holding_ticker = holding.get('holding_ticker')
            
            # Try to extract ticker from holding_name if not present
            if not holding_ticker:
                # Simple heuristic: look for uppercase words
                name = holding.get('holding_name', '')
                words = name.split()
                for word in words:
                    if word.isupper() and len(word) <= 5:
                        holding_ticker = word
                        break
            
            if holding_ticker and holding_ticker not in self.collected_stocks:
                LOG.info("  Collecting stock data for %s (holding of %s)", holding_ticker, etf_ticker)
                stock_data = self.fetch_stock_data(holding_ticker)
                
                if stock_data:
                    try:
                        future = self.producer.send(
                            self.stock_topic,
                            key=holding_ticker,
                            value=stock_data
                        )
                        future.get(timeout=10)
                        self.collected_stocks.add(holding_ticker)
                        stocks_collected += 1
                        LOG.debug("    Published stock data for %s", holding_ticker)
                    except KafkaError as exc:
                        LOG.error("    Failed to publish stock for %s: %s", holding_ticker, exc)
                
                # Rate limiting
                time.sleep(0.5)
        
        return stocks_collected
    
    def run(self):
        """Main execution logic"""
        LOG.info("=" * 70)
        LOG.info("Trending ETF Holdings + Stock Data Collector")
        LOG.info("Date: %s", date.today())
        LOG.info("=" * 70)
        
        # Get trending ETFs from database
        trending_etfs = self.get_trending_etfs()
        
        if not trending_etfs:
            LOG.warning("No trending ETFs to collect - exiting")
            return 0, 0
        
        total_holdings = 0
        total_stocks = 0
        
        # Process each trending ETF
        for idx, etf in enumerate(trending_etfs, 1):
            ticker = etf['ticker']
            etf_type = etf['etf_type']
            sector_name = etf['sector_name']
            
            LOG.info("[%d/%d] Processing trending ETF: %s (%s)",
                    idx, len(trending_etfs), ticker,
                    sector_name if sector_name else 'Benchmark')
            
            stocks_count = self.produce_holdings_and_stocks(ticker, etf_type, sector_name)
            total_stocks += stocks_count
            total_holdings += 1
            
            # Rate limiting between ETFs
            if idx < len(trending_etfs):
                time.sleep(2.0)
        
        LOG.info("=" * 70)
        LOG.info("Collection Complete:")
        LOG.info("  Trending ETFs processed: %d", total_holdings)
        LOG.info("  Unique stocks collected: %d", len(self.collected_stocks))
        LOG.info("=" * 70)
        
        return total_holdings, len(self.collected_stocks)
    
    def close(self):
        """Close connections"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            LOG.info("Kafka producer closed")
        if self.db:
            self.db.disconnect()
            LOG.info("Database connection closed")


def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    producer = None
    try:
        producer = TrendingETFHoldingsProducer()
        etfs_processed, stocks_collected = producer.run()
        
        if etfs_processed == 0:
            LOG.warning("No data collected")
            sys.exit(0)  # Not an error, just no trending ETFs
        
        LOG.info("Successfully collected data for %d ETFs, %d stocks",
                etfs_processed, stocks_collected)
        
    except KeyboardInterrupt:
        LOG.info("Interrupted by user")
        sys.exit(1)
    except Exception as exc:
        LOG.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
