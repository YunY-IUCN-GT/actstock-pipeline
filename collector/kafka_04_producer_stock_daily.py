#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Producer - Stock Daily Data
Collects daily OHLC data for stocks from ETF holdings and publishes to Kafka
Used by scheduled Airflow DAGs (4x daily to avoid rate limits)
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Set

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import KAFKA_CONFIG
from database.db_helper import DatabaseHelper
from utils.logging_utils import setup_logger

LOG = setup_logger(__name__, "kafka-producer-stock-daily.log")


class StockDailyDataProducer:
    """Stock daily OHLC data collector and Kafka producer"""
    
    def __init__(self):
        """Initialize Kafka producer and database connection"""
        self.topic = "stock-daily-data"
        self.producer = None
        self.db = DatabaseHelper()
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
    
    def get_holdings_tickers(self) -> Set[str]:
        """
        Get unique stock tickers from ETF holdings
        
        Returns:
            Set of unique ticker symbols
        """
        try:
            query = """
                SELECT DISTINCT ticker
                FROM 04_collected_etf_holdings
                WHERE ticker IS NOT NULL
                  AND ticker != ''
                ORDER BY ticker
            """
            
            results = self.db.fetch_all(query)
            tickers = {row['ticker'] for row in results if row['ticker']}
            
            LOG.info("Found %d unique tickers from ETF holdings", len(tickers))
            return tickers
            
        except Exception as exc:
            LOG.error("Error fetching holdings tickers: %s", exc)
            return set()
    
    def fetch_stock_daily_data(self, ticker: str) -> Optional[Dict]:
        """
        Fetch daily OHLC data for a single stock
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with OHLC data or None if failed
        """
        try:
            stock = yf.Ticker(ticker)
            
            # Fetch last 5 days to ensure we get latest data
            hist = stock.history(period='5d')
            
            if hist.empty:
                LOG.warning("No data returned for %s", ticker)
                return None
            
            # Get stock info
            info = stock.info
            
            # Get the most recent trading day
            latest_date = hist.index[-1].date()
            latest_row = hist.iloc[-1]
            
            # Calculate price change
            if len(hist) >= 2:
                prev_close = hist.iloc[-2]['Close']
                price_change_pct = ((latest_row['Close'] - prev_close) / prev_close) * 100
            else:
                price_change_pct = 0.0
            
            data = {
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
            
            LOG.debug("Fetched %s: date=%s, close=%.2f, sector=%s",
                     ticker, latest_date, latest_row['Close'], data['sector'])
            return data
                
        except Exception as exc:
            LOG.warning("Error fetching data for %s: %s", ticker, exc)
            return None
    
    def produce_stock_data(self, ticker: str) -> bool:
        """
        Fetch and produce stock daily data to Kafka
        
        Args:
            ticker: Stock ticker
            
        Returns:
            True if successful, False otherwise
        """
        data = self.fetch_stock_daily_data(ticker)
        
        if not data:
            return False
        
        # Send to Kafka
        try:
            future = self.producer.send(
                self.topic,
                key=ticker,
                value=data
            )
            # Block for confirmation
            record_metadata = future.get(timeout=10)
            LOG.debug("Sent %s to Kafka: partition=%d, offset=%d",
                     ticker, record_metadata.partition, record_metadata.offset)
            return True
            
        except KafkaError as exc:
            LOG.error("Failed to send %s to Kafka: %s", ticker, exc)
            return False
    
    def produce_all_stocks(self, delay_seconds: float = 0.5, max_tickers: int = None):
        """
        Produce all stock data with rate limiting
        
        Args:
            delay_seconds: Delay between API calls (default 0.5s = 120 calls/min)
            max_tickers: Optional limit for testing
        """
        LOG.info("=" * 70)
        LOG.info("Starting Stock Daily Data Collection")
        LOG.info("Rate limit: %.2f seconds between calls", delay_seconds)
        LOG.info("=" * 70)
        
        # Get tickers from holdings
        tickers = self.get_holdings_tickers()
        
        if not tickers:
            LOG.warning("No tickers found in holdings!")
            return 0, 0
        
        # Limit for testing
        if max_tickers:
            tickers = set(list(tickers)[:max_tickers])
            LOG.info("Limited to %d tickers for testing", max_tickers)
        
        LOG.info("Processing %d tickers...", len(tickers))
        
        successful = []
        failed = []
        
        for idx, ticker in enumerate(sorted(tickers), 1):
            if idx % 50 == 0:
                LOG.info("Progress: [%d/%d] processed, %d success, %d failed",
                        idx, len(tickers), len(successful), len(failed))
            
            if self.produce_stock_data(ticker):
                successful.append(ticker)
            else:
                failed.append(ticker)
            
            # Rate limiting - crucial to avoid yfinance rate limits
            if idx < len(tickers):
                time.sleep(delay_seconds)
        
        LOG.info("=" * 70)
        LOG.info("Collection Summary:")
        LOG.info("  Total tickers: %d", len(tickers))
        LOG.info("  Successful: %d (%.1f%%)", len(successful), 
                len(successful) / len(tickers) * 100 if tickers else 0)
        LOG.info("  Failed: %d (%.1f%%)", len(failed),
                len(failed) / len(tickers) * 100 if tickers else 0)
        if failed and len(failed) <= 20:
            LOG.warning("Failed tickers: %s", ', '.join(failed))
        LOG.info("=" * 70)
        
        return len(successful), len(failed)
    
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
    """Main execution"""
    producer = None
    try:
        producer = StockDailyDataProducer()
        
        # Check for test mode
        max_tickers = None
        if '--test' in sys.argv:
            max_tickers = 20
            LOG.info("Running in TEST mode: limited to %d tickers", max_tickers)
        
        # Run collection with rate limiting (0.5s = 120 calls/min)
        success, failed = producer.produce_all_stocks(
            delay_seconds=0.5,
            max_tickers=max_tickers
        )
        
        # Exit code based on success rate
        if success == 0:
            sys.exit(1)
        
        success_rate = success / (success + failed) * 100
        if success_rate < 50:
            LOG.warning("Success rate below 50%%, exiting with error code")
            sys.exit(1)
        
        LOG.info("Collection completed successfully")
        
    except KeyboardInterrupt:
        LOG.info("Interrupted by user")
        sys.exit(1)
    except Exception as exc:
        LOG.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if producer:
            producer.close()


if __name__ == '__main__':
    main()
