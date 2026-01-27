#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Producer - ETF Daily OHLC Data
Collects daily OHLC data for benchmark and sector ETFs and publishes to Kafka
Used by scheduled Airflow DAGs
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, date
from typing import Dict, List, Optional

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import KAFKA_CONFIG, BENCHMARK_TICKERS, SECTOR_ETF_TICKERS
from utils.logging_utils import setup_logger

LOG = setup_logger(__name__, "kafka-producer-etf-daily.log")


class ETFDailyDataProducer:
    """ETF daily OHLC data collector and Kafka producer"""
    
    def __init__(self):
        """Initialize Kafka producer"""
        self.topic = "etf-daily-data"
        self.producer = None
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
    
    def fetch_etf_daily_data(self, ticker: str) -> Optional[Dict]:
        """
        Fetch daily OHLC data for a single ETF
        
        Args:
            ticker: ETF ticker symbol
            
        Returns:
            Dictionary with OHLC data or None if failed
        """
        try:
            LOG.info("Fetching daily data for %s...", ticker)
            etf = yf.Ticker(ticker)
            
            # Fetch last 5 days to ensure we get latest data
            hist = etf.history(period='5d')
            
            if hist.empty:
                LOG.warning("No data returned for %s", ticker)
                return None
            
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
                'trade_date': latest_date.strftime('%Y-%m-%d'),
                'open_price': float(latest_row['Open']),
                'high_price': float(latest_row['High']),
                'low_price': float(latest_row['Low']),
                'close_price': float(latest_row['Close']),
                'volume': int(latest_row['Volume']),
                'price_change_percent': float(price_change_pct),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            LOG.info("Fetched %s: date=%s, close=%.2f, change=%.2f%%",
                    ticker, latest_date, latest_row['Close'], price_change_pct)
            return data
                
        except Exception as exc:
            LOG.error("Error fetching data for %s: %s", ticker, exc)
            return None
    
    def produce_etf_data(self, ticker: str, etf_type: str, sector_name: Optional[str] = None):
        """
        Fetch and produce ETF daily data to Kafka
        
        Args:
            ticker: ETF ticker
            etf_type: 'benchmark' or 'sector'
            sector_name: Sector name for sector ETFs
        """
        data = self.fetch_etf_daily_data(ticker)
        
        if not data:
            LOG.warning("Skipping %s - no data available", ticker)
            return False
        
        # Enrich with metadata
        data['etf_type'] = etf_type
        data['sector_name'] = sector_name
        
        # Send to Kafka
        try:
            future = self.producer.send(
                self.topic,
                key=ticker,
                value=data
            )
            # Block for confirmation
            record_metadata = future.get(timeout=10)
            LOG.info("Sent %s to Kafka: topic=%s, partition=%d, offset=%d",
                    ticker, record_metadata.topic, record_metadata.partition, record_metadata.offset)
            return True
            
        except KafkaError as exc:
            LOG.error("Failed to send %s to Kafka: %s", ticker, exc)
            return False
    
    def produce_benchmark_group(self, delay_seconds: int = 5):
        """
        Produce all benchmark ETF data (Group 1)
        
        Args:
            delay_seconds: Delay between tickers (rate limiting)
        """
        LOG.info("=" * 70)
        LOG.info("Starting Group 1: Benchmark ETFs (%d tickers)", len(BENCHMARK_TICKERS))
        LOG.info("=" * 70)
        
        successful = []
        failed = []
        
        for idx, ticker in enumerate(BENCHMARK_TICKERS, 1):
            LOG.info("[%d/%d] Processing %s...", idx, len(BENCHMARK_TICKERS), ticker)
            
            if self.produce_etf_data(ticker, 'benchmark'):
                successful.append(ticker)
            else:
                failed.append(ticker)
            
            # Rate limiting
            if idx < len(BENCHMARK_TICKERS):
                time.sleep(delay_seconds)
        
        LOG.info("=" * 70)
        LOG.info("Group 1 Summary: Success=%d, Failed=%d", len(successful), len(failed))
        if successful:
            LOG.info("Successful: %s", ', '.join(successful))
        if failed:
            LOG.warning("Failed: %s", ', '.join(failed))
        LOG.info("=" * 70)
        
        return len(successful), len(failed)
    
    def produce_sector_group(self, delay_seconds: int = 5):
        """
        Produce all sector ETF data (Group 2)
        
        Args:
            delay_seconds: Delay between tickers (rate limiting)
        """
        LOG.info("=" * 70)
        LOG.info("Starting Group 2: Sector ETFs (%d tickers)", len(SECTOR_ETF_TICKERS))
        LOG.info("=" * 70)
        
        # Map sector names
        SECTOR_MAP = {
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
        
        successful = []
        failed = []
        
        for idx, ticker in enumerate(SECTOR_ETF_TICKERS, 1):
            LOG.info("[%d/%d] Processing %s...", idx, len(SECTOR_ETF_TICKERS), ticker)
            sector_name = SECTOR_MAP.get(ticker)
            
            if self.produce_etf_data(ticker, 'sector', sector_name):
                successful.append(ticker)
            else:
                failed.append(ticker)
            
            # Rate limiting
            if idx < len(SECTOR_ETF_TICKERS):
                time.sleep(delay_seconds)
        
        LOG.info("=" * 70)
        LOG.info("Group 2 Summary: Success=%d, Failed=%d", len(successful), len(failed))
        if successful:
            LOG.info("Successful: %s", ', '.join(successful))
        if failed:
            LOG.warning("Failed: %s", ', '.join(failed))
        LOG.info("=" * 70)
        
        return len(successful), len(failed)
    
    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            LOG.info("Kafka producer closed")


def main():
    """Main execution for scheduled runs"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETF Daily Data Kafka Producer')
    parser.add_argument('--group', choices=['benchmark', 'sector', 'both'], default='both',
                       help='Which group to process')
    parser.add_argument('--delay', type=int, default=5,
                       help='Delay between tickers in seconds')
    
    args = parser.parse_args()
    
    producer = ETFDailyDataProducer()
    
    try:
        if args.group in ['benchmark', 'both']:
            producer.produce_benchmark_group(delay_seconds=args.delay)
        
        if args.group in ['sector', 'both']:
            producer.produce_sector_group(delay_seconds=args.delay)
    
    finally:
        producer.close()


if __name__ == '__main__':
    main()
