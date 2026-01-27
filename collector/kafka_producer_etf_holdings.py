#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Producer - ETF Holdings Data + Stock Data (Top 5 Holdings)
Collects holdings data for benchmark and sector ETFs and publishes to Kafka
Also collects stock data for TOP 5 holdings of each ETF to avoid rate limits
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

from config.config import KAFKA_CONFIG, BENCHMARK_TICKERS, SECTOR_ETF_TICKERS
from utils.logging_utils import setup_logger

LOG = setup_logger(__name__, "kafka-producer-etf-holdings.log")


class ETFHoldingsProducer:
    """ETF holdings data collector and Kafka producer + stock data for top 5 holdings"""
    
    def __init__(self):
        """Initialize Kafka producer"""
        self.holdings_topic = "etf-holdings-data"
        self.stock_topic = "stock-daily-data"
        self.producer = None
        self.top5_tickers = set()  # Track top 5 holdings for stock collection
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
    
    def fetch_etf_holdings(self, ticker: str) -> Optional[List[Dict]]:
        """
        Fetch holdings data for a single ETF
        
        Args:
            ticker: ETF ticker symbol
            
        Returns:
            List of holding dictionaries or None if failed
        """
        try:
            LOG.info("Fetching holdings for %s...", ticker)
            etf = yf.Ticker(ticker)
            
            # Get holdings data
            # Note: yfinance API may not always have holdings data
            # This is a best-effort approach
            holdings = []
            
            # Try to get major holders
            try:
                major_holders = etf.major_holders
                if major_holders is not None and not major_holders.empty:
                    LOG.info("Found major holders data for %s", ticker)
            except Exception as e:
                LOG.debug("No major holders data for %s: %s", ticker, e)
            
            # Try to get institutional holders
            try:
                inst_holders = etf.institutional_holders
                if inst_holders is not None and not inst_holders.empty:
                    LOG.info("Found %d institutional holders for %s", len(inst_holders), ticker)
                    
                    for idx, row in inst_holders.head(20).iterrows():
                        holding = {
                            'etf_ticker': ticker,
                            'holding_name': row.get('Holder', 'Unknown'),
                            'holding_shares': int(row.get('Shares', 0)),
                            'holding_value': float(row.get('Value', 0)) if 'Value' in row else None,
                            'holding_percent': float(row.get('% Out', 0)) if '% Out' in row else None,
                            'date_reported': row.get('Date Reported', date.today()).strftime('%Y-%m-%d') if 'Date Reported' in row else date.today().strftime('%Y-%m-%d'),
                        }
                        holdings.append(holding)
            except Exception as e:
                LOG.debug("No institutional holders for %s: %s", ticker, e)
            
            # Fallback: use fund info if available
            if not holdings:
                try:
                    info = etf.info
                    if info:
                        LOG.info("Using info data for %s", ticker)
                        # Extract what we can from info
                        holding = {
                            'etf_ticker': ticker,
                            'holding_name': info.get('longName', ticker),
                            'holding_ticker': ticker,
                            'holding_shares': None,
                            'holding_value': info.get('totalAssets'),
                            'holding_percent': None,
                            'date_reported': date.today().strftime('%Y-%m-%d'),
                        }
                        holdings.append(holding)
                except Exception as e:
                    LOG.warning("Could not get info for %s: %s", ticker, e)
            
            if holdings:
                LOG.info("Fetched %d holdings for %s", len(holdings), ticker)
                return holdings
            else:
                LOG.warning("No holdings data available for %s", ticker)
                return None
                
        except Exception as exc:
            LOG.error("Error fetching holdings for %s: %s", ticker, exc)
            return None
    
    def fetch_stock_data(self, ticker: str) -> Optional[Dict]:
        """
        Fetch stock daily data for a holding ticker
        
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
    
    def produce_stock_data(self, ticker: str) -> bool:
        """
        Produce stock data to Kafka
        
        Args:
            ticker: Stock ticker
            
        Returns:
            True if successful
        """
        data = self.fetch_stock_data(ticker)
        if not data:
            return False
        
        try:
            future = self.producer.send(
                self.stock_topic,
                key=ticker,
                value=data
            )
            future.get(timeout=10)
            LOG.debug("Published stock data for %s", ticker)
            return True
        except KafkaError as exc:
            LOG.error("Failed to publish stock data for %s: %s", ticker, exc)
            return False
    
    def produce_holdings(self, ticker: str, etf_type: str, sector_name: Optional[str] = None):
        """
        Fetch and produce holdings data to Kafka
        Also collect stock data for TOP 5 holdings
        
        Args:
            ticker: ETF ticker
            etf_type: 'benchmark' or 'sector'
            sector_name: Sector name for sector ETFs
        """
        holdings = self.fetch_etf_holdings(ticker)
        
        if not holdings:
            LOG.warning("Skipping %s - no holdings data", ticker)
            return
        
        # Enrich with metadata
        for holding in holdings:
            holding['etf_type'] = etf_type
            holding['sector_name'] = sector_name
            holding['timestamp'] = datetime.utcnow().isoformat()
            holding['as_of_date'] = date.today().strftime('%Y-%m-%d')
        
        # Send holdings to Kafka
        message = {
            'etf_ticker': ticker,
            'etf_type': etf_type,
            'sector_name': sector_name,
            'holdings_count': len(holdings),
            'holdings': holdings,
            'timestamp': datetime.utcnow().isoformat(),
        }
        
        try:
            future = self.producer.send(
                self.holdings_topic,
                key=ticker,
                value=message
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            LOG.info(
                "Published %s holdings to %s:%d:%d",
                ticker,
                record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset
            )
            
            # Extract top 5 holdings by percent (if available)
            top5 = sorted(
                [h for h in holdings if h.get('holding_ticker') and h.get('holding_percent')],
                key=lambda x: x.get('holding_percent', 0),
                reverse=True
            )[:5]
            
            if not top5:
                # Fallback: just take first 5 if no percent data
                top5 = [h for h in holdings if h.get('holding_ticker')][:5]
            
            # Collect stock data for top 5 holdings
            if top5:
                LOG.info("Collecting stock data for top 5 holdings of %s", ticker)
                for holding in top5:
                    holding_ticker = holding.get('holding_ticker')
                    if holding_ticker and holding_ticker not in self.top5_tickers:
                        self.top5_tickers.add(holding_ticker)
                        LOG.info("  - Collecting %s (%.2f%% of %s)",
                                holding_ticker,
                                holding.get('holding_percent', 0),
                                ticker)
                        self.produce_stock_data(holding_ticker)
                        time.sleep(0.5)  # Rate limiting between stocks
            
        except KafkaError as exc:
            LOG.error("Failed to publish %s: %s", ticker, exc)
    
    def run(self):
        """Main execution loop"""
        LOG.info("Starting ETF Holdings Producer")
        
        # Sector ETF mapping
        sector_mapping = {
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
        
        all_tickers = []
        
        # Add benchmark ETFs
        for ticker in BENCHMARK_TICKERS:
            all_tickers.append((ticker, 'benchmark', None))
        
        # Add sector ETFs
        for ticker in SECTOR_ETF_TICKERS:
            sector = sector_mapping.get(ticker)
            all_tickers.append((ticker, 'sector', sector))
        
        LOG.info("Processing %d ETFs (%d benchmark, %d sector)", 
                 len(all_tickers), len(BENCHMARK_TICKERS), len(SECTOR_ETF_TICKERS))
        
        # Process each ETF with delay
        for idx, (ticker, etf_type, sector) in enumerate(all_tickers, 1):
            LOG.info("[%d/%d] Processing %s (%s)", idx, len(all_tickers), ticker, etf_type)
            
            try:
                self.produce_holdings(ticker, etf_type, sector)
            except Exception as exc:
                LOG.error("Error processing %s: %s", ticker, exc)
            
            # Rate limiting: wait between requests
            if idx < len(all_tickers):
                delay = 3.0
                LOG.info("Waiting %.1f seconds before next ticker...", delay)
                time.sleep(delay)
        
        LOG.info("="  * 70)
        LOG.info("Completed ETF holdings collection")
        LOG.info("Total unique stock tickers collected: %d", len(self.top5_tickers))
        LOG.info("="  * 70)
    
    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            LOG.info("Kafka producer closed")


def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    producer = None
    try:
        producer = ETFHoldingsProducer()
        producer.run()
    except KeyboardInterrupt:
        LOG.info("Interrupted by user")
    except Exception as exc:
        LOG.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
