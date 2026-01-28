#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Consumer - ETF Holdings Data
Consumes ETF holdings data from Kafka and stores in PostgreSQL
"""

import json
import logging
import os
import sys
import signal
from datetime import datetime, date
from typing import Dict, List

from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import KAFKA_CONFIG, DB_CONFIG
from database.db_helper import DatabaseHelper
from utils.logging_utils import setup_logger

LOG = setup_logger(__name__, "kafka-consumer-etf-holdings.log")


class ETFHoldingsConsumer:
    """Kafka consumer for ETF holdings data"""
    
    def __init__(self):
        """Initialize Kafka consumer and database connection"""
        self.topic = "etf-holdings-data"
        self.consumer = None
        self.db = DatabaseHelper()
        self.running = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self._connect_kafka()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        LOG.info("Received signal %d, shutting down gracefully...", signum)
        self.running = False
    
    def _connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
                group_id=KAFKA_CONFIG["consumer_group_id"] + "-etf-holdings",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset=KAFKA_CONFIG["auto_offset_reset"],
                enable_auto_commit=KAFKA_CONFIG["enable_auto_commit"],
            )
            LOG.info("Connected to Kafka topic: %s", self.topic)
        except KafkaError as exc:
            LOG.error("Failed to connect to Kafka: %s", exc)
            raise
    
    def _store_holdings(self, message: Dict):
        """
        Store holdings data in database
        
        Args:
            message: Message from Kafka containing holdings data
        """
        try:
            etf_ticker = message.get('etf_ticker')
            holdings = message.get('holdings', [])
            
            if not holdings:
                LOG.warning("No holdings in message for %s", etf_ticker)
                return
            
            # Insert query with conflict handling
            insert_query = """
                INSERT INTO collected_04_etf_holdings (
                    etf_ticker,
                    holding_ticker,
                    holding_name,
                    holding_percent,
                    holding_shares,
                    holding_value,
                    as_of_date,
                    source
                ) VALUES (
                    %(etf_ticker)s,
                    %(holding_ticker)s,
                    %(holding_name)s,
                    %(holding_percent)s,
                    %(holding_shares)s,
                    %(holding_value)s,
                    %(as_of_date)s,
                    %(source)s
                )
                ON CONFLICT (etf_ticker, holding_ticker, as_of_date)
                DO UPDATE SET
                    holding_name = EXCLUDED.holding_name,
                    holding_percent = EXCLUDED.holding_percent,
                    holding_shares = EXCLUDED.holding_shares,
                    holding_value = EXCLUDED.holding_value,
                    source = EXCLUDED.source,
                    created_at = NOW()
            """
            
            inserted = 0
            for holding in holdings:
                # Extract holding_ticker from holding_name if not present
                holding_ticker = holding.get('holding_ticker')
                if not holding_ticker:
                    # Try to extract ticker from name (fallback)
                    holding_ticker = holding.get('holding_name', 'UNKNOWN')[:10]
                
                row_data = {
                    'etf_ticker': etf_ticker,
                    'holding_ticker': holding_ticker,
                    'holding_name': holding.get('holding_name'),
                    'holding_percent': holding.get('holding_percent'),
                    'holding_shares': holding.get('holding_shares'),
                    'holding_value': holding.get('holding_value'),
                    'as_of_date': holding.get('as_of_date', date.today().strftime('%Y-%m-%d')),
                    'source': 'yfinance'
                }
                
                if self.db.execute_query(insert_query, row_data):
                    inserted += 1
            
            LOG.info("Stored %d/%d holdings for %s", inserted, len(holdings), etf_ticker)
            
        except Exception as exc:
            LOG.error("Error storing holdings: %s", exc, exc_info=True)
    
    def run(self):
        """Main consumer loop"""
        LOG.info("Starting ETF Holdings Consumer")
        LOG.info("Listening to topic: %s", self.topic)
        
        processed = 0
        errors = 0
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    data = message.value
                    LOG.debug("Received message: %s", data.get('etf_ticker'))
                    
                    self._store_holdings(data)
                    processed += 1
                    
                    if processed % 10 == 0:
                        LOG.info("Processed %d messages (errors: %d)", processed, errors)
                
                except Exception as exc:
                    LOG.error("Error processing message: %s", exc)
                    errors += 1
        
        except KeyboardInterrupt:
            LOG.info("Interrupted by user")
        finally:
            LOG.info("Consumer shutting down. Processed: %d, Errors: %d", processed, errors)
            self.close()
    
    def close(self):
        """Close consumer and database connections"""
        if self.consumer:
            self.consumer.close()
            LOG.info("Kafka consumer closed")
        
        if self.db:
            self.db.disconnect()


def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    consumer = None
    try:
        consumer = ETFHoldingsConsumer()
        consumer.run()
    except Exception as exc:
        LOG.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if consumer:
            consumer.close()


if __name__ == "__main__":
    main()
