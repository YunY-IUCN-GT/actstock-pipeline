#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Consumer - ETF Daily OHLC Data
Consumes ETF daily data from Kafka and stores in PostgreSQL
Runs continuously to process messages as they arrive
"""

import json
import logging
import os
import sys
import signal
from datetime import datetime, date
from typing import Dict

from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import KAFKA_CONFIG, DB_CONFIG
from database.db_helper import DatabaseHelper
from utils.logging_utils import setup_logger

LOG = setup_logger(__name__, "kafka-consumer-etf-daily.log")


class ETFDailyDataConsumer:
    """Kafka consumer for ETF daily OHLC data"""
    
    def __init__(self):
        """Initialize Kafka consumer and database connection"""
        self.topic = "etf-daily-data"
        self.consumer = None
        self.db = DatabaseHelper()
        self.running = True
        
        # Setup signal handlers for graceful shutdown
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
                group_id=KAFKA_CONFIG["consumer_group_id"] + "-etf-daily",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset=KAFKA_CONFIG["auto_offset_reset"],
                enable_auto_commit=KAFKA_CONFIG["enable_auto_commit"],
            )
            LOG.info("Connected to Kafka topic: %s", self.topic)
        except KafkaError as exc:
            LOG.error("Failed to connect to Kafka: %s", exc)
            raise
    
    def _store_etf_data(self, message: Dict):
        """
        Store ETF daily data in database
        
        Args:
            message: Message from Kafka containing ETF daily data
        """
        try:
            ticker = message.get('ticker')
            
            if not ticker:
                LOG.warning("Missing ticker in message: %s", message)
                return False
            
            # Insert query with conflict handling (upsert)
            insert_query = """
                INSERT INTO "01_collected_daily_etf_ohlc" (
                    ticker,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    price_change_percent
                ) VALUES (
                    %(ticker)s,
                    %(trade_date)s,
                    %(open_price)s,
                    %(high_price)s,
                    %(low_price)s,
                    %(close_price)s,
                    %(volume)s,
                    %(price_change_percent)s
                )
                ON CONFLICT (ticker, trade_date)
                DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    price_change_percent = EXCLUDED.price_change_percent,
                    created_at = NOW()
            """
            
            row_data = {
                'ticker': ticker,
                'trade_date': message.get('trade_date'),
                'open_price': message.get('open_price'),
                'high_price': message.get('high_price'),
                'low_price': message.get('low_price'),
                'close_price': message.get('close_price'),
                'volume': message.get('volume'),
                'price_change_percent': message.get('price_change_percent')
            }
            
            if self.db.execute_query(insert_query, row_data):
                LOG.info("Stored %s: date=%s, close=%.2f, change=%.2f%%",
                        ticker, row_data['trade_date'], row_data['close_price'], 
                        row_data['price_change_percent'])
                return True
            else:
                LOG.error("Failed to store %s", ticker)
                return False
            
        except Exception as exc:
            LOG.error("Error storing ETF data: %s", exc, exc_info=True)
            return False
    
    def run(self):
        """Main consumer loop"""
        LOG.info("Starting ETF Daily Data Consumer")
        LOG.info("Listening to topic: %s", self.topic)
        LOG.info("Consumer group: %s", KAFKA_CONFIG["consumer_group_id"] + "-etf-daily")
        
        processed = 0
        errors = 0
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    LOG.debug("Received message: partition=%d, offset=%d",
                             message.partition, message.offset)
                    
                    if self._store_etf_data(message.value):
                        processed += 1
                    else:
                        errors += 1
                    
                    # Log progress every 10 messages
                    if (processed + errors) % 10 == 0:
                        LOG.info("Progress: processed=%d, errors=%d", processed, errors)
                
                except Exception as exc:
                    LOG.error("Error processing message: %s", exc, exc_info=True)
                    errors += 1
        
        except KeyboardInterrupt:
            LOG.info("Interrupted by user")
        
        finally:
            LOG.info("=" * 70)
            LOG.info("Consumer shutting down")
            LOG.info("Total processed: %d", processed)
            LOG.info("Total errors: %d", errors)
            LOG.info("=" * 70)
            
            if self.consumer:
                self.consumer.close()
                LOG.info("Kafka consumer closed")
            
            if self.db:
                self.db.disconnect()
                LOG.info("Database connection closed")


def main():
    """Main execution"""
    consumer = ETFDailyDataConsumer()
    consumer.run()


if __name__ == '__main__':
    main()
