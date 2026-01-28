#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import psycopg2
from datetime import datetime, timedelta
import random

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import DB_CONFIG, BENCHMARK_TICKERS, SECTOR_ETFS

def generate_mock_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    tickers = BENCHMARK_TICKERS + list(SECTOR_ETFS.values())
    end_date = datetime.now().date()
    days = 30
    
    print(f"Generating {days} days of mock data for {len(tickers)} ETFs...")
    
    # 1. Mock collected_01_daily_etf_ohlc
    for ticker in tickers:
        price = random.uniform(50, 500)
        for i in range(days):
            date = end_date - timedelta(days=days-i-1)
            # Skip weekends
            if date.weekday() >= 5:
                continue
                
            change = random.uniform(-2, 2.5) # Slightly positive bias
            # Make some tickers "trending" (higher returns recently)
            if ticker in ['XLK', 'QQQ', 'XLC'] and i > 20:
                change = random.uniform(0.5, 4.0)
            
            old_price = price
            price = price * (1 + change / 100)
            
            cur.execute("""
                INSERT INTO collected_01_daily_etf_ohlc 
                (ticker, trade_date, open_price, high_price, low_price, close_price, volume, price_change_percent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, trade_date) DO NOTHING
            """, (ticker, date, old_price, max(old_price, price) * 1.01, min(old_price, price) * 0.99, price, random.randint(1000000, 10000000), change))
            
    # 2. Mock collected_04_etf_holdings (Top 5 for trending ETFs)
    trending_etfs = ['XLK', 'QQQ', 'XLY']
    holdings_per_etf = {
        'XLK': [('AAPL', 'Apple Inc.'), ('MSFT', 'Microsoft Corp.'), ('NVDA', 'NVIDIA Corp.'), ('AVGO', 'Broadcom Inc.'), ('ORCL', 'Oracle Corp.')],
        'QQQ': [('AAPL', 'Apple Inc.'), ('MSFT', 'Microsoft Corp.'), ('AMZN', 'Amazon.com Inc.'), ('META', 'Meta Platforms Inc.'), ('GOOGL', 'Alphabet Inc.')],
        'XLY': [('AMZN', 'Amazon.com Inc.'), ('TSLA', 'Tesla Inc.'), ('HD', 'Home Depot Inc.'), ('MCD', "McDonald's Corp."), ('NKE', 'Nike Inc.')]
    }
    
    # Also add some for everything else just in case
    for t in [t for t in tickers if t not in trending_etfs]:
        holdings_per_etf[t] = [('STOCK1', 'Stock One'), ('STOCK2', 'Stock Two'), ('STOCK3', 'Stock Three'), ('STOCK4', 'Stock Four'), ('STOCK5', 'Stock Five')]

    print("Seeding holdings and stock history...")
    all_stocks = set()
    for etf, stocks in holdings_per_etf.items():
        for i, (ticker, name) in enumerate(stocks):
            all_stocks.add((ticker, name))
            cur.execute("""
                INSERT INTO collected_04_etf_holdings 
                (etf_ticker, holding_ticker, holding_name, holding_percent, as_of_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (etf_ticker, holding_ticker, as_of_date) DO NOTHING
            """, (etf, ticker, name, 10.0 - i, end_date))

    # 3. Mock collected_06_daily_stock_history
    for ticker, name in all_stocks:
        price = random.uniform(10, 1000)
        sector = "Technology" if ticker in ['AAPL', 'MSFT', 'NVDA', 'AVGO', 'ORCL', 'AMZN', 'META', 'GOOGL'] else "Other"
        for i in range(days):
            date = end_date - timedelta(days=days-i-1)
            if date.weekday() >= 5:
                continue
                
            change = random.uniform(-3, 3.5)
            old_price = price
            price = price * (1 + change / 100)
            
            cur.execute("""
                INSERT INTO collected_06_daily_stock_history 
                (ticker, company_name, sector, trade_date, open_price, high_price, low_price, close_price, volume, price_change_percent, market_cap)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, trade_date) DO NOTHING
            """, (ticker, name, sector, date, old_price, max(old_price, price) * 1.01, min(old_price, price) * 0.99, price, random.randint(500000, 5000000), change, random.randint(1000000000, 500000000000)))

    conn.commit()
    cur.close()
    conn.close()
    print("Handled mock data generation.")

if __name__ == "__main__":
    generate_mock_data()
