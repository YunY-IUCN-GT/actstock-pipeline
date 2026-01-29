#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import psycopg2
from datetime import datetime, timedelta
import random
import re

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import DB_CONFIG, BENCHMARK_TICKERS, SECTOR_ETFS

TICKER_RE = re.compile(r"^[A-Z0-9\.\-]{1,10}$")


def _validate_ticker(ticker: str):
    if not ticker or not TICKER_RE.match(ticker):
        raise ValueError(f"Invalid ticker: {ticker}")


ETF_TOP_HOLDINGS = {
    # Benchmarks
    "SPY": ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"],
    "QQQ": ["AAPL", "MSFT", "NVDA", "AMZN", "META"],
    "IWM": ["SMCI", "RBLX", "CROX", "FND", "PTC"],
    "DIA": ["MSFT", "AAPL", "UNH", "HD", "JNJ"],
    "EWY": ["KB", "PKX", "SKM", "KT", "LPL"],
    "SCHD": ["KO", "PEP", "PG", "VZ", "TXN"],
    # Sector ETFs
    "XLK": ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL"],
    "XLV": ["UNH", "JNJ", "LLY", "ABBV", "MRK"],
    "XLF": ["JPM", "BAC", "WFC", "MS", "GS"],
    "XLY": ["AMZN", "TSLA", "HD", "MCD", "NKE"],
    "XLC": ["META", "GOOGL", "NFLX", "TMUS", "DIS"],
    "XLI": ["GE", "CAT", "RTX", "HON", "UNP"],
    "XLP": ["PG", "KO", "PEP", "WMT", "COST"],
    "XLU": ["NEE", "DUK", "SO", "AEP", "EXC"],
    "XLRE": ["AMT", "PLD", "EQIX", "SPG", "O"],
    "XLB": ["LIN", "SHW", "APD", "FCX", "NEM"],
}

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
            
    # 2. Mock collected_04_etf_holdings (Top 5 per ETF)
    holdings_per_etf = {}

    for etf_ticker in tickers:
        top_list = ETF_TOP_HOLDINGS.get(etf_ticker)
        if not top_list:
            # Fallback to a safe, valid set of large-cap tickers
            top_list = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]
        holdings_per_etf[etf_ticker] = [(t, t) for t in top_list]

    print("Seeding holdings and stock history...")
    all_stocks = set()
    for etf, stocks in holdings_per_etf.items():
        for i, (ticker, name) in enumerate(stocks):
            _validate_ticker(ticker)
            all_stocks.add((ticker, name))
            cur.execute("""
                INSERT INTO collected_04_etf_holdings 
                (etf_ticker, holding_ticker, holding_name, holding_percent, as_of_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (etf_ticker, holding_ticker, as_of_date) DO NOTHING
            """, (etf, ticker, name, 10.0 - i, end_date))

    # 3. Mock collected_06_daily_stock_history
    for ticker, name in all_stocks:
        _validate_ticker(ticker)
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
