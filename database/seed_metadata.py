#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import psycopg2

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import DB_CONFIG, BENCHMARK_TICKERS, SECTOR_ETFS

def seed_etf_metadata():
    print(f"Connecting to database: {DB_CONFIG['host']}:{DB_CONFIG['port']}...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # 1. Clear established metadata
        cur.execute("DELETE FROM collected_00_meta_etf")
        
        # 2. Add Benchmark ETFs
        print("Seeding benchmark ETFs...")
        for ticker in BENCHMARK_TICKERS:
            cur.execute(
                "INSERT INTO collected_00_meta_etf (ticker, etf_type) VALUES (%s, 'benchmark') ON CONFLICT (ticker) DO NOTHING",
                (ticker,)
            )
            print(f" Added benchmark: {ticker}")
        
        # 3. Add Sector ETFs
        print("Seeding sector ETFs...")
        for sector, ticker in SECTOR_ETFS.items():
            # If already exists as benchmark, change to 'both' or keep as is?
            # Usually they are distinct in this project's current config.
            cur.execute(
                "INSERT INTO collected_00_meta_etf (ticker, etf_type, sector_name) VALUES (%s, 'sector', %s) "
                "ON CONFLICT (ticker) DO UPDATE SET etf_type = 'both', sector_name = EXCLUDED.sector_name",
                (ticker, sector)
            )
            print(f" Added sector: {ticker} ({sector})")
        
        conn.commit()
        cur.close()
        conn.close()
        print("=" * 40)
        print("Seeded ETF metadata successfully.")
        print("=" * 40)
    except Exception as e:
        print(f"Error seeding metadata: {e}")
        sys.exit(1)

if __name__ == "__main__":
    seed_etf_metadata()
