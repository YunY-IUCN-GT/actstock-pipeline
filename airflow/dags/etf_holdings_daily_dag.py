#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - ETF holdings daily close price collection.
Uses latest holdings snapshot from benchmark_etf_holdings (benchmark + sector ETFs).
"""

from datetime import datetime, timedelta
import logging
import os
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import yfinance as yf

import sys
sys.path.insert(0, '/opt/airflow/project')

from database.db_helper import DatabaseHelper

logger = logging.getLogger(__name__)

REQUEST_DELAY = int(os.getenv("HOLDINGS_REQUEST_DELAY", "10"))
MAX_RETRIES = int(os.getenv("HOLDINGS_MAX_RETRIES", "2"))

FALLBACK_HOLDINGS = {
    'Technology': ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META', 'TSLA', 'AMD', 'INTC', 'QCOM', 'PYPL'],
    'Healthcare': ['UNH', 'JNJ', 'LLY', 'PFE', 'ABBV', 'MRK', 'AZN', 'AMGN', 'GILD', 'VRTX'],
    'Financial': ['JPM', 'BAC', 'GS', 'MS', 'AXP', 'C', 'WFC', 'BK', 'CME', 'ICE'],
    'Consumer Cyclical': ['AMZN', 'TSLA', 'HD', 'NKE', 'MCD', 'SBUX', 'DIS', 'TJX', 'MAR', 'CMG'],
    'Communication': ['GOOGL', 'META', 'NFLX', 'CMCSA', 'VZ', 'T', 'TMUS', 'DISH', 'LUMN', 'PARA'],
    'Industrial': ['BA', 'CAT', 'GE', 'LMT', 'RTX', 'MMM', 'HON', 'ETN', 'EMR', 'ROK'],
    'Consumer Defensive': ['WMT', 'PG', 'KO', 'PEP', 'CL', 'MO', 'KHC', 'MDLZ', 'GIS', 'SJM'],
    'Energy': ['XOM', 'CVX', 'COP', 'EOG', 'MPC', 'PSX', 'SLB', 'HES', 'OXY', 'MAR'],
    'Utilities': ['NEE', 'DUK', 'SO', 'EXC', 'SRE', 'AEP', 'XEL', 'PPL', 'AWK', 'CMS'],
    'Real Estate': ['PLD', 'AMT', 'DLR', 'EQIX', 'REXR', 'VTR', 'WELL', 'AVB', 'O', 'SPG'],
    'Basic Materials': ['LIN', 'SHW', 'LYB', 'MOS', 'APD', 'NEM', 'FCX', 'DD', 'AA', 'ALB']
}


def load_holdings_snapshot(db: DatabaseHelper):
    query = """
        WITH latest AS (
            SELECT etf_ticker, MAX(as_of_date) AS as_of_date
            FROM benchmark_etf_holdings
            GROUP BY etf_ticker
        )
        SELECT h.etf_ticker, h.holding_ticker, h.holding_name
        FROM benchmark_etf_holdings h
        JOIN latest l
          ON l.etf_ticker = h.etf_ticker
         AND l.as_of_date = h.as_of_date
        ORDER BY h.etf_ticker, h.holding_percent DESC
    """
    rows = db.fetch_all(query)
    return rows


def build_sector_map(db: DatabaseHelper):
    sector_map = {}

    history_rows = db.fetch_all(
        """
        SELECT DISTINCT ON (ticker)
            ticker, sector, company_name
        FROM stock_daily_history
        ORDER BY ticker, trade_date DESC
        """
    )
    for row in history_rows:
        sector = row.get("sector")
        if sector:
            sector_map[row["ticker"]] = (sector, row.get("company_name") or row["ticker"])

    market_rows = db.fetch_all(
        """
        SELECT DISTINCT ON (ticker)
            ticker, sector, company_name
        FROM stock_market_data
        ORDER BY ticker, timestamp DESC
        """
    )
    for row in market_rows:
        if row["ticker"] in sector_map:
            continue
        sector = row.get("sector")
        if sector:
            sector_map[row["ticker"]] = (sector, row.get("company_name") or row["ticker"])

    return sector_map


def download_ticker(ticker: str):
    for attempt in range(MAX_RETRIES):
        try:
            data = yf.download(ticker, period='5d', progress=False, threads=False)
            if data is None or data.empty:
                continue
            if isinstance(data.columns, pd.MultiIndex):
                if ticker in data.columns.get_level_values(-1):
                    data = data.xs(ticker, axis=1, level=-1)
                elif ticker in data.columns.get_level_values(0):
                    data = data.xs(ticker, axis=1, level=0)
                else:
                    data = data.droplevel(-1, axis=1)
            return data
        except Exception as exc:
            logger.warning("%s download failed: %s", ticker, exc)
        time.sleep(REQUEST_DELAY * (2 ** attempt))
    return None


def collect_daily_etf_holdings(**context):
    db = DatabaseHelper()
    trade_date = datetime.now().date()

    logger.info("ETF holdings daily collection: %s", trade_date)

    snapshot_rows = load_holdings_snapshot(db)
    sector_map = build_sector_map(db)

    if snapshot_rows:
        tickers = sorted({row["holding_ticker"] for row in snapshot_rows})
        name_map = {row["holding_ticker"]: row.get("holding_name") for row in snapshot_rows}
        fallback = False
    else:
        tickers = sorted({t for tickers in FALLBACK_HOLDINGS.values() for t in tickers})
        name_map = {t: t for t in tickers}
        fallback = True

    if fallback:
        logger.warning("No holdings snapshot found. Using fallback holdings list.")
    else:
        logger.info("Using %s holdings tickers from snapshot.", len(tickers))

    insert_query = """
        INSERT INTO stock_daily_history
        (ticker, company_name, sector, trade_date,
         open_price, high_price, low_price, close_price,
         volume, price_change_percent, created_at)
        VALUES (%(ticker)s, %(company_name)s, %(sector)s, %(trade_date)s,
                %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
                %(volume)s, %(price_change_percent)s, NOW())
        ON CONFLICT (ticker, trade_date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            price_change_percent = EXCLUDED.price_change_percent
    """

    saved = 0

    for idx, ticker in enumerate(tickers, 1):
        data = download_ticker(ticker)
        if data is None or data.empty:
            logger.warning("No data for %s", ticker)
            time.sleep(REQUEST_DELAY)
            continue

        latest = data.iloc[-1]
        latest_close = float(latest["Close"]) if pd.notna(latest["Close"]) else None
        prev_close_raw = data.iloc[-2]["Close"] if len(data) > 1 else latest["Close"]
        prev_close = float(prev_close_raw) if pd.notna(prev_close_raw) else latest_close
        if prev_close and latest_close is not None:
            change_percent = ((latest_close - prev_close) / prev_close) * 100
        else:
            change_percent = 0.0

        sector, company = sector_map.get(ticker, ("Unknown", name_map.get(ticker) or ticker))

        payload = {
            "ticker": ticker,
            "company_name": company,
            "sector": sector,
            "trade_date": trade_date,
            "open_price": float(latest["Open"]) if pd.notna(latest["Open"]) else None,
            "high_price": float(latest["High"]) if pd.notna(latest["High"]) else None,
            "low_price": float(latest["Low"]) if pd.notna(latest["Low"]) else None,
            "close_price": latest_close,
            "volume": int(latest["Volume"]) if pd.notna(latest["Volume"]) else 0,
            "price_change_percent": float(change_percent),
        }

        if db.execute_query(insert_query, payload):
            saved += 1

        if idx < len(tickers):
            time.sleep(REQUEST_DELAY)

    logger.info("Saved %s holdings rows for %s", saved, trade_date)


def validate_collection(**context):
    db = DatabaseHelper()
    trade_date = datetime.now().date()

    query = "SELECT COUNT(*) AS count FROM stock_daily_history WHERE trade_date = %s"
    result = db.fetch_one(query, (trade_date,))
    count = int(result.get("count", 0)) if result else 0

    logger.info("%s holdings rows for %s", count, trade_date)

    if count == 0:
        logger.warning("No holdings rows inserted")


default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'etf_holdings_daily_collection',
    default_args=default_args,
    description='ETF holdings daily close price collection',
    schedule_interval='0 18 * * 1-5',
    start_date=days_ago(1),
    catchup=False,
    tags=['etf-holdings', 'daily'],
)

collect_task = PythonOperator(
    task_id='collect_etf_holdings',
    python_callable=collect_daily_etf_holdings,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_collection',
    python_callable=validate_collection,
    dag=dag,
)

collect_task >> validate_task
