#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG - Backfill top 5 ETF holdings by market cap (benchmark + sector ETFs).
Uses yfinance to fetch ETF holdings and stock market caps.
Stores results in collected_04_etf_holdings and collected_06_daily_stock_history.
"""

from datetime import datetime, timedelta
import logging
import os
import re
import time
from typing import Dict, List, Optional

import pandas as pd
import requests
import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/project")

from config.config import BENCHMARK_TICKERS, SECTOR_ETF_TICKERS, SECTOR_ETFS
from database.db_helper import DatabaseHelper

logger = logging.getLogger(__name__)

REQUEST_DELAY = float(os.getenv("HOLDINGS_REQUEST_DELAY", "8"))
MAX_RETRIES = int(os.getenv("HOLDINGS_MAX_RETRIES", "4"))

TICKER_RE = re.compile(r"^[A-Z][A-Z0-9\.\-]{0,9}$")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})


def _normalize_ticker(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    ticker = str(value).strip().upper()
    if not ticker:
        return None
    if TICKER_RE.match(ticker):
        return ticker
    return None


def _infer_ticker_column(df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty:
        return None
    candidates = list(df.columns)
    if not candidates:
        return None
    best_col = None
    best_ratio = 0.0
    sample = df.head(12)
    for col in candidates:
        values = sample[col].tolist()
        if not values:
            continue
        valid = 0
        for val in values:
            if _normalize_ticker(val):
                valid += 1
        ratio = valid / max(len(values), 1)
        if ratio > best_ratio:
            best_ratio = ratio
            best_col = col
    return best_col if best_ratio >= 0.4 else None


def _pick_column(df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    cols = {str(c).lower(): c for c in df.columns}
    for key in keywords:
        for col_lower, col in cols.items():
            if key in col_lower:
                return col
    return None


class RateLimitError(RuntimeError):
    pass


def _get_fund_holdings(etf: yf.Ticker) -> Optional[pd.DataFrame]:
    try:
        funds_data = getattr(etf, "funds_data", None)
        if funds_data is not None:
            top_holdings = getattr(funds_data, "top_holdings", None)
            if isinstance(top_holdings, pd.DataFrame) and not top_holdings.empty:
                return top_holdings
    except Exception as exc:
        msg = str(exc).lower()
        if "too many requests" in msg or "rate limited" in msg or "429" in msg:
            raise RateLimitError(str(exc))
        logger.warning("Funds data not available: %s", exc)
    return None


def _get_stock_info(ticker: str, cache: Dict[str, Dict]) -> Dict:
    if ticker in cache:
        return cache[ticker]
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception as exc:
        logger.warning("Failed to fetch info for %s: %s", ticker, exc)
        info = {}
    cache[ticker] = info
    return info


def _get_market_cap(ticker: str, cache: Dict[str, Dict]) -> Optional[float]:
    info = _get_stock_info(ticker, cache)
    market_cap = info.get("marketCap")
    try:
        return float(market_cap) if market_cap is not None else None
    except Exception:
        return None


def fetch_top_holdings_by_mcap(etf_ticker: str, info_cache: Dict[str, Dict]) -> List[Dict]:
    for attempt in range(MAX_RETRIES):
        try:
            etf = yf.Ticker(etf_ticker, session=SESSION)
            holdings_df = _get_fund_holdings(etf)
            if holdings_df is None or holdings_df.empty:
                logger.warning("No holdings data for %s", etf_ticker)
                return []

            ticker_col = _infer_ticker_column(holdings_df)
            name_col = _pick_column(holdings_df, ["name", "holding", "security"])
            weight_col = _pick_column(holdings_df, ["weight", "%", "asset"])
            shares_col = _pick_column(holdings_df, ["shares"])
            value_col = _pick_column(holdings_df, ["value"])

            holdings = []
            for _, row in holdings_df.iterrows():
                ticker = _normalize_ticker(row.get(ticker_col)) if ticker_col else None
                if not ticker:
                    continue
                holding_name = row.get(name_col) if name_col else ticker
                holding_percent = row.get(weight_col) if weight_col else None
                holding_shares = row.get(shares_col) if shares_col else None
                holding_value = row.get(value_col) if value_col else None
                market_cap = _get_market_cap(ticker, info_cache)
                holdings.append({
                    "holding_ticker": ticker,
                    "holding_name": holding_name,
                    "holding_percent": holding_percent,
                    "holding_shares": holding_shares,
                    "holding_value": holding_value,
                    "market_cap": market_cap,
                })

            holdings = [h for h in holdings if h.get("market_cap")]
            holdings.sort(key=lambda h: h.get("market_cap", 0), reverse=True)
            return holdings[:5]
        except RateLimitError as exc:
            logger.warning("Rate limited for %s (attempt %d): %s", etf_ticker, attempt + 1, exc)
            time.sleep(REQUEST_DELAY * (2 ** attempt))
        except Exception as exc:
            logger.warning("Holdings fetch failed for %s (attempt %d): %s", etf_ticker, attempt + 1, exc)
            time.sleep(REQUEST_DELAY * (2 ** attempt))
    return []


def fetch_stock_snapshot(ticker: str, info_cache: Dict[str, Dict]) -> Optional[Dict]:
    try:
        stock = yf.Ticker(ticker, session=SESSION)
        hist = stock.history(period="5d")
        if hist is None or hist.empty:
            return None
        latest = hist.iloc[-1]
        latest_date = hist.index[-1].date()
        prev_close_raw = hist.iloc[-2]["Close"] if len(hist) > 1 else latest["Close"]
        prev_close = float(prev_close_raw) if pd.notna(prev_close_raw) else None
        latest_close = float(latest["Close"]) if pd.notna(latest["Close"]) else None
        if prev_close and latest_close is not None:
            change_percent = ((latest_close - prev_close) / prev_close) * 100
        else:
            change_percent = 0.0

        info = _get_stock_info(ticker, info_cache)
        return {
            "ticker": ticker,
            "company_name": info.get("longName", ticker),
            "sector": info.get("sector", "Unknown"),
            "trade_date": latest_date,
            "open_price": float(latest["Open"]) if pd.notna(latest["Open"]) else None,
            "high_price": float(latest["High"]) if pd.notna(latest["High"]) else None,
            "low_price": float(latest["Low"]) if pd.notna(latest["Low"]) else None,
            "close_price": latest_close,
            "volume": int(latest["Volume"]) if pd.notna(latest["Volume"]) else 0,
            "price_change_percent": float(change_percent),
            "market_cap": info.get("marketCap", 0),
        }
    except Exception as exc:
        logger.warning("Stock snapshot fetch failed for %s: %s", ticker, exc)
        return None


def backfill_etf_holdings(**context):
    db = DatabaseHelper()
    as_of_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    logger.info("ETF holdings backfill for %s", as_of_date)

    sector_mapping = SECTOR_ETFS
    info_cache: Dict[str, Dict] = {}
    seen_stocks = set()
    ticker_filter_raw = os.getenv("BACKFILL_ETF_TICKERS", "").strip()
    ticker_filter = []
    if ticker_filter_raw:
        ticker_filter = [t.strip().upper() for t in ticker_filter_raw.split(",") if t.strip()]
        logger.info("Ticker filter enabled: %s", ticker_filter)

    insert_holdings_query = """
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

    insert_stock_query = """
        INSERT INTO collected_06_daily_stock_history (
            ticker, company_name, sector, trade_date,
            open_price, high_price, low_price, close_price,
            volume, price_change_percent, market_cap
        ) VALUES (
            %(ticker)s, %(company_name)s, %(sector)s, %(trade_date)s,
            %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
            %(volume)s, %(price_change_percent)s, %(market_cap)s
        )
        ON CONFLICT (ticker, trade_date) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            sector = EXCLUDED.sector,
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            price_change_percent = EXCLUDED.price_change_percent,
            market_cap = EXCLUDED.market_cap
    """

    all_etfs = []
    for ticker in BENCHMARK_TICKERS:
        all_etfs.append((ticker, "benchmark", None))
    for ticker in SECTOR_ETF_TICKERS:
        all_etfs.append((ticker, "sector", sector_mapping.get(ticker)))

    if ticker_filter:
        all_etfs = [item for item in all_etfs if item[0] in ticker_filter]

    for idx, (etf_ticker, etf_type, sector_name) in enumerate(all_etfs, 1):
        logger.info("[%d/%d] %s (%s)", idx, len(all_etfs), etf_ticker, etf_type)

        holdings = fetch_top_holdings_by_mcap(etf_ticker, info_cache)
        if not holdings:
            continue

        for holding in holdings:
            payload = {
                "etf_ticker": etf_ticker,
                "holding_ticker": holding["holding_ticker"],
                "holding_name": holding["holding_name"],
                "holding_percent": holding.get("holding_percent"),
                "holding_shares": holding.get("holding_shares"),
                "holding_value": holding.get("holding_value"),
                "as_of_date": as_of_date,
                "source": "yfinance_mcap_backfill"
            }
            db.execute_query(insert_holdings_query, payload)

            holding_ticker = holding["holding_ticker"]
            if holding_ticker and holding_ticker not in seen_stocks:
                snapshot = fetch_stock_snapshot(holding_ticker, info_cache)
                if snapshot:
                    db.execute_query(insert_stock_query, snapshot)
                    seen_stocks.add(holding_ticker)
                time.sleep(REQUEST_DELAY)

        time.sleep(REQUEST_DELAY)

    logger.info("Backfill complete. Holdings: %d, Stocks: %d", len(all_etfs) * 5, len(seen_stocks))


default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="etf_holdings_mcap_backfill",
    default_args=default_args,
    description="Backfill ETF top 5 holdings by market cap (benchmark + sector)",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["backfill", "etf-holdings", "benchmark", "sector"],
)

PythonOperator(
    task_id="backfill_top5_holdings_mcap",
    python_callable=backfill_etf_holdings,
    dag=dag,
)
