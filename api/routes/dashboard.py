#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FastAPI routes for dashboard data endpoints
대시보드에서 필요한 모든 데이터를 제공하는 API 엔드포인트
"""

from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from database.db_helper import DatabaseHelper

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

# Benchmark ETF tickers to always include in multi-period views
BENCHMARK_TICKERS = ("SPY", "QQQ", "IWM", "DIA", "EWY", "SCHD")
BENCHMARK_PRIORITY = ("QQQ", "SPY", "IWM", "DIA", "EWY", "SCHD")


@router.get("/etf-holdings")
async def get_etf_holdings_data(
    days: int = Query(60, ge=1, le=365),
    ticker: Optional[str] = Query(None, description="Optional stock ticker filter")
):
    """
    ETF Holdings 일별 성과 데이터 조회
    
    Args:
        days: 조회할 일수 (기본값: 60일)
    
    Returns:
        List of stock daily history records
    """
    db = DatabaseHelper()
    
    query = """
        SELECT
            ticker, company_name, sector, trade_date,
            close_price, price_change_percent, volume, market_cap
        FROM collected_06_daily_stock_history
        WHERE trade_date >= CURRENT_DATE - INTERVAL '%s days'
    """

    params = [days]
    if ticker:
        query += " AND ticker = %s"
        params.append(ticker.upper())

    query += " ORDER BY trade_date DESC, ticker"
    
    results = db.fetch_all(query, params)
    
    if not results:
        return []
    
    # Convert date objects to string for JSON serialization
    for row in results:
        if isinstance(row.get('trade_date'), date):
            row['trade_date'] = row['trade_date'].isoformat()
    
    return results


@router.get("/spy-benchmark")
async def get_spy_benchmark_data(days: int = Query(60, ge=1, le=365)):
    """
    SPY 벤치마크 데이터 조회
    
    Args:
        days: 조회할 일수 (기본값: 60일)
    
    Returns:
        List with columns: trade_date, price_change_percent, close_price
    """
    db = DatabaseHelper()
    
    query = """
        SELECT trade_date, price_change_percent, close_price
        FROM collected_01_daily_etf_ohlc
        WHERE ticker = 'SPY'
        AND trade_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY trade_date DESC
    """
    
    results = db.fetch_all(query, (days,))
    
    if not results:
        return []
    
    # Convert date objects to string
    for row in results:
        if isinstance(row.get('trade_date'), date):
            row['trade_date'] = row['trade_date'].isoformat()
    
    return results


@router.get("/etf-benchmark")
async def get_etf_benchmark_data(ticker: str = Query(...), days: int = Query(60, ge=1, le=365)):
    """
    특정 ETF 벤치마크 데이터 조회

    Args:
        ticker: ETF 티커 (예: SPY, QQQ, IWM)
        days: 조회할 일수 (기본값: 60일)

    Returns:
        List with columns: trade_date, price_change_percent, close_price
    """
    db = DatabaseHelper()

    query = """
        SELECT trade_date, price_change_percent, close_price, ticker
        FROM collected_01_daily_etf_ohlc
        WHERE ticker = %s
        AND trade_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY trade_date DESC
    """

    results = db.fetch_all(query, (ticker, days))

    if not results:
        return []

    # Convert date objects to string
    for row in results:
        if isinstance(row.get('trade_date'), date):
            row['trade_date'] = row['trade_date'].isoformat()

    return results


@router.get("/active-allocations")
async def get_active_allocations(
    period_days: int = Query(20, ge=5, le=20, description="Analysis period (5, 10, or 20 days)"),
    as_of_date: str = Query(None, description="Specific date (YYYY-MM-DD), default: latest")
):
    """
    액티브 포트폴리오 조회 (트렌딩 ETF 기반) - 멀티 기간 지원
    
    Args:
        period_days: 분석 기간 (5, 10, 20일)
        as_of_date: 조회 날짜 (기본값: 최신)
    
    Returns:
        최신 날짜의 액티브 포트폴리오 (선택한 기간 기준)
    """
    db = DatabaseHelper()
    
    if as_of_date:
        date_filter = "as_of_date = %s"
        date_param = as_of_date
    else:
        date_filter = "as_of_date = (SELECT MAX(as_of_date) FROM analytics_05_portfolio_allocation WHERE period_days = %s)"
        date_param = period_days
    
    query = f"""
        SELECT 
            as_of_date,
            ticker,
            company_name,
            sector,
            market_cap,
            return_pct,
            sector_avg,
            is_trending,
            rank,
            portfolio_weight,
            allocation_reason,
            period_days,
            created_at
        FROM analytics_05_portfolio_allocation
        WHERE {date_filter}
          AND period_days = %s
        ORDER BY portfolio_weight DESC
    """
    
    params = [date_param, period_days] if as_of_date else [date_param, period_days]
    results = db.fetch_all(query, params)
    
    if not results:
        return []
    
    # Convert date/datetime objects to string
    for row in results:
        if isinstance(row.get('as_of_date'), date):
            row['as_of_date'] = row['as_of_date'].isoformat()
        if isinstance(row.get('created_at'), datetime):
            row['created_at'] = row['created_at'].isoformat()
    
    return results


@router.get("/sector-trending")
async def get_sector_trending_data():
    """
    섹터별 트렌딩 데이터 조회
    
    Returns:
        List of sector trending information with stock counts and performance
    """
    db = DatabaseHelper()
    
    query = """
        WITH latest_sector_performance AS (
            SELECT 
                sector,
                AVG(price_change_percent) as avg_change_pct,
                COUNT(*) as stock_count,
                SUM(volume) as total_volume,
                MAX(trade_date) as latest_date
            FROM collected_06_daily_stock_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY sector
        )
        SELECT 
            sector,
            avg_change_pct,
            stock_count,
            total_volume,
            latest_date,
            CASE 
                WHEN avg_change_pct > 0 THEN true 
                ELSE false 
            END as is_trending
        FROM latest_sector_performance
        WHERE sector IS NOT NULL
        ORDER BY avg_change_pct DESC
    """
    
    results = db.fetch_all(query)
    
    if not results:
        return []
    
    # Convert date objects to string
    for row in results:
        if isinstance(row.get('latest_date'), date):
            row['latest_date'] = row['latest_date'].isoformat()
    
    return results


@router.get("/monthly-performance")
async def get_monthly_performance(months: int = Query(12, ge=1, le=24)):
    """
    월별 ETF 성과 데이터 조회
    
    Args:
        months: 조회할 월수 (기본값: 12개월)
    
    Returns:
        Monthly performance data for ETFs
    """
    db = DatabaseHelper()
    
    query = """
        WITH monthly_data AS (
            SELECT 
                ticker,
                DATE_TRUNC('month', trade_date) as month,
                FIRST_VALUE(close_price) OVER (
                    PARTITION BY ticker, DATE_TRUNC('month', trade_date)
                    ORDER BY trade_date
                ) as month_open,
                LAST_VALUE(close_price) OVER (
                    PARTITION BY ticker, DATE_TRUNC('month', trade_date)
                    ORDER BY trade_date
                    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as month_close
            FROM collected_01_daily_etf_ohlc
            WHERE trade_date >= CURRENT_DATE - INTERVAL '%s months'
        )
        SELECT DISTINCT
            ticker,
            month,
            month_open,
            month_close,
            ((month_close - month_open) / month_open * 100) as monthly_return
        FROM monthly_data
        ORDER BY month DESC, ticker
    """
    
    results = db.fetch_all(query, (months,))
    
    if not results:
        return []
    
    # Convert date/datetime objects to string
    for row in results:
        if isinstance(row.get('month'), (date, datetime)):
            row['month'] = row['month'].isoformat() if isinstance(row['month'], date) else row['month'].date().isoformat()
    
    return results


@router.get("/realtime-summary")
async def get_realtime_summary():
    """
    ⚠️ DEPRECATED: Real-time collection disabled (yfinance rate limit)
    
    Use batch data from 06_collected_daily_stock_history instead.
    This endpoint returns empty data.
    
    Returns:
        Empty data structure (real-time collection disabled)
    """
    # Return empty data - real-time collection is disabled
    return {
        "total_tickers": 0,
        "avg_change_pct": 0.0,
        "latest_update": None,
        "total_records": 0,
        "warning": "Real-time collection disabled - use batch data endpoints instead"
    }


@router.get("/top-performers")
async def get_top_performers(
    limit: int = Query(10, ge=1, le=50),
    window_days: int = Query(5, ge=1, le=30)
):
    """
    최고 성과 종목 조회
    
    Args:
        limit: 반환할 종목 수 (기본값: 10)
        window_days: 계산 기간 (기본값: 5일)
    
    Returns:
        Top performing stocks based on return percentage
    """
    db = DatabaseHelper()
    
    query = """
        WITH date_range AS (
            SELECT 
                CURRENT_DATE as end_date,
                CURRENT_DATE - INTERVAL '%s days' as start_date
        ),
        stock_prices AS (
            SELECT 
                s.ticker,
                s.company_name,
                s.sector,
                s.trade_date,
                s.close_price,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.trade_date) as rn_start,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.trade_date DESC) as rn_end
            FROM collected_06_daily_stock_history s, date_range dr
            WHERE s.trade_date BETWEEN dr.start_date AND dr.end_date
        ),
        stock_returns AS (
            SELECT 
                start_prices.ticker,
                start_prices.company_name,
                start_prices.sector,
                start_prices.close_price as start_price,
                end_prices.close_price as end_price,
                end_prices.trade_date as latest_date,
                ((end_prices.close_price - start_prices.close_price) / start_prices.close_price * 100) as return_pct
            FROM 
                (SELECT * FROM stock_prices WHERE rn_start = 1) start_prices
            JOIN 
                (SELECT * FROM stock_prices WHERE rn_end = 1) end_prices
            ON start_prices.ticker = end_prices.ticker
            WHERE start_prices.close_price > 0
        )
        SELECT 
            ticker,
            company_name,
            sector,
            start_price,
            end_price as latest_price,
            return_pct,
            latest_date
        FROM stock_returns
        ORDER BY return_pct DESC
        LIMIT %s
    """
    
    results = db.fetch_all(query, (window_days, limit))
    
    if not results:
        return []
    
    # Convert date objects to string
    for row in results:
        if isinstance(row.get('latest_date'), date):
            row['latest_date'] = row['latest_date'].isoformat()
    
    return results


@router.get("/top-performers-by-sector")
async def get_top_performers_by_sector(
    sector: str = Query(..., description="Sector name (e.g., Financial, Technology)"),
    limit: int = Query(2, ge=1, le=10),
    window_days: int = Query(5, ge=1, le=30)
):
    """
    섹터별 최고 성과 종목 조회
    
    Args:
        sector: 섹터명
        limit: 반환할 종목 수 (기본값: 2)
        window_days: 계산 기간 (기본값: 5일)
    
    Returns:
        Top performing stocks in the sector
    """
    db = DatabaseHelper()
    
    query = """
        WITH date_range AS (
            SELECT 
                CURRENT_DATE as end_date,
                CURRENT_DATE - INTERVAL '%s days' as start_date
        ),
        stock_prices AS (
            SELECT 
                s.ticker,
                s.company_name,
                s.sector,
                s.trade_date,
                s.close_price,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.trade_date) as rn_start,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.trade_date DESC) as rn_end
            FROM collected_06_daily_stock_history s, date_range dr
            WHERE s.trade_date BETWEEN dr.start_date AND dr.end_date
              AND s.sector = %s
        ),
        stock_returns AS (
            SELECT 
                start_prices.ticker,
                start_prices.company_name,
                start_prices.sector,
                start_prices.close_price as start_price,
                end_prices.close_price as end_price,
                end_prices.trade_date as latest_date,
                ((end_prices.close_price - start_prices.close_price) / start_prices.close_price * 100) as return_pct
            FROM 
                (SELECT * FROM stock_prices WHERE rn_start = 1) start_prices
            JOIN 
                (SELECT * FROM stock_prices WHERE rn_end = 1) end_prices
            ON start_prices.ticker = end_prices.ticker
            WHERE start_prices.close_price > 0
        )
        SELECT 
            ticker,
            company_name,
            sector,
            start_price,
            end_price as latest_price,
            return_pct,
            latest_date
        FROM stock_returns
        ORDER BY return_pct DESC
        LIMIT %s
    """
    
    results = db.fetch_all(query, (window_days, sector, limit))
    
    if not results:
        return []
    
    for row in results:
        if isinstance(row.get('latest_date'), date):
            row['latest_date'] = row['latest_date'].isoformat()
    
    return results


@router.get("/trending-etf-top-holdings")
async def get_trending_etf_top_holdings(
    period_days: int = Query(20, ge=5, le=20),
    limit: int = Query(5, ge=1, le=20)
):
    """
    트렌딩 ETF 기준 상위 보유 종목 조회
    
    Args:
        period_days: ETF 수익률 계산 기간 (5, 10, 20)
        limit: 반환할 종목 수
    
    Returns:
        ETF별 상위 보유 종목과 포트폴리오 후보
    """
    db = DatabaseHelper()

    benchmark_in = "('" + "','".join(BENCHMARK_TICKERS) + "')"
    query = f"""
        WITH date_range AS (
            SELECT 
                CURRENT_DATE as end_date,
                CURRENT_DATE - INTERVAL '%s days' as start_date
        ),
        etf_prices AS (
            SELECT 
                o.ticker,
                o.trade_date,
                o.close_price,
                ROW_NUMBER() OVER (PARTITION BY o.ticker ORDER BY o.trade_date) as rn_start,
                ROW_NUMBER() OVER (PARTITION BY o.ticker ORDER BY o.trade_date DESC) as rn_end
            FROM collected_01_daily_etf_ohlc o, date_range dr
            WHERE o.trade_date BETWEEN dr.start_date AND dr.end_date
        ),
        etf_returns AS (
            SELECT 
                s.ticker,
                s.close_price as start_price,
                e.close_price as end_price,
                ((e.close_price - s.close_price) / s.close_price * 100) as return_pct
            FROM (SELECT * FROM etf_prices WHERE rn_start = 1) s
            JOIN (SELECT * FROM etf_prices WHERE rn_end = 1) e
            ON s.ticker = e.ticker
            WHERE s.close_price > 0
        ),
        spy AS (
            SELECT return_pct as spy_return
            FROM etf_returns
            WHERE ticker = 'SPY'
        ),
        target_etfs AS (
            SELECT 
                er.ticker,
                er.return_pct,
                CASE WHEN er.ticker IN {benchmark_in} THEN true ELSE false END as is_benchmark,
                CASE 
                    WHEN er.ticker <> 'SPY'
                     AND er.return_pct > COALESCE(sp.spy_return, 0)
                     AND er.return_pct > 0
                    THEN true ELSE false
                END as is_trending
            FROM etf_returns er
            LEFT JOIN spy sp ON true
            WHERE er.ticker IN {benchmark_in}
               OR (er.ticker <> 'SPY'
                   AND er.return_pct > COALESCE(sp.spy_return, 0)
                   AND er.return_pct > 0)
        ),
        latest_holdings AS (
            SELECT h.*
            FROM collected_04_etf_holdings h
            JOIN (
                SELECT etf_ticker, MAX(as_of_date) as as_of_date
                FROM collected_04_etf_holdings
                GROUP BY etf_ticker
            ) m
            ON h.etf_ticker = m.etf_ticker
           AND h.as_of_date = m.as_of_date
        ),
        latest_market_cap AS (
            SELECT DISTINCT ON (s.ticker)
                s.ticker,
                s.company_name,
                s.market_cap,
                s.trade_date
            FROM collected_06_daily_stock_history s, date_range dr
            WHERE s.trade_date BETWEEN dr.start_date AND dr.end_date
            ORDER BY s.ticker, s.trade_date DESC
        ),
        holdings_ranked AS (
            SELECT 
                t.ticker as etf_ticker,
                t.return_pct as etf_return_pct,
                t.is_benchmark,
                t.is_trending,
                lh.holding_ticker,
                COALESCE(lh.holding_name, mc.company_name) as holding_name,
                mc.market_cap,
                mc.trade_date,
                lh.as_of_date,
                ROW_NUMBER() OVER (
                    PARTITION BY t.ticker 
                    ORDER BY mc.market_cap DESC NULLS LAST
                ) as mcap_rank
            FROM target_etfs t
            JOIN latest_holdings lh
              ON lh.etf_ticker = t.ticker
            LEFT JOIN latest_market_cap mc
              ON mc.ticker = lh.holding_ticker
        ),
        stock_prices AS (
            SELECT 
                s.ticker,
                s.trade_date,
                s.close_price,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.trade_date) as rn_start,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.trade_date DESC) as rn_end
            FROM collected_06_daily_stock_history s, date_range dr
            WHERE s.trade_date BETWEEN dr.start_date AND dr.end_date
        ),
        stock_returns AS (
            SELECT 
                s.ticker,
                s.close_price as start_price,
                e.close_price as end_price,
                ((e.close_price - s.close_price) / s.close_price * 100) as return_pct
            FROM (SELECT * FROM stock_prices WHERE rn_start = 1) s
            JOIN (SELECT * FROM stock_prices WHERE rn_end = 1) e
            ON s.ticker = e.ticker
            WHERE s.close_price > 0
        )
        SELECT 
            hr.etf_ticker,
            hr.etf_return_pct,
            hr.is_benchmark,
            hr.is_trending,
            hr.holding_ticker,
            hr.holding_name,
            hr.market_cap,
            hr.trade_date,
            hr.as_of_date,
            hr.mcap_rank,
            sr.return_pct as holding_return_pct
        FROM holdings_ranked hr
        LEFT JOIN stock_returns sr
          ON sr.ticker = hr.holding_ticker
        WHERE hr.mcap_rank <= %s
        ORDER BY hr.is_benchmark DESC, hr.etf_return_pct DESC NULLS LAST, hr.etf_ticker, hr.mcap_rank
    """

    results = db.fetch_all(query, (period_days, limit))

    if not results:
        return {
            "period_days": period_days,
            "etfs": [],
            "portfolio_pick": None
        }

    etf_map: Dict[str, Dict[str, Any]] = {}
    benchmark_holdings = []
    trending_holdings = []

    for row in results:
        if isinstance(row.get('as_of_date'), date):
            row['as_of_date'] = row['as_of_date'].isoformat()
        if isinstance(row.get('trade_date'), date):
            row['trade_date'] = row['trade_date'].isoformat()

        etf_ticker = row.get('etf_ticker')
        if etf_ticker not in etf_map:
            etf_map[etf_ticker] = {
                "etf_ticker": etf_ticker,
                "etf_return_pct": float(row.get('etf_return_pct') or 0),
                "is_benchmark": bool(row.get('is_benchmark')),
                "is_trending": bool(row.get('is_trending')),
                "holdings": []
            }

        holding = {
            "holding_ticker": row.get('holding_ticker'),
            "holding_name": row.get('holding_name'),
            "market_cap": row.get('market_cap'),
            "holding_return_pct": row.get('holding_return_pct'),
            "mcap_rank": row.get('mcap_rank')
        }
        etf_map[etf_ticker]["holdings"].append(holding)

        if row.get('is_benchmark'):
            benchmark_holdings.append({**holding, "etf_ticker": etf_ticker})
        if row.get('is_trending'):
            trending_holdings.append({**holding, "etf_ticker": etf_ticker, "etf_return_pct": row.get('etf_return_pct')})

    # Determine portfolio pick: best performer among top holdings of trending ETFs
    portfolio_pick = None
    trending_candidates = [
        h for h in trending_holdings
        if h.get("holding_return_pct") is not None
    ]
    if trending_candidates:
        trending_candidates.sort(
            key=lambda h: (float(h.get("holding_return_pct") or 0), float(h.get("market_cap") or 0)),
            reverse=True
        )
        portfolio_pick = trending_candidates[0]

        # If the portfolio pick is in any benchmark ETF top-5, prefer benchmark for display
        benchmark_match = [
            b for b in benchmark_holdings
            if b.get("holding_ticker") == portfolio_pick.get("holding_ticker")
        ]
        if benchmark_match:
            benchmark_match.sort(
                key=lambda b: (
                    BENCHMARK_PRIORITY.index(b.get("etf_ticker")) if b.get("etf_ticker") in BENCHMARK_PRIORITY else 999
                )
            )
            preferred = benchmark_match[0]
            portfolio_pick["source_etf"] = preferred.get("etf_ticker")
            portfolio_pick["source_type"] = "benchmark"
        else:
            portfolio_pick["source_etf"] = portfolio_pick.get("etf_ticker")
            portfolio_pick["source_type"] = "trending"

    # Normalize holdings ordering
    for etf in etf_map.values():
        etf["holdings"] = sorted(etf["holdings"], key=lambda h: h.get("mcap_rank") or 999)

    etfs = list(etf_map.values())
    etfs.sort(key=lambda e: (0 if e.get("is_benchmark") else 1, -(e.get("etf_return_pct") or 0)))

    return {
        "period_days": period_days,
        "etfs": etfs,
        "portfolio_pick": portfolio_pick
    }


@router.get("/etf-list")
async def get_etf_list():
    """
    추적 중인 ETF 목록 조회
    
    Returns:
        List of all tracked ETFs with metadata
    """
    db = DatabaseHelper()
    
    query = """
        SELECT 
            ticker,
            etf_type,
            sector_name,
            description
        FROM collected_00_meta_etf
        ORDER BY ticker
    """
    
    results = db.fetch_all(query)
    
    return results if results else []
