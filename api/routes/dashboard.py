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


@router.get("/etf-holdings")
async def get_etf_holdings_data(days: int = Query(60, ge=1, le=365)):
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
        FROM 06_collected_daily_stock_history
        WHERE trade_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY trade_date DESC, ticker
    """
    
    results = db.fetch_all(query, (days,))
    
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
        FROM 01_collected_daily_etf_ohlc
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
        FROM 01_collected_daily_etf_ohlc
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
    
    # Build date filter
    if as_of_date:
        date_filter = "as_of_date = %s"
        date_param = as_of_date
    else:
        date_filter = "as_of_date = (SELECT MAX(as_of_date) FROM 05_analytics_portfolio_allocation WHERE period_days = %s)"
        date_param = period_days
    
    query = f"""
        SELECT 
            as_of_date,
            ticker,
            company_name,
            sector,
            market_cap,
            return_5d,
            sector_avg_5d,
            is_trending_5d,
            rank_5d,
            return_10d,
            sector_avg_10d,
            is_trending_10d,
            rank_10d,
            return_20d,
            sector_avg_20d,
            is_trending_20d,
            rank_20d,
            portfolio_weight,
            allocation_reason,
            period_days,
            created_at
        FROM 05_analytics_portfolio_allocation
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
            FROM 06_collected_daily_stock_history
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
            FROM 01_collected_daily_etf_ohlc
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
            FROM 06_collected_daily_stock_history s, date_range dr
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
        FROM 00_collected_meta_etf
        ORDER BY ticker
    """
    
    results = db.fetch_all(query)
    
    return results if results else []
