#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FastAPI routes for stock data endpoints
"""

from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, date
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from database.db_helper import DatabaseHelper
from api.models.schemas import StockLatest, StockHistory, SectorInfo

router = APIRouter(prefix="/stocks", tags=["stocks"])


@router.get("/{symbol}/latest", response_model=StockLatest)
async def get_latest_stock(symbol: str):
    """Get latest daily stock data (batch collected only)"""
    db = DatabaseHelper()
    
    query = """
        SELECT 
            ticker as symbol, sector, close_price as avg_price, volume as total_volume,
            low_price as min_price, high_price as max_price, 1 as record_count,
            trade_date as window_start, trade_date as window_end, created_at as processed_at
        FROM collected_daily_stock_history
        WHERE ticker = %s
        ORDER BY trade_date DESC
        LIMIT 1
    """
    
    result = db.fetch_one(query, (symbol.upper(),))
    
    if not result:
        raise HTTPException(status_code=404, detail=f"No data found for symbol: {symbol}")
    
    return StockLatest(**result)


@router.get("/{symbol}/history", response_model=list[StockHistory])
async def get_history(
    symbol: str,
    start_date: str = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(20, ge=1, le=500, description="Number of records to return")
):
    """Get historical daily stock data (batch collected only)"""
    db = DatabaseHelper()
    
    query = """
        SELECT 
            ticker as symbol, sector, close_price as avg_price, volume as total_volume,
            low_price as min_price, high_price as max_price, 1 as record_count,
            trade_date as window_start, trade_date as window_end
        FROM collected_daily_stock_history
        WHERE ticker = %s
    """
    params = [symbol.upper()]
    
    if start_date:
        query += " AND trade_date >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND trade_date <= %s"
        params.append(end_date)
    
    query += " ORDER BY trade_date DESC LIMIT %s"
    params.append(limit)
    
    results = db.fetch_all(query, params)
    
    if not results:
        raise HTTPException(
            status_code=404, 
            detail=f"No history found for symbol: {symbol}"
        )
    
    return [StockHistory(**row) for row in results]


@router.get("/portfolio")
async def get_portfolio_allocation(
    period_days: int = Query(20, ge=5, le=20, description="Period for allocation (5, 10, or 20 days)"),
    as_of_date: str = Query(None, description="Specific date (YYYY-MM-DD), default: latest")
):
    """
    Get portfolio allocation for specified period
    
    Args:
        period_days: Analysis period (5, 10, or 20 days)
        as_of_date: Optional date filter (default: latest available)
    
    Returns:
        List of portfolio allocations with ticker, weight, returns, etc.
    """
    db = DatabaseHelper()
    
    # Build query with period filter
    if as_of_date:
        date_filter = "as_of_date = %s"
        date_param = as_of_date
    else:
        date_filter = "as_of_date = (SELECT MAX(as_of_date) FROM analytics_portfolio_allocation WHERE period_days = %s)"
        date_param = period_days
    
    query = f"""
        SELECT 
            as_of_date,
            ticker,
            company_name,
            sector,
            market_cap,
            return_20d as return_pct,
            portfolio_weight as weight,
            allocation_reason,
            rank_20d as rank,
            period_days,
            created_at
        FROM analytics_portfolio_allocation
        WHERE {date_filter}
          AND period_days = %s
        ORDER BY portfolio_weight DESC
    """
    
    params = [date_param, period_days] if as_of_date else [date_param, period_days]
    results = db.fetch_all(query, params)
    
    if not results:
        return []
    
    # Convert date/datetime objects to strings
    for row in results:
        if isinstance(row.get('as_of_date'), date):
            row['as_of_date'] = row['as_of_date'].isoformat()
        if isinstance(row.get('created_at'), datetime):
            row['created_at'] = row['created_at'].isoformat()
        # Convert weight from decimal to percentage
        if row.get('weight') is not None:
            row['weight'] = float(row['weight']) * 100.0
    
    return results


@router.get("/monthly-portfolio")
async def get_monthly_portfolio(
    rebalance_date: str = Query(None, description="Specific rebalance date (YYYY-MM-DD), default: latest")
):
    """
    Get monthly rebalanced portfolio (5d/10d/20d 통합)
    
    매월 마지막 일요일에 생성된 최종 포트폴리오를 반환합니다.
    
    Args:
        rebalance_date: 리밸런싱 날짜 (default: latest)
    
    Returns:
        List of monthly portfolio allocations with final_rank, final_weight, score, source_periods
    """
    db = DatabaseHelper()
    
    # Build query
    if rebalance_date:
        date_filter = "rebalance_date = %s"
        params = [rebalance_date]
    else:
        date_filter = "rebalance_date = (SELECT MAX(rebalance_date) FROM analytics_monthly_portfolio)"
        params = []
    
    query = f"""
        SELECT 
            rebalance_date,
            valid_until,
            ticker,
            company_name,
            sector,
            final_rank as rank,
            final_weight as weight,
            score,
            source_periods,
            return_20d as return_pct,
            market_cap,
            allocation_reason,
            created_at
        FROM analytics_monthly_portfolio
        WHERE {date_filter}
        ORDER BY final_rank ASC
    """
    
    results = db.fetch_all(query, params)
    
    if not results:
        return {
            "message": "No monthly portfolio data available",
            "data": []
        }
    
    # Convert date/datetime objects to strings and weight to percentage
    for row in results:
        if isinstance(row.get('rebalance_date'), date):
            row['rebalance_date'] = row['rebalance_date'].isoformat()
        if isinstance(row.get('valid_until'), date):
            row['valid_until'] = row['valid_until'].isoformat()
        if isinstance(row.get('created_at'), datetime):
            row['created_at'] = row['created_at'].isoformat()
        # Convert weight from decimal to percentage
        if row.get('weight') is not None:
            row['weight'] = float(row['weight']) * 100.0
        # Convert score to float
        if row.get('score') is not None:
            row['score'] = float(row['score'])
    
    # 요약 정보 추가
    total_stocks = len(results)
    total_weight = sum(row.get('weight', 0) for row in results)
    rebal_date = results[0].get('rebalance_date') if results else None
    valid_date = results[0].get('valid_until') if results else None
    
    return {
        "rebalance_date": rebal_date,
        "valid_until": valid_date,
        "total_stocks": total_stocks,
        "total_weight": round(total_weight, 2),
        "data": results
    }

