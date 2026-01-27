#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
API Routes - ETF Top Holdings
Endpoints for querying top performing holdings of ETFs
"""

from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_helper import DatabaseHelper

router = APIRouter(prefix="/api/etf-holdings", tags=["ETF Holdings"])


# Pydantic models
class HoldingDetail(BaseModel):
    """Single holding detail"""
    holding_ticker: str
    holding_name: Optional[str]
    holding_sector: Optional[str]
    rank_position: int = Field(..., ge=1, le=5, description="Rank 1-5")
    avg_return: Optional[float] = Field(None, description="Average daily return %")
    total_return: Optional[float] = Field(None, description="Total return % over period")
    volatility: Optional[float] = Field(None, description="Standard deviation of returns")
    avg_volume: Optional[int] = Field(None, description="Average daily volume")
    current_price: Optional[float]
    market_cap: Optional[int]


class ETFTopHoldings(BaseModel):
    """Top holdings for a single ETF"""
    etf_ticker: str
    etf_type: str
    sector_name: Optional[str]
    time_period: int = Field(..., description="Time period in days (5, 10, or 20)")
    as_of_date: date
    holdings: List[HoldingDetail]


class HoldingsSummary(BaseModel):
    """Summary statistics"""
    total_etfs: int
    time_period: int
    as_of_date: date
    avg_holdings_per_etf: float


@router.get("/top-holdings/{etf_ticker}", response_model=List[ETFTopHoldings])
def get_etf_top_holdings(
    etf_ticker: str,
    time_period: Optional[int] = Query(None, description="Time period: 5, 10, or 20 days"),
    as_of_date: Optional[date] = Query(None, description="Date (default: latest)")
):
    """
    Get top 5 holdings for a specific ETF
    
    - **etf_ticker**: ETF symbol (e.g., XLK, SPY)
    - **time_period**: Optional filter by period (5, 10, or 20 days)
    - **as_of_date**: Optional date (default: most recent)
    """
    db = DatabaseHelper()
    
    try:
        # Build query
        query = """
            SELECT 
                etf_ticker,
                etf_type,
                sector_name,
                time_period,
                as_of_date,
                holding_ticker,
                holding_name,
                holding_sector,
                rank_position,
                avg_return,
                total_return,
                volatility,
                avg_volume,
                current_price,
                market_cap
            FROM 09_analytics_etf_top_holdings
            WHERE etf_ticker = %(etf_ticker)s
        """
        
        params = {'etf_ticker': etf_ticker.upper()}
        
        if time_period is not None:
            if time_period not in [5, 10, 20]:
                raise HTTPException(400, "time_period must be 5, 10, or 20")
            query += " AND time_period = %(time_period)s"
            params['time_period'] = time_period
        
        if as_of_date is not None:
            query += " AND as_of_date = %(as_of_date)s"
            params['as_of_date'] = as_of_date
        else:
            # Use latest date
            query += " AND as_of_date = (SELECT MAX(as_of_date) FROM 09_analytics_etf_top_holdings WHERE etf_ticker = %(etf_ticker)s)"
        
        query += " ORDER BY time_period, rank_position"
        
        rows = db.fetch_all(query, params)
        
        if not rows:
            raise HTTPException(404, f"No data found for ETF {etf_ticker}")
        
        # Group by time period
        results = {}
        for row in rows:
            period = row['time_period']
            
            if period not in results:
                results[period] = {
                    'etf_ticker': row['etf_ticker'],
                    'etf_type': row['etf_type'],
                    'sector_name': row['sector_name'],
                    'time_period': period,
                    'as_of_date': row['as_of_date'],
                    'holdings': []
                }
            
            results[period]['holdings'].append({
                'holding_ticker': row['holding_ticker'],
                'holding_name': row['holding_name'],
                'holding_sector': row['holding_sector'],
                'rank_position': row['rank_position'],
                'avg_return': float(row['avg_return']) if row['avg_return'] else None,
                'total_return': float(row['total_return']) if row['total_return'] else None,
                'volatility': float(row['volatility']) if row['volatility'] else None,
                'avg_volume': row['avg_volume'],
                'current_price': float(row['current_price']) if row['current_price'] else None,
                'market_cap': row['market_cap']
            })
        
        return list(results.values())
        
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"Database error: {str(exc)}")
    finally:
        db.disconnect()


@router.get("/all-holdings", response_model=List[ETFTopHoldings])
def get_all_etf_holdings(
    time_period: int = Query(..., description="Time period: 5, 10, or 20 days"),
    etf_type: Optional[str] = Query(None, description="Filter by ETF type: benchmark or sector"),
    as_of_date: Optional[date] = Query(None, description="Date (default: latest)"),
    limit: int = Query(50, ge=1, le=100, description="Max ETFs to return")
):
    """
    Get top holdings for all ETFs
    
    - **time_period**: Required - 5, 10, or 20 days
    - **etf_type**: Optional filter - 'benchmark' or 'sector'
    - **as_of_date**: Optional date (default: most recent)
    - **limit**: Max number of ETFs (default: 50)
    """
    if time_period not in [5, 10, 20]:
        raise HTTPException(400, "time_period must be 5, 10, or 20")
    
    db = DatabaseHelper()
    
    try:
        query = """
            SELECT 
                etf_ticker,
                etf_type,
                sector_name,
                time_period,
                as_of_date,
                holding_ticker,
                holding_name,
                holding_sector,
                rank_position,
                avg_return,
                total_return,
                volatility,
                avg_volume,
                current_price,
                market_cap
            FROM 09_analytics_etf_top_holdings
            WHERE time_period = %(time_period)s
        """
        
        params = {'time_period': time_period}
        
        if etf_type is not None:
            if etf_type not in ['benchmark', 'sector']:
                raise HTTPException(400, "etf_type must be 'benchmark' or 'sector'")
            query += " AND etf_type = %(etf_type)s"
            params['etf_type'] = etf_type
        
        if as_of_date is not None:
            query += " AND as_of_date = %(as_of_date)s"
            params['as_of_date'] = as_of_date
        else:
            query += " AND as_of_date = (SELECT MAX(as_of_date) FROM 09_analytics_etf_top_holdings WHERE time_period = %(time_period)s)"
        
        query += " ORDER BY etf_ticker, rank_position LIMIT %(limit)s"
        params['limit'] = limit * 5  # Each ETF has up to 5 holdings
        
        rows = db.fetch_all(query, params)
        
        if not rows:
            raise HTTPException(404, "No data found")
        
        # Group by ETF
        results = {}
        for row in rows:
            ticker = row['etf_ticker']
            
            if ticker not in results:
                results[ticker] = {
                    'etf_ticker': ticker,
                    'etf_type': row['etf_type'],
                    'sector_name': row['sector_name'],
                    'time_period': row['time_period'],
                    'as_of_date': row['as_of_date'],
                    'holdings': []
                }
            
            results[ticker]['holdings'].append({
                'holding_ticker': row['holding_ticker'],
                'holding_name': row['holding_name'],
                'holding_sector': row['holding_sector'],
                'rank_position': row['rank_position'],
                'avg_return': float(row['avg_return']) if row['avg_return'] else None,
                'total_return': float(row['total_return']) if row['total_return'] else None,
                'volatility': float(row['volatility']) if row['volatility'] else None,
                'avg_volume': row['avg_volume'],
                'current_price': float(row['current_price']) if row['current_price'] else None,
                'market_cap': row['market_cap']
            })
        
        return list(results.values())[:limit]
        
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"Database error: {str(exc)}")
    finally:
        db.disconnect()


@router.get("/summary", response_model=HoldingsSummary)
def get_holdings_summary(
    time_period: int = Query(20, description="Time period: 5, 10, or 20 days")
):
    """
    Get summary statistics for ETF holdings
    
    - **time_period**: Time period in days (default: 20)
    """
    if time_period not in [5, 10, 20]:
        raise HTTPException(400, "time_period must be 5, 10, or 20")
    
    db = DatabaseHelper()
    
    try:
        query = """
            SELECT 
                COUNT(DISTINCT etf_ticker) as total_etfs,
                COUNT(*) as total_holdings,
                MAX(as_of_date) as as_of_date
            FROM 09_analytics_etf_top_holdings
            WHERE time_period = %(time_period)s
        """
        
        result = db.fetch_one(query, {'time_period': time_period})
        
        if not result or result['total_etfs'] == 0:
            raise HTTPException(404, f"No data found for {time_period}-day period")
        
        avg_holdings = result['total_holdings'] / result['total_etfs'] if result['total_etfs'] > 0 else 0
        
        return {
            'total_etfs': result['total_etfs'],
            'time_period': time_period,
            'as_of_date': result['as_of_date'],
            'avg_holdings_per_etf': round(avg_holdings, 2)
        }
        
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"Database error: {str(exc)}")
    finally:
        db.disconnect()


@router.get("/comparison/{holding_ticker}")
def compare_holding_across_etfs(
    holding_ticker: str,
    time_period: int = Query(20, description="Time period: 5, 10, or 20 days")
):
    """
    Compare a single stock's performance across different ETFs
    
    - **holding_ticker**: Stock ticker to compare
    - **time_period**: Time period in days (default: 20)
    """
    if time_period not in [5, 10, 20]:
        raise HTTPException(400, "time_period must be 5, 10, or 20")
    
    db = DatabaseHelper()
    
    try:
        query = """
            SELECT 
                etf_ticker,
                etf_type,
                sector_name,
                rank_position,
                total_return,
                volatility,
                as_of_date
            FROM 09_analytics_etf_top_holdings
            WHERE holding_ticker = %(holding_ticker)s
              AND time_period = %(time_period)s
              AND as_of_date = (SELECT MAX(as_of_date) FROM 09_analytics_etf_top_holdings WHERE time_period = %(time_period)s)
            ORDER BY rank_position
        """
        
        rows = db.fetch_all(query, {
            'holding_ticker': holding_ticker.upper(),
            'time_period': time_period
        })
        
        if not rows:
            raise HTTPException(404, f"Stock {holding_ticker} not found in any ETF top holdings")
        
        return {
            'holding_ticker': holding_ticker.upper(),
            'time_period': time_period,
            'as_of_date': rows[0]['as_of_date'],
            'etf_appearances': len(rows),
            'etfs': [{
                'etf_ticker': row['etf_ticker'],
                'etf_type': row['etf_type'],
                'sector_name': row['sector_name'],
                'rank_position': row['rank_position'],
                'total_return': float(row['total_return']) if row['total_return'] else None,
                'volatility': float(row['volatility']) if row['volatility'] else None
            } for row in rows]
        }
        
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"Database error: {str(exc)}")
    finally:
        db.disconnect()
