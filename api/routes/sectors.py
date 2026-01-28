#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FastAPI routes for sector data endpoints
"""

from fastapi import APIRouter
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from database.db_helper import DatabaseHelper
from api.models.schemas import SectorInfo

router = APIRouter(prefix="/sectors", tags=["sectors"])


@router.get("/", response_model=list[SectorInfo])
async def get_sectors():
    """Get latest sector information (11 sectors)"""
    db = DatabaseHelper()
    
    try:
        etf_query = """
            SELECT DISTINCT ON (m.ticker)
                m.ticker,
                m.sector_name,
                o.trade_date,
                0::numeric AS price_change_percent
            FROM collected_01_daily_etf_ohlc o
            JOIN collected_00_meta_etf m ON m.ticker = o.ticker
            WHERE m.etf_type = 'sector'
            ORDER BY m.ticker, o.trade_date DESC
        """

        results = db.fetch_all(etf_query)

        if not results:
            return []

        count_query = """
            SELECT sector, COUNT(*) AS stock_count
            FROM collected_06_daily_stock_history
            GROUP BY sector
        """
        count_results = db.fetch_all(count_query)
    except Exception:
        return []
    counts_by_sector = {row["sector"]: row["stock_count"] for row in count_results}

    response = []

    for row in results:
        sector = row.get("sector_name")
        if not sector:
            continue

        change_pct = float(row.get("price_change_percent") or 0)
        response.append(
            SectorInfo(
                sector=sector,
                total_market_cap=None,
                avg_change_percent=change_pct,
                stock_count=int(counts_by_sector.get(sector, 0)),
                is_trending=change_pct > 0,
            )
        )

    return response
