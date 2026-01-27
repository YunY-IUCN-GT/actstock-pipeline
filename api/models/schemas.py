#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pydantic schemas for API responses
"""

from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List


class StockLatest(BaseModel):
    """Latest stock aggregates response"""
    symbol: str
    sector: Optional[str] = None
    avg_price: float
    total_volume: int
    min_price: float
    max_price: float
    record_count: int
    window_start: datetime
    window_end: datetime
    processed_at: datetime
    
    class Config:
        from_attributes = True


class StockHistory(BaseModel):
    """Historical stock data response"""
    symbol: str
    sector: Optional[str] = None
    avg_price: float
    total_volume: int
    min_price: float
    max_price: float
    record_count: int
    window_start: datetime
    window_end: datetime
    
    class Config:
        from_attributes = True


class SectorInfo(BaseModel):
    """Sector information response"""
    sector: str
    total_market_cap: Optional[int] = None
    avg_change_percent: float
    stock_count: int
    is_trending: bool
    
    class Config:
        from_attributes = True
