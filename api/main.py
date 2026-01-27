#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FastAPI main application
"""

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from api.routes import stocks, sectors, dashboard, etf_holdings
from database.db_helper import DatabaseHelper
from api.auth import get_api_key

# Create FastAPI app
app = FastAPI(
    title="Active Stock Allocation API",
    description="REST API for ETF-based active stock allocation data and analytics",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(stocks.router, dependencies=[Depends(get_api_key)])
app.include_router(sectors.router, dependencies=[Depends(get_api_key)])
app.include_router(dashboard.router, dependencies=[Depends(get_api_key)])
app.include_router(etf_holdings.router, dependencies=[Depends(get_api_key)])

# Initialize database helper
db = DatabaseHelper()


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "service": "actstock-api"}


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Active Stock Allocation API",
        "docs": "/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
