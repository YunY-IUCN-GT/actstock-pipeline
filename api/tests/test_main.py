#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tests for the FastAPI application
"""

import pytest
from httpx import AsyncClient
from fastapi.testclient import TestClient
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from api.main import app
from api.auth import get_api_key

# Override the API key dependency for testing
def get_test_api_key():
    return "test_api_key"

app.dependency_overrides[get_api_key] = get_test_api_key

client = TestClient(app)

def test_health_check():
    """Test the health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "actstock-api"}

def test_root():
    """Test the root endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {
        "message": "Active Stock Allocation API",
        "docs": "/docs",
        "health": "/health",
    }

@pytest.mark.asyncio
async def test_get_latest_stock_not_found():
    """Test getting latest stock data for a symbol that does not exist"""
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/stocks/UNKNOWN/latest")
    assert response.status_code == 404
    assert response.json() == {"detail": "No data found for symbol: UNKNOWN"}

@pytest.mark.asyncio
async def test_get_history_not_found():
    """Test getting historical stock data for a symbol that does not exist"""
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/stocks/UNKNOWN/history")
    assert response.status_code == 404
    assert response.json() == {"detail": "No history found for symbol: UNKNOWN"}
