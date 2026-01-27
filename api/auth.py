#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
API Key authentication
"""

from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
import os

API_KEY_HEADER = APIKeyHeader(name="X-API-Key")

API_KEY = os.environ.get("API_KEY")

def get_api_key(api_key_header: str = Security(API_KEY_HEADER)) -> str:
    """
    Retrieves the API key from the header and validates it.

    Args:
        api_key_header: The API key from the X-API-Key header.

    Returns:
        The validated API key.

    Raises:
        HTTPException: If the API key is invalid or missing.
    """
    if api_key_header == API_KEY:
        return api_key_header
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
        )
