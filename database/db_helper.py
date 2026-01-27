#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
데이터베이스 헬퍼 클래스
PostgreSQL 데이터베이스 연결 및 쿼리 실행을 위한 유틸리티 클래스입니다.
"""

import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import sys
import os

# 현재 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import DB_CONFIG

logger = logging.getLogger(__name__)

class DatabaseHelper:
    """PostgreSQL 데이터베이스 연결 및 쿼리 실행을 위한 헬퍼 클래스"""
    
    def __init__(self):
        """데이터베이스 헬퍼 초기화"""
        self.config = DB_CONFIG
        self.conn = None
    
    def connect(self):
        """데이터베이스에 연결"""
        try:
            self.conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                dbname=self.config['dbname']
            )
            logger.info("데이터베이스에 연결되었습니다.")
            return True
        except Exception as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            return False
    
    def disconnect(self):
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("데이터베이스 연결이 종료되었습니다.")
    
    def execute_query(self, query, params=None):
        """쿼리 실행"""
        if not self.conn:
            if not self.connect():
                return False
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params or ())
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"쿼리 실행 실패: {e}")
            self.conn.rollback()
            return False
    
    def fetch_all(self, query, params=None):
        """쿼리 실행 후 모든 결과 반환"""
        if not self.conn:
            if not self.connect():
                return []
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"쿼리 실행 실패: {e}")
            return []
    
    def fetch_one(self, query, params=None):
        """쿼리 실행 후 단일 결과 반환"""
        if not self.conn:
            if not self.connect():
                return None
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchone()
        except Exception as e:
            logger.error(f"쿼리 실행 실패: {e}")
            return None
