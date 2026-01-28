#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
프로젝트 설정 파일
대표 ETF 기반 액티브 주식 배분 (Active Stock Allocation Based on Representative ETFs)
"""

import os
from dotenv import load_dotenv

# .env 파일 로드
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)

# 일반 설정
PROJECT_NAME = "Active Stock Allocation Based on Representative ETFs"
VERSION = "1.0.0"
TIMEZONE = "America/New_York"

# 로깅 설정
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DIR = "logs"
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT = 5

# Kafka 설정
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    "client_id": os.getenv("KAFKA_CLIENT_ID", "actstock-client"),
    "topics": {
        "market_data": os.getenv("KAFKA_MARKET_DATA_TOPIC", "stock-market-data"),
    },
    "consumer_group_id": os.getenv("KAFKA_CONSUMER_GROUP_ID", "actstock-consumer-group"),
    "auto_offset_reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    "enable_auto_commit": os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "True").lower() in ["true", "1", "yes"],
    "max_poll_records": int(os.getenv("KAFKA_MAX_POLL_RECORDS", "500")),
}

def _default_db_host() -> str:
    if os.path.exists("/.dockerenv"):
        return "postgres"
    return "localhost"

# 데이터베이스 설정
DB_CONFIG = {
    "host": os.getenv("DB_HOST", _default_db_host()),
    "port": int(os.getenv("DB_PORT", 5432)),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "dbname": os.getenv("DB_NAME", "stockdb")
}

# 수집 설정
COLLECTION_CONFIG = {
    "interval_seconds": int(os.getenv("COLLECTION_INTERVAL", 3600)),  # 1시간(3600초)마다 수집
    "batch_size": int(os.getenv("COLLECTION_BATCH_SIZE", 10)),
}

# 대시보드 설정
DASHBOARD_CONFIG = {
    "refresh_interval": int(os.getenv("DASHBOARD_REFRESH_INTERVAL", 30)),  # 30초
    "port": int(os.getenv("DASHBOARD_PORT", 8050)),
    "host": os.getenv("DASHBOARD_HOST", "0.0.0.0"),
}

# 섹터 ETF 매핑 (10개 주요 섹터 ETF - XLK는 Technology 대표)
SECTOR_ETFS = {
    'Technology': 'XLK',
    'Healthcare': 'XLV',
    'Financial': 'XLF',
    'Consumer Cyclical': 'XLY',
    'Communication': 'XLC',
    'Industrial': 'XLI',
    'Consumer Defensive': 'XLP',
    'Utilities': 'XLU',
    'Real Estate': 'XLRE',
    'Basic Materials': 'XLB'
}

# 벤치마크 티커 (6개 - 시장 전체 지표 + 국가 + 배당)
# QQQ는 벤치마크 전용 (섹터 ETF는 XLK 사용)
# 총 16개 unique ETF (벤치마크 6개 + 섹터 10개)
BENCHMARK_TICKERS = ['SPY', 'QQQ', 'IWM', 'EWY', 'DIA', 'SCHD']

# 섹터 ETF 티커 리스트 (10개)
SECTOR_ETF_TICKERS = list(SECTOR_ETFS.values())
