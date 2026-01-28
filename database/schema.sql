-- ========================================
-- 액티브 주식 배분 시스템 데이터베이스 스키마
-- 명명 규칙: [category]_[frequency]_[entity]
-- 
-- 데이터 플로우:
-- 1. yfinance → Kafka Producer → Topic → Consumer → PostgreSQL (collected_*)
-- 2. Airflow → Spark → PostgreSQL (analytics_*, aggregated_*)
-- 3. PostgreSQL → API → Dashboard
-- ========================================

-- ========================================
-- COLLECTED 테이블 (Kafka가 수집한 원천 데이터)
-- Kafka Producer → Topic → Consumer → DB 저장
-- NO REAL-TIME COLLECTION - Scheduled batch only (5-Stage Pipeline)
-- ========================================

-- 일별 주식 히스토리 (Stage 4에서 조건부 수집) (Stage 4에서 조건부 수집)
-- 수집: Stage 4 (12:00 UTC) - Trending ETF holdings만
-- Producer: kafka_producer_conditional_holdings.py
-- Consumer: kafka_consumer_stock_daily.py
-- 읽기: Spark (Stage 5), API
CREATE TABLE IF NOT EXISTS 06_collected_daily_stock_history (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100) NOT NULL,
    trade_date DATE NOT NULL,
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    volume BIGINT,
    price_change_percent NUMERIC(6, 3),
    market_cap BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ticker, trade_date)
);

-- ETF 메타데이터
-- 수집: kafka_producer_etf_meta.py → Topic → Consumer
-- 저장: kafka_consumer_market_writer.py
-- 읽기: Spark, API
CREATE TABLE IF NOT EXISTS 00_collected_meta_etf (
    ticker VARCHAR(10) PRIMARY KEY,
    etf_type VARCHAR(20) NOT NULL, -- benchmark | sector
    sector_name VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO "00_collected_meta_etf" (ticker, etf_type, sector_name)
VALUES
    ('SPY', 'benchmark', NULL),
    ('QQQ', 'both', 'Technology'),
    ('IWM', 'benchmark', NULL),
    ('EWY', 'benchmark', NULL),
    ('DIA', 'benchmark', NULL),
    ('SCHD', 'benchmark', NULL),
    ('XLV', 'sector', 'Healthcare'),
    ('XLF', 'sector', 'Financial'),
    ('XLY', 'sector', 'Consumer Cyclical'),
    ('XLC', 'sector', 'Communication'),
    ('XLI', 'sector', 'Industrial'),
    ('XLP', 'sector', 'Consumer Defensive'),
    ('XLU', 'sector', 'Utilities'),
    ('XLRE', 'sector', 'Real Estate'),
    ('XLB', 'sector', 'Basic Materials')
ON CONFLICT (ticker) DO NOTHING;

-- ETF 일별 OHLCV
-- 수집: kafka_producer_etf_daily.py → Topic → Consumer
-- 저장: kafka_consumer_market_writer.py
-- 읽기: Spark, API
CREATE TABLE IF NOT EXISTS 01_collected_daily_etf_ohlc (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL REFERENCES 00_collected_meta_etf(ticker),
    trade_date DATE NOT NULL,
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    volume BIGINT,
    price_change_percent NUMERIC(6, 3) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ticker, trade_date)
);

-- 벤치마크 ETF 보유 종목 (월간 스냅샷)
-- 수집: kafka_producer_benchmark.py → Topic → Consumer
-- 저장: kafka_consumer_market_writer.py
-- 읽기: Spark
CREATE TABLE IF NOT EXISTS collected_monthly_benchmark_holdings (
    id SERIAL PRIMARY KEY,
    etf_ticker VARCHAR(10) NOT NULL,
    holding_ticker VARCHAR(10) NOT NULL,
    holding_name VARCHAR(255),
    holding_percent NUMERIC(8, 4),
    as_of_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(etf_ticker, holding_ticker, as_of_date)
);

-- ========================================
-- ANALYTICS 테이블 (Spark가 계산한 분석 결과)
-- Airflow → Spark → DB 저장
-- ========================================

-- 액티브 포트폴리오 배분 (트렌딩 섹터 기반)
-- 계산: spark_batch_portfolio.py (Airflow DAG가 스케줄링)
-- 읽기: API, Dashboard
CREATE TABLE IF NOT EXISTS 05_analytics_portfolio_allocation (
    id SERIAL PRIMARY KEY,
    as_of_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100) NOT NULL,
    market_cap BIGINT,
    -- 5일 기준 성과
    return_5d NUMERIC(8, 3),
    sector_avg_5d NUMERIC(8, 3),
    is_trending_5d BOOLEAN DEFAULT FALSE,
    rank_5d INT,
    -- 10일 기준 성과
    return_10d NUMERIC(8, 3),
    sector_avg_10d NUMERIC(8, 3),
    is_trending_10d BOOLEAN DEFAULT FALSE,
    rank_10d INT,
    -- 20일 기준 성과 (포트폴리오 기준)
    return_20d NUMERIC(8, 3),
    sector_avg_20d NUMERIC(8, 3),
    is_trending_20d BOOLEAN DEFAULT FALSE,
    rank_20d INT,
    -- 포트폴리오 비중 (시가총액 가중)
    portfolio_weight NUMERIC(8, 5),
    allocation_reason VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(as_of_date, ticker)
);

-- 월별 섹터 트렌딩 분석
-- 계산: spark_batch_sector.py (Airflow DAG가 스케줄링)
-- 읽기: API
CREATE TABLE IF NOT EXISTS analytics_sector_trending (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    sector VARCHAR(100) NOT NULL,
    stock_count INT,
    avg_monthly_return NUMERIC(6, 3),
    volatility NUMERIC(6, 3),
    positive_days INT,
    total_trading_days INT,
    positive_rate NUMERIC(5, 2),
    best_day NUMERIC(6, 3),
    worst_day NUMERIC(6, 3),
    is_trending BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(year, month, sector)
);

-- 월별 종목 트렌딩 분석 (섹터 내)
-- 계산: spark_batch_sector.py (Airflow DAG가 스케줄링)
-- 읽기: API
CREATE TABLE IF NOT EXISTS analytics_stock_trending (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100) NOT NULL,
    avg_monthly_return NUMERIC(6, 3),
    vs_sector NUMERIC(6, 3),
    volatility NUMERIC(6, 3),
    total_volume BIGINT,
    month_high NUMERIC(10, 2),
    month_low NUMERIC(10, 2),
    trading_days INT,
    is_trending BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(year, month, ticker)
);

-- ========================================
-- ANALYTICS 테이블 (Spark가 계산한 분석 결과)
-- Batch analysis only - NO streaming/real-time
-- ========================================

-- 월간 포트폴리오 (5일/10일/20일 통합)
-- 생성: spark_monthly_portfolio_rebalancer.py (매월 마지막 일요일)
-- 읽기: API, Dashboard
-- 용도: 다음 20영업일 동안 유지할 최종 포트폴리오
CREATE TABLE IF NOT EXISTS 08_analytics_monthly_portfolio (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    final_rank INT NOT NULL,
    final_weight NUMERIC(10, 6) NOT NULL,
    score NUMERIC(10, 2) NOT NULL,
    source_periods VARCHAR(50),  -- 예: "20d,10d,5d"
    return_20d NUMERIC(10, 2),
    market_cap BIGINT,
    allocation_reason TEXT,
    rebalance_date DATE NOT NULL,
    valid_until DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ticker, rebalance_date)
);

-- ========================================
-- LOGS 테이블 (시스템 로그 및 에러)
-- ========================================

-- Kafka Consumer 파싱 에러
-- 저장: kafka_consumer_market_writer.py
-- 읽기: Monitoring
CREATE TABLE IF NOT EXISTS logs_consumer_error (
    id SERIAL PRIMARY KEY,
    raw_value TEXT,
    error_msg TEXT,
    received_at TIMESTAMPTZ DEFAULT NOW()
);

-- ========================================
-- 인덱스 생성
-- ========================================

-- Collected 테이블 인덱스 (Batch collection only)
CREATE INDEX IF NOT EXISTS idx_collected_stock_date ON 06_collected_daily_stock_history(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_collected_stock_sector ON 06_collected_daily_stock_history(sector);
CREATE INDEX IF NOT EXISTS idx_collected_stock_ticker ON 06_collected_daily_stock_history(ticker);
CREATE INDEX IF NOT EXISTS idx_collected_meta_type ON 00_collected_meta_etf(etf_type);
CREATE INDEX IF NOT EXISTS idx_collected_meta_sector ON 00_collected_meta_etf(sector_name);
CREATE INDEX IF NOT EXISTS idx_collected_etf_ticker ON 01_collected_daily_etf_ohlc(ticker);
CREATE INDEX IF NOT EXISTS idx_collected_etf_date ON 01_collected_daily_etf_ohlc(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_collected_benchmark_etf ON collected_monthly_benchmark_holdings(etf_ticker);
CREATE INDEX IF NOT EXISTS idx_collected_benchmark_date ON collected_monthly_benchmark_holdings(as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_collected_benchmark_holding ON collected_monthly_benchmark_holdings(holding_ticker);

-- Analytics 테이블 인덱스
CREATE INDEX IF NOT EXISTS idx_analytics_portfolio_date ON 05_analytics_portfolio_allocation(as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_portfolio_ticker ON 05_analytics_portfolio_allocation(ticker);
CREATE INDEX IF NOT EXISTS idx_analytics_portfolio_sector ON 05_analytics_portfolio_allocation(sector);
CREATE INDEX IF NOT EXISTS idx_analytics_portfolio_weight ON 05_analytics_portfolio_allocation(portfolio_weight DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_trending_etf_date ON 03_analytics_trending_etfs(as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_trending_etf_ticker ON 03_analytics_trending_etfs(etf_ticker);
CREATE INDEX IF NOT EXISTS idx_08_analytics_monthly_portfolio_date ON 08_analytics_monthly_portfolio(rebalance_date DESC);
CREATE INDEX IF NOT EXISTS idx_08_analytics_monthly_portfolio_ticker ON 08_analytics_monthly_portfolio(ticker);
CREATE INDEX IF NOT EXISTS idx_08_analytics_monthly_portfolio_rank ON 08_analytics_monthly_portfolio(final_rank);
CREATE INDEX IF NOT EXISTS idx_08_analytics_monthly_portfolio_valid ON 08_analytics_monthly_portfolio(valid_until DESC);

-- Logs 테이블 인덱스
CREATE INDEX IF NOT EXISTS idx_logs_consumer_received ON logs_consumer_error(received_at DESC);
