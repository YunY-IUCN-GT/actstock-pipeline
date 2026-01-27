-- ========================================
-- Table Renaming Migration: Add Stage Numbers
-- Aligns database tables with workflow execution order
-- Preserves all existing data during migration
-- Execute this script to rename all tables with stage prefixes
-- ========================================

BEGIN;

-- ========================================
-- STEP 1: Rename tables with data preservation
-- Note: ALTER TABLE RENAME preserves all data, indexes, and constraints
-- ========================================

DO $$
BEGIN
    -- Reference data (Stage 00 - metadata)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'collected_meta_etf') THEN
        ALTER TABLE collected_meta_etf RENAME TO "00_collected_meta_etf";
        RAISE NOTICE 'Renamed: collected_meta_etf → 00_collected_meta_etf';
    END IF;

    -- Stage 01-02: ETF OHLC collection
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'collected_daily_etf_ohlc') THEN
        ALTER TABLE collected_daily_etf_ohlc RENAME TO "01_collected_daily_etf_ohlc";
        RAISE NOTICE 'Renamed: collected_daily_etf_ohlc → 01_collected_daily_etf_ohlc';
    END IF;

    -- Stage 03: Trending ETF analysis  
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'analytics_trending_etfs') THEN
        ALTER TABLE analytics_trending_etfs RENAME TO "03_analytics_trending_etfs";
        RAISE NOTICE 'Renamed: analytics_trending_etfs → 03_analytics_trending_etfs';
    END IF;

    -- Stage 04: ETF holdings collection
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'collected_etf_holdings') THEN
        ALTER TABLE collected_etf_holdings RENAME TO "04_collected_etf_holdings";
        RAISE NOTICE 'Renamed: collected_etf_holdings → 04_collected_etf_holdings';
    END IF;

    -- Stage 05: Portfolio allocation
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'analytics_portfolio_allocation') THEN
        ALTER TABLE analytics_portfolio_allocation RENAME TO "05_analytics_portfolio_allocation";
        RAISE NOTICE 'Renamed: analytics_portfolio_allocation → 05_analytics_portfolio_allocation';
    END IF;

    -- Stage 06: Stock history collection
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'collected_daily_stock_history') THEN
        ALTER TABLE collected_daily_stock_history RENAME TO "06_collected_daily_stock_history";
        RAISE NOTICE 'Renamed: collected_daily_stock_history → 06_collected_daily_stock_history';
    END IF;

    -- Stage 08: Monthly portfolio rebalance
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'analytics_monthly_portfolio') THEN
        ALTER TABLE analytics_monthly_portfolio RENAME TO "08_analytics_monthly_portfolio";
        RAISE NOTICE 'Renamed: analytics_monthly_portfolio → 08_analytics_monthly_portfolio';
    END IF;

    -- Stage 09: ETF top holdings analysis
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'analytics_etf_top_holdings') THEN
        ALTER TABLE analytics_etf_top_holdings RENAME TO "09_analytics_etf_top_holdings";
        RAISE NOTICE 'Renamed: analytics_etf_top_holdings → 09_analytics_etf_top_holdings';
    END IF;

    -- Legacy tables (keep for reference, but mark as legacy)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'collected_monthly_benchmark_holdings') THEN
        ALTER TABLE collected_monthly_benchmark_holdings RENAME TO "collected_monthly_benchmark_holdings_legacy";
        RAISE NOTICE 'Renamed: collected_monthly_benchmark_holdings → legacy';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'analytics_sector_trending') THEN
        ALTER TABLE analytics_sector_trending RENAME TO "analytics_sector_trending_legacy";
        RAISE NOTICE 'Renamed: analytics_sector_trending → legacy';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'analytics_stock_trending') THEN
        ALTER TABLE analytics_stock_trending RENAME TO "analytics_stock_trending_legacy";
        RAISE NOTICE 'Renamed: analytics_stock_trending → legacy';
    END IF;

END $$;

-- ========================================
-- STEP 2: Rename constraints and indexes
-- (PostgreSQL automatically renames most constraints with table)
-- ========================================

-- Update foreign key constraint names for clarity
DO $$
BEGIN
    -- Update foreign key on 01_collected_daily_etf_ohlc
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'collected_daily_etf_ohlc_ticker_fkey'
    ) THEN
        ALTER TABLE "01_collected_daily_etf_ohlc" 
            RENAME CONSTRAINT collected_daily_etf_ohlc_ticker_fkey 
            TO "01_collected_daily_etf_ohlc_ticker_fkey";
    END IF;
END $$;

END $$;

-- ========================================
-- STEP 3: Verify data migration
-- ========================================

DO $$
DECLARE
    rec RECORD;
    row_count BIGINT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'DATA VERIFICATION REPORT';
    RAISE NOTICE '========================================';
    
    FOR rec IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND (tablename LIKE '0%' OR tablename LIKE 'analytics_%' OR tablename LIKE 'collected_%')
        ORDER BY tablename
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM %I', rec.tablename) INTO row_count;
        RAISE NOTICE '% : % rows', RPAD(rec.tablename, 50, ' '), row_count;
    END LOOP;
    
    RAISE NOTICE '========================================';
END $$;

COMMIT;

-- ========================================
-- Migration Complete!
-- ========================================
-- 
-- Renamed Tables (with all data preserved):
-- 00_collected_meta_etf                    (Reference: ETF metadata)
-- 01_collected_daily_etf_ohlc             (Stage 01-02: ETF collection)
-- 03_analytics_trending_etfs              (Stage 03: Trending analysis)
-- 04_collected_etf_holdings               (Stage 04, 07: Holdings collection)
-- 05_analytics_portfolio_allocation       (Stage 05: Portfolio allocation)
-- 06_collected_daily_stock_history        (Stage 06: Stock prices)
-- 08_analytics_monthly_portfolio          (Stage 08: Monthly rebalance)
-- 09_analytics_etf_top_holdings           (Stage 09: Top holdings analysis)
--
-- Next Steps:
-- 1. Update all Python files with new table names
-- 2. Rebuild Docker containers
-- 3. Restart services: docker compose down && docker compose up -d --build
-- ========================================
