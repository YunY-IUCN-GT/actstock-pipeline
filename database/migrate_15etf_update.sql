-- Migration script to update ETF metadata for 15-ETF configuration
-- Date: 2024
-- Changes: 
--   - Remove XLK (Technology), XLE (Energy) 
--   - Update QQQ to 'both' type (benchmark + Technology sector)
--   - Total: 15 unique ETFs (6 benchmarks, 10 sectors, QQQ counted once)

-- Delete removed ETFs (XLK, XLE)
DELETE FROM collected_meta_etf 
WHERE ticker IN ('XLK', 'XLE');

-- Update QQQ to reflect dual role (benchmark + sector)
UPDATE collected_meta_etf 
SET etf_type = 'both', sector_name = 'Technology'
WHERE ticker = 'QQQ';

-- Verify final ETF list (should be 15 rows)
SELECT 
    etf_type,
    COUNT(*) as count,
    STRING_AGG(ticker, ', ' ORDER BY ticker) as tickers
FROM collected_meta_etf
GROUP BY etf_type
ORDER BY etf_type;

-- Expected output:
-- benchmark  | 5 | DIA, EWY, IWM, SCHD, SPY
-- both       | 1 | QQQ
-- sector     | 9 | XLB, XLC, XLF, XLI, XLP, XLRE, XLU, XLV, XLY

COMMIT;
