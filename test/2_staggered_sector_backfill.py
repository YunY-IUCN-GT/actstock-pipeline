"""
Staggered Sector ETF Backfill Script

섹터 ETF 데이터를 시간차를 두고 수집하여 Rate Limit 회피
- 각 ETF 수집 후 5초 대기
- 배치 단위로 나누어 실행 가능
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import yfinance as yf
import psycopg2
from datetime import datetime, timedelta
import time
import argparse

# 섹터 ETF 리스트
SECTOR_ETFS = ['QQQ', 'XLF', 'XLV', 'XLY', 'XLC', 'XLI', 'XLP', 'XLU', 'XLRE', 'XLB']

# 데이터베이스 연결 정보
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'stockdb',
    'user': 'postgres',
    'password': 'postgres'
}


def fetch_and_insert_etf(symbol: str, days: int, delay: int = 5):
    """
    단일 ETF 데이터 수집 및 DB 삽입
    
    Args:
        symbol: ETF 심볼
        days: 수집할 과거 거래일 수
        delay: 다음 수집까지 대기 시간(초)
    """
    print(f"\n{'='*60}")
    print(f"수집 중: {symbol}")
    print(f"{'='*60}")
    
    try:
        # 데이터 수집
        calendar_days = int(days * 1.5)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=calendar_days)
        
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"✗ {symbol}: 데이터 없음")
            return 0
        
        hist = hist.tail(days)
        print(f"✓ {symbol}: {len(hist)}개 레코드 수집")
        
        # 데이터베이스 삽입
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        insert_count = 0
        
        for date, row in hist.iterrows():
            cursor.execute("""
                INSERT INTO collected_daily_etf_ohlc 
                (symbol, date, open, high, low, close, volume, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol, date) DO NOTHING
            """, (
                symbol,
                date.strftime('%Y-%m-%d'),
                float(row['Open']),
                float(row['High']),
                float(row['Low']),
                float(row['Close']),
                int(row['Volume'])
            ))
            
            if cursor.rowcount > 0:
                insert_count += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ {symbol}: {insert_count}개 레코드 삽입 (중복 제외)")
        
        # 다음 요청 전 대기
        if delay > 0:
            print(f"⏳ {delay}초 대기 중...")
            time.sleep(delay)
        
        return insert_count
        
    except Exception as e:
        print(f"✗ {symbol}: 오류 - {str(e)}")
        return 0


def main():
    parser = argparse.ArgumentParser(description='섹터 ETF 시간차 백필')
    parser.add_argument('--days', type=int, default=365,
                       help='수집할 과거 거래일 수 (기본값: 365)')
    parser.add_argument('--delay', type=int, default=5,
                       help='각 ETF 수집 후 대기 시간(초) (기본값: 5)')
    parser.add_argument('--batch', type=int,
                       help='배치 크기 (기본값: 전체)')
    parser.add_argument('--start-idx', type=int, default=0,
                       help='시작 인덱스 (기본값: 0)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("섹터 ETF 시간차 백필")
    print("=" * 60)
    print(f"수집 거래일: {args.days}일")
    print(f"대기 시간: {args.delay}초")
    print(f"전체 ETF: {', '.join(SECTOR_ETFS)}")
    print()
    
    # 배치 설정
    start_idx = args.start_idx
    end_idx = len(SECTOR_ETFS)
    
    if args.batch:
        end_idx = min(start_idx + args.batch, len(SECTOR_ETFS))
        print(f"배치 실행: {start_idx+1}번째 ~ {end_idx}번째 ETF")
    
    symbols = SECTOR_ETFS[start_idx:end_idx]
    print(f"수집 대상: {', '.join(symbols)}")
    print()
    
    total_inserted = 0
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{start_idx + i}/{len(SECTOR_ETFS)}] {symbol}")
        inserted = fetch_and_insert_etf(symbol, args.days, args.delay)
        total_inserted += inserted
    
    print("\n" + "=" * 60)
    print(f"백필 완료!")
    print(f"  처리 ETF: {len(symbols)}개")
    print(f"  총 삽입: {total_inserted}개 레코드")
    print("=" * 60)


if __name__ == "__main__":
    main()
