"""
ETF Benchmark Data Backfill Script

벤치마크 및 섹터 ETF의 과거 일별 OHLC 데이터를 수집합니다.
- 대상: SPY, QQQ, IWM, EWY, DIA, SCHD (벤치마크) + XLF, XLV, XLY, XLC, XLI, XLP, XLU, XLRE, XLB (섹터)
- 저장: collected_daily_etf_ohlc 테이블
- 기능: 스마트 캐싱 (24시간), 거래일 기준, Rate-limit 보호
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import yfinance as yf
import psycopg2
from datetime import datetime, timedelta
import time
import argparse
import json
from pathlib import Path

# 15 ETF 구성: 6 benchmarks + 10 sectors (QQQ는 both)
BENCHMARK_ETFS = ['SPY', 'QQQ', 'IWM', 'EWY', 'DIA', 'SCHD']
SECTOR_ETFS = ['QQQ', 'XLF', 'XLV', 'XLY', 'XLC', 'XLI', 'XLP', 'XLU', 'XLRE', 'XLB']
ALL_ETFS = list(set(BENCHMARK_ETFS + SECTOR_ETFS))  # 15 unique ETFs

# 데이터베이스 연결 정보
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'stockdb',
    'user': 'postgres',
    'password': 'postgres'
}

# 캐시 디렉토리
CACHE_DIR = Path(__file__).parent / '.cache'
CACHE_DIR.mkdir(exist_ok=True)
CACHE_VALIDITY_HOURS = 24


def get_cache_file(symbol: str) -> Path:
    """캐시 파일 경로 반환"""
    return CACHE_DIR / f"{symbol}_daily_cache.json"


def is_cache_valid(cache_file: Path) -> bool:
    """캐시 파일이 유효한지 확인 (24시간 이내)"""
    if not cache_file.exists():
        return False
    
    file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
    age_hours = (datetime.now() - file_time).total_seconds() / 3600
    
    return age_hours < CACHE_VALIDITY_HOURS


def load_from_cache(symbol: str):
    """캐시에서 데이터 로드"""
    cache_file = get_cache_file(symbol)
    
    if is_cache_valid(cache_file):
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)
            print(f"  ✓ 캐시에서 로드: {symbol} ({cache_data['record_count']}개 레코드)")
            return cache_data['data']
    
    return None


def save_to_cache(symbol: str, data: list):
    """데이터를 캐시에 저장"""
    cache_file = get_cache_file(symbol)
    
    cache_data = {
        'symbol': symbol,
        'timestamp': datetime.now().isoformat(),
        'record_count': len(data),
        'data': data
    }
    
    with open(cache_file, 'w') as f:
        json.dump(cache_data, f)
    
    print(f"  ✓ 캐시 저장: {symbol} ({len(data)}개 레코드)")


def fetch_etf_data(symbol: str, days: int = 365) -> list:
    """
    ETF 일별 OHLC 데이터 수집
    
    Args:
        symbol: ETF 심볼
        days: 과거 거래일 수 (영업일 기준)
    
    Returns:
        List of dictionaries with OHLC data
    """
    # 캐시 확인
    cached_data = load_from_cache(symbol)
    if cached_data:
        return cached_data
    
    print(f"  → yfinance API 호출: {symbol}")
    
    try:
        # 거래일 기준으로 약간 더 많은 일수 가져오기 (주말/휴일 고려)
        calendar_days = int(days * 1.5)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=calendar_days)
        
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"  ✗ 데이터 없음: {symbol}")
            return []
        
        # 최근 N 거래일만 선택
        hist = hist.tail(days)
        
        data = []
        for date, row in hist.iterrows():
            data.append({
                'symbol': symbol,
                'date': date.strftime('%Y-%m-%d'),
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': int(row['Volume'])
            })
        
        # 캐시 저장
        save_to_cache(symbol, data)
        
        print(f"  ✓ 수집 완료: {symbol} ({len(data)}개 레코드)")
        return data
        
    except Exception as e:
        if "429" in str(e) or "rate" in str(e).lower():
            print(f"\n⚠️  Rate Limit 도달!")
            print(f"  다음 시간대에 다시 시도하세요.")
            print(f"  현재까지 수집된 데이터는 캐시에 저장되었습니다.")
            sys.exit(1)
        else:
            print(f"  ✗ 오류 발생: {symbol} - {str(e)}")
            return []


def insert_to_database(data: list):
    """수집된 데이터를 데이터베이스에 삽입"""
    if not data:
        return 0
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    insert_count = 0
    
    try:
        for record in data:
            # 중복 체크 후 삽입
            cursor.execute("""
                INSERT INTO collected_daily_etf_ohlc 
                (symbol, date, open, high, low, close, volume, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol, date) DO NOTHING
            """, (
                record['symbol'],
                record['date'],
                record['open'],
                record['high'],
                record['low'],
                record['close'],
                record['volume']
            ))
            
            if cursor.rowcount > 0:
                insert_count += 1
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"  ✗ DB 삽입 오류: {str(e)}")
    
    finally:
        cursor.close()
        conn.close()
    
    return insert_count


def main():
    parser = argparse.ArgumentParser(description='ETF 벤치마크 데이터 백필')
    parser.add_argument('--days', type=int, default=365, 
                       help='수집할 과거 거래일 수 (기본값: 365)')
    parser.add_argument('--symbols', type=str, 
                       help='특정 심볼만 수집 (쉼표로 구분, 예: SPY,QQQ)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("ETF 벤치마크 데이터 백필")
    print("=" * 60)
    print(f"수집 거래일: {args.days}일")
    print(f"캐시 디렉토리: {CACHE_DIR}")
    print(f"캐시 유효기간: {CACHE_VALIDITY_HOURS}시간")
    print()
    
    # 수집 대상 심볼 결정
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]
        print(f"수집 대상: {', '.join(symbols)}")
    else:
        symbols = sorted(ALL_ETFS)
        print(f"전체 15 ETF 수집:")
        print(f"  벤치마크 (6): {', '.join(sorted(BENCHMARK_ETFS))}")
        print(f"  섹터 (10): {', '.join(sorted(SECTOR_ETFS))}")
    
    print()
    print("=" * 60)
    print()
    
    total_inserted = 0
    
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] {symbol}")
        
        # 데이터 수집
        data = fetch_etf_data(symbol, args.days)
        
        if data:
            # 데이터베이스 삽입
            inserted = insert_to_database(data)
            total_inserted += inserted
            print(f"  ✓ DB 삽입: {inserted}개 (중복 제외)")
        
        print()
        
        # Rate limit 방지를 위한 짧은 대기
        if i < len(symbols):
            time.sleep(0.5)
    
    print("=" * 60)
    print(f"백필 완료!")
    print(f"  총 삽입: {total_inserted}개 레코드")
    print("=" * 60)


if __name__ == "__main__":
    main()
