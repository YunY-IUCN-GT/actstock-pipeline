@echo off
REM ETF Benchmark Data Backfill - Windows Runner
REM 벤치마크 및 섹터 ETF의 과거 데이터를 수집합니다.

echo ========================================
echo ETF Benchmark Data Backfill
echo ========================================
echo.

REM Default: 20일 데이터 수집
set DAYS=20

REM 인자가 있으면 사용
if not "%1"=="" set DAYS=%1

echo 수집 거래일: %DAYS%일
echo.

REM Python 스크립트 실행
python 1_backfill_etf_benchmarks.py --days %DAYS%

echo.
echo ========================================
echo 백필 완료!
echo ========================================
echo.

pause
