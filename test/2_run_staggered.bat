@echo off
REM Staggered Sector ETF Backfill Runner
REM 섹터 ETF를 시간차를 두고 백필합니다.

echo ========================================
echo Staggered Sector ETF Backfill
echo ========================================
echo.

REM 기본값 설정
set DAYS=365
set DELAY=5

REM 인자 처리
if not "%1"=="" set DAYS=%1
if not "%2"=="" set DELAY=%2

echo 수집 거래일: %DAYS%일
echo 대기 시간: %DELAY%초
echo.

echo 배치 1/2: QQQ, XLF, XLV, XLY, XLC (5개)
python 2_staggered_sector_backfill.py --days %DAYS% --delay %DELAY% --batch 5 --start-idx 0

echo.
echo 잠시 대기 중... (30초)
timeout /t 30 /nobreak

echo.
echo 배치 2/2: XLI, XLP, XLU, XLRE, XLB (5개)
python 2_staggered_sector_backfill.py --days %DAYS% --delay %DELAY% --batch 5 --start-idx 5

echo.
echo ========================================
echo 전체 백필 완료!
echo ========================================
echo.

pause
