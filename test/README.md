# 테스트 및 백필 도구 가이드

이 폴더는 시스템 테스트, 데이터 백필, 수동 실행 스크립트를 포함합니다.

---

## 📁 폴더 구조

```
test/
├── README.md                          # 이 파일
├── 1_backfill_etf_benchmarks.py      # [백필 1단계] ETF 벤치마크 데이터 백필
├── 1_run_backfill.bat                # [실행] 백필 스크립트 실행 (Windows)
├── test.sh                           # [실행] 전체 테스트 실행 (Linux/Mac)
├── spark_jobs_launcher.sh            # [실행] Spark Job 수동 실행
└── unit_tests/                       # 단위 테스트
    └── test_spark_jobs.py            # Spark Job 테스트
```

---

## 🔄 백필 워크플로우 (Backfill Workflow)

시스템을 처음 시작하거나 과거 데이터를 채울 때 사용합니다.

### 실행 순서

#### ✅ 1단계: ETF 벤치마크 데이터 백필 (필수)

**파일**: `1_backfill_etf_benchmarks.py`

**목적**: SPY 및 11개 섹터 ETF의 과거 일별 OHLC 데이터를 수집하여 `collected_daily_etf_ohlc` 테이블에 저장합니다.

**주요 기능**:
- ✅ **스마트 캐싱**: 다운로드한 데이터를 24시간 동안 로컬 캐시 저장
- ✅ **거래일 기준**: `--days` 파라미터는 거래일 수를 의미 (영업일 기준)
- ✅ **Rate-limit 보호**: yfinance 제한 발생 시 즉시 중단 및 재개 시간 표시
- ✅ **재실행 안전**: 캐시 사용으로 24시간 내 재실행 시 API 호출 없음

**대상 ETF**:
- SPY (S&P 500 벤치마크)
- XLK (Technology)
- XLV (Healthcare)
- XLF (Financials)
- XLE (Energy)
- XLY (Consumer Discretionary)
- XLP (Consumer Staples)
- XLI (Industrials)
- XLB (Materials)
- XLU (Utilities)
- XLRE (Real Estate)
- XLC (Communication Services)

**실행 방법**:

```bash
# Windows (기본 실행: 20 거래일)
test\1_run_backfill.bat

# Linux/Mac (기본 실행: 20 거래일, 캐시 활성화)
docker-compose exec api python /app/test/1_backfill_etf_benchmarks.py
```

**파라미터**:
-- `--days`: 백필할 과거 거래일 수 (기본값: 20 거래일)
- `--delay`: API 요청 간 대기 시간 초 (기본값: 5.0초)
- `--rate-limit-sleep`: Rate-limit 감지 시 권장 대기 시간 (기본값: 900초 = 15분)
- `--no-batch`: 배치 다운로드 비활성화 (티커별 순차 처리)
- `--max-retries`: 다운로드 재시도 횟수 (기본값: 3회)

**캐싱 동작**:
- 캐시 저장 위치: `test/.cache/`
- 캐시 유효 기간: 24시간
- 재실행 시 캐시 자동 사용 (API 호출 없음)
- Rate-limit 걱정 없이 안전하게 재실행 가능

**예상 소요 시간**: 
- 첫 실행 (API): 약 10-15초 (6개 ETF × 20 거래일)
- 재실행 (캐시): 약 1초

**완료 확인**:
```sql
-- PostgreSQL에서 확인
SELECT ticker, COUNT(*) as record_count, MIN(trade_date), MAX(trade_date)
FROM collected_daily_etf_ohlc
GROUP BY ticker
ORDER BY ticker;
```

**주의사항**:
- yfinance API 제한을 피하기 위해 `--delay` 값을 너무 낮게 설정하지 마세요
- 이미 데이터가 있는 경우 ON CONFLICT로 중복 방지됩니다
- 네트워크 오류 발생 시 다시 실행하면 누락된 데이터만 채워집니다

---

---

#### ✅ 2단계: 섹터 ETF 백필 - Staggered Runner (선택)

**파일**: `2_staggered_sector_backfill.py`, `2_run_staggered.bat`

**목적**: 섹터 ETF 데이터를 **Rate-Limit 없이 안전하게** 백필합니다. 티커당 5분 간격으로 순차 수집합니다.

**주요 기능**:
- ✅ **Rate-Limit 방지**: 1개 티커씩 5분 간격으로 수집 (429 에러 완전 차단)
- ✅ **스마트 스킵**: 이미 충분한 데이터가 있는 티커 자동 건너뛰기 (`--skip-existing`)
- ✅ **재개 가능**: 중단 시 특정 티커부터 재개 가능 (`--start-from TICKER`)
- ✅ **진행 추적**: 현재 진행률 및 예상 완료 시간 표시
- ✅ **안전한 실패**: 개별 티커 실패 시에도 나머지 티커 계속 처리

**대상 ETF** (11개 섹터):
- XLK (Technology)
- XLV (Healthcare)
- XLF (Financials)
- XLY (Consumer Discretionary)
- XLC (Communication Services)
- XLI (Industrials)
- XLP (Consumer Staples)
- XLE (Energy)
- XLU (Utilities)
- XLRE (Real Estate)
- XLB (Materials)

**실행 방법**:

```bash
# Windows (간편 실행: 기본 20일, 5분 간격, 기존 데이터 스킵)
test\2_run_staggered.bat

# Linux/Mac (수동 실행)
docker compose exec -T api sh -lc "cd /app && PYTHONPATH=/app python test/2_staggered_sector_backfill.py --days 20 --delay-minutes 5.0 --skip-existing"
```

**파라미터**:
- `--days`: 백필할 과거 거래일 수 (기본값: 20)
- `--delay-minutes`: 티커 간 대기 시간 분 (기본값: 5.0분)
- `--skip-existing`: 충분한 데이터가 있는 티커 건너뛰기
- `--start-from TICKER`: 특정 티커부터 재개 (예: `--start-from XLV`)
- `--tickers`: 특정 티커만 처리 (예: `--tickers "XLK,XLV,XLF"`)
- `--dry-run`: 실행 계획만 표시 (실제 수집 안 함)

**예상 소요 시간**:
- **11개 섹터 ETF** × 5분 = **약 50-60분**
- 3분 간격: 약 30-35분 (리스크 증가)
- 10분 간격: 약 100-110분 (매우 안전)

**중단 및 재개**:
실행 중 **Ctrl+C**로 중단 가능. 화면에 재개 명령이 표시됩니다:
```
To resume, run with: --start-from XLV
```

재개 명령:
```bash
docker compose exec -T api sh -lc "cd /app && PYTHONPATH=/app python test/2_staggered_sector_backfill.py --start-from XLV"
```

**사용 예시**:

```bash
# 1. 실행 계획 미리보기 (실제 수집 없음)
docker compose exec -T api sh -lc "cd /app && PYTHONPATH=/app python test/2_staggered_sector_backfill.py --dry-run"

# 2. 전체 11개 섹터 ETF 백필 (5분 간격, 기존 데이터 스킵)
test\2_run_staggered.bat

# 3. XLV부터 재개 (이전 실행 중단 시)
docker compose exec -T api sh -lc "cd /app && PYTHONPATH=/app python test/2_staggered_sector_backfill.py --start-from XLV"

# 4. 더 빠른 실행 (3분 간격, 리스크 약간 증가)
docker compose exec -T api sh -lc "cd /app && PYTHONPATH=/app python test/2_staggered_sector_backfill.py --delay-minutes 3.0"

# 5. 특정 ETF만 백필
docker compose exec -T api sh -lc "cd /app && PYTHONPATH=/app python test/2_staggered_sector_backfill.py --tickers 'XLK,XLV,XLF' --delay-minutes 3.0"
```

**동작 방식**:
1. **데이터베이스 확인**: 각 티커의 기존 데이터 확인 (--skip-existing 시)
2. **필터링**: 충분한 최신 데이터가 있는 티커 제외
3. **순차 수집**: 각 티커마다 `1_backfill_etf_benchmarks.py` 호출
4. **대기**: N분 대기 후 다음 티커
5. **진행 표시**: 현재 티커, 예상 완료 시간, 재개 명령 표시
6. **에러 처리**: 개별 실패 로그하되 나머지 티커 계속 처리

**완료 확인**:
```sql
-- PostgreSQL에서 확인
SELECT ticker, COUNT(*) as record_count, MIN(trade_date), MAX(trade_date)
FROM collected_daily_etf_ohlc
WHERE ticker IN ('XLK', 'XLV', 'XLF', 'XLY', 'XLC', 'XLI', 'XLP', 'XLE', 'XLU', 'XLRE', 'XLB')
GROUP BY ticker
ORDER BY ticker;

-- 기대 결과: 11개 티커 모두 20개 레코드
```

**주의사항**:
- ⚠️ **긴 실행 시간**: 11개 티커 × 5분 = 약 50-60분 소요
- ✅ **Rate-Limit 안전**: 5분 간격으로 429 에러 완전 방지
- ✅ **중단 안전**: Ctrl+C로 중단 후 `--start-from`으로 재개 가능
- ✅ **캐시 활용**: 24시간 내 재실행 시 캐시에서 데이터 로드 (API 호출 없음)

---

#### ✅ 2단계: 5-Stage 파이프라인 확인 (Airflow 자동 스케줄)

⚠️ **NO real-time/streaming collection** - Airflow 스케줄 배치만 사용

**5-Stage Daily Pipeline** (월-금 09:00-13:00 UTC):
1. Stage 1 (09:00 UTC): ETF OHLC 수집 → `collected_daily_etf_ohlc`
2. Stage 2 (10:00 UTC): ETF 보유종목 → `collected_meta_etf`  
3. Stage 3 (11:00 UTC): 트렌딩 섹터 식별
4. Stage 4 (12:00 UTC): 주식 히스토리 → `collected_daily_stock_history`
5. Stage 5 (13:00 UTC): 포트폴리오 분석 → `analytics_portfolio_allocation`

**확인 방법**:
```bash
# Kafka 토픽 메시지 확인 (5-Stage 통합 토픽)
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-market-data \
  --from-beginning \
  --max-messages 10
```

---

#### ✅ 3단계: Spark 분석 Job 실행 (자동 또는 수동)

**자동 실행**: Airflow 5-Stage Pipeline Stage 5 (월-금 13:00 UTC 자동 실행)

**수동 실행**:
```bash
# Spark Job 수동 실행 스크립트 사용
./test/spark_jobs_launcher.sh

# 또는 직접 실행
docker-compose exec spark-master bash -c "\
  /opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 2g \
  --executor-memory 2g \
  --packages org.postgresql:postgresql:42.6.0 \
  batch/spark_active_stock_allocator.py"
```

**처리 내용**:
- `collected_daily_etf_ohlc`, `collected_daily_stock_history`에서 데이터 읽기
- 5일/10일/20일 기준으로 트렌딩 섹터 및 종목 분석
- 포트폴리오 비중 계산 (시가총액 가중)
- `analytics_portfolio_allocation` 테이블에 결과 저장

**완료 확인**:
```sql
SELECT as_of_date, COUNT(*) as stock_count, SUM(portfolio_weight) as total_weight
FROM analytics_portfolio_allocation
GROUP BY as_of_date
ORDER BY as_of_date DESC
LIMIT 10;
```

---

## 🧪 테스트 실행

### 단위 테스트 (Unit Tests)

**파일**: `unit_tests/test_spark_jobs.py`

Spark Job의 SQL 쿼리, 데이터 변환 로직 검증

**실행**:
```bash
# 전체 테스트 실행
./test/test.sh

# 또는 개별 실행
cd test/unit_tests
python -m pytest test_spark_jobs.py -v
```

**테스트 항목**:
- `test_active_allocation_upsert_sql`: 포트폴리오 배분 SQL 검증
- `test_error_tracking_sql`: 에러 로깅 SQL 검증

---

---

## ⏱️ Staggered Backfill (Rate-limit 회피)

섹터 ETF나 rate-limit이 우려될 때는 **staggered runner**를 사용하세요:

**파일**: `2_staggered_sector_backfill.py`

**특징**:
- ✅ 한 번에 1개 티커만 처리 (configurable delay)
- ✅ 이미 데이터가 있는 티커는 자동 스킵 (`--skip-existing`)
- ✅ 중단 후 재개 가능 (`--start-from TICKER`)
- ✅ 진행 상황 및 예상 완료 시간 표시
- ✅ 개별 티커 실패 시에도 계속 진행

**실행 방법**:

```bash
# Windows (권장)
test\2_run_staggered.bat

# Linux/Mac (기본 실행: 20일, 5분 간격)
docker compose exec -T api sh -lc "cd /app && PYTHONPATH=/app python test/2_staggered_sector_backfill.py --days 20 --delay-minutes 5.0 --skip-existing"
```

**파라미터**:
- `--days 20` - 거래일 수 (기본값: 20)
- `--delay-minutes 5.0` - 티커 간 대기 시간 (기본값: 5분)
- `--skip-existing` - 충분한 데이터가 있는 티커 스킵
- `--start-from XLV` - 특정 티커부터 재개
- `--dry-run` - 실행 계획만 표시 (실제 백필 안 함)
- `--tickers "XLK,XLV,XLF"` - 특정 티커만 처리

**예상 소요 시간**:
- 11개 섹터 ETF × 5분 간격 = 약 50-60분
- 백그라운드에서 실행 가능 (다른 작업 병행)

**중단된 실행 재개**:
```bash
# XLV부터 재개 (XLK, XLV 이미 완료한 경우)
docker compose exec -T api sh -lc "cd /app && PYTHONPATH=/app python test/2_staggered_sector_backfill.py --days 20 --delay-minutes 5.0 --start-from XLF"
```

**실행 계획 미리보기**:
```bash
docker compose exec -T api sh -lc "cd /app && PYTHONPATH=/app python test/2_staggered_sector_backfill.py --dry-run"
```

---

## 🛠️ 트러블슈팅

### 백필 실패 시

**문제**: `yfinance API rate limit exceeded`
```
해결 1 (권장): Staggered runner 사용
test\2_run_staggered.bat

해결 2: 스크립트가 자동으로 중단되고 재개 시간(UTC)을 표시합니다
해결 3: 24시간 내 재실행 시 캐시를 사용하므로 rate-limit 없음
해결 4: --delay 값을 증가
docker-compose exec api python /app/test/1_backfill_etf_benchmarks.py --delay 5.0

# 캐시 초기화 (필요 시)
rm -rf test/.cache/
```

**문제**: `Connection to PostgreSQL failed`
```
해결: DB 컨테이너 확인
docker-compose ps postgres
docker-compose logs postgres
```

**문제**: `No data found for ticker XXX`
```
해결: 정상 - 일부 ETF는 데이터가 없을 수 있음. 로그 확인 후 진행
```

### Spark Job 실패 시

**문제**: `No stock data found in collected_daily_stock_history`
```
해결: 2단계(실시간 수집)가 완료될 때까지 대기 (최소 1시간)
```

**문제**: `java.sql.SQLException: No suitable driver`
```
해결: PostgreSQL JDBC 드라이버 확인
docker-compose exec spark-master ls -la /opt/spark/jars/postgresql*.jar
```

---

## � 캐싱 시스템

백필 스크립트는 스마트 캐싱을 사용하여 성능과 안정성을 개선합니다.

### 작동 방식

1. **첫 실행**: yfinance API에서 데이터 다운로드 후 `test/.cache/` 에 저장
2. **재실행**: 캐시가 유효하면(24시간 이내) API 호출 없이 캐시 사용
3. **부분 캐시**: 일부 티커만 캐시된 경우 나머지만 API로 가져오기
4. **자동 갱신**: 캐시가 만료되면(24시간 경과) 자동으로 API에서 새로 다운로드

### 캐시 관리

```bash
# 캐시 상태 확인
ls -lh test/.cache/

# 캐시 초기화 (강제 재다운로드)
rm -rf test/.cache/

# 특정 티커 캐시 삭제
rm test/.cache/spy_20d.pkl
```

### 이점

- ⚡ **속도**: 캐시 사용 시 13배 이상 빠름 (13초 → 1초)
- 🛡️ **안전**: Rate-limit 걱정 없이 재실행 가능
- 💰 **비용**: API 호출 최소화
- 🔄 **편의성**: 실패 후 즉시 재시도 가능

## �📊 데이터 검증

백필 및 테스트 완료 후 다음 쿼리로 데이터 상태를 확인하세요:

```sql
-- 1. 수집된 ETF 데이터 확인
SELECT 'ETF OHLC' as table_name, COUNT(*) as records,
       MIN(trade_date) as earliest, MAX(trade_date) as latest
FROM collected_daily_etf_ohlc;

-- 2. 수집된 주식 데이터 확인  
SELECT 'Stock History' as table_name, COUNT(*) as records,
       MIN(trade_date) as earliest, MAX(trade_date) as latest
FROM collected_daily_stock_history;

-- 3. 벤치마크 데이터 확인 (SPY/QQQ)
SELECT 'Benchmark OHLC' as table_name, COUNT(DISTINCT ticker) as tickers,
       MIN(trade_date) as earliest, MAX(trade_date) as latest
FROM collected_daily_benchmark_ohlc;

-- 4. 포트폴리오 분석 결과 확인
SELECT 'Portfolio Allocation' as table_name, 
       COUNT(DISTINCT as_of_date) as analysis_dates,
       MAX(as_of_date) as latest_analysis
FROM analytics_portfolio_allocation;

-- 5. 테이블 크기 확인
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename LIKE 'collected_%' 
   OR tablename LIKE 'analytics_%'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

---

## 📝 참고 사항

### 백필 실행 타이밍

- **최초 설치 시**: 필수 실행 (1단계)
- **데이터 손실 복구**: 필요 시 실행
- **과거 데이터 추가**: `--days` 파라미터 조정하여 실행

### API 제한 사항

**yfinance API**:
- 일일 요청 제한: 48,000회
- 초당 요청 제한: 2,000회
- 권장 대기 시간: 1초 이상

**계산**:
- 12개 ETF × 730일 = 약 50회 요청
- 550개 주식 × 매시간 = 약 13,200회/일

### 데이터 보관 정책

- **실시간 데이터**: 최근 30일
- **일별 히스토리**: 최근 2년
- **분석 결과**: 전체 보관 (용량 작음)
- **로그**: 최근 7일

---

## 🔗 관련 문서

- [ARCHITECTURE.md](../ARCHITECTURE.md) - 전체 시스템 아키텍처
- [README.md](../README.md) - 프로젝트 개요
- [database/NAMING_CONVENTION.md](../database/NAMING_CONVENTION.md) - 테이블 명명 규칙
- [WORKFLOW.md](../WORKFLOW.md) - 운영 워크플로우
