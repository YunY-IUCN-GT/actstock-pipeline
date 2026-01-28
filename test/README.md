## ⚙️ 사전 설정 및 설치 (Setup)

백필 스크립트를 실행하기 전에 필요한 Python 패키지를 설치해야 합니다.

### 1. 전용 환경 활성화
```cmd
conda activate conda_DE
```

### 2. 필요한 패키지 설치
`test` 디렉토리에서 다음 명령어를 실행하여 필요한 의존성을 설치합니다.
```cmd
pip install psycopg2-binary yfinance pandas python-dotenv
```
또는 루트 디렉토리에서 `pip install -r requirements.txt`를 실행하십시오.

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

**목적**: SPY 및 15개 유니크 ETF의 과거 일별 OHLC 데이터를 수집하여 `collected_01_daily_etf_ohlc` 테이블에 저장합니다.

**주요 기능**:
- ✅ **스마트 캐싱**: 다운로드한 데이터를 24시간 동안 로컬 캐시 저장
- ✅ **거래일 기준**: `--days` 파라미터는 거래일 수를 의미 (영업일 기준)
- ✅ **Rate-limit 보호**: yfinance 제한 발생 시 즉시 중단 및 재개 시간 표시
- ✅ **재실행 안전**: 캐시 사용으로 24시간 내 재실행 시 API 호출 없음

**대상 ETF (15개)**:
- **벤치마크 (6개)**: SPY, QQQ, IWM, EWY, DIA, SCHD
- **섹터 (10개)**: QQQ, XLV, XLF, XLY, XLC, XLI, XLP, XLU, XLRE, XLB
- *(참고: QQQ는 양적 역할을 모두 수행하며 총 유니크 개수는 15개입니다)*

**실행 방법**:

```bash
# Windows (기본 실행: 20 거래일)
test\1_run_backfill.bat

# Linux/Mac (기본 실행: 20 거래일, 캐시 활성화)
docker-compose exec api python /app/test/1_backfill_etf_benchmarks.py
```

**파라미터**:
- `--days`: 백필할 과거 거래일 수 (기본값: 20 거래일)
- `--delay`: API 요청 간 대기 시간 초 (기본값: 5.0초)
- `--rate-limit-sleep`: Rate-limit 감지 시 권장 대기 시간 (기본값: 900초 = 15분)
- `--no-batch`: 배치 다운로드 비활성화 (티커별 순차 처리)
- `--max-retries`: 다운로드 재시도 횟수 (기본값: 3회)

**캐싱 동작**:
- 캐시 저장 위치: `test/.cache/`
- 캐시 유효 기간: 24시간
- 재실행 시 캐시 자동 사용 (API 호출 없음)

**예상 소요 시간**: 
- 첫 실행 (API): 약 20-30초 (15개 ETF × 20 거래일)
- 재실행 (캐시): 약 1초

**완료 확인**:
```sql
-- PostgreSQL에서 확인
SELECT ticker, COUNT(*) as record_count, MIN(trade_date), MAX(trade_date)
FROM collected_01_daily_etf_ohlc
GROUP BY ticker
ORDER BY ticker;
```

---

#### ✅ 2단계: 섹터 ETF 백필 - Staggered Runner (선택)

**파일**: `2_staggered_sector_backfill.py`, `2_run_staggered.bat`

**목적**: 섹터 ETF 데이터를 **Rate-Limit 없이 안전하게** 백필합니다. 티커당 일정 간격(기본 5분)으로 순차 수집합니다.

**주요 기능**:
- ✅ **Rate-Limit 방지**: 1개 티커씩 5분 간격으로 수집 (429 에러 예방)
- ✅ **스마트 스킵**: 이미 충분한 데이터가 있는 티커 자동 건너뛰기 (`--skip-existing`)
- ✅ **재개 가능**: 중단 시 특정 티커부터 재개 가능 (`--start-from TICKER`)
- ✅ **진행 추적**: 현재 진행률 및 예상 완료 시간 표시

**대상 ETF** (10개 섹터):
- QQQ, XLV, XLF, XLY, XLC, XLI, XLP, XLU, XLRE, XLB

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

**예상 소요 시간**:
- **10개 섹터 ETF** × 5분 = **약 50분**

**중단 및 재개**:
실행 중 **Ctrl+C**로 중단 가능. 화면에 재개 명령이 표시됩니다:
```
To resume, run with: --start-from XLV
```

---

#### ✅ 3단계: 5-Stage 파이프라인 확인 (Airflow 자동 스케줄)

⚠️ **실시간/스트리밍 수집은 하지 않음** - Airflow 스케줄 배치만 사용

**5-Stage Daily Pipeline** (월-금 21:30 UTC 부터 순차 시작):
1. **Stage 1 (Benchmark)**: `collected_01_daily_etf_ohlc`
2. **Stage 2 (Sector)**: `collected_01_daily_etf_ohlc`
3. **Stage 3 (Trending Analysis)**: `analytics_03_trending_etfs`
4. **Stage 4 (Holdings/Stock)**: `collected_04_etf_holdings`, `collected_06_daily_stock_history`
5. **Stage 5 (Allocation)**: `analytics_05_portfolio_allocation`

---

#### ✅ 4단계: Spark 분석 Job 실행 (자동 또는 수동)

**자동 실행**: Airflow 5-Stage Pipeline Stage 3, Stage 5에서 자동 실행

**수동 실행**:
```bash
# Spark Job 수동 실행 스크립트 사용
./test/spark_jobs_launcher.sh

# 또는 직접 실행 (예: 포트폴리오 배분)
docker-compose exec spark-master bash -c "\
  /opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 2g \
  --executor-memory 2g \
  --packages org.postgresql:postgresql:42.6.0 \
  batch/spark_02_active_stock_allocator.py"
```

**완료 확인**:
```sql
SELECT as_of_date, period_days, COUNT(*) as stock_count
FROM analytics_05_portfolio_allocation
GROUP BY as_of_date, period_days
ORDER BY as_of_date DESC, period_days;
```

---

## 🧪 테스트 실행

### 단위 테스트 (Unit Tests)

**파일**: `unit_tests/test_spark_jobs.py`
Spark Job의 SQL 쿼리, 데이터 변환 로직 검증

**실행**:
```bash
# 전체 테스트 실행 (test.sh 내부에서 pytest 호출)
./test/test.sh

# 또는 개별 실행
cd test/unit_tests
python -m pytest test_spark_jobs.py -v
```

---

## 🛠️ 트러블슈팅

### 백필 실패 시

**문제**: `yfinance API rate limit exceeded`
- **해결 1**: Staggered runner 사용 (`test\2_run_staggered.bat`)
- **해결 2**: 캐시된 데이터는 24시간 유효하므로 나중에 다시 실행하면 API 호출 없이 진행됩니다.
- **해결 3**: `--delay` 값을 증가시킵니다.

**문제**: `Connection to PostgreSQL failed`
- **해결**: Docker 컨테이너가 정상적으로 실행 중인지 확인하십시오 (`docker-compose ps`).

---

## 📁 캐싱 시스템

백필 스크립트는 효율적인 수집을 위해 스마트 캐싱을 사용합니다.

1. **첫 실행**: yfinance API에서 데이터 다운로드 후 `test/.cache/`에 저장
2. **재실행**: 캐시가 유효하면(24시간 이내) API 호출 없이 캐시 사용
3. **만료 시**: 24시간이 경과하면 자동으로 API에서 새로 다운로드

---

## 📊 데이터 검증

백필 및 테스트 완료 후 다음 쿼리로 데이터 상태를 확인하세요:

```sql
-- 1. 수집된 ETF 데이터 확인
SELECT ticker, COUNT(*) FROM collected_01_daily_etf_ohlc GROUP BY ticker;

-- 2. 수집된 주식 데이터 확인  
SELECT ticker, COUNT(*) FROM collected_06_daily_stock_history GROUP BY ticker;

-- 3. 포트폴리오 분석 결과 확인
SELECT as_of_date, period_days, COUNT(*) FROM analytics_05_portfolio_allocation GROUP BY as_of_date, period_days;
```

---

## 🔗 관련 문서

- [ARCHITECTURE.md](../ARCHITECTURE.md) - 전체 시스템 아키텍처
- [README.md](../README.md) - 프로젝트 개요
- [database/NAMING_CONVENTION.md](../database/NAMING_CONVENTION.md) - 테이블 명명 규칙
