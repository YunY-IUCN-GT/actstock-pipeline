# 액티브 주식 배분 파이프라인

**트렌딩 기반 포트폴리오 자동 구성 시스템**

yfinance + Kafka + Spark + PostgreSQL + Airflow로 구성된 **스케줄 배치 전용** 데이터 파이프라인

⚠️ **중요**: NO REAL-TIME/STREAMING COLLECTION (yfinance rate limit 회피)

---

## 🎯 핵심 기능

**5-Stage 트렌딩 ETF 기반 스마트 포트폴리오 자동 구성**

### 🆕 멀티기간 분석 (Multi-Period Portfolio)
- **5일 단기**: 빠른 반응, 변동성 높음
- **10일 중기**: 균형잡힌 접근
- **20일 장기**: 안정적인 트렌드
- 각 기간별로 독립적인 포트폴리오 생성
- 대시보드에서 기간 선택 가능

### � 대시보드 (Dashboard - Port 8050)
- **다국어 지원**: 한국어 UI (Bootstrap 기반 디자인)
- **멀티 기간 탭**: 5일/10일/20일/월간 비교 (각 탭별 20종목)
- **ETF 추적**: 
  - 섹터 ETF (10개): QQQ, XLF, XLV, XLY, XLC, XLI, XLP, XLU, XLRE, XLB
  - 벤치마크 ETF (5개): SPY, IWM, DIA, EWY, SCHD
  - 총 15개 unique ETFs (QQQ는 Technology 섹터 대표)
- **색상 구분**: 벤치마크 ETF는 노란색 배경으로 표시
- **당월 ETF 성과**: 전체 15개 ETF의 20일 수익률 순위 표시
- **월간 섹터 비교**: 최근 12개월 섹터별 + 벤치마크 성과 추이 테이블

### �🚫 수집 제약사항
- ❌ 실시간(real-time) 수집 금지
- ❌ 스트리밍(streaming) 수집 금지
- ❌ 시간별(hourly) 자동 수집 금지
- ✅ Airflow 스케줄 배치 수집만 사용 (월-금 09:00-13:00 UTC)

### 트렌딩 ETF 식별 (Stage 3)
- ETF 수익률 > SPY **AND** ETF 수익률 > 0%
- 20일 기준으로 outperformance 계산

### 포트폴리오 구성 (Stage 5)
1. **멀티기간 분석**: 5일/10일/20일 각각 독립적으로 계산
2. **선정 로직**: 각 트렌딩 ETF당 TOP 1 최고 성과 종목 선택
3. **가중치 계산**: Weight = Performance × (1/Market Cap)
   - 고성과 + 소형주 = 높은 비중 (더 큰 수익 잠재력)
   - 정규화하여 총합 100%
4. **결과 저장**: `analytics_05_portfolio_allocation` (period_days: 5, 10, 20)

---

## 🚀 빠른 시작

```bash
# 시스템 시작
./start.sh  # Linux/Mac
.\start.bat # Windows

# 상태 확인
docker compose ps

# 대시보드 접속
http://localhost:8050

# Airflow 접속 및 실행
# http://localhost:8080 (admin/admin)
# 'daily_pipeline_controller' DAG를 활성화(Unpause) 하세요.
```

---

## 📊 데이터 플로우

```
5-Stage Pipeline (Mon-Fri, Starting 21:30 UTC via Controller):

Stage 1 (Start) │ Benchmark ETF Collection
      └─→ collected_01_daily_etf_ohlc
      ↓ (1 hour delay)
Stage 2         │ Sector ETF Collection  
      └─→ collected_01_daily_etf_ohlc
      ↓ (Immediate)
Stage 3         │ Trending ETF Analysis (Spark)
      └─→ Write: analytics_03_trending_etfs
      ↓ (1 hour delay)
Stage 4         │ Conditional Holdings Collection
      └─→ Write: collected_04_etf_holdings, collected_06_daily_stock_history
      ↓ (Immediate)
Stage 5         │ Multi-Period Portfolio Allocation (Spark)
      └─→ Write: analytics_05_portfolio_allocation

API/Dashboard:
      └─→ Read: analytics_03_trending_etfs, analytics_05_portfolio_allocation
```

<!-- 상세 스케줄 정보는 ARCHITECTURE.md를 참조하십시오. -->

---

## 🗄️ 주요 테이블 (데이터 플로우 기반 명명)

### Collected (수집 데이터)
| 테이블 | 용도 | 업데이트 주기 |
|--------|------|---------------|
| `collected_01_daily_etf_ohlc` | 일별 ETF OHLC (17 ETFs) | Stage 1+2 (09:00, 10:00 UTC) |
| `collected_04_etf_holdings` | ETF 보유종목 (조건부) | Stage 4 (12:00 UTC - 트렌딩만) |
| `collected_06_daily_stock_history` | 일별 주식 OHLC (조건부) | Stage 4 (12:00 UTC - 트렌딩만) |
| `collected_00_meta_etf` | ETF 메타데이터 (17 ETFs) | 정적 데이터 |

### Analytics (분석 결과 - Spark 계산)
| 테이블 | 용도 | 업데이트 주기 |
|--------|------|---------------|
| `analytics_03_trending_etfs` | 트렌딩 ETF 식별 (vs SPY) | 매일 11:00 UTC (Stage 3) |
| `analytics_05_portfolio_allocation` | **멀티기간** 포트폴리오 배분 (5d/10d/20d) | 매일 13:00 UTC (Stage 5) |

### Logs (로그)
| 테이블 | 용도 | 업데이트 주기 |
|--------|------|---------------|
| `logs_consumer_error` | Consumer 에러 로그 | 실시간 |

---

### 📖 상세 문서
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: 전체 시스템 아키텍처 및 상세 데이터 플로우
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)**: 자주 사용하는 명령어 퀵 레퍼런스
- **[test/README.md](test/README.md)**: 테스트 및 백필 도구 가이드
- **[database/NAMING_CONVENTION.md](database/NAMING_CONVENTION.md)**: 데이터베이스 명명 규칙

---

## 🚀 초기 설정 및 실행

### 1단계: 시스템 시작
```bash
# Linux/Mac
./start.sh

# Windows
start.bat

# 상태 확인
docker compose ps
```

### 2단계: 초기 데이터 백필 (선택)
```bash
# 5-Stage 파이프라인은 자동으로 데이터 수집
# 과거 데이터가 필요한 경우에만 백필 실행

# ETF 벤치마크 데이터 백필 (기본값: 최근 20 거래일)
docker compose exec spark-master python /app/backfill_benchmarks.py
```

### 3단계: Airflow DAG 활성화
```bash
# Airflow UI (http://localhost:8080)에서 DAG 활성화:
# 1. 01_daily_benchmark_etf_collection_dag (09:00 UTC)
# 2. 02_daily_sector_etf_collection_dag (10:00 UTC)
# 3. 03_daily_trending_etf_analysis_dag (11:00 UTC)
# 4. 04_daily_trending_etf_holdings_collection_dag (12:00 UTC)
# 5. 05_daily_portfolio_allocation_dag (13:00 UTC)

# 수동 Spark 실행:
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/batch/spark_02_active_stock_allocator.py
```

---

## ▶️ 고급 사용

### ETF 벤치마크 데이터 업데이트
```bash
# 기본 실행 (최근 20 거래일, 캐시 사용)
docker compose exec api python /app/test/1_backfill_etf_benchmarks.py

# 더 많은 데이터 수집 (예: 60 거래일)
docker compose exec api python /app/test/1_backfill_etf_benchmarks.py --days 60 --delay 2.0

# 캐시 초기화 (필요 시)
rm -rf test/.cache/

# 주의: yfinance rate-limit 발생 시 자동 중단 및 재개 시간 표시
```

### Spark Job 수동 실행
```bash
docker compose exec spark-master bash -c "\
  /opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 2g \
  --executor-memory 2g \
  --packages org.postgresql:postgresql:42.6.0 \
  batch/spark_02_active_stock_allocator.py"
```

### 데이터 검증
```sql
-- 멀티기간 포트폴리오 결과 확인
SELECT 
    period_days,
    COUNT(*) as stock_count,
    MAX(as_of_date) as latest_date,
    ROUND(SUM(portfolio_weight) * 100, 2) as total_weight_pct
FROM analytics_05_portfolio_allocation
GROUP BY period_days
ORDER BY period_days;

-- 특정 기간 포트폴리오 상세
SELECT 
    as_of_date,
    ticker,
    company_name,
    ROUND(portfolio_weight * 100, 2) as weight_pct,
    ROUND(return_pct, 2) as return_pct,
    market_cap,
    allocation_reason
FROM analytics_05_portfolio_allocation
WHERE as_of_date = (SELECT MAX(as_of_date) FROM analytics_05_portfolio_allocation)
  AND period_days = 20  -- 5, 10, 또는 20
ORDER BY portfolio_weight DESC
LIMIT 10;

-- 트렌딩 ETF 확인
SELECT 
    etf_ticker,
    ROUND(return_pct, 2) as etf_return,
    is_trending
FROM analytics_03_trending_etfs
WHERE as_of_date = (SELECT MAX(as_of_date) FROM analytics_03_trending_etfs)
ORDER BY return_pct DESC;
```

---

## 🌐 서비스 접속

| 서비스 | URL | 비고 |
|--------|-----|------|
| 대시보드 | http://localhost:8050 | 기간 선택 드롭다운 (5일/10일/20일) |
| Airflow | http://localhost:8080 | ID: admin / PW: admin |
| API | http://localhost:8000/docs | Swagger UI 문서 |
| Spark Master | http://localhost:8081 | 클러스터 상태 |

### 주요 API 엔드포인트

```bash
# 멀티기간 포트폴리오 조회
GET /stocks/portfolio?period_days=5   # 5일 단기
GET /stocks/portfolio?period_days=10  # 10일 중기
GET /stocks/portfolio?period_days=20  # 20일 장기

# 트렌딩 ETF 조회
GET /dashboard/trending-etfs

# Active 포트폴리오 (기간별)
GET /dashboard/active-allocations?period_days=20
```

---

## 📚 문서
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - 상세 시스템 아키텍처 및 데이터 플로우
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - 자주 사용하는 명령어 모음
- **[test/README.md](test/README.md)** - 테스트 및 백필 도구 가이드
- **[database/NAMING_CONVENTION.md](database/NAMING_CONVENTION.md)** - 데이터베이스 명명 규칙

---

**마지막 업데이트**: 2026-01-26  
**명명 규칙**: 데이터 플로우 기반 (collected/analytics/aggregated/logs)
