````markdown
# 빠른 참조 - 스케줄 ETF 수집

## 시스템 개요

**수집 유형**: 스케줄 배치 (실시간 스트리밍 아님)  
**빈도**: 하루 4회 (월-금) + 월간 1회 (마지막 토)  
**목적**: Rate-limit 안전한 ETF 데이터 수집

## 일별 스케줄 (월-금, UTC) - 5-Stage Pipeline

| 시간 | Stage | DAG 이름 | 작업 | 소요 |
|------|-------|----------|------|----------|
| 09:00 | 1 | `daily_benchmark_etf_collection` | 벤치마크 6개 ETF OHLC | ~30초 |
| 10:00 | 2 | `daily_sector_etf_collection` | 섹터 11개 ETF OHLC | ~55초 |
| 11:00 | 3 | `daily_trending_etf_analysis` | 트렌딩 ETF 식별 (Spark) | 5-10분 |
| 12:00 | 4 | `daily_trending_etf_holdings_collection` | 트렌딩 ETF holdings만 수집 | 5-15분 |
| 13:00 | 5 | `daily_portfolio_allocation` | 포트폴리오 배분 (Spark) | 5-10분 |

**⚠️ 각 Stage는 하루에 1회만 실행** (스케줄 배치, 연속 아님)

## 비활성화된 DAG (Legacy)

| DAG 이름 | 이전 스케줄 | 비활성화 사유 |
|----------|------------|---------------|
| `daily_benchmark_top_holdings` | 11:00 UTC | 조건부 holdings 수집으로 대체 |
| `daily_sector_top_holdings` | 12:00 UTC | 조건부 holdings 수집으로 대체 |
| `daily_stock_collection` | 09:00-12:00 | 조건부 수집에 통합 |
| `stock_market_pipeline` | 매시간 | 실시간 수집 불필요 |

## ETF 목록

**벤치마크 (6개)**: SPY, QQQ, IWM, EWY, DIA, SCHD  
**섹터 (11개)**: XLK, XLV, XLF, XLY, XLC, XLI, XLP, XLE, XLU, XLRE, XLB

## 빠른 명령어

### 배포
```bash
deploy_staggered_dags.bat
```

### 단일 DAG 활성화
```bash
docker compose exec airflow airflow dags unpause daily_benchmark_etf_collection
```

### 평일 로직 테스트
```bash
# 월요일 (성공해야 함)
docker compose exec airflow airflow dags test daily_benchmark_etf_collection 2024-01-15

# 일요일 (실패해야 함)
docker compose exec airflow airflow dags test daily_benchmark_etf_collection 2024-01-14
```

### 월간 로직 테스트
```bash
# 마지막 토요일 (성공해야 함)
docker compose exec airflow airflow dags test monthly_etf_collection_last_weekend 2024-01-27

# 첫 번째 토요일 (실패해야 함)
docker compose exec airflow airflow dags test monthly_etf_collection_last_weekend 2024-01-06
```

### 모니터링
```bash
docker compose logs -f airflow | grep -E "(Group|ERROR|429)"
```

### 데이터베이스 확인
```bash
docker compose exec postgres psql -U airflow -d market_data
```
```sql
SELECT COUNT(*) FROM analytics_etf_top_holdings WHERE date = CURRENT_DATE;
```

## 임계값

- 벤치마크 데이터 준비: **4/6 ETF** (66%)
- 섹터 데이터 준비: **8/11 ETF** (73%)

## Rate Limit

- 일별 수집: 티커 간 **5초** 간격
- 월간 수집: 티커 간 **8초** 간격

## 종속성

```
그룹 1 → 그룹 3 (2시간)
그룹 2 → 그룹 4 (2시간)
```

## 환경 변수

- `ETF_TYPE_FILTER=benchmark` - 벤치마크 ETF만 필터링
- `ETF_TYPE_FILTER=sector` - 섹터 ETF만 필터링
- (없음) - 모든 ETF 처리

## 파일

- **DAG**: `airflow/dags/daily_*_dag.py`, `monthly_*_dag.py`
- **Spark**: `batch/spark_etf_top_holdings.py`
- **문서**: `SCHEDULING_STRATEGY.md`, `TESTING_GUIDE.md`
- **배포**: `deploy_staggered_dags.bat`

## 트러블슈팅

| 문제 | 해결책 |
|---------|----------|
| 429 에러 | 지연 시간 증가 (5초 → 8초) |
| 주말 실행 | `is_weekday()` 로직 확인 |
| 매주 토요일 월간 실행 | `is_last_weekend()` 계산 검증 |
| 결과 누락 | 데이터 준비 임계값 확인 |

## 성공 지표

✅ 로그에 429 에러 없음  
✅ 일별 작업이 월-금에만 실행  
✅ 월간 작업이 마지막 토요일에만 실행  
✅ 모든 ETF가 4시간 내 수집됨  
✅ 3개 기간의 Top Holdings 계산됨

````