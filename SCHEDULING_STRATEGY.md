# ETF 데이터 수집 스케줄링 전략 (Controller 기반)

## 개요
이 문서는 yfinance API Rate Limit을 피하고 데이터 의존성을 보장하기 위해 설계된 **Controller DAG 기반 순차적 스케줄링 전략**을 설명합니다.

## 핵심 변경 사항 (Controller 도입)
기존의 시간 기반 독립 실행 방식에서 **Master Controller DAG**가 전체 파이프라인을 순차적으로 제어하는 방식으로 변경되었습니다.

### ✅ 새로운 실행 로직
1. **단일 진입점**: `daily_pipeline_controller_dag`만 스케줄링됨 (매일 21:30 UTC, 장 마감 30분 후)
2. **순차 실행**: Controller가 각 단계(Stage)를 순서대로 트리거하고 완료를 대기
3. **지연 보장**: 단계 사이에 명시적인 대기 시간(1시간)을 Controller가 강제

## 상세 스케줄 (월-금, UTC)

**트리거**: 매일 21:30 UTC (`00_daily_pipeline_controller_dag`)

| 순서 | 작업 (Stage) | 실행 시점 | 비고 |
|------|-------------|-----------|------|
| 1 | **Stage 1**: Benchmark ETF Collection | 21:30 UTC | 장 마감 +30분 후 시작 |
| - | *(대기)* | Stage 1 완료 후 | **1시간 대기** |
| 2 | **Stage 2**: Sector ETF Collection | Stage 1 + 1h | 벤치마크 수집 완료 보장 |
| 3 | **Stage 3**: Trending ETF Analysis (Spark) | Stage 2 완료 즉시 | 데이터분석 즉시 실행 |
| - | *(대기)* | Stage 3 완료 후 | **1시간 대기** |
| 4 | **Stage 4**: Trending Holdings Collection | Stage 3 + 1h | 트렌딩 분석 완료 보장 |
| 5 | **Stage 5**: Portfolio Allocation (Spark) | Stage 4 완료 즉시 | 최종 포트폴리오 생성 |

## 단계별 상세

### Controller (`00_daily_pipeline_controller_dag`)
- **역할**: 오케스트레이션 마스터
- **스케줄**: `30 21 * * 1-5` (유일하게 스케줄링되는 DAG)
- **로직**: `TriggerDagRunOperator`와 `PythonOperator(sleep)`를 사용하여 순차 실행 제어

### Stage 1: Benchmark ETF (`01_daily_benchmark_etf_collection_dag`)
- **대상**: SPY, QQQ, IWM, EWY, DIA, SCHD
- **실행**: Controller에 의해 트리거 (Schedule=None)
- **특징**: 가장 먼저 실행되어 시장 데이터 확보

### Stage 2: Sector ETF (`02_daily_sector_etf_collection_dag`)
- **대상**: 10개 섹터 ETF (XLF, XLV 등)
- **실행**: Stage 1 완료 **1시간 후** 트리거 (Schedule=None)
- **이유**: yfinance API 부하 분산

### Stage 3: Trending Analysis (`03_daily_trending_etf_analysis_dag`)
- **작업**: Spark로 트렌딩 ETF 식별 (vs SPY)
- **실행**: Stage 2 완료 **즉시** 트리거
- **이유**: 데이터 수집이 끝났으므로 분석은 바로 수행 가능

### Stage 4: Holdings Collection (`04_daily_trending_etf_holdings_collection_dag`)
- **작업**: 트렌딩 ETF의 보유 종목 수집
- **실행**: Stage 3 완료 **1시간 후** 트리거
- **이유**: 분석 결과 확인 및 API 쿨링 타임

### Stage 5: Portfolio Allocation (`05_daily_portfolio_allocation_dag`)
- **작업**: Spark로 포트폴리오 비중 계산 (5d/10d/20d)
- **실행**: Stage 4 완료 **즉시** 트리거

## 수동 실행 및 Backfill
- **수동 실행**: Airflow UI에서 `daily_pipeline_controller` DAG만 트리거하면 전체 파이프라인이 순차적으로 실행됩니다.
- **실패 시 복구**: 특정 단계가 실패하면, 문제를 해결한 후 해당 단계의 Sub-DAG를 클리어하거나 Controller를 다시 실행할 수 있습니다.
- **Backfill**: `TriggerDagRunOperator`가 execution_date를 전파하므로 Controller DAG를 Backfill하면 하위 DAG들도 해당 날짜로 실행됩니다.

## 장점
1. **확실한 순서 보장**: 이전 단계가 실패하면 다음 단계가 실행되지 않음 (데이터 정합성 유지)
2. **명확한 지연 시간**: `ExternalTaskSensor`의 폴링 방식보다 명시적인 `sleep`이나 `TimeDelta`가 API Rate Limit 관리에 더 유리
3. **관리 용이성**: Controller DAG 하나만 관리하면 됨

## 모니터링
Airflow의 **Graph View** 또는 **Gantt Chart**에서 `daily_pipeline_controller`를 보면 전체 흐름과 진행 상황을 한눈에 파악할 수 있습니다.