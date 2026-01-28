# 빠른 참조 - Controller 기반 실행

## 시스템 실행 방식 변경
이제 모든 파이프라인 스테이지는 **Controller DAG**에 의해 중앙 제어됩니다. 개별 DAG를 수동으로 실행하지 마십시오.

## 핵심 명령어

### 전체 파이프라인 수동 실행
```bash
# Docker 내부에서 실행 시
docker compose exec airflow airflow dags trigger daily_pipeline_controller

# 또는 Airflow UI (http://localhost:8080)에서 'daily_pipeline_controller' 재생 버튼 클릭
```

### 특정 날짜 데이터 Backfill
```bash
# 예: 2024-01-15 데이터 다시 처리
docker compose exec airflow airflow dags trigger daily_pipeline_controller -e 2024-01-15
```

## 스케줄 및 지연
- **시작**: 21:30 UTC (`daily_pipeline_controller`)
- **Stage 1 실행**
- **1시간 대기**
- **Stage 2 실행**
- **Stage 3 실행** (즉시)
- **1시간 대기**
- **Stage 4 실행**
- **Stage 5 실행** (즉시)

## 상태 확인
Airflow UI에서 `daily_pipeline_controller`의 **Graph View**를 통해 현재 어느 단계가 실행 중인지 또는 대기 중(Sleep)인지 확인할 수 있습니다.