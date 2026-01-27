#!/bin/bash
# Spark Jobs Manual Launcher
# Spark 작업을 수동으로 실행하는 스크립트

set -e

SPARK_MASTER="spark://spark-master:7077"
BATCH_DIR="/opt/airflow/project/batch"

# 색상 정의
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# 사용법 출력
usage() {
    echo "Usage: $0 [job_name]"
    echo ""
    echo "Available jobs:"
    echo "  1. trending        - 트렌딩 ETF 식별"
    echo "  2. allocation      - 액티브 주식 배분"
    echo "  3. top-holdings    - ETF 상위 보유 종목 분석"
    echo "  4. rebalance       - 월간 포트폴리오 리밸런싱"
    echo "  5. all             - 모든 작업 순차 실행"
    echo ""
    echo "Examples:"
    echo "  $0 trending"
    echo "  $0 all"
    exit 1
}

# Spark 작업 실행 함수
run_spark_job() {
    local job_name=$1
    local script_path=$2
    
    log_info "Starting Spark Job: $job_name"
    
    docker compose exec spark-master spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --conf spark.driver.memory=2g \
        --conf spark.executor.memory=2g \
        --conf spark.sql.shuffle.partitions=10 \
        $script_path
    
    if [ $? -eq 0 ]; then
        log_success "$job_name completed successfully"
    else
        log_error "$job_name failed"
        exit 1
    fi
}

# 메인 로직
main() {
    if [ $# -eq 0 ]; then
        usage
    fi
    
    JOB=$1
    
    echo "================================================"
    echo "  Spark Jobs Manual Launcher"
    echo "================================================"
    echo ""
    
    case $JOB in
        trending|1)
            run_spark_job "Trending ETF Identifier" "$BATCH_DIR/spark_trending_etf_identifier.py"
            ;;
        allocation|2)
            run_spark_job "Active Stock Allocator" "$BATCH_DIR/spark_active_stock_allocator.py"
            ;;
        top-holdings|3)
            run_spark_job "ETF Top Holdings" "$BATCH_DIR/spark_etf_top_holdings.py"
            ;;
        rebalance|4)
            run_spark_job "Monthly Portfolio Rebalancer" "$BATCH_DIR/spark_monthly_portfolio_rebalancer.py"
            ;;
        all|5)
            log_info "Running all jobs sequentially..."
            echo ""
            
            run_spark_job "Trending ETF Identifier" "$BATCH_DIR/spark_trending_etf_identifier.py"
            echo ""
            
            run_spark_job "Active Stock Allocator" "$BATCH_DIR/spark_active_stock_allocator.py"
            echo ""
            
            run_spark_job "ETF Top Holdings" "$BATCH_DIR/spark_etf_top_holdings.py"
            echo ""
            
            run_spark_job "Monthly Portfolio Rebalancer" "$BATCH_DIR/spark_monthly_portfolio_rebalancer.py"
            
            log_success "All jobs completed successfully!"
            ;;
        *)
            log_error "Unknown job: $JOB"
            echo ""
            usage
            ;;
    esac
    
    echo ""
    echo "================================================"
    echo "  Job execution completed"
    echo "================================================"
}

main "$@"
