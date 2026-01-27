#!/bin/bash
# General Test Runner for ActStock Pipeline
# 시스템 컴포넌트 테스트 스크립트

set -e

# 색상 정의
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 로그 함수
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

# 헤더 출력
print_header() {
    echo ""
    echo "================================================"
    echo "  $1"
    echo "================================================"
}

# Docker 컨테이너 상태 확인
test_docker_containers() {
    print_header "1. Docker Container Health Check"
    
    log_info "Checking container status..."
    
    CONTAINERS=(
        "actstock-postgres"
        "actstock-airflow-db"
        "actstock-airflow-webserver"
        "actstock-airflow-scheduler"
        "actstock-api"
        "actstock-dashboard"
        "actstock-kafka"
        "actstock-zookeeper"
        "actstock-spark-master"
        "actstock-consumer-trend"
        "actstock-consumer-hourly"
        "actstock-consumer-active"
    )
    
    ALL_HEALTHY=true
    
    for container in "${CONTAINERS[@]}"; do
        STATUS=$(docker inspect --format='{{.State.Status}}' $container 2>/dev/null || echo "not_found")
        HEALTH=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}no_health_check{{end}}' $container 2>/dev/null || echo "not_found")
        
        if [ "$STATUS" = "running" ]; then
            if [ "$HEALTH" = "healthy" ] || [ "$HEALTH" = "no_health_check" ]; then
                log_success "$container: $STATUS ($HEALTH)"
            else
                log_warning "$container: $STATUS ($HEALTH)"
                ALL_HEALTHY=false
            fi
        else
            log_error "$container: $STATUS"
            ALL_HEALTHY=false
        fi
    done
    
    echo ""
    if [ "$ALL_HEALTHY" = true ]; then
        log_success "All containers are healthy"
        return 0
    else
        log_error "Some containers are not healthy"
        return 1
    fi
}

# 데이터베이스 연결 테스트
test_database_connection() {
    print_header "2. Database Connection Test"
    
    log_info "Testing PostgreSQL connection..."
    
    # Main DB 테스트
    docker compose exec -T postgres psql -U actstock_user -d actstock_db -c "SELECT version();" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        log_success "Main DB (actstock_db) connection OK"
    else
        log_error "Main DB connection FAILED"
        return 1
    fi
    
    # Airflow DB 테스트
    docker compose exec -T airflow-db psql -U airflow -d airflow -c "SELECT version();" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        log_success "Airflow DB connection OK"
    else
        log_error "Airflow DB connection FAILED"
        return 1
    fi
    
    return 0
}

# API 엔드포인트 테스트
test_api_endpoints() {
    print_header "3. API Endpoint Test"
    
    log_info "Testing API endpoints..."
    
    # Health check
    HEALTH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health)
    if [ "$HEALTH_STATUS" = "200" ]; then
        log_success "API health check: $HEALTH_STATUS"
    else
        log_error "API health check: $HEALTH_STATUS"
        return 1
    fi
    
    # Stocks endpoint
    STOCKS_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/v1/stocks/)
    if [ "$STOCKS_STATUS" = "200" ]; then
        log_success "Stocks API: $STOCKS_STATUS"
    else
        log_warning "Stocks API: $STOCKS_STATUS (might be empty)"
    fi
    
    # Sectors endpoint
    SECTORS_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/v1/sectors/)
    if [ "$SECTORS_STATUS" = "200" ]; then
        log_success "Sectors API: $SECTORS_STATUS"
    else
        log_warning "Sectors API: $SECTORS_STATUS (might be empty)"
    fi
    
    return 0
}

# Kafka 토픽 확인
test_kafka_topics() {
    print_header "4. Kafka Topics Check"
    
    log_info "Checking Kafka topics..."
    
    TOPICS=$(docker compose exec -T kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null)
    
    if [ -z "$TOPICS" ]; then
        log_warning "No Kafka topics found (may need to run producer first)"
    else
        echo "$TOPICS" | while read -r topic; do
            log_success "Topic: $topic"
        done
    fi
    
    return 0
}

# Airflow DAG 상태 확인
test_airflow_dags() {
    print_header "5. Airflow DAGs Status"
    
    log_info "Checking Airflow DAGs..."
    
    # Airflow CLI를 통한 DAG 리스트 확인
    DAGS=$(docker compose exec -T airflow-webserver airflow dags list 2>/dev/null | tail -n +4)
    
    if [ -z "$DAGS" ]; then
        log_warning "No DAGs found"
    else
        echo "$DAGS" | while read -r dag; do
            if [ -n "$dag" ]; then
                log_success "DAG: $dag"
            fi
        done
    fi
    
    return 0
}

# 테이블 데이터 확인
test_database_tables() {
    print_header "6. Database Tables Check"
    
    log_info "Checking database tables..."
    
    # ETF 테이블 확인
    ETF_COUNT=$(docker compose exec -T postgres psql -U actstock_user -d actstock_db -t -c "SELECT COUNT(*) FROM collected_daily_etf_ohlc;" 2>/dev/null | tr -d ' ')
    if [ -n "$ETF_COUNT" ]; then
        log_success "collected_daily_etf_ohlc: $ETF_COUNT rows"
    else
        log_warning "collected_daily_etf_ohlc: Table may not exist or is empty"
    fi
    
    # Holdings 테이블 확인
    HOLDINGS_COUNT=$(docker compose exec -T postgres psql -U actstock_user -d actstock_db -t -c "SELECT COUNT(*) FROM etf_holdings;" 2>/dev/null | tr -d ' ')
    if [ -n "$HOLDINGS_COUNT" ]; then
        log_success "etf_holdings: $HOLDINGS_COUNT rows"
    else
        log_warning "etf_holdings: Table may not exist or is empty"
    fi
    
    return 0
}

# 메인 실행
main() {
    echo "================================================"
    echo "  ActStock Pipeline - System Test Runner"
    echo "================================================"
    
    FAILED_TESTS=0
    
    # 각 테스트 실행
    test_docker_containers || ((FAILED_TESTS++))
    
    test_database_connection || ((FAILED_TESTS++))
    
    test_api_endpoints || ((FAILED_TESTS++))
    
    test_kafka_topics || ((FAILED_TESTS++))
    
    test_airflow_dags || ((FAILED_TESTS++))
    
    test_database_tables || ((FAILED_TESTS++))
    
    # 최종 결과
    echo ""
    print_header "Test Summary"
    
    if [ $FAILED_TESTS -eq 0 ]; then
        log_success "All tests passed! ✓"
        echo ""
        echo "System Status: HEALTHY"
        echo "Airflow UI: http://localhost:8080"
        echo "API Docs: http://localhost:8000/docs"
        echo "Dashboard: http://localhost:8050"
    else
        log_error "$FAILED_TESTS test(s) failed"
        echo ""
        echo "Please check the errors above and fix them."
    fi
    
    echo "================================================"
    
    exit $FAILED_TESTS
}

main "$@"
