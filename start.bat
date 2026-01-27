@echo off
REM Windows Complete Startup Script for Active Stock Allocation
REM Starts all services: Kafka, Postgres, Airflow, Spark, Dashboard, API

echo ========================================
echo Active Stock Allocation - Complete Start
echo ========================================
echo.

echo [1/7] Checking Docker...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker is not installed or not running!
    echo Please install Docker Desktop and try again.
    pause
    exit /b 1
)
echo ✓ Docker is ready!
echo.

echo [2/7] Starting infrastructure services...
echo - Zookeeper, Kafka, PostgreSQL, Airflow DB
docker-compose up -d zookeeper kafka postgres airflow-db
if %errorlevel% neq 0 (
    echo ERROR: Failed to start infrastructure services!
    pause
    exit /b 1
)
echo ✓ Infrastructure started
echo.

echo [3/7] Waiting for databases to be ready (20 seconds)...
timeout /t 20 /nobreak
echo ✓ Databases ready
echo.

echo [4/7] Starting Spark cluster...
echo - Spark Master, Spark Worker
docker-compose up -d spark-master spark-worker
echo ✓ Spark cluster started
echo.

echo [5/7] Starting Airflow services...
echo - Airflow Webserver, Scheduler
docker-compose up -d airflow-webserver airflow-scheduler
echo ✓ Airflow started
echo.

echo [6/7] Waiting for Airflow to initialize (30 seconds)...
timeout /t 30 /nobreak
echo ✓ Airflow ready
echo.

echo [7/7] Starting application services...
echo - Dashboard, API (NO collector/consumer - using Airflow scheduled batch only)
docker-compose up -d dashboard api
if %errorlevel% neq 0 (
    echo WARNING: Some application services may have failed
)
echo ✓ Applications started
echo.

echo ========================================
echo All Services Started Successfully!
echo ========================================
echo.
echo 📊 WEB INTERFACES:
echo    Dashboard:     http://localhost:8050
echo    Airflow:       http://localhost:8080 (admin/admin)
echo    Spark Master:  http://localhost:8081
echo    Spark Worker:  http://localhost:8082
echo    API:           http://localhost:8000
echo.
echo 🗄️ DATABASES:
echo    PostgreSQL:    localhost:5432 (postgres/postgres/stockdb)
echo    Airflow DB:    localhost:5433 (airflow/airflow/airflow)
echo.
echo 📡 KAFKA:
echo    Bootstrap:     localhost:9092
echo.
echo ========================================
echo Service Status Check:
echo ========================================
docker-compose ps
echo.
echo ========================================
echo Next Steps:
echo ========================================
echo 1. Open Dashboard:     http://localhost:8050
echo 2. Check Airflow DAGs: http://localhost:8080
echo 3. Monitor Spark:      http://localhost:8081
echo.
echo 📋 Useful Commands:
echo    Stop all:       docker-compose down
echo    View logs:      docker-compose logs -f [service-name]
echo    Restart service: docker-compose restart [service-name]
echo    Check status:   docker-compose ps
echo.
echo 🔄 Trigger benchmark backfill (optional):
echo    docker exec actstock-airflow-webserver airflow dags trigger benchmark_data_historical_backfill
echo.
pause
