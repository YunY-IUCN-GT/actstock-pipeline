#!/bin/bash
# Airflow 초기화 스크립트

set -e

# Remove old SQLite database and config if they exist
echo "🧹 기존 SQLite 데이터베이스 및 설정 파일 제거 중..."
rm -f /opt/airflow/airflow.db /opt/airflow/airflow.cfg

# Ensure environment variable is set
echo "🔧 데이터베이스 연결 설정: $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"

echo "🚀 Airflow 데이터베이스 초기화 중..."
airflow db init

echo "👤 Airflow 관리자 사용자 생성..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || echo "User already exists"

echo "✅ Airflow 초기화 완료!"
echo "🌐 Airflow UI: http://localhost:8080"
echo "👤 Username: admin"
echo "🔐 Password: admin"
