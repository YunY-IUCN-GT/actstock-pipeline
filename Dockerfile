FROM python:3.9-slim AS base

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 패키지 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 요구사항 파일 복사 및 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY . .

# 로그 폴더 생성
RUN mkdir -p logs

# 서비스별 이미지 생성
# ⚠️ Producer/Consumer removed - NO real-time collection (yfinance rate limit)
# Use Airflow DAGs for scheduled batch collection only

FROM base AS dashboard
EXPOSE 8050
CMD ["python", "dashboard/dashboard_finviz_app.py"]
