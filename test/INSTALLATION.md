# Backfill Scripts - Installation Guide

## Prerequisites

You need to install Python packages before running the backfill scripts.

## Installation Steps

### Option 1: Using the install script (Recommended)

Open **PowerShell** or **CMD** in the `test` directory and run:

```cmd
conda activate conda_DE
install_requirements.bat
```

### Option 2: Manual installation

In your conda_DE environment, run:

```cmd
conda activate conda_DE
pip install psycopg2-binary==2.9.7
pip install yfinance==0.2.54
pip install pandas==2.1.0
pip install python-dotenv==1.0.0
```

### Option 3: Install from project requirements

From the project root directory:

```cmd
conda activate conda_DE
pip install -r requirements.txt
```

## After Installation

Run the backfill scripts:

```cmd
cd test

# Backfill benchmark ETFs (6개) - 20일
1_run_backfill.bat

# Backfill sector ETFs (10개) - 20일, 시간차 수집
2_run_staggered.bat
```

## Troubleshooting

If you get `ModuleNotFoundError: No module named 'psycopg2'`:
1. Make sure conda_DE environment is activated: `conda activate conda_DE`
2. Verify pip is using the correct environment: `where pip`
3. Install missing package: `pip install psycopg2-binary`

If you get database connection errors:
1. Make sure Docker containers are running: `docker compose ps`
2. Check database is healthy: `bash test.sh` (from test directory)
