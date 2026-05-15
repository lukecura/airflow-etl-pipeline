"""
Apple Stock Monthly ETL Pipeline
Downloads daily Apple share prices for 2014, cleans the data, aggregates it
into monthly statistics (open, close, high, low, average, return %),
and reports the summary. Demonstrates a simple ETL DAG with four tasks.
"""
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DATA_DIR = Path("/opt/airflow/logs/apple_stock_pipeline")
RAW_PATH = DATA_DIR / "raw.csv"
CLEAN_PATH = DATA_DIR / "clean.csv"
OUTPUT_PATH = DATA_DIR / "monthly_summary.csv"

# Daily Apple Inc. (AAPL) closing share prices for 2014.
# Columns: AAPL_x (date), AAPL_y (closing price USD).
DATA_URL = (
    "https://raw.githubusercontent.com/plotly/datasets/master/"
    "2014_apple_stock.csv"
)


def extract():
    """Task 1: Download raw Apple stock CSV from public source."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    response = requests.get(DATA_URL, timeout=30)
    response.raise_for_status()
    RAW_PATH.write_bytes(response.content)
    print(f"Downloaded {len(response.content)} bytes to {RAW_PATH}")


def clean():
    """Task 2: Load CSV, drop rows with missing prices, save cleaned version."""
    df = pd.read_csv(RAW_PATH)
    print(f"Raw shape: {df.shape}")
    df = df.dropna()
    print(f"Cleaned shape: {df.shape}")
    df.to_csv(CLEAN_PATH, index=False)


def aggregate():
    """Task 3: Aggregate daily prices into monthly statistics:
    open, close, high, low, average, and month-over-month return percentage."""
    df = pd.read_csv(CLEAN_PATH)

    df["AAPL_x"] = pd.to_datetime(df["AAPL_x"])
    df = df.sort_values("AAPL_x")
    df["year_month"] = df["AAPL_x"].dt.to_period("M").astype(str)

    monthly = df.groupby("year_month").agg(
        trading_days=("AAPL_y", "count"),
        month_open=("AAPL_y", "first"),
        month_close=("AAPL_y", "last"),
        month_high=("AAPL_y", "max"),
        month_low=("AAPL_y", "min"),
        month_avg=("AAPL_y", "mean"),
    ).reset_index()

    monthly["month_return_pct"] = (
        (monthly["month_close"] - monthly["month_open"]) / monthly["month_open"] * 100
    ).round(2)

    for col in ["month_open", "month_close", "month_high", "month_low", "month_avg"]:
        monthly[col] = monthly[col].round(2)

    print(f"Monthly summary:\n{monthly.to_string(index=False)}")
    monthly.to_csv(OUTPUT_PATH, index=False)


def report():
    """Task 4: Print monthly summary to logs for visibility."""
    df = pd.read_csv(OUTPUT_PATH)
    print(f"Monthly summary written to {OUTPUT_PATH}")
    print(f"Months covered: {len(df)}")
    print(df.to_string(index=False))


default_args = {
    "owner": "luke",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="apple_stock_monthly_etl",
    description="ETL: download daily AAPL prices, clean, aggregate to monthly, report",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning", "etl", "finance"],
) as dag:

    extract_task = PythonOperator(task_id="extract",   python_callable=extract)
    clean_task   = PythonOperator(task_id="clean",     python_callable=clean)
    agg_task     = PythonOperator(task_id="aggregate", python_callable=aggregate)
    report_task  = PythonOperator(task_id="report",    python_callable=report)

    extract_task >> clean_task >> agg_task >> report_task
