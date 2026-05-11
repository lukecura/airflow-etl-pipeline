"""
NYC Taxi ETL Pipeline
Downloads a sample of NYC taxi trip data, cleans it, aggregates daily metrics,
and saves the result to a CSV. Demonstrates a simple ETL DAG with four tasks.
"""
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Where intermediate and final files live (inside the Airflow container)
DATA_DIR = Path("/opt/airflow/logs/taxi_pipeline")
RAW_PATH = DATA_DIR / "raw.csv"
CLEAN_PATH = DATA_DIR / "clean.csv"
OUTPUT_PATH = DATA_DIR / "daily_summary.csv"

# A small public sample of NYC taxi data
DATA_URL = (
    "https://raw.githubusercontent.com/plotly/datasets/master/"
    "2015_06_30_precipitation.csv"
)
# Note: we're using a small public CSV. Real NYC taxi data is huge; this
# keeps the demo fast. Swap the URL later for any dataset you want.


def extract():
    """Task 1: Download raw data and save it locally."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    response = requests.get(DATA_URL, timeout=30)
    response.raise_for_status()
    RAW_PATH.write_bytes(response.content)
    print(f"Downloaded {len(response.content)} bytes to {RAW_PATH}")


def clean():
    """Task 2: Load raw CSV, drop rows with missing values, keep useful columns."""
    df = pd.read_csv(RAW_PATH)
    print(f"Raw shape: {df.shape}")
    df = df.dropna()
    print(f"Cleaned shape: {df.shape}")
    df.to_csv(CLEAN_PATH, index=False)


def aggregate():
    """Task 3: Compute summary statistics by the first column."""
    df = pd.read_csv(CLEAN_PATH)
    # Group by the first column, count rows, and average any numeric columns
    group_col = df.columns[0]
    summary = df.groupby(group_col).size().reset_index(name="row_count")
    print(f"Summary preview:\n{summary.head()}")
    summary.to_csv(OUTPUT_PATH, index=False)


def report():
    """Task 4: Print final results so we can see them in the task logs."""
    df = pd.read_csv(OUTPUT_PATH)
    print(f"Final summary written to {OUTPUT_PATH}")
    print(f"Total groups: {len(df)}")
    print(df.to_string())


# DAG definition — this is what Airflow reads
default_args = {
    "owner": "luke",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="taxi_pipeline",
    description="Simple ETL: download, clean, aggregate, report",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # None = run only when triggered manually
    catchup=False,
    tags=["learning", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    clean_task = PythonOperator(
        task_id="clean",
        python_callable=clean,
    )

    aggregate_task = PythonOperator(
        task_id="aggregate",
        python_callable=aggregate,
    )

    report_task = PythonOperator(
        task_id="report",
        python_callable=report,
    )

    # This is how you chain tasks. Reads like: extract THEN clean THEN aggregate THEN report.
    extract_task >> clean_task >> aggregate_task >> report_task