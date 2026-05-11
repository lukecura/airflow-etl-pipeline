# Airflow ETL Pipeline

A simple end-to-end ETL pipeline built with Apache Airflow 3.x, running locally via Docker Compose.

## What it does

The DAG (`taxi_pipeline.py`) defines a four-task pipeline:

1. **extract** — Downloads a public CSV from a URL using `requests`.
2. **clean** — Loads the CSV with `pandas`, drops rows with missing values.
3. **aggregate** — Groups the data and computes summary counts.
4. **report** — Prints the final summary table to the task logs.

Tasks are chained sequentially with the `>>` operator. Each task is a `PythonOperator` calling a Python function.

## Tech stack

- Apache Airflow 3.x
- Python 3.12
- pandas, requests
- Docker Compose (for local orchestration)

## Running locally

1. Clone this repo.
2. Set up Airflow's standard Docker Compose environment:
```bash
   curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
   mkdir -p ./dags ./logs ./plugins ./config
   echo "AIRFLOW_UID=50000" > .env
   cp taxi_pipeline.py dags/
```
3. Initialize and start:
```bash
   docker compose up airflow-init
   docker compose up -d
```
4. Open http://localhost:8080 (login: `airflow` / `airflow`), find `taxi_pipeline`, toggle it on, and trigger a run.

## What I learned

- Defining DAGs and chaining tasks with `>>`.
- The roles of the Airflow scheduler and DAG processor.
- Reading task instance logs and navigating the Airflow 3.x UI.
- Airflow 3.x changes: standard operators moved to `airflow.providers.standard`.
