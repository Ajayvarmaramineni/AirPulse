"""
Air Quality Intelligence Pipeline DAG
--------------------------------------
Schedule: Every 6 hours
Flow:
  1. ingest   — Pull latest readings from OpenAQ for each configured city
  2. validate — Run data quality checks
  3. transform — Run dbt (staging → intermediate → marts)
  4. test     — Run dbt tests (schema + custom)
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

# Make ingestion module importable — resolves to the project root
PROJECT_DIR = Path(__file__).resolve().parents[2]   # dags/ → airflow/ → project root
sys.path.insert(0, str(PROJECT_DIR))

DBT_DIR = str(PROJECT_DIR / "dbt")

# ── Default args ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,       # set to True + add email in prod
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

CITIES = [c.strip() for c in os.environ.get("OPENAQ_CITIES", "Los Angeles,New York").split(",")]

# ── Python callables ──────────────────────────────────────────────────────────

def ingest_city(city: str, **context) -> None:
    """Fetch last 6 hours of readings for one city and load to PostgreSQL."""
    from ingestion.openaq_client import OpenAQClient
    from ingestion.db_loader import load_readings

    client = OpenAQClient()
    rows = client.fetch_city_readings(city=city, hours_back=7)  # 7h overlap to catch late data
    inserted = load_readings(rows)

    # Push row count to XCom for downstream logging
    context["ti"].xcom_push(key=f"rows_inserted_{city}", value=inserted)


def run_data_quality_checks(**context) -> None:
    """
    Basic data quality assertions on raw.openaq_readings.
    Replace with Great Expectations suite for production.
    """
    from ingestion.db_loader import get_connection

    checks = {
        "no_null_parameters": "SELECT COUNT(*) FROM raw.openaq_readings WHERE parameter IS NULL",
        "no_future_timestamps": (
            "SELECT COUNT(*) FROM raw.openaq_readings "
            "WHERE date_utc > NOW() + INTERVAL '1 hour'"
        ),
        "recent_data_exists": (
            "SELECT COUNT(*) FROM raw.openaq_readings "
            "WHERE inserted_at > NOW() - INTERVAL '7 hours'"
        ),
    }

    conn = get_connection()
    failed = []
    try:
        with conn.cursor() as cur:
            for check_name, sql in checks.items():
                cur.execute(sql)
                count = cur.fetchone()[0]
                # "no_" checks should return 0; "exists" checks should return > 0
                if "no_" in check_name and count > 0:
                    failed.append(f"{check_name}: {count} violations")
                elif "exists" in check_name and count == 0:
                    failed.append(f"{check_name}: no recent data found")
    finally:
        conn.close()

    if failed:
        raise ValueError(f"Data quality checks failed:\n" + "\n".join(failed))


def log_pipeline_complete(**context) -> None:
    """Log completion — Streamlit dashboard reads live from DB so no refresh needed."""
    from ingestion.db_loader import get_row_count
    total = get_row_count()
    print(f"Pipeline complete. Total rows in raw.openaq_readings: {total:,}")
    print("Dashboard: streamlit run dashboard/app.py")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="air_quality_intelligence_pipeline",
    description="Ingest OpenAQ data → dbt transform → Superset dashboard refresh",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 */6 * * *",   # every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=["air-quality", "openaq", "dbt", "superset"],
) as dag:

    # ── 1. Ingestion (one task per city, run in parallel) ─────────────────
    with TaskGroup("ingest") as ingest_group:
        ingest_tasks = []
        for city in CITIES:
            task = PythonOperator(
                task_id=f"ingest_{city.lower().replace(' ', '_')}",
                python_callable=ingest_city,
                op_kwargs={"city": city},
            )
            ingest_tasks.append(task)

    # ── 2. Data quality validation ────────────────────────────────────────
    validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=run_data_quality_checks,
    )

    # ── 3. dbt transformations ────────────────────────────────────────────
    with TaskGroup("transform") as transform_group:

        dbt_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=f"cd {DBT_DIR} && dbt run --select staging --profiles-dir {DBT_DIR}",
        )

        dbt_intermediate = BashOperator(
            task_id="dbt_run_intermediate",
            bash_command=f"cd {DBT_DIR} && dbt run --select intermediate --profiles-dir {DBT_DIR}",
        )

        dbt_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=f"cd {DBT_DIR} && dbt run --select marts --profiles-dir {DBT_DIR}",
        )

        dbt_staging >> dbt_intermediate >> dbt_marts

    # ── 4. dbt tests ──────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
    )

    # ── 5. Completion log ─────────────────────────────────────────────────
    pipeline_complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=log_pipeline_complete,
    )

    # ── DAG wiring ────────────────────────────────────────────────────────
    ingest_group >> validate >> transform_group >> dbt_test >> pipeline_complete
