"""
run_pipeline.py
---------------
Runs the full AirPulse pipeline once, end to end:
  1. Ingest live AQI data from WAQI for each city
  2. Validate raw data quality
  3. Run dbt (staging → intermediate → marts)
  4. Run dbt tests

Usage:
    source .venv/bin/activate
    python run_pipeline.py
"""

import os
import sys
import time
import subprocess
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

PROJECT_DIR = Path(__file__).parent
DBT_DIR = PROJECT_DIR / "dbt"
sys.path.insert(0, str(PROJECT_DIR))

CITIES = [c.strip() for c in os.environ.get("WAQI_CITIES", "Delhi,London,Beijing,Seoul,Jakarta,Mumbai,Paris,Tokyo,New York,Chicago").split(",")]


# ── Step 1: Ingest ────────────────────────────────────────────────────────────

def run_ingestion():
    from ingestion.waqi_client import WAQIClient
    from ingestion.db_loader import load_readings, load_forecasts

    # ── Source 1: WAQI ───────────────────────────────────────────
    waqi = WAQIClient()
    waqi_total = 0

    for city in CITIES:
        logger.info(f"[WAQI] {city}")
        try:
            rows = waqi.fetch_city_readings(city)
            waqi_total += load_readings(rows)
        except Exception as e:
            logger.error(f"  WAQI {city} readings — {e}")

        try:
            load_forecasts(waqi.fetch_city_forecast(city))
        except Exception as e:
            logger.warning(f"  WAQI {city} forecast — {e}")

        time.sleep(0.5)

    logger.success(f"WAQI done: {waqi_total} rows")
    return waqi_total


# ── Step 2: Validate ─────────────────────────────────────────────────────────

def run_validation():
    from ingestion.db_loader import get_connection

    logger.info("Running data quality checks...")
    conn = get_connection()
    failed = []

    checks = {
        "no_null_aqi":          "SELECT COUNT(*) FROM raw.waqi_readings WHERE aqi IS NULL",
        "no_future_timestamps": "SELECT COUNT(*) FROM raw.waqi_readings WHERE reading_time > NOW() + INTERVAL '1 hour'",
        "recent_data_exists":   "SELECT COUNT(*) FROM raw.waqi_readings WHERE inserted_at > NOW() - INTERVAL '2 hours'",
    }

    try:
        with conn.cursor() as cur:
            for name, sql in checks.items():
                cur.execute(sql)
                count = cur.fetchone()[0]
                if name.startswith("no_") and count > 0:
                    failed.append(f"{name}: {count} violations")
                elif "exists" in name and count == 0:
                    failed.append(f"{name}: no recent data")
                else:
                    logger.success(f"  {name}: OK")
    finally:
        conn.close()

    if failed:
        for f in failed:
            logger.warning(f"  WARN — {f}")
        logger.warning("Continuing to dbt anyway so tables are created.")


# ── Step 3 & 4: dbt ──────────────────────────────────────────────────────────

def run_dbt(command: list[str]):
    result = subprocess.run(
        ["dbt"] + command + ["--profiles-dir", str(DBT_DIR)],
        cwd=str(DBT_DIR),
    )
    if result.returncode != 0:
        raise SystemExit(f"dbt {' '.join(command)} failed.")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("AirPulse Pipeline — starting")
    logger.info("=" * 50)

    logger.info("Step 1/4 — Ingestion (WAQI)")
    run_ingestion()

    logger.info("Step 2/4 — Validation")
    run_validation()

    logger.info("Step 3/4 — dbt transformations")
    run_dbt(["run"])

    logger.info("Step 4/4 — dbt tests")
    run_dbt(["test"])

    logger.success("Done! Run: uvicorn api.main:app --reload --port 8000  →  open http://localhost:8000")
