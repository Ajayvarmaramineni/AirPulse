"""
Database Loader
---------------
Writes WAQI station snapshots to PostgreSQL raw.waqi_readings.
Idempotent — ON CONFLICT DO NOTHING means re-runs are safe.
"""

import os
from typing import List

import psycopg2
import psycopg2.extras
from loguru import logger


def get_connection():
    pwd = os.environ.get("POSTGRES_PASSWORD", "")
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "airquality"),
        user=os.environ.get("POSTGRES_USER", ""),
        password=pwd if pwd else None,
    )


INSERT_SQL = """
    INSERT INTO raw.waqi_readings (
        city, station_name, station_id, aqi, dominant_pollutant,
        pm25, pm10, no2, o3, co, so2, temperature, humidity,
        latitude, longitude, reading_time
    ) VALUES (
        %(city)s, %(station_name)s, %(station_id)s, %(aqi)s, %(dominant_pollutant)s,
        %(pm25)s, %(pm10)s, %(no2)s, %(o3)s, %(co)s, %(so2)s, %(temperature)s, %(humidity)s,
        %(latitude)s, %(longitude)s, %(reading_time)s
    )
    ON CONFLICT (station_id, reading_time) DO NOTHING;
"""


def load_readings(rows: List[dict]) -> int:
    """Bulk-insert WAQI rows. Returns count of rows attempted."""
    if not rows:
        logger.warning("load_readings called with empty list — nothing to insert.")
        return 0

    valid = [r for r in rows if r.get("station_id") and r.get("reading_time")]
    skipped = len(rows) - len(valid)
    if skipped:
        logger.warning(f"Skipped {skipped} rows missing station_id or reading_time")

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, INSERT_SQL, valid, page_size=200)
        logger.info(f"Loaded {len(valid)} rows (conflicts silently skipped)")
        return len(valid)
    except Exception as e:
        logger.error(f"DB load failed: {e}")
        raise
    finally:
        conn.close()


FORECAST_SQL = """
    INSERT INTO raw.waqi_forecasts (
        city, forecast_day, pollutant, avg, min, max, latitude, longitude
    ) VALUES (
        %(city)s, %(forecast_day)s, %(pollutant)s, %(avg)s, %(min)s, %(max)s,
        %(latitude)s, %(longitude)s
    )
    ON CONFLICT (city, forecast_day, pollutant)
    DO UPDATE SET avg = EXCLUDED.avg, min = EXCLUDED.min, max = EXCLUDED.max,
                  fetched_at = NOW();
"""


def load_forecasts(rows: list[dict]) -> int:
    """Upsert daily forecast rows into raw.waqi_forecasts."""
    if not rows:
        return 0
    valid = [r for r in rows if r.get("city") and r.get("forecast_day") and r.get("pollutant")]
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, FORECAST_SQL, valid, page_size=100)
        logger.info(f"Upserted {len(valid)} forecast rows")
        return len(valid)
    except Exception as e:
        logger.error(f"Forecast load failed: {e}")
        raise
    finally:
        conn.close()


OPENAQ_INSERT_SQL = """
    INSERT INTO raw.openaq_readings (
        city, station_name, location_id,
        pm25, pm10, no2, o3, co, so2,
        aqi_computed, latitude, longitude, country_code, reading_time
    ) VALUES (
        %(city)s, %(station_name)s, %(location_id)s,
        %(pm25)s, %(pm10)s, %(no2)s, %(o3)s, %(co)s, %(so2)s,
        %(aqi_computed)s, %(latitude)s, %(longitude)s, %(country_code)s, %(reading_time)s
    )
    ON CONFLICT (location_id, reading_time) DO NOTHING;
"""


def load_openaq_readings(rows: list[dict]) -> int:
    """Insert OpenAQ v3 station readings. Returns rows attempted."""
    if not rows:
        return 0
    valid = [r for r in rows if r.get("location_id") and r.get("reading_time")]
    if not valid:
        return 0
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, OPENAQ_INSERT_SQL, valid, page_size=200)
        logger.info(f"OpenAQ: loaded {len(valid)} rows")
        return len(valid)
    except Exception as e:
        logger.error(f"OpenAQ DB load failed: {e}")
        raise
    finally:
        conn.close()


def get_row_count(city: str | None = None) -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if city:
                cur.execute("SELECT COUNT(*) FROM raw.waqi_readings WHERE city = %s", (city,))
            else:
                cur.execute("SELECT COUNT(*) FROM raw.waqi_readings")
            return cur.fetchone()[0]
    finally:
        conn.close()
