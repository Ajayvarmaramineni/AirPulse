"""
Integration tests for the loader against a real PostgreSQL.

These prove the idempotency claims (re-running ingestion never duplicates rows,
forecasts are true upserts). They only run when AIRPULSE_TEST_DB=1, which CI sets
against a throwaway Postgres service container. To run locally, point POSTGRES_*
at a scratch database initialised with scripts/init_db.sql, not your real data:

    AIRPULSE_TEST_DB=1 POSTGRES_DB=airpulse_scratch pytest tests/test_db_integration.py
"""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("AIRPULSE_TEST_DB") != "1",
    reason="needs a scratch PostgreSQL: set AIRPULSE_TEST_DB=1 and POSTGRES_*",
)

CITY = "Integration Test City"


def _reading(station_id: int, reading_time: str, aqi: int = 80) -> dict:
    return {
        "city": CITY, "station_name": "Test Station", "station_id": station_id,
        "aqi": aqi, "dominant_pollutant": "pm25",
        "pm25": 25.0, "pm10": 40.0, "no2": 10.0, "o3": 20.0, "co": None, "so2": None,
        "temperature": 20.0, "humidity": 50.0, "latitude": 1.0, "longitude": 2.0,
        "reading_time": reading_time,
    }


@pytest.fixture
def conn():
    from ingestion.db_loader import get_connection

    connection = get_connection()

    def cleanup():
        with connection, connection.cursor() as cur:
            cur.execute("DELETE FROM raw.waqi_readings WHERE city = %s", (CITY,))
            cur.execute("DELETE FROM raw.waqi_forecasts WHERE city = %s", (CITY,))

    cleanup()
    yield connection
    cleanup()
    connection.close()


def _count(conn, table: str) -> int:
    with conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table} WHERE city = %s", (CITY,))
        return cur.fetchone()[0]


def test_reloading_the_same_readings_creates_no_duplicates(conn):
    from ingestion.db_loader import load_readings

    rows = [
        _reading(990001, "2026-01-01T00:00:00+00:00"),
        _reading(990001, "2026-01-01T01:00:00+00:00"),
        _reading(990002, "2026-01-01T00:00:00+00:00"),
    ]
    load_readings(rows)
    assert _count(conn, "raw.waqi_readings") == 3

    load_readings(rows)  # a full pipeline re-run
    assert _count(conn, "raw.waqi_readings") == 3


def test_conflicting_reading_keeps_the_original_row(conn):
    from ingestion.db_loader import load_readings

    load_readings([_reading(990001, "2026-01-01T00:00:00+00:00", aqi=80)])
    load_readings([_reading(990001, "2026-01-01T00:00:00+00:00", aqi=999)])

    with conn, conn.cursor() as cur:
        cur.execute("SELECT aqi FROM raw.waqi_readings WHERE city = %s", (CITY,))
        assert [r[0] for r in cur.fetchall()] == [80]  # ON CONFLICT DO NOTHING


def test_forecasts_are_upserted_not_duplicated(conn):
    from ingestion.db_loader import load_forecasts

    def forecast(avg):
        return {
            "city": CITY, "forecast_day": "2026-01-02", "pollutant": "pm25",
            "avg": avg, "min": 1.0, "max": 9.0, "latitude": 1.0, "longitude": 2.0,
        }

    load_forecasts([forecast(10.0)])
    load_forecasts([forecast(42.0)])  # newer fetch for the same city/day/pollutant

    assert _count(conn, "raw.waqi_forecasts") == 1
    with conn, conn.cursor() as cur:
        cur.execute("SELECT avg FROM raw.waqi_forecasts WHERE city = %s", (CITY,))
        assert float(cur.fetchone()[0]) == 42.0  # ON CONFLICT DO UPDATE
