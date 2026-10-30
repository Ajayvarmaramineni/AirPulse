"""
Export a small JSON snapshot of real pipeline output for the static landing page.

The GitHub Pages site (docs/index.html) has no backend, so instead of calling the
FastAPI service it renders docs/data/snapshot.json, produced by this script from
whatever is currently in the local PostgreSQL marts.

Usage:
    python scripts/export_snapshot.py                    # writes docs/data/snapshot.json
    python scripts/export_snapshot.py --city Delhi       # pick the featured city

Rankings only include cities whose latest reading is at most MAX_AGE_DAYS old.
Some WAQI stations stop reporting for months, and ranking those as "current" would
be misleading; each ranked row carries its own reading_date so nothing is hidden.
"""

import argparse
import datetime as dt
import decimal
import json
import sys
from pathlib import Path

import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from ingestion.db_loader import get_connection  # noqa: E402

MAX_AGE_DAYS = 3
OUT_PATH = ROOT / "docs" / "data" / "snapshot.json"


def _json_default(value):
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dt.datetime | dt.date):
        return value.isoformat()
    raise TypeError(f"not serialisable: {type(value)}")


def _rows(cur, sql, params=None):
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def build_snapshot(city: str | None) -> dict:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            today = _rows(cur, "SELECT CURRENT_DATE AS d")[0]["d"]

            totals = _rows(cur, """
                SELECT
                  (SELECT COUNT(*) FROM raw.waqi_readings)                       AS raw_station_readings,
                  (SELECT COUNT(DISTINCT station_id) FROM raw.waqi_readings)     AS stations,
                  (SELECT COUNT(DISTINCT city) FROM raw.waqi_readings)           AS cities_ingested,
                  (SELECT COUNT(*) FROM raw.waqi_forecasts)                      AS forecast_rows,
                  (SELECT COUNT(*) FROM public_marts.fct_aqi_health_risk)        AS city_days,
                  (SELECT COUNT(*) FROM public_marts.fct_anomaly_events)         AS anomaly_events_all_time
            """)[0]

            latest = _rows(cur, """
                SELECT DISTINCT ON (city)
                    city, reading_date, avg_aqi, aqi_category, dominant_pollutant,
                    avg_pm25, avg_pm10, avg_no2, avg_o3, station_count, risk_score
                FROM public_marts.fct_aqi_health_risk
                WHERE avg_aqi IS NOT NULL
                ORDER BY city, reading_date DESC
            """)
            fresh = [r for r in latest if (today - r["reading_date"]).days <= MAX_AGE_DAYS]
            fresh.sort(key=lambda r: float(r["avg_aqi"]), reverse=True)

            anomalies = _rows(cur, """
                SELECT city, reading_date, anomaly_aqi, baseline_mean_aqi, baseline_stddev,
                       z_score, pct_above_baseline, severity, dominant_pollutant
                FROM public_marts.fct_anomaly_events
                WHERE reading_date >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY z_score DESC
                LIMIT 20
            """)

            if city is None:
                best = _rows(cur, """
                    SELECT city, COUNT(*) AS days, MAX(station_count) AS stations
                    FROM public_marts.fct_aqi_health_risk
                    WHERE reading_date >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY city ORDER BY days DESC, stations DESC LIMIT 1
                """)
                city = best[0]["city"] if best else (fresh[0]["city"] if fresh else None)

            featured = None
            if city:
                trend = _rows(cur, """
                    SELECT reading_date, avg_aqi, rolling_7d_avg_aqi, aqi_category
                    FROM public_marts.fct_aqi_health_risk
                    WHERE LOWER(city) = LOWER(%s) AND reading_date >= CURRENT_DATE - INTERVAL '30 days'
                    ORDER BY reading_date
                """, (city,))
                featured = {
                    "city": city,
                    "latest": next((r for r in latest if r["city"].lower() == city.lower()), None),
                    "trend_30d": trend,
                }
    finally:
        conn.close()

    slim = ("city", "reading_date", "avg_aqi", "aqi_category", "dominant_pollutant")
    return {
        "generated_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
        "note": (
            "Real output of the AirPulse pipeline (WAQI -> PostgreSQL -> dbt marts), exported by "
            f"scripts/export_snapshot.py. Rankings only include cities whose latest reading is at "
            f"most {MAX_AGE_DAYS} days old."
        ),
        "totals": totals,
        "cities_in_rankings": len(fresh),
        "most_polluted": [{k: r[k] for k in slim} for r in fresh[:10]],
        "cleanest": [{k: r[k] for k in slim} for r in reversed(fresh[-5:])],
        "anomalies_last_30d": anomalies,
        "featured_city": featured,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--city", help="city to feature (default: most history in the last 30 days)")
    parser.add_argument("--out", type=Path, default=OUT_PATH)
    args = parser.parse_args()

    snapshot = build_snapshot(args.city)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(snapshot, indent=2, default=_json_default) + "\n")
    print(f"wrote {args.out} ({args.out.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
