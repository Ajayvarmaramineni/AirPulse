"""
AirPulse FastAPI Backend
-------------------------
Serves air quality data from PostgreSQL to the HTML/JS frontend.

Run: uvicorn api.main:app --reload --port 8000
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import psycopg2
import psycopg2.extras

load_dotenv(Path(__file__).parents[1] / ".env")

app = FastAPI(title="AirPulse API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Serve the dashboard folder as static files
DASHBOARD_DIR = Path(__file__).parents[1] / "dashboard"
app.mount("/static", StaticFiles(directory=str(DASHBOARD_DIR)), name="static")


# ── DB helper ─────────────────────────────────────────────────────────────────

def get_conn():
    pwd = os.environ.get("POSTGRES_PASSWORD", "")
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "airquality"),
        user=os.environ.get("POSTGRES_USER", ""),
        password=pwd or None,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def query(sql: str, params=None) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def serve_dashboard():
    return FileResponse(str(DASHBOARD_DIR / "index.html"))


@app.get("/api/cities")
def cities():
    """Latest AQI snapshot for all monitored cities — used to populate the map and city cards."""
    rows = query("""
        SELECT DISTINCT ON (city)
            city,
            reading_date,
            avg_aqi,
            aqi_category,
            risk_score,
            dominant_pollutant,
            avg_pm25,
            avg_pm10,
            avg_no2,
            avg_o3,
            rolling_7d_avg_aqi,
            city_lat  AS latitude,
            city_lon  AS longitude,
            station_count,
            day_over_day_change,
            is_above_7d_baseline
        FROM public_marts.fct_aqi_health_risk
        ORDER BY city, reading_date DESC
    """)
    return rows


@app.get("/api/city/{city_name}")
def city_detail(city_name: str):
    """30-day history for one city."""
    rows = query("""
        SELECT
            city, reading_date, avg_aqi, max_aqi, min_aqi,
            aqi_category, risk_score, dominant_pollutant,
            avg_pm25, avg_pm10, avg_no2, avg_o3,
            rolling_7d_avg_aqi, day_over_day_change,
            city_lat AS latitude, city_lon AS longitude
        FROM public_marts.fct_aqi_health_risk
        WHERE LOWER(city) = LOWER(%s)
          AND reading_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY reading_date ASC
    """, (city_name,))
    if not rows:
        raise HTTPException(404, f"No data for city: {city_name}")
    return rows


@app.get("/api/forecast/{city_name}")
def city_forecast(city_name: str):
    """Daily pm25/pm10/o3 forecast for one city (from WAQI)."""
    rows = query("""
        SELECT city, forecast_day, pollutant, avg, min, max
        FROM raw.waqi_forecasts
        WHERE LOWER(city) = LOWER(%s)
          AND forecast_day >= CURRENT_DATE
        ORDER BY forecast_day ASC, pollutant
    """, (city_name,))
    return rows


@app.get("/api/anomalies")
def anomalies():
    """Recent anomaly events (last 30 days)."""
    rows = query("""
        SELECT city, reading_date, anomaly_aqi, baseline_mean_aqi,
               pct_above_baseline, severity, dominant_pollutant,
               city_lat AS latitude, city_lon AS longitude
        FROM public_marts.fct_anomaly_events
        WHERE reading_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY reading_date DESC
        LIMIT 50
    """)
    return rows


@app.get("/api/feed/{city_name}")
def city_feed(city_name: str):
    """
    Returns city data shaped like the WAQI /feed/ response so the
    uploaded HTML frontend can consume it without changes to its
    rendering functions.
    """
    # ── Latest mart snapshot ──────────────────────────────────────
    mart = query("""
        SELECT DISTINCT ON (city)
            city, reading_date, avg_aqi, dominant_pollutant,
            avg_pm25, avg_pm10, avg_no2, avg_o3,
            city_lat, city_lon, station_count
        FROM public_marts.fct_aqi_health_risk
        WHERE LOWER(city) = LOWER(%s)
        ORDER BY city, reading_date DESC
    """, (city_name,))

    if not mart:
        raise HTTPException(404, f"No data for '{city_name}'. "
                            f"Make sure it's in WAQI_CITIES and the pipeline has run.")

    r = mart[0]

    # ── Weather from raw readings (last 6 h) ─────────────────────
    wx = query("""
        SELECT
            AVG(temperature) AS t,
            AVG(humidity)    AS h
        FROM raw.waqi_readings
        WHERE LOWER(city) = LOWER(%s)
          AND reading_time > NOW() - INTERVAL '48 hours'
    """, (city_name,))
    w = wx[0] if wx else {}

    # ── Forecast (pm25 only, next 5 days) ────────────────────────
    fc_rows = query("""
        SELECT forecast_day, avg, min, max
        FROM raw.waqi_forecasts
        WHERE LOWER(city) = LOWER(%s)
          AND pollutant = 'pm25'
          AND forecast_day >= CURRENT_DATE
        ORDER BY forecast_day ASC
        LIMIT 5
    """, (city_name,))

    import datetime as _dt
    def day_label(d):
        if isinstance(d, (_dt.date, _dt.datetime)):
            return d.strftime("%a")
        return str(d)

    pm25_fc = [
        {"day": day_label(f["forecast_day"]),
         "avg": round(float(f["avg"]), 1) if f["avg"] else 0,
         "min": round(float(f["min"]), 1) if f["min"] else 0,
         "max": round(float(f["max"]), 1) if f["max"] else 0}
        for f in fc_rows
    ]

    def iv(val):
        """Wrap a value in WAQI iaqi format {"v": ...}"""
        return {"v": round(float(val), 2)} if val is not None else None

    iaqi = {
        "pm25": iv(r["avg_pm25"]),
        "pm10": iv(r["avg_pm10"]),
        "no2":  iv(r["avg_no2"]),
        "o3":   iv(r["avg_o3"]),
        "t":    iv(w.get("t")),
        "h":    iv(w.get("h")),
    }
    # Drop nulls so frontend skips missing sensors
    iaqi = {k: v for k, v in iaqi.items() if v is not None}

    lat = float(r["city_lat"]) if r["city_lat"] else 0
    lon = float(r["city_lon"]) if r["city_lon"] else 0

    return {
        "aqi":         int(r["avg_aqi"]) if r["avg_aqi"] else 0,
        "dominentpol": (r["dominant_pollutant"] or "pm25").lower(),
        "city": {
            "name": r["city"],
            "geo":  [lat, lon],
            "url":  f"https://aqicn.org/city/{r['city'].lower().replace(' ', '-')}/",
        },
        "iaqi": iaqi,
        "attributions": [{"name": "World Air Quality Index (WAQI)"}],
        "time": {
            "s":   str(r["reading_date"]),
            "iso": str(r["reading_date"]) + "T00:00:00Z",
        },
        "forecast": {"daily": {"pm25": pm25_fc}},
    }


@app.get("/api/stations/{city_name}")
def city_stations(city_name: str):
    """
    Returns per-station readings for a city in the shape the frontend's
    renderStations() / renderMap() expect.
    """
    rows = query("""
        SELECT DISTINCT ON (station_id)
            station_id, station_name, aqi,
            latitude, longitude, reading_time
        FROM raw.waqi_readings
        WHERE LOWER(city) = LOWER(%s)
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND reading_time > NOW() - INTERVAL '48 hours'
        ORDER BY station_id, reading_time DESC
        LIMIT 15
    """, (city_name,))

    import datetime as _dt
    def rel_time(ts):
        if not ts:
            return "Unknown"
        if isinstance(ts, _dt.datetime):
            delta = _dt.datetime.utcnow().replace(tzinfo=None) - ts.replace(tzinfo=None)
            mins = int(delta.total_seconds() / 60)
            if mins < 2:
                return "Just now"
            if mins < 60:
                return f"{mins} min ago"
            return f"{mins // 60}h ago"
        return str(ts)

    return [
        {
            "uid": r["station_id"],
            "aqi": r["aqi"] if r["aqi"] is not None else "-",
            "lat": float(r["latitude"]),
            "lon": float(r["longitude"]),
            "station": {
                "name": r["station_name"] or f"Station {r['station_id']}",
                "time": rel_time(r["reading_time"]),
            },
        }
        for r in rows
    ]


@app.get("/api/leaderboard")
def leaderboard():
    """Top 10 most polluted + 5 cleanest cities, latest reading per city."""
    rows = query("""
        SELECT DISTINCT ON (city)
            city, avg_aqi, aqi_category, dominant_pollutant, reading_date,
            day_over_day_change
        FROM public_marts.fct_aqi_health_risk
        WHERE avg_aqi IS NOT NULL
        ORDER BY city, reading_date DESC
    """)
    rows.sort(key=lambda r: float(r["avg_aqi"] or 0), reverse=True)
    return {
        "worst": rows[:10],
        "best":  list(reversed(rows[-5:])) if len(rows) >= 5 else list(reversed(rows)),
    }


@app.get("/api/anomaly/{city_name}")
def city_anomaly(city_name: str):
    """Most recent anomaly event for a city in the last 3 days (empty dict if none)."""
    rows = query("""
        SELECT city, reading_date, anomaly_aqi, baseline_mean_aqi,
               pct_above_baseline, severity, dominant_pollutant
        FROM public_marts.fct_anomaly_events
        WHERE LOWER(city) = LOWER(%s)
          AND reading_date >= CURRENT_DATE - INTERVAL '3 days'
        ORDER BY reading_date DESC
        LIMIT 1
    """, (city_name,))
    return rows[0] if rows else {}


@app.get("/api/health")
def health():
    try:
        query("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, str(e))
