"""
OpenAQ v3 API Client
--------------------
Fetches real-time air quality data from OpenAQ v3.
Covers 170+ countries, 10,000+ monitoring locations globally.
API docs: https://docs.openaq.org

Key differences from WAQI:
  - Returns raw pollutant concentrations (µg/m³), not AQI directly
  - AQI is computed here from PM2.5 using the US EPA formula
  - Auth via X-API-Key header (not query param)
  - City lookup uses city + country_id filter (more reliable than coordinates)
"""

import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://api.openaq.org/v3"

# Cities focused on gaps in WAQI coverage
# (Africa, Middle East, South/SE Asia underserved by WAQI free tier)
OPENAQ_CITIES = [
    # ── Africa ────────────────────────────────────────────────────
    {"city": "Cairo",            "country": "EG"},
    {"city": "Lagos",            "country": "NG"},
    {"city": "Nairobi",          "country": "KE"},
    {"city": "Johannesburg",     "country": "ZA"},
    {"city": "Cape Town",        "country": "ZA"},
    {"city": "Accra",            "country": "GH"},
    {"city": "Kampala",          "country": "UG"},
    {"city": "Dar es Salaam",    "country": "TZ"},
    {"city": "Casablanca",       "country": "MA"},
    {"city": "Tunis",            "country": "TN"},
    # ── Middle East ───────────────────────────────────────────────
    {"city": "Dubai",            "country": "AE"},
    {"city": "Abu Dhabi",        "country": "AE"},
    {"city": "Riyadh",           "country": "SA"},
    {"city": "Amman",            "country": "JO"},
    {"city": "Kuwait City",      "country": "KW"},
    {"city": "Muscat",           "country": "OM"},
    {"city": "Doha",             "country": "QA"},
    # ── South / Southeast Asia ────────────────────────────────────
    {"city": "Dhaka",            "country": "BD"},
    {"city": "Karachi",          "country": "PK"},
    {"city": "Lahore",           "country": "PK"},
    {"city": "Islamabad",        "country": "PK"},
    {"city": "Colombo",          "country": "LK"},
    {"city": "Kathmandu",        "country": "NP"},
    {"city": "Manila",           "country": "PH"},
    {"city": "Ho Chi Minh City", "country": "VN"},
    {"city": "Hanoi",            "country": "VN"},
    {"city": "Phnom Penh",       "country": "KH"},
    {"city": "Yangon",           "country": "MM"},
    # ── Europe (supplement WAQI gaps) ────────────────────────────
    {"city": "Kyiv",             "country": "UA"},
    {"city": "Krakow",           "country": "PL"},
    {"city": "Prague",           "country": "CZ"},
    {"city": "Lisbon",           "country": "PT"},
    {"city": "Helsinki",         "country": "FI"},
    {"city": "Oslo",             "country": "NO"},
    {"city": "Copenhagen",       "country": "DK"},
    # ── Americas (supplement WAQI gaps) ──────────────────────────
    {"city": "Lima",             "country": "PE"},
    {"city": "Buenos Aires",     "country": "AR"},
    {"city": "Rio de Janeiro",   "country": "BR"},
    {"city": "Montevideo",       "country": "UY"},
    {"city": "Quito",            "country": "EC"},
    {"city": "La Paz",           "country": "BO"},
]


# ── US EPA AQI formula ────────────────────────────────────────────────────────

_EPA_BREAKPOINTS = [
    (0.0,    12.0,    0,   50),
    (12.1,   35.4,   51,  100),
    (35.5,   55.4,  101,  150),
    (55.5,  150.4,  151,  200),
    (150.5, 250.4,  201,  300),
    (250.5, 350.4,  301,  400),
    (350.5, 500.4,  401,  500),
]

def pm25_to_aqi(pm25: float) -> Optional[int]:
    """Convert PM2.5 concentration (µg/m³) → US EPA AQI (0–500)."""
    if pm25 is None or pm25 < 0:
        return None
    for c_lo, c_hi, aqi_lo, aqi_hi in _EPA_BREAKPOINTS:
        if c_lo <= round(pm25, 1) <= c_hi:
            return round(((aqi_hi - aqi_lo) / (c_hi - c_lo)) * (pm25 - c_lo) + aqi_lo)
    return 500  # above 500.4 µg/m³


# ── Client ────────────────────────────────────────────────────────────────────

class OpenAQClient:
    """Wrapper around the OpenAQ v3 REST API."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ["OPENAQ_API_KEY"]
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": self.api_key,
            "Accept":    "application/json",
        })

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    def _get(self, endpoint: str, params: dict = {}) -> dict:
        url = f"{BASE_URL}/{endpoint}"
        resp = self.session.get(url, params=params, timeout=20)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            logger.warning(f"Rate limited — sleeping {wait}s")
            time.sleep(wait)
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()

    def get_locations(self, city: str, country: str, limit: int = 50) -> list[dict]:
        """
        GET /v3/locations — monitoring stations in a city.
        Ordered by most recently updated first so stale stations are skipped naturally.
        """
        try:
            data = self._get("locations", {
                "city":       city,
                "country_id": country,
                "limit":      limit,
            })
            return data.get("results", [])
        except Exception as e:
            logger.error(f"OpenAQ locations({city}, {country}): {e}")
            return []

    def get_latest(self, location_id: int) -> list[dict]:
        """
        GET /v3/locations/{id}/latest — one latest measurement per sensor.
        """
        try:
            data = self._get(f"locations/{location_id}/latest")
            return data.get("results", [])
        except Exception as e:
            logger.warning(f"OpenAQ latest(loc={location_id}): {e}")
            return []

    def fetch_city_readings(self, city: str, country: str) -> list[dict]:
        """
        Full snapshot for one city: discover stations → fetch latest measurements.
        Returns list of dicts ready for raw.openaq_readings insertion.
        """
        locations = self.get_locations(city, country)
        rows = []
        now_utc = datetime.now(tz=timezone.utc).isoformat()

        for loc in locations:
            loc_id = loc.get("id")
            name   = loc.get("name", "")
            coords = loc.get("coordinates") or {}
            lat    = coords.get("latitude")
            lon    = coords.get("longitude")

            latest = self.get_latest(loc_id)
            time.sleep(0.3)

            if not latest:
                continue

            pollutants   = {}
            reading_time = now_utc

            for m in latest:
                param = ((m.get("parameter") or {}).get("name") or "").lower()
                value = m.get("value")
                dt    = m.get("datetime") or {}
                if isinstance(dt, dict) and dt.get("utc"):
                    reading_time = dt["utc"]
                elif isinstance(dt, str):
                    reading_time = dt
                if param and value is not None:
                    pollutants[param] = float(value)

            pm25 = pollutants.get("pm25")
            aqi  = pm25_to_aqi(pm25) if pm25 is not None else None

            # Skip station if it has no useful pollutant data at all
            if not any(pollutants.get(k) is not None for k in ("pm25", "pm10", "no2", "o3")):
                continue

            rows.append({
                "city":         city,
                "station_name": name,
                "location_id":  loc_id,
                "pm25":         pm25,
                "pm10":         pollutants.get("pm10"),
                "no2":          pollutants.get("no2"),
                "o3":           pollutants.get("o3"),
                "co":           pollutants.get("co"),
                "so2":          pollutants.get("so2"),
                "aqi_computed": aqi,
                "latitude":     lat,
                "longitude":    lon,
                "country_code": country,
                "reading_time": reading_time,
            })

        logger.info(f"OpenAQ {city} ({country}): {len(rows)} station readings fetched")
        return rows
