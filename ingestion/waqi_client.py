"""
World Air Quality Index (WAQI) API Client
------------------------------------------
Fetches real-time AQI + pollutant data for global cities.

API docs: https://aqicn.org/api/
Free token: https://aqicn.org/data-platform/token

Endpoints used:
  GET /feed/{city}/?token=...          → main station for a city
  GET /map/bounds/?latlng=...&token=.. → all stations in a bounding box
"""

import os
import time
from datetime import datetime, timezone

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://api.waqi.info"

# Cities + bounding boxes [lat_min, lon_min, lat_max, lon_max]
# Wide enough to capture multiple monitoring stations per city
CITY_BOUNDS = {
    # ── Asia ─────────────────────────────────────────────────────────────────
    "Delhi":         [28.40,  76.80,  28.90,  77.40],
    "Mumbai":        [18.85,  72.75,  19.25,  73.05],
    "Bangalore":     [12.85,  77.45,  13.10,  77.75],
    "Kolkata":       [22.45,  88.25,  22.70,  88.50],
    "Chennai":       [12.90,  80.10,  13.15,  80.35],
    "Beijing":       [39.75, 116.10,  40.15, 116.70],
    "Shanghai":      [31.00, 121.25,  31.40, 121.70],
    "Chengdu":       [30.50, 103.90,  30.80, 104.25],
    "Guangzhou":     [23.00, 113.10,  23.30, 113.45],
    "Tokyo":         [35.55, 139.55,  35.80, 139.90],
    "Osaka":         [34.55, 135.40,  34.75, 135.60],
    "Seoul":         [37.40, 126.80,  37.70, 127.20],
    "Bangkok":       [13.60,  100.35, 13.95, 100.70],
    "Jakarta":       [-6.40, 106.65,  -6.05, 107.00],
    "Singapore":     [ 1.21, 103.65,   1.45, 103.95],
    "Kuala Lumpur":  [ 3.00, 101.55,   3.25, 101.80],
    "Manila":        [14.45, 120.95,  14.75, 121.20],
    "Ho Chi Minh":   [10.65, 106.55,  10.90, 106.80],
    "Taipei":        [24.95, 121.45,  25.15, 121.65],
    "Hong Kong":     [22.20, 113.85,  22.45, 114.25],
    "Dhaka":         [23.65,  90.30,  23.90,  90.55],
    "Karachi":       [24.80,  66.90,  25.05,  67.15],
    "Lahore":        [31.40,  74.20,  31.65,  74.45],
    "Kathmandu":     [27.65,  85.25,  27.80,  85.40],
    "Colombo":       [ 6.80,  79.80,   6.95,  80.00],
    # ── Middle East ──────────────────────────────────────────────────────────
    "Dubai":         [25.05,  55.10,  25.35,  55.45],
    "Riyadh":        [24.55,  46.55,  24.80,  46.85],
    "Tehran":        [35.55,  51.20,  35.85,  51.60],
    "Baghdad":       [33.25,  44.25,  33.45,  44.50],
    # ── Europe ───────────────────────────────────────────────────────────────
    "London":        [51.30,  -0.50,  51.70,   0.30],
    "Paris":         [48.75,   2.20,  48.95,   2.55],
    "Berlin":        [52.40,  13.25,  52.60,  13.55],
    "Madrid":        [40.30,  -3.80,  40.55,  -3.55],
    "Rome":          [41.80,  12.35,  42.00,  12.60],
    "Amsterdam":     [52.30,   4.80,  52.45,   5.05],
    "Brussels":      [50.80,   4.25,  50.95,   4.45],
    "Vienna":        [48.10,  16.25,  48.30,  16.50],
    "Warsaw":        [52.10,  20.85,  52.35,  21.15],
    "Stockholm":     [59.25,  17.85,  59.45,  18.15],
    "Moscow":        [55.55,  37.35,  55.85,  37.75],
    "Istanbul":      [40.90,  28.80,  41.20,  29.20],
    "Athens":        [37.90,  23.60,  38.10,  23.85],
    "Zurich":        [47.30,   8.45,  47.45,   8.65],
    "Prague":        [50.00,  14.30,  50.15,  14.55],
    "Bucharest":     [44.35,  26.00,  44.55,  26.20],
    "Kyiv":          [50.30,  30.40,  50.55,  30.70],
    "Krakow":        [50.00,  19.85,  50.10,  20.05],
    # ── Africa ───────────────────────────────────────────────────────────────
    "Cairo":         [29.95,  31.10,  30.20,  31.40],
    "Lagos":         [ 6.40,   3.30,   6.65,   3.55],
    "Nairobi":       [-1.40,  36.70,  -1.20,  36.90],
    "Johannesburg":  [-26.25,  27.95, -26.05,  28.15],
    "Casablanca":    [33.50,  -7.75,  33.70,  -7.55],
    "Addis Ababa":   [ 8.90,  38.65,   9.10,  38.85],
    "Accra":         [ 5.50,  -0.30,   5.65,  -0.15],
    "Dakar":         [14.65, -17.50,  14.80, -17.35],
    # ── North America ────────────────────────────────────────────────────────
    "New York":      [40.55,  -74.10,  40.90, -73.70],
    "Chicago":       [41.70,  -87.80,  42.05, -87.50],
    "Los Angeles":   [33.90, -118.50,  34.20,-118.10],
    "Houston":       [29.65,  -95.60,  29.90, -95.25],
    "Phoenix":       [33.35, -112.20,  33.60,-111.90],
    "Philadelphia":  [39.90,  -75.25,  40.10, -75.05],
    "San Antonio":   [29.35,  -98.65,  29.55, -98.40],
    "San Diego":     [32.65, -117.25,  32.85,-117.05],
    "Dallas":        [32.65,  -97.00,  32.90, -96.70],
    "San Francisco": [37.65, -122.55,  37.85,-122.35],
    "Seattle":       [47.45, -122.45,  47.75,-122.20],
    "Denver":        [39.60, -105.10,  39.85,-104.85],
    "Atlanta":       [33.65,  -84.55,  33.85, -84.30],
    "Boston":        [42.25,  -71.15,  42.45, -70.95],
    "Miami":         [25.65,  -80.40,  25.90, -80.15],
    "Toronto":       [43.55,  -79.55,  43.80, -79.30],
    "Vancouver":     [49.15, -123.25,  49.35,-122.90],
    "Montreal":      [45.45,  -73.65,  45.65, -73.45],
    "Mexico City":   [19.25,  -99.25,  19.55, -99.00],
    "Guadalajara":   [20.60, -103.45,  20.75,-103.25],
    # ── South America ────────────────────────────────────────────────────────
    "Sao Paulo":     [-23.70,  -46.85, -23.45, -46.55],
    "Rio de Janeiro":[-22.95,  -43.40, -22.75, -43.10],
    "Buenos Aires":  [-34.70,  -58.55, -34.50, -58.30],
    "Santiago":      [-33.60,  -70.80, -33.40, -70.55],
    "Bogota":        [  4.50,  -74.20,   4.75, -73.95],
    "Lima":          [-12.20,  -77.20, -11.95, -76.95],
    "Medellin":      [  6.15,  -75.65,   6.30, -75.50],
    "Caracas":       [ 10.45,  -67.05,  10.60, -66.85],
    # ── Oceania ──────────────────────────────────────────────────────────────
    "Sydney":        [-34.05, 150.90,  -33.75, 151.30],
    "Melbourne":     [-37.90, 144.85,  -37.65, 145.10],
    "Brisbane":      [-27.55, 152.90,  -27.35, 153.10],
    "Perth":         [-32.05, 115.75,  -31.85, 116.00],
    "Auckland":      [-36.95, 174.65,  -36.75, 174.90],
}


class WAQIClient:
    """Wrapper around the WAQI REST API."""

    def __init__(self, token: str | None = None):
        self.token = token or os.environ["WAQI_TOKEN"]
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=20),
        reraise=True,
    )
    def _get(self, endpoint: str, params: dict = {}) -> dict:
        url = f"{BASE_URL}/{endpoint}"
        params = {"token": self.token, **params}
        resp = self.session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def get_stations_in_bounds(self, city: str) -> list[dict]:
        """
        GET /map/bounds — returns all monitoring stations inside the city bounding box.
        Each station has: uid, aqi, lat, lon, station.name
        """
        bounds = CITY_BOUNDS.get(city)
        if not bounds:
            logger.warning(f"No bounds defined for '{city}'")
            return []

        lat_min, lon_min, lat_max, lon_max = bounds
        try:
            data = self._get("map/bounds/", {
                "latlng": f"{lat_min},{lon_min},{lat_max},{lon_max}",
            })
        except Exception as e:
            logger.error(f"{city} bounds query failed: {e}")
            return []

        if data.get("status") != "ok":
            logger.warning(f"{city}: API returned status={data.get('status')}")
            return []

        stations = data.get("data", [])
        # Filter out stations with sentinel AQI value "-" (no data)
        active = [s for s in stations if str(s.get("aqi", "")).lstrip("-").isdigit()]
        logger.info(f"{city}: {len(active)} active stations (of {len(stations)} in bounds)")
        return active

    def get_station_feed(self, station_uid: int) -> dict | None:
        """
        GET /feed/@{uid} — full feed for one station.
        Returns AQI, all pollutants, city name, coordinates, timestamp.
        """
        try:
            data = self._get(f"feed/@{station_uid}/")
        except Exception as e:
            logger.warning(f"Station {station_uid} feed failed: {e}")
            return None

        if data.get("status") != "ok":
            return None

        return data.get("data")

    def _feed_to_row(self, city: str, uid: int, feed: dict, now_utc: str) -> dict:
        """Convert a WAQI station feed dict → DB row dict."""
        iaqi = feed.get("iaqi", {})
        geo  = feed.get("city", {}).get("geo", [None, None])
        t_iso = feed.get("time", {}).get("iso") or now_utc

        def poll(key):
            return iaqi.get(key, {}).get("v")

        return {
            "city":               city,
            "station_name":       feed.get("city", {}).get("name", ""),
            "station_id":         uid,
            "aqi":                feed.get("aqi"),
            "dominant_pollutant": feed.get("dominentpol", ""),
            "pm25":               poll("pm25"),
            "pm10":               poll("pm10"),
            "no2":                poll("no2"),
            "o3":                 poll("o3"),
            "co":                 poll("co"),
            "so2":                poll("so2"),
            "temperature":        poll("t"),
            "humidity":           poll("h"),
            "latitude":           geo[0] if geo else None,
            "longitude":          geo[1] if geo else None,
            "reading_time":       t_iso,
            "inserted_at":        now_utc,
        }

    def fetch_city_name_feed(self, city: str) -> dict | None:
        """
        GET /feed/{city}/ — city-level feed using WAQI's own city name index.
        Falls back to this when bounds-based discovery returns 0 active stations.
        Returns the raw feed dict (same shape as get_station_feed).
        """
        try:
            data = self._get(f"feed/{city}/")
        except Exception as e:
            logger.warning(f"{city} name-feed failed: {e}")
            return None
        if data.get("status") != "ok":
            return None
        return data.get("data")

    def fetch_city_readings(self, city: str) -> list[dict]:
        """
        Fetch a full snapshot of all active monitoring stations in a city.
        Falls back to /feed/{city}/ for cities where bounds returns 0 active stations.
        Returns flat list of dicts ready for DB insertion.
        """
        stations = self.get_stations_in_bounds(city)
        rows = []
        now_utc = datetime.now(tz=timezone.utc).isoformat()

        if stations:
            for station in stations:
                uid  = station.get("uid")
                feed = self.get_station_feed(uid)
                time.sleep(0.5)
                if not feed:
                    continue
                rows.append(self._feed_to_row(city, uid, feed, now_utc))
        else:
            # Bounds returned nothing — try WAQI's city name endpoint as fallback
            feed = self.fetch_city_name_feed(city)
            if feed:
                uid = feed.get("idx") or 0
                rows.append(self._feed_to_row(city, uid, feed, now_utc))
                logger.info(f"{city}: 0 stations in bounds, got 1 via /feed/{city}/ fallback")

        logger.info(f"{city}: {len(rows)} station readings fetched")
        return rows

    def fetch_city_forecast(self, city: str) -> list[dict]:
        """
        GET /feed/{city}/ — city-level feed including daily forecast.
        WAQI returns ~5-day daily pm25/pm10/o3 forecasts in the response.
        Returns flat list of forecast rows (one per pollutant per day).
        """
        try:
            data = self._get(f"feed/{city}/")
        except Exception as e:
            logger.warning(f"{city} forecast fetch failed: {e}")
            return []

        if data.get("status") != "ok":
            logger.warning(f"{city} forecast: status={data.get('status')}")
            return []

        feed     = data.get("data", {})
        forecast = feed.get("forecast", {}).get("daily", {})
        now_utc  = datetime.now(tz=timezone.utc).isoformat()
        geo      = feed.get("city", {}).get("geo", [None, None])

        rows = []
        for pollutant, days in forecast.items():
            for day in days:
                rows.append({
                    "city":        city,
                    "forecast_day": day.get("day"),           # "YYYY-MM-DD"
                    "pollutant":   pollutant,                  # pm25, pm10, o3
                    "avg":         day.get("avg"),
                    "min":         day.get("min"),
                    "max":         day.get("max"),
                    "latitude":    geo[0] if geo else None,
                    "longitude":   geo[1] if geo else None,
                    "fetched_at":  now_utc,
                })

        logger.info(f"{city}: {len(rows)} forecast rows fetched")
        return rows
