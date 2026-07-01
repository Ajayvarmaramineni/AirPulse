"""
Unit tests for the AirPulse ingestion layer.
Run: pytest tests/ -v
"""

import pytest
from unittest.mock import patch, MagicMock


# ─────────────────────────────────────────────────────────────────────────────
# WAQI Client tests
# ─────────────────────────────────────────────────────────────────────────────

class TestWAQIClient:

    def setup_method(self):
        import os
        import sys
        os.environ["WAQI_TOKEN"] = "test_token_12345"
        sys.path.insert(0, ".")
        from ingestion.waqi_client import WAQIClient
        self.client = WAQIClient(token="test_token_12345")

    @patch("ingestion.waqi_client.requests.Session.get")
    def test_get_stations_in_bounds_returns_list(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "ok",
            "data": [
                {"uid": 1, "aqi": "45", "lat": 28.6, "lon": 77.2,
                 "station": {"name": "Delhi ITO", "time": "2024-01-15T12:00:00Z"}},
                {"uid": 2, "aqi": "82", "lat": 28.7, "lon": 77.1,
                 "station": {"name": "Delhi Anand Vihar", "time": "2024-01-15T12:00:00Z"}},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        stations = self.client.get_stations_in_bounds("Delhi")
        assert isinstance(stations, list)
        assert len(stations) == 2
        assert stations[0]["uid"] == 1

    @patch("ingestion.waqi_client.requests.Session.get")
    def test_get_station_feed_returns_dict(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "ok",
            "data": {
                "idx": 1,
                "aqi": 90,
                "dominentpol": "pm25",
                "city": {"name": "Delhi ITO", "geo": [28.63, 77.22]},
                "time": {"iso": "2024-01-15T12:00:00+05:30"},
                "iaqi": {"pm25": {"v": 38.5}, "pm10": {"v": 60.0}, "t": {"v": 32.0}, "h": {"v": 55.0}},
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        feed = self.client.get_station_feed(1)
        assert feed is not None
        assert feed["aqi"] == 90
        assert "iaqi" in feed

    def test_client_instantiation_without_network(self):
        from ingestion.waqi_client import WAQIClient
        c = WAQIClient(token="dummy_token")
        assert c.token == "dummy_token"

    def test_unknown_city_returns_empty_list(self):
        """get_stations_in_bounds should return [] for cities not in CITY_BOUNDS."""
        stations = self.client.get_stations_in_bounds("UnknownCityXYZ")
        assert stations == []


# ─────────────────────────────────────────────────────────────────────────────
# DB Loader tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDbLoader:

    def test_empty_rows_returns_zero(self):
        from ingestion.db_loader import load_readings
        assert load_readings([]) == 0

    def test_load_forecasts_empty_returns_zero(self):
        from ingestion.db_loader import load_forecasts
        assert load_forecasts([]) == 0

    def test_rows_missing_required_fields_are_skipped(self):
        """Rows without station_id or reading_time must be filtered before insert."""
        rows = [
            # valid row
            {"city": "Delhi", "station_name": "ITO", "station_id": 1,
             "aqi": 90, "dominant_pollutant": "pm25",
             "pm25": 38.5, "pm10": 60.0, "no2": 10.0, "o3": 5.0,
             "co": None, "so2": None, "temperature": 32.0, "humidity": 55.0,
             "latitude": 28.63, "longitude": 77.22,
             "reading_time": "2024-01-15T12:00:00+00:00",
             "inserted_at": "2024-01-15T12:00:00+00:00"},
            # invalid — missing reading_time
            {"city": "Delhi", "station_name": "Bad", "station_id": 2,
             "aqi": 50, "dominant_pollutant": "pm25",
             "pm25": None, "pm10": None, "no2": None, "o3": None,
             "co": None, "so2": None, "temperature": None, "humidity": None,
             "latitude": None, "longitude": None,
             "reading_time": None,
             "inserted_at": "2024-01-15T12:00:00+00:00"},
        ]
        with patch("ingestion.db_loader.get_connection") as mock_conn:
            mock_cursor = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_conn.return_value)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_conn.return_value.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

            with patch("ingestion.db_loader.psycopg2.extras.execute_batch") as mock_batch:
                from ingestion.db_loader import load_readings
                load_readings(rows)
                inserted = mock_batch.call_args[0][2]
                # Only the valid row should be passed to execute_batch
                assert len(inserted) == 1
                assert inserted[0]["station_id"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# AQI breakpoint logic tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAQICategories:
    """
    Validates PM2.5 → AQI category breakpoints.
    Mirrors the CASE logic in fct_aqi_health_risk.sql.
    """

    @staticmethod
    def categorize_pm25(value: float) -> str:
        if value <= 12.0:
            return "Good"
        if value <= 35.4:
            return "Moderate"
        if value <= 55.4:
            return "Unhealthy for Sensitive Groups"
        if value <= 150.4:
            return "Unhealthy"
        if value <= 250.4:
            return "Very Unhealthy"
        return "Hazardous"

    @pytest.mark.parametrize("value,expected", [
        (5.0,   "Good"),
        (12.0,  "Good"),
        (12.1,  "Moderate"),
        (35.4,  "Moderate"),
        (35.5,  "Unhealthy for Sensitive Groups"),
        (55.4,  "Unhealthy for Sensitive Groups"),
        (55.5,  "Unhealthy"),
        (150.4, "Unhealthy"),
        (150.5, "Very Unhealthy"),
        (250.5, "Hazardous"),
        (999.0, "Hazardous"),
    ])
    def test_pm25_breakpoints(self, value, expected):
        assert self.categorize_pm25(value) == expected
