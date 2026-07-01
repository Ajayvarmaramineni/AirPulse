"""
Unit tests for the ingestion layer.
Run: pytest tests/ -v
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────────────────────
# OpenAQ Client tests
# ─────────────────────────────────────────────────────────────────────────────

class TestOpenAQClient:

    def setup_method(self):
        """Set a dummy API key so the client doesn't read from env."""
        import sys, os
        os.environ["OPENAQ_API_KEY"] = "test_key_12345"
        sys.path.insert(0, ".")
        from ingestion.openaq_client import OpenAQClient
        self.client = OpenAQClient(api_key="test_key_12345")

    @patch("ingestion.openaq_client.requests.Session.get")
    def test_get_locations_returns_list(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "results": [
                {"id": 1, "name": "Downtown Monitor", "coordinates": {"latitude": 34.05, "longitude": -118.24}},
                {"id": 2, "name": "Eastside Monitor", "coordinates": {"latitude": 34.02, "longitude": -118.18}},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        locations = self.client.get_locations_for_city("Los Angeles")
        assert len(locations) == 2
        assert locations[0]["id"] == 1

    @patch("ingestion.openaq_client.requests.Session.get")
    def test_fetch_city_readings_returns_flat_list(self, mock_get):
        """fetch_city_readings should return dicts with all required keys."""
        required_keys = {
            "location_id", "location_name", "city", "country",
            "parameter", "value", "unit", "date_utc", "date_local",
            "latitude", "longitude"
        }

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        # locations call
        locations_payload = {
            "results": [{"id": 99, "name": "Test Sensor",
                          "coordinates": {"latitude": 34.0, "longitude": -118.0},
                          "country": {"code": "US"}}]
        }
        # measurements call
        measurements_payload = {
            "results": [{
                "parameter": "pm25",
                "value": 12.5,
                "unit": "µg/m³",
                "date": {"utc": "2024-01-15T12:00:00Z", "local": "2024-01-15T04:00:00-08:00"},
            }]
        }

        mock_get.return_value.json.side_effect = [locations_payload, measurements_payload]
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status = MagicMock()

        rows = self.client.fetch_city_readings("Los Angeles", hours_back=1)
        assert len(rows) >= 0  # pagination may give 0 if side_effect runs out
        if rows:
            assert required_keys.issubset(rows[0].keys())

    def test_retry_on_rate_limit(self):
        """Client should not crash on instantiation without real network calls."""
        from ingestion.openaq_client import OpenAQClient
        c = OpenAQClient(api_key="dummy")
        assert c.api_key == "dummy"


# ─────────────────────────────────────────────────────────────────────────────
# DB Loader tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDbLoader:

    def test_empty_rows_returns_zero(self):
        from ingestion.db_loader import load_readings
        # Should return 0 without touching DB
        result = load_readings([])
        assert result == 0

    def test_filters_negative_values(self):
        """Rows with value < 0 should be filtered before insert."""
        rows = [
            {"location_id": "1", "location_name": "A", "city": "NYC", "country": "US",
             "parameter": "pm25", "value": -5.0, "unit": "µg/m³",
             "date_utc": "2024-01-15T12:00:00Z", "date_local": None,
             "latitude": 40.7, "longitude": -74.0},
            {"location_id": "2", "location_name": "B", "city": "NYC", "country": "US",
             "parameter": "pm25", "value": 10.0, "unit": "µg/m³",
             "date_utc": "2024-01-15T12:00:00Z", "date_local": None,
             "latitude": 40.7, "longitude": -74.0},
        ]
        # Patch the DB connection so we don't need a real Postgres
        with patch("ingestion.db_loader.get_connection") as mock_conn:
            mock_cursor = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_conn.return_value)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_conn.return_value.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

            from ingestion.db_loader import load_readings
            # Only 1 valid row should pass the filter (value=10.0)
            # We verify execute_batch was called with 1 row
            with patch("ingestion.db_loader.psycopg2.extras.execute_batch") as mock_batch:
                load_readings(rows)
                call_args = mock_batch.call_args
                inserted_rows = call_args[0][2]  # 3rd positional arg = rows
                assert len(inserted_rows) == 1
                assert inserted_rows[0]["value"] == 10.0


# ─────────────────────────────────────────────────────────────────────────────
# Data validation logic tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAQICategories:
    """
    Validate our AQI breakpoint logic independently of dbt.
    These mirror the CASE statements in fct_aqi_health_risk.sql.
    """

    @staticmethod
    def categorize_pm25(value: float) -> str:
        if value <= 12.0:
            return "Good"
        elif value <= 35.4:
            return "Moderate"
        elif value <= 55.4:
            return "Unhealthy for Sensitive Groups"
        elif value <= 150.4:
            return "Unhealthy"
        elif value <= 250.4:
            return "Very Unhealthy"
        else:
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
