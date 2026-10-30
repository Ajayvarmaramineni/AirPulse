"""
WAQI reports offline stations with the string "-" instead of an AQI number.
These tests pin how the client treats that sentinel in both discovery paths.
"""

from unittest.mock import patch

import pytest

from ingestion.waqi_client import WAQIClient, parse_aqi


@pytest.mark.parametrize("value,expected", [
    (45, 45),
    ("45", 45),
    (45.9, 45),
    ("-", None),
    ("", None),
    (None, None),
    ("nan", None),
    ("inf", None),
])
def test_parse_aqi(value, expected):
    assert parse_aqi(value) == expected


def _feed(aqi):
    return {
        "idx": 1234, "aqi": aqi, "dominentpol": "pm25",
        "city": {"name": "Test Station", "geo": [1.0, 2.0]},
        "time": {"iso": "2026-01-01T00:00:00+00:00"},
        "iaqi": {"pm25": {"v": 30.0}},
    }


def test_fallback_feed_with_sentinel_aqi_is_skipped():
    """Previously the "-" string reached the INT column and failed the whole city load."""
    client = WAQIClient(token="t")
    with patch.object(client, "get_stations_in_bounds", return_value=[]), \
         patch.object(client, "fetch_city_name_feed", return_value=_feed("-")):
        assert client.fetch_city_readings("Chengdu") == []


def test_fallback_feed_with_numeric_aqi_is_kept_as_int():
    client = WAQIClient(token="t")
    with patch.object(client, "get_stations_in_bounds", return_value=[]), \
         patch.object(client, "fetch_city_name_feed", return_value=_feed("87")):
        rows = client.fetch_city_readings("Chengdu")
    assert len(rows) == 1
    assert rows[0]["aqi"] == 87
    assert isinstance(rows[0]["aqi"], int)


def test_station_feed_with_sentinel_aqi_is_skipped():
    client = WAQIClient(token="t")
    stations = [{"uid": 1}, {"uid": 2}]
    feeds = {1: _feed("-"), 2: _feed(55)}
    with patch.object(client, "get_stations_in_bounds", return_value=stations), \
         patch.object(client, "get_station_feed", side_effect=lambda uid: feeds[uid]), \
         patch("ingestion.waqi_client.time.sleep"):
        rows = client.fetch_city_readings("Delhi")
    assert [r["station_id"] for r in rows] == [2]
    assert rows[0]["aqi"] == 55
