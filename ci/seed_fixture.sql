-- Synthetic CI fixture — NOT real air-quality data.
--
-- Gives `dbt build` rows to chew on so the staging filters, the Z-score window
-- function and the dbt tests are exercised in CI instead of passing vacuously
-- on empty tables.
--
--   "Fixture City A": 34 normal days (AQI 45-57), then a spike to AQI 320 yesterday
--                     -> must be flagged Critical by fct_anomaly_events and
--                        categorised Hazardous by fct_aqi_health_risk.
--   "Fixture City B": 35 normal days, no spike  -> must NOT be flagged.
--   Station 900003:   AQI 999 (out of range)    -> must be dropped by stg_waqi__readings.

SET timezone = 'UTC';

INSERT INTO raw.waqi_readings
    (city, station_name, station_id, aqi, dominant_pollutant,
     pm25, pm10, no2, o3, temperature, humidity, latitude, longitude, reading_time)
SELECT
    'Fixture City A', 'Fixture Station A', 900001,
    CASE WHEN d = 1 THEN 320 ELSE 45 + (d % 5) * 3 END,
    'pm25',
    CASE WHEN d = 1 THEN 280.0 ELSE 12.0 + (d % 5) END,
    30.0, 15.0, 20.0, 18.0, 55.0, 10.0, 20.0,
    date_trunc('day', now()) - d * INTERVAL '1 day' + INTERVAL '6 hours'
FROM generate_series(1, 35) AS d;

INSERT INTO raw.waqi_readings
    (city, station_name, station_id, aqi, dominant_pollutant,
     pm25, pm10, no2, o3, temperature, humidity, latitude, longitude, reading_time)
SELECT
    'Fixture City B', 'Fixture Station B', 900002,
    40 + (d % 7) * 2,
    'pm25',
    10.0 + (d % 7), 25.0, 12.0, 30.0, 12.0, 60.0, 30.0, 40.0,
    date_trunc('day', now()) - d * INTERVAL '1 day' + INTERVAL '6 hours'
FROM generate_series(1, 35) AS d;

-- Out-of-range sentinel row (WAQI AQI is 0-500) for City B
INSERT INTO raw.waqi_readings
    (city, station_name, station_id, aqi, dominant_pollutant, reading_time)
VALUES
    ('Fixture City B', 'Fixture Station Bad', 900003, 999, 'pm25',
     date_trunc('day', now()) - INTERVAL '1 day' + INTERVAL '7 hours');
