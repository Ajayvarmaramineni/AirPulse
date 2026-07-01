-- AirPulse DB schema — WAQI data source
-- Run manually: psql airquality -f scripts/init_db.sql

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw table: one row per station snapshot per pipeline run
CREATE TABLE IF NOT EXISTS raw.waqi_readings (
    id                  BIGSERIAL PRIMARY KEY,
    city                TEXT          NOT NULL,
    station_name        TEXT,
    station_id          INT           NOT NULL,
    aqi                 INT,
    dominant_pollutant  TEXT,
    pm25                NUMERIC(10,2),
    pm10                NUMERIC(10,2),
    no2                 NUMERIC(10,2),
    o3                  NUMERIC(10,2),
    co                  NUMERIC(10,4),
    so2                 NUMERIC(10,2),
    temperature         NUMERIC(6,2),
    humidity            NUMERIC(6,2),
    latitude            NUMERIC(9,6),
    longitude           NUMERIC(9,6),
    reading_time        TIMESTAMPTZ   NOT NULL,
    inserted_at         TIMESTAMPTZ   DEFAULT NOW(),
    -- One row per station per reading timestamp
    UNIQUE (station_id, reading_time)
);

CREATE INDEX IF NOT EXISTS idx_waqi_city    ON raw.waqi_readings (city);
CREATE INDEX IF NOT EXISTS idx_waqi_time    ON raw.waqi_readings (reading_time);
CREATE INDEX IF NOT EXISTS idx_waqi_station ON raw.waqi_readings (station_id);

-- Daily forecast table: WAQI pm25/pm10/o3 5-day outlook per city
CREATE TABLE IF NOT EXISTS raw.waqi_forecasts (
    id            BIGSERIAL PRIMARY KEY,
    city          TEXT          NOT NULL,
    forecast_day  DATE          NOT NULL,
    pollutant     TEXT          NOT NULL,   -- pm25, pm10, o3
    avg           NUMERIC(8,2),
    min           NUMERIC(8,2),
    max           NUMERIC(8,2),
    latitude      NUMERIC(9,6),
    longitude     NUMERIC(9,6),
    fetched_at    TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (city, forecast_day, pollutant)  -- upsert-safe
);

CREATE INDEX IF NOT EXISTS idx_forecast_city ON raw.waqi_forecasts (city);
CREATE INDEX IF NOT EXISTS idx_forecast_day  ON raw.waqi_forecasts (forecast_day);

-- OpenAQ v3 readings: one row per station snapshot
-- AQI is computed from PM2.5 using US EPA formula (WAQI provides it directly)
CREATE TABLE IF NOT EXISTS raw.openaq_readings (
    id              BIGSERIAL PRIMARY KEY,
    city            TEXT          NOT NULL,
    station_name    TEXT,
    location_id     INT           NOT NULL,
    pm25            NUMERIC(10,2),
    pm10            NUMERIC(10,2),
    no2             NUMERIC(10,2),
    o3              NUMERIC(10,2),
    co              NUMERIC(10,4),
    so2             NUMERIC(10,2),
    aqi_computed    INT,
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    country_code    TEXT,
    reading_time    TIMESTAMPTZ   NOT NULL,
    inserted_at     TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (location_id, reading_time)
);

CREATE INDEX IF NOT EXISTS idx_openaq_city ON raw.openaq_readings (city);
CREATE INDEX IF NOT EXISTS idx_openaq_time ON raw.openaq_readings (reading_time);
