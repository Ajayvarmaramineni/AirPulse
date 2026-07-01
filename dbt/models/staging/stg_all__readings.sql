-- stg_all__readings
-- Unified staging layer: WAQI + OpenAQ normalized to a single schema.
-- This is what all downstream intermediate and mart models should reference.
-- Adding a new data source = add a new UNION block here.

WITH waqi AS (
    SELECT
        reading_id,
        city,
        station_name,
        station_id,
        aqi,
        dominant_pollutant,
        pm25, pm10, no2, o3, co, so2,
        temperature,
        humidity,
        latitude, longitude,
        reading_timestamp,
        reading_date,
        reading_hour,
        'waqi' AS data_source,
        inserted_at
    FROM {{ ref('stg_waqi__readings') }}
),

openaq AS (
    SELECT
        reading_id,
        city,
        station_name,
        station_id,
        aqi,
        dominant_pollutant,
        pm25, pm10, no2, o3, co, so2,
        temperature,
        humidity,
        latitude, longitude,
        reading_timestamp,
        reading_date,
        reading_hour,
        'openaq' AS data_source,
        inserted_at
    FROM {{ ref('stg_openaq__readings') }}
)

SELECT * FROM waqi
UNION ALL
SELECT * FROM openaq
