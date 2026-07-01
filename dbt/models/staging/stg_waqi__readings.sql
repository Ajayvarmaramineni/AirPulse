-- stg_waqi__readings
-- Cleans and type-casts raw WAQI station snapshots.
-- One row per station per pipeline run.

WITH source AS (
    SELECT * FROM {{ source('raw', 'waqi_readings') }}
),

cleaned AS (
    SELECT
        id                                          AS reading_id,
        TRIM(city)                                  AS city,
        TRIM(station_name)                          AS station_name,
        station_id,

        -- AQI is already computed by WAQI (US AQI scale)
        CASE WHEN aqi BETWEEN 0 AND 500 THEN aqi ELSE NULL END AS aqi,

        LOWER(TRIM(dominant_pollutant))             AS dominant_pollutant,

        -- Individual pollutants (may be null if station doesn't measure them)
        CASE WHEN pm25 >= 0 THEN ROUND(pm25, 2) ELSE NULL END  AS pm25,
        CASE WHEN pm10 >= 0 THEN ROUND(pm10, 2) ELSE NULL END  AS pm10,
        CASE WHEN no2  >= 0 THEN ROUND(no2,  2) ELSE NULL END  AS no2,
        CASE WHEN o3   >= 0 THEN ROUND(o3,   2) ELSE NULL END  AS o3,
        CASE WHEN co   >= 0 THEN ROUND(co,   4) ELSE NULL END  AS co,
        CASE WHEN so2  >= 0 THEN ROUND(so2,  2) ELSE NULL END  AS so2,

        temperature,
        humidity,

        latitude::NUMERIC(9,6)                      AS latitude,
        longitude::NUMERIC(9,6)                     AS longitude,

        reading_time                                AS reading_timestamp,
        DATE(reading_time AT TIME ZONE 'UTC')       AS reading_date,
        DATE_PART('hour', reading_time)::INT        AS reading_hour,

        inserted_at
    FROM source
    WHERE
        aqi IS NOT NULL
        AND aqi BETWEEN 0 AND 500
        AND reading_time IS NOT NULL
)

SELECT * FROM cleaned
