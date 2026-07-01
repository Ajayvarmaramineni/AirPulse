-- stg_openaq__readings
-- Cleans and normalizes raw OpenAQ v3 station snapshots.
-- AQI is computed from PM2.5 (raw concentrations) using the US EPA formula.

WITH source AS (
    SELECT * FROM {{ source('raw', 'openaq_readings') }}
),

cleaned AS (
    SELECT
        id                                              AS reading_id,
        TRIM(city)                                      AS city,
        TRIM(station_name)                              AS station_name,
        location_id                                     AS station_id,

        -- Use computed AQI (from PM2.5 EPA formula), filter implausible values
        CASE WHEN aqi_computed BETWEEN 0 AND 500 THEN aqi_computed ELSE NULL END AS aqi,

        -- Dominant pollutant: infer from which is highest relative to its threshold
        CASE
            WHEN pm25  >= 35.5 THEN 'pm25'
            WHEN pm10  >= 55   THEN 'pm10'
            WHEN no2   >= 100  THEN 'no2'
            WHEN o3    >= 100  THEN 'o3'
            WHEN so2   >= 75   THEN 'so2'
            WHEN co    >= 9    THEN 'co'
            ELSE 'pm25'
        END AS dominant_pollutant,

        CASE WHEN pm25 >= 0 THEN ROUND(pm25, 2) ELSE NULL END  AS pm25,
        CASE WHEN pm10 >= 0 THEN ROUND(pm10, 2) ELSE NULL END  AS pm10,
        CASE WHEN no2  >= 0 THEN ROUND(no2,  2) ELSE NULL END  AS no2,
        CASE WHEN o3   >= 0 THEN ROUND(o3,   2) ELSE NULL END  AS o3,
        CASE WHEN co   >= 0 THEN ROUND(co,   4) ELSE NULL END  AS co,
        CASE WHEN so2  >= 0 THEN ROUND(so2,  2) ELSE NULL END  AS so2,

        -- OpenAQ doesn't provide weather data
        NULL::NUMERIC AS temperature,
        NULL::NUMERIC AS humidity,

        latitude::NUMERIC(9,6)                          AS latitude,
        longitude::NUMERIC(9,6)                         AS longitude,

        reading_time                                    AS reading_timestamp,
        DATE(reading_time AT TIME ZONE 'UTC')           AS reading_date,
        DATE_PART('hour', reading_time)::INT            AS reading_hour,

        'openaq'                                        AS data_source,
        inserted_at
    FROM source
    WHERE
        aqi_computed IS NOT NULL
        AND aqi_computed BETWEEN 0 AND 500
        AND reading_time IS NOT NULL
)

SELECT * FROM cleaned
