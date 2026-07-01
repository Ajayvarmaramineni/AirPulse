-- int_city_daily_averages
-- Aggregates all station snapshots (WAQI + OpenAQ) to city + day level.

WITH readings AS (
    SELECT * FROM {{ ref('stg_all__readings') }}
),

daily AS (
    SELECT
        city,
        reading_date,
        ROUND(AVG(aqi), 1)                                          AS avg_aqi,
        MAX(aqi)                                                    AS max_aqi,
        MIN(aqi)                                                    AS min_aqi,
        ROUND(AVG(pm25), 2)                                         AS avg_pm25,
        ROUND(MAX(pm25), 2)                                         AS max_pm25,
        ROUND(AVG(pm10), 2)                                         AS avg_pm10,
        ROUND(AVG(no2),  2)                                         AS avg_no2,
        ROUND(AVG(o3),   2)                                         AS avg_o3,
        ROUND(AVG(latitude)::NUMERIC,  6)                           AS city_lat,
        ROUND(AVG(longitude)::NUMERIC, 6)                           AS city_lon,
        COUNT(*)                                                    AS reading_count,
        COUNT(DISTINCT station_id)                                  AS station_count,
        MODE() WITHIN GROUP (ORDER BY dominant_pollutant)           AS dominant_pollutant
    FROM readings
    GROUP BY 1, 2
),

with_rolling AS (
    SELECT
        *,
        ROUND(AVG(avg_aqi) OVER (
            PARTITION BY city ORDER BY reading_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 1) AS rolling_7d_avg_aqi,

        ROUND(AVG(avg_pm25) OVER (
            PARTITION BY city ORDER BY reading_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2) AS rolling_7d_avg_pm25
    FROM daily
)

SELECT * FROM with_rolling
