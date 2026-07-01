-- fct_aqi_health_risk
-- City-level daily AQI health risk categories.
-- WAQI already provides US AQI — we just bucket it into EPA categories.

WITH daily AS (
    SELECT * FROM {{ ref('int_city_daily_averages') }}
),

categorized AS (
    SELECT
        city,
        reading_date,
        avg_aqi,
        max_aqi,
        min_aqi,
        avg_pm25,
        avg_pm10,
        avg_no2,
        avg_o3,
        rolling_7d_avg_aqi,
        rolling_7d_avg_pm25,
        city_lat,
        city_lon,
        reading_count,
        station_count,
        dominant_pollutant,

        CASE
            WHEN avg_aqi <= 50   THEN 'Good'
            WHEN avg_aqi <= 100  THEN 'Moderate'
            WHEN avg_aqi <= 150  THEN 'Unhealthy for Sensitive Groups'
            WHEN avg_aqi <= 200  THEN 'Unhealthy'
            WHEN avg_aqi <= 300  THEN 'Very Unhealthy'
            ELSE                      'Hazardous'
        END AS aqi_category,

        LEAST(100, ROUND((avg_aqi::NUMERIC / 300) * 100, 1)) AS risk_score

    FROM daily
),

final AS (
    SELECT
        *,
        ROUND(avg_aqi - LAG(avg_aqi) OVER (
            PARTITION BY city ORDER BY reading_date
        ), 1) AS day_over_day_change,

        CASE WHEN avg_aqi > rolling_7d_avg_aqi * 1.20 THEN TRUE ELSE FALSE END
            AS is_above_7d_baseline

    FROM categorized
)

SELECT * FROM final
