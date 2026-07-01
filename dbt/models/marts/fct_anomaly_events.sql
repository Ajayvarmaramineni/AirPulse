-- fct_anomaly_events
-- Flags city-days where AQI is a statistical outlier (Z-score > 2.5 above 30-day baseline).
-- Powers the "Anomaly Alert Feed" panel in the Streamlit dashboard.
--
-- Source: int_city_daily_averages (city-level daily AQI)
-- Z-score = (daily_aqi - rolling_mean) / rolling_stddev

WITH daily AS (
    SELECT * FROM {{ ref('int_city_daily_averages') }}
),

with_stats AS (
    SELECT
        *,
        AVG(avg_aqi) OVER (
            PARTITION BY city
            ORDER BY reading_date
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING   -- 30-day baseline
        ) AS rolling_mean,

        STDDEV(avg_aqi) OVER (
            PARTITION BY city
            ORDER BY reading_date
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) AS rolling_stddev
    FROM daily
),

z_scored AS (
    SELECT
        *,
        CASE
            WHEN rolling_stddev > 0 THEN
                ROUND(((avg_aqi - rolling_mean) / rolling_stddev)::NUMERIC, 2)
            ELSE NULL
        END AS z_score
    FROM with_stats
),

anomalies AS (
    SELECT
        city,
        reading_date,
        avg_aqi                                                     AS anomaly_aqi,
        ROUND(rolling_mean::NUMERIC, 1)                             AS baseline_mean_aqi,
        ROUND(rolling_stddev::NUMERIC, 2)                           AS baseline_stddev,
        z_score,
        ROUND(((avg_aqi - rolling_mean) / NULLIF(rolling_mean, 0)) * 100, 1)
                                                                    AS pct_above_baseline,
        dominant_pollutant,
        station_count,
        city_lat,
        city_lon,

        CASE
            WHEN z_score >= 4.0 THEN 'Critical'
            WHEN z_score >= 3.0 THEN 'High'
            WHEN z_score >= 2.5 THEN 'Medium'
            ELSE 'Low'
        END AS severity

    FROM z_scored
    WHERE
        z_score >= 2.5
        AND rolling_mean IS NOT NULL
        AND avg_aqi > 0
)

SELECT * FROM anomalies
ORDER BY reading_date DESC
