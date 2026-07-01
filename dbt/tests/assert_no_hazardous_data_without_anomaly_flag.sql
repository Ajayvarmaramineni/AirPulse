-- Custom dbt test:
-- Every city-day where aqi_category = 'Hazardous' must have
-- at least one corresponding anomaly event record.
-- Returns rows that VIOLATE this rule (test fails if any rows returned).

SELECT
    h.city,
    h.reading_date,
    h.aqi_category,
    h.avg_aqi
FROM {{ ref('fct_aqi_health_risk') }} h
LEFT JOIN {{ ref('fct_anomaly_events') }} a
    ON  h.city         = a.city
    AND h.reading_date = a.reading_date
WHERE
    h.aqi_category = 'Hazardous'
    AND a.city IS NULL
