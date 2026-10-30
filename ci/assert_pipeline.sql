-- Post-`dbt build` assertions against the synthetic fixture in ci/seed_fixture.sql.
-- Any RAISE EXCEPTION fails the CI step (psql runs with ON_ERROR_STOP=1).

DO $$
DECLARE
    n integer;
BEGIN
    -- 1. Staging drops the out-of-range AQI row: 35 + 35 valid rows survive, 1 is dropped.
    SELECT count(*) INTO n FROM public_staging.stg_waqi__readings WHERE city LIKE 'Fixture City%';
    IF n <> 70 THEN
        RAISE EXCEPTION 'staging: expected 70 fixture rows after filtering, got %', n;
    END IF;

    -- 2. The spike in City A is flagged as a Critical anomaly (Z-score >= 4).
    SELECT count(*) INTO n FROM public_marts.fct_anomaly_events
    WHERE city = 'Fixture City A' AND severity = 'Critical';
    IF n < 1 THEN
        RAISE EXCEPTION 'anomaly: expected >=1 Critical anomaly for Fixture City A, got %', n;
    END IF;

    -- 3. City B has no spike, so it must not be flagged.
    SELECT count(*) INTO n FROM public_marts.fct_anomaly_events WHERE city = 'Fixture City B';
    IF n <> 0 THEN
        RAISE EXCEPTION 'anomaly: expected 0 anomalies for Fixture City B, got %', n;
    END IF;

    -- 4. The spike day is categorised Hazardous in the health-risk mart.
    SELECT count(*) INTO n FROM public_marts.fct_aqi_health_risk
    WHERE city = 'Fixture City A' AND aqi_category = 'Hazardous';
    IF n <> 1 THEN
        RAISE EXCEPTION 'risk: expected exactly 1 Hazardous day for Fixture City A, got %', n;
    END IF;

    -- 5. 7-day rolling average is populated by the intermediate model.
    SELECT count(*) INTO n FROM public_intermediate.int_city_daily_averages
    WHERE city = 'Fixture City B' AND rolling_7d_avg_aqi IS NOT NULL;
    IF n <> 35 THEN
        RAISE EXCEPTION 'intermediate: expected 35 days with rolling avg for Fixture City B, got %', n;
    END IF;

    RAISE NOTICE 'All pipeline assertions passed';
END $$;
