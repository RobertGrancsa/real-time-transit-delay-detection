-- Seed realistic synthetic delay history for demos.
-- The generated records are intentionally marked with synthetic_demo identifiers.

SET TIME ZONE 'Europe/Bucharest';

BEGIN;

DELETE FROM transit.stop_delay_events
WHERE match_method = 'synthetic_demo_seed'
  AND observed_time >= CURRENT_DATE - INTERVAL '16 days';

DELETE FROM transit.realtime_feature_windows
WHERE zone_id = 'synthetic_demo'
  AND feature_time >= CURRENT_DATE - INTERVAL '16 days';

DELETE FROM transit.route_delay_predictions
WHERE model_version = 'synthetic-demo-warm-start'
  AND target_time >= CURRENT_DATE - INTERVAL '1 day';

WITH route_dirs AS MATERIALIZED (
    SELECT DISTINCT
        t.route_id,
        t.direction_id,
        r.route_type
    FROM transit.trips t
    JOIN transit.routes r ON r.route_id = t.route_id
),
feature_hours AS MATERIALIZED (
    SELECT feature_time
    FROM generate_series(
        CURRENT_DATE - INTERVAL '15 days' + INTERVAL '5 hours',
        CURRENT_DATE - INTERVAL '1 day' + INTERVAL '23 hours',
        INTERVAL '1 hour'
    ) AS feature_time
),
synthetic_features AS (
    SELECT
        h.feature_time,
        rd.route_id,
        rd.direction_id,
        rd.route_type,
        EXTRACT(HOUR FROM h.feature_time)::smallint AS hour_of_day,
        EXTRACT(DOW FROM h.feature_time)::smallint AS day_of_week,
        EXTRACT(ISODOW FROM h.feature_time) IN (6, 7) AS is_weekend,
        EXTRACT(HOUR FROM h.feature_time) BETWEEN 7 AND 9 AS is_am_peak,
        EXTRACT(HOUR FROM h.feature_time) BETWEEN 16 AND 19 AS is_pm_peak,
        mod(abs(hashtext(rd.route_id || ':' || COALESCE(rd.direction_id::text, 'x'))::bigint), 100)
            AS route_seed,
        mod(abs(hashtext(rd.route_id || ':' || h.feature_time::date::text || ':incident')::bigint), 100)
            AS incident_seed
    FROM route_dirs rd
    CROSS JOIN feature_hours h
),
delay_calc AS (
    SELECT
        *,
        GREATEST(
            0,
            CASE route_type
                WHEN 1 THEN 35
                WHEN 0 THEN 80
                WHEN 11 THEN 120
                ELSE 150
            END
            + CASE
                WHEN is_am_peak THEN 260
                WHEN is_pm_peak THEN 340
                WHEN hour_of_day IN (5, 22, 23) THEN 60
                ELSE 110
            END
            - CASE WHEN is_weekend THEN 110 ELSE 0 END
            + route_seed * CASE route_type
                WHEN 1 THEN 1.1
                WHEN 0 THEN 1.8
                WHEN 11 THEN 2.4
                ELSE 3.1
            END
            + CASE
                WHEN incident_seed < 4 THEN 650 + incident_seed * 80
                ELSE 0
            END
        )::double precision AS avg_delay_sec
    FROM synthetic_features
)
INSERT INTO transit.realtime_feature_windows (
    feature_time,
    route_id,
    direction_id,
    zone_id,
    route_type,
    hour_of_day,
    day_of_week,
    is_weekend,
    is_am_peak,
    is_pm_peak,
    vehicle_count,
    observation_count,
    avg_delay_sec,
    p95_delay_sec,
    max_delay_sec,
    avg_speed_kmh,
    min_speed_kmh,
    speed_drop_kmh,
    bunching_count,
    service_gap_count,
    total_gap_sec,
    avg_dwell_sec,
    hotspot_score,
    avg_freshness_sec,
    generated_at
)
SELECT
    feature_time,
    route_id,
    direction_id,
    'synthetic_demo',
    route_type,
    hour_of_day,
    day_of_week,
    is_weekend,
    is_am_peak,
    is_pm_peak,
    (2 + mod(route_seed, 8) + CASE WHEN is_am_peak OR is_pm_peak THEN 4 ELSE 0 END)::integer,
    (10 + mod(route_seed, 18) + CASE WHEN is_am_peak OR is_pm_peak THEN 16 ELSE 0 END)::integer,
    avg_delay_sec,
    avg_delay_sec + 90 + route_seed * 2,
    avg_delay_sec + 180 + route_seed * 3,
    CASE route_type
        WHEN 1 THEN 33
        WHEN 0 THEN 19
        WHEN 11 THEN 16
        ELSE 18
    END - LEAST(avg_delay_sec / 220, 8),
    CASE route_type
        WHEN 1 THEN 24
        WHEN 0 THEN 11
        WHEN 11 THEN 9
        ELSE 10
    END - LEAST(avg_delay_sec / 320, 5),
    LEAST(avg_delay_sec / 100, 14),
    CASE WHEN avg_delay_sec > 520 THEN 1 + mod(route_seed, 3) ELSE 0 END,
    CASE WHEN avg_delay_sec > 720 THEN 1 ELSE 0 END,
    CASE WHEN avg_delay_sec > 720 THEN avg_delay_sec * 1.4 ELSE NULL END,
    18 + mod(route_seed, 45),
    GREATEST(0.0, avg_delay_sec / 600.0) + GREATEST(0.0, (avg_delay_sec + 180) / 900.0),
    15 + mod(route_seed, 35),
    NOW()
FROM delay_calc;

WITH stop_samples AS MATERIALIZED (
    SELECT *
    FROM (
        SELECT
            t.route_id,
            t.direction_id,
            t.trip_id,
            st.stop_id,
            st.stop_sequence,
            EXTRACT(EPOCH FROM COALESCE(st.arrival_time, st.departure_time))::integer AS stop_seconds,
            EXTRACT(HOUR FROM COALESCE(st.arrival_time, st.departure_time))::smallint AS stop_hour,
            ROW_NUMBER() OVER (
                PARTITION BY
                    t.route_id,
                    t.direction_id,
                    EXTRACT(HOUR FROM COALESCE(st.arrival_time, st.departure_time))
                ORDER BY md5(t.trip_id || ':' || st.stop_id || ':' || st.stop_sequence::text)
            ) AS sample_rank
        FROM transit.trips t
        JOIN transit.stop_times st ON st.trip_id = t.trip_id
        WHERE COALESCE(st.arrival_time, st.departure_time) IS NOT NULL
          AND EXTRACT(HOUR FROM COALESCE(st.arrival_time, st.departure_time)) BETWEEN 5 AND 23
    ) ranked
    WHERE sample_rank = 1
),
feature_samples AS (
    SELECT
        fw.feature_time,
        fw.route_id,
        fw.direction_id,
        fw.avg_delay_sec,
        ss.trip_id,
        ss.stop_id,
        ss.stop_sequence,
        ss.stop_seconds,
        mod(abs(hashtext(fw.route_id || ':' || fw.feature_time::text)::bigint), 121) - 60
            AS event_jitter_sec
    FROM transit.realtime_feature_windows fw
    JOIN stop_samples ss
      ON ss.route_id = fw.route_id
     AND (ss.direction_id = fw.direction_id OR (ss.direction_id IS NULL AND fw.direction_id IS NULL))
     AND ss.stop_hour = fw.hour_of_day
    WHERE fw.zone_id = 'synthetic_demo'
      AND fw.feature_time >= CURRENT_DATE - INTERVAL '15 days'
      AND fw.feature_time < CURRENT_DATE
)
INSERT INTO transit.stop_delay_events (
    observed_time,
    scheduled_time,
    service_date,
    vehicle_id,
    route_id,
    direction_id,
    trip_id,
    stop_id,
    stop_sequence,
    delay_sec,
    distance_to_stop_m,
    match_confidence,
    match_method,
    created_at
)
SELECT
    (date_trunc('day', feature_time) + make_interval(secs => stop_seconds))
        + make_interval(secs => GREATEST(-60, avg_delay_sec + event_jitter_sec)::integer),
    date_trunc('day', feature_time) + make_interval(secs => stop_seconds),
    feature_time::date,
    'demo-' || route_id || '-' || COALESCE(direction_id::text, 'x') || '-' || stop_id,
    route_id,
    direction_id,
    trip_id,
    stop_id,
    stop_sequence,
    GREATEST(-60, avg_delay_sec + event_jitter_sec),
    8 + mod(abs(hashtext(stop_id || ':' || feature_time::text)::bigint), 55),
    0.72 + mod(abs(hashtext(route_id || ':' || stop_id)::bigint), 24) / 100.0,
    'synthetic_demo_seed',
    NOW()
FROM feature_samples
ON CONFLICT DO NOTHING;

WITH route_dirs AS MATERIALIZED (
    SELECT DISTINCT
        t.route_id,
        t.direction_id,
        r.route_type
    FROM transit.trips t
    JOIN transit.routes r ON r.route_id = t.route_id
),
prediction_hours AS MATERIALIZED (
    SELECT target_time
    FROM generate_series(
        CURRENT_DATE + INTERVAL '5 hours',
        CURRENT_DATE + INTERVAL '2 days' + INTERVAL '23 hours',
        INTERVAL '1 hour'
    ) AS target_time
),
prediction_base AS (
    SELECT
        NOW() AS prediction_time,
        h.target_time,
        GREATEST(15, CEIL(EXTRACT(EPOCH FROM (h.target_time - NOW())) / 60.0))::integer
            AS horizon_minutes,
        rd.route_id,
        rd.direction_id,
        rd.route_type,
        EXTRACT(HOUR FROM h.target_time)::smallint AS hour_of_day,
        EXTRACT(ISODOW FROM h.target_time) IN (6, 7) AS is_weekend,
        EXTRACT(HOUR FROM h.target_time) BETWEEN 7 AND 9 AS is_am_peak,
        EXTRACT(HOUR FROM h.target_time) BETWEEN 16 AND 19 AS is_pm_peak,
        mod(abs(hashtext(rd.route_id || ':' || COALESCE(rd.direction_id::text, 'x'))::bigint), 100)
            AS route_seed,
        mod(abs(hashtext(rd.route_id || ':' || h.target_time::date::text || ':future_incident')::bigint), 100)
            AS incident_seed
    FROM route_dirs rd
    CROSS JOIN prediction_hours h
),
prediction_calc AS (
    SELECT
        *,
        GREATEST(
            0,
            CASE route_type
                WHEN 1 THEN 35
                WHEN 0 THEN 80
                WHEN 11 THEN 120
                ELSE 150
            END
            + CASE
                WHEN is_am_peak THEN 260
                WHEN is_pm_peak THEN 340
                WHEN hour_of_day IN (5, 22, 23) THEN 60
                ELSE 110
            END
            - CASE WHEN is_weekend THEN 110 ELSE 0 END
            + route_seed * CASE route_type
                WHEN 1 THEN 1.1
                WHEN 0 THEN 1.8
                WHEN 11 THEN 2.4
                ELSE 3.1
            END
            + CASE
                WHEN incident_seed < 4 THEN 650 + incident_seed * 80
                ELSE 0
            END
        )::double precision AS predicted_delay_sec
    FROM prediction_base
)
INSERT INTO transit.route_delay_predictions (
    prediction_time,
    target_time,
    horizon_minutes,
    route_id,
    direction_id,
    predicted_delay_sec,
    prediction_lower_sec,
    prediction_upper_sec,
    risk_level,
    confidence,
    model_version,
    feature_time,
    generated_at
)
SELECT
    prediction_time,
    target_time,
    horizon_minutes,
    route_id,
    direction_id,
    predicted_delay_sec,
    GREATEST(0, predicted_delay_sec - 120),
    predicted_delay_sec + 180,
    CASE
        WHEN predicted_delay_sec >= 600 THEN 'high'
        WHEN predicted_delay_sec >= 300 THEN 'medium'
        ELSE 'low'
    END,
    LEAST(0.88, 0.58 + route_seed / 350.0),
    'synthetic-demo-warm-start',
    target_time - INTERVAL '7 days',
    NOW()
FROM prediction_calc;

COMMIT;

SELECT 'synthetic_stop_delay_events' AS metric, COUNT(*) AS rows
FROM transit.stop_delay_events
WHERE match_method = 'synthetic_demo_seed'
  AND observed_time >= CURRENT_DATE - INTERVAL '16 days'
UNION ALL
SELECT 'synthetic_feature_windows', COUNT(*)
FROM transit.realtime_feature_windows
WHERE zone_id = 'synthetic_demo'
  AND feature_time >= CURRENT_DATE - INTERVAL '16 days'
UNION ALL
SELECT 'synthetic_route_predictions', COUNT(*)
FROM transit.route_delay_predictions
WHERE model_version = 'synthetic-demo-warm-start'
  AND target_time >= CURRENT_DATE - INTERVAL '1 day';
