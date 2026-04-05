-- =============================================================================
-- Migration 004: TimescaleDB Continuous Aggregates
-- Pre-computed materialized views that refresh automatically.
-- These provide fast historical queries for Grafana trend panels.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Hourly Delay Trends — per-route hourly delay rollup
--    Powers the "Delay over Time" time-series graph.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS transit.cagg_hourly_delay
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', window_start)  AS bucket,
    route_id,
    direction_id,
    SUM(vehicle_count)                   AS total_observations,
    AVG(avg_delay_sec)                   AS avg_delay_sec,
    MAX(max_delay_sec)                   AS max_delay_sec,
    MIN(min_delay_sec)                   AS min_delay_sec
FROM transit.route_delay_metrics
GROUP BY bucket, route_id, direction_id
WITH NO DATA;

-- Refresh policy: materialize up to 2 hours ago, keep refreshing every 30 min
SELECT add_continuous_aggregate_policy('transit.cagg_hourly_delay',
    start_offset    => INTERVAL '24 hours',
    end_offset      => INTERVAL '2 hours',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists   => TRUE
);


-- ---------------------------------------------------------------------------
-- 2. Daily Delay Summary — one row per route per day
--    Powers the "Daily Performance" bar chart and heatmap.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS transit.cagg_daily_delay
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', window_start)   AS bucket,
    route_id,
    direction_id,
    SUM(vehicle_count)                   AS total_observations,
    AVG(avg_delay_sec)                   AS avg_delay_sec,
    MAX(max_delay_sec)                   AS peak_delay_sec,
    MIN(min_delay_sec)                   AS min_delay_sec
FROM transit.route_delay_metrics
GROUP BY bucket, route_id, direction_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('transit.cagg_daily_delay',
    start_offset    => INTERVAL '3 days',
    end_offset      => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists   => TRUE
);


-- ---------------------------------------------------------------------------
-- 3. Hourly Bunching Frequency — count of bunching events per route/hour
--    Powers "Bunching Trend" time-series panels.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS transit.cagg_hourly_bunching
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', event_time)    AS bucket,
    route_id,
    direction_id,
    COUNT(*)                             AS bunching_count,
    AVG(gap_seconds)                     AS avg_gap_seconds,
    MIN(gap_seconds)                     AS min_gap_seconds
FROM transit.bunching_events
GROUP BY bucket, route_id, direction_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('transit.cagg_hourly_bunching',
    start_offset    => INTERVAL '24 hours',
    end_offset      => INTERVAL '2 hours',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists   => TRUE
);


-- ---------------------------------------------------------------------------
-- 4. Hourly Speed Trends — average speed per route/hour
--    Powers "Speed over Time" and congestion trend panels.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS transit.cagg_hourly_speed
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', window_start)  AS bucket,
    route_id,
    direction_id,
    SUM(vehicle_count)                   AS total_observations,
    AVG(avg_speed_kmh)                   AS avg_speed_kmh,
    MIN(min_speed_kmh)                   AS min_speed_kmh,
    MAX(max_speed_kmh)                   AS max_speed_kmh
FROM transit.segment_speed_stats
GROUP BY bucket, route_id, direction_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('transit.cagg_hourly_speed',
    start_offset    => INTERVAL '24 hours',
    end_offset      => INTERVAL '2 hours',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists   => TRUE
);


-- ---------------------------------------------------------------------------
-- 5. Hourly Service Reliability — gap count + total gap duration per route
--    Powers "Service Reliability Score" panel.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS transit.cagg_hourly_reliability
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', gap_start)     AS bucket,
    route_id,
    direction_id,
    COUNT(*)                             AS gap_count,
    SUM(gap_duration_sec)                AS total_gap_sec,
    AVG(gap_duration_sec)                AS avg_gap_sec,
    MAX(gap_duration_sec)                AS max_gap_sec
FROM transit.service_gap_alerts
GROUP BY bucket, route_id, direction_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('transit.cagg_hourly_reliability',
    start_offset    => INTERVAL '24 hours',
    end_offset      => INTERVAL '2 hours',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists   => TRUE
);


-- ---------------------------------------------------------------------------
-- 6. Daily Fleet Activity — vehicle/route counts per day
--    Powers the "Fleet Utilization" panel.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS transit.cagg_daily_fleet
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', event_time)     AS bucket,
    COUNT(DISTINCT vehicle_id)           AS unique_vehicles,
    COUNT(DISTINCT route_id)             AS unique_routes,
    COUNT(*)                             AS total_position_reports
FROM transit.vehicle_positions
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('transit.cagg_daily_fleet',
    start_offset    => INTERVAL '3 days',
    end_offset      => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists   => TRUE
);


-- ---------------------------------------------------------------------------
-- Retention on continuous aggregates (keep 1 year of hourly, 2 years daily)
-- ---------------------------------------------------------------------------
SELECT add_retention_policy('transit.cagg_hourly_delay',    INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.cagg_hourly_bunching', INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.cagg_hourly_speed',    INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.cagg_hourly_reliability', INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.cagg_daily_delay',     INTERVAL '730 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.cagg_daily_fleet',     INTERVAL '730 days', if_not_exists => TRUE);
