-- =============================================================================
-- Migration 002: Real-time Analytics Output Tables (TimescaleDB hypertables)
-- These tables receive processed metrics from the PyFlink pipeline via JDBC.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Vehicle Positions — raw telemetry sink from Kafka (used for replay/debug)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transit.vehicle_positions (
    event_time       TIMESTAMPTZ NOT NULL,
    vehicle_id       TEXT NOT NULL,
    route_id         TEXT NOT NULL,
    direction_id     SMALLINT,
    agency_id        TEXT,
    latitude         DOUBLE PRECISION NOT NULL,
    longitude        DOUBLE PRECISION NOT NULL,
    license_plate    TEXT,
    trip_start_time  TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.vehicle_positions', 'event_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 hour'
);

CREATE INDEX IF NOT EXISTS idx_vp_route_time
    ON transit.vehicle_positions (route_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_vp_vehicle_time
    ON transit.vehicle_positions (vehicle_id, event_time DESC);


-- ---------------------------------------------------------------------------
-- 2. Route Delay Metrics — per-vehicle delay computed against GTFS schedule
--    Flink compares live position timestamps with expected stop_times
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transit.route_delay_metrics (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    route_id         TEXT NOT NULL,
    direction_id     SMALLINT,
    vehicle_count    INTEGER NOT NULL DEFAULT 0,
    avg_delay_sec    DOUBLE PRECISION,       -- positive = late, negative = early
    max_delay_sec    DOUBLE PRECISION,
    min_delay_sec    DOUBLE PRECISION,
    p95_delay_sec    DOUBLE PRECISION,
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.route_delay_metrics', 'window_start',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_rdm_route_window
    ON transit.route_delay_metrics (route_id, window_start DESC);


-- ---------------------------------------------------------------------------
-- 3. Route Bunching Events — two vehicles on the same route/direction
--    detected within a configurable threshold (e.g., <2 min apart).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transit.bunching_events (
    event_time       TIMESTAMPTZ NOT NULL,
    route_id         TEXT NOT NULL,
    direction_id     SMALLINT,
    vehicle_a_id     TEXT NOT NULL,
    vehicle_b_id     TEXT NOT NULL,
    gap_seconds      DOUBLE PRECISION NOT NULL,   -- time gap between the two vehicles
    lat_a            DOUBLE PRECISION,
    lon_a            DOUBLE PRECISION,
    lat_b            DOUBLE PRECISION,
    lon_b            DOUBLE PRECISION,
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.bunching_events', 'event_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_be_route_time
    ON transit.bunching_events (route_id, event_time DESC);


-- ---------------------------------------------------------------------------
-- 4. Service Gap Alerts — route/direction with no vehicle seen for too long.
--    Flink Session windows detect prolonged absence per route.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transit.service_gap_alerts (
    gap_start        TIMESTAMPTZ NOT NULL,
    gap_end          TIMESTAMPTZ NOT NULL,
    route_id         TEXT NOT NULL,
    direction_id     SMALLINT,
    gap_duration_sec DOUBLE PRECISION NOT NULL,
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.service_gap_alerts', 'gap_start',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_sga_route_time
    ON transit.service_gap_alerts (route_id, gap_start DESC);


-- ---------------------------------------------------------------------------
-- 5. Segment Speed Aggregates — avg speed per route segment over 15-min
--    tumbling windows.  Used for congestion hotspot detection.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transit.segment_speed_stats (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    route_id         TEXT NOT NULL,
    direction_id     SMALLINT,
    vehicle_count    INTEGER NOT NULL DEFAULT 0,
    avg_speed_kmh    DOUBLE PRECISION,
    min_speed_kmh    DOUBLE PRECISION,
    max_speed_kmh    DOUBLE PRECISION,
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.segment_speed_stats', 'window_start',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_sss_route_window
    ON transit.segment_speed_stats (route_id, window_start DESC);


-- ---------------------------------------------------------------------------
-- Retention policies — automatically drop old data (adjustable)
-- ---------------------------------------------------------------------------
SELECT add_retention_policy('transit.vehicle_positions', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.bunching_events', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.service_gap_alerts', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.route_delay_metrics', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.segment_speed_stats', INTERVAL '90 days', if_not_exists => TRUE);
