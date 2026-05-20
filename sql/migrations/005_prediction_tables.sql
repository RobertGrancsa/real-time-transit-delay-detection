-- =============================================================================
-- Migration 005: Ground-truth delay, feature store, and prediction tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS transit.stop_delay_events (
    observed_time       TIMESTAMPTZ NOT NULL,
    scheduled_time      TIMESTAMPTZ NOT NULL,
    service_date        DATE NOT NULL,
    vehicle_id          TEXT NOT NULL,
    route_id            TEXT NOT NULL,
    direction_id        SMALLINT,
    trip_id             TEXT,
    stop_id             TEXT,
    stop_sequence       INTEGER,
    delay_sec           DOUBLE PRECISION NOT NULL,
    distance_to_stop_m  DOUBLE PRECISION,
    match_confidence    DOUBLE PRECISION,
    match_method        TEXT NOT NULL DEFAULT 'unknown',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.stop_delay_events', 'observed_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_sde_route_time
    ON transit.stop_delay_events (route_id, observed_time DESC);
CREATE INDEX IF NOT EXISTS idx_sde_vehicle_time
    ON transit.stop_delay_events (vehicle_id, observed_time DESC);
CREATE INDEX IF NOT EXISTS idx_sde_stop_time
    ON transit.stop_delay_events (stop_id, observed_time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sde_unique_match
    ON transit.stop_delay_events (
        observed_time,
        vehicle_id,
        route_id,
        direction_id,
        trip_id,
        stop_id
    );

COMMENT ON TABLE transit.stop_delay_events IS
    'Matched live vehicle observations against GTFS stops and scheduled times.';


CREATE TABLE IF NOT EXISTS transit.telemetry_freshness_metrics (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    route_id            TEXT NOT NULL,
    direction_id        SMALLINT,
    vehicle_count       INTEGER NOT NULL DEFAULT 0,
    avg_freshness_sec   DOUBLE PRECISION,
    max_freshness_sec   DOUBLE PRECISION,
    min_freshness_sec   DOUBLE PRECISION,
    p95_freshness_sec   DOUBLE PRECISION,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.telemetry_freshness_metrics', 'window_start',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_tfm_route_window
    ON transit.telemetry_freshness_metrics (route_id, window_start DESC);

COMMENT ON TABLE transit.telemetry_freshness_metrics IS
    'Data-quality metric measuring age of received GPS fixes. This is not schedule delay.';


CREATE TABLE IF NOT EXISTS transit.stop_dwell_times (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    route_id            TEXT NOT NULL,
    direction_id        SMALLINT,
    stop_id             TEXT NOT NULL,
    vehicle_id          TEXT,
    observation_count   INTEGER NOT NULL DEFAULT 0,
    avg_dwell_sec       DOUBLE PRECISION,
    max_dwell_sec       DOUBLE PRECISION,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.stop_dwell_times', 'window_start',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_sdt_stop_window
    ON transit.stop_dwell_times (stop_id, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_sdt_route_window
    ON transit.stop_dwell_times (route_id, window_start DESC);


CREATE TABLE IF NOT EXISTS transit.realtime_feature_windows (
    feature_time            TIMESTAMPTZ NOT NULL,
    route_id                TEXT NOT NULL,
    direction_id            SMALLINT,
    zone_id                 TEXT NOT NULL DEFAULT 'network',
    route_type              SMALLINT,
    hour_of_day             SMALLINT NOT NULL,
    day_of_week             SMALLINT NOT NULL,
    is_weekend              BOOLEAN NOT NULL DEFAULT FALSE,
    is_am_peak              BOOLEAN NOT NULL DEFAULT FALSE,
    is_pm_peak              BOOLEAN NOT NULL DEFAULT FALSE,
    vehicle_count           INTEGER NOT NULL DEFAULT 0,
    observation_count       INTEGER NOT NULL DEFAULT 0,
    avg_delay_sec           DOUBLE PRECISION,
    p95_delay_sec           DOUBLE PRECISION,
    max_delay_sec           DOUBLE PRECISION,
    avg_speed_kmh           DOUBLE PRECISION,
    min_speed_kmh           DOUBLE PRECISION,
    speed_drop_kmh          DOUBLE PRECISION,
    bunching_count          INTEGER NOT NULL DEFAULT 0,
    service_gap_count       INTEGER NOT NULL DEFAULT 0,
    total_gap_sec           DOUBLE PRECISION,
    avg_dwell_sec           DOUBLE PRECISION,
    hotspot_score           DOUBLE PRECISION,
    avg_freshness_sec       DOUBLE PRECISION,
    generated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.realtime_feature_windows', 'feature_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_rfw_route_time
    ON transit.realtime_feature_windows (route_id, direction_id, feature_time DESC);
CREATE INDEX IF NOT EXISTS idx_rfw_zone_time
    ON transit.realtime_feature_windows (zone_id, feature_time DESC);

COMMENT ON TABLE transit.realtime_feature_windows IS
    'Windowed features used by delay and bunching prediction models.';


CREATE TABLE IF NOT EXISTS transit.route_delay_predictions (
    prediction_time         TIMESTAMPTZ NOT NULL,
    target_time             TIMESTAMPTZ NOT NULL,
    horizon_minutes         INTEGER NOT NULL,
    route_id                TEXT NOT NULL,
    direction_id            SMALLINT,
    predicted_delay_sec     DOUBLE PRECISION NOT NULL,
    prediction_lower_sec    DOUBLE PRECISION,
    prediction_upper_sec    DOUBLE PRECISION,
    risk_level              TEXT NOT NULL DEFAULT 'unknown',
    confidence              DOUBLE PRECISION,
    model_version           TEXT NOT NULL,
    feature_time            TIMESTAMPTZ,
    generated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.route_delay_predictions', 'prediction_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_rdp_route_target
    ON transit.route_delay_predictions (route_id, direction_id, target_time DESC);
CREATE INDEX IF NOT EXISTS idx_rdp_model_time
    ON transit.route_delay_predictions (model_version, prediction_time DESC);


CREATE TABLE IF NOT EXISTS transit.bunching_predictions (
    prediction_time         TIMESTAMPTZ NOT NULL,
    target_time             TIMESTAMPTZ NOT NULL,
    horizon_minutes         INTEGER NOT NULL,
    route_id                TEXT NOT NULL,
    direction_id            SMALLINT,
    bunching_probability    DOUBLE PRECISION NOT NULL,
    risk_level              TEXT NOT NULL DEFAULT 'unknown',
    confidence              DOUBLE PRECISION,
    model_version           TEXT NOT NULL,
    feature_time            TIMESTAMPTZ,
    generated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.bunching_predictions', 'prediction_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_bp_route_target
    ON transit.bunching_predictions (route_id, direction_id, target_time DESC);


CREATE TABLE IF NOT EXISTS transit.delay_feature_attributions (
    attribution_time        TIMESTAMPTZ NOT NULL,
    route_id                TEXT NOT NULL,
    direction_id            SMALLINT,
    target_time             TIMESTAMPTZ NOT NULL,
    model_version           TEXT NOT NULL,
    feature_name            TEXT NOT NULL,
    feature_value           DOUBLE PRECISION,
    contribution_score      DOUBLE PRECISION NOT NULL,
    feature_rank            INTEGER NOT NULL,
    generated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.delay_feature_attributions', 'attribution_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_dfa_route_target
    ON transit.delay_feature_attributions (route_id, direction_id, target_time DESC, feature_rank);

COMMENT ON TABLE transit.delay_feature_attributions IS
    'Feature contribution rows for explaining predicted delay risk. These are associations, not causal proof.';


CREATE TABLE IF NOT EXISTS transit.geo_delay_hotspots (
    window_start            TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,
    zone_id                 TEXT NOT NULL,
    route_id                TEXT,
    direction_id            SMALLINT,
    centroid_lat            DOUBLE PRECISION,
    centroid_lon            DOUBLE PRECISION,
    observation_count       INTEGER NOT NULL DEFAULT 0,
    active_vehicle_count    INTEGER NOT NULL DEFAULT 0,
    avg_delay_sec           DOUBLE PRECISION,
    p95_delay_sec           DOUBLE PRECISION,
    avg_speed_kmh           DOUBLE PRECISION,
    bunching_count          INTEGER NOT NULL DEFAULT 0,
    service_gap_count       INTEGER NOT NULL DEFAULT 0,
    hotspot_score           DOUBLE PRECISION,
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('transit.geo_delay_hotspots', 'window_start',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_gdh_zone_window
    ON transit.geo_delay_hotspots (zone_id, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_gdh_route_window
    ON transit.geo_delay_hotspots (route_id, window_start DESC);

COMMENT ON TABLE transit.geo_delay_hotspots IS
    'Geographic or route-segment windows that concentrate delay, low speed, bunching, and service gaps.';


SELECT add_retention_policy('transit.stop_delay_events', INTERVAL '180 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.telemetry_freshness_metrics', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.stop_dwell_times', INTERVAL '180 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.realtime_feature_windows', INTERVAL '180 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.route_delay_predictions', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.bunching_predictions', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.delay_feature_attributions', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('transit.geo_delay_hotspots', INTERVAL '180 days', if_not_exists => TRUE);