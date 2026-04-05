-- =============================================================================
-- Migration 003: Grafana-ready Views & Functions
-- Provides pre-built views that Grafana panels can query directly.
-- These simplify dashboard SQL and avoid exposing raw hypertable complexity.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Live Fleet Map — latest known position per vehicle (for Geomap panel)
--    Returns one row per active vehicle seen in the last 5 minutes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW transit.v_live_fleet AS
SELECT DISTINCT ON (vehicle_id)
    event_time,
    vehicle_id,
    route_id,
    direction_id,
    agency_id,
    latitude,
    longitude,
    license_plate,
    trip_start_time
FROM transit.vehicle_positions
WHERE event_time > NOW() - INTERVAL '5 minutes'
ORDER BY vehicle_id, event_time DESC;

COMMENT ON VIEW transit.v_live_fleet IS
    'Latest known position per vehicle (last 5 min). Use for Grafana Geomap panel.';


-- ---------------------------------------------------------------------------
-- 2. Route Status Overview — current delay + vehicle count per route
--    Uses the most recent 1-minute window from route_delay_metrics.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW transit.v_route_status AS
SELECT
    rdm.route_id,
    r.route_short_name,
    r.route_type,
    rdm.direction_id,
    rdm.vehicle_count,
    ROUND(rdm.avg_delay_sec::numeric, 1)  AS avg_delay_sec,
    ROUND(rdm.max_delay_sec::numeric, 1)  AS max_delay_sec,
    ROUND(rdm.min_delay_sec::numeric, 1)  AS min_delay_sec,
    rdm.window_start,
    rdm.window_end,
    CASE
        WHEN rdm.avg_delay_sec > 300 THEN 'critical'
        WHEN rdm.avg_delay_sec > 120 THEN 'warning'
        ELSE 'normal'
    END AS status
FROM transit.route_delay_metrics rdm
LEFT JOIN transit.routes r ON r.route_id = rdm.route_id
WHERE rdm.window_start = (
    SELECT MAX(window_start)
    FROM transit.route_delay_metrics
);

COMMENT ON VIEW transit.v_route_status IS
    'Current delay status per route from the latest aggregation window.';


-- ---------------------------------------------------------------------------
-- 3. Active Bunching Alerts — bunching events in the last 30 minutes
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW transit.v_active_bunching AS
SELECT
    be.event_time,
    be.route_id,
    r.route_short_name,
    r.route_type,
    be.direction_id,
    be.vehicle_a_id,
    be.vehicle_b_id,
    ROUND(be.gap_seconds::numeric, 0) AS gap_seconds,
    be.lat_a,
    be.lon_a,
    be.lat_b,
    be.lon_b
FROM transit.bunching_events be
LEFT JOIN transit.routes r ON r.route_id = be.route_id
WHERE be.event_time > NOW() - INTERVAL '30 minutes'
ORDER BY be.event_time DESC;

COMMENT ON VIEW transit.v_active_bunching IS
    'Bunching events from the last 30 minutes. Use for alerts table panel.';


-- ---------------------------------------------------------------------------
-- 4. Active Service Gaps — routes with prolonged absence, last 60 minutes
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW transit.v_active_service_gaps AS
SELECT
    sga.gap_start,
    sga.gap_end,
    sga.route_id,
    r.route_short_name,
    r.route_type,
    sga.direction_id,
    ROUND(sga.gap_duration_sec::numeric, 0) AS gap_duration_sec,
    ROUND((sga.gap_duration_sec / 60.0)::numeric, 1)   AS gap_duration_min
FROM transit.service_gap_alerts sga
LEFT JOIN transit.routes r ON r.route_id = sga.route_id
WHERE sga.gap_start > NOW() - INTERVAL '60 minutes'
ORDER BY sga.gap_duration_sec DESC;

COMMENT ON VIEW transit.v_active_service_gaps IS
    'Service gaps detected in the last 60 minutes. Use for alerts panel.';


-- ---------------------------------------------------------------------------
-- 5. Route Speed Summary — latest 15-min speed stats per route
--    For the speed heatmap / bar chart panels.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW transit.v_route_speed_summary AS
SELECT
    sss.route_id,
    r.route_short_name,
    r.route_type,
    sss.direction_id,
    sss.vehicle_count,
    ROUND(sss.avg_speed_kmh::numeric, 1) AS avg_speed_kmh,
    ROUND(sss.min_speed_kmh::numeric, 1) AS min_speed_kmh,
    ROUND(sss.max_speed_kmh::numeric, 1) AS max_speed_kmh,
    sss.window_start,
    sss.window_end,
    CASE
        WHEN sss.avg_speed_kmh < 8  THEN 'congested'
        WHEN sss.avg_speed_kmh < 15 THEN 'slow'
        ELSE 'normal'
    END AS traffic_status
FROM transit.segment_speed_stats sss
LEFT JOIN transit.routes r ON r.route_id = sss.route_id
WHERE sss.window_start = (
    SELECT MAX(window_start)
    FROM transit.segment_speed_stats
);

COMMENT ON VIEW transit.v_route_speed_summary IS
    'Latest segment speed stats per route. Use for speed heatmap panel.';


-- ---------------------------------------------------------------------------
-- 6. Fleet Statistics — real-time KPI counters for stat panels
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW transit.v_fleet_kpis AS
SELECT
    (SELECT COUNT(DISTINCT vehicle_id)
     FROM transit.vehicle_positions
     WHERE event_time > NOW() - INTERVAL '5 minutes')       AS active_vehicles,

    (SELECT COUNT(DISTINCT route_id)
     FROM transit.vehicle_positions
     WHERE event_time > NOW() - INTERVAL '5 minutes')       AS active_routes,

    (SELECT ROUND(AVG(avg_delay_sec)::numeric, 1)
     FROM transit.route_delay_metrics
     WHERE window_start > NOW() - INTERVAL '5 minutes')     AS avg_network_delay_sec,

    (SELECT COUNT(*)
     FROM transit.bunching_events
     WHERE event_time > NOW() - INTERVAL '30 minutes')      AS bunching_events_30m,

    (SELECT COUNT(*)
     FROM transit.service_gap_alerts
     WHERE gap_start > NOW() - INTERVAL '60 minutes')       AS service_gaps_60m,

    (SELECT ROUND(AVG(avg_speed_kmh)::numeric, 1)
     FROM transit.segment_speed_stats
     WHERE window_start > NOW() - INTERVAL '15 minutes')    AS avg_network_speed_kmh;

COMMENT ON VIEW transit.v_fleet_kpis IS
    'Single-row KPI view for Grafana stat panels (active vehicles, avg delay, etc.).';


-- ---------------------------------------------------------------------------
-- 7. Route Type Labels — helper for Grafana variable dropdowns
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW transit.v_route_types AS
SELECT DISTINCT
    r.route_type,
    CASE r.route_type
        WHEN 0 THEN 'Tram'
        WHEN 1 THEN 'Metro'
        WHEN 3 THEN 'Bus'
        WHEN 11 THEN 'Trolleybus'
        ELSE 'Other (' || r.route_type || ')'
    END AS route_type_label,
    r.route_id,
    r.route_short_name
FROM transit.routes r
ORDER BY r.route_type, r.route_short_name;

COMMENT ON VIEW transit.v_route_types IS
    'Route type labels for Grafana template variable dropdowns.';
