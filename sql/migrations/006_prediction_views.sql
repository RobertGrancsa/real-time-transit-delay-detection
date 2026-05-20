-- =============================================================================
-- Migration 006: Prediction and root-cause views for Grafana
-- =============================================================================

CREATE OR REPLACE VIEW transit.v_recent_stop_delay AS
SELECT
    sde.observed_time,
    sde.scheduled_time,
    sde.service_date,
    sde.vehicle_id,
    sde.route_id,
    r.route_short_name,
    r.route_type,
    sde.direction_id,
    sde.trip_id,
    sde.stop_id,
    s.stop_name,
    ROUND(sde.delay_sec::numeric, 1) AS delay_sec,
    ROUND(sde.distance_to_stop_m::numeric, 1) AS distance_to_stop_m,
    ROUND(sde.match_confidence::numeric, 3) AS match_confidence,
    sde.match_method
FROM transit.stop_delay_events sde
LEFT JOIN transit.routes r ON r.route_id = sde.route_id
LEFT JOIN transit.stops s ON s.stop_id = sde.stop_id
WHERE sde.observed_time > NOW() - INTERVAL '2 hours'
ORDER BY sde.observed_time DESC;

COMMENT ON VIEW transit.v_recent_stop_delay IS
    'Recent stop-level schedule-delay events with route and stop labels.';


CREATE OR REPLACE VIEW transit.v_telemetry_freshness_status AS
SELECT
    tfm.window_start,
    tfm.window_end,
    tfm.route_id,
    r.route_short_name,
    r.route_type,
    tfm.direction_id,
    tfm.vehicle_count,
    ROUND(tfm.avg_freshness_sec::numeric, 1) AS avg_freshness_sec,
    ROUND(tfm.max_freshness_sec::numeric, 1) AS max_freshness_sec,
    ROUND(tfm.p95_freshness_sec::numeric, 1) AS p95_freshness_sec,
    CASE
        WHEN tfm.p95_freshness_sec > 120 THEN 'stale'
        WHEN tfm.p95_freshness_sec > 45 THEN 'degraded'
        ELSE 'healthy'
    END AS freshness_status
FROM transit.telemetry_freshness_metrics tfm
LEFT JOIN transit.routes r ON r.route_id = tfm.route_id
WHERE tfm.window_start = (
    SELECT MAX(window_start)
    FROM transit.telemetry_freshness_metrics
);

COMMENT ON VIEW transit.v_telemetry_freshness_status IS
    'Latest telemetry freshness by route. This is a data-quality view, not schedule delay.';


CREATE OR REPLACE VIEW transit.v_route_prediction_latest AS
SELECT DISTINCT ON (rdp.route_id, rdp.direction_id, rdp.target_time)
    rdp.prediction_time,
    rdp.target_time,
    rdp.horizon_minutes,
    rdp.route_id,
    r.route_short_name,
    r.route_type,
    rdp.direction_id,
    ROUND(rdp.predicted_delay_sec::numeric, 1) AS predicted_delay_sec,
    ROUND(rdp.prediction_lower_sec::numeric, 1) AS prediction_lower_sec,
    ROUND(rdp.prediction_upper_sec::numeric, 1) AS prediction_upper_sec,
    rdp.risk_level,
    ROUND(rdp.confidence::numeric, 3) AS confidence,
    rdp.model_version,
    rdp.feature_time,
    rdp.generated_at,
    CASE
        WHEN rdp.generated_at < NOW() - INTERVAL '2 hours' THEN TRUE
        ELSE FALSE
    END AS is_stale
FROM transit.route_delay_predictions rdp
LEFT JOIN transit.routes r ON r.route_id = rdp.route_id
WHERE rdp.target_time >= NOW() - INTERVAL '15 minutes'
ORDER BY rdp.route_id, rdp.direction_id, rdp.target_time, rdp.prediction_time DESC;

COMMENT ON VIEW transit.v_route_prediction_latest IS
    'Latest delay predictions by route, direction, and target time.';


CREATE OR REPLACE VIEW transit.v_bunching_prediction_latest AS
SELECT DISTINCT ON (bp.route_id, bp.direction_id, bp.target_time)
    bp.prediction_time,
    bp.target_time,
    bp.horizon_minutes,
    bp.route_id,
    r.route_short_name,
    r.route_type,
    bp.direction_id,
    ROUND(bp.bunching_probability::numeric, 3) AS bunching_probability,
    bp.risk_level,
    ROUND(bp.confidence::numeric, 3) AS confidence,
    bp.model_version,
    bp.feature_time,
    bp.generated_at
FROM transit.bunching_predictions bp
LEFT JOIN transit.routes r ON r.route_id = bp.route_id
WHERE bp.target_time >= NOW() - INTERVAL '15 minutes'
ORDER BY bp.route_id, bp.direction_id, bp.target_time, bp.prediction_time DESC;

COMMENT ON VIEW transit.v_bunching_prediction_latest IS
    'Latest bunching probability forecasts with route metadata.';


CREATE OR REPLACE VIEW transit.v_delay_feature_attribution_latest AS
SELECT
    dfa.attribution_time,
    dfa.route_id,
    r.route_short_name,
    r.route_type,
    dfa.direction_id,
    dfa.target_time,
    dfa.model_version,
    dfa.feature_name,
    ROUND(dfa.feature_value::numeric, 3) AS feature_value,
    ROUND(dfa.contribution_score::numeric, 3) AS contribution_score,
    dfa.feature_rank
FROM transit.delay_feature_attributions dfa
LEFT JOIN transit.routes r ON r.route_id = dfa.route_id
WHERE dfa.attribution_time > NOW() - INTERVAL '24 hours'
  AND dfa.feature_rank <= 5
ORDER BY dfa.target_time DESC, dfa.route_id, dfa.direction_id, dfa.feature_rank;

COMMENT ON VIEW transit.v_delay_feature_attribution_latest IS
    'Top model feature contributions for recent predicted delay risk.';


CREATE OR REPLACE VIEW transit.v_geo_delay_hotspots_recent AS
SELECT
    gdh.window_start,
    gdh.window_end,
    gdh.zone_id,
    gdh.route_id,
    r.route_short_name,
    r.route_type,
    gdh.direction_id,
    gdh.centroid_lat,
    gdh.centroid_lon,
    gdh.observation_count,
    gdh.active_vehicle_count,
    ROUND(gdh.avg_delay_sec::numeric, 1) AS avg_delay_sec,
    ROUND(gdh.p95_delay_sec::numeric, 1) AS p95_delay_sec,
    ROUND(gdh.avg_speed_kmh::numeric, 1) AS avg_speed_kmh,
    gdh.bunching_count,
    gdh.service_gap_count,
    ROUND(gdh.hotspot_score::numeric, 3) AS hotspot_score
FROM transit.geo_delay_hotspots gdh
LEFT JOIN transit.routes r ON r.route_id = gdh.route_id
WHERE gdh.window_start > NOW() - INTERVAL '2 hours'
ORDER BY gdh.hotspot_score DESC NULLS LAST, gdh.window_start DESC;

COMMENT ON VIEW transit.v_geo_delay_hotspots_recent IS
    'Recent geographic delay hotspots ranked by combined operational signals.';


CREATE OR REPLACE VIEW transit.v_delay_by_hour_of_day AS
SELECT
    EXTRACT(HOUR FROM bucket)::SMALLINT AS hour_of_day,
    route_id,
    direction_id,
    ROUND(AVG(avg_delay_sec)::numeric, 1) AS avg_delay_sec,
    ROUND(MAX(max_delay_sec)::numeric, 1) AS max_delay_sec,
    SUM(total_observations) AS total_observations
FROM transit.cagg_hourly_delay
GROUP BY EXTRACT(HOUR FROM bucket), route_id, direction_id
ORDER BY hour_of_day, route_id, direction_id;

COMMENT ON VIEW transit.v_delay_by_hour_of_day IS
    'Historical schedule delay grouped by route, direction, and hour of day.';


CREATE OR REPLACE VIEW transit.v_delay_by_route_type AS
SELECT
    r.route_type,
    CASE r.route_type
        WHEN 0 THEN 'Tram'
        WHEN 1 THEN 'Metro'
        WHEN 3 THEN 'Bus'
        WHEN 11 THEN 'Trolleybus'
        ELSE 'Other (' || r.route_type || ')'
    END AS route_type_label,
    ROUND(AVG(chd.avg_delay_sec)::numeric, 1) AS avg_delay_sec,
    ROUND(MAX(chd.max_delay_sec)::numeric, 1) AS max_delay_sec,
    SUM(chd.total_observations) AS total_observations
FROM transit.cagg_hourly_delay chd
LEFT JOIN transit.routes r ON r.route_id = chd.route_id
GROUP BY r.route_type
ORDER BY avg_delay_sec DESC NULLS LAST;

COMMENT ON VIEW transit.v_delay_by_route_type IS
    'Historical delay grouped by GTFS route_type.';


CREATE OR REPLACE VIEW transit.v_route_root_cause_hourly AS
SELECT
    d.bucket,
    d.route_id,
    r.route_short_name,
    r.route_type,
    d.direction_id,
    ROUND(d.avg_delay_sec::numeric, 1) AS avg_delay_sec,
    ROUND(d.max_delay_sec::numeric, 1) AS max_delay_sec,
    ROUND(s.avg_speed_kmh::numeric, 1) AS avg_speed_kmh,
    COALESCE(b.bunching_count, 0) AS bunching_count,
    COALESCE(g.gap_count, 0) AS service_gap_count,
    ROUND(g.total_gap_sec::numeric, 1) AS total_gap_sec,
    CASE
        WHEN d.avg_delay_sec > 300 AND s.avg_speed_kmh < 8 THEN 'low_speed_associated'
        WHEN d.avg_delay_sec > 300 AND COALESCE(b.bunching_count, 0) > 0 THEN 'bunching_associated'
        WHEN d.avg_delay_sec > 300 AND COALESCE(g.gap_count, 0) > 0 THEN 'service_gap_associated'
        WHEN d.avg_delay_sec > 300 THEN 'unexplained_high_delay'
        ELSE 'normal'
    END AS associated_factor
FROM transit.cagg_hourly_delay d
LEFT JOIN transit.routes r
    ON r.route_id = d.route_id
LEFT JOIN transit.cagg_hourly_speed s
    ON s.bucket = d.bucket
   AND s.route_id = d.route_id
   AND s.direction_id = d.direction_id
LEFT JOIN transit.cagg_hourly_bunching b
    ON b.bucket = d.bucket
   AND b.route_id = d.route_id
   AND b.direction_id = d.direction_id
LEFT JOIN transit.cagg_hourly_reliability g
    ON g.bucket = d.bucket
   AND g.route_id = d.route_id
   AND g.direction_id = d.direction_id
WHERE d.bucket > NOW() - INTERVAL '30 days'
ORDER BY d.bucket DESC, d.avg_delay_sec DESC NULLS LAST;

COMMENT ON VIEW transit.v_route_root_cause_hourly IS
    'Associates high-delay windows with speed, bunching, and service-gap signals. This is explanatory, not proof of causality.';