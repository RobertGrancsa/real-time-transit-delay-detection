-- =============================================================================
-- Migration 007: GTFS matching runtime indexes
-- =============================================================================

DELETE FROM transit.stop_delay_events sde
USING (
    SELECT
        tableoid,
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY observed_time, vehicle_id, route_id, direction_id, trip_id, stop_id
            ORDER BY created_at DESC
        ) AS duplicate_rank
    FROM transit.stop_delay_events
) duplicates
WHERE sde.ctid = duplicates.ctid
    AND sde.tableoid = duplicates.tableoid
  AND duplicates.duplicate_rank > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_sde_unique_match
    ON transit.stop_delay_events (
        observed_time,
        vehicle_id,
        route_id,
        direction_id,
        trip_id,
        stop_id
    );

CREATE INDEX IF NOT EXISTS idx_vp_matcher_unmatched
    ON transit.vehicle_positions (event_time DESC, vehicle_id, route_id);

CREATE INDEX IF NOT EXISTS idx_trips_route_direction_service
    ON transit.trips (route_id, direction_id, service_id);

CREATE INDEX IF NOT EXISTS idx_calendar_service_dates
    ON transit.calendar (service_id, start_date, end_date);

CREATE INDEX IF NOT EXISTS idx_stop_times_trip_time
    ON transit.stop_times (trip_id, stop_sequence, arrival_time, departure_time);
