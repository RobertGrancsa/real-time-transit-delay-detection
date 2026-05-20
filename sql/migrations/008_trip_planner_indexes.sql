-- =============================================================================
-- Migration 008: Trip planner lookup indexes
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_stop_times_stop_departure_trip_sequence
    ON transit.stop_times (stop_id, departure_time, trip_id, stop_sequence);

CREATE INDEX IF NOT EXISTS idx_stop_times_stop_arrival_trip_sequence
    ON transit.stop_times (stop_id, arrival_time, trip_id, stop_sequence);

