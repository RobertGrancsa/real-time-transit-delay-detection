from __future__ import annotations

from datetime import date, datetime, timezone

from services.gtfs_matching.matcher import StopDelayEvent, parse_gtfs_time_to_seconds


def test_parse_gtfs_time_to_seconds_supports_after_midnight_times() -> None:
    assert parse_gtfs_time_to_seconds("00:00:00") == 0
    assert parse_gtfs_time_to_seconds("13:48:05") == 49685
    assert parse_gtfs_time_to_seconds("25:30:00") == 91800


def test_parse_gtfs_time_to_seconds_rejects_missing_or_malformed_values() -> None:
    assert parse_gtfs_time_to_seconds(None) is None
    assert parse_gtfs_time_to_seconds("") is None
    assert parse_gtfs_time_to_seconds("13:48") is None
    assert parse_gtfs_time_to_seconds("bad:value:00") is None


def test_stop_delay_event_tuple_matches_insert_shape() -> None:
    event = StopDelayEvent(
        observed_time=datetime(2026, 5, 20, 10, 5, tzinfo=timezone.utc),
        scheduled_time=datetime(2026, 5, 20, 10, 0, tzinfo=timezone.utc),
        service_date=date(2026, 5, 20),
        vehicle_id="veh-1",
        route_id="10",
        direction_id=1,
        trip_id="trip-1",
        stop_id="stop-1",
        stop_sequence=3,
        delay_sec=300.0,
        distance_to_stop_m=12.5,
        match_confidence=0.9,
        match_method="trip_start_geofence",
    )

    assert len(event.as_tuple()) == 13
    assert event.as_tuple()[9] == 300.0
