"""Match live vehicle positions to GTFS stops and generate true delay features."""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import psycopg2
import psycopg2.extras

from config.settings import settings

logger = logging.getLogger(__name__)

FETCH_UNMATCHED_POSITIONS_SQL = """
SELECT
    vp.event_time AS observed_time,
    vp.vehicle_id,
    vp.route_id,
    vp.direction_id,
    vp.latitude,
    vp.longitude,
    vp.trip_start_time
FROM transit.vehicle_positions vp
WHERE vp.event_time >= NOW() - make_interval(mins => %(lookback_minutes)s)
    AND vp.trip_start_time IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM transit.stop_delay_events sde
      WHERE sde.observed_time = vp.event_time
        AND sde.vehicle_id = vp.vehicle_id
        AND sde.route_id = vp.route_id
  )
ORDER BY vp.event_time DESC
LIMIT %(limit)s;
"""

MATCH_STOP_SQL = """
WITH params AS (
    SELECT
        %(observed_time)s::timestamptz AS observed_time,
        (%(observed_time)s::timestamptz AT TIME ZONE %(schedule_timezone)s)::date AS service_date,
        %(route_id)s::text AS route_id,
        %(direction_id)s::smallint AS direction_id,
        %(latitude)s::double precision AS latitude,
        %(longitude)s::double precision AS longitude,
        %(trip_start_seconds)s::double precision AS trip_start_seconds,
        %(trip_start_tolerance_seconds)s::double precision AS trip_start_tolerance_seconds,
        %(schedule_tolerance_seconds)s::double precision AS schedule_tolerance_seconds,
        %(stop_geofence_meters)s::double precision AS stop_geofence_meters,
        %(lat_delta)s::double precision AS lat_delta,
        %(lon_delta)s::double precision AS lon_delta,
        %(schedule_timezone)s::text AS schedule_timezone
),
first_stops AS (
    SELECT DISTINCT ON (trip_id)
        trip_id,
        COALESCE(departure_time, arrival_time) AS first_time
    FROM transit.stop_times
    WHERE COALESCE(departure_time, arrival_time) IS NOT NULL
    ORDER BY trip_id, stop_sequence
),
candidate_stops AS (
    SELECT
        p.observed_time,
        p.service_date,
        p.stop_geofence_meters,
        p.schedule_tolerance_seconds,
        p.trip_start_seconds,
        t.trip_id,
        st.stop_id,
        st.stop_sequence,
        COALESCE(st.arrival_time, st.departure_time) AS stop_time,
        (111320.0 * SQRT(
            POWER(p.latitude - s.stop_lat, 2) +
            POWER(COS(RADIANS(p.latitude)) * (p.longitude - s.stop_lon), 2)
        )) AS distance_to_stop_m
    FROM params p
    JOIN transit.trips t
        ON t.route_id = p.route_id
       AND (p.direction_id IS NULL OR t.direction_id = p.direction_id)
    JOIN transit.calendar c
        ON c.service_id = t.service_id
       AND p.service_date BETWEEN c.start_date AND c.end_date
       AND CASE EXTRACT(ISODOW FROM p.service_date)::int
            WHEN 1 THEN c.monday = 1
            WHEN 2 THEN c.tuesday = 1
            WHEN 3 THEN c.wednesday = 1
            WHEN 4 THEN c.thursday = 1
            WHEN 5 THEN c.friday = 1
            WHEN 6 THEN c.saturday = 1
            WHEN 7 THEN c.sunday = 1
            ELSE FALSE
       END
    JOIN first_stops fs ON fs.trip_id = t.trip_id
    JOIN transit.stop_times st ON st.trip_id = t.trip_id
    JOIN transit.stops s ON s.stop_id = st.stop_id
    WHERE COALESCE(st.arrival_time, st.departure_time) IS NOT NULL
            AND ABS(s.stop_lat - p.latitude) <= p.lat_delta
            AND ABS(s.stop_lon - p.longitude) <= p.lon_delta
      AND (
          p.trip_start_seconds IS NULL
          OR ABS(EXTRACT(EPOCH FROM fs.first_time) - p.trip_start_seconds)
             <= p.trip_start_tolerance_seconds
      )
),
scored AS (
    SELECT
        cs.*,
        (cs.service_date::timestamp + cs.stop_time) AT TIME ZONE %(schedule_timezone)s
            AS scheduled_time
    FROM candidate_stops cs
    WHERE cs.distance_to_stop_m <= cs.stop_geofence_meters
)
SELECT
    trip_id,
    stop_id,
    stop_sequence,
    service_date,
    scheduled_time,
    EXTRACT(EPOCH FROM (observed_time - scheduled_time))::double precision AS delay_sec,
    distance_to_stop_m,
    GREATEST(0.05, LEAST(1.0, 1.0 - (distance_to_stop_m / stop_geofence_meters)))
        AS match_confidence,
    CASE
        WHEN trip_start_seconds IS NULL THEN 'route_direction_geofence'
        ELSE 'trip_start_geofence'
    END AS match_method
FROM scored
WHERE ABS(EXTRACT(EPOCH FROM (observed_time - scheduled_time))) <= schedule_tolerance_seconds
ORDER BY distance_to_stop_m ASC, ABS(EXTRACT(EPOCH FROM (observed_time - scheduled_time))) ASC
LIMIT 1;
"""

INSERT_STOP_DELAY_SQL = """
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
    match_method
) VALUES %s
ON CONFLICT DO NOTHING;
"""

REFRESH_ROUTE_DELAY_SQL = """
DELETE FROM transit.route_delay_metrics
WHERE window_start >= time_bucket('1 minute', NOW() - make_interval(mins => %(lookback_minutes)s));

INSERT INTO transit.route_delay_metrics (
    window_start,
    window_end,
    route_id,
    direction_id,
    vehicle_count,
    avg_delay_sec,
    max_delay_sec,
    min_delay_sec,
    p95_delay_sec,
    computed_at
)
SELECT
    time_bucket('1 minute', observed_time) AS window_start,
    time_bucket('1 minute', observed_time) + INTERVAL '1 minute' AS window_end,
    route_id,
    direction_id,
    COUNT(DISTINCT vehicle_id)::integer AS vehicle_count,
    AVG(delay_sec) AS avg_delay_sec,
    MAX(delay_sec) AS max_delay_sec,
    MIN(delay_sec) AS min_delay_sec,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY delay_sec) AS p95_delay_sec,
    NOW() AS computed_at
FROM transit.stop_delay_events
WHERE observed_time >= NOW() - make_interval(mins => %(lookback_minutes)s)
GROUP BY time_bucket('1 minute', observed_time), route_id, direction_id;
"""

REFRESH_DWELL_SQL = """
DELETE FROM transit.stop_dwell_times
WHERE window_start >= time_bucket(
    make_interval(mins => %(window_minutes)s),
    NOW() - make_interval(mins => %(lookback_minutes)s)
);

INSERT INTO transit.stop_dwell_times (
    window_start,
    window_end,
    route_id,
    direction_id,
    stop_id,
    vehicle_id,
    observation_count,
    avg_dwell_sec,
    max_dwell_sec,
    computed_at
)
WITH nearest_positions AS (
    SELECT
        time_bucket(make_interval(mins => %(window_minutes)s), observed_time) AS window_start,
        time_bucket(make_interval(mins => %(window_minutes)s), observed_time)
            + make_interval(mins => %(window_minutes)s) AS window_end,
        route_id,
        direction_id,
        vehicle_id,
        stop_id,
        observed_time
    FROM transit.stop_delay_events
    WHERE observed_time >= NOW() - make_interval(mins => %(lookback_minutes)s)
      AND stop_id IS NOT NULL
),
dwell AS (
    SELECT
        window_start,
        window_end,
        route_id,
        direction_id,
        stop_id,
        vehicle_id,
        COUNT(*)::integer AS observation_count,
        EXTRACT(EPOCH FROM (MAX(observed_time) - MIN(observed_time)))::double precision AS dwell_sec
    FROM nearest_positions
    GROUP BY window_start, window_end, route_id, direction_id, stop_id, vehicle_id
    HAVING COUNT(*) >= 2
)
SELECT
    window_start,
    window_end,
    route_id,
    direction_id,
    stop_id,
    vehicle_id,
    observation_count,
    dwell_sec AS avg_dwell_sec,
    dwell_sec AS max_dwell_sec,
    NOW() AS computed_at
FROM dwell;
"""

REFRESH_GEO_HOTSPOTS_SQL = """
DELETE FROM transit.geo_delay_hotspots
WHERE window_start >= time_bucket(
    make_interval(mins => %(window_minutes)s),
    NOW() - make_interval(mins => %(lookback_minutes)s)
);

INSERT INTO transit.geo_delay_hotspots (
    window_start,
    window_end,
    zone_id,
    route_id,
    direction_id,
    centroid_lat,
    centroid_lon,
    observation_count,
    active_vehicle_count,
    avg_delay_sec,
    p95_delay_sec,
    avg_speed_kmh,
    bunching_count,
    service_gap_count,
    hotspot_score,
    computed_at
)
WITH stop_delay AS (
    SELECT
        time_bucket(make_interval(mins => %(window_minutes)s), sde.observed_time) AS window_start,
        time_bucket(make_interval(mins => %(window_minutes)s), sde.observed_time)
            + make_interval(mins => %(window_minutes)s) AS window_end,
        CONCAT(ROUND(s.stop_lat::numeric, 3), ':', ROUND(s.stop_lon::numeric, 3)) AS zone_id,
        sde.route_id,
        sde.direction_id,
        AVG(s.stop_lat) AS centroid_lat,
        AVG(s.stop_lon) AS centroid_lon,
        COUNT(*)::integer AS observation_count,
        COUNT(DISTINCT sde.vehicle_id)::integer AS active_vehicle_count,
        AVG(sde.delay_sec) AS avg_delay_sec,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY sde.delay_sec) AS p95_delay_sec
    FROM transit.stop_delay_events sde
    JOIN transit.stops s ON s.stop_id = sde.stop_id
    WHERE sde.observed_time >= NOW() - make_interval(mins => %(lookback_minutes)s)
    GROUP BY window_start, window_end, zone_id, sde.route_id, sde.direction_id
)
SELECT
    window_start,
    window_end,
    zone_id,
    route_id,
    direction_id,
    centroid_lat,
    centroid_lon,
    observation_count,
    active_vehicle_count,
    avg_delay_sec,
    p95_delay_sec,
    NULL::double precision AS avg_speed_kmh,
    0 AS bunching_count,
    0 AS service_gap_count,
    GREATEST(0.0, COALESCE(avg_delay_sec, 0.0) / 600.0)
        + GREATEST(0.0, COALESCE(p95_delay_sec, 0.0) / 900.0) AS hotspot_score,
    NOW() AS computed_at
FROM stop_delay;
"""

REFRESH_FEATURE_WINDOWS_SQL = """
DELETE FROM transit.realtime_feature_windows
WHERE feature_time >= time_bucket(
    make_interval(mins => %(window_minutes)s),
    NOW() - make_interval(mins => %(lookback_minutes)s)
);

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
WITH delay AS (
    SELECT
        time_bucket(make_interval(mins => %(window_minutes)s), window_start) AS feature_time,
        route_id,
        direction_id,
        SUM(vehicle_count)::integer AS vehicle_count,
        COUNT(*)::integer AS observation_count,
        AVG(avg_delay_sec) AS avg_delay_sec,
        AVG(p95_delay_sec) AS p95_delay_sec,
        MAX(max_delay_sec) AS max_delay_sec
    FROM transit.route_delay_metrics
    WHERE window_start >= NOW() - make_interval(mins => %(lookback_minutes)s)
    GROUP BY feature_time, route_id, direction_id
),
speed AS (
    SELECT
        time_bucket(make_interval(mins => %(window_minutes)s), window_start) AS feature_time,
        route_id,
        direction_id,
        AVG(avg_speed_kmh) AS avg_speed_kmh,
        MIN(min_speed_kmh) AS min_speed_kmh
    FROM transit.segment_speed_stats
    WHERE window_start >= NOW() - make_interval(mins => %(lookback_minutes)s)
    GROUP BY feature_time, route_id, direction_id
),
bunching AS (
    SELECT
        time_bucket(make_interval(mins => %(window_minutes)s), event_time) AS feature_time,
        route_id,
        direction_id,
        COUNT(*)::integer AS bunching_count
    FROM transit.bunching_events
    WHERE event_time >= NOW() - make_interval(mins => %(lookback_minutes)s)
    GROUP BY feature_time, route_id, direction_id
),
gaps AS (
    SELECT
        time_bucket(make_interval(mins => %(window_minutes)s), gap_start) AS feature_time,
        route_id,
        direction_id,
        COUNT(*)::integer AS service_gap_count,
        SUM(gap_duration_sec) AS total_gap_sec
    FROM transit.service_gap_alerts
    WHERE gap_start >= NOW() - make_interval(mins => %(lookback_minutes)s)
    GROUP BY feature_time, route_id, direction_id
),
dwell AS (
    SELECT
        time_bucket(make_interval(mins => %(window_minutes)s), window_start) AS feature_time,
        route_id,
        direction_id,
        AVG(avg_dwell_sec) AS avg_dwell_sec
    FROM transit.stop_dwell_times
    WHERE window_start >= NOW() - make_interval(mins => %(lookback_minutes)s)
    GROUP BY feature_time, route_id, direction_id
),
freshness AS (
    SELECT
        time_bucket(make_interval(mins => %(window_minutes)s), window_start) AS feature_time,
        route_id,
        direction_id,
        AVG(avg_freshness_sec) AS avg_freshness_sec
    FROM transit.telemetry_freshness_metrics
    WHERE window_start >= NOW() - make_interval(mins => %(lookback_minutes)s)
    GROUP BY feature_time, route_id, direction_id
),
hotspots AS (
    SELECT
        time_bucket(make_interval(mins => %(window_minutes)s), window_start) AS feature_time,
        route_id,
        direction_id,
        AVG(hotspot_score) AS hotspot_score
    FROM transit.geo_delay_hotspots
    WHERE window_start >= NOW() - make_interval(mins => %(lookback_minutes)s)
    GROUP BY feature_time, route_id, direction_id
)
SELECT
    d.feature_time,
    d.route_id,
    d.direction_id,
    'network' AS zone_id,
    r.route_type,
    EXTRACT(HOUR FROM d.feature_time AT TIME ZONE %(schedule_timezone)s)::smallint,
    EXTRACT(ISODOW FROM d.feature_time AT TIME ZONE %(schedule_timezone)s)::smallint,
    EXTRACT(ISODOW FROM d.feature_time AT TIME ZONE %(schedule_timezone)s) IN (6, 7),
    EXTRACT(HOUR FROM d.feature_time AT TIME ZONE %(schedule_timezone)s) BETWEEN 7 AND 9,
    EXTRACT(HOUR FROM d.feature_time AT TIME ZONE %(schedule_timezone)s) BETWEEN 16 AND 19,
    d.vehicle_count,
    d.observation_count,
    d.avg_delay_sec,
    d.p95_delay_sec,
    d.max_delay_sec,
    s.avg_speed_kmh,
    s.min_speed_kmh,
    GREATEST(0.0, 15.0 - COALESCE(s.avg_speed_kmh, 15.0)),
    COALESCE(b.bunching_count, 0),
    COALESCE(g.service_gap_count, 0),
    g.total_gap_sec,
    dw.avg_dwell_sec,
    COALESCE(h.hotspot_score, 0.0),
    f.avg_freshness_sec,
    NOW()
FROM delay d
LEFT JOIN transit.routes r ON r.route_id = d.route_id
LEFT JOIN speed s
    ON s.feature_time = d.feature_time
   AND s.route_id = d.route_id
   AND s.direction_id IS NOT DISTINCT FROM d.direction_id
LEFT JOIN bunching b
    ON b.feature_time = d.feature_time
   AND b.route_id = d.route_id
   AND b.direction_id IS NOT DISTINCT FROM d.direction_id
LEFT JOIN gaps g
    ON g.feature_time = d.feature_time
   AND g.route_id = d.route_id
   AND g.direction_id IS NOT DISTINCT FROM d.direction_id
LEFT JOIN dwell dw
    ON dw.feature_time = d.feature_time
   AND dw.route_id = d.route_id
   AND dw.direction_id IS NOT DISTINCT FROM d.direction_id
LEFT JOIN freshness f
    ON f.feature_time = d.feature_time
   AND f.route_id = d.route_id
   AND f.direction_id IS NOT DISTINCT FROM d.direction_id
LEFT JOIN hotspots h
    ON h.feature_time = d.feature_time
   AND h.route_id = d.route_id
   AND h.direction_id IS NOT DISTINCT FROM d.direction_id;
"""


@dataclass(frozen=True)
class StopDelayEvent:
    observed_time: datetime
    scheduled_time: datetime
    service_date: date
    vehicle_id: str
    route_id: str
    direction_id: int | None
    trip_id: str
    stop_id: str
    stop_sequence: int
    delay_sec: float
    distance_to_stop_m: float
    match_confidence: float
    match_method: str

    def as_tuple(self) -> tuple[Any, ...]:
        return (
            self.observed_time,
            self.scheduled_time,
            self.service_date,
            self.vehicle_id,
            self.route_id,
            self.direction_id,
            self.trip_id,
            self.stop_id,
            self.stop_sequence,
            self.delay_sec,
            self.distance_to_stop_m,
            self.match_confidence,
            self.match_method,
        )


def parse_gtfs_time_to_seconds(value: str | None) -> int | None:
    if value is None or not str(value).strip():
        return None
    parts = str(value).strip().split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    return hours * 3600 + minutes * 60 + seconds


def fetch_unmatched_positions(
    conn: Any, *, lookback_minutes: int, limit: int
) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            FETCH_UNMATCHED_POSITIONS_SQL,
            {"lookback_minutes": lookback_minutes, "limit": limit},
        )
        return [dict(row) for row in cur.fetchall()]


def find_stop_match(conn: Any, position: dict[str, Any]) -> StopDelayEvent | None:
    trip_start_seconds = parse_gtfs_time_to_seconds(position.get("trip_start_time"))
    stop_geofence_meters = settings.analytics.stop_geofence_meters
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            MATCH_STOP_SQL,
            {
                "observed_time": position["observed_time"],
                "route_id": position["route_id"],
                "direction_id": position.get("direction_id"),
                "latitude": position["latitude"],
                "longitude": position["longitude"],
                "trip_start_seconds": trip_start_seconds,
                "trip_start_tolerance_seconds": (
                    settings.analytics.trip_start_tolerance_minutes * 60
                ),
                "schedule_tolerance_seconds": (
                    settings.analytics.gtfs_match_schedule_tolerance_minutes * 60
                ),
                "stop_geofence_meters": stop_geofence_meters,
                "lat_delta": stop_geofence_meters / 111_320.0,
                "lon_delta": stop_geofence_meters / 75_000.0,
                "schedule_timezone": settings.analytics.schedule_timezone,
            },
        )
        match = cur.fetchone()

    if match is None:
        return None

    return StopDelayEvent(
        observed_time=position["observed_time"],
        scheduled_time=match["scheduled_time"],
        service_date=match["service_date"],
        vehicle_id=str(position["vehicle_id"]),
        route_id=str(position["route_id"]),
        direction_id=position.get("direction_id"),
        trip_id=str(match["trip_id"]),
        stop_id=str(match["stop_id"]),
        stop_sequence=int(match["stop_sequence"]),
        delay_sec=float(match["delay_sec"]),
        distance_to_stop_m=float(match["distance_to_stop_m"]),
        match_confidence=float(match["match_confidence"]),
        match_method=str(match["match_method"]),
    )


def insert_stop_delay_events(conn: Any, events: list[StopDelayEvent]) -> int:
    if not events:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            INSERT_STOP_DELAY_SQL,
            [event.as_tuple() for event in events],
            page_size=500,
        )
    conn.commit()
    return len(events)


def _matching_sql_params(*, lookback_minutes: int) -> dict[str, Any]:
    stop_geofence_meters = settings.analytics.stop_geofence_meters
    return {
        "lookback_minutes": lookback_minutes,
        "window_minutes": settings.analytics.feature_window_minutes,
        "stop_geofence_meters": stop_geofence_meters,
        "schedule_timezone": settings.analytics.schedule_timezone,
        "lat_delta": stop_geofence_meters / 111_320.0,
        "lon_delta": stop_geofence_meters / 75_000.0,
    }


def refresh_derived_analytics(conn: Any, *, lookback_minutes: int) -> None:
    params = _matching_sql_params(lookback_minutes=lookback_minutes)
    with conn.cursor() as cur:
        cur.execute(REFRESH_DWELL_SQL, params)
        cur.execute(REFRESH_ROUTE_DELAY_SQL, params)
        cur.execute(REFRESH_GEO_HOTSPOTS_SQL, params)
        cur.execute(REFRESH_FEATURE_WINDOWS_SQL, params)
    conn.commit()


def run_matching_once() -> dict[str, int]:
    conn = psycopg2.connect(settings.postgres.dsn)
    try:
        positions = fetch_unmatched_positions(
            conn,
            lookback_minutes=settings.analytics.gtfs_match_lookback_minutes,
            limit=settings.analytics.gtfs_match_batch_size,
        )
        events = [event for position in positions if (event := find_stop_match(conn, position))]
        inserted = insert_stop_delay_events(conn, events)
        refresh_derived_analytics(
            conn,
            lookback_minutes=settings.analytics.gtfs_match_lookback_minutes,
        )
    finally:
        conn.close()

    stats = {
        "positions_scanned": len(positions),
        "stop_matches": len(events),
        "stop_delay_events_inserted": inserted,
    }
    logger.info("GTFS matching cycle complete: %s", stats)
    return stats


def run_forever() -> None:
    while True:
        try:
            run_matching_once()
        except psycopg2.Error:
            logger.exception("GTFS matching cycle failed. Waiting before retry.")
        time.sleep(settings.analytics.gtfs_match_interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Match live vehicle positions to GTFS stops")
    parser.add_argument("--once", action="store_true", help="Run one matching cycle and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.once:
        run_matching_once()
        return
    run_forever()


if __name__ == "__main__":
    main()
