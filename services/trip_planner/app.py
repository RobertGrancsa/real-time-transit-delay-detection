"""FastAPI trip planner for Bucharest GTFS stops and schedules."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from config.settings import settings

BUCHAREST_TZ = ZoneInfo(settings.analytics.schedule_timezone)
DEFAULT_SEARCH_WINDOW_SECONDS = 6 * 60 * 60
DEFAULT_TRANSFER_WAIT_SECONDS = 60 * 60
DEFAULT_TRANSFER_WALK_METERS = 180
DEFAULT_MIN_TRANSFER_SECONDS = 120
LOCATION_STOP_SEARCH_METERS = 900
LOCATION_STOP_LIMIT = 10
WALKING_SPEED_MPS = 1.25
MAX_WALK_ONLY_MINUTES = 45
WALK_FIRST_MAX_MINUTES = 15
WALK_OVER_TRANSIT_GAIN_MINUTES = 8
MAX_FIRST_LEG_CANDIDATES = 900
MAX_ITINERARIES = 5
CANDIDATE_ITINERARY_LIMIT = 30
ROUTE_FILTER_WALK_SLACK_MINUTES = 4
ROUTE_FILTER_TIME_GAIN_MINUTES = 3

STOP_PLACE_ID_SQL = """
COALESCE(
    NULLIF(parent_station, ''),
    'geo:' || md5(
        lower(regexp_replace(stop_name, '\\s+', ' ', 'g'))
        || ':' || round(stop_lat::numeric, 4)::text
        || ':' || round(stop_lon::numeric, 4)::text
    )
)
"""

DAY_COLUMNS = {
    0: "monday",
    1: "tuesday",
    2: "wednesday",
    3: "thursday",
    4: "friday",
    5: "saturday",
    6: "sunday",
}

ROUTE_TYPE_LABELS = {
    0: "tram",
    1: "metro",
    2: "rail",
    3: "bus",
    4: "ferry",
    5: "cable tram",
    6: "aerial lift",
    7: "funicular",
    11: "trolleybus",
    12: "monorail",
}


app = FastAPI(
    title="Bucharest Transit Trip Planner",
    description="GTFS-based stop map and departure-time trip planner.",
    version="0.1.0",
)


@dataclass(frozen=True)
class ServiceWindow:
    service_date: date
    earliest_departure_seconds: int
    service_ids: list[str]


@dataclass(frozen=True)
class Endpoint:
    id: str
    kind: str
    name: str
    lat: float | None
    lon: float | None
    stop_ids: list[str]


def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(settings.postgres.dsn)


def _parse_departure(value: str | None) -> datetime:
    if not value:
        return datetime.now(BUCHAREST_TZ).replace(second=0, microsecond=0)

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="departure_at must be an ISO datetime, e.g. 2026-05-20T23:30",
        ) from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=BUCHAREST_TZ)
    return parsed.astimezone(BUCHAREST_TZ)


def _seconds_since_midnight(value: datetime) -> int:
    return value.hour * 3600 + value.minute * 60 + value.second


def _active_services(conn: psycopg2.extensions.connection, service_date: date) -> list[str]:
    day_column = DAY_COLUMNS[service_date.weekday()]
    query = f"""
        SELECT service_id
        FROM transit.calendar
        WHERE start_date <= %s
          AND end_date >= %s
          AND {day_column} = 1
        ORDER BY service_id
    """
    with conn.cursor() as cur:
        cur.execute(query, (service_date, service_date))
        return [row[0] for row in cur.fetchall()]


def _service_windows(
    conn: psycopg2.extensions.connection, departure_at: datetime
) -> list[ServiceWindow]:
    departure_seconds = _seconds_since_midnight(departure_at)
    today = departure_at.date()
    yesterday = today - timedelta(days=1)

    windows: list[ServiceWindow] = []
    today_services = _active_services(conn, today)
    if today_services:
        windows.append(ServiceWindow(today, departure_seconds, today_services))

    previous_services = _active_services(conn, yesterday)
    if previous_services:
        windows.append(ServiceWindow(yesterday, departure_seconds + 24 * 3600, previous_services))

    return windows


def _seconds_to_datetime(service_date: date, seconds: float | int) -> datetime:
    return datetime.combine(service_date, time.min, tzinfo=BUCHAREST_TZ) + timedelta(
        seconds=int(seconds)
    )


def _datetime_to_service_seconds(value: datetime, service_date: date) -> int:
    start = datetime.combine(service_date, time.min, tzinfo=BUCHAREST_TZ)
    return int((value - start).total_seconds())


def _minutes_between(start: datetime, end: datetime) -> int:
    return max(0, math.ceil((end - start).total_seconds() / 60))


def _route_type_label(route_type: int | None) -> str:
    if route_type is None:
        return "unknown"
    return ROUTE_TYPE_LABELS.get(route_type, f"type {route_type}")


def _row_to_stop(row: dict[str, Any], prefix: str) -> dict[str, Any]:
    return {
        "stop_id": row[f"{prefix}_stop_id"],
        "name": row[f"{prefix}_stop_name"],
        "lat": row[f"{prefix}_lat"],
        "lon": row[f"{prefix}_lon"],
    }


@lru_cache(maxsize=512)
def _shape_points(shape_id: str) -> tuple[tuple[float, float], ...]:
    query = """
        SELECT shape_pt_lat, shape_pt_lon
        FROM transit.shapes
        WHERE shape_id = %s
        ORDER BY shape_pt_sequence
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(query, (shape_id,))
        return tuple((float(lat), float(lon)) for lat, lon in cur.fetchall())


def _distance_meters(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    return math.sqrt(
        ((a_lat - b_lat) * 111_320) ** 2
        + ((a_lon - b_lon) * 111_320 * math.cos(math.radians(a_lat))) ** 2
    )


def _nearest_shape_index(points: tuple[tuple[float, float], ...], lat: float, lon: float) -> int:
    return min(
        range(len(points)),
        key=lambda idx: _distance_meters(lat, lon, points[idx][0], points[idx][1]),
    )


def _leg_geometry(
    row: dict[str, Any], prefix: str, from_prefix: str, to_prefix: str
) -> list[list[float]]:
    start = [row[f"{from_prefix}_lat"], row[f"{from_prefix}_lon"]]
    end = [row[f"{to_prefix}_lat"], row[f"{to_prefix}_lon"]]
    shape_id = row.get(f"{prefix}_shape_id")
    if not shape_id:
        return [start, end]

    points = _shape_points(shape_id)
    if len(points) < 2:
        return [start, end]

    start_index = _nearest_shape_index(points, start[0], start[1])
    end_index = _nearest_shape_index(points, end[0], end[1])
    if start_index <= end_index:
        segment = points[start_index : end_index + 1]
    else:
        segment = tuple(reversed(points[end_index : start_index + 1]))

    geometry = [start, *([list(point) for point in segment]), end]
    deduped: list[list[float]] = []
    for point in geometry:
        if not deduped or point != deduped[-1]:
            deduped.append(point)
    return deduped


def _leg(
    row: dict[str, Any],
    *,
    prefix: str,
    from_prefix: str,
    to_prefix: str,
    service_date: date,
) -> dict[str, Any]:
    departure = _seconds_to_datetime(service_date, row[f"{prefix}_departure_seconds"])
    arrival = _seconds_to_datetime(service_date, row[f"{prefix}_arrival_seconds"])
    route_type = row.get(f"{prefix}_route_type")
    return {
        "mode": _route_type_label(route_type),
        "route_id": row[f"{prefix}_route_id"],
        "direction_id": row.get(f"{prefix}_direction_id"),
        "route_short_name": row.get(f"{prefix}_route_short_name") or row[f"{prefix}_route_id"],
        "headsign": row.get(f"{prefix}_headsign"),
        "trip_id": row[f"{prefix}_trip_id"],
        "from": _row_to_stop(row, from_prefix),
        "to": _row_to_stop(row, to_prefix),
        "departure_time": departure.isoformat(),
        "arrival_time": arrival.isoformat(),
        "duration_minutes": _minutes_between(departure, arrival),
        "geometry": _leg_geometry(row, prefix, from_prefix, to_prefix),
    }


def _place_stop_ids(conn: psycopg2.extensions.connection, place_id: str) -> list[str]:
    cached = next((place for place in _stops_cache() if place["place_id"] == place_id), None)
    if cached:
        return list(cached["stop_ids"])

    query = f"""
        SELECT ARRAY_AGG(stop_id ORDER BY stop_id)
        FROM transit.stops
        WHERE ({STOP_PLACE_ID_SQL}) = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (place_id,))
        row = cur.fetchone()
    if not row or not row[0]:
        raise HTTPException(status_code=404, detail=f"Unknown stop/place id: {place_id}")
    return list(row[0])


def _stop_place_by_id(place_id: str) -> dict[str, Any] | None:
    return next((stop for stop in _stops_cache() if stop["place_id"] == place_id), None)


def _parse_location_id(value: str) -> tuple[float, float] | None:
    if not value.startswith("loc:"):
        return None
    try:
        raw_lat, raw_lon = value[4:].split(",", 1)
        lat = float(raw_lat)
        lon = float(raw_lon)
    except ValueError:
        return None
    if not (44.0 <= lat <= 45.0 and 25.5 <= lon <= 26.8):
        return None
    return lat, lon


def _nearest_stop_places(lat: float, lon: float) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for stop in _stops_cache():
        distance = _distance_meters(lat, lon, stop["lat"], stop["lon"])
        if distance <= LOCATION_STOP_SEARCH_METERS:
            candidate = dict(stop)
            candidate["walk_meters"] = distance
            candidate["walk_seconds"] = int(math.ceil(distance / WALKING_SPEED_MPS))
            candidates.append(candidate)

    candidates.sort(key=lambda stop: stop["walk_meters"])
    return candidates[:LOCATION_STOP_LIMIT]


def _resolve_endpoint(value: str, label: str) -> Endpoint:
    if location := _parse_location_id(value):
        lat, lon = location
        nearby = _nearest_stop_places(lat, lon)
        if not nearby:
            raise HTTPException(
                status_code=404,
                detail=f"No transit stops were found close enough to {label}.",
            )
        stop_ids = sorted({stop_id for stop in nearby for stop_id in stop["stop_ids"]})
        return Endpoint(
            id=value,
            kind="location",
            name="Selected map location",
            lat=lat,
            lon=lon,
            stop_ids=stop_ids,
        )

    stop = _stop_place_by_id(value)
    if stop is None:
        raise HTTPException(status_code=404, detail=f"The stop for {label} does not exist.")
    return Endpoint(
        id=stop["place_id"],
        kind="stop",
        name=stop["name"],
        lat=stop["lat"],
        lon=stop["lon"],
        stop_ids=list(stop["stop_ids"]),
    )


def _endpoint_walk_to_stop(endpoint: Endpoint, stop: dict[str, Any]) -> dict[str, Any] | None:
    if endpoint.kind != "location" or endpoint.lat is None or endpoint.lon is None:
        return None
    distance = _distance_meters(endpoint.lat, endpoint.lon, stop["lat"], stop["lon"])
    return {
        "meters": distance,
        "seconds": int(math.ceil(distance / WALKING_SPEED_MPS)),
        "lat": endpoint.lat,
        "lon": endpoint.lon,
        "name": endpoint.name,
    }


@lru_cache(maxsize=1)
def _stops_cache() -> list[dict[str, Any]]:
    query = f"""
        SELECT
            ({STOP_PLACE_ID_SQL}) AS place_id,
            MIN(stop_name) AS name,
            AVG(stop_lat)::float AS lat,
            AVG(stop_lon)::float AS lon,
            ARRAY_AGG(stop_id ORDER BY stop_id) AS stop_ids,
            COUNT(*) AS stop_count,
            ARRAY_AGG(DISTINCT stop_name ORDER BY stop_name) AS names
        FROM transit.stops
        WHERE COALESCE(location_type, 0) = 0
        GROUP BY place_id
        ORDER BY name
    """
    with _connect() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        return [
            {
                "place_id": row["place_id"],
                "name": row["name"],
                "lat": row["lat"],
                "lon": row["lon"],
                "stop_ids": row["stop_ids"],
                "stop_count": row["stop_count"],
                "names": row["names"],
            }
            for row in cur.fetchall()
        ]


def _search_direct(
    conn: psycopg2.extensions.connection,
    *,
    origin_stop_ids: list[str],
    destination_stop_ids: list[str],
    window: ServiceWindow,
    search_window_seconds: int,
    limit: int,
) -> list[dict[str, Any]]:
    latest_departure = window.earliest_departure_seconds + search_window_seconds
    query = """
        SELECT
            'direct' AS itinerary_type,
            t.trip_id AS leg1_trip_id,
            t.shape_id AS leg1_shape_id,
            t.trip_headsign AS leg1_headsign,
            t.route_id AS leg1_route_id,
            t.direction_id AS leg1_direction_id,
            r.route_short_name AS leg1_route_short_name,
            r.route_type AS leg1_route_type,
            o.stop_id AS origin_stop_id,
            os.stop_name AS origin_stop_name,
            os.stop_lat AS origin_lat,
            os.stop_lon AS origin_lon,
            d.stop_id AS destination_stop_id,
            ds.stop_name AS destination_stop_name,
            ds.stop_lat AS destination_lat,
            ds.stop_lon AS destination_lon,
            EXTRACT(EPOCH FROM COALESCE(o.departure_time, o.arrival_time))::int
                AS leg1_departure_seconds,
            EXTRACT(EPOCH FROM COALESCE(d.arrival_time, d.departure_time))::int
                AS leg1_arrival_seconds
        FROM transit.stop_times o
        JOIN transit.trips t ON t.trip_id = o.trip_id
        JOIN transit.routes r ON r.route_id = t.route_id
        JOIN transit.stop_times d
          ON d.trip_id = o.trip_id
         AND d.stop_sequence > o.stop_sequence
        JOIN transit.stops os ON os.stop_id = o.stop_id
        JOIN transit.stops ds ON ds.stop_id = d.stop_id
        WHERE o.stop_id = ANY(%s)
          AND d.stop_id = ANY(%s)
          AND t.service_id = ANY(%s)
          AND COALESCE(o.departure_time, o.arrival_time) IS NOT NULL
          AND COALESCE(d.arrival_time, d.departure_time) IS NOT NULL
          AND EXTRACT(EPOCH FROM COALESCE(o.departure_time, o.arrival_time))
              BETWEEN %s AND %s
        ORDER BY leg1_arrival_seconds, leg1_departure_seconds
        LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            query,
            (
                origin_stop_ids,
                destination_stop_ids,
                window.service_ids,
                window.earliest_departure_seconds,
                latest_departure,
                limit,
            ),
        )
        return [dict(row) | {"service_date": window.service_date} for row in cur.fetchall()]


def _search_one_transfer(
    conn: psycopg2.extensions.connection,
    *,
    origin_stop_ids: list[str],
    destination_stop_ids: list[str],
    window: ServiceWindow,
    search_window_seconds: int,
    min_transfer_seconds: int,
    max_transfer_wait_seconds: int,
    transfer_walk_meters: int,
    limit: int,
) -> list[dict[str, Any]]:
    latest_departure = window.earliest_departure_seconds + search_window_seconds
    query = """
        WITH first_legs AS MATERIALIZED (
            SELECT
                t.trip_id AS leg1_trip_id,
                t.shape_id AS leg1_shape_id,
                t.trip_headsign AS leg1_headsign,
                t.route_id AS leg1_route_id,
                t.direction_id AS leg1_direction_id,
                r.route_short_name AS leg1_route_short_name,
                r.route_type AS leg1_route_type,
                o.stop_id AS origin_stop_id,
                os.stop_name AS origin_stop_name,
                os.stop_lat AS origin_lat,
                os.stop_lon AS origin_lon,
                x.stop_id AS transfer_from_stop_id,
                xs.stop_name AS transfer_from_stop_name,
                xs.stop_lat AS transfer_from_lat,
                xs.stop_lon AS transfer_from_lon,
                EXTRACT(EPOCH FROM COALESCE(o.departure_time, o.arrival_time))::int
                    AS leg1_departure_seconds,
                EXTRACT(EPOCH FROM COALESCE(x.arrival_time, x.departure_time))::int
                    AS leg1_arrival_seconds
            FROM transit.stop_times o
            JOIN transit.trips t ON t.trip_id = o.trip_id
            JOIN transit.routes r ON r.route_id = t.route_id
            JOIN transit.stop_times x
              ON x.trip_id = o.trip_id
             AND x.stop_sequence > o.stop_sequence
            JOIN transit.stops os ON os.stop_id = o.stop_id
            JOIN transit.stops xs ON xs.stop_id = x.stop_id
            WHERE o.stop_id = ANY(%s)
              AND t.service_id = ANY(%s)
              AND COALESCE(o.departure_time, o.arrival_time) IS NOT NULL
              AND COALESCE(x.arrival_time, x.departure_time) IS NOT NULL
              AND EXTRACT(EPOCH FROM COALESCE(o.departure_time, o.arrival_time))
                  BETWEEN %s AND %s
            ORDER BY leg1_arrival_seconds
            LIMIT %s
        )
        SELECT
            'one_transfer' AS itinerary_type,
            fl.*,
            t2.trip_id AS leg2_trip_id,
            t2.shape_id AS leg2_shape_id,
            t2.trip_headsign AS leg2_headsign,
            t2.route_id AS leg2_route_id,
            t2.direction_id AS leg2_direction_id,
            r2.route_short_name AS leg2_route_short_name,
            r2.route_type AS leg2_route_type,
            b.stop_id AS transfer_to_stop_id,
            bs.stop_name AS transfer_to_stop_name,
            bs.stop_lat AS transfer_to_lat,
            bs.stop_lon AS transfer_to_lon,
            d.stop_id AS destination_stop_id,
            ds.stop_name AS destination_stop_name,
            ds.stop_lat AS destination_lat,
            ds.stop_lon AS destination_lon,
            EXTRACT(EPOCH FROM COALESCE(b.departure_time, b.arrival_time))::int
                AS leg2_departure_seconds,
            EXTRACT(EPOCH FROM COALESCE(d.arrival_time, d.departure_time))::int
                AS leg2_arrival_seconds,
            SQRT(
                POWER((bs.stop_lat - fl.transfer_from_lat) * 111320, 2)
                + POWER(
                    (bs.stop_lon - fl.transfer_from_lon)
                    * 111320
                    * COS(RADIANS(fl.transfer_from_lat)),
                    2
                )
            )::float AS transfer_walk_meters
        FROM first_legs fl
        JOIN transit.stops bs
          ON SQRT(
                POWER((bs.stop_lat - fl.transfer_from_lat) * 111320, 2)
                + POWER(
                    (bs.stop_lon - fl.transfer_from_lon)
                    * 111320
                    * COS(RADIANS(fl.transfer_from_lat)),
                    2
                )
             ) <= %s
        JOIN transit.stop_times b ON b.stop_id = bs.stop_id
        JOIN transit.trips t2 ON t2.trip_id = b.trip_id
        JOIN transit.routes r2 ON r2.route_id = t2.route_id
        JOIN transit.stop_times d
          ON d.trip_id = b.trip_id
         AND d.stop_sequence > b.stop_sequence
        JOIN transit.stops ds ON ds.stop_id = d.stop_id
        WHERE d.stop_id = ANY(%s)
          AND t2.service_id = ANY(%s)
          AND t2.trip_id <> fl.leg1_trip_id
          AND COALESCE(b.departure_time, b.arrival_time) IS NOT NULL
          AND COALESCE(d.arrival_time, d.departure_time) IS NOT NULL
          AND EXTRACT(EPOCH FROM COALESCE(b.departure_time, b.arrival_time))
                >= fl.leg1_arrival_seconds + %s
          AND EXTRACT(EPOCH FROM COALESCE(b.departure_time, b.arrival_time))
                <= fl.leg1_arrival_seconds + %s
        ORDER BY leg2_arrival_seconds, leg1_departure_seconds
        LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            query,
            (
                origin_stop_ids,
                window.service_ids,
                window.earliest_departure_seconds,
                latest_departure,
                MAX_FIRST_LEG_CANDIDATES,
                transfer_walk_meters,
                destination_stop_ids,
                window.service_ids,
                min_transfer_seconds,
                min_transfer_seconds + max_transfer_wait_seconds,
                limit,
            ),
        )
        return [dict(row) | {"service_date": window.service_date} for row in cur.fetchall()]


def _walk_leg(
    *,
    start: dict[str, Any],
    end: dict[str, Any],
    departure: datetime,
    meters: float,
) -> dict[str, Any]:
    seconds = int(math.ceil(meters / WALKING_SPEED_MPS))
    arrival = departure + timedelta(seconds=seconds)
    return {
        "mode": "walk",
        "from": start,
        "to": end,
        "departure_time": departure.isoformat(),
        "arrival_time": arrival.isoformat(),
        "duration_minutes": max(1, math.ceil(seconds / 60)),
        "distance_meters": round(meters),
        "geometry": [[start["lat"], start["lon"]], [end["lat"], end["lon"]]],
    }


def _endpoint_point(endpoint: Endpoint) -> dict[str, Any]:
    if endpoint.lat is None or endpoint.lon is None:
        raise HTTPException(status_code=400, detail=f"Endpoint without coordinates: {endpoint.id}")
    return {
        "stop_id": endpoint.id,
        "name": endpoint.name,
        "lat": endpoint.lat,
        "lon": endpoint.lon,
    }


def _walking_only_itinerary(
    origin: Endpoint,
    destination: Endpoint,
    requested_departure: datetime,
) -> dict[str, Any]:
    start = _endpoint_point(origin)
    end = _endpoint_point(destination)
    meters = _distance_meters(start["lat"], start["lon"], end["lat"], end["lon"])
    leg = _walk_leg(start=start, end=end, departure=requested_departure, meters=meters)
    arrival = datetime.fromisoformat(leg["arrival_time"])
    total_minutes = _minutes_between(requested_departure, arrival)
    leg["expected_departure_time"] = leg["departure_time"]
    leg["expected_arrival_time"] = leg["arrival_time"]
    return {
        "type": "walk",
        "departure_time": requested_departure.isoformat(),
        "arrival_time": arrival.isoformat(),
        "expected_arrival_time": arrival.isoformat(),
        "total_minutes": total_minutes,
        "expected_total_minutes": total_minutes,
        "expected_delay_minutes": 0,
        "waiting_minutes": 0,
        "transfers": 0,
        "legs": [leg],
        "prediction_sources": ["walking"],
    }


def _should_include_walking_option(
    walk_itinerary: dict[str, Any],
    transit_itineraries: list[dict[str, Any]],
) -> bool:
    walk_minutes = int(walk_itinerary["expected_total_minutes"])
    if not transit_itineraries:
        return walk_minutes <= MAX_WALK_ONLY_MINUTES
    best_transit_minutes = min(
        int(itinerary.get("expected_total_minutes", itinerary["total_minutes"]))
        for itinerary in transit_itineraries
    )
    if walk_minutes <= WALK_FIRST_MAX_MINUTES:
        return walk_minutes <= best_transit_minutes
    return best_transit_minutes - walk_minutes >= WALK_OVER_TRANSIT_GAIN_MINUTES


def _build_itinerary(
    row: dict[str, Any],
    requested_departure: datetime,
    *,
    origin: Endpoint,
    destination: Endpoint,
) -> dict[str, Any] | None:
    service_date = row["service_date"]
    if row["itinerary_type"] == "direct":
        legs = [
            _leg(
                row,
                prefix="leg1",
                from_prefix="origin",
                to_prefix="destination",
                service_date=service_date,
            )
        ]
    else:
        first_leg = _leg(
            row,
            prefix="leg1",
            from_prefix="origin",
            to_prefix="transfer_from",
            service_date=service_date,
        )
        second_leg = _leg(
            row,
            prefix="leg2",
            from_prefix="transfer_to",
            to_prefix="destination",
            service_date=service_date,
        )
        transfer_departure = datetime.fromisoformat(first_leg["arrival_time"])
        legs = [
            first_leg,
            _walk_leg(
                start=first_leg["to"],
                end=second_leg["from"],
                departure=transfer_departure,
                meters=row.get("transfer_walk_meters") or 0,
            ),
            second_leg,
        ]

    first_transit_leg = next(leg for leg in legs if leg["mode"] != "walk")
    origin_walk = _endpoint_walk_to_stop(origin, first_transit_leg["from"])
    if origin_walk:
        first_transit_departure = datetime.fromisoformat(first_transit_leg["departure_time"])
        walk_arrival = requested_departure + timedelta(seconds=origin_walk["seconds"])
        if first_transit_departure < walk_arrival:
            return None
        legs.insert(
            0,
            _walk_leg(
                start={
                    "stop_id": origin.id,
                    "name": origin.name,
                    "lat": origin_walk["lat"],
                    "lon": origin_walk["lon"],
                },
                end=first_transit_leg["from"],
                departure=requested_departure,
                meters=origin_walk["meters"],
            ),
        )

    last_transit_leg = next(leg for leg in reversed(legs) if leg["mode"] != "walk")
    destination_walk = _endpoint_walk_to_stop(destination, last_transit_leg["to"])
    if destination_walk:
        legs.append(
            _walk_leg(
                start=last_transit_leg["to"],
                end={
                    "stop_id": destination.id,
                    "name": destination.name,
                    "lat": destination_walk["lat"],
                    "lon": destination_walk["lon"],
                },
                departure=datetime.fromisoformat(last_transit_leg["arrival_time"]),
                meters=destination_walk["meters"],
            )
        )

    first_departure = datetime.fromisoformat(legs[0]["departure_time"])
    final_arrival = datetime.fromisoformat(legs[-1]["arrival_time"])
    return {
        "type": row["itinerary_type"],
        "departure_time": first_departure.isoformat(),
        "arrival_time": final_arrival.isoformat(),
        "total_minutes": _minutes_between(requested_departure, final_arrival),
        "waiting_minutes": _minutes_between(requested_departure, first_departure),
        "transfers": 0 if row["itinerary_type"] == "direct" else 1,
        "legs": legs,
    }


def _clamp_delay(seconds: float | None) -> int:
    if seconds is None or not math.isfinite(seconds):
        return 0
    return round(max(-120, min(1_800, seconds)))


def _estimate_leg_delay(
    conn: psycopg2.extensions.connection,
    leg: dict[str, Any],
) -> dict[str, Any]:
    route_id = leg.get("route_id")
    if not route_id:
        return {"seconds": 0, "source": "none", "confidence": None}

    direction_id = leg.get("direction_id")
    target_time = datetime.fromisoformat(leg["departure_time"])
    dow = (target_time.weekday() + 1) % 7

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT predicted_delay_sec, confidence, risk_level, model_version,
                   prediction_lower_sec, prediction_upper_sec
            FROM transit.route_delay_predictions
            WHERE route_id = %s
              AND (direction_id = %s OR %s IS NULL OR direction_id IS NULL)
              AND target_time BETWEEN %s::timestamptz - INTERVAL '90 minutes'
                                  AND %s::timestamptz + INTERVAL '90 minutes'
            ORDER BY ABS(EXTRACT(EPOCH FROM (target_time - %s::timestamptz))),
                     prediction_time DESC
            LIMIT 1
            """,
            (route_id, direction_id, direction_id, target_time, target_time, target_time),
        )
        row = cur.fetchone()
        if row:
            return {
                "seconds": _clamp_delay(row["predicted_delay_sec"]),
                "source": "model_prediction",
                "confidence": row["confidence"],
                "risk_level": row["risk_level"],
                "model_version": row["model_version"],
                "range_seconds": [
                    _clamp_delay(row["prediction_lower_sec"]),
                    _clamp_delay(row["prediction_upper_sec"]),
                ],
            }

        cur.execute(
            """
            SELECT AVG(avg_delay_sec) AS delay_sec, SUM(observation_count) AS observations
            FROM transit.realtime_feature_windows
            WHERE route_id = %s
              AND (direction_id = %s OR %s IS NULL OR direction_id IS NULL)
              AND hour_of_day = %s
              AND day_of_week = %s
              AND avg_delay_sec IS NOT NULL
              AND feature_time >= NOW() - INTERVAL '60 days'
            """,
            (route_id, direction_id, direction_id, target_time.hour, dow),
        )
        row = cur.fetchone()
        if row and row["delay_sec"] is not None:
            observations = int(row["observations"] or 0)
            return {
                "seconds": _clamp_delay(row["delay_sec"]),
                "source": "historical_feature_windows",
                "confidence": min(0.85, 0.35 + observations / 200),
                "observations": observations,
            }

        cur.execute(
            """
            SELECT AVG(delay_sec) AS delay_sec, COUNT(*) AS observations
            FROM transit.stop_delay_events
            WHERE route_id = %s
              AND (direction_id = %s OR %s IS NULL OR direction_id IS NULL)
              AND EXTRACT(HOUR FROM scheduled_time) = %s
              AND EXTRACT(DOW FROM scheduled_time) = %s
              AND observed_time >= NOW() - INTERVAL '60 days'
            """,
            (route_id, direction_id, direction_id, target_time.hour, dow),
        )
        row = cur.fetchone()
        if row and row["delay_sec"] is not None:
            observations = int(row["observations"] or 0)
            return {
                "seconds": _clamp_delay(row["delay_sec"]),
                "source": "historical_stop_events",
                "confidence": min(0.7, 0.25 + observations / 300),
                "observations": observations,
            }

    return {"seconds": 0, "source": "static_schedule", "confidence": None}


def _add_seconds_to_iso(value: str, seconds: int) -> str:
    return (datetime.fromisoformat(value) + timedelta(seconds=seconds)).isoformat()


def _apply_expected_delays(
    conn: psycopg2.extensions.connection,
    itineraries: list[dict[str, Any]],
    requested_departure: datetime,
) -> list[dict[str, Any]]:
    for itinerary in itineraries:
        cumulative_delay = 0
        sources: set[str] = set()
        for leg in itinerary["legs"]:
            if leg["mode"] == "walk":
                leg["expected_departure_time"] = _add_seconds_to_iso(
                    leg["departure_time"], cumulative_delay
                )
                leg["expected_arrival_time"] = _add_seconds_to_iso(
                    leg["arrival_time"], cumulative_delay
                )
                continue

            estimate = _estimate_leg_delay(conn, leg)
            delay_seconds = int(estimate["seconds"])
            cumulative_delay += delay_seconds
            sources.add(estimate["source"])
            leg["expected_delay_seconds"] = delay_seconds
            leg["prediction"] = estimate
            leg["expected_departure_time"] = _add_seconds_to_iso(
                leg["departure_time"], cumulative_delay - delay_seconds
            )
            leg["expected_arrival_time"] = _add_seconds_to_iso(
                leg["arrival_time"], cumulative_delay
            )

        expected_arrival = datetime.fromisoformat(itinerary["legs"][-1]["expected_arrival_time"])
        scheduled_arrival = datetime.fromisoformat(itinerary["arrival_time"])
        itinerary["expected_arrival_time"] = expected_arrival.isoformat()
        itinerary["expected_total_minutes"] = _minutes_between(
            requested_departure, expected_arrival
        )
        itinerary["expected_delay_minutes"] = round(
            (expected_arrival - scheduled_arrival).total_seconds() / 60
        )
        itinerary["prediction_sources"] = sorted(sources) or ["static_schedule"]
    return itineraries


def _transit_signature(itinerary: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            leg.get("mode"),
            leg.get("route_id"),
            leg.get("direction_id"),
            leg.get("route_short_name"),
        )
        for leg in itinerary["legs"]
        if leg.get("mode") != "walk"
    )


def _itinerary_minutes(itinerary: dict[str, Any]) -> int:
    return int(itinerary.get("expected_total_minutes", itinerary["total_minutes"]))


def _access_egress_walk_minutes(itinerary: dict[str, Any]) -> int:
    legs = itinerary["legs"]
    if len(legs) <= 1:
        return 0

    minutes = 0
    if legs[0].get("mode") == "walk":
        minutes += int(legs[0].get("duration_minutes") or 0)
    if legs[-1].get("mode") == "walk":
        minutes += int(legs[-1].get("duration_minutes") or 0)
    return minutes


def _soft_filter_itineraries(itineraries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_signature: dict[tuple[tuple[Any, ...], ...], list[dict[str, Any]]] = {}
    passthrough: list[dict[str, Any]] = []

    for itinerary in itineraries:
        signature = _transit_signature(itinerary)
        if not signature:
            passthrough.append(itinerary)
            continue
        by_signature.setdefault(signature, []).append(itinerary)

    filtered = list(passthrough)
    for group in by_signature.values():
        lightest_walk = min(_access_egress_walk_minutes(itinerary) for itinerary in group)
        lightest_walk_total = min(
            _itinerary_minutes(itinerary)
            for itinerary in group
            if _access_egress_walk_minutes(itinerary) == lightest_walk
        )

        for itinerary in group:
            extra_walk = _access_egress_walk_minutes(itinerary) - lightest_walk
            time_gain = lightest_walk_total - _itinerary_minutes(itinerary)
            if (
                extra_walk <= ROUTE_FILTER_WALK_SLACK_MINUTES
                or time_gain >= ROUTE_FILTER_TIME_GAIN_MINUTES
            ):
                filtered.append(itinerary)

    return filtered or itineraries


def _dedupe_and_sort_itineraries(itineraries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    unique: list[dict[str, Any]] = []
    for itinerary in sorted(
        itineraries,
        key=lambda item: (
            item.get("expected_arrival_time", item["arrival_time"]),
            item["transfers"],
            item["departure_time"],
        ),
    ):
        signature = tuple(
            (
                leg.get("mode"),
                leg.get("trip_id"),
                leg.get("from", {}).get("stop_id"),
                leg.get("to", {}).get("stop_id"),
            )
            for leg in itinerary["legs"]
        )
        if signature in seen:
            continue
        seen.add(signature)
        unique.append(itinerary)
        if len(unique) >= MAX_ITINERARIES:
            break
    return unique


def _web_mercator_pixels(lat: float, lon: float, zoom: int) -> tuple[float, float]:
    sin_lat = math.sin(math.radians(max(-85.05112878, min(85.05112878, lat))))
    scale = 256 * (2**zoom)
    x = (lon + 180) / 360 * scale
    y = (0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)) * scale
    return x, y


def _map_cluster_radius(zoom: int) -> int:
    if zoom <= 11:
        return 96
    if zoom <= 13:
        return 78
    if zoom <= 15:
        return 58
    return 40


def _stop_feature(stop: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "stop",
        "id": stop["place_id"],
        "place_id": stop["place_id"],
        "name": stop["name"],
        "lat": stop["lat"],
        "lon": stop["lon"],
        "stop_count": stop["stop_count"],
        "stop_ids": stop["stop_ids"],
        "names": stop["names"],
    }


def _parse_bbox(value: str) -> tuple[float, float, float, float]:
    try:
        south, west, north, east = [float(part) for part in value.split(",")]
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="bbox must be south,west,north,east",
        ) from exc

    if south > north:
        south, north = north, south
    valid_lat = -90 <= south <= 90 and -90 <= north <= 90
    valid_lon = -180 <= west <= 180 and -180 <= east <= 180
    if not (valid_lat and valid_lon):
        raise HTTPException(
            status_code=400,
            detail="bbox is outside valid latitude/longitude range",
        )
    return south, west, north, east


def _cluster_map_stops(stops: list[dict[str, Any]], zoom: int) -> list[dict[str, Any]]:
    if zoom >= 17 or len(stops) <= 120:
        return [_stop_feature(stop) for stop in stops]

    radius = _map_cluster_radius(zoom)
    buckets: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for stop in stops:
        x, y = _web_mercator_pixels(stop["lat"], stop["lon"], zoom)
        buckets.setdefault((int(x // radius), int(y // radius)), []).append(stop)

    features: list[dict[str, Any]] = []
    for (cell_x, cell_y), bucket in buckets.items():
        if len(bucket) == 1:
            features.append(_stop_feature(bucket[0]))
            continue
        lat = sum(stop["lat"] for stop in bucket) / len(bucket)
        lon = sum(stop["lon"] for stop in bucket) / len(bucket)
        count = sum(int(stop["stop_count"]) for stop in bucket)
        features.append(
            {
                "type": "cluster",
                "id": f"cluster:{zoom}:{cell_x}:{cell_y}",
                "lat": lat,
                "lon": lon,
                "count": count,
                "place_count": len(bucket),
            }
        )
    return features


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return HTML


@app.get("/healthz")
def healthz() -> dict[str, str]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/api/stops")
def stops(
    q: str | None = Query(default=None, max_length=80),
    limit: int = Query(default=80, ge=1, le=5000),
) -> dict[str, Any]:
    places = _stops_cache()
    if q:
        needle = q.casefold()
        places = [
            place
            for place in places
            if needle in place["name"].casefold()
            or any(needle in stop_id.casefold() for stop_id in place["stop_ids"])
            or any(needle in name.casefold() for name in place.get("names", []))
        ]
    return {"count": len(places), "stops": places[:limit]}


@app.get("/api/map-stops")
def map_stops(
    bbox: str = Query(..., description="south,west,north,east"),
    zoom: int = Query(..., ge=8, le=20),
) -> dict[str, Any]:
    south, west, north, east = _parse_bbox(bbox)
    stops_in_view = [
        stop
        for stop in _stops_cache()
        if south <= stop["lat"] <= north and west <= stop["lon"] <= east
    ]
    features = _cluster_map_stops(stops_in_view, zoom)
    return {
        "count": len(stops_in_view),
        "feature_count": len(features),
        "features": features,
    }


@app.get("/api/plan")
def plan(
    origin: str,
    destination: str,
    departure_at: str | None = None,
    max_transfers: int = Query(default=1, ge=0, le=1),
) -> dict[str, Any]:
    if origin == destination:
        raise HTTPException(status_code=400, detail="Origin and destination must be different.")

    requested_departure = _parse_departure(departure_at)
    with _connect() as conn:
        origin_endpoint = _resolve_endpoint(origin, "origin")
        destination_endpoint = _resolve_endpoint(destination, "destination")
        same_stop_set = set(origin_endpoint.stop_ids) == set(destination_endpoint.stop_ids)
        walk_itinerary = _walking_only_itinerary(
            origin_endpoint, destination_endpoint, requested_departure
        )

        windows = _service_windows(conn, requested_departure)

        if not windows:
            if _should_include_walking_option(walk_itinerary, []):
                itineraries = [walk_itinerary]
            else:
                raise HTTPException(
                    status_code=404,
                    detail="No active GTFS service for the selected date.",
                )
        else:
            rows: list[dict[str, Any]] = []
            if not same_stop_set:
                for window in windows:
                    rows.extend(
                        _search_direct(
                            conn,
                            origin_stop_ids=origin_endpoint.stop_ids,
                            destination_stop_ids=destination_endpoint.stop_ids,
                            window=window,
                            search_window_seconds=DEFAULT_SEARCH_WINDOW_SECONDS,
                            limit=CANDIDATE_ITINERARY_LIMIT,
                        )
                    )
                    if max_transfers >= 1:
                        rows.extend(
                            _search_one_transfer(
                                conn,
                                origin_stop_ids=origin_endpoint.stop_ids,
                                destination_stop_ids=destination_endpoint.stop_ids,
                                window=window,
                                search_window_seconds=DEFAULT_SEARCH_WINDOW_SECONDS,
                                min_transfer_seconds=DEFAULT_MIN_TRANSFER_SECONDS,
                                max_transfer_wait_seconds=DEFAULT_TRANSFER_WAIT_SECONDS,
                                transfer_walk_meters=DEFAULT_TRANSFER_WALK_METERS,
                                limit=CANDIDATE_ITINERARY_LIMIT,
                            )
                        )

            built_itineraries = [
                itinerary
                for row in rows
                if (
                    itinerary := _build_itinerary(
                        row,
                        requested_departure,
                        origin=origin_endpoint,
                        destination=destination_endpoint,
                    )
                )
                is not None
            ]
            candidate_itineraries = _apply_expected_delays(
                conn, built_itineraries, requested_departure
            )
            candidate_itineraries = _soft_filter_itineraries(candidate_itineraries)
            if _should_include_walking_option(walk_itinerary, candidate_itineraries):
                candidate_itineraries.append(walk_itinerary)
            itineraries = _dedupe_and_sort_itineraries(candidate_itineraries)

    return {
        "requested_departure": requested_departure.isoformat(),
        "origin": {
            "id": origin_endpoint.id,
            "kind": origin_endpoint.kind,
            "name": origin_endpoint.name,
            "lat": origin_endpoint.lat,
            "lon": origin_endpoint.lon,
        },
        "destination": {
            "id": destination_endpoint.id,
            "kind": destination_endpoint.kind,
            "name": destination_endpoint.name,
            "lat": destination_endpoint.lat,
            "lon": destination_endpoint.lon,
        },
        "count": len(itineraries),
        "itineraries": itineraries,
        "notes": [
            "Starts from the GTFS schedule and adjusts ETAs with delay predictions when available.",
            "For map-selected locations, snaps to nearby stops and adds walking legs.",
            "When no prediction exists for the interval, uses recent history by route/hour/day.",
        ],
    }


HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Bucharest Transit Planner</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <style>
    :root {
      color-scheme: dark;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      --panel: rgba(15, 23, 42, 0.92);
      --panel-strong: rgba(2, 6, 23, 0.94);
      --line: rgba(148, 163, 184, 0.22);
      --text: #f8fafc;
      --muted: #9ca3af;
      --blue: #3b82f6;
      --green: #22c55e;
      --red: #fb7185;
      --orange: #f97316;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: #020617; color: var(--text); overflow: hidden; }
    #map { position: fixed; inset: 0; z-index: 1; background: #0f172a; }
    .shell {
      position: relative; z-index: 2; width: min(430px, calc(100vw - 24px));
      max-height: calc(100vh - 24px); margin: 12px; overflow: hidden auto;
      border: 1px solid var(--line); border-radius: 28px;
      background: linear-gradient(180deg, rgba(15,23,42,.96), rgba(2,6,23,.92));
      box-shadow: 0 24px 80px rgba(0,0,0,.45); backdrop-filter: blur(18px);
    }
    header { padding: 20px 20px 14px; border-bottom: 1px solid var(--line); }
    h1 { margin: 0; font-size: 23px; letter-spacing: -.04em; }
    .sub { margin: 6px 0 0; color: var(--muted); font-size: 13px; line-height: 1.45; }
    .content { padding: 16px; }
    .pick-mode {
      display: grid; grid-template-columns: 1fr 1fr; gap: 8px; padding: 4px;
      border: 1px solid var(--line); border-radius: 16px; background: rgba(15,23,42,.78);
    }
    .mode-btn {
      min-height: 46px; border: 0; border-radius: 12px; color: #cbd5e1; background: transparent;
      font-weight: 800; cursor: pointer;
    }
    .mode-btn.active[data-kind="origin"] { background: rgba(34,197,94,.18); color: #bbf7d0; }
    .mode-btn.active[data-kind="destination"] { background: rgba(251,113,133,.18); color: #ffe4e6; }
    label { display: block; margin: 14px 0 7px; color: #cbd5e1; font-size: 12px; font-weight: 800; text-transform: uppercase; letter-spacing: .08em; }
    input, button {
      width: 100%; border-radius: 16px; border: 1px solid var(--line);
      background: rgba(2,6,23,.82); color: var(--text); padding: 14px 15px; font-size: 15px;
      outline: none;
    }
    input:focus { border-color: rgba(96,165,250,.75); box-shadow: 0 0 0 4px rgba(59,130,246,.16); }
    .field-row { display: grid; grid-template-columns: 1fr 48px; gap: 8px; align-items: end; }
    .search-box { position: relative; }
    .suggestions {
      position: absolute; z-index: 5; left: 0; right: 0; top: calc(100% + 6px); max-height: 230px;
      overflow: auto; border: 1px solid var(--line); border-radius: 16px;
      background: rgba(2,6,23,.98); box-shadow: 0 18px 55px rgba(0,0,0,.42);
    }
    .suggestions:empty { display: none; }
    .suggestion {
      width: 100%; border: 0; border-radius: 0; padding: 12px 14px; text-align: left;
      background: transparent; color: #e5e7eb; cursor: pointer;
    }
    .suggestion:hover { background: rgba(59,130,246,.18); }
    .suggestion small { display: block; color: var(--muted); margin-top: 3px; }
    .icon-btn {
      min-height: 49px; padding: 0; display: grid; place-items: center; border-radius: 16px;
      border: 1px solid var(--line); background: rgba(15,23,42,.84); cursor: pointer; font-size: 20px;
    }
    .selected {
      margin-top: 8px; min-height: 34px; padding: 9px 11px; border-radius: 14px;
      background: rgba(15,23,42,.7); color: #dbeafe; font-size: 13px; border: 1px solid transparent;
    }
    .selected.origin { border-color: rgba(34,197,94,.26); }
    .selected.destination { border-color: rgba(251,113,133,.26); }
    .primary {
      margin-top: 16px; min-height: 54px; border: 0; border-radius: 18px;
      background: linear-gradient(135deg, #2563eb, #7c3aed); font-weight: 900; cursor: pointer;
      box-shadow: 0 14px 35px rgba(37,99,235,.35);
    }
    .primary:hover { filter: brightness(1.08); }
    .hint, .status { color: var(--muted); font-size: 13px; line-height: 1.45; }
    .status { margin-top: 12px; color: #fbbf24; min-height: 19px; }
    .itinerary {
      margin-top: 13px; border: 1px solid var(--line); border-radius: 20px; padding: 14px;
      background: rgba(15,23,42,.78); cursor: pointer; transition: transform .15s ease, border-color .15s ease;
    }
    .itinerary:hover { transform: translateY(-1px); border-color: rgba(96,165,250,.5); }
    .itinerary.active { border-color: rgba(249,115,22,.8); box-shadow: 0 0 0 3px rgba(249,115,22,.14); }
    .itinerary h3 { display: flex; align-items: baseline; justify-content: space-between; gap: 12px; margin: 0 0 6px; font-size: 18px; }
    .time { color: #bfdbfe; font-size: 13px; font-weight: 700; }
    .delay-panel {
      margin-top: 14px; border: 1px solid rgba(96,165,250,.28); border-radius: 20px; padding: 14px;
      background: linear-gradient(135deg, rgba(37,99,235,.16), rgba(124,58,237,.12));
    }
    .delay-panel h2 { margin: 0 0 8px; font-size: 15px; letter-spacing: -.02em; }
    .metric-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-top: 10px; }
    .metric {
      padding: 10px; border-radius: 14px; background: rgba(2,6,23,.45); border: 1px solid rgba(148,163,184,.14);
    }
    .metric b { display: block; font-size: 16px; color: #f8fafc; }
    .metric span { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .06em; }
    .source-chip {
      display: inline-flex; align-items: center; gap: 6px; margin: 6px 6px 0 0; padding: 6px 9px;
      border-radius: 999px; background: rgba(15,23,42,.72); color: #dbeafe; font-size: 12px; font-weight: 800;
      border: 1px solid rgba(147,197,253,.2);
    }
    .eta-row { display: flex; justify-content: space-between; gap: 10px; margin: 8px 0; font-size: 13px; }
    .eta-row b { color: #fef3c7; }
    .delay-positive { color: #fca5a5; }
    .delay-zero { color: #86efac; }
    .delay-negative { color: #93c5fd; }
    .leg { display: grid; grid-template-columns: 78px 1fr; gap: 10px; padding: 10px 0; border-top: 1px solid rgba(148,163,184,.16); font-size: 13px; }
    .leg:first-of-type { border-top: 0; }
    .pill { display: inline-flex; align-items: center; justify-content: center; height: 28px; padding: 0 10px; border-radius: 999px; background: rgba(59,130,246,.2); color: #bfdbfe; font-weight: 900; }
    .pill.walk { background: rgba(148,163,184,.18); color: #e2e8f0; }
    .stop-marker {
      width: 34px; height: 34px; margin-left: -17px; margin-top: -17px; border-radius: 999px;
      display: grid; place-items: center; color: white; font-size: 11px; font-weight: 950;
      border: 3px solid white; background: var(--blue); box-shadow: 0 8px 26px rgba(0,0,0,.36);
      cursor: pointer; transition: transform .12s ease, background .12s ease;
    }
    .stop-marker:hover { transform: scale(1.22); }
    .stop-marker.origin { background: var(--green); transform: scale(1.28); }
    .stop-marker.destination { background: var(--red); transform: scale(1.28); }
    .location-pin {
      width: 42px; height: 42px; margin-left: -21px; margin-top: -36px; border-radius: 999px 999px 999px 8px;
      transform: rotate(-45deg); display: grid; place-items: center;
      border: 3px solid white; box-shadow: 0 14px 36px rgba(0,0,0,.4);
    }
    .location-pin span { transform: rotate(45deg); color: white; font-weight: 950; font-size: 13px; }
    .location-pin.origin { background: var(--green); }
    .location-pin.destination { background: var(--red); }
    .cluster-marker {
      width: 52px; height: 52px; margin-left: -26px; margin-top: -26px; display: grid; place-items: center;
      border-radius: 999px; color: white; font-weight: 950; font-size: 15px;
      border: 3px solid rgba(255,255,255,.95);
      background: radial-gradient(circle at 30% 25%, #60a5fa, #2563eb 58%, #1d4ed8);
      box-shadow: 0 16px 42px rgba(37,99,235,.42), 0 0 0 8px rgba(59,130,246,.18);
      cursor: pointer;
    }
    .cluster-marker.medium {
      background: radial-gradient(circle at 30% 25%, #a78bfa, #7c3aed 58%, #5b21b6);
      box-shadow: 0 16px 42px rgba(124,58,237,.42), 0 0 0 9px rgba(124,58,237,.18);
    }
    .cluster-marker.large {
      background: radial-gradient(circle at 30% 25%, #fb923c, #f97316 58%, #c2410c);
      box-shadow: 0 16px 42px rgba(249,115,22,.42), 0 0 0 10px rgba(249,115,22,.18);
    }
    .leaflet-popup-content-wrapper, .leaflet-popup-tip { background: var(--panel-strong); color: var(--text); }
    .leaflet-control-attribution { font-size: 10px; }
    @media (max-width: 760px) {
      body { overflow: auto; }
      #map { position: relative; height: 56vh; }
      .shell { width: 100%; max-height: none; margin: 0; border-radius: 0; }
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <main class="shell">
    <header>
      <h1>Bucharest Transit Planner</h1>
      <p class="sub">Pick two locations or stops and a departure time. The planner estimates the trip using schedules plus historical/predictive delays.</p>
    </header>
    <section class="content">
      <div class="pick-mode" aria-label="Map pick mode">
        <button class="mode-btn active" data-kind="origin" id="mode-origin">Set origin</button>
        <button class="mode-btn" data-kind="destination" id="mode-destination">Set destination</button>
      </div>

      <label for="originSearch">Origin</label>
      <div class="field-row">
        <div class="search-box">
          <input id="originSearch" placeholder="Search for a stop or click the map..." autocomplete="off" />
          <div id="originSuggest" class="suggestions"></div>
        </div>
        <button class="icon-btn" id="swap" title="Swap">⇅</button>
      </div>
      <div id="originSelected" class="selected origin">Not selected</div>

      <label for="destinationSearch">Destination</label>
      <div class="search-box">
        <input id="destinationSearch" placeholder="Search for a stop or click the map..." autocomplete="off" />
        <div id="destinationSuggest" class="suggestions"></div>
      </div>
      <div id="destinationSelected" class="selected destination">Not selected</div>

      <label for="departure">Departure time</label>
      <input id="departure" type="datetime-local" />
      <button class="primary" id="plan">Plan trip</button>
      <div class="status" id="status"></div>
      <section id="results"></section>
    </section>
  </main>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const state = {
      stopsById: new Map(),
      markers: new Map(),
      pickMarkers: { origin: null, destination: null },
      stopLayer: null,
      routeLayer: null,
      activePick: 'origin',
      origin: null,
      destination: null,
      selectedItinerary: 0,
      renderSeq: 0,
      searchTimers: { origin: null, destination: null }
    };

    const map = L.map('map', { zoomControl: false, preferCanvas: true }).setView([44.4268, 26.1025], 12);
    L.control.zoom({ position: 'bottomright' }).addTo(map);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    const els = {
      originInput: document.getElementById('originSearch'),
      destinationInput: document.getElementById('destinationSearch'),
      originSelected: document.getElementById('originSelected'),
      destinationSelected: document.getElementById('destinationSelected'),
      departure: document.getElementById('departure'),
      plan: document.getElementById('plan'),
      status: document.getElementById('status'),
      results: document.getElementById('results'),
      originSuggest: document.getElementById('originSuggest'),
      destinationSuggest: document.getElementById('destinationSuggest'),
      swap: document.getElementById('swap'),
      modes: [...document.querySelectorAll('.mode-btn')]
    };

    function localDateTimeValue(date) {
      const shifted = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
      return shifted.toISOString().slice(0, 16);
    }

    function setStatus(message) { els.status.textContent = message || ''; }

    function escapeHtml(value) {
      return String(value).replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[c]));
    }

    function optionLabel(stop) {
      return `${stop.name} · ${stop.stop_count} points · ${stop.place_id.slice(-6)}`;
    }

    function setActivePick(kind) {
      state.activePick = kind;
      els.modes.forEach(btn => btn.classList.toggle('active', btn.dataset.kind === kind));
      (kind === 'origin' ? els.originInput : els.destinationInput).focus();
    }

    function markerClass(placeId) {
      if (state.origin?.place_id === placeId) return 'stop-marker origin';
      if (state.destination?.place_id === placeId) return 'stop-marker destination';
      return 'stop-marker';
    }

    function updateMarker(placeId) {
      const marker = state.markers.get(placeId);
      if (!marker) return;
      const stop = state.stopsById.get(placeId);
      marker.setIcon(L.divIcon({
        className: '',
        html: markerHtml(stop),
        iconSize: [34, 34],
        iconAnchor: [17, 17]
      }));
    }

    function markerHtml(stop) {
      const count = stop?.stop_count || 1;
      const label = count > 9 ? '9+' : String(count);
      return `<div class="${markerClass(stop.place_id)}">${count > 1 ? label : ''}</div>`;
    }

    function renderSelected() {
      els.originSelected.textContent = state.origin ? state.origin.name : 'Not selected';
      els.destinationSelected.textContent = state.destination ? state.destination.name : 'Not selected';
      els.originInput.value = state.origin ? (state.origin.kind === 'stop' ? optionLabel(state.origin) : state.origin.name) : '';
      els.destinationInput.value = state.destination ? (state.destination.kind === 'stop' ? optionLabel(state.destination) : state.destination.name) : '';
    }

    function locationId(latlng) {
      return `loc:${latlng.lat.toFixed(6)},${latlng.lng.toFixed(6)}`;
    }

    function locationName(latlng) {
      return `Selected location (${latlng.lat.toFixed(5)}, ${latlng.lng.toFixed(5)})`;
    }

    function locationIcon(kind) {
      return L.divIcon({
        className: '',
        html: `<div class="location-pin ${kind}"><span>${kind === 'origin' ? 'A' : 'B'}</span></div>`,
        iconSize: [42, 42],
        iconAnchor: [21, 36]
      });
    }

    function clearLocationMarker(kind) {
      if (!state.pickMarkers[kind]) return;
      state.pickMarkers[kind].remove();
      state.pickMarkers[kind] = null;
    }

    function drawLocationMarker(kind, point) {
      clearLocationMarker(kind);
      state.pickMarkers[kind] = L.marker([point.lat, point.lon], {
        icon: locationIcon(kind),
        draggable: true,
        zIndexOffset: 1000
      }).addTo(map);
      state.pickMarkers[kind].on('dragend', event => {
        const latlng = event.target.getLatLng();
        setLocation(kind, latlng, false);
      });
    }

    function clearPreviousVisual(kind, previous) {
      if (previous?.kind === 'location') clearLocationMarker(kind);
      if (previous?.kind === 'stop') updateMarker(previous.place_id);
    }

    function setSelected(kind, placeId, pan = true) {
      const stop = state.stopsById.get(placeId);
      if (!stop) return;
      const previous = state[kind];
      clearPreviousVisual(kind, previous);
      state[kind] = { ...stop, kind: 'stop', id: stop.place_id };
      renderSelected();
      updateMarker(stop.place_id);
      if (pan) {
        const marker = state.markers.get(stop.place_id);
        if (marker) {
          map.setView([stop.lat, stop.lon], Math.max(map.getZoom(), 18), { animate: true });
          marker.openPopup();
        } else {
          map.setView([stop.lat, stop.lon], Math.max(map.getZoom(), 18), { animate: true });
        }
      }
      setActivePick(kind === 'origin' ? 'destination' : 'origin');
      setStatus(`${kind === 'origin' ? 'Origin' : 'Destination'}: ${stop.name}`);
    }

    function setLocation(kind, latlng, pan = true) {
      const previous = state[kind];
      clearPreviousVisual(kind, previous);
      const point = {
        kind: 'location',
        id: locationId(latlng),
        name: locationName(latlng),
        lat: latlng.lat,
        lon: latlng.lng
      };
      state[kind] = point;
      drawLocationMarker(kind, point);
      renderSelected();
      if (pan) map.panTo(latlng, { animate: true });
      setActivePick(kind === 'origin' ? 'destination' : 'origin');
      setStatus(`${kind === 'origin' ? 'Origin' : 'Destination'} set on the map.`);
    }

    function markerPopup(stop) {
      return `<strong>${escapeHtml(stop.name)}</strong><br>${stop.stop_count} GTFS points<br><span style="color:#93c5fd">Click this marker to set the ${state.activePick}.</span>`;
    }

    function clusterHtml(feature) {
      const size = feature.count >= 100 ? 'large' : feature.count >= 25 ? 'medium' : 'small';
      const label = feature.count > 999 ? '999+' : String(feature.count);
      return `<div class="cluster-marker ${size}">${label}</div>`;
    }

    function drawMapFeatures(features) {
      if (!state.stopLayer) state.stopLayer = L.layerGroup().addTo(map);
      state.stopLayer.clearLayers();
      state.markers.clear();

      features.forEach(feature => {
        if (feature.type === 'cluster') {
          const marker = L.marker([feature.lat, feature.lon], {
            icon: L.divIcon({
              className: '',
              html: clusterHtml(feature),
              iconSize: [52, 52],
              iconAnchor: [26, 26]
            }),
            title: `${feature.count} stops`
          });
          marker.on('click', event => {
            if (event.originalEvent) L.DomEvent.stopPropagation(event.originalEvent);
            map.setView([feature.lat, feature.lon], Math.min(18, map.getZoom() + 2), { animate: true });
          });
          state.stopLayer.addLayer(marker);
          return;
        }

        state.stopsById.set(feature.place_id, feature);
        const marker = L.marker([feature.lat, feature.lon], {
          icon: L.divIcon({
            className: '',
            html: markerHtml(feature),
            iconSize: [34, 34],
            iconAnchor: [17, 17]
          }),
          riseOnHover: true,
          title: feature.name
        }).bindPopup(() => markerPopup(feature));

        marker.on('click', event => {
          if (event.originalEvent) L.DomEvent.stopPropagation(event.originalEvent);
          setSelected(state.activePick, feature.place_id, false);
        });
        state.markers.set(feature.place_id, marker);
        state.stopLayer.addLayer(marker);
      });
    }

    async function loadMapStops() {
      const seq = ++state.renderSeq;
      const bounds = map.getBounds();
      const bbox = [
        bounds.getSouth().toFixed(6),
        bounds.getWest().toFixed(6),
        bounds.getNorth().toFixed(6),
        bounds.getEast().toFixed(6)
      ].join(',');
      const params = new URLSearchParams({
        bbox,
        zoom: String(Math.round(map.getZoom()))
      });
      const response = await fetch(`/api/map-stops?${params.toString()}`);
      if (!response.ok) throw new Error(`Map API failed: ${response.status}`);
      const payload = await response.json();
      if (seq !== state.renderSeq) return;
      drawMapFeatures(payload.features);
      setStatus(`${payload.count} stops in this area. Click the map or search by name.`);
    }

    function scheduleMapStops() {
      window.clearTimeout(state.mapTimer);
      state.mapTimer = window.setTimeout(() => {
        loadMapStops().catch(error => setStatus(error.message));
      }, 90);
    }

    function fmtTime(iso) {
      return new Intl.DateTimeFormat('en-GB', {
        hour: '2-digit', minute: '2-digit', day: '2-digit', month: '2-digit'
      }).format(new Date(iso));
    }

    function sourceLabel(source) {
      return ({
        model_prediction: 'ML prediction',
        historical_feature_windows: 'Recent historical pattern',
        historical_stop_events: 'Matched vehicle history',
        static_schedule: 'Static GTFS schedule',
        walking: 'Walking only',
        none: 'No delay model'
      })[source] || source.replaceAll('_', ' ');
    }

    function formatDelay(minutes) {
      if (!minutes) return 'on time';
      return minutes > 0 ? `+${minutes} min` : `${minutes} min`;
    }

    function delayClass(minutes) {
      if (!minutes) return 'delay-zero';
      return minutes > 0 ? 'delay-positive' : 'delay-negative';
    }

    function legDelayLine(leg) {
      if (leg.mode === 'walk') return '<span class="hint">No delay model needed for walking.</span>';
      const delayMinutes = Math.round((leg.expected_delay_seconds || 0) / 60);
      const source = sourceLabel(leg.prediction?.source || 'static_schedule');
      return `<span class="hint ${delayClass(delayMinutes)}">${formatDelay(delayMinutes)} expected delay · ${escapeHtml(source)}</span>`;
    }

    function delaySummary(payload) {
      const best = payload.itineraries[0];
      if (!best) return '';
      const delayMinutes = best.expected_delay_minutes || 0;
      const sources = [...new Set(best.prediction_sources || ['static_schedule'])].map(sourceLabel);
      const notes = (payload.notes || []).map(note => `<div class="hint">${escapeHtml(note)}</div>`).join('');
      return `<section class="delay-panel">
        <h2>Expected delay intelligence</h2>
        <div class="hint">The first option is sorted by expected arrival time, not just the static schedule.</div>
        <div class="metric-grid">
          <div class="metric"><b>${best.total_minutes} min</b><span>Scheduled</span></div>
          <div class="metric"><b>${best.expected_total_minutes ?? best.total_minutes} min</b><span>Expected</span></div>
          <div class="metric"><b class="${delayClass(delayMinutes)}">${formatDelay(delayMinutes)}</b><span>Delay</span></div>
        </div>
        <div>${sources.map(source => `<span class="source-chip">${escapeHtml(source)}</span>`).join('')}</div>
        ${notes}
      </section>`;
    }

    function legText(leg) {
      const timeLine = `${fmtTime(leg.expected_departure_time || leg.departure_time)} - ${fmtTime(leg.expected_arrival_time || leg.arrival_time)}`;
      if (leg.mode === 'walk') {
        return `<div class="leg"><span class="pill walk">walk</span><div>
          <strong>${escapeHtml(leg.from.name)}</strong> -> <strong>${escapeHtml(leg.to.name)}</strong><br>
          ${timeLine} · ~${leg.duration_minutes} min (${leg.distance_meters} m)
          <br>${legDelayLine(leg)}
        </div></div>`;
      }
      const scheduledLine = leg.expected_arrival_time && leg.expected_arrival_time !== leg.arrival_time
        ? `<br><span class="hint">Scheduled: ${fmtTime(leg.departure_time)} - ${fmtTime(leg.arrival_time)}</span>`
        : '';
      return `<div class="leg"><span class="pill">${escapeHtml(leg.route_short_name)}</span><div>
        <strong>${escapeHtml(leg.from.name)}</strong> -> <strong>${escapeHtml(leg.to.name)}</strong><br>
        Expected: ${timeLine} · ${leg.duration_minutes} min
        ${scheduledLine}
        ${leg.headsign ? `<br><span class="hint">towards ${escapeHtml(leg.headsign)}</span>` : ''}
        <br>${legDelayLine(leg)}
      </div></div>`;
    }

    function drawItinerary(itinerary) {
      if (state.routeLayer) state.routeLayer.remove();
      const layers = [];
      itinerary.legs.forEach(leg => {
        const geometry = leg.geometry?.length
          ? leg.geometry
          : [[leg.from.lat, leg.from.lon], [leg.to.lat, leg.to.lon]];
        const isWalk = leg.mode === 'walk';
        layers.push(L.polyline(geometry, {
          color: isWalk ? '#94a3b8' : '#f97316',
          weight: isWalk ? 5 : 8,
          opacity: isWalk ? 0.8 : 0.95,
          dashArray: isWalk ? '8 10' : null,
          lineCap: 'round',
          lineJoin: 'round'
        }));
      });
      state.routeLayer = L.featureGroup(layers).addTo(map);
      map.fitBounds(state.routeLayer.getBounds(), { padding: [45, 45] });
    }

    function renderResults(payload) {
      if (!payload.itineraries.length) {
        els.results.innerHTML = '<div class="itinerary">No route found in the next 6 hours.</div>';
        return;
      }
      window.latestItineraries = payload.itineraries;
      const cards = payload.itineraries.map((itinerary, index) => `
        <article class="itinerary ${index === 0 ? 'active' : ''}" onclick="showItinerary(${index})">
          <h3><span>${itinerary.expected_total_minutes ?? itinerary.total_minutes} min</span><span class="time">${fmtTime(itinerary.departure_time)} -> ${fmtTime(itinerary.expected_arrival_time || itinerary.arrival_time)}</span></h3>
          <div class="eta-row"><span>Scheduled ETA</span><span>${itinerary.total_minutes} min · ${fmtTime(itinerary.arrival_time)}</span></div>
          <div class="eta-row"><span>Expected ETA</span><b>${itinerary.expected_total_minutes ?? itinerary.total_minutes} min · ${fmtTime(itinerary.expected_arrival_time || itinerary.arrival_time)}</b></div>
          <div class="hint">${itinerary.transfers} transfer · <span class="${delayClass(itinerary.expected_delay_minutes || 0)}">${formatDelay(itinerary.expected_delay_minutes || 0)} expected delay</span> · ${(itinerary.prediction_sources || []).map(sourceLabel).join(', ')} · click the card to view the route</div>
          ${itinerary.legs.map(legText).join('')}
        </article>
      `).join('');
      els.results.innerHTML = delaySummary(payload) + cards;
      drawItinerary(payload.itineraries[0]);
    }

    window.showItinerary = (index) => {
      state.selectedItinerary = index;
      [...document.querySelectorAll('.itinerary')].forEach((card, idx) => {
        card.classList.toggle('active', idx === index);
      });
      drawItinerary(window.latestItineraries[index]);
    };

    function clearSuggestions(kind) {
      (kind === 'origin' ? els.originSuggest : els.destinationSuggest).replaceChildren();
    }

    async function searchStops(kind, term) {
      const box = kind === 'origin' ? els.originSuggest : els.destinationSuggest;
      const trimmed = term.trim();
      if (trimmed.length < 2) {
        clearSuggestions(kind);
        return;
      }

      const params = new URLSearchParams({ q: trimmed, limit: '8' });
      const response = await fetch(`/api/stops?${params.toString()}`);
      if (!response.ok) return;
      const payload = await response.json();
      const fragment = document.createDocumentFragment();
      payload.stops.forEach(stop => {
        state.stopsById.set(stop.place_id, stop);
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'suggestion';
        button.innerHTML = `${escapeHtml(stop.name)}<small>${stop.stop_count} GTFS points</small>`;
        button.addEventListener('mousedown', event => event.preventDefault());
        button.addEventListener('click', () => {
          clearSuggestions(kind);
          setSelected(kind, stop.place_id);
        });
        fragment.appendChild(button);
      });
      box.replaceChildren(fragment);
    }

    function scheduleSearch(kind, value) {
      window.clearTimeout(state.searchTimers[kind]);
      state.searchTimers[kind] = window.setTimeout(() => {
        searchStops(kind, value).catch(() => clearSuggestions(kind));
      }, 180);
    }

    async function planRoute() {
      const origin = state.origin?.id;
      const destination = state.destination?.id;
      if (!origin || !destination) {
        setStatus('Choose an origin and a destination.');
        return;
      }
      if (origin === destination) {
        setStatus('Origin and destination must be different.');
        return;
      }
      const params = new URLSearchParams({
        origin,
        destination,
        departure_at: els.departure.value,
        max_transfers: '1'
      });
      setStatus('Planning trip...');
      els.results.innerHTML = '';
      const response = await fetch(`/api/plan?${params.toString()}`);
      const payload = await response.json();
      if (!response.ok) {
        setStatus(payload.detail || `Error ${response.status}`);
        return;
      }
      setStatus(`Found ${payload.count} options with ETA adjusted by history/predictions.`);
      renderResults(payload);
    }

    els.modes.forEach(btn => btn.addEventListener('click', () => setActivePick(btn.dataset.kind)));
    els.originInput.addEventListener('focus', () => setActivePick('origin'));
    els.destinationInput.addEventListener('focus', () => setActivePick('destination'));
    els.originInput.addEventListener('input', event => scheduleSearch('origin', event.target.value));
    els.destinationInput.addEventListener('input', event => scheduleSearch('destination', event.target.value));
    els.originInput.addEventListener('blur', () => window.setTimeout(() => clearSuggestions('origin'), 120));
    els.destinationInput.addEventListener('blur', () => window.setTimeout(() => clearSuggestions('destination'), 120));
    els.swap.addEventListener('click', () => {
      const oldOrigin = state.origin;
      state.origin = state.destination;
      state.destination = oldOrigin;
      clearLocationMarker('origin');
      clearLocationMarker('destination');
      if (state.origin?.kind === 'location') drawLocationMarker('origin', state.origin);
      if (state.destination?.kind === 'location') drawLocationMarker('destination', state.destination);
      renderSelected();
      scheduleMapStops();
      setStatus('Swapped origin and destination.');
    });
    map.on('click', event => setLocation(state.activePick, event.latlng));
    map.on('moveend zoomend', scheduleMapStops);
    els.plan.addEventListener('click', planRoute);
    els.departure.value = localDateTimeValue(new Date(Date.now() + 5 * 60000));
    scheduleMapStops();
  </script>
</body>
</html>
"""
