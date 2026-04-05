-- =============================================================================
-- Migration 001: GTFS Static Tables
-- Creates relational tables for the Bucharest TPBI GTFS static feed.
-- Run against the 'transit' database after TimescaleDB init.
-- =============================================================================

-- Agency: transit operators (STB, Metrorex, STV, etc.)
CREATE TABLE IF NOT EXISTS transit.agency (
    agency_id   TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL,
    agency_url  TEXT,
    agency_timezone TEXT NOT NULL DEFAULT 'Europe/Bucharest',
    agency_lang TEXT,
    agency_phone TEXT,
    agency_fare_url TEXT,
    agency_email TEXT
);

-- Calendar: service day patterns (weekday, saturday, sunday service IDs)
CREATE TABLE IF NOT EXISTS transit.calendar (
    service_id TEXT PRIMARY KEY,
    monday     SMALLINT NOT NULL DEFAULT 0,
    tuesday    SMALLINT NOT NULL DEFAULT 0,
    wednesday  SMALLINT NOT NULL DEFAULT 0,
    thursday   SMALLINT NOT NULL DEFAULT 0,
    friday     SMALLINT NOT NULL DEFAULT 0,
    saturday   SMALLINT NOT NULL DEFAULT 0,
    sunday     SMALLINT NOT NULL DEFAULT 0,
    start_date DATE NOT NULL,
    end_date   DATE NOT NULL
);

-- Routes: bus/tram/trolleybus/metro lines
CREATE TABLE IF NOT EXISTS transit.routes (
    route_id         TEXT PRIMARY KEY,
    agency_id        TEXT REFERENCES transit.agency(agency_id),
    route_short_name TEXT,
    route_type       SMALLINT NOT NULL,  -- 0=tram, 1=metro, 3=bus, 11=trolleybus
    route_color      TEXT,
    route_text_color TEXT,
    route_long_name  TEXT
);

CREATE INDEX IF NOT EXISTS idx_routes_agency ON transit.routes(agency_id);

-- Stops: physical stops/stations
CREATE TABLE IF NOT EXISTS transit.stops (
    stop_id        TEXT PRIMARY KEY,
    stop_name      TEXT NOT NULL,
    stop_desc      TEXT,
    stop_lat       DOUBLE PRECISION NOT NULL,
    stop_lon       DOUBLE PRECISION NOT NULL,
    location_type  SMALLINT,          -- 0=stop, 1=station, 2=entrance/exit
    platform_code  TEXT,
    parent_station TEXT,
    level_id       TEXT
);

CREATE INDEX IF NOT EXISTS idx_stops_parent ON transit.stops(parent_station);
CREATE INDEX IF NOT EXISTS idx_stops_lat_lon ON transit.stops(stop_lat, stop_lon);

-- Shapes: geographic path of a route variant
CREATE TABLE IF NOT EXISTS transit.shapes (
    shape_id          TEXT NOT NULL,
    shape_pt_lat      DOUBLE PRECISION NOT NULL,
    shape_pt_lon      DOUBLE PRECISION NOT NULL,
    shape_pt_sequence INTEGER NOT NULL,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);

-- Trips: individual scheduled journeys on a route
CREATE TABLE IF NOT EXISTS transit.trips (
    trip_id                TEXT PRIMARY KEY,
    route_id               TEXT NOT NULL REFERENCES transit.routes(route_id),
    service_id             TEXT NOT NULL REFERENCES transit.calendar(service_id),
    trip_headsign          TEXT,
    trip_short_name        TEXT,
    direction_id           SMALLINT,        -- 0=outbound, 1=inbound
    block_id               TEXT,
    shape_id               TEXT,
    wheelchair_accessible  SMALLINT,
    bikes_allowed          SMALLINT
);

CREATE INDEX IF NOT EXISTS idx_trips_route ON transit.trips(route_id);
CREATE INDEX IF NOT EXISTS idx_trips_service ON transit.trips(service_id);
CREATE INDEX IF NOT EXISTS idx_trips_shape ON transit.trips(shape_id);

-- Stop Times: arrival/departure at each stop for every trip
-- This is the largest table (~62k+ trips × N stops each)
CREATE TABLE IF NOT EXISTS transit.stop_times (
    trip_id             TEXT NOT NULL REFERENCES transit.trips(trip_id),
    arrival_time        INTERVAL NOT NULL,
    departure_time      INTERVAL NOT NULL,
    stop_id             TEXT NOT NULL REFERENCES transit.stops(stop_id),
    stop_sequence       INTEGER NOT NULL,
    stop_headsign       TEXT,
    pickup_type         SMALLINT,
    drop_off_type       SMALLINT,
    shape_dist_traveled DOUBLE PRECISION,
    timepoint           SMALLINT,
    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE INDEX IF NOT EXISTS idx_stop_times_stop ON transit.stop_times(stop_id);
CREATE INDEX IF NOT EXISTS idx_stop_times_trip ON transit.stop_times(trip_id);
