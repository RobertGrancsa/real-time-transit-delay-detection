"""PyFlink Processing Pipeline — Real-time transit analytics.

This job reads live vehicle telemetry from Kafka, applies time-windowed
analytics (delay computation, bunching detection, service gap alerts,
segment speed aggregation), and sinks the results into TimescaleDB via JDBC.

Architecture:
    Kafka (transit-live-telemetry)
        → Flink SQL with event-time watermarks
        → Tumbling / Session windows
        → JDBC sinks → TimescaleDB hypertables

Usage:
    Submitted to the Flink cluster via:
        flink run -py processing_job.py

    Or locally for testing:
        python processing_job.py
"""

from __future__ import annotations

import os
import logging

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — read from env vars with Docker-compose defaults
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_LIVE_TELEMETRY", "transit-live-telemetry")

PG_URL = "jdbc:postgresql://{}:{}/{}".format(
    os.getenv("POSTGRES_HOST", "timescaledb"),
    os.getenv("POSTGRES_PORT", "5432"),
    os.getenv("POSTGRES_DB", "transit"),
)
PG_USER = os.getenv("POSTGRES_USER", "transit")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "transit_secret")


def _jdbc_opts(table: str) -> str:
    """Return the WITH clause options for a JDBC sink table."""
    return f"""
        'connector' = 'jdbc',
        'url' = '{PG_URL}',
        'table-name' = '{table}',
        'username' = '{PG_USER}',
        'password' = '{PG_PASS}',
        'sink.buffer-flush.max-rows' = '500',
        'sink.buffer-flush.interval' = '5s'
    """


def build_pipeline(t_env: StreamTableEnvironment) -> None:
    """Register all sources, sinks, and processing queries."""

    # -----------------------------------------------------------------------
    # 1. Kafka Source — live vehicle telemetry
    # -----------------------------------------------------------------------
    # The producer writes flat JSON keyed by route_id.
    # We extract event_time from the `timestamp` field (epoch seconds) and
    # define a watermark with 15-second tolerance for out-of-order events.
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS kafka_telemetry (
            vehicle_id       STRING,
            label            STRING,
            license_plate    STRING,
            route_id         STRING,
            direction_id     INT,
            agency_id        STRING,
            trip_start_time  STRING,
            latitude         DOUBLE,
            longitude        DOUBLE,
            `timestamp`      BIGINT,
            ingested_at      STRING,
            event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 0),
            WATERMARK FOR event_time AS event_time - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-transit-processor',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # -----------------------------------------------------------------------
    # 2. JDBC Sinks — one per analytics output table
    # -----------------------------------------------------------------------

    # 2a. Raw vehicle positions sink (for replay / dashboard map)
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS sink_vehicle_positions (
            event_time       TIMESTAMP(3) WITH LOCAL TIME ZONE,
            vehicle_id       STRING,
            route_id         STRING,
            direction_id     SMALLINT,
            agency_id        STRING,
            latitude         DOUBLE,
            longitude        DOUBLE,
            license_plate    STRING,
            trip_start_time  STRING,
            ingested_at      TIMESTAMP(3) WITH LOCAL TIME ZONE
        ) WITH (
            {_jdbc_opts('transit.vehicle_positions')}
        )
    """)

    # 2b. Route delay metrics sink
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS sink_route_delay_metrics (
            window_start     TIMESTAMP(3) WITH LOCAL TIME ZONE,
            window_end       TIMESTAMP(3) WITH LOCAL TIME ZONE,
            route_id         STRING,
            direction_id     SMALLINT,
            vehicle_count    INT,
            avg_delay_sec    DOUBLE,
            max_delay_sec    DOUBLE,
            min_delay_sec    DOUBLE,
            computed_at      TIMESTAMP(3) WITH LOCAL TIME ZONE
        ) WITH (
            {_jdbc_opts('transit.route_delay_metrics')}
        )
    """)

    # 2c. Bunching events sink
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS sink_bunching_events (
            event_time       TIMESTAMP(3) WITH LOCAL TIME ZONE,
            route_id         STRING,
            direction_id     SMALLINT,
            vehicle_a_id     STRING,
            vehicle_b_id     STRING,
            gap_seconds      DOUBLE,
            lat_a            DOUBLE,
            lon_a            DOUBLE,
            lat_b            DOUBLE,
            lon_b            DOUBLE
        ) WITH (
            {_jdbc_opts('transit.bunching_events')}
        )
    """)

    # 2d. Service gap alerts sink
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS sink_service_gap_alerts (
            gap_start        TIMESTAMP(3) WITH LOCAL TIME ZONE,
            gap_end          TIMESTAMP(3) WITH LOCAL TIME ZONE,
            route_id         STRING,
            direction_id     SMALLINT,
            gap_duration_sec DOUBLE
        ) WITH (
            {_jdbc_opts('transit.service_gap_alerts')}
        )
    """)

    # 2e. Segment speed stats sink
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS sink_segment_speed_stats (
            window_start     TIMESTAMP(3) WITH LOCAL TIME ZONE,
            window_end       TIMESTAMP(3) WITH LOCAL TIME ZONE,
            route_id         STRING,
            direction_id     SMALLINT,
            vehicle_count    INT,
            avg_speed_kmh    DOUBLE,
            min_speed_kmh    DOUBLE,
            max_speed_kmh    DOUBLE
        ) WITH (
            {_jdbc_opts('transit.segment_speed_stats')}
        )
    """)

    # -----------------------------------------------------------------------
    # 3. Processing Queries — uses Flink SQL windowing functions
    # -----------------------------------------------------------------------

    # Create a StatementSet so all INSERT queries run as a single job graph
    stmt_set = t_env.create_statement_set()

    # 3a. Raw positions passthrough — store every telemetry event
    #     This feeds the Grafana map panel with live vehicle locations.
    stmt_set.add_insert_sql("""
        INSERT INTO sink_vehicle_positions
        SELECT
            event_time,
            vehicle_id,
            route_id,
            CAST(direction_id AS SMALLINT),
            agency_id,
            latitude,
            longitude,
            license_plate,
            trip_start_time,
            TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(), 0) AS ingested_at
        FROM kafka_telemetry
    """)

    # 3b. Route Delay Metrics — 1-minute tumbling windows
    #     Computes the difference between the vehicle's reported timestamp and
    #     the current processing time as a proxy for schedule adherence.
    #     (A full delay computation would join with GTFS stop_times; this gives
    #      a useful "freshness" metric and route-level aggregation.)
    stmt_set.add_insert_sql("""
        INSERT INTO sink_route_delay_metrics
        SELECT
            window_start,
            window_end,
            route_id,
            CAST(direction_id AS SMALLINT),
            CAST(COUNT(DISTINCT vehicle_id) AS INT) AS vehicle_count,
            AVG(delay_sec)  AS avg_delay_sec,
            MAX(delay_sec)  AS max_delay_sec,
            MIN(delay_sec)  AS min_delay_sec,
            LOCALTIMESTAMP   AS computed_at
        FROM (
            SELECT
                route_id,
                direction_id,
                vehicle_id,
                event_time,
                -- Delay proxy: how old the GPS fix is relative to Flink processing time.
                -- Large values indicate the vehicle is "stale" or delayed in reporting.
                CAST(
                    (UNIX_TIMESTAMP() - `timestamp`) AS DOUBLE
                ) AS delay_sec
            FROM kafka_telemetry
        )
        GROUP BY
            TUMBLE(event_time, INTERVAL '1' MINUTE),
            route_id,
            direction_id
    """)

    # 3c. Route Bunching Detection — 30-second tumbling windows
    #     Within each window, for each route+direction, self-join to find
    #     pairs of distinct vehicles whose timestamps are <120s apart.
    #     This detects bunching (two buses arriving nearly simultaneously).
    stmt_set.add_insert_sql("""
        INSERT INTO sink_bunching_events
        SELECT
            a.event_time,
            a.route_id,
            CAST(a.direction_id AS SMALLINT),
            a.vehicle_id AS vehicle_a_id,
            b.vehicle_id AS vehicle_b_id,
            ABS(CAST(a.`timestamp` - b.`timestamp` AS DOUBLE)) AS gap_seconds,
            a.latitude  AS lat_a,
            a.longitude  AS lon_a,
            b.latitude  AS lat_b,
            b.longitude  AS lon_b
        FROM kafka_telemetry a
        INNER JOIN kafka_telemetry b
            ON a.route_id = b.route_id
            AND a.direction_id = b.direction_id
            AND a.vehicle_id < b.vehicle_id
            AND b.event_time BETWEEN a.event_time - INTERVAL '30' SECOND
                                 AND a.event_time + INTERVAL '30' SECOND
        WHERE ABS(a.`timestamp` - b.`timestamp`) < 120
    """)

    # 3d. Service Gap Alerts — detect routes with no vehicle for >10 minutes
    #     Uses a 5-minute tumbling window: if a route+direction appears in one
    #     window but NOT in the next, we emit a gap alert.
    #     Simplified approach: flag routes where the max time between consecutive
    #     events in a 10-minute window exceeds a threshold.
    stmt_set.add_insert_sql("""
        INSERT INTO sink_service_gap_alerts
        SELECT
            window_start AS gap_start,
            window_end   AS gap_end,
            route_id,
            CAST(direction_id AS SMALLINT),
            CAST(
                TIMESTAMPDIFF(SECOND, MIN(event_time), MAX(event_time))
            AS DOUBLE) AS gap_duration_sec
        FROM kafka_telemetry
        GROUP BY
            TUMBLE(event_time, INTERVAL '10' MINUTE),
            route_id,
            direction_id
        HAVING COUNT(DISTINCT vehicle_id) <= 1
           AND TIMESTAMPDIFF(SECOND, MIN(event_time), MAX(event_time)) > 600
    """)

    # 3e. Segment Speed Aggregates — 15-minute tumbling windows
    #     Estimates average speed per route/direction using the Haversine
    #     distance between consecutive position reports of each vehicle.
    #     We use a subquery with LAG() to get previous position per vehicle,
    #     then compute speed = distance / time_delta.
    #
    #     Haversine approximation in SQL (flat-earth shortcut for short distances):
    #       distance_km ≈ 111.32 * sqrt((Δlat)² + (cos(lat)*Δlon)²)
    stmt_set.add_insert_sql("""
        INSERT INTO sink_segment_speed_stats
        SELECT
            window_start,
            window_end,
            route_id,
            CAST(direction_id AS SMALLINT),
            CAST(COUNT(*) AS INT)           AS vehicle_count,
            AVG(speed_kmh)                   AS avg_speed_kmh,
            MIN(speed_kmh)                   AS min_speed_kmh,
            MAX(speed_kmh)                   AS max_speed_kmh
        FROM (
            SELECT
                route_id,
                direction_id,
                event_time,
                -- Flat-earth approximation for short distances (good for city-scale)
                CASE WHEN time_delta_sec > 0 THEN
                    (111.32 * SQRT(
                        POWER(latitude - prev_lat, 2) +
                        POWER(COS(RADIANS(latitude)) * (longitude - prev_lon), 2)
                    )) / (time_delta_sec / 3600.0)
                ELSE NULL END AS speed_kmh
            FROM (
                SELECT
                    route_id,
                    direction_id,
                    vehicle_id,
                    latitude,
                    longitude,
                    event_time,
                    `timestamp`,
                    LAG(latitude)    OVER w AS prev_lat,
                    LAG(longitude)   OVER w AS prev_lon,
                    LAG(`timestamp`) OVER w AS prev_ts,
                    CAST(`timestamp` - LAG(`timestamp`) OVER w AS DOUBLE) AS time_delta_sec
                FROM kafka_telemetry
                WINDOW w AS (PARTITION BY vehicle_id ORDER BY event_time)
            )
            WHERE time_delta_sec > 5        -- ignore duplicate/too-close pings
              AND time_delta_sec < 300       -- ignore gaps >5 min (vehicle stopped/lost)
              AND prev_lat IS NOT NULL
        )
        WHERE speed_kmh > 0 AND speed_kmh < 120   -- sanity: discard impossible speeds
        GROUP BY
            TUMBLE(event_time, INTERVAL '15' MINUTE),
            route_id,
            direction_id
    """)

    # Execute all inserts as a unified job graph
    stmt_set.execute()


def main() -> None:
    """Entry point — configure Flink environment and launch the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Use streaming mode with Blink planner
    env = StreamExecutionEnvironment.get_execution_environment()

    # Event-time processing for correct windowing
    env.get_config().set_auto_watermark_interval(5000)  # watermark every 5s

    # Checkpointing every 60s for fault tolerance
    env.enable_checkpointing(60_000)

    t_env = StreamTableEnvironment.create(
        env,
        environment_settings=EnvironmentSettings.in_streaming_mode(),
    )

    # Set parallelism to match TaskManager slot count
    t_env.get_config().set("parallelism.default", "4")
    # Mini-batch optimization for better throughput
    t_env.get_config().set("table.exec.mini-batch.enabled", "true")
    t_env.get_config().set("table.exec.mini-batch.allow-latency", "5s")
    t_env.get_config().set("table.exec.mini-batch.size", "1000")

    logger.info("Building Flink pipeline — Kafka(%s) → Processing → JDBC(%s)", KAFKA_TOPIC, PG_URL)
    build_pipeline(t_env)
    logger.info("Pipeline submitted successfully.")


if __name__ == "__main__":
    main()
