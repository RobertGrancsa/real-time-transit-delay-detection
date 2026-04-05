"""Async Kafka Producer — Polls live vehicle positions and publishes to Kafka.

Continuously fetches GPS telemetry from the mo-bi.ro REST API every N seconds,
sanitizes the payloads, and produces them as JSON messages to the
'transit-live-telemetry' Kafka topic keyed by route_id for partition affinity.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
from confluent_kafka import KafkaError, Producer

from config.settings import settings

logger = logging.getLogger(__name__)


def _delivery_callback(err: KafkaError | None, msg: Any) -> None:
    """Log failed deliveries (called once per message by librdkafka)."""
    if err is not None:
        logger.error("Message delivery failed [%s]: %s", msg.topic(), err)


def _build_producer() -> Producer:
    """Create a confluent-kafka Producer with sensible defaults."""
    return Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "client.id": "transit-live-producer",
            # Batching for throughput — flush at most every 500ms or 64KB
            "linger.ms": 500,
            "batch.size": 65536,
            # Idempotent producer prevents duplicates on retries
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
        }
    )


def _sanitize_vehicle(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Extract and normalize a single vehicle record from the mo-bi.ro payload.

    Returns None if the record is malformed or missing critical fields.

    Output schema (flat JSON):
    {
        "vehicle_id": "10",
        "label": "10",
        "license_plate": "B02213",
        "route_id": "54",
        "direction_id": 1,
        "agency_id": "1",
        "trip_start_time": "13:48:00",
        "latitude": 44.443,
        "longitude": 26.060,
        "timestamp": 1775387205,
        "ingested_at": "2026-04-05T14:06:45.123456+00:00"
    }
    """
    try:
        v = raw.get("vehicle", {})
        trip = v.get("trip", {})
        vehicle = v.get("vehicle", {})
        position = v.get("position", {})

        # Skip records without position or route (idle / garage vehicles)
        lat = position.get("latitude")
        lon = position.get("longitude")
        route_id = trip.get("routeId")
        if lat is None or lon is None or not route_id:
            return None

        vehicle_id = vehicle.get("id", "")
        # mo-bi.ro sometimes returns '0' for unknown vehicles — keep them but flag
        return {
            "vehicle_id": str(vehicle_id),
            "label": str(vehicle.get("label", "")),
            "license_plate": str(vehicle.get("licensePlate", "")),
            "route_id": str(route_id),
            "direction_id": trip.get("directionId"),
            "agency_id": str(trip.get("agencyId", "")),
            "trip_start_time": trip.get("startTime"),
            "latitude": float(lat),
            "longitude": float(lon),
            "timestamp": v.get("timestamp"),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    except (TypeError, ValueError, AttributeError) as exc:
        logger.debug("Skipping malformed vehicle record: %s — %s", raw.get("id"), exc)
        return None


async def _fetch_vehicles(session: aiohttp.ClientSession) -> list[dict[str, Any]]:
    """Fetch the full vehicle list from the live API. Returns [] on failure."""
    try:
        async with session.get(settings.live_api.url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                logger.warning("Live API returned status %d", resp.status)
                return []
            return await resp.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logger.warning("Live API request failed: %s", exc)
        return []


async def produce_loop() -> None:
    """Main async loop: poll API → sanitize → produce to Kafka."""
    producer = _build_producer()
    topic = settings.kafka.topic_live_telemetry
    interval = settings.live_api.poll_interval_seconds
    running = True

    def _stop(*_: Any) -> None:
        nonlocal running
        logger.info("Shutdown signal received, stopping producer loop...")
        running = False

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler; use fallback
            signal.signal(sig, _stop)

    logger.info(
        "Starting live producer — topic=%s, poll_interval=%ds, api=%s",
        topic, interval, settings.live_api.url,
    )

    async with aiohttp.ClientSession() as session:
        while running:
            cycle_start = time.monotonic()

            raw_vehicles = await _fetch_vehicles(session)
            produced = 0
            for raw in raw_vehicles:
                record = _sanitize_vehicle(raw)
                if record is None:
                    continue

                # Key by route_id so all vehicles on the same route land in the same partition
                key = record["route_id"].encode("utf-8")
                value = json.dumps(record).encode("utf-8")

                producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    callback=_delivery_callback,
                )
                produced += 1

            # Trigger delivery of buffered messages
            producer.poll(0)

            elapsed = time.monotonic() - cycle_start
            logger.info(
                "Cycle: %d vehicles fetched, %d produced in %.2fs",
                len(raw_vehicles), produced, elapsed,
            )

            # Sleep for the remainder of the interval
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0 and running:
                await asyncio.sleep(sleep_time)

    # Final flush — deliver all remaining messages (up to 10s timeout)
    remaining = producer.flush(timeout=10)
    if remaining > 0:
        logger.warning("%d messages were not delivered before shutdown", remaining)
    logger.info("Producer shut down cleanly.")


def main() -> None:
    """CLI entry point for the live Kafka producer."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(produce_loop())


if __name__ == "__main__":
    main()
