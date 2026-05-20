"""Feature extraction helpers for route delay prediction.

The functions in this module are deliberately independent from a specific ML
library. Training scripts can convert the returned rows to pandas or NumPy, but
tests and lightweight data validation can use only the standard library.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from config.settings import settings

FEATURE_COLUMNS: tuple[str, ...] = (
    "vehicle_count",
    "observation_count",
    "avg_delay_sec",
    "p95_delay_sec",
    "max_delay_sec",
    "avg_speed_kmh",
    "min_speed_kmh",
    "speed_drop_kmh",
    "bunching_count",
    "service_gap_count",
    "total_gap_sec",
    "avg_dwell_sec",
    "hotspot_score",
    "avg_freshness_sec",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_am_peak",
    "is_pm_peak",
    "route_type",
)

TARGET_COLUMN = "target_delay_sec"

FEATURE_EXTRACTION_SQL = """
WITH labeled AS (
    SELECT
        f.*,
        future.avg_delay_sec AS target_delay_sec
    FROM transit.realtime_feature_windows f
    LEFT JOIN transit.realtime_feature_windows future
        ON future.route_id = f.route_id
        AND future.direction_id IS NOT DISTINCT FROM f.direction_id
        AND future.zone_id = f.zone_id
        AND future.feature_time = f.feature_time + (%(horizon_minutes)s || ' minutes')::INTERVAL
    WHERE f.feature_time >= %(start_time)s
      AND f.feature_time < %(end_time)s
)
SELECT *
FROM labeled
WHERE target_delay_sec IS NOT NULL
ORDER BY feature_time, route_id, direction_id, zone_id;
"""

LATEST_FEATURES_SQL = """
SELECT DISTINCT ON (route_id, direction_id, zone_id)
    *
FROM transit.realtime_feature_windows
WHERE feature_time >= NOW() - INTERVAL '2 hours'
ORDER BY route_id, direction_id, zone_id, feature_time DESC;
"""


@dataclass(frozen=True)
class TrainingDataset:
    features: list[dict[str, float]]
    targets: list[float]
    metadata: list[dict[str, Any]]

    @property
    def row_count(self) -> int:
        return len(self.targets)


def _as_float(value: Any, *, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    return float(value)


def risk_level_from_delay(delay_sec: float) -> str:
    threshold = settings.prediction.high_delay_threshold_seconds
    if delay_sec >= threshold * 2:
        return "critical"
    if delay_sec >= threshold:
        return "high"
    if delay_sec >= threshold / 2:
        return "medium"
    return "low"


def build_high_delay_label(delay_sec: float) -> int:
    return int(delay_sec >= settings.prediction.high_delay_threshold_seconds)


def normalize_feature_row(row: Mapping[str, Any]) -> dict[str, float]:
    return {column: _as_float(row.get(column)) for column in FEATURE_COLUMNS}


def build_training_dataset(rows: Iterable[Mapping[str, Any]]) -> TrainingDataset:
    features: list[dict[str, float]] = []
    targets: list[float] = []
    metadata: list[dict[str, Any]] = []

    for row in rows:
        target = row.get(TARGET_COLUMN)
        if target is None:
            continue

        features.append(normalize_feature_row(row))
        targets.append(float(target))
        metadata.append(
            {
                "feature_time": row.get("feature_time"),
                "route_id": row.get("route_id"),
                "direction_id": row.get("direction_id"),
                "zone_id": row.get("zone_id"),
            }
        )

    return TrainingDataset(features=features, targets=targets, metadata=metadata)


def temporal_train_test_split(
    rows: Sequence[Mapping[str, Any]],
    *,
    test_fraction: float = 0.2,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    if not 0 < test_fraction < 1:
        raise ValueError("test_fraction must be between 0 and 1")
    ordered = sorted(rows, key=lambda row: row.get("feature_time") or datetime.min)
    split_index = max(1, int(len(ordered) * (1 - test_fraction)))
    return ordered[:split_index], ordered[split_index:]


def fetch_training_rows(
    conn: Any,
    *,
    start_time: datetime,
    end_time: datetime,
    horizon_minutes: int | None = None,
) -> list[dict[str, Any]]:
    horizon = horizon_minutes or settings.prediction.horizon_minutes
    with conn.cursor() as cur:
        cur.execute(
            FEATURE_EXTRACTION_SQL,
            {
                "start_time": start_time,
                "end_time": end_time,
                "horizon_minutes": horizon,
            },
        )
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
