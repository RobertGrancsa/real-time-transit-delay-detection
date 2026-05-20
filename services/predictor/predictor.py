"""Batch inference service for route delay predictions."""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

from config.settings import settings
from services.model_training.feature_engineer import (
    FEATURE_COLUMNS,
    LATEST_FEATURES_SQL,
    normalize_feature_row,
    risk_level_from_delay,
)

logger = logging.getLogger(__name__)

WARM_START_MODEL_VERSION = "warm-start-baseline"

INSERT_PREDICTION_SQL = """
INSERT INTO transit.route_delay_predictions (
    prediction_time,
    target_time,
    horizon_minutes,
    route_id,
    direction_id,
    predicted_delay_sec,
    prediction_lower_sec,
    prediction_upper_sec,
    risk_level,
    confidence,
    model_version,
    feature_time
) VALUES %s
"""


@dataclass(frozen=True)
class PredictionRecord:
    prediction_time: datetime
    target_time: datetime
    horizon_minutes: int
    route_id: str
    direction_id: int | None
    predicted_delay_sec: float
    prediction_lower_sec: float | None
    prediction_upper_sec: float | None
    risk_level: str
    confidence: float | None
    model_version: str
    feature_time: datetime | None

    def as_tuple(self) -> tuple[Any, ...]:
        return (
            self.prediction_time,
            self.target_time,
            self.horizon_minutes,
            self.route_id,
            self.direction_id,
            self.predicted_delay_sec,
            self.prediction_lower_sec,
            self.prediction_upper_sec,
            self.risk_level,
            self.confidence,
            self.model_version,
            self.feature_time,
        )


class WarmStartDelayModel:
    def __init__(self, feature_columns: tuple[str, ...]) -> None:
        self._indexes = {column: index for index, column in enumerate(feature_columns)}

    def _value(self, row: list[float], column: str) -> float:
        index = self._indexes.get(column)
        if index is None or index >= len(row):
            return 0.0
        return float(row[index])

    def predict(self, rows: list[list[float]]) -> list[float]:
        predictions: list[float] = []
        for row in rows:
            avg_delay = self._value(row, "avg_delay_sec")
            p95_delay = self._value(row, "p95_delay_sec")
            max_delay = self._value(row, "max_delay_sec")
            speed_drop = self._value(row, "speed_drop_kmh")
            bunching_count = self._value(row, "bunching_count")
            service_gap_count = self._value(row, "service_gap_count")
            hotspot_score = self._value(row, "hotspot_score")
            avg_freshness = self._value(row, "avg_freshness_sec")

            tail_delay = max(0.0, p95_delay - avg_delay) * 0.25
            severe_delay = max(0.0, max_delay - p95_delay) * 0.10
            operational_pressure = (
                min(speed_drop, 60.0) * 2.0
                + min(bunching_count, 10.0) * 15.0
                + min(service_gap_count, 10.0) * 45.0
                + min(hotspot_score, 5.0) * 90.0
                + min(avg_freshness, 120.0) * 0.05
            )
            predicted_delay = avg_delay + tail_delay + severe_delay + operational_pressure
            predictions.append(max(-300.0, predicted_delay))
        return predictions


def build_warm_start_model_bundle(reason: str = "model_unavailable") -> dict[str, Any]:
    return {
        "model": WarmStartDelayModel(FEATURE_COLUMNS),
        "feature_columns": FEATURE_COLUMNS,
        "model_version": WARM_START_MODEL_VERSION,
        "confidence": 0.35,
        "reason": reason,
    }


def _load_model_bundle(model_path: Path, *, require_model: bool = False) -> dict[str, Any]:
    try:
        from joblib import load
    except ImportError as exc:
        if not require_model:
            logger.warning("ML dependencies missing. Using warm-start prediction baseline.")
            return build_warm_start_model_bundle("missing_ml_dependencies")
        raise SystemExit(
            "Prediction dependencies are missing. Install them with: pip install -e .[ml]"
        ) from exc

    if not model_path.exists():
        if not require_model:
            logger.warning("Model artifact not found at %s. Using warm-start baseline.", model_path)
            return build_warm_start_model_bundle("missing_model_artifact")
        raise SystemExit(f"Model artifact not found: {model_path}")
    bundle = load(model_path)
    if "model" not in bundle:
        raise SystemExit(f"Model artifact is missing a 'model' entry: {model_path}")
    return bundle


def fetch_latest_features(conn: Any) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(LATEST_FEATURES_SQL)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def build_prediction_records(
    feature_rows: list[dict[str, Any]],
    model_bundle: dict[str, Any],
    *,
    prediction_time: datetime | None = None,
) -> list[PredictionRecord]:
    if not feature_rows:
        return []

    feature_columns = tuple(model_bundle.get("feature_columns") or FEATURE_COLUMNS)
    model = model_bundle["model"]
    prediction_time = prediction_time or datetime.now(timezone.utc)
    target_time = prediction_time + timedelta(minutes=settings.prediction.horizon_minutes)

    feature_vectors = [
        [normalize_feature_row(row).get(column, 0.0) for column in feature_columns]
        for row in feature_rows
    ]
    predicted_values = model.predict(feature_vectors)

    records: list[PredictionRecord] = []
    model_version = str(model_bundle.get("model_version") or settings.prediction.model_version)
    confidence = model_bundle.get("confidence")

    for row, predicted_delay in zip(feature_rows, predicted_values, strict=True):
        delay = float(predicted_delay)
        records.append(
            PredictionRecord(
                prediction_time=prediction_time,
                target_time=target_time,
                horizon_minutes=settings.prediction.horizon_minutes,
                route_id=str(row["route_id"]),
                direction_id=row.get("direction_id"),
                predicted_delay_sec=delay,
                prediction_lower_sec=None,
                prediction_upper_sec=None,
                risk_level=risk_level_from_delay(delay),
                confidence=float(confidence) if confidence is not None else None,
                model_version=model_version,
                feature_time=row.get("feature_time"),
            )
        )
    return records


def write_prediction_records(conn: Any, records: list[PredictionRecord]) -> int:
    if not records:
        return 0
    values = [record.as_tuple() for record in records]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, INSERT_PREDICTION_SQL, values, page_size=500)
    conn.commit()
    return len(records)


def run_prediction_once(model_path: Path, *, require_model: bool = False) -> int:
    bundle = _load_model_bundle(model_path, require_model=require_model)
    conn = psycopg2.connect(settings.postgres.dsn)
    try:
        feature_rows = fetch_latest_features(conn)
        records = build_prediction_records(feature_rows, bundle)
        written = write_prediction_records(conn, records)
    finally:
        conn.close()
    logger.info("Wrote %d route delay predictions.", written)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Run route delay prediction inference")
    parser.add_argument(
        "--model-path",
        type=Path,
        default=Path(settings.prediction.model_path),
        help="Path to the serialized model artifact",
    )
    parser.add_argument(
        "--require-model",
        action="store_true",
        help="Fail when the trained model is unavailable instead of using the warm-start baseline",
    )
    parser.add_argument("--once", action="store_true", help="Run one inference cycle and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.once:
        run_prediction_once(args.model_path, require_model=args.require_model)
        return

    while True:
        run_prediction_once(args.model_path, require_model=args.require_model)
        time.sleep(settings.prediction.interval_seconds)


if __name__ == "__main__":
    main()
