from __future__ import annotations

from datetime import datetime, timezone

from services.predictor.predictor import (
    WARM_START_MODEL_VERSION,
    build_prediction_records,
    build_warm_start_model_bundle,
)


class ConstantModel:
    def __init__(self, value: float) -> None:
        self.value = value

    def predict(self, rows: list[list[float]]) -> list[float]:
        return [self.value for _ in rows]


def test_build_prediction_records_maps_features_to_route_predictions() -> None:
    feature_time = datetime(2026, 5, 20, 10, 0, tzinfo=timezone.utc)
    prediction_time = datetime(2026, 5, 20, 10, 5, tzinfo=timezone.utc)
    feature_rows = [
        {
            "feature_time": feature_time,
            "route_id": "10",
            "direction_id": 1,
            "vehicle_count": 4,
            "avg_delay_sec": 200,
            "hour_of_day": 10,
            "day_of_week": 3,
        }
    ]
    bundle = {"model": ConstantModel(360), "feature_columns": ("vehicle_count", "avg_delay_sec")}

    records = build_prediction_records(feature_rows, bundle, prediction_time=prediction_time)

    assert len(records) == 1
    assert records[0].route_id == "10"
    assert records[0].direction_id == 1
    assert records[0].predicted_delay_sec == 360
    assert records[0].risk_level == "high"
    assert records[0].feature_time == feature_time


def test_warm_start_model_builds_baseline_predictions() -> None:
    feature_time = datetime(2026, 5, 20, 10, 0, tzinfo=timezone.utc)
    prediction_time = datetime(2026, 5, 20, 10, 5, tzinfo=timezone.utc)
    feature_rows = [
        {
            "feature_time": feature_time,
            "route_id": "10",
            "direction_id": 1,
            "avg_delay_sec": 120,
            "p95_delay_sec": 240,
            "max_delay_sec": 300,
            "speed_drop_kmh": 8,
            "bunching_count": 1,
            "service_gap_count": 0,
            "hotspot_score": 0.5,
            "avg_freshness_sec": 20,
        }
    ]

    records = build_prediction_records(
        feature_rows,
        build_warm_start_model_bundle(),
        prediction_time=prediction_time,
    )

    assert len(records) == 1
    assert records[0].model_version == WARM_START_MODEL_VERSION
    assert records[0].predicted_delay_sec > 120
    assert records[0].predicted_delay_sec < 500
    assert records[0].confidence == 0.35
