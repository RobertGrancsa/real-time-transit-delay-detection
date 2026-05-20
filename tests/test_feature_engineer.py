from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.model_training.feature_engineer import (
    FEATURE_COLUMNS,
    build_high_delay_label,
    build_training_dataset,
    normalize_feature_row,
    risk_level_from_delay,
    temporal_train_test_split,
)


def test_normalize_feature_row_fills_missing_values() -> None:
    normalized = normalize_feature_row({"vehicle_count": 4, "is_weekend": True})

    assert set(normalized) == set(FEATURE_COLUMNS)
    assert normalized["vehicle_count"] == 4.0
    assert normalized["is_weekend"] == 1.0
    assert normalized["avg_delay_sec"] == 0.0


def test_build_training_dataset_skips_unlabeled_rows() -> None:
    dataset = build_training_dataset(
        [
            {"route_id": "10", "target_delay_sec": 120, "vehicle_count": 3},
            {"route_id": "11", "target_delay_sec": None, "vehicle_count": 2},
        ]
    )

    assert dataset.row_count == 1
    assert dataset.targets == [120.0]
    assert dataset.metadata[0]["route_id"] == "10"


def test_temporal_train_test_split_orders_by_feature_time() -> None:
    rows = [
        {"feature_time": datetime(2026, 1, 3, tzinfo=timezone.utc)},
        {"feature_time": datetime(2026, 1, 1, tzinfo=timezone.utc)},
        {"feature_time": datetime(2026, 1, 2, tzinfo=timezone.utc)},
    ]

    train, test = temporal_train_test_split(rows, test_fraction=1 / 3)

    assert [row["feature_time"].day for row in train] == [1, 2]
    assert [row["feature_time"].day for row in test] == [3]


def test_temporal_train_test_split_rejects_invalid_fraction() -> None:
    with pytest.raises(ValueError):
        temporal_train_test_split([], test_fraction=1.0)


def test_delay_risk_helpers_use_prediction_threshold() -> None:
    assert build_high_delay_label(299) == 0
    assert build_high_delay_label(300) == 1
    assert risk_level_from_delay(100) == "low"
    assert risk_level_from_delay(200) == "medium"
    assert risk_level_from_delay(300) == "high"
    assert risk_level_from_delay(600) == "critical"
