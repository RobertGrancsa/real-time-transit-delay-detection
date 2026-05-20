"""Train the route delay prediction model from TimescaleDB feature windows."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

from config.settings import settings  # noqa: E402
from services.model_training.feature_engineer import (  # noqa: E402
    FEATURE_COLUMNS,
    build_training_dataset,
    fetch_training_rows,
    temporal_train_test_split,
)

logger = logging.getLogger(__name__)


def _load_ml_dependencies() -> tuple[object, object, object]:
    try:
        from joblib import dump
        from sklearn.ensemble import HistGradientBoostingRegressor
        from sklearn.metrics import mean_absolute_error, mean_squared_error
    except ImportError as exc:
        raise SystemExit(
            "ML dependencies are missing. Install them with: pip install -e .[ml]"
        ) from exc
    return dump, HistGradientBoostingRegressor, (mean_absolute_error, mean_squared_error)


def train_delay_model(*, days: int, model_path: Path, report_path: Path) -> dict[str, float | int]:
    dump, regressor_cls, metrics = _load_ml_dependencies()
    mean_absolute_error, mean_squared_error = metrics

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)

    conn = psycopg2.connect(settings.postgres.dsn)
    try:
        rows = fetch_training_rows(conn, start_time=start_time, end_time=end_time)
    finally:
        conn.close()

    if len(rows) < 20:
        raise SystemExit(
            f"Not enough feature rows to train: found {len(rows)}, need at least 20."
        )

    train_rows, test_rows = temporal_train_test_split(rows, test_fraction=0.2)
    train_dataset = build_training_dataset(train_rows)
    test_dataset = build_training_dataset(test_rows)

    model = regressor_cls(random_state=42, max_iter=200, learning_rate=0.06)
    x_train = [[row[column] for column in FEATURE_COLUMNS] for row in train_dataset.features]
    x_test = [[row[column] for column in FEATURE_COLUMNS] for row in test_dataset.features]

    model.fit(x_train, train_dataset.targets)
    predictions = model.predict(x_test)

    mae = float(mean_absolute_error(test_dataset.targets, predictions))
    rmse = float(mean_squared_error(test_dataset.targets, predictions, squared=False))
    report: dict[str, float | int] = {
        "rows_total": len(rows),
        "rows_train": train_dataset.row_count,
        "rows_test": test_dataset.row_count,
        "mae_sec": mae,
        "rmse_sec": rmse,
        "horizon_minutes": settings.prediction.horizon_minutes,
    }

    model_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    dump({"model": model, "feature_columns": FEATURE_COLUMNS, "report": report}, model_path)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Train route delay prediction model")
    parser.add_argument("--days", type=int, default=30, help="Historical days to train on")
    parser.add_argument(
        "--model-path",
        type=Path,
        default=Path(settings.prediction.model_path),
        help="Path for the serialized model artifact",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path("models/delay_model_report.json"),
        help="Path for the JSON evaluation report",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    report = train_delay_model(
        days=args.days,
        model_path=args.model_path,
        report_path=args.report_path,
    )
    logger.info("Training complete: %s", report)


if __name__ == "__main__":
    main()
