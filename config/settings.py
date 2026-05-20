"""Centralized configuration - reads from environment variables with Docker-friendly defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Load .env file if present (no-op in Docker where env vars are injected directly)
_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_env_path)


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _env_int(key: str, default: int = 0) -> int:
    return int(os.getenv(key, str(default)))


def _env_float(key: str, default: float = 0.0) -> float:
    return float(os.getenv(key, str(default)))


@dataclass(frozen=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: _env_int("POSTGRES_PORT", 5432))
    database: str = field(default_factory=lambda: _env("POSTGRES_DB", "transit"))
    user: str = field(default_factory=lambda: _env("POSTGRES_USER", "transit"))
    password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "transit_secret"))

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic_live_telemetry: str = field(
        default_factory=lambda: _env("KAFKA_TOPIC_LIVE_TELEMETRY", "transit-live-telemetry")
    )


@dataclass(frozen=True)
class LiveApiConfig:
    url: str = field(
        default_factory=lambda: _env("LIVE_API_URL", "https://maps.mo-bi.ro/api/busData")
    )
    poll_interval_seconds: int = field(
        default_factory=lambda: _env_int("LIVE_API_POLL_INTERVAL_SECONDS", 10)
    )


@dataclass(frozen=True)
class GtfsConfig:
    download_url: str = field(
        default_factory=lambda: _env(
            "GTFS_DOWNLOAD_URL", "https://gtfs.tpbi.ro/regional/BUCHAREST-REGION.zip"
        )
    )


@dataclass(frozen=True)
class FlinkConfig:
    jobmanager_host: str = field(
        default_factory=lambda: _env("FLINK_JOBMANAGER_HOST", "localhost")
    )
    jobmanager_port: int = field(
        default_factory=lambda: _env_int("FLINK_JOBMANAGER_PORT", 8081)
    )


@dataclass(frozen=True)
class AnalyticsConfig:
    watermark_lag_seconds: int = field(
        default_factory=lambda: _env_int("ANALYTICS_WATERMARK_LAG_SECONDS", 15)
    )
    stop_geofence_meters: float = field(
        default_factory=lambda: _env_float("ANALYTICS_STOP_GEOFENCE_METERS", 75.0)
    )
    trip_start_tolerance_minutes: int = field(
        default_factory=lambda: _env_int("ANALYTICS_TRIP_START_TOLERANCE_MINUTES", 20)
    )
    bunching_threshold_seconds: int = field(
        default_factory=lambda: _env_int("ANALYTICS_BUNCHING_THRESHOLD_SECONDS", 120)
    )
    service_gap_threshold_minutes: int = field(
        default_factory=lambda: _env_int("ANALYTICS_SERVICE_GAP_THRESHOLD_MINUTES", 10)
    )
    feature_window_minutes: int = field(
        default_factory=lambda: _env_int("ANALYTICS_FEATURE_WINDOW_MINUTES", 15)
    )
    schedule_timezone: str = field(
        default_factory=lambda: _env("ANALYTICS_SCHEDULE_TIMEZONE", "Europe/Bucharest")
    )
    gtfs_match_interval_seconds: int = field(
        default_factory=lambda: _env_int("ANALYTICS_GTFS_MATCH_INTERVAL_SECONDS", 30)
    )
    gtfs_match_lookback_minutes: int = field(
        default_factory=lambda: _env_int("ANALYTICS_GTFS_MATCH_LOOKBACK_MINUTES", 120)
    )
    gtfs_match_batch_size: int = field(
        default_factory=lambda: _env_int("ANALYTICS_GTFS_MATCH_BATCH_SIZE", 20)
    )
    gtfs_match_schedule_tolerance_minutes: int = field(
        default_factory=lambda: _env_int("ANALYTICS_GTFS_MATCH_SCHEDULE_TOLERANCE_MINUTES", 90)
    )


@dataclass(frozen=True)
class PredictionConfig:
    horizon_minutes: int = field(
        default_factory=lambda: _env_int("PREDICTION_HORIZON_MINUTES", 30)
    )
    high_delay_threshold_seconds: int = field(
        default_factory=lambda: _env_int("PREDICTION_HIGH_DELAY_THRESHOLD_SECONDS", 300)
    )
    interval_seconds: int = field(
        default_factory=lambda: _env_int("PREDICTION_INTERVAL_SECONDS", 900)
    )
    model_path: str = field(
        default_factory=lambda: _env("PREDICTION_MODEL_PATH", "models/delay_model.joblib")
    )
    model_version: str = field(
        default_factory=lambda: _env("PREDICTION_MODEL_VERSION", "local-dev")
    )
    cache_ttl_seconds: int = field(
        default_factory=lambda: _env_int("PREDICTION_CACHE_TTL_SECONDS", 300)
    )
    cache_size: int = field(
        default_factory=lambda: _env_int("PREDICTION_CACHE_SIZE", 300)
    )


@dataclass(frozen=True)
class Settings:
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    live_api: LiveApiConfig = field(default_factory=LiveApiConfig)
    gtfs: GtfsConfig = field(default_factory=GtfsConfig)
    flink: FlinkConfig = field(default_factory=FlinkConfig)
    analytics: AnalyticsConfig = field(default_factory=AnalyticsConfig)
    prediction: PredictionConfig = field(default_factory=PredictionConfig)


# Singleton - import this from anywhere: `from config.settings import settings`
settings = Settings()
