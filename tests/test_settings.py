from __future__ import annotations

from config.settings import Settings


def test_default_analytics_settings_are_available() -> None:
    settings = Settings()

    assert settings.analytics.watermark_lag_seconds == 15
    assert settings.analytics.stop_geofence_meters == 75.0
    assert settings.analytics.bunching_threshold_seconds == 120
    assert settings.prediction.horizon_minutes == 30
    assert settings.prediction.high_delay_threshold_seconds == 300
