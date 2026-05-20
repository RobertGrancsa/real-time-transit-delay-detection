# Analytics And Prediction Specification

This document turns the project proposal into implementation-level definitions. It answers three questions that must be clear in a full-semester transit analytics project: which data sources are used, what each metric means, and how prediction is produced from collected data.

## Data Sources

| Source | Link | Format | Ingestion mode | Used for |
|--------|------|--------|----------------|----------|
| TPBI GTFS static feed | `https://gtfs.tpbi.ro/regional/BUCHAREST-REGION.zip` | ZIP archive with GTFS CSV files: `agency.txt`, `routes.txt`, `stops.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt`, `shapes.txt` | Batch download and bulk insert into PostgreSQL | Planned schedule, route geometry, stop metadata, service calendar |
| TPBI live vehicle positions | `https://maps.mo-bi.ro/api/busData` | Public REST JSON array with vehicle, trip, position, timestamp, route, direction, and agency fields | Polled every 10 seconds, sanitized, then written to Kafka topic `transit-live-telemetry` | Live vehicle stream, operational metrics, true delay matching, prediction features |

The live source is not a log file. The project creates a stream by polling the public REST endpoint at a fixed interval and publishing each sanitized vehicle object to Kafka.

## Metric Definitions

| Metric | Grain | Formula or rule | Output |
|--------|-------|-----------------|--------|
| Telemetry freshness | 1 minute by route and direction | `processing_time - vehicle_timestamp` | `transit.telemetry_freshness_metrics` |
| Stop-level schedule delay | Vehicle, trip, stop, observed event | `observed_stop_time - scheduled_stop_time` after GTFS trip and stop match | `transit.stop_delay_events` |
| Route delay | 1 minute by route and direction | Avg, min, max, p95 of stop-level schedule delay | `transit.route_delay_metrics` |
| Bunching | Event by route and direction | Two different vehicles on the same route and direction observed within `ANALYTICS_BUNCHING_THRESHOLD_SECONDS` | `transit.bunching_events` |
| Service gap | Window by route and direction | No sufficient vehicle coverage for more than `ANALYTICS_SERVICE_GAP_THRESHOLD_MINUTES` | `transit.service_gap_alerts` |
| Speed | 15 minutes by route and direction | Distance between consecutive GPS points divided by elapsed time, filtered for impossible speeds | `transit.segment_speed_stats` |
| Stop dwell time | Stop, vehicle, route, direction | Time spent inside the stop geofence across consecutive observations | `transit.stop_dwell_times` |
| Geographic hotspot score | Window by zone or route segment | Weighted combination of high delay, low speed, bunching count, gap count, and active vehicle density | `transit.geo_delay_hotspots` |
| Delay prediction | Route, direction, prediction target time | Model estimate of future route delay at `PREDICTION_HORIZON_MINUTES` | `transit.route_delay_predictions` |
| Feature attribution | Prediction and feature | Contribution score or normalized importance for top features | `transit.delay_feature_attributions` |

The old `delay_sec = processing_time - vehicle_timestamp` calculation is a data-quality metric. It must not be presented as schedule delay.

## Schedule Delay Matching

True delay requires a match between a live vehicle observation and the planned GTFS schedule.

1. Candidate trips are selected by `route_id`, `direction_id`, service date, and `trip_start_time` near the scheduled first stop time.
2. Candidate stops are selected by geofence distance from the vehicle position to `transit.stops` and the stop sequence of the selected trip.
3. A matched event emits `observed_time`, `scheduled_time`, `delay_sec`, `distance_to_stop_m`, `match_confidence`, and `match_method`.
4. Route-level delay is aggregated from matched stop events, not from raw telemetry timestamps.

If a GTFS match cannot be made confidently, the event is excluded from schedule-delay aggregates but can still contribute to speed, freshness, and vehicle-density metrics.

## Prediction Targets

| Target | Type | Definition | Primary metric |
|--------|------|------------|----------------|
| `avg_delay_next_horizon_sec` | Regression | Average route delay over the next prediction horizon | MAE and RMSE |
| `high_delay_next_horizon` | Classification | Whether future route delay exceeds the configured high-delay threshold | Precision, recall, F1 |
| `bunching_probability_next_30m` | Classification | Probability of at least one bunching event in the next 30 minutes | ROC AUC and F1 |

Recommended baseline models are persistence and hour-of-day averages. The first learned model should use scikit-learn tree models because they are easy to train, fast to serve, and support feature importance. XGBoost and SHAP can be added later if dependency weight is acceptable.

## Feature Store

`transit.realtime_feature_windows` stores model-ready features at a fixed time grain.

Suggested feature families:

- Recent average, p95, max, and trend of true schedule delay.
- Recent speed average and speed drop versus historical average.
- Bunching count and service gap count.
- Active vehicle count and observation count.
- Stop dwell-time summary.
- Route type, direction, hour of day, day of week, weekend flag, and peak flag.
- Geographic hotspot score if a zone or segment can be assigned.
- Telemetry freshness average and p95 for data-quality awareness.

Feature windows should be generated by Flink or SQL continuous jobs and then reused by model training, prediction serving, and Grafana.

## Prediction Flow

1. Flink and TimescaleDB generate route-level metrics and feature windows.
2. A training script reads historical feature windows and labels from TimescaleDB.
3. The trained model is versioned and written to `PREDICTION_MODEL_PATH`.
4. A predictor service periodically reads the latest feature windows, scores all route and direction pairs, and writes results to `transit.route_delay_predictions`.
5. Grafana reads predictions, confidence, model version, and feature attributions from SQL views.

This keeps Flink focused on streaming analytics while Python handles model training and inference.

## Explainability

Root-cause panels should use careful wording. SQL correlations and model feature contributions show associated factors, not guaranteed causal causes. Initial contributors should include recent delay, speed drop, bunching count, service gaps, dwell time, hour of day, route type, and hotspot score.

## Validation Criteria

- Source coverage: GTFS tables loaded and live API events visible in Kafka and TimescaleDB.
- Metric correctness: synthetic stop arrivals produce expected positive, negative, and zero delays.
- Prediction quality: learned model beats persistence and hour-of-day baselines.
- Operational quality: predictions include model version, generated time, target time, horizon, and stale-data indicators.
- Dashboard clarity: telemetry freshness, schedule delay, predicted delay, hotspots, and feature attribution are visually separate.