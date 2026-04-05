# Real-Time Public Transit Delay Detection for Bucharest

## The Team

The project will be developed by Robert Grancsa, Ana-Maria Toader, Andrei-Daniel Anghelescu, and Rares-Alexandru Constantin. The workload is structurally divided across data ingestion, stream processing, anomaly detection, and dashboard integration to ensure a complete end-to-end delivery.

## The Objective

The main goal is to build a smart city congestion monitoring system that detects operational anomalies in the Bucharest public transit network. By continuously comparing live vehicle positions against planned schedules, the system will actively identify route bunching, sudden delays, and prolonged service gaps in real time.

## Data Sources and Datasets

The foundational dataset is the official Bucharest and Ilfov Region GTFS feed provided by TPBI, which contains the static routes, active stops, and baseline schedules. For real-time telemetry, the project will ingest live GPS vehicle trajectories from Bucharest-Ilfov open-data REST APIs and active community-documented endpoints.

## Processing Engines Used

Apache Kafka will serve as the distributed streaming platform to reliably ingest high-throughput live transit events. Apache Flink will act as the primary stream processing engine, executing time-windowed aggregations to calculate live delay metrics and detect spatial anomalies. The processed insights will then be routed to PostgreSQL for persistent storage and visualized dynamically through Grafana.

## Goal: make a meaningful real data processing
• Predictions
• Trends
• Analytics
• Dashboards
• etc.
