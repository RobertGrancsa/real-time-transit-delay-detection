-- =============================================================================
-- TimescaleDB Initialization Script
-- Runs automatically on first container start via /docker-entrypoint-initdb.d/
-- =============================================================================

-- Enable the TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create a dedicated schema for transit analytics
CREATE SCHEMA IF NOT EXISTS transit;

-- Placeholder tables will be created by Phase 4 migration scripts.
-- This file ensures the extension and schema exist on first boot.
