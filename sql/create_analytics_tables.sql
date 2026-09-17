-- ============================================================
-- UrbanPulse Analytics Serving Layer
-- PostgreSQL table definitions for persisted Gold datasets
--
-- IMPORTANT:
-- UrbanPulse uses SYNTHETIC / SIMULATED transportation data.
-- These tables must not be represented as official
-- Johannesburg transportation data.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;


-- ============================================================
-- 1. ROUTE PERFORMANCE
--
-- Grain:
-- One row per route_id + 15-minute window
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.route_performance (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    route_id TEXT NOT NULL,

    observation_count BIGINT NOT NULL,
    active_vehicle_count BIGINT NOT NULL,

    avg_speed_kmh DOUBLE PRECISION,
    min_speed_kmh DOUBLE PRECISION,
    max_speed_kmh DOUBLE PRECISION,

    in_service_observations BIGINT,
    delayed_observations BIGINT,
    out_of_service_observations BIGINT,

    low_occupancy_observations BIGINT,
    moderate_occupancy_observations BIGINT,
    high_occupancy_observations BIGINT,

    delay_rate_pct DOUBLE PRECISION,
    high_occupancy_rate_pct DOUBLE PRECISION,
    out_of_service_rate_pct DOUBLE PRECISION,

    gold_processed_at TIMESTAMP,

    CONSTRAINT pk_route_performance
        PRIMARY KEY (route_id, window_start, window_end),

    CONSTRAINT chk_route_performance_window
        CHECK (window_end > window_start),

    CONSTRAINT chk_route_observation_count
        CHECK (observation_count >= 0),

    CONSTRAINT chk_route_active_vehicle_count
        CHECK (active_vehicle_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_route_performance_window
    ON analytics.route_performance (window_start);

CREATE INDEX IF NOT EXISTS idx_route_performance_route
    ON analytics.route_performance (route_id);


-- ============================================================
-- 2. CONGESTION SUMMARY
--
-- Grain:
-- One row per road_segment + aggregation window
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.congestion_summary (
    road_segment TEXT NOT NULL,

    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,

    observation_count BIGINT NOT NULL,

    average_speed_kmh DOUBLE PRECISION,
    minimum_speed_kmh DOUBLE PRECISION,
    maximum_speed_kmh DOUBLE PRECISION,

    congestion_classification TEXT,

    gold_generated_at TIMESTAMP,

    aggregation_window TEXT,
    data_classification TEXT,
    gold_model TEXT,

    CONSTRAINT pk_congestion_summary
        PRIMARY KEY (road_segment, window_start, window_end),

    CONSTRAINT chk_congestion_summary_window
        CHECK (window_end > window_start),

    CONSTRAINT chk_congestion_observation_count
        CHECK (observation_count >= 0),

    CONSTRAINT chk_congestion_average_speed
        CHECK (
            average_speed_kmh IS NULL
            OR average_speed_kmh >= 0
        ),

    CONSTRAINT chk_congestion_minimum_speed
        CHECK (
            minimum_speed_kmh IS NULL
            OR minimum_speed_kmh >= 0
        ),

    CONSTRAINT chk_congestion_maximum_speed
        CHECK (
            maximum_speed_kmh IS NULL
            OR maximum_speed_kmh >= 0
        )
);

CREATE INDEX IF NOT EXISTS idx_congestion_summary_window
    ON analytics.congestion_summary (window_start);

CREATE INDEX IF NOT EXISTS idx_congestion_summary_segment
    ON analytics.congestion_summary (road_segment);

CREATE INDEX IF NOT EXISTS idx_congestion_summary_classification
    ON analytics.congestion_summary (congestion_classification);


-- ============================================================
-- 3. INCIDENT IMPACT
--
-- Grain:
-- One row per road + 15-minute window
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.incident_impact (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,

    road_name TEXT NOT NULL,
    normalized_road_name TEXT,

    traffic_segment_id TEXT,
    incident_segment_id TEXT,

    -- Traffic metrics
    traffic_observation_count BIGINT NOT NULL,

    avg_speed_kmh DOUBLE PRECISION,
    min_speed_kmh DOUBLE PRECISION,
    max_speed_kmh DOUBLE PRECISION,

    avg_vehicle_count DOUBLE PRECISION,
    max_vehicle_count INTEGER,

    avg_travel_time_seconds DOUBLE PRECISION,
    max_travel_time_seconds INTEGER,

    heavy_congestion_observations BIGINT,
    moderate_congestion_observations BIGINT,
    low_congestion_observations BIGINT,

    -- Incident metrics
    incident_count BIGINT NOT NULL,
    active_incident_count BIGINT,

    high_severity_incident_count BIGINT,
    moderate_severity_incident_count BIGINT,
    low_severity_incident_count BIGINT,

    road_closure_count BIGINT,
    vehicle_breakdown_count BIGINT,
    road_hazard_count BIGINT,

    avg_estimated_clearance_minutes DOUBLE PRECISION,
    max_estimated_clearance_minutes INTEGER,

    -- Derived congestion metrics
    heavy_congestion_percentage DOUBLE PRECISION,
    moderate_congestion_percentage DOUBLE PRECISION,
    low_congestion_percentage DOUBLE PRECISION,

    dominant_congestion_level TEXT,

    -- Derived impact metrics
    incident_severity_score BIGINT,
    traffic_disruption_score DOUBLE PRECISION,
    incident_impact_score DOUBLE PRECISION,
    incident_impact_level TEXT,

    -- Gold metadata
    gold_model TEXT,
    aggregation_window TEXT,
    data_classification TEXT,
    generated_at TIMESTAMP,

    CONSTRAINT pk_incident_impact
        PRIMARY KEY (road_name, window_start, window_end),

    CONSTRAINT chk_incident_impact_window
        CHECK (window_end > window_start),

    CONSTRAINT chk_incident_count
        CHECK (incident_count >= 1),

    CONSTRAINT chk_incident_traffic_count
        CHECK (traffic_observation_count >= 1),

    CONSTRAINT chk_incident_impact_score
        CHECK (
            incident_impact_score IS NULL
            OR incident_impact_score >= 0
        ),

    CONSTRAINT chk_incident_avg_speed
        CHECK (
            avg_speed_kmh IS NULL
            OR avg_speed_kmh >= 0
        ),

    CONSTRAINT chk_incident_avg_travel_time
        CHECK (
            avg_travel_time_seconds IS NULL
            OR avg_travel_time_seconds >= 0
        )
);

CREATE INDEX IF NOT EXISTS idx_incident_impact_window
    ON analytics.incident_impact (window_start);

CREATE INDEX IF NOT EXISTS idx_incident_impact_road
    ON analytics.incident_impact (road_name);

CREATE INDEX IF NOT EXISTS idx_incident_impact_level
    ON analytics.incident_impact (incident_impact_level);

CREATE INDEX IF NOT EXISTS idx_incident_congestion_level
    ON analytics.incident_impact (dominant_congestion_level);


-- ============================================================
-- TABLE DOCUMENTATION
-- ============================================================

COMMENT ON TABLE analytics.route_performance IS
'UrbanPulse synthetic Gold route performance metrics aggregated by route and time window.';

COMMENT ON TABLE analytics.congestion_summary IS
'UrbanPulse synthetic Gold congestion metrics aggregated by road segment and time window.';

COMMENT ON TABLE analytics.incident_impact IS
'UrbanPulse synthetic Gold cross-domain incident and traffic impact metrics aggregated by road and 15-minute window.';


-- ============================================================
-- END
-- ============================================================