## 8. Data Source Specification

UrbanPulse will use a combination of public real-world datasets, external APIs, and controlled synthetic data.

The purpose of combining multiple source types is to reproduce the heterogeneous data environment that would exist in a real transportation data platform while maintaining sufficient control over data availability and reproducibility.

The initial source strategy is:

| Data Domain | Source Strategy | Processing Pattern |
|---|---|---|
| Transit schedules and network | Public GTFS data | Batch |
| Vehicle telemetry | Synthetic event generator | Streaming |
| Traffic conditions | Synthetic event generator / public data where suitable | Streaming/Batch |
| Weather observations | Open-Meteo API | Batch |
| Transportation incidents | Synthetic event generator / public data where suitable | Streaming/Batch |
| Geographic data | OpenStreetMap and public geographic datasets | Batch/Reference |

---

### 8.1 Transit Data

UrbanPulse will use the General Transit Feed Specification (GTFS) as the primary logical standard for transit schedule and network data.

GTFS provides a standardized structure for information such as agencies, routes, stops, trips, stop times, and service calendars.

Johannesburg's transportation ecosystem includes multiple public transport modes, including Gautrain and Rea Vaya. Publicly available South African transit data will be investigated and used where suitable.

The initial transit data strategy will prioritize:

- route definitions;
- stop locations;
- trip definitions;
- stop schedules;
- service calendars; and
- route geometry where available.

Static transit data will primarily support the batch processing and analytical components of UrbanPulse.

GTFS Realtime-compatible data will be investigated separately for potential use in future versions of the platform.

---

### 8.2 Vehicle Telemetry

A reliable public real-time vehicle-position feed for the Johannesburg transportation network may not be consistently available in a form suitable for this project.

UrbanPulse will therefore implement a synthetic vehicle telemetry generator.

The generator will simulate realistic vehicle movement across defined Johannesburg routes and locations.

Generated events will include:

- vehicle identifier;
- route identifier;
- trip identifier;
- timestamp;
- latitude;
- longitude;
- speed;
- operational status; and
- event identifier.

The generator will produce events continuously and will serve as the primary input to the Kafka streaming pipeline.

Synthetic telemetry will be explicitly identified as simulated data.

---

### 8.3 Weather Data

UrbanPulse will use the Open-Meteo API for weather observations.

Open-Meteo provides historical weather data and supports hourly weather variables including temperature, precipitation, humidity, wind speed, and weather codes.

Weather data can therefore be associated with transportation observations using geographic coordinates and timestamps.

The weather ingestion pipeline will retrieve configurable historical and/or current weather observations for selected Johannesburg coordinates.

Weather data will primarily support:

- historical analysis;
- transportation/weather correlation;
- analytical enrichment; and
- data-quality and ingestion demonstrations.

---

### 8.4 Traffic Data

Traffic conditions are an important component of UrbanPulse but consistent public real-time traffic feeds may be subject to availability, access restrictions, API limits, or licensing constraints.

The initial implementation will therefore use a controlled synthetic traffic event generator.

The generator will simulate:

- road segments;
- average speed;
- traffic volume;
- congestion levels;
- timestamps; and
- geographic coordinates.

The simulation will support realistic scenarios such as:

- normal traffic;
- peak-hour congestion;
- severe congestion;
- road closures;
- incidents;
- weather-related slowdowns; and
- sudden traffic anomalies.

Where a suitable public traffic dataset is identified, it may be incorporated as an additional source.

---

### 8.5 Transportation Incidents

UrbanPulse will initially use synthetic incident events to provide reliable and controllable streaming data.

The incident generator will simulate events including:

- accidents;
- road closures;
- construction;
- vehicle breakdowns;
- special events; and
- other transportation disruptions.

Each incident will include a geographic location, timestamp, severity, type, and status.

The generator will support scenarios in which incidents affect nearby traffic and transportation routes.

This will allow the platform to demonstrate event correlation and operational anomaly detection without depending on the continuous availability of a third-party incident API.

---

### 8.6 Geographic Data

Geographic data will provide spatial context for Johannesburg transportation analysis.

OpenStreetMap and other suitable public geographic datasets will be investigated for:

- roads;
- road segments;
- geographic boundaries;
- points of interest;
- transit-related infrastructure; and
- other relevant geographic entities.

Geographic data will be treated primarily as reference data and will not be ingested at the same frequency as operational event data.

---

### 8.7 Source Reliability

External data sources will not be assumed to be continuously available.

The ingestion architecture shall account for:

- API failures;
- connection failures;
- rate limits;
- malformed responses;
- schema changes;
- incomplete responses;
- authentication failures where applicable; and
- temporary source unavailability.

Source failures shall be logged and handled according to the failure and retry policies defined by the platform.

---

### 8.8 Source Metadata

For each external source, UrbanPulse shall maintain metadata including:

- source name;
- source type;
- source URL or dataset location;
- data domain;
- ingestion method;
- expected frequency;
- schema version where applicable;
- licensing or usage considerations;
- authentication requirements;
- known limitations; and
- last successful ingestion timestamp.

---

### 8.9 Source-to-Platform Mapping

The following initial mapping will be used:

| Source | Raw Layer | Processing | Primary Consumer |
|---|---|---|---|
| GTFS transit data | Bronze | PySpark | Warehouse / BI |
| Open-Meteo | Bronze | PySpark | Warehouse / BI |
| Vehicle simulator | Bronze / Kafka | Streaming processor | Real-time metrics / Warehouse |
| Traffic simulator | Bronze / Kafka | Streaming processor | Real-time metrics / Warehouse |
| Incident simulator | Bronze / Kafka | Streaming processor | Alerts / Warehouse |
| Geographic datasets | Bronze | Batch processing | Reference / Warehouse |

---

### 8.10 Source Selection Principles

Data sources will be selected according to the following principles:

1. Reproducibility
2. Public accessibility where possible
3. Appropriate licensing or permitted use
4. Data quality
5. Geographic relevance to Johannesburg
6. Schema stability
7. Suitability for automated ingestion
8. Sufficient historical or real-time coverage
9. Practicality for local development
10. Ability to support realistic data engineering scenarios

The platform will clearly distinguish between real-world source data and synthetic data throughout the pipeline and documentation.