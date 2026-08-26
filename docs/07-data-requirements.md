## 7. Data Requirements

UrbanPulse will integrate multiple datasets representing different aspects of urban transportation operations in Johannesburg.

The platform will ingest data from external APIs, public datasets, and controlled simulations where suitable real-time data is unavailable.

The initial data domains are:

1. Transit and vehicle telemetry
2. Traffic conditions
3. Weather observations
4. Transportation incidents
5. Geographic and location data

The platform shall maintain clear ownership, schemas, identifiers, timestamps, and quality expectations for each data domain.

---

### 7.1 Transit and Vehicle Telemetry

Transit data represents the movement and operational status of vehicles within the transportation network.

The dataset may contain information such as:

| Field | Description | Expected Type |
|---|---|---|
| `vehicle_id` | Unique identifier for the vehicle | String |
| `route_id` | Identifier of the route being operated | String |
| `trip_id` | Unique identifier for the trip | String |
| `timestamp` | Time at which the observation occurred | Timestamp |
| `latitude` | Vehicle latitude | Decimal |
| `longitude` | Vehicle longitude | Decimal |
| `speed` | Vehicle speed | Decimal |
| `status` | Current vehicle operational status | String |
| `stop_id` | Associated transit stop where applicable | String |

The vehicle telemetry dataset will be one of the primary sources for the streaming component of UrbanPulse.

The system should support both simulated real-time vehicle events and historical vehicle observations.

---

### 7.2 Traffic Conditions

Traffic data represents the observed conditions of roads and transportation corridors.

The dataset may contain:

| Field | Description | Expected Type |
|---|---|---|
| `traffic_observation_id` | Unique observation identifier | String |
| `road_id` | Identifier for the road or road segment | String |
| `timestamp` | Observation timestamp | Timestamp |
| `latitude` | Observation latitude | Decimal |
| `longitude` | Observation longitude | Decimal |
| `average_speed` | Average observed vehicle speed | Decimal |
| `traffic_volume` | Estimated traffic volume | Integer |
| `congestion_level` | Categorized congestion level | String |

Traffic observations should support both historical analysis and near-real-time monitoring.

---

### 7.3 Weather Observations

Weather data provides environmental context that can be associated with transportation conditions.

The dataset may contain:

| Field | Description | Expected Type |
|---|---|---|
| `weather_observation_id` | Unique weather observation identifier | String |
| `timestamp` | Observation timestamp | Timestamp |
| `location_id` | Geographic location identifier | String |
| `temperature` | Temperature at observation time | Decimal |
| `humidity` | Relative humidity | Decimal |
| `wind_speed` | Wind speed | Decimal |
| `precipitation` | Recorded precipitation | Decimal |
| `weather_condition` | General weather condition | String |

Weather observations should be associated with geographic locations and timestamps so they can be joined with transportation and traffic observations.

---

### 7.4 Transportation Incidents

Incident data represents events that may affect transportation operations.

Examples include:

- traffic accidents;
- road closures;
- construction;
- vehicle breakdowns;
- special events; and
- other disruptions.

The dataset may contain:

| Field | Description | Expected Type |
|---|---|---|
| `incident_id` | Unique incident identifier | String |
| `incident_type` | Type of incident | String |
| `severity` | Incident severity classification | String |
| `latitude` | Incident latitude | Decimal |
| `longitude` | Incident longitude | Decimal |
| `start_time` | Incident start time | Timestamp |
| `end_time` | Incident end time | Timestamp |
| `description` | Human-readable incident description | String |
| `status` | Current incident status | String |

Incidents should be timestamped and geographically located so that their potential impact on nearby traffic and transportation routes can be analyzed.

---

### 7.5 Geographic Data

Geographic data provides the spatial context required to analyze transportation activity across Johannesburg.

Potential geographic entities include:

- regions;
- suburbs;
- roads;
- road segments;
- transit stops;
- routes; and
- service areas.

Geographic data may be sourced from public geographic datasets and standardized into reusable reference tables.

The platform should maintain stable identifiers for geographic entities wherever possible.

---

### 7.6 Data Identifiers

Each major event or entity shall have a stable identifier where appropriate.

Examples include:

- `vehicle_id`
- `route_id`
- `trip_id`
- `incident_id`
- `road_id`
- `location_id`
- `weather_observation_id`
- `traffic_observation_id`

Identifiers will be used to support deduplication, joins, referential integrity, lineage, and analytical modelling.

---

### 7.7 Timestamps

Time is a critical dimension of UrbanPulse analysis.

All event-based datasets shall contain timestamps representing when the underlying event or observation occurred.

The platform shall distinguish between:

- **event time** — when the event actually occurred; and
- **ingestion time** — when the platform received the event.

Where applicable, processing time may also be recorded.

This distinction will allow the platform to handle late-arriving data and measure ingestion latency.

All timestamps should be normalized to a consistent representation and stored in a manner that supports reliable time-zone handling.

---

### 7.8 Data Volume

The platform shall be designed to accommodate increasing data volumes as the number of vehicles, routes, observations, and events grows.

Initial development data volumes may be simulated to represent realistic operational workloads.

The project will use controlled data-volume scenarios to evaluate:

- ingestion throughput;
- processing duration;
- storage requirements;
- streaming throughput; and
- query performance.

---

### 7.9 Data Frequency

Different data sources will have different ingestion frequencies.

The initial target frequencies are:

| Data Domain | Target Frequency | Processing Pattern |
|---|---|---|
| Vehicle telemetry | Seconds/minutes | Streaming |
| Traffic observations | Minutes | Streaming/Batch |
| Weather | Hourly or configurable | Batch |
| Incidents | Event-driven | Streaming/Batch |
| Geographic data | Infrequent | Batch/reference |

These frequencies are initial design targets and may be adjusted based on the availability and limitations of selected data sources.

---

### 7.10 Historical Data

UrbanPulse shall retain historical data to support:

- trend analysis;
- route performance comparisons;
- congestion analysis;
- incident impact analysis;
- weather correlation analysis; and
- historical benchmarking.

Historical data should remain traceable to the original raw source data.

---

### 7.11 Data Lineage

The platform shall maintain sufficient metadata to identify:

1. the original data source;
2. the ingestion operation;
3. the processing stage;
4. the transformation applied; and
5. the resulting analytical dataset.

This will allow data consumers and engineers to understand how analytical results were produced.

---

### 7.12 Synthetic Data

Where suitable public or real-time datasets are unavailable, synthetic data may be generated to simulate operational events.

Synthetic data shall be clearly identified and shall not be represented as actual observations from Johannesburg transportation operators.

Synthetic data generation should produce realistic distributions, relationships, timestamps, geographic coordinates, and operational scenarios suitable for testing the platform.

The synthetic data generator should also support controlled scenarios for testing pipeline behaviour, including:

- duplicate events;
- missing values;
- invalid coordinates;
- delayed events;
- unusually high traffic volumes;
- sudden changes in vehicle speed; and
- source interruptions.