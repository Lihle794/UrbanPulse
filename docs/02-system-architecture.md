# UrbanPulse — System Architecture

## 1. Architecture Overview

UrbanPulse is designed as a modular, layered data platform capable of processing both batch and near-real-time transportation data.

The architecture separates data ingestion, storage, processing, orchestration, analytical serving, observability, and deployment concerns.

The platform is designed to support local development through containerized infrastructure while maintaining a path toward cloud deployment on Microsoft Azure.

The architecture follows the following high-level data flow:

**Data Sources → Ingestion → Bronze → Processing → Silver → Gold → Analytical Serving → Consumption**

A parallel streaming path supports near-real-time transportation events:

**Event Sources → Kafka → Stream Processing → Analytical/Operational Outputs**

---

## 2. Architecture Principles

### 2.1 Separation of Concerns

Each major component shall have a clearly defined responsibility.

Ingestion, processing, orchestration, storage, analytical serving, monitoring, and application layers should remain independently maintainable.

### 2.2 Layered Data Architecture

The platform shall use Bronze, Silver, and Gold data layers to separate raw data from progressively refined datasets.

### 2.3 Batch and Streaming Architecture

The platform shall support both scheduled batch processing and near-real-time event processing.

### 2.4 Reproducibility

The development environment and infrastructure should be reproducible using containerization and version-controlled configuration.

### 2.5 Data Quality by Design

Data-quality validation shall be incorporated into the data lifecycle rather than treated as a downstream reporting activity.

### 2.6 Observability by Design

Critical pipelines and services shall produce measurable operational signals including logs, metrics, execution status, data freshness, and processing latency.

### 2.7 Failure Isolation

Failures in individual data sources or processing components should not unnecessarily corrupt previously processed data or unrelated datasets.

### 2.8 Incremental Processing

Where practical, pipelines should process only newly arrived or changed data rather than repeatedly processing complete historical datasets.

### 2.9 Idempotent Processing

Pipeline components should be designed to safely handle retries and repeated execution without producing unintended duplicate results.

### 2.10 Cloud Portability

The architecture should remain sufficiently modular that components can be moved from local development infrastructure to cloud-managed services without requiring a complete redesign.

### 2.11 Security by Design

Credentials, permissions, network communication, infrastructure, and data access shall be designed according to appropriate security principles from the beginning of implementation.

### 2.12 Automation

Testing, validation, deployment, data processing, and operational workflows should be automated wherever practical. 


## 3. Storage Architecture

UrbanPulse will use a layered data lake architecture for persistent raw and processed data, combined with PostgreSQL as the analytical serving layer.

The storage architecture is designed to separate durable data storage from analytical consumption.

### 3.1 Object Storage

The data lake will use object storage as the primary persistent storage layer.

During local development, MinIO will provide an S3-compatible object-storage environment running in containers.

The architecture will maintain a conceptual compatibility path to Microsoft Azure Data Lake Storage Gen2 for cloud deployment.

### 3.2 Data Lake Layers

The data lake will contain three primary logical layers:

#### Bronze

The Bronze layer will contain raw source data with minimal transformation.

Its purpose is to preserve source data for:

- recovery;
- auditing;
- lineage;
- reprocessing; and
- historical reference.

#### Silver

The Silver layer will contain cleaned, standardized, validated, and enriched datasets.

This layer will be the primary input for downstream analytical transformations.

#### Gold

The Gold layer will contain business-oriented datasets optimized for analytical consumption.

Examples include:

- route performance;
- congestion metrics;
- vehicle activity;
- incident analysis; and
- weather impact analysis.

### 3.3 Storage Format

Processed datasets will primarily use Apache Parquet.

Parquet is selected because it provides a columnar storage format suitable for analytical workloads and integrates efficiently with Apache Spark.

### 3.4 Partitioning

Large datasets shall be partitioned according to appropriate query and processing patterns.

Potential partitioning attributes include:

- event date;
- ingestion date;
- year;
- month; and
- geographic or operational dimensions where justified.

Partitioning strategies shall avoid excessive partition fragmentation.

### 3.5 PostgreSQL Analytical Serving Layer

PostgreSQL will provide the primary analytical serving layer for the initial implementation.

Gold datasets will be transformed into analytical tables designed for efficient querying by downstream consumers.

The PostgreSQL layer will contain dimensional and fact tables where appropriate.

### 3.6 Separation of Storage and Serving

The data lake will remain the authoritative storage environment for raw and processed datasets.

PostgreSQL will serve curated analytical datasets to downstream applications and business intelligence tools.

This separation will allow historical data to remain independently recoverable while providing consumers with optimized analytical tables.

### 3.7 Local-to-Cloud Portability

The local storage architecture shall be designed to support future migration to cloud object storage.

The initial target mapping is:

| Local Component | Cloud Equivalent |
|---|---|
| MinIO | Azure Data Lake Storage Gen2 |
| PostgreSQL | Azure Database for PostgreSQL or equivalent |
| Docker services | Azure-hosted compute/services |

Cloud deployment decisions will be finalized during the infrastructure design phase.