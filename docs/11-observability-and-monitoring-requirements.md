## 11. Observability and Monitoring Requirements

Observability is a core capability of the UrbanPulse platform. The system shall provide sufficient visibility into pipeline execution, data movement, data quality, processing performance, and infrastructure health.

The observability layer shall enable data engineers to detect, investigate, and respond to operational issues.

---

### 11.1 Pipeline Monitoring

The platform shall monitor the execution of all critical data pipelines.

For each pipeline execution, the system should record:

- pipeline name;
- run identifier;
- start timestamp;
- end timestamp;
- execution duration;
- execution status;
- records read;
- records written;
- records rejected;
- retry count; and
- error information where applicable.

---

### 11.2 Pipeline Status

Pipeline executions shall have clearly defined states.

Examples include:

- scheduled;
- running;
- successful;
- failed;
- retrying;
- skipped; and
- cancelled.

Pipeline status shall be accessible through the orchestration and monitoring layer.

---

### 11.3 Data Freshness Monitoring

The platform shall monitor the freshness of critical datasets.

Freshness shall be calculated using the difference between the current time and the most recent valid data timestamp.

The system shall support dataset-specific freshness thresholds.

For example:

```text
Vehicle telemetry:
Maximum expected age = 5 minutes

Weather:
Maximum expected age = 2 hours

Daily transit data:
Maximum expected age = 26 hours