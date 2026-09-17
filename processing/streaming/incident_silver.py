import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    lit,
    try_to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ============================================================
# STORAGE PATHS
# ============================================================

BRONZE_PATH = "s3a://urbanpulse/bronze/incidents/"
SILVER_PATH = "s3a://urbanpulse/silver/incidents/"
QUARANTINE_PATH = "s3a://urbanpulse/quarantine/incidents/"

SILVER_CHECKPOINT_PATH = (
    "s3a://urbanpulse/checkpoints/incidents_silver/"
)

QUARANTINE_CHECKPOINT_PATH = (
    "s3a://urbanpulse/checkpoints/incidents_quarantine/"
)


# ============================================================
# MINIO CONFIGURATION
# ============================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "urbanpulse",
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "urbanpulse123",
)


# ============================================================
# BRONZE SCHEMA
# ============================================================

BRONZE_SCHEMA = StructType(
    [
        StructField(
            "kafka_key",
            StringType(),
            True,
        ),
        StructField(
            "raw_payload",
            StringType(),
            True,
        ),
        StructField(
            "topic",
            StringType(),
            True,
        ),
        StructField(
            "partition",
            IntegerType(),
            True,
        ),
        StructField(
            "offset",
            LongType(),
            True,
        ),
        StructField(
            "kafka_timestamp",
            TimestampType(),
            True,
        ),
        StructField(
            "ingestion_timestamp",
            TimestampType(),
            True,
        ),
    ]
)


# ============================================================
# INCIDENT EVENT SCHEMA
# ============================================================

INCIDENT_SCHEMA = StructType(
    [
        StructField(
            "event_id",
            StringType(),
            True,
        ),
        StructField(
            "event_type",
            StringType(),
            True,
        ),
        StructField(
            "event_version",
            StringType(),
            True,
        ),
        StructField(
            "event_timestamp",
            StringType(),
            True,
        ),
        StructField(
            "source",
            StringType(),
            True,
        ),
        StructField(
            "incident_id",
            StringType(),
            True,
        ),
        StructField(
            "incident_type",
            StringType(),
            True,
        ),
        StructField(
            "severity",
            StringType(),
            True,
        ),
        StructField(
            "status",
            StringType(),
            True,
        ),
        StructField(
            "segment_id",
            StringType(),
            True,
        ),
        StructField(
            "road_name",
            StringType(),
            True,
        ),
        StructField(
            "latitude",
            DoubleType(),
            True,
        ),
        StructField(
            "longitude",
            DoubleType(),
            True,
        ),
        StructField(
            "description",
            StringType(),
            True,
        ),
        StructField(
            "reported_at",
            StringType(),
            True,
        ),
        StructField(
            "estimated_clearance_minutes",
            IntegerType(),
            True,
        ),
    ]
)


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName(
            "UrbanPulseIncidentSilver"
        )
        .master("local[2]")
        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )
        .config(
            "spark.default.parallelism",
            "2",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            MINIO_ENDPOINT,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


# ============================================================
# READ BRONZE
# ============================================================

def read_bronze_stream(spark):
    return (
        spark.readStream
        .schema(BRONZE_SCHEMA)
        .format("parquet")
        .load(BRONZE_PATH)
    )


# ============================================================
# PARSE INCIDENT EVENTS
# ============================================================

def parse_incidents(bronze_stream):
    """
    Parse the raw JSON payload stored in Bronze.

    Timestamp parsing deliberately uses try_to_timestamp().
    Malformed timestamps therefore become NULL rather than
    terminating the Spark streaming query.

    Those NULL values are subsequently detected by the
    Silver data-quality rules and routed to quarantine.
    """

    return (
        bronze_stream
        .withColumn(
            "event",
            from_json(
                col("raw_payload"),
                INCIDENT_SCHEMA,
            ),
        )
        .select(
            "kafka_key",
            "raw_payload",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "ingestion_timestamp",

            col(
                "event.event_id"
            ).alias(
                "event_id"
            ),

            col(
                "event.event_type"
            ).alias(
                "event_type"
            ),

            col(
                "event.event_version"
            ).alias(
                "event_version"
            ),

            col(
                "event.event_timestamp"
            ).alias(
                "event_timestamp_raw"
            ),

            col(
                "event.source"
            ).alias(
                "source"
            ),

            col(
                "event.incident_id"
            ).alias(
                "incident_id"
            ),

            col(
                "event.incident_type"
            ).alias(
                "incident_type"
            ),

            col(
                "event.severity"
            ).alias(
                "severity"
            ),

            col(
                "event.status"
            ).alias(
                "status"
            ),

            col(
                "event.segment_id"
            ).alias(
                "segment_id"
            ),

            col(
                "event.road_name"
            ).alias(
                "road_name"
            ),

            col(
                "event.latitude"
            ).alias(
                "latitude"
            ),

            col(
                "event.longitude"
            ).alias(
                "longitude"
            ),

            col(
                "event.description"
            ).alias(
                "description"
            ),

            col(
                "event.reported_at"
            ).alias(
                "reported_at_raw"
            ),

            col(
                "event.estimated_clearance_minutes"
            ).alias(
                "estimated_clearance_minutes"
            ),
        )

        # ----------------------------------------------------
        # SAFE TIMESTAMP PARSING
        # ----------------------------------------------------
        #
        # try_to_timestamp() returns NULL for malformed
        # timestamp strings instead of raising an exception.
        #
        # Example:
        #
        # "not-a-valid-timestamp"
        #           ↓
        #          NULL
        #           ↓
        # reason_invalid_event_timestamp
        #           ↓
        # quarantine
        #
        # ----------------------------------------------------

        .withColumn(
            "event_timestamp",
            try_to_timestamp(
                col("event_timestamp_raw")
            ),
        )

        .withColumn(
            "reported_at",
            try_to_timestamp(
                col("reported_at_raw")
            ),
        )

        .withColumn(
            "processed_at",
            current_timestamp(),
        )
    )


# ============================================================
# DATA QUALITY RULES
# ============================================================

def apply_quality_rules(parsed_stream):
    """
    Apply Silver-layer incident data-quality rules.

    Every failed rule creates an individual rejection reason.
    Multiple failures can therefore be preserved on the same
    quarantined record.
    """

    return (
        parsed_stream

        # ----------------------------------------------------
        # CORE IDENTIFIERS
        # ----------------------------------------------------

        .withColumn(
            "reason_missing_event_id",
            when(
                col("event_id").isNull()
                | (col("event_id") == ""),
                lit("missing_event_id"),
            ),
        )

        .withColumn(
            "reason_missing_incident_id",
            when(
                col("incident_id").isNull()
                | (col("incident_id") == ""),
                lit("missing_incident_id"),
            ),
        )

        .withColumn(
            "reason_missing_segment_id",
            when(
                col("segment_id").isNull()
                | (col("segment_id") == ""),
                lit("missing_segment_id"),
            ),
        )

        # ----------------------------------------------------
        # EVENT CONTRACT
        # ----------------------------------------------------

        .withColumn(
            "reason_invalid_event_type",
            when(
                col("event_type").isNull()
                | (
                    col("event_type")
                    != "incident"
                ),
                lit("invalid_event_type"),
            ),
        )

        .withColumn(
            "reason_invalid_event_version",
            when(
                col("event_version").isNull()
                | (
                    col("event_version")
                    != "1.0"
                ),
                lit("invalid_event_version"),
            ),
        )

        .withColumn(
            "reason_invalid_source",
            when(
                col("source").isNull()
                | (
                    ~col("source").isin(
                        "urbanpulse_incident_simulator",
                        "urbanpulse_incident_quality_test",
                    )
                ),
                lit("invalid_source"),
            ),
        )

        # ----------------------------------------------------
        # TIMESTAMPS
        # ----------------------------------------------------

        .withColumn(
            "reason_invalid_event_timestamp",
            when(
                col("event_timestamp").isNull(),
                lit(
                    "invalid_event_timestamp"
                ),
            ),
        )

        .withColumn(
            "reason_invalid_reported_at",
            when(
                col("reported_at").isNull(),
                lit(
                    "invalid_reported_at"
                ),
            ),
        )

        # ----------------------------------------------------
        # INCIDENT ATTRIBUTES
        # ----------------------------------------------------

        .withColumn(
            "reason_invalid_incident_type",
            when(
                col("incident_type").isNull()
                | ~col("incident_type").isin(
                    "collision",
                    "road_closure",
                    "vehicle_breakdown",
                    "construction",
                    "road_hazard",
                ),
                lit(
                    "invalid_incident_type"
                ),
            ),
        )

        .withColumn(
            "reason_invalid_severity",
            when(
                col("severity").isNull()
                | ~col("severity").isin(
                    "low",
                    "moderate",
                    "high",
                    "critical",
                ),
                lit(
                    "invalid_severity"
                ),
            ),
        )

        .withColumn(
            "reason_invalid_status",
            when(
                col("status").isNull()
                | ~col("status").isin(
                    "active",
                    "monitoring",
                    "cleared",
                ),
                lit(
                    "invalid_status"
                ),
            ),
        )

        # ----------------------------------------------------
        # LOCATION / REFERENCE DATA
        # ----------------------------------------------------

        .withColumn(
            "reason_missing_road_name",
            when(
                col("road_name").isNull()
                | (col("road_name") == ""),
                lit(
                    "missing_road_name"
                ),
            ),
        )

        .withColumn(
            "reason_invalid_latitude",
            when(
                col("latitude").isNull()
                | (
                    col("latitude")
                    < -26.5
                )
                | (
                    col("latitude")
                    > -25.8
                ),
                lit(
                    "invalid_latitude"
                ),
            ),
        )

        .withColumn(
            "reason_invalid_longitude",
            when(
                col("longitude").isNull()
                | (
                    col("longitude")
                    < 27.7
                )
                | (
                    col("longitude")
                    > 28.4
                ),
                lit(
                    "invalid_longitude"
                ),
            ),
        )

        # ----------------------------------------------------
        # OPERATIONAL FIELDS
        # ----------------------------------------------------

        .withColumn(
            "reason_missing_description",
            when(
                col("description").isNull()
                | (
                    col("description")
                    == ""
                ),
                lit(
                    "missing_description"
                ),
            ),
        )

        .withColumn(
            "reason_invalid_clearance",
            when(
                col(
                    "estimated_clearance_minutes"
                ).isNull()
                | (
                    col(
                        "estimated_clearance_minutes"
                    )
                    < 0
                )
                | (
                    col(
                        "estimated_clearance_minutes"
                    )
                    > 1440
                ),
                lit(
                    "invalid_estimated_clearance_minutes"
                ),
            ),
        )

        # ----------------------------------------------------
        # COMBINE REJECTION REASONS
        # ----------------------------------------------------

        .withColumn(
            "rejection_reason",
            concat_ws(
                ",",
                col(
                    "reason_missing_event_id"
                ),
                col(
                    "reason_missing_incident_id"
                ),
                col(
                    "reason_missing_segment_id"
                ),
                col(
                    "reason_invalid_event_type"
                ),
                col(
                    "reason_invalid_event_version"
                ),
                col(
                    "reason_invalid_source"
                ),
                col(
                    "reason_invalid_event_timestamp"
                ),
                col(
                    "reason_invalid_reported_at"
                ),
                col(
                    "reason_invalid_incident_type"
                ),
                col(
                    "reason_invalid_severity"
                ),
                col(
                    "reason_invalid_status"
                ),
                col(
                    "reason_missing_road_name"
                ),
                col(
                    "reason_invalid_latitude"
                ),
                col(
                    "reason_invalid_longitude"
                ),
                col(
                    "reason_missing_description"
                ),
                col(
                    "reason_invalid_clearance"
                ),
            ),
        )

        .withColumn(
            "quality_status",
            when(
                col("rejection_reason")
                == "",
                lit("valid"),
            ).otherwise(
                lit("invalid")
            ),
        )
    )


# ============================================================
# VALID SILVER STREAM
# ============================================================

def build_valid_stream(
    quality_stream,
):
    return (
        quality_stream
        .filter(
            col("quality_status")
            == "valid"
        )
        .select(
            "event_id",
            "event_type",
            "event_version",
            "event_timestamp",
            "source",
            "incident_id",
            "incident_type",
            "severity",
            "status",
            "segment_id",
            "road_name",
            "latitude",
            "longitude",
            "description",
            "reported_at",
            "estimated_clearance_minutes",
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "ingestion_timestamp",
            "processed_at",
            "quality_status",
        )
    )


# ============================================================
# QUARANTINE STREAM
# ============================================================

def build_quarantine_stream(
    quality_stream,
):
    return (
        quality_stream
        .filter(
            col("quality_status")
            == "invalid"
        )
        .select(
            "event_id",
            "event_type",
            "event_version",
            "event_timestamp_raw",
            "source",
            "incident_id",
            "incident_type",
            "severity",
            "status",
            "segment_id",
            "road_name",
            "latitude",
            "longitude",
            "description",
            "reported_at_raw",
            "estimated_clearance_minutes",
            "raw_payload",
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "ingestion_timestamp",
            "processed_at",
            "quality_status",
            "rejection_reason",
        )
    )


# ============================================================
# WRITE VALID SILVER RECORDS
# ============================================================

def write_silver_stream(
    valid_stream,
):
    return (
        valid_stream.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            SILVER_PATH,
        )
        .option(
            "checkpointLocation",
            SILVER_CHECKPOINT_PATH,
        )
        .start()
    )


# ============================================================
# WRITE INVALID RECORDS TO QUARANTINE
# ============================================================

def write_quarantine_stream(
    quarantine_stream,
):
    return (
        quarantine_stream.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            QUARANTINE_PATH,
        )
        .option(
            "checkpointLocation",
            QUARANTINE_CHECKPOINT_PATH,
        )
        .start()
    )


# ============================================================
# APPLICATION ENTRY POINT
# ============================================================

def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    bronze_stream = (
        read_bronze_stream(
            spark
        )
    )

    parsed_stream = (
        parse_incidents(
            bronze_stream
        )
    )

    quality_stream = (
        apply_quality_rules(
            parsed_stream
        )
    )

    valid_stream = (
        build_valid_stream(
            quality_stream
        )
    )

    quarantine_stream = (
        build_quarantine_stream(
            quality_stream
        )
    )

    silver_query = (
        write_silver_stream(
            valid_stream
        )
    )

    quarantine_query = (
        write_quarantine_stream(
            quarantine_stream
        )
    )

    print()
    print("=" * 72)

    print(
        "URBANPULSE — INCIDENT SILVER QUALITY PIPELINE"
    )

    print("=" * 72)

    print(
        f"Reading Bronze data from: "
        f"{BRONZE_PATH}"
    )

    print(
        f"Valid incidents -> "
        f"{SILVER_PATH}"
    )

    print(
        f"Invalid incidents -> "
        f"{QUARANTINE_PATH}"
    )

    print(
        "Data classification: "
        "SYNTHETIC / SIMULATED"
    )

    print()

    print(
        "Safe timestamp parsing: ENABLED"
    )

    print(
        "Malformed timestamps -> NULL -> quarantine"
    )

    print()

    print(
        "Waiting for incident records..."
    )

    try:

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:

        print()

        print(
            "Stopping Incident Silver pipeline..."
        )

    finally:

        if silver_query.isActive:
            silver_query.stop()

        if quarantine_query.isActive:
            quarantine_query.stop()

        spark.stop()

        print(
            "Incident Silver pipeline stopped."
        )


if __name__ == "__main__":
    main()