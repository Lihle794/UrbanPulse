import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    lit,
    to_timestamp,
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


BRONZE_PATH = "s3a://urbanpulse/bronze/vehicle_telemetry/"
SILVER_PATH = "s3a://urbanpulse/silver/vehicle_telemetry/"
QUARANTINE_PATH = "s3a://urbanpulse/quarantine/vehicle_telemetry/"

SILVER_CHECKPOINT_PATH = (
    "s3a://urbanpulse/checkpoints/vehicle_telemetry_silver/"
)

QUARANTINE_CHECKPOINT_PATH = (
    "s3a://urbanpulse/checkpoints/vehicle_telemetry_quarantine/"
)

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


BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_key", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField(
            "ingestion_timestamp",
            TimestampType(),
            True,
        ),
    ]
)


TELEMETRY_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_version", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed_kmh", DoubleType(), True),
        StructField("heading", IntegerType(), True),
        StructField("occupancy_status", StringType(), True),
        StructField("vehicle_status", StringType(), True),
    ]
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("UrbanPulseVehicleTelemetrySilver")
        .master("local[*]")
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


def read_bronze_stream(spark):
    return (
        spark.readStream
        .schema(BRONZE_SCHEMA)
        .format("parquet")
        .load(BRONZE_PATH)
    )


def parse_telemetry(bronze_stream):
    return (
        bronze_stream
        .withColumn(
            "event",
            from_json(
                col("raw_payload"),
                TELEMETRY_SCHEMA,
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
            col("event.event_id").alias("event_id"),
            col("event.event_type").alias("event_type"),
            col("event.event_version").alias(
                "event_version"
            ),
            col("event.event_timestamp").alias(
                "event_timestamp_raw"
            ),
            col("event.source").alias("source"),
            col("event.vehicle_id").alias("vehicle_id"),
            col("event.route_id").alias("route_id"),
            col("event.latitude").alias("latitude"),
            col("event.longitude").alias("longitude"),
            col("event.speed_kmh").alias("speed_kmh"),
            col("event.heading").alias("heading"),
            col("event.occupancy_status").alias(
                "occupancy_status"
            ),
            col("event.vehicle_status").alias(
                "vehicle_status"
            ),
        )
        .withColumn(
            "event_timestamp",
            to_timestamp(
                col("event_timestamp_raw")
            ),
        )
        .withColumn(
            "processed_at",
            current_timestamp(),
        )
    )


def apply_quality_rules(parsed_stream):
    return (
        parsed_stream
        .withColumn(
            "reason_missing_event_id",
            when(
                col("event_id").isNull()
                | (col("event_id") == ""),
                lit("missing_event_id"),
            ),
        )
        .withColumn(
            "reason_invalid_event_type",
            when(
                col("event_type").isNull()
                | (
                    col("event_type")
                    != "vehicle_telemetry"
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
            "reason_missing_source",
            when(
                col("source").isNull()
                | (col("source") == ""),
                lit("missing_source"),
            ),
        )
        .withColumn(
            "reason_missing_vehicle_id",
            when(
                col("vehicle_id").isNull()
                | (col("vehicle_id") == ""),
                lit("missing_vehicle_id"),
            ),
        )
        .withColumn(
            "reason_missing_route_id",
            when(
                col("route_id").isNull()
                | (col("route_id") == ""),
                lit("missing_route_id"),
            ),
        )
        .withColumn(
            "reason_invalid_timestamp",
            when(
                col("event_timestamp").isNull(),
                lit("invalid_event_timestamp"),
            ),
        )
        .withColumn(
            "reason_invalid_latitude",
            when(
                col("latitude").isNull()
                | (col("latitude") < -26.5)
                | (col("latitude") > -25.8),
                lit("invalid_latitude"),
            ),
        )
        .withColumn(
            "reason_invalid_longitude",
            when(
                col("longitude").isNull()
                | (col("longitude") < 27.7)
                | (col("longitude") > 28.4),
                lit("invalid_longitude"),
            ),
        )
        .withColumn(
            "reason_invalid_speed",
            when(
                col("speed_kmh").isNull()
                | (col("speed_kmh") < 0)
                | (col("speed_kmh") > 120),
                lit("invalid_speed"),
            ),
        )
        .withColumn(
            "reason_invalid_heading",
            when(
                col("heading").isNull()
                | (col("heading") < 0)
                | (col("heading") > 359),
                lit("invalid_heading"),
            ),
        )
        .withColumn(
            "reason_invalid_occupancy",
            when(
                col("occupancy_status").isNull()
                | (
                    ~col("occupancy_status").isin(
                        "low",
                        "moderate",
                        "high",
                    )
                ),
                lit("invalid_occupancy_status"),
            ),
        )
        .withColumn(
            "reason_invalid_vehicle_status",
            when(
                col("vehicle_status").isNull()
                | (
                    ~col("vehicle_status").isin(
                        "in_service",
                        "delayed",
                        "out_of_service",
                    )
                ),
                lit("invalid_vehicle_status"),
            ),
        )
        .withColumn(
            "rejection_reason",
            concat_ws(
                ",",
                col("reason_missing_event_id"),
                col("reason_invalid_event_type"),
                col("reason_invalid_event_version"),
                col("reason_missing_source"),
                col("reason_missing_vehicle_id"),
                col("reason_missing_route_id"),
                col("reason_invalid_timestamp"),
                col("reason_invalid_latitude"),
                col("reason_invalid_longitude"),
                col("reason_invalid_speed"),
                col("reason_invalid_heading"),
                col("reason_invalid_occupancy"),
                col(
                    "reason_invalid_vehicle_status"
                ),
            ),
        )
        .withColumn(
            "quality_status",
            when(
                col("rejection_reason") == "",
                lit("valid"),
            ).otherwise(
                lit("invalid")
            ),
        )
    )


def build_valid_stream(quality_stream):
    return (
        quality_stream
        .filter(
            col("quality_status") == "valid"
        )
        .select(
            "event_id",
            "event_type",
            "event_version",
            "event_timestamp",
            "source",
            "vehicle_id",
            "route_id",
            "latitude",
            "longitude",
            "speed_kmh",
            "heading",
            "occupancy_status",
            "vehicle_status",
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "ingestion_timestamp",
            "processed_at",
            "quality_status",
        )
        .withWatermark(
            "event_timestamp",
            "10 minutes",
        )
        .dropDuplicates(
            ["event_id"]
        )
    )


def build_quarantine_stream(quality_stream):
    return (
        quality_stream
        .filter(
            col("quality_status") == "invalid"
        )
        .select(
            "event_id",
            "event_type",
            "event_version",
            "event_timestamp_raw",
            "source",
            "vehicle_id",
            "route_id",
            "latitude",
            "longitude",
            "speed_kmh",
            "heading",
            "occupancy_status",
            "vehicle_status",
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


def write_silver_stream(valid_stream):
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


def write_quarantine_stream(
    quarantine_stream
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


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    bronze_stream = read_bronze_stream(
        spark
    )

    parsed_stream = parse_telemetry(
        bronze_stream
    )

    quality_stream = apply_quality_rules(
        parsed_stream
    )

    valid_stream = build_valid_stream(
        quality_stream
    )

    quarantine_stream = (
        build_quarantine_stream(
            quality_stream
        )
    )

    silver_query = write_silver_stream(
        valid_stream
    )

    quarantine_query = (
        write_quarantine_stream(
            quarantine_stream
        )
    )

    print(
        "UrbanPulse Silver quality pipeline started."
    )
    print(
        f"Reading Bronze data from: "
        f"{BRONZE_PATH}"
    )
    print(
        f"Valid data -> {SILVER_PATH}"
    )
    print(
        f"Invalid data -> {QUARANTINE_PATH}"
    )
    print(
        "Waiting for vehicle telemetry..."
    )

    try:
        spark.streams.awaitAnyTermination()

    finally:
        silver_query.stop()
        quarantine_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()