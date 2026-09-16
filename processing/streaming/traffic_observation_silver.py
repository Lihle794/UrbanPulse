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


BRONZE_PATH = (
    "s3a://urbanpulse/bronze/traffic_observations/"
)

SILVER_PATH = (
    "s3a://urbanpulse/silver/traffic_observations/"
)

QUARANTINE_PATH = (
    "s3a://urbanpulse/quarantine/traffic_observations/"
)

SILVER_CHECKPOINT_PATH = (
    "s3a://urbanpulse/checkpoints/"
    "traffic_observations_silver/"
)

QUARANTINE_CHECKPOINT_PATH = (
    "s3a://urbanpulse/checkpoints/"
    "traffic_observations_quarantine/"
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


TRAFFIC_SCHEMA = StructType(
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
            "average_speed_kmh",
            DoubleType(),
            True,
        ),
        StructField(
            "vehicle_count",
            IntegerType(),
            True,
        ),
        StructField(
            "congestion_level",
            StringType(),
            True,
        ),
        StructField(
            "travel_time_seconds",
            IntegerType(),
            True,
        ),
    ]
)


def create_spark_session():
    """
    Create the Spark session used by the
    Traffic Silver quality pipeline.
    """

    return (
        SparkSession.builder
        .appName(
            "UrbanPulseTrafficObservationSilver"
        )
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
    """
    Read raw traffic observations from Bronze.
    """

    return (
        spark.readStream
        .schema(BRONZE_SCHEMA)
        .format("parquet")
        .load(BRONZE_PATH)
    )


def parse_traffic_observations(
    bronze_stream,
):
    """
    Parse the raw JSON payload into typed
    traffic observation fields.
    """

    return (
        bronze_stream
        .withColumn(
            "event",
            from_json(
                col("raw_payload"),
                TRAFFIC_SCHEMA,
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
                "event.average_speed_kmh"
            ).alias(
                "average_speed_kmh"
            ),

            col(
                "event.vehicle_count"
            ).alias(
                "vehicle_count"
            ),

            col(
                "event.congestion_level"
            ).alias(
                "congestion_level"
            ),

            col(
                "event.travel_time_seconds"
            ).alias(
                "travel_time_seconds"
            ),
        )
        .withColumn(
            "event_timestamp",
            to_timestamp(
                col(
                    "event_timestamp_raw"
                )
            ),
        )
        .withColumn(
            "processed_at",
            current_timestamp(),
        )
    )


def apply_quality_rules(
    parsed_stream,
):
    """
    Apply Traffic Silver validation rules.

    Invalid records are annotated with one or
    more rejection reasons.
    """

    return (
        parsed_stream
        .withColumn(
            "reason_missing_event_id",
            when(
                col("event_id").isNull()
                | (col("event_id") == ""),
                lit(
                    "missing_event_id"
                ),
            ),
        )
        .withColumn(
            "reason_invalid_event_type",
            when(
                col("event_type").isNull()
                | (
                    col("event_type")
                    != "traffic_observation"
                ),
                lit(
                    "invalid_event_type"
                ),
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
                lit(
                    "invalid_event_version"
                ),
            ),
        )
        .withColumn(
            "reason_invalid_source",
            when(
                col("source").isNull()
                | (
                    col("source")
                    != "urbanpulse_traffic_simulator"
                ),
                lit(
                    "invalid_source"
                ),
            ),
        )
        .withColumn(
            "reason_missing_segment_id",
            when(
                col("segment_id").isNull()
                | (
                    col("segment_id")
                    == ""
                ),
                lit(
                    "missing_segment_id"
                ),
            ),
        )
        .withColumn(
            "reason_missing_road_name",
            when(
                col("road_name").isNull()
                | (
                    col("road_name")
                    == ""
                ),
                lit(
                    "missing_road_name"
                ),
            ),
        )
        .withColumn(
            "reason_invalid_timestamp",
            when(
                col(
                    "event_timestamp"
                ).isNull(),
                lit(
                    "invalid_event_timestamp"
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
        .withColumn(
            "reason_invalid_speed",
            when(
                col(
                    "average_speed_kmh"
                ).isNull()
                | (
                    col(
                        "average_speed_kmh"
                    )
                    < 0
                )
                | (
                    col(
                        "average_speed_kmh"
                    )
                    > 120
                ),
                lit(
                    "invalid_average_speed"
                ),
            ),
        )
        .withColumn(
            "reason_invalid_vehicle_count",
            when(
                col(
                    "vehicle_count"
                ).isNull()
                | (
                    col(
                        "vehicle_count"
                    )
                    < 0
                )
                | (
                    col(
                        "vehicle_count"
                    )
                    > 500
                ),
                lit(
                    "invalid_vehicle_count"
                ),
            ),
        )
        .withColumn(
            "reason_invalid_congestion",
            when(
                col(
                    "congestion_level"
                ).isNull()
                | (
                    ~col(
                        "congestion_level"
                    ).isin(
                        "free_flow",
                        "moderate",
                        "heavy",
                        "severe",
                    )
                ),
                lit(
                    "invalid_congestion_level"
                ),
            ),
        )
        .withColumn(
            "reason_invalid_travel_time",
            when(
                col(
                    "travel_time_seconds"
                ).isNull()
                | (
                    col(
                        "travel_time_seconds"
                    )
                    < 1
                )
                | (
                    col(
                        "travel_time_seconds"
                    )
                    > 7200
                ),
                lit(
                    "invalid_travel_time"
                ),
            ),
        )
        .withColumn(
            "rejection_reason",
            concat_ws(
                ",",
                col(
                    "reason_missing_event_id"
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
                    "reason_missing_segment_id"
                ),
                col(
                    "reason_missing_road_name"
                ),
                col(
                    "reason_invalid_timestamp"
                ),
                col(
                    "reason_invalid_latitude"
                ),
                col(
                    "reason_invalid_longitude"
                ),
                col(
                    "reason_invalid_speed"
                ),
                col(
                    "reason_invalid_vehicle_count"
                ),
                col(
                    "reason_invalid_congestion"
                ),
                col(
                    "reason_invalid_travel_time"
                ),
            ),
        )
        .withColumn(
            "quality_status",
            when(
                col(
                    "rejection_reason"
                )
                == "",
                lit(
                    "valid"
                ),
            ).otherwise(
                lit(
                    "invalid"
                )
            ),
        )
    )


def build_valid_stream(
    quality_stream,
):
    """
    Keep valid records for the Silver layer.
    """

    return (
        quality_stream
        .filter(
            col(
                "quality_status"
            )
            == "valid"
        )
        .select(
            "event_id",
            "event_type",
            "event_version",
            "event_timestamp",
            "source",
            "segment_id",
            "road_name",
            "latitude",
            "longitude",
            "average_speed_kmh",
            "vehicle_count",
            "congestion_level",
            "travel_time_seconds",
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
            [
                "event_id",
                "event_timestamp",
            ]
        )
    )


def build_quarantine_stream(
    quality_stream,
):
    """
    Preserve invalid records together with the
    raw payload and rejection reasons.
    """

    return (
        quality_stream
        .filter(
            col(
                "quality_status"
            )
            == "invalid"
        )
        .select(
            "event_id",
            "event_type",
            "event_version",
            "event_timestamp_raw",
            "source",
            "segment_id",
            "road_name",
            "latitude",
            "longitude",
            "average_speed_kmh",
            "vehicle_count",
            "congestion_level",
            "travel_time_seconds",
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


def write_silver_stream(
    valid_stream,
):
    """
    Persist validated Traffic Silver records.
    """

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
    quarantine_stream,
):
    """
    Persist rejected traffic observations.
    """

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

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    bronze_stream = (
        read_bronze_stream(
            spark
        )
    )

    parsed_stream = (
        parse_traffic_observations(
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
    print(
        "UrbanPulse Traffic Silver "
        "quality pipeline started."
    )

    print(
        f"Reading Bronze data from: "
        f"{BRONZE_PATH}"
    )

    print(
        f"Valid traffic data -> "
        f"{SILVER_PATH}"
    )

    print(
        f"Invalid traffic data -> "
        f"{QUARANTINE_PATH}"
    )

    print(
        "Waiting for traffic observations..."
    )

    try:
        spark.streams.awaitAnyTermination()

    finally:
        silver_query.stop()
        quarantine_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()