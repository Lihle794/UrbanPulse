import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = "incidents.events"

BRONZE_PATH = (
    "s3a://urbanpulse/bronze/incidents/"
)

CHECKPOINT_PATH = (
    "s3a://urbanpulse/checkpoints/incidents/"
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


def create_spark_session():
    return (
        SparkSession.builder
        .appName("UrbanPulseIncidentBronze")
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


def read_incident_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "subscribe",
            TOPIC,
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )


def build_bronze_stream(kafka_stream):
    """
    Preserve the original Kafka payload together
    with Kafka metadata.

    Bronze intentionally performs no business-level
    transformations or data-quality filtering.
    """

    return kafka_stream.select(
        col("key")
        .cast("string")
        .alias("kafka_key"),

        col("value")
        .cast("string")
        .alias("raw_payload"),

        col("topic"),

        col("partition"),

        col("offset"),

        col("timestamp")
        .alias("kafka_timestamp"),

        current_timestamp()
        .alias("ingestion_timestamp"),
    )


def write_bronze_stream(bronze_stream):
    return (
        bronze_stream.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            BRONZE_PATH,
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH,
        )
        .start()
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    kafka_stream = (
        read_incident_stream(
            spark
        )
    )

    bronze_stream = (
        build_bronze_stream(
            kafka_stream
        )
    )

    query = write_bronze_stream(
        bronze_stream
    )

    print()
    print(
        "=" * 72
    )
    print(
        "URBANPULSE — INCIDENT BRONZE PIPELINE"
    )
    print(
        "=" * 72
    )

    print(
        f"Kafka topic: {TOPIC}"
    )

    print(
        f"Bronze destination: {BRONZE_PATH}"
    )

    print(
        f"Checkpoint: {CHECKPOINT_PATH}"
    )

    print(
        "Data classification: "
        "SYNTHETIC / SIMULATED"
    )

    print()
    print(
        "Incident Bronze stream started."
    )

    print(
        "Waiting for incident events..."
    )

    try:
        query.awaitTermination()

    except KeyboardInterrupt:
        print()
        print(
            "Stopping Incident Bronze stream..."
        )

    finally:
        if query.isActive:
            query.stop()

        spark.stop()

        print(
            "Incident Bronze stream stopped."
        )


if __name__ == "__main__":
    main()