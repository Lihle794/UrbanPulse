import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = "traffic.observations"

BRONZE_PATH = (
    "s3a://urbanpulse/bronze/traffic_observations/"
)

CHECKPOINT_PATH = (
    "s3a://urbanpulse/checkpoints/traffic_observations/"
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
    """
    Create a Spark session configured for Kafka
    ingestion and MinIO/S3A storage.
    """

    return (
        SparkSession.builder
        .appName(
            "UrbanPulseTrafficObservationBronze"
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


def read_kafka_stream(spark):
    """
    Read synthetic traffic observations from Kafka.
    """

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "subscribe",
            KAFKA_TOPIC,
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
    Preserve the raw Kafka event and operational
    metadata in the Bronze layer.
    """

    return (
        kafka_stream
        .select(
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
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )
    )


def write_bronze_stream(bronze_stream):
    """
    Persist raw traffic events to MinIO as Parquet.
    """

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

    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = read_kafka_stream(
        spark
    )

    bronze_stream = build_bronze_stream(
        kafka_stream
    )

    query = write_bronze_stream(
        bronze_stream
    )

    print()
    print(
        "UrbanPulse traffic Bronze "
        "stream started."
    )

    print(
        f"Kafka topic: {KAFKA_TOPIC}"
    )

    print(
        f"Bronze destination: "
        f"{BRONZE_PATH}"
    )

    print(
        f"Checkpoint destination: "
        f"{CHECKPOINT_PATH}"
    )

    print(
        "Waiting for traffic observations..."
    )

    try:
        query.awaitTermination()

    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()