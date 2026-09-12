import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "vehicle.telemetry"

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

BRONZE_PATH = "s3a://urbanpulse/bronze/vehicle_telemetry/"
CHECKPOINT_PATH = (
    "s3a://urbanpulse/checkpoints/vehicle_telemetry/"
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("UrbanPulseVehicleTelemetryBronze")
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


def read_vehicle_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )


def build_bronze_stream(raw_stream):
    return (
        raw_stream
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_payload"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )
    )


def write_bronze_stream(bronze_stream):
    return (
        bronze_stream.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH,
        )
        .start()
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = read_vehicle_stream(spark)

    bronze_stream = build_bronze_stream(raw_stream)

    query = write_bronze_stream(bronze_stream)

    print("UrbanPulse Bronze streaming pipeline started.")
    print(f"Kafka topic: {TOPIC}")
    print(f"Bronze path: {BRONZE_PATH}")
    print(f"Checkpoint path: {CHECKPOINT_PATH}")

    query.awaitTermination()


if __name__ == "__main__":
    main()