import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc


SILVER_PATH = "s3a://urbanpulse/silver/vehicle_telemetry/"
QUARANTINE_PATH = "s3a://urbanpulse/quarantine/vehicle_telemetry/"

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
    Create a local Spark session configured to read
    UrbanPulse data from MinIO using the S3A filesystem.
    """

    return (
        SparkSession.builder
        .appName("UrbanPulseVehicleTelemetryInspection")
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


def print_section(title):
    """
    Print a clear separator in the terminal.
    """

    print("\n")
    print("=" * 90)
    print(title)
    print("=" * 90)


def inspect_silver(spark):
    """
    Read and inspect valid vehicle telemetry records
    stored in the Silver layer.
    """

    print_section("URBANPULSE — SILVER VEHICLE TELEMETRY")

    try:
        silver_df = (
            spark.read
            .format("parquet")
            .load(SILVER_PATH)
        )

        total_records = silver_df.count()

        print(f"\nSilver path: {SILVER_PATH}")
        print(f"Total Silver records: {total_records}")

        print("\nSilver schema:")
        silver_df.printSchema()

        if total_records == 0:
            print("\nNo Silver records were found.")
            return 0

        print("\nSample Silver records:")

        silver_df.select(
            "event_id",
            "event_timestamp",
            "vehicle_id",
            "route_id",
            "latitude",
            "longitude",
            "speed_kmh",
            "heading",
            "occupancy_status",
            "vehicle_status",
            "quality_status",
        ).orderBy(
            desc("event_timestamp")
        ).show(
            20,
            truncate=False,
        )

        print("\nRecords by vehicle:")

        silver_df.groupBy(
            "vehicle_id"
        ).agg(
            count("*").alias("record_count")
        ).orderBy(
            desc("record_count")
        ).show(
            truncate=False
        )

        print("\nRecords by route:")

        silver_df.groupBy(
            "route_id"
        ).agg(
            count("*").alias("record_count")
        ).orderBy(
            desc("record_count")
        ).show(
            truncate=False
        )

        print("\nVehicle status distribution:")

        silver_df.groupBy(
            "vehicle_status"
        ).agg(
            count("*").alias("record_count")
        ).orderBy(
            desc("record_count")
        ).show(
            truncate=False
        )

        print("\nOccupancy distribution:")

        silver_df.groupBy(
            "occupancy_status"
        ).agg(
            count("*").alias("record_count")
        ).orderBy(
            desc("record_count")
        ).show(
            truncate=False
        )

        return total_records

    except Exception as exc:
        print("\nUnable to inspect Silver data.")
        print(f"Error: {exc}")

        return 0


def inspect_quarantine(spark):
    """
    Read and inspect rejected vehicle telemetry records
    stored in the quarantine layer.
    """

    print_section("URBANPULSE — QUARANTINED VEHICLE TELEMETRY")

    try:
        quarantine_df = (
            spark.read
            .format("parquet")
            .load(QUARANTINE_PATH)
        )

        total_records = quarantine_df.count()

        print(f"\nQuarantine path: {QUARANTINE_PATH}")
        print(f"Total quarantined records: {total_records}")

        print("\nQuarantine schema:")
        quarantine_df.printSchema()

        if total_records == 0:
            print(
                "\nNo invalid records are currently "
                "stored in quarantine."
            )
            return 0

        print("\nQuarantined records:")

        quarantine_df.select(
            "event_id",
            "event_timestamp_raw",
            "vehicle_id",
            "route_id",
            "latitude",
            "longitude",
            "speed_kmh",
            "heading",
            "occupancy_status",
            "vehicle_status",
            "quality_status",
            "rejection_reason",
        ).show(
            50,
            truncate=False,
        )

        print("\nRejection reason summary:")

        quarantine_df.groupBy(
            "rejection_reason"
        ).agg(
            count("*").alias("record_count")
        ).orderBy(
            desc("record_count")
        ).show(
            50,
            truncate=False,
        )

        print("\nRaw payloads for quarantined records:")

        quarantine_df.select(
            "rejection_reason",
            "raw_payload",
        ).show(
            20,
            truncate=False,
        )

        return total_records

    except Exception as exc:
        print("\nUnable to inspect Quarantine data.")
        print(f"Error: {exc}")

        return 0


def print_quality_summary(
    silver_count,
    quarantine_count,
):
    """
    Print a high-level data quality summary.
    """

    print_section("URBANPULSE — DATA QUALITY SUMMARY")

    total_processed = (
        silver_count
        + quarantine_count
    )

    print(f"Valid Silver records: {silver_count}")
    print(
        f"Quarantined records: {quarantine_count}"
    )
    print(
        f"Total inspected records: {total_processed}"
    )

    if total_processed == 0:
        print(
            "\nNo processed telemetry records "
            "were available for inspection."
        )
        return

    valid_percentage = (
        silver_count
        / total_processed
    ) * 100

    invalid_percentage = (
        quarantine_count
        / total_processed
    ) * 100

    print(
        f"Valid percentage: "
        f"{valid_percentage:.2f}%"
    )

    print(
        f"Quarantined percentage: "
        f"{invalid_percentage:.2f}%"
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        silver_count = inspect_silver(
            spark
        )

        quarantine_count = inspect_quarantine(
            spark
        )

        print_quality_summary(
            silver_count,
            quarantine_count,
        )

    finally:
        spark.stop()

        print_section(
            "URBANPULSE INSPECTION COMPLETE"
        )

        print(
            "Spark session stopped successfully."
        )


if __name__ == "__main__":
    main()