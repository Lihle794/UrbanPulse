import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


SILVER_PATH = (
    "s3a://urbanpulse/silver/traffic_observations/"
)

QUARANTINE_PATH = (
    "s3a://urbanpulse/quarantine/traffic_observations/"
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
        .appName(
            "UrbanPulseTrafficInspection"
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


def read_dataset(
    spark,
    path,
    dataset_name,
):
    """
    Read a Parquet dataset from MinIO.

    Returns None when the path does not yet contain
    readable Parquet data.
    """

    try:
        dataframe = (
            spark.read
            .format("parquet")
            .load(path)
        )

        return dataframe

    except Exception as error:
        print()
        print(
            f"{dataset_name} could not be read."
        )

        print(
            "This normally means the dataset "
            "does not contain Parquet records yet."
        )

        print(
            f"Spark message: {error}"
        )

        return None


def inspect_silver(
    silver_df,
):
    print()
    print(
        "=" * 80
    )

    print(
        "URBANPULSE — TRAFFIC SILVER SAMPLE"
    )

    print(
        "=" * 80
    )

    if silver_df is None:
        print(
            "No readable Silver dataset found."
        )

        return 0

    count = silver_df.count()

    if count == 0:
        print(
            "No valid traffic observations "
            "are currently stored in Silver."
        )

        return 0

    silver_df.select(
        "event_id",
        "event_timestamp",
        "segment_id",
        "road_name",
        "average_speed_kmh",
        "vehicle_count",
        "congestion_level",
        "travel_time_seconds",
        "quality_status",
    ).orderBy(
        col(
            "event_timestamp"
        ).desc()
    ).show(
        20,
        truncate=False,
    )

    print(
        f"Valid Silver records: {count}"
    )

    return count


def inspect_congestion(
    silver_df,
):
    print()
    print(
        "=" * 80
    )

    print(
        "CONGESTION DISTRIBUTION"
    )

    print(
        "=" * 80
    )

    if silver_df is None:
        print(
            "No Silver data available."
        )

        return

    if silver_df.count() == 0:
        print(
            "No Silver data available."
        )

        return

    (
        silver_df
        .groupBy(
            "congestion_level"
        )
        .count()
        .orderBy(
            col("count").desc()
        )
        .show(
            truncate=False
        )
    )


def inspect_quarantine(
    quarantine_df,
):
    print()
    print(
        "=" * 80
    )

    print(
        "URBANPULSE — TRAFFIC QUARANTINE"
    )

    print(
        "=" * 80
    )

    if quarantine_df is None:
        print(
            "No readable quarantine "
            "dataset found."
        )

        return 0

    count = quarantine_df.count()

    if count == 0:
        print(
            "No invalid traffic observations "
            "are currently stored in quarantine."
        )

        return 0

    quarantine_df.select(
        "event_id",
        "segment_id",
        "road_name",
        "average_speed_kmh",
        "vehicle_count",
        "congestion_level",
        "travel_time_seconds",
        "rejection_reason",
    ).show(
        50,
        truncate=False,
    )

    print()
    print(
        "Rejection reason distribution:"
    )

    (
        quarantine_df
        .groupBy(
            "rejection_reason"
        )
        .count()
        .orderBy(
            col("count").desc()
        )
        .show(
            truncate=False
        )
    )

    return count


def print_quality_summary(
    valid_count,
    quarantine_count,
):
    total = (
        valid_count
        + quarantine_count
    )

    print()
    print(
        "=" * 80
    )

    print(
        "URBANPULSE — TRAFFIC DATA QUALITY SUMMARY"
    )

    print(
        "=" * 80
    )

    print(
        f"Valid Silver records: "
        f"{valid_count}"
    )

    print(
        f"Quarantined records: "
        f"{quarantine_count}"
    )

    print(
        f"Total inspected records: "
        f"{total}"
    )

    if total == 0:
        print(
            "Valid percentage: 0.00%"
        )

        print(
            "Quarantined percentage: 0.00%"
        )

        return

    valid_percentage = (
        valid_count
        / total
        * 100
    )

    quarantine_percentage = (
        quarantine_count
        / total
        * 100
    )

    print(
        f"Valid percentage: "
        f"{valid_percentage:.2f}%"
    )

    print(
        f"Quarantined percentage: "
        f"{quarantine_percentage:.2f}%"
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    print()
    print(
        "=" * 80
    )

    print(
        "URBANPULSE TRAFFIC DATA INSPECTION"
    )

    print(
        "=" * 80
    )

    print(
        f"Silver path: {SILVER_PATH}"
    )

    print(
        f"Quarantine path: "
        f"{QUARANTINE_PATH}"
    )

    silver_df = read_dataset(
        spark,
        SILVER_PATH,
        "Traffic Silver",
    )

    quarantine_df = read_dataset(
        spark,
        QUARANTINE_PATH,
        "Traffic quarantine",
    )

    valid_count = inspect_silver(
        silver_df
    )

    inspect_congestion(
        silver_df
    )

    quarantine_count = (
        inspect_quarantine(
            quarantine_df
        )
    )

    print_quality_summary(
        valid_count,
        quarantine_count,
    )

    print()
    print(
        "=" * 80
    )

    print(
        "URBANPULSE TRAFFIC INSPECTION COMPLETE"
    )

    print(
        "=" * 80
    )

    spark.stop()

    print(
        "Spark session stopped successfully."
    )


if __name__ == "__main__":
    main()