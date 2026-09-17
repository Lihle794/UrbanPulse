import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


SILVER_PATH = "s3a://urbanpulse/silver/incidents/"
QUARANTINE_PATH = "s3a://urbanpulse/quarantine/incidents/"

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
    """Create a local Spark session configured for MinIO."""

    return (
        SparkSession.builder
        .appName("UrbanPulseIncidentInspection")
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


def path_exists(spark, path):
    """Check whether a MinIO/S3A path exists."""

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    path_object = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
        path
    )

    filesystem = path_object.getFileSystem(
        hadoop_conf
    )

    return filesystem.exists(path_object)


def read_parquet_if_available(spark, path):
    """Read Parquet data only when the path exists."""

    if not path_exists(spark, path):
        return None

    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def inspect_silver(silver_df):
    print()
    print("=" * 90)
    print("URBANPULSE - VALID INCIDENT SILVER DATA")
    print("=" * 90)

    if silver_df is None:
        print("Silver incident data does not exist yet.")
        return 0

    count = silver_df.count()

    if count == 0:
        print("No valid incident records are currently stored in Silver.")
        return 0

    print(f"Valid incident records: {count}")
    print()

    print("Silver schema:")
    silver_df.printSchema()

    print()
    print("Sample valid incident records:")

    columns_to_show = [
        column
        for column in [
            "event_id",
            "incident_id",
            "event_timestamp",
            "road_name",
            "incident_type",
            "severity",
            "status",
            "latitude",
            "longitude",
            "quality_status",
        ]
        if column in silver_df.columns
    ]

    silver_df.select(
        *columns_to_show
    ).show(
        10,
        truncate=False,
    )

    return count


def inspect_quarantine(quarantine_df):
    print()
    print("=" * 90)
    print("URBANPULSE - INCIDENT QUARANTINE")
    print("=" * 90)

    if quarantine_df is None:
        print(
            "No incident quarantine dataset "
            "currently exists."
        )
        return 0

    count = quarantine_df.count()

    if count == 0:
        print(
            "No invalid incident records are "
            "currently stored in quarantine."
        )
        return 0

    print(f"Quarantined incident records: {count}")
    print()

    print("Sample quarantined records:")

    columns_to_show = [
        column
        for column in [
            "event_id",
            "incident_id",
            "event_timestamp_raw",
            "road_name",
            "incident_type",
            "severity",
            "status",
            "latitude",
            "longitude",
            "rejection_reason",
        ]
        if column in quarantine_df.columns
    ]

    quarantine_df.select(
        *columns_to_show
    ).show(
        20,
        truncate=False,
    )

    if "rejection_reason" in quarantine_df.columns:
        print()
        print("Rejection reasons:")

        quarantine_df.groupBy(
            "rejection_reason"
        ).count().orderBy(
            desc("count")
        ).show(
            50,
            truncate=False,
        )

    return count


def print_summary(valid_count, quarantine_count):
    total_count = valid_count + quarantine_count

    if total_count > 0:
        valid_percentage = (
            valid_count / total_count
        ) * 100

        quarantine_percentage = (
            quarantine_count / total_count
        ) * 100
    else:
        valid_percentage = 0.0
        quarantine_percentage = 0.0

    print()
    print("=" * 90)
    print("URBANPULSE - INCIDENT DATA QUALITY SUMMARY")
    print("=" * 90)

    print(
        f"Valid Silver records: {valid_count}"
    )

    print(
        f"Quarantined records: {quarantine_count}"
    )

    print(
        f"Total inspected records: {total_count}"
    )

    print(
        f"Valid percentage: {valid_percentage:.2f}%"
    )

    print(
        "Quarantined percentage: "
        f"{quarantine_percentage:.2f}%"
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        silver_df = read_parquet_if_available(
            spark,
            SILVER_PATH,
        )

        quarantine_df = read_parquet_if_available(
            spark,
            QUARANTINE_PATH,
        )

        valid_count = inspect_silver(
            silver_df
        )

        quarantine_count = inspect_quarantine(
            quarantine_df
        )

        print_summary(
            valid_count,
            quarantine_count,
        )

        print()
        print("=" * 90)
        print("URBANPULSE INCIDENT INSPECTION COMPLETE")
        print("=" * 90)

    finally:
        spark.stop()

        print(
            "Spark session stopped successfully."
        )


if __name__ == "__main__":
    main()