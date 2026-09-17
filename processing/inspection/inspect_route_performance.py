import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    max as spark_max,
    min as spark_min,
)


GOLD_PATH = "s3a://urbanpulse/gold/route_performance/"

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
        .appName("UrbanPulseRoutePerformanceInspection")
        .master("local[2]")
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
        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )
        .getOrCreate()
    )


def print_section(title):
    print()
    print("=" * 90)
    print(title)
    print("=" * 90)


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:
        print_section(
            "URBANPULSE - GOLD ROUTE PERFORMANCE INSPECTION"
        )

        print(f"Reading Gold dataset from: {GOLD_PATH}")

        gold_df = (
            spark.read
            .format("parquet")
            .load(GOLD_PATH)
        )

        # Cache because we deliberately perform several
        # validation actions against this small Gold dataset.
        gold_df.cache()

        total_rows = gold_df.count()

        print_section("GOLD DATASET SCHEMA")

        gold_df.printSchema()

        print_section("GOLD ROUTE PERFORMANCE RECORDS")

        gold_df.orderBy(
            "route_id",
            "window_start",
        ).show(
            total_rows,
            truncate=False,
            vertical=False,
        )

        print_section("GOLD DATASET SUMMARY")

        distinct_routes = (
            gold_df
            .select("route_id")
            .distinct()
            .count()
        )

        print(
            f"Total Gold rows: {total_rows}"
        )

        print(
            f"Distinct routes: {distinct_routes}"
        )

        if total_rows > 0:
            window_bounds = (
                gold_df
                .agg(
                    spark_min(
                        col("window_start")
                    ).alias("earliest_window"),
                    spark_max(
                        col("window_end")
                    ).alias("latest_window"),
                )
                .first()
            )

            print(
                "Earliest Gold window: "
                f"{window_bounds['earliest_window']}"
            )

            print(
                "Latest Gold window: "
                f"{window_bounds['latest_window']}"
            )

        print_section("ROWS PER ROUTE")

        (
            gold_df
            .groupBy("route_id")
            .agg(
                count("*").alias("gold_rows"),
            )
            .orderBy("route_id")
            .show(
                truncate=False
            )
        )

        print_section("BASIC DATA QUALITY CHECKS")

        null_route_count = (
            gold_df
            .filter(
                col("route_id").isNull()
            )
            .count()
        )

        null_window_start_count = (
            gold_df
            .filter(
                col("window_start").isNull()
            )
            .count()
        )

        null_window_end_count = (
            gold_df
            .filter(
                col("window_end").isNull()
            )
            .count()
        )

        duplicate_route_window_count = (
            gold_df
            .groupBy(
                "route_id",
                "window_start",
                "window_end",
            )
            .agg(
                count("*").alias("record_count"),
            )
            .filter(
                col("record_count") > 1
            )
            .count()
        )

        unique_route_windows = (
            gold_df
            .select(
                "route_id",
                "window_start",
                "window_end",
            )
            .distinct()
            .count()
        )

        print(
            f"Null route IDs: {null_route_count}"
        )

        print(
            "Null window starts: "
            f"{null_window_start_count}"
        )

        print(
            "Null window ends: "
            f"{null_window_end_count}"
        )

        print(
            "Duplicate route-window combinations: "
            f"{duplicate_route_window_count}"
        )

        print(
            "Unique route-window combinations: "
            f"{unique_route_windows}"
        )

        print_section("GOLD VALIDATION RESULT")

        checks_passed = (
            total_rows > 0
            and null_route_count == 0
            and null_window_start_count == 0
            and null_window_end_count == 0
            and duplicate_route_window_count == 0
            and unique_route_windows == total_rows
        )

        if checks_passed:
            print(
                "PASS - Gold route performance dataset "
                "passed structural validation."
            )
        else:
            print(
                "REVIEW REQUIRED - One or more Gold "
                "validation checks failed."
            )

        print_section(
            "URBANPULSE GOLD ROUTE PERFORMANCE "
            "INSPECTION COMPLETE"
        )

    finally:
        try:
            gold_df.unpersist()
        except UnboundLocalError:
            pass

        spark.stop()

        print("Spark session stopped successfully.")


if __name__ == "__main__":
    main()