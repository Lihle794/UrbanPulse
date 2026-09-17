import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# URBANPULSE - GOLD CONGESTION SUMMARY INSPECTION
# ============================================================
#
# Purpose:
# Independently inspect and validate the persisted Gold
# congestion_summary dataset stored in MinIO.
#
# Data classification:
# SYNTHETIC / SIMULATED
# ============================================================


GOLD_CONGESTION_PATH = (
    "s3a://urbanpulse/gold/congestion_summary/"
)


# ------------------------------------------------------------
# MinIO configuration
# ------------------------------------------------------------

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


EXPECTED_COLUMNS = {
    "road_segment",
    "window_start",
    "window_end",
    "observation_count",
    "average_speed_kmh",
    "minimum_speed_kmh",
    "maximum_speed_kmh",
    "congestion_classification",
    "gold_generated_at",
    "aggregation_window",
    "data_classification",
    "gold_model",
}


VALID_CONGESTION_CLASSES = {
    "low",
    "moderate",
    "high",
    "severe",
}


def create_spark_session():
    """
    Create a small local Spark session configured to read
    Gold data from the UrbanPulse MinIO object store.
    """

    return (
        SparkSession.builder
        .appName(
            "UrbanPulseInspectGoldCongestionSummary"
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


def read_gold_dataset(spark):
    """
    Read the persisted congestion summary directly from Gold.
    """

    return (
        spark.read
        .format("parquet")
        .load(GOLD_CONGESTION_PATH)
    )


def inspect_schema(gold_df):
    """
    Display the persisted Gold schema and verify that the
    expected analytics columns exist.
    """

    print()
    print("=" * 80)
    print(
        "URBANPULSE - GOLD CONGESTION SUMMARY SCHEMA"
    )
    print("=" * 80)

    gold_df.printSchema()

    actual_columns = set(
        gold_df.columns
    )

    missing_columns = (
        EXPECTED_COLUMNS - actual_columns
    )

    print(
        "Expected required columns: "
        f"{len(EXPECTED_COLUMNS)}"
    )

    print(
        "Missing required columns: "
        f"{len(missing_columns)}"
    )

    if missing_columns:
        print(
            "Missing columns: "
            + ", ".join(
                sorted(missing_columns)
            )
        )

    return missing_columns


def inspect_sample_records(gold_df):
    """
    Display a small sample of persisted Gold records.
    """

    print()
    print("=" * 80)
    print(
        "URBANPULSE - GOLD CONGESTION SAMPLE"
    )
    print("=" * 80)

    (
        gold_df
        .orderBy(
            F.col("window_start").desc(),
            F.col("road_segment"),
        )
        .show(
            10,
            truncate=False,
        )
    )


def inspect_classification_distribution(gold_df):
    """
    Show how Gold records are distributed across the
    congestion classifications.
    """

    print()
    print("=" * 80)
    print(
        "URBANPULSE - CONGESTION CLASS DISTRIBUTION"
    )
    print("=" * 80)

    (
        gold_df
        .groupBy(
            "congestion_classification"
        )
        .count()
        .orderBy(
            F.col("count").desc()
        )
        .show(
            truncate=False
        )
    )


def validate_gold_dataset(
    gold_df,
    missing_columns,
):
    """
    Validate the persisted Gold congestion dataset.

    Checks:
    - dataset is not empty
    - expected columns exist
    - road/segment identifiers are present
    - window boundaries are present and ordered correctly
    - observation counts are positive
    - speed metrics are non-negative
    - average speed falls between min and max
    - congestion classifications are recognized
    - no duplicate road-window combinations exist
    - Gold metadata is correct
    """

    print()
    print("=" * 80)
    print(
        "URBANPULSE - GOLD CONGESTION VALIDATION"
    )
    print("=" * 80)

    total_rows = gold_df.count()

    null_road_segments = (
        gold_df
        .filter(
            F.col("road_segment").isNull()
            | (
                F.trim(
                    F.col("road_segment")
                ) == ""
            )
        )
        .count()
    )

    null_window_starts = (
        gold_df
        .filter(
            F.col("window_start").isNull()
        )
        .count()
    )

    null_window_ends = (
        gold_df
        .filter(
            F.col("window_end").isNull()
        )
        .count()
    )

    invalid_windows = (
        gold_df
        .filter(
            F.col("window_end")
            <= F.col("window_start")
        )
        .count()
    )

    invalid_observation_counts = (
        gold_df
        .filter(
            F.col("observation_count").isNull()
            | (
                F.col("observation_count")
                <= 0
            )
        )
        .count()
    )

    invalid_average_speeds = (
        gold_df
        .filter(
            F.col("average_speed_kmh").isNull()
            | (
                F.col("average_speed_kmh")
                < 0
            )
        )
        .count()
    )

    invalid_minimum_speeds = (
        gold_df
        .filter(
            F.col("minimum_speed_kmh").isNull()
            | (
                F.col("minimum_speed_kmh")
                < 0
            )
        )
        .count()
    )

    invalid_maximum_speeds = (
        gold_df
        .filter(
            F.col("maximum_speed_kmh").isNull()
            | (
                F.col("maximum_speed_kmh")
                < 0
            )
        )
        .count()
    )

    inconsistent_speed_ranges = (
        gold_df
        .filter(
            (
                F.col("average_speed_kmh")
                < F.col("minimum_speed_kmh")
            )
            | (
                F.col("average_speed_kmh")
                > F.col("maximum_speed_kmh")
            )
            | (
                F.col("minimum_speed_kmh")
                > F.col("maximum_speed_kmh")
            )
        )
        .count()
    )

    invalid_classifications = (
        gold_df
        .filter(
            F.col(
                "congestion_classification"
            ).isNull()
            | (
                ~F.col(
                    "congestion_classification"
                ).isin(
                    *VALID_CONGESTION_CLASSES
                )
            )
        )
        .count()
    )

    duplicate_combinations = (
        gold_df
        .groupBy(
            "road_segment",
            "window_start",
            "window_end",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    invalid_model_metadata = (
        gold_df
        .filter(
            F.col("gold_model")
            != "congestion_summary"
        )
        .count()
    )

    invalid_classification_metadata = (
        gold_df
        .filter(
            F.col("data_classification")
            != "SYNTHETIC"
        )
        .count()
    )

    invalid_window_metadata = (
        gold_df
        .filter(
            F.col("aggregation_window")
            != "15 minutes"
        )
        .count()
    )

    null_generated_timestamps = (
        gold_df
        .filter(
            F.col("gold_generated_at").isNull()
        )
        .count()
    )

    print(
        f"Gold records: {total_rows}"
    )

    print(
        "Missing required columns: "
        f"{len(missing_columns)}"
    )

    print(
        "Null/blank road segments: "
        f"{null_road_segments}"
    )

    print(
        f"Null window starts: {null_window_starts}"
    )

    print(
        f"Null window ends: {null_window_ends}"
    )

    print(
        f"Invalid time windows: {invalid_windows}"
    )

    print(
        "Invalid observation counts: "
        f"{invalid_observation_counts}"
    )

    print(
        "Invalid average speeds: "
        f"{invalid_average_speeds}"
    )

    print(
        "Invalid minimum speeds: "
        f"{invalid_minimum_speeds}"
    )

    print(
        "Invalid maximum speeds: "
        f"{invalid_maximum_speeds}"
    )

    print(
        "Inconsistent speed ranges: "
        f"{inconsistent_speed_ranges}"
    )

    print(
        "Invalid congestion classifications: "
        f"{invalid_classifications}"
    )

    print(
        "Duplicate road-window combinations: "
        f"{duplicate_combinations}"
    )

    print(
        "Invalid Gold model metadata: "
        f"{invalid_model_metadata}"
    )

    print(
        "Invalid data classification metadata: "
        f"{invalid_classification_metadata}"
    )

    print(
        "Invalid aggregation-window metadata: "
        f"{invalid_window_metadata}"
    )

    print(
        "Null Gold generation timestamps: "
        f"{null_generated_timestamps}"
    )

    failures = (
        total_rows == 0
        or len(missing_columns) > 0
        or null_road_segments > 0
        or null_window_starts > 0
        or null_window_ends > 0
        or invalid_windows > 0
        or invalid_observation_counts > 0
        or invalid_average_speeds > 0
        or invalid_minimum_speeds > 0
        or invalid_maximum_speeds > 0
        or inconsistent_speed_ranges > 0
        or invalid_classifications > 0
        or duplicate_combinations > 0
        or invalid_model_metadata > 0
        or invalid_classification_metadata > 0
        or invalid_window_metadata > 0
        or null_generated_timestamps > 0
    )

    print()
    print("=" * 80)
    print("GOLD VALIDATION RESULT")
    print("=" * 80)

    if failures:
        print(
            "FAIL - Gold congestion summary failed "
            "persisted-data validation."
        )

        raise ValueError(
            "Persisted Gold congestion summary "
            "failed validation."
        )

    print(
        "PASS - Gold congestion summary passed "
        "persisted-data validation."
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:
        print()
        print("=" * 80)
        print(
            "URBANPULSE - GOLD CONGESTION "
            "SUMMARY INSPECTION"
        )
        print("=" * 80)

        print(
            f"Gold source: {GOLD_CONGESTION_PATH}"
        )

        print(
            "Data classification: "
            "SYNTHETIC / SIMULATED"
        )

        gold_df = read_gold_dataset(
            spark
        )

        gold_df = gold_df.cache()

        missing_columns = inspect_schema(
            gold_df
        )

        if missing_columns:
            print()
            print(
                "Required columns are missing. "
                "Skipping row-level inspection."
            )

            raise ValueError(
                "Gold congestion summary schema "
                "validation failed."
            )

        inspect_sample_records(
            gold_df
        )

        inspect_classification_distribution(
            gold_df
        )

        validate_gold_dataset(
            gold_df,
            missing_columns,
        )

        gold_df.unpersist()

        print()
        print("=" * 80)
        print(
            "URBANPULSE GOLD CONGESTION "
            "INSPECTION COMPLETE"
        )
        print("=" * 80)

    finally:
        spark.stop()

        print(
            "Spark session stopped successfully."
        )


if __name__ == "__main__":
    main()