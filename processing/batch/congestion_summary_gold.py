import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# URBANPULSE - GOLD CONGESTION SUMMARY
# ============================================================
#
# Purpose:
# Transform validated Silver traffic observations into an
# analytics-ready congestion summary.
#
# Grain:
# One row per road/segment per 15-minute window.
#
# Data classification:
# SYNTHETIC / SIMULATED
# ============================================================


# ------------------------------------------------------------
# Storage paths
# ------------------------------------------------------------

SILVER_TRAFFIC_PATH = (
    "s3a://urbanpulse/silver/traffic_observations/"
)

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


# ------------------------------------------------------------
# Gold configuration
# ------------------------------------------------------------

WINDOW_DURATION = "15 minutes"


def create_spark_session():
    """
    Create the Spark session configured to communicate with
    the local MinIO object store through the S3A filesystem.
    """

    return (
        SparkSession.builder
        .appName("UrbanPulseGoldCongestionSummary")
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


def read_silver_traffic(spark):
    """
    Read validated Silver traffic observations.

    Silver is treated as the trusted source for this Gold
    transformation.
    """

    return (
        spark.read
        .format("parquet")
        .load(SILVER_TRAFFIC_PATH)
    )


def select_valid_gold_input(traffic_df):
    """
    Select records eligible for Gold aggregation.

    Silver should already contain valid records, but these
    checks protect the Gold layer from structurally unusable
    observations.
    """

    filtered_df = traffic_df

    if "quality_status" in traffic_df.columns:
        filtered_df = filtered_df.filter(
            F.col("quality_status") == "valid"
        )

    required_conditions = []

    if "event_timestamp" in traffic_df.columns:
        required_conditions.append(
            F.col("event_timestamp").isNotNull()
        )

    if "road_name" in traffic_df.columns:
        required_conditions.append(
            F.col("road_name").isNotNull()
        )

    for condition in required_conditions:
        filtered_df = filtered_df.filter(condition)

    return filtered_df


def determine_location_column(traffic_df):
    """
    Determine the most appropriate road/segment identifier
    available in the Silver traffic dataset.

    UrbanPulse prefers segment_id when available because it
    provides a more precise analytical grain. If the current
    traffic schema does not contain segment_id, road_name is
    used instead.
    """

    if "segment_id" in traffic_df.columns:
        return "segment_id"

    if "road_name" in traffic_df.columns:
        return "road_name"

    raise ValueError(
        "Traffic Silver dataset must contain either "
        "'segment_id' or 'road_name'."
    )


def determine_speed_column(traffic_df):
    """
    Find the traffic speed field available in Silver.
    """

    candidates = [
        "average_speed_kmh",
        "avg_speed_kmh",
        "speed_kmh",
    ]

    for candidate in candidates:
        if candidate in traffic_df.columns:
            return candidate

    raise ValueError(
        "Could not find a supported traffic speed column. "
        "Expected one of: "
        "average_speed_kmh, avg_speed_kmh, speed_kmh."
    )


def determine_congestion_column(traffic_df):
    """
    Find a numeric congestion field when one exists.

    The function returns None when congestion is represented
    only by a categorical field. The Gold model can still be
    built from speed metrics in that case.
    """

    candidates = [
        "congestion_score",
        "congestion_level_numeric",
        "congestion_index",
    ]

    for candidate in candidates:
        if candidate in traffic_df.columns:
            return candidate

    return None


def build_congestion_summary(traffic_df):
    """
    Aggregate Silver traffic observations into 15-minute
    road/segment congestion summaries.
    """

    location_column = determine_location_column(
        traffic_df
    )

    speed_column = determine_speed_column(
        traffic_df
    )

    congestion_column = determine_congestion_column(
        traffic_df
    )

    working_df = traffic_df.withColumn(
        "traffic_window",
        F.window(
            F.col("event_timestamp"),
            WINDOW_DURATION,
        ),
    )

    aggregations = [
        F.count("*").alias(
            "observation_count"
        ),
        F.avg(
            F.col(speed_column)
        ).alias(
            "average_speed_kmh"
        ),
        F.min(
            F.col(speed_column)
        ).alias(
            "minimum_speed_kmh"
        ),
        F.max(
            F.col(speed_column)
        ).alias(
            "maximum_speed_kmh"
        ),
    ]

    if congestion_column is not None:
        aggregations.extend(
            [
                F.avg(
                    F.col(congestion_column)
                ).alias(
                    "average_congestion_score"
                ),
                F.max(
                    F.col(congestion_column)
                ).alias(
                    "maximum_congestion_score"
                ),
            ]
        )

    grouped_df = (
        working_df
        .groupBy(
            F.col(location_column),
            F.col("traffic_window"),
        )
        .agg(*aggregations)
    )

    result_df = (
        grouped_df
        .select(
            F.col(location_column).alias(
                "road_segment"
            ),
            F.col(
                "traffic_window.start"
            ).alias(
                "window_start"
            ),
            F.col(
                "traffic_window.end"
            ).alias(
                "window_end"
            ),
            *[
                F.col(column)
                for column in grouped_df.columns
                if column
                not in {
                    location_column,
                    "traffic_window",
                }
            ],
        )
        .withColumn(
            "average_speed_kmh",
            F.round(
                F.col("average_speed_kmh"),
                2,
            ),
        )
        .withColumn(
            "minimum_speed_kmh",
            F.round(
                F.col("minimum_speed_kmh"),
                2,
            ),
        )
        .withColumn(
            "maximum_speed_kmh",
            F.round(
                F.col("maximum_speed_kmh"),
                2,
            ),
        )
    )

    if "average_congestion_score" in result_df.columns:
        result_df = (
            result_df
            .withColumn(
                "average_congestion_score",
                F.round(
                    F.col(
                        "average_congestion_score"
                    ),
                    2,
                ),
            )
            .withColumn(
                "maximum_congestion_score",
                F.round(
                    F.col(
                        "maximum_congestion_score"
                    ),
                    2,
                ),
            )
        )

    return result_df


def add_congestion_classification(gold_df):
    """
    Add a dashboard-friendly congestion classification.

    The classification is based on average observed speed.
    This is a synthetic UrbanPulse analytical rule rather
    than an official Johannesburg traffic classification.
    """

    return gold_df.withColumn(
        "congestion_classification",
        F.when(
            F.col("average_speed_kmh") < 15,
            F.lit("severe"),
        )
        .when(
            F.col("average_speed_kmh") < 30,
            F.lit("high"),
        )
        .when(
            F.col("average_speed_kmh") < 45,
            F.lit("moderate"),
        )
        .otherwise(
            F.lit("low")
        ),
    )


def add_gold_metadata(gold_df):
    """
    Add metadata describing the Gold model.
    """

    return (
        gold_df
        .withColumn(
            "gold_generated_at",
            F.current_timestamp(),
        )
        .withColumn(
            "aggregation_window",
            F.lit(WINDOW_DURATION),
        )
        .withColumn(
            "data_classification",
            F.lit("SYNTHETIC"),
        )
        .withColumn(
            "gold_model",
            F.lit("congestion_summary"),
        )
    )


def validate_gold_dataset(gold_df):
    """
    Perform structural quality checks before publishing Gold.

    Gold output must:
    - contain records
    - have no null road/segment identifiers
    - have no null time windows
    - have positive observation counts
    - have no duplicate road-window combinations
    """

    total_rows = gold_df.count()

    null_locations = (
        gold_df
        .filter(
            F.col("road_segment").isNull()
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

    invalid_observation_counts = (
        gold_df
        .filter(
            F.col("observation_count") <= 0
        )
        .count()
    )

    duplicate_rows = (
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

    print()
    print("=" * 80)
    print(
        "URBANPULSE - GOLD CONGESTION VALIDATION"
    )
    print("=" * 80)

    print(
        f"Gold rows: {total_rows}"
    )

    print(
        f"Null road/segment IDs: {null_locations}"
    )

    print(
        f"Null window starts: {null_window_starts}"
    )

    print(
        f"Null window ends: {null_window_ends}"
    )

    print(
        "Invalid observation counts: "
        f"{invalid_observation_counts}"
    )

    print(
        "Duplicate road-window combinations: "
        f"{duplicate_rows}"
    )

    failures = (
        total_rows == 0
        or null_locations > 0
        or null_window_starts > 0
        or null_window_ends > 0
        or invalid_observation_counts > 0
        or duplicate_rows > 0
    )

    if failures:
        raise ValueError(
            "Gold congestion summary failed "
            "structural validation."
        )

    print()
    print(
        "PASS - Gold congestion summary passed "
        "structural validation."
    )


def write_gold_dataset(gold_df):
    """
    Publish the congestion summary to the Gold layer.

    Overwrite is appropriate because this is currently a
    reproducible batch aggregate derived from Silver.
    """

    (
        gold_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .format("parquet")
        .save(GOLD_CONGESTION_PATH)
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:
        print()
        print("=" * 80)
        print(
            "URBANPULSE - GOLD CONGESTION SUMMARY"
        )
        print("=" * 80)

        print(
            f"Silver source: {SILVER_TRAFFIC_PATH}"
        )

        print(
            f"Gold destination: {GOLD_CONGESTION_PATH}"
        )

        print(
            f"Aggregation window: {WINDOW_DURATION}"
        )

        print(
            "Data classification: "
            "SYNTHETIC / SIMULATED"
        )

        print()

        traffic_df = read_silver_traffic(
            spark
        )

        silver_count = traffic_df.count()

        print(
            f"Silver traffic records: {silver_count}"
        )

        eligible_df = select_valid_gold_input(
            traffic_df
        )

        eligible_count = eligible_df.count()

        print(
            "Eligible Gold input records: "
            f"{eligible_count}"
        )

        if eligible_count == 0:
            raise ValueError(
                "No eligible Silver traffic records "
                "are available for Gold processing."
            )

        location_column = determine_location_column(
            eligible_df
        )

        speed_column = determine_speed_column(
            eligible_df
        )

        congestion_column = (
            determine_congestion_column(
                eligible_df
            )
        )

        print(
            "Location grain source: "
            f"{location_column}"
        )

        print(
            "Speed metric source: "
            f"{speed_column}"
        )

        print(
            "Numeric congestion source: "
            f"{congestion_column or 'not available'}"
        )

        gold_df = build_congestion_summary(
            eligible_df
        )

        gold_df = add_congestion_classification(
            gold_df
        )

        gold_df = add_gold_metadata(
            gold_df
        )

        gold_df = gold_df.cache()

        gold_count = gold_df.count()

        print(
            f"Gold congestion rows: {gold_count}"
        )

        print()
        print("Sample Gold records:")
        print()

        gold_df.orderBy(
            F.col("window_start").desc(),
            F.col("road_segment"),
        ).show(
            10,
            truncate=False,
        )

        validate_gold_dataset(
            gold_df
        )

        write_gold_dataset(
            gold_df
        )

        print()
        print("=" * 80)
        print(
            "GOLD CONGESTION SUMMARY BUILD COMPLETE"
        )
        print("=" * 80)

        print(
            f"Published to: {GOLD_CONGESTION_PATH}"
        )

        gold_df.unpersist()

    finally:
        spark.stop()

        print()
        print(
            "Spark session stopped successfully."
        )


if __name__ == "__main__":
    main()