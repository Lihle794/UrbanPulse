import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    when,
    window,
)


# ============================================================
# STORAGE PATHS
# ============================================================

SILVER_PATH = (
    "s3a://urbanpulse/silver/vehicle_telemetry/"
)

GOLD_PATH = (
    "s3a://urbanpulse/gold/route_performance/"
)


# ============================================================
# MINIO CONFIGURATION
# ============================================================

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


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName(
            "UrbanPulseRoutePerformanceGold"
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


# ============================================================
# READ TRUSTED SILVER DATA
# ============================================================

def read_silver_telemetry(spark):
    """
    Read validated vehicle telemetry from the Silver layer.

    Only records that successfully passed the Silver
    data-quality pipeline should exist in this dataset.
    """

    return (
        spark.read
        .format("parquet")
        .load(SILVER_PATH)
    )


# ============================================================
# PREPARE GOLD INPUT
# ============================================================

def prepare_telemetry(silver_df):
    """
    Select the Silver fields required for route-performance
    analytics.

    Defensive filtering is retained at the Gold boundary so
    incomplete analytical keys cannot enter aggregations.
    """

    return (
        silver_df
        .filter(
            col("quality_status") == "valid"
        )
        .filter(
            col("event_timestamp").isNotNull()
        )
        .filter(
            col("route_id").isNotNull()
        )
        .filter(
            col("vehicle_id").isNotNull()
        )
        .filter(
            col("speed_kmh").isNotNull()
        )
        .select(
            "event_timestamp",
            "vehicle_id",
            "route_id",
            "speed_kmh",
            "occupancy_status",
            "vehicle_status",
        )
    )


# ============================================================
# BUILD ROUTE PERFORMANCE FACT
# ============================================================

def build_route_performance(
    telemetry_df,
):
    """
    Gold grain:

        One row per route per 15-minute event-time window.

    Measures describe vehicle activity, speed behaviour,
    occupancy and operational status for the route during
    that period.
    """

    aggregated = (
        telemetry_df
        .groupBy(
            window(
                col("event_timestamp"),
                "15 minutes",
            ),
            col("route_id"),
        )
        .agg(
            # ------------------------------------------------
            # EVENT / VEHICLE ACTIVITY
            # ------------------------------------------------

            count("*").alias(
                "observation_count"
            ),

            countDistinct(
                "vehicle_id"
            ).alias(
                "active_vehicle_count"
            ),

            # ------------------------------------------------
            # SPEED METRICS
            # ------------------------------------------------

            avg(
                "speed_kmh"
            ).alias(
                "avg_speed_kmh"
            ),

            spark_min(
                "speed_kmh"
            ).alias(
                "min_speed_kmh"
            ),

            spark_max(
                "speed_kmh"
            ).alias(
                "max_speed_kmh"
            ),

            # ------------------------------------------------
            # VEHICLE STATUS METRICS
            # ------------------------------------------------

            spark_sum(
                when(
                    col("vehicle_status")
                    == "in_service",
                    1,
                ).otherwise(0)
            ).alias(
                "in_service_observations"
            ),

            spark_sum(
                when(
                    col("vehicle_status")
                    == "delayed",
                    1,
                ).otherwise(0)
            ).alias(
                "delayed_observations"
            ),

            spark_sum(
                when(
                    col("vehicle_status")
                    == "out_of_service",
                    1,
                ).otherwise(0)
            ).alias(
                "out_of_service_observations"
            ),

            # ------------------------------------------------
            # OCCUPANCY METRICS
            # ------------------------------------------------

            spark_sum(
                when(
                    col("occupancy_status")
                    == "low",
                    1,
                ).otherwise(0)
            ).alias(
                "low_occupancy_observations"
            ),

            spark_sum(
                when(
                    col("occupancy_status")
                    == "moderate",
                    1,
                ).otherwise(0)
            ).alias(
                "moderate_occupancy_observations"
            ),

            spark_sum(
                when(
                    col("occupancy_status")
                    == "high",
                    1,
                ).otherwise(0)
            ).alias(
                "high_occupancy_observations"
            ),
        )
    )

    return (
        aggregated
        .select(
            col("window.start").alias(
                "window_start"
            ),

            col("window.end").alias(
                "window_end"
            ),

            "route_id",

            "observation_count",

            "active_vehicle_count",

            spark_round(
                col("avg_speed_kmh"),
                2,
            ).alias(
                "avg_speed_kmh"
            ),

            spark_round(
                col("min_speed_kmh"),
                2,
            ).alias(
                "min_speed_kmh"
            ),

            spark_round(
                col("max_speed_kmh"),
                2,
            ).alias(
                "max_speed_kmh"
            ),

            "in_service_observations",

            "delayed_observations",

            "out_of_service_observations",

            "low_occupancy_observations",

            "moderate_occupancy_observations",

            "high_occupancy_observations",
        )

        # ----------------------------------------------------
        # DERIVED GOLD METRICS
        # ----------------------------------------------------

        .withColumn(
            "delay_rate_pct",
            spark_round(
                (
                    col("delayed_observations")
                    / col("observation_count")
                )
                * 100,
                2,
            ),
        )

        .withColumn(
            "high_occupancy_rate_pct",
            spark_round(
                (
                    col(
                        "high_occupancy_observations"
                    )
                    / col("observation_count")
                )
                * 100,
                2,
            ),
        )

        .withColumn(
            "out_of_service_rate_pct",
            spark_round(
                (
                    col(
                        "out_of_service_observations"
                    )
                    / col("observation_count")
                )
                * 100,
                2,
            ),
        )

        .withColumn(
            "gold_processed_at",
            current_timestamp(),
        )

        .orderBy(
            "window_start",
            "route_id",
        )
    )


# ============================================================
# GOLD DATA QUALITY
# ============================================================

def validate_gold(
    gold_df,
):
    """
    Apply basic analytical invariants before publishing Gold.

    Gold should never contain:
    - missing analytical keys
    - empty aggregation windows
    - negative speed values
    - impossible percentage metrics
    """

    invalid_df = (
        gold_df
        .filter(
            col("window_start").isNull()
            | col("window_end").isNull()
            | col("route_id").isNull()
            | (
                col("observation_count")
                <= 0
            )
            | (
                col("active_vehicle_count")
                <= 0
            )
            | (
                col("avg_speed_kmh")
                < 0
            )
            | (
                col("min_speed_kmh")
                < 0
            )
            | (
                col("max_speed_kmh")
                < 0
            )
            | (
                col("delay_rate_pct")
                < 0
            )
            | (
                col("delay_rate_pct")
                > 100
            )
            | (
                col(
                    "high_occupancy_rate_pct"
                )
                < 0
            )
            | (
                col(
                    "high_occupancy_rate_pct"
                )
                > 100
            )
            | (
                col(
                    "out_of_service_rate_pct"
                )
                < 0
            )
            | (
                col(
                    "out_of_service_rate_pct"
                )
                > 100
            )
        )
    )

    invalid_count = invalid_df.count()

    if invalid_count > 0:

        print()
        print("=" * 72)

        print(
            "GOLD DATA QUALITY FAILURE"
        )

        print("=" * 72)

        print(
            "Invalid route-performance rows: "
            f"{invalid_count}"
        )

        print()

        invalid_df.show(
            20,
            truncate=False,
        )

        raise ValueError(
            "Gold route-performance validation failed."
        )

    return gold_df


# ============================================================
# WRITE GOLD DATA
# ============================================================

def write_gold(
    gold_df,
):
    """
    Publish the current route-performance Gold snapshot.

    Overwrite is intentional for this first deterministic
    batch model: Gold is rebuilt from the authoritative
    validated Silver dataset.
    """

    (
        gold_df
        .write
        .mode("overwrite")
        .format("parquet")
        .save(GOLD_PATH)
    )


# ============================================================
# PIPELINE SUMMARY
# ============================================================

def print_summary(
    silver_df,
    telemetry_df,
    gold_df,
):

    silver_count = (
        silver_df.count()
    )

    telemetry_count = (
        telemetry_df.count()
    )

    gold_count = (
        gold_df.count()
    )

    route_count = (
        gold_df
        .select(
            "route_id"
        )
        .distinct()
        .count()
    )

    print()
    print("=" * 72)

    print(
        "URBANPULSE — GOLD ROUTE PERFORMANCE SUMMARY"
    )

    print("=" * 72)

    print(
        f"Silver telemetry records: "
        f"{silver_count}"
    )

    print(
        f"Eligible Gold input records: "
        f"{telemetry_count}"
    )

    print(
        f"Gold route-window rows: "
        f"{gold_count}"
    )

    print(
        f"Distinct routes: "
        f"{route_count}"
    )

    print(
        "Aggregation window: "
        "15 minutes"
    )

    print(
        f"Gold destination: "
        f"{GOLD_PATH}"
    )

    print()

    print(
        "Sample Gold records:"
    )

    gold_df.show(
        20,
        truncate=False,
    )


# ============================================================
# APPLICATION ENTRY POINT
# ============================================================

def main():

    spark = (
        create_spark_session()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print()
    print("=" * 72)

    print(
        "URBANPULSE — ROUTE PERFORMANCE GOLD PIPELINE"
    )

    print("=" * 72)

    print(
        f"Reading trusted Silver telemetry from:"
    )

    print(
        SILVER_PATH
    )

    print()

    print(
        "Gold grain: one route per 15-minute window"
    )

    print()

    try:

        silver_df = (
            read_silver_telemetry(
                spark
            )
        )

        telemetry_df = (
            prepare_telemetry(
                silver_df
            )
        )

        gold_df = (
            build_route_performance(
                telemetry_df
            )
        )

        validate_gold(
            gold_df
        )

        print_summary(
            silver_df,
            telemetry_df,
            gold_df,
        )

        write_gold(
            gold_df
        )

        print()
        print("=" * 72)

        print(
            "GOLD ROUTE PERFORMANCE BUILD COMPLETE"
        )

        print("=" * 72)

        print(
            f"Published to: {GOLD_PATH}"
        )

    finally:

        spark.stop()

        print()

        print(
            "Spark session stopped successfully."
        )


if __name__ == "__main__":
    main()