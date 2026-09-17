"""
UrbanPulse - Incident Impact Gold Inspection

Purpose
-------
Independently inspect and validate the persisted Incident Impact Gold
dataset stored in the UrbanPulse MinIO data lake.

The Incident Impact Gold model combines valid traffic observations and
incident events at a road + 15-minute window grain.

IMPORTANT:
All UrbanPulse data used by this project is SYNTHETIC / SIMULATED.
It must not be represented as official Johannesburg transport data.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

GOLD_PATH = "s3a://urbanpulse/gold/incident_impact/"

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ROOT_USER",
    "urbanpulse",
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_ROOT_PASSWORD",
    "urbanpulse123",
)

SCENARIO_ROAD = "Jan Smuts Avenue"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def print_section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("UrbanPulseIncidentImpactGoldInspection")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ---------------------------------------------------------------------
# Main inspection
# ---------------------------------------------------------------------

def main() -> None:
    spark = build_spark_session()

    try:
        print_section(
            "URBANPULSE - INCIDENT IMPACT GOLD PERSISTED DATA INSPECTION"
        )

        print(f"Gold source: {GOLD_PATH}")
        print("Expected grain: road + 15-minute window")
        print("Data classification: SYNTHETIC / SIMULATED")

        # -------------------------------------------------------------
        # Read persisted Gold data
        # -------------------------------------------------------------

        gold_df = spark.read.parquet(GOLD_PATH)

        total_rows = gold_df.count()

        print_section("PERSISTED GOLD DATASET SUMMARY")

        print(f"Persisted Gold rows: {total_rows}")

        print("\nGold schema:")
        gold_df.printSchema()

        if total_rows == 0:
            raise ValueError(
                "Incident Impact Gold exists but contains zero rows. "
                "Expected at least one correlated synthetic scenario."
            )

        # -------------------------------------------------------------
        # Structural validation
        # -------------------------------------------------------------

        required_columns = [
            "window_start",
            "window_end",
            "road_name",
            "incident_count",
            "high_severity_incident_count",
            "traffic_observation_count",
            "avg_speed_kmh",
            "avg_travel_time_seconds",
            "dominant_congestion_level",
            "incident_impact_score",
            "incident_impact_level",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in gold_df.columns
        ]

        if missing_columns:
            raise ValueError(
                "Missing required Gold columns: "
                + ", ".join(missing_columns)
            )

        null_road_names = gold_df.filter(
            F.col("road_name").isNull()
        ).count()

        null_window_starts = gold_df.filter(
            F.col("window_start").isNull()
        ).count()

        null_window_ends = gold_df.filter(
            F.col("window_end").isNull()
        ).count()

        invalid_window_order = gold_df.filter(
            F.col("window_end") <= F.col("window_start")
        ).count()

        invalid_incident_counts = gold_df.filter(
            F.col("incident_count").isNull()
            | (F.col("incident_count") <= 0)
        ).count()

        invalid_traffic_counts = gold_df.filter(
            F.col("traffic_observation_count").isNull()
            | (F.col("traffic_observation_count") <= 0)
        ).count()

        invalid_speed_values = gold_df.filter(
            F.col("avg_speed_kmh").isNull()
            | (F.col("avg_speed_kmh") < 0)
        ).count()

        invalid_travel_times = gold_df.filter(
            F.col("avg_travel_time_seconds").isNull()
            | (F.col("avg_travel_time_seconds") < 0)
        ).count()

        invalid_impact_scores = gold_df.filter(
            F.col("incident_impact_score").isNull()
            | (F.col("incident_impact_score") < 0)
        ).count()

        invalid_impact_levels = gold_df.filter(
            F.col("incident_impact_level").isNull()
            | ~F.col("incident_impact_level").isin(
                "low",
                "moderate",
                "high",
                "critical",
            )
        ).count()

        duplicate_road_windows = (
            gold_df
            .groupBy(
                "road_name",
                "window_start",
                "window_end",
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print_section("STRUCTURAL VALIDATION")

        print(f"Null road names: {null_road_names}")
        print(f"Null window starts: {null_window_starts}")
        print(f"Null window ends: {null_window_ends}")
        print(f"Invalid window ordering: {invalid_window_order}")
        print(f"Invalid incident counts: {invalid_incident_counts}")
        print(
            "Invalid traffic observation counts: "
            f"{invalid_traffic_counts}"
        )
        print(f"Invalid average speeds: {invalid_speed_values}")
        print(f"Invalid average travel times: {invalid_travel_times}")
        print(f"Invalid impact scores: {invalid_impact_scores}")
        print(f"Invalid impact levels: {invalid_impact_levels}")
        print(
            "Duplicate road-window combinations: "
            f"{duplicate_road_windows}"
        )

        validation_results = [
            null_road_names,
            null_window_starts,
            null_window_ends,
            invalid_window_order,
            invalid_incident_counts,
            invalid_traffic_counts,
            invalid_speed_values,
            invalid_travel_times,
            invalid_impact_scores,
            invalid_impact_levels,
            duplicate_road_windows,
        ]

        if any(value > 0 for value in validation_results):
            raise ValueError(
                "Incident Impact Gold failed persisted-data validation."
            )

        print(
            "\nPASS - Persisted Incident Impact Gold dataset "
            "passed structural validation."
        )

        # -------------------------------------------------------------
        # Display persisted Gold records
        # -------------------------------------------------------------

        print_section("PERSISTED INCIDENT IMPACT GOLD RECORDS")

        display_columns = [
            "window_start",
            "window_end",
            "road_name",
            "incident_count",
            "high_severity_incident_count",
            "road_closure_count",
            "traffic_observation_count",
            "avg_speed_kmh",
            "avg_travel_time_seconds",
            "dominant_congestion_level",
            "incident_impact_score",
            "incident_impact_level",
        ]

        available_display_columns = [
            column
            for column in display_columns
            if column in gold_df.columns
        ]

        (
            gold_df
            .select(*available_display_columns)
            .orderBy("window_start", "road_name")
            .show(50, truncate=False)
        )

        # -------------------------------------------------------------
        # Controlled scenario verification
        # -------------------------------------------------------------

        scenario_df = gold_df.filter(
            F.lower(F.trim(F.col("road_name")))
            == SCENARIO_ROAD.lower()
        )

        scenario_count = scenario_df.count()

        print_section("CONTROLLED CORRELATED SCENARIO VERIFICATION")

        print(f"Scenario road: {SCENARIO_ROAD}")
        print(f"Matching persisted Gold rows: {scenario_count}")

        if scenario_count == 0:
            raise ValueError(
                "No persisted Incident Impact Gold record was found "
                f"for the controlled scenario road: {SCENARIO_ROAD}"
            )

        (
            scenario_df
            .select(*available_display_columns)
            .orderBy(F.col("window_start").desc())
            .show(20, truncate=False)
        )

        # -------------------------------------------------------------
        # Basic analytical summary
        # -------------------------------------------------------------

        print_section("INCIDENT IMPACT ANALYTICAL SUMMARY")

        gold_df.select(
            F.count("*").alias("gold_rows"),
            F.countDistinct("road_name").alias("distinct_roads"),
            F.sum("incident_count").alias("total_incidents"),
            F.sum("traffic_observation_count").alias(
                "total_traffic_observations"
            ),
            F.round(
                F.avg("avg_speed_kmh"),
                2,
            ).alias("mean_window_speed_kmh"),
            F.round(
                F.avg("avg_travel_time_seconds"),
                2,
            ).alias("mean_window_travel_time_seconds"),
            F.round(
                F.avg("incident_impact_score"),
                2,
            ).alias("mean_incident_impact_score"),
            F.max("incident_impact_score").alias(
                "maximum_incident_impact_score"
            ),
        ).show(truncate=False)

        print("\nImpact level distribution:")

        (
            gold_df
            .groupBy("incident_impact_level")
            .count()
            .orderBy(F.desc("count"))
            .show(truncate=False)
        )

        print("\nDominant congestion distribution:")

        (
            gold_df
            .groupBy("dominant_congestion_level")
            .count()
            .orderBy(F.desc("count"))
            .show(truncate=False)
        )

        # -------------------------------------------------------------
        # Final result
        # -------------------------------------------------------------

        print_section(
            "URBANPULSE INCIDENT IMPACT GOLD INSPECTION COMPLETE"
        )

        print(f"Persisted Gold rows verified: {total_rows}")
        print(f"Controlled scenario rows verified: {scenario_count}")
        print("Structural validation: PASS")
        print("Persistence validation: PASS")
        print("Data classification: SYNTHETIC / SIMULATED")

    finally:
        spark.stop()
        print("\nSpark session stopped successfully.")


if __name__ == "__main__":
    main()