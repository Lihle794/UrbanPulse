"""
UrbanPulse - Gold to PostgreSQL Serving Loader

Reads persisted Gold Parquet datasets from MinIO and publishes them
to the PostgreSQL analytics serving layer.

Serving strategy
----------------
The PostgreSQL analytics tables are treated as serving snapshots of
the authoritative Gold datasets stored in MinIO.

Each table is refreshed using JDBC overwrite with truncate=true.
This makes repeated executions idempotent for the current Gold state
and preserves the PostgreSQL table definitions, indexes and constraints.

IMPORTANT
---------
UrbanPulse uses SYNTHETIC / SIMULATED transportation data.
It must not be represented as official Johannesburg transport data.
"""

import os
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------
# MinIO configuration
# ---------------------------------------------------------------------

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


# ---------------------------------------------------------------------
# PostgreSQL configuration
# ---------------------------------------------------------------------

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost",
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5434",
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "urbanpulse",
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "urbanpulse",
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "urbanpulse123",
)

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/{POSTGRES_DB}"
)

JDBC_DRIVER = "org.postgresql.Driver"


# ---------------------------------------------------------------------
# Gold datasets
# ---------------------------------------------------------------------

GOLD_DATASETS: Dict[str, Dict[str, str]] = {
    "route_performance": {
        "path": "s3a://urbanpulse/gold/route_performance/",
        "table": "analytics.route_performance",
    },
    "congestion_summary": {
        "path": "s3a://urbanpulse/gold/congestion_summary/",
        "table": "analytics.congestion_summary",
    },
    "incident_impact": {
        "path": "s3a://urbanpulse/gold/incident_impact/",
        "table": "analytics.incident_impact",
    },
}


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
        .appName("UrbanPulseGoldToPostgresServing")
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

    spark.sparkContext.setLogLevel("WARN")

    return spark


def read_gold_dataset(
    spark: SparkSession,
    dataset_name: str,
    path: str,
) -> DataFrame:
    print(f"\nReading {dataset_name}")
    print(f"Source: {path}")

    df = spark.read.parquet(path)

    row_count = df.count()

    print(f"Gold rows found: {row_count}")

    if row_count == 0:
        raise ValueError(
            f"{dataset_name} contains zero Gold rows. "
            "Serving refresh aborted to prevent replacing an "
            "existing PostgreSQL table with an empty dataset."
        )

    return df


def validate_required_columns(
    df: DataFrame,
    dataset_name: str,
    required_columns: list[str],
) -> None:
    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            + ", ".join(missing_columns)
        )


def validate_route_performance(df: DataFrame) -> None:
    validate_required_columns(
        df,
        "route_performance",
        [
            "window_start",
            "window_end",
            "route_id",
            "observation_count",
            "active_vehicle_count",
        ],
    )

    invalid_rows = df.filter(
        F.col("route_id").isNull()
        | F.col("window_start").isNull()
        | F.col("window_end").isNull()
        | (F.col("window_end") <= F.col("window_start"))
        | (F.col("observation_count") < 0)
        | (F.col("active_vehicle_count") < 0)
    ).count()

    duplicate_rows = (
        df.groupBy(
            "route_id",
            "window_start",
            "window_end",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if invalid_rows > 0 or duplicate_rows > 0:
        raise ValueError(
            "route_performance failed serving validation. "
            f"Invalid rows: {invalid_rows}; "
            f"duplicate analytical keys: {duplicate_rows}"
        )

    print("Validation: PASS")


def validate_congestion_summary(df: DataFrame) -> None:
    validate_required_columns(
        df,
        "congestion_summary",
        [
            "road_segment",
            "window_start",
            "window_end",
            "observation_count",
        ],
    )

    invalid_rows = df.filter(
        F.col("road_segment").isNull()
        | F.col("window_start").isNull()
        | F.col("window_end").isNull()
        | (F.col("window_end") <= F.col("window_start"))
        | (F.col("observation_count") < 0)
    ).count()

    duplicate_rows = (
        df.groupBy(
            "road_segment",
            "window_start",
            "window_end",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if invalid_rows > 0 or duplicate_rows > 0:
        raise ValueError(
            "congestion_summary failed serving validation. "
            f"Invalid rows: {invalid_rows}; "
            f"duplicate analytical keys: {duplicate_rows}"
        )

    print("Validation: PASS")


def validate_incident_impact(df: DataFrame) -> None:
    validate_required_columns(
        df,
        "incident_impact",
        [
            "road_name",
            "window_start",
            "window_end",
            "incident_count",
            "traffic_observation_count",
            "incident_impact_score",
        ],
    )

    invalid_rows = df.filter(
        F.col("road_name").isNull()
        | F.col("window_start").isNull()
        | F.col("window_end").isNull()
        | (F.col("window_end") <= F.col("window_start"))
        | (F.col("incident_count") < 1)
        | (F.col("traffic_observation_count") < 1)
        | F.col("incident_impact_score").isNull()
        | (F.col("incident_impact_score") < 0)
    ).count()

    duplicate_rows = (
        df.groupBy(
            "road_name",
            "window_start",
            "window_end",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if invalid_rows > 0 or duplicate_rows > 0:
        raise ValueError(
            "incident_impact failed serving validation. "
            f"Invalid rows: {invalid_rows}; "
            f"duplicate analytical keys: {duplicate_rows}"
        )

    print("Validation: PASS")


def write_to_postgres(
    df: DataFrame,
    dataset_name: str,
    table_name: str,
) -> None:
    print(f"\nPublishing {dataset_name}")
    print(f"Target: {table_name}")

    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    print("PostgreSQL refresh: COMPLETE")


def read_postgres_count(
    spark: SparkSession,
    table_name: str,
) -> int:
    postgres_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option(
            "query",
            f"SELECT COUNT(*) AS row_count FROM {table_name}",
        )
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .load()
    )

    return int(postgres_df.first()["row_count"])


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    spark = build_spark_session()

    try:
        print_section(
            "URBANPULSE - GOLD TO POSTGRESQL SERVING REFRESH"
        )

        print("Source system: MinIO Gold Parquet")
        print(
            f"Target system: PostgreSQL "
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        print("Target schema: analytics")
        print("Serving mode: idempotent snapshot refresh")
        print("Data classification: SYNTHETIC / SIMULATED")

        # -------------------------------------------------------------
        # Read all Gold datasets BEFORE modifying PostgreSQL.
        #
        # This ensures a missing/empty Gold dataset causes the run to
        # fail before any serving table is refreshed.
        # -------------------------------------------------------------

        print_section("READING AUTHORITATIVE GOLD DATASETS")

        route_df = read_gold_dataset(
            spark,
            "route_performance",
            GOLD_DATASETS["route_performance"]["path"],
        )

        congestion_df = read_gold_dataset(
            spark,
            "congestion_summary",
            GOLD_DATASETS["congestion_summary"]["path"],
        )

        incident_df = read_gold_dataset(
            spark,
            "incident_impact",
            GOLD_DATASETS["incident_impact"]["path"],
        )

        # -------------------------------------------------------------
        # Validate all datasets BEFORE modifying PostgreSQL.
        # -------------------------------------------------------------

        print_section("PRE-SERVING VALIDATION")

        print("\nroute_performance")
        validate_route_performance(route_df)

        print("\ncongestion_summary")
        validate_congestion_summary(congestion_df)

        print("\nincident_impact")
        validate_incident_impact(incident_df)

        expected_counts = {
            "route_performance": route_df.count(),
            "congestion_summary": congestion_df.count(),
            "incident_impact": incident_df.count(),
        }

        print("\nAll Gold datasets passed pre-serving validation.")

        # -------------------------------------------------------------
        # Publish snapshots
        # -------------------------------------------------------------

        print_section("POSTGRESQL SERVING REFRESH")

        write_to_postgres(
            route_df,
            "route_performance",
            GOLD_DATASETS["route_performance"]["table"],
        )

        write_to_postgres(
            congestion_df,
            "congestion_summary",
            GOLD_DATASETS["congestion_summary"]["table"],
        )

        write_to_postgres(
            incident_df,
            "incident_impact",
            GOLD_DATASETS["incident_impact"]["table"],
        )

        # -------------------------------------------------------------
        # Verify persisted PostgreSQL row counts
        # -------------------------------------------------------------

        print_section("POSTGRESQL PERSISTENCE VERIFICATION")

        verification_passed = True

        for dataset_name, config in GOLD_DATASETS.items():
            expected = expected_counts[dataset_name]

            actual = read_postgres_count(
                spark,
                config["table"],
            )

            status = (
                "PASS"
                if expected == actual
                else "FAIL"
            )

            print(
                f"{dataset_name}: "
                f"Gold={expected}, "
                f"PostgreSQL={actual}, "
                f"{status}"
            )

            if expected != actual:
                verification_passed = False

        if not verification_passed:
            raise ValueError(
                "PostgreSQL persistence verification failed."
            )

        print_section("SERVING REFRESH COMPLETE")

        print("All three Gold models were successfully served.")
        print("PostgreSQL persistence verification: PASS")
        print("Data classification: SYNTHETIC / SIMULATED")

    finally:
        spark.stop()
        print("\nSpark session stopped successfully.")


if __name__ == "__main__":
    main()