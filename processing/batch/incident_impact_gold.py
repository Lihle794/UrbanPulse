import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType


# ============================================================
# UrbanPulse - Gold Incident Impact Model
# ============================================================
#
# Purpose:
#   Correlate validated traffic observations with validated
#   incident events at a common road / 15-minute time grain.
#
# Data classification:
#   SYNTHETIC / SIMULATED
#
# Inputs:
#   Silver traffic observations
#   Silver incidents
#
# Output:
#   Gold incident impact analytical dataset
#
# ============================================================


TRAFFIC_SILVER_PATH = (
    "s3a://urbanpulse/silver/traffic_observations/"
)

INCIDENT_SILVER_PATH = (
    "s3a://urbanpulse/silver/incidents/"
)

GOLD_OUTPUT_PATH = (
    "s3a://urbanpulse/gold/incident_impact/"
)

AGGREGATION_WINDOW = "15 minutes"

GOLD_MODEL_NAME = "incident_impact"

DATA_CLASSIFICATION = "SYNTHETIC"


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
# Spark session
# ============================================================


def create_spark_session():
    return (
        SparkSession.builder
        .appName("UrbanPulseIncidentImpactGold")
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
        .config(
            "spark.default.parallelism",
            "2",
        )
        .getOrCreate()
    )


# ============================================================
# Utility helpers
# ============================================================


def print_section(title):
    print()
    print("=" * 90)
    print(title)
    print("=" * 90)


def require_columns(
    dataframe,
    required_columns,
    dataset_name,
):
    missing = sorted(
        set(required_columns)
        - set(dataframe.columns)
    )

    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{', '.join(missing)}"
        )


def ensure_numeric_column(
    dataframe,
    column_name,
    dataset_name,
):
    field = next(
        (
            field
            for field in dataframe.schema.fields
            if field.name == column_name
        ),
        None,
    )

    if field is None:
        raise ValueError(
            f"{dataset_name} does not contain "
            f"{column_name}."
        )

    if not isinstance(
        field.dataType,
        NumericType,
    ):
        raise TypeError(
            f"{dataset_name}.{column_name} must "
            "be numeric."
        )


# ============================================================
# Read Silver datasets
# ============================================================


def read_silver_datasets(spark):
    print_section(
        "READING URBANPULSE SILVER DATASETS"
    )

    traffic = (
        spark.read
        .format("parquet")
        .load(TRAFFIC_SILVER_PATH)
    )

    incidents = (
        spark.read
        .format("parquet")
        .load(INCIDENT_SILVER_PATH)
    )

    print(
        "Traffic Silver source: "
        f"{TRAFFIC_SILVER_PATH}"
    )

    print(
        "Incident Silver source: "
        f"{INCIDENT_SILVER_PATH}"
    )

    return traffic, incidents


# ============================================================
# Validate source schemas
# ============================================================


def validate_source_schemas(
    traffic,
    incidents,
):
    traffic_required = [
        "event_id",
        "event_timestamp",
        "segment_id",
        "road_name",
        "average_speed_kmh",
        "vehicle_count",
        "congestion_level",
        "travel_time_seconds",
        "quality_status",
    ]

    incident_required = [
        "event_id",
        "event_timestamp",
        "incident_id",
        "incident_type",
        "severity",
        "status",
        "segment_id",
        "road_name",
        "estimated_clearance_minutes",
        "quality_status",
    ]

    require_columns(
        traffic,
        traffic_required,
        "Traffic Silver",
    )

    require_columns(
        incidents,
        incident_required,
        "Incident Silver",
    )

    ensure_numeric_column(
        traffic,
        "average_speed_kmh",
        "Traffic Silver",
    )

    ensure_numeric_column(
        traffic,
        "vehicle_count",
        "Traffic Silver",
    )

    ensure_numeric_column(
        traffic,
        "travel_time_seconds",
        "Traffic Silver",
    )

    ensure_numeric_column(
        incidents,
        "estimated_clearance_minutes",
        "Incident Silver",
    )


# ============================================================
# Prepare valid traffic records
# ============================================================


def prepare_traffic(traffic):
    return (
        traffic
        .filter(
            F.col("quality_status") == "valid"
        )
        .filter(
            F.col("event_timestamp").isNotNull()
        )
        .filter(
            F.col("road_name").isNotNull()
        )
        .filter(
            F.trim(
                F.col("road_name")
            ) != ""
        )
        .withColumn(
            "normalized_road_name",
            F.lower(
                F.trim(
                    F.col("road_name")
                )
            ),
        )
        .withColumn(
            "traffic_window",
            F.window(
                F.col("event_timestamp"),
                AGGREGATION_WINDOW,
            ),
        )
    )


# ============================================================
# Prepare valid incident records
# ============================================================


def prepare_incidents(incidents):
    return (
        incidents
        .filter(
            F.col("quality_status") == "valid"
        )
        .filter(
            F.col("event_timestamp").isNotNull()
        )
        .filter(
            F.col("road_name").isNotNull()
        )
        .filter(
            F.trim(
                F.col("road_name")
            ) != ""
        )
        .withColumn(
            "normalized_road_name",
            F.lower(
                F.trim(
                    F.col("road_name")
                )
            ),
        )
        .withColumn(
            "incident_window",
            F.window(
                F.col("event_timestamp"),
                AGGREGATION_WINDOW,
            ),
        )
    )


# ============================================================
# Aggregate traffic metrics
# ============================================================


def aggregate_traffic(traffic):
    return (
        traffic
        .groupBy(
            "normalized_road_name",
            "traffic_window",
        )
        .agg(
            F.first(
                "road_name",
                ignorenulls=True,
            ).alias("road_name"),

            F.first(
                "segment_id",
                ignorenulls=True,
            ).alias("traffic_segment_id"),

            F.count(
                "*"
            ).alias(
                "traffic_observation_count"
            ),

            F.avg(
                "average_speed_kmh"
            ).alias(
                "avg_speed_kmh"
            ),

            F.min(
                "average_speed_kmh"
            ).alias(
                "min_speed_kmh"
            ),

            F.max(
                "average_speed_kmh"
            ).alias(
                "max_speed_kmh"
            ),

            F.avg(
                "vehicle_count"
            ).alias(
                "avg_vehicle_count"
            ),

            F.max(
                "vehicle_count"
            ).alias(
                "max_vehicle_count"
            ),

            F.avg(
                "travel_time_seconds"
            ).alias(
                "avg_travel_time_seconds"
            ),

            F.max(
                "travel_time_seconds"
            ).alias(
                "max_travel_time_seconds"
            ),

            F.sum(
                F.when(
                    F.col(
                        "congestion_level"
                    ) == "heavy",
                    1,
                ).otherwise(0)
            ).alias(
                "heavy_congestion_observations"
            ),

            F.sum(
                F.when(
                    F.col(
                        "congestion_level"
                    ) == "moderate",
                    1,
                ).otherwise(0)
            ).alias(
                "moderate_congestion_observations"
            ),

            F.sum(
                F.when(
                    F.col(
                        "congestion_level"
                    ) == "low",
                    1,
                ).otherwise(0)
            ).alias(
                "low_congestion_observations"
            ),
        )
        .withColumn(
            "window_start",
            F.col(
                "traffic_window.start"
            ),
        )
        .withColumn(
            "window_end",
            F.col(
                "traffic_window.end"
            ),
        )
        .drop(
            "traffic_window"
        )
    )


# ============================================================
# Aggregate incident metrics
# ============================================================


def aggregate_incidents(incidents):
    return (
        incidents
        .groupBy(
            "normalized_road_name",
            "incident_window",
        )
        .agg(
            F.first(
                "road_name",
                ignorenulls=True,
            ).alias(
                "incident_road_name"
            ),

            F.first(
                "segment_id",
                ignorenulls=True,
            ).alias(
                "incident_segment_id"
            ),

            F.countDistinct(
                "incident_id"
            ).alias(
                "incident_count"
            ),

            F.sum(
                F.when(
                    F.col("status")
                    == "active",
                    1,
                ).otherwise(0)
            ).alias(
                "active_incident_count"
            ),

            F.sum(
                F.when(
                    F.col("severity")
                    == "high",
                    1,
                ).otherwise(0)
            ).alias(
                "high_severity_incident_count"
            ),

            F.sum(
                F.when(
                    F.col("severity")
                    == "moderate",
                    1,
                ).otherwise(0)
            ).alias(
                "moderate_severity_incident_count"
            ),

            F.sum(
                F.when(
                    F.col("severity")
                    == "low",
                    1,
                ).otherwise(0)
            ).alias(
                "low_severity_incident_count"
            ),

            F.sum(
                F.when(
                    F.col("incident_type")
                    == "road_closure",
                    1,
                ).otherwise(0)
            ).alias(
                "road_closure_count"
            ),

            F.sum(
                F.when(
                    F.col("incident_type")
                    == "vehicle_breakdown",
                    1,
                ).otherwise(0)
            ).alias(
                "vehicle_breakdown_count"
            ),

            F.sum(
                F.when(
                    F.col("incident_type")
                    == "road_hazard",
                    1,
                ).otherwise(0)
            ).alias(
                "road_hazard_count"
            ),

            F.avg(
                "estimated_clearance_minutes"
            ).alias(
                "avg_estimated_clearance_minutes"
            ),

            F.max(
                "estimated_clearance_minutes"
            ).alias(
                "max_estimated_clearance_minutes"
            ),
        )
        .withColumn(
            "window_start",
            F.col(
                "incident_window.start"
            ),
        )
        .withColumn(
            "window_end",
            F.col(
                "incident_window.end"
            ),
        )
        .drop(
            "incident_window"
        )
    )


# ============================================================
# Join traffic and incident aggregates
# ============================================================


def build_incident_impact(
    traffic_metrics,
    incident_metrics,
):
    traffic_alias = traffic_metrics.alias(
        "traffic"
    )

    incident_alias = incident_metrics.alias(
        "incident"
    )

    join_condition = (
        (
            F.col(
                "traffic.normalized_road_name"
            )
            ==
            F.col(
                "incident.normalized_road_name"
            )
        )
        &
        (
            F.col(
                "traffic.window_start"
            )
            ==
            F.col(
                "incident.window_start"
            )
        )
        &
        (
            F.col(
                "traffic.window_end"
            )
            ==
            F.col(
                "incident.window_end"
            )
        )
    )

    joined = (
        traffic_alias
        .join(
            incident_alias,
            join_condition,
            "inner",
        )
    )

    gold = (
        joined
        .select(
            F.col(
                "traffic.window_start"
            ).alias(
                "window_start"
            ),

            F.col(
                "traffic.window_end"
            ).alias(
                "window_end"
            ),

            F.col(
                "traffic.road_name"
            ).alias(
                "road_name"
            ),

            F.col(
                "traffic.normalized_road_name"
            ).alias(
                "normalized_road_name"
            ),

            F.col(
                "traffic.traffic_segment_id"
            ).alias(
                "traffic_segment_id"
            ),

            F.col(
                "incident.incident_segment_id"
            ).alias(
                "incident_segment_id"
            ),

            F.col(
                "traffic.traffic_observation_count"
            ),

            F.col(
                "traffic.avg_speed_kmh"
            ),

            F.col(
                "traffic.min_speed_kmh"
            ),

            F.col(
                "traffic.max_speed_kmh"
            ),

            F.col(
                "traffic.avg_vehicle_count"
            ),

            F.col(
                "traffic.max_vehicle_count"
            ),

            F.col(
                "traffic.avg_travel_time_seconds"
            ),

            F.col(
                "traffic.max_travel_time_seconds"
            ),

            F.col(
                "traffic.heavy_congestion_observations"
            ),

            F.col(
                "traffic.moderate_congestion_observations"
            ),

            F.col(
                "traffic.low_congestion_observations"
            ),

            F.col(
                "incident.incident_count"
            ),

            F.col(
                "incident.active_incident_count"
            ),

            F.col(
                "incident.high_severity_incident_count"
            ),

            F.col(
                "incident.moderate_severity_incident_count"
            ),

            F.col(
                "incident.low_severity_incident_count"
            ),

            F.col(
                "incident.road_closure_count"
            ),

            F.col(
                "incident.vehicle_breakdown_count"
            ),

            F.col(
                "incident.road_hazard_count"
            ),

            F.col(
                "incident.avg_estimated_clearance_minutes"
            ),

            F.col(
                "incident.max_estimated_clearance_minutes"
            ),
        )
    )

    return add_derived_metrics(
        gold
    )


# ============================================================
# Derived analytical metrics
# ============================================================


def add_derived_metrics(gold):
    result = (
        gold
        .withColumn(
            "heavy_congestion_percentage",
            F.round(
                (
                    F.col(
                        "heavy_congestion_observations"
                    )
                    /
                    F.col(
                        "traffic_observation_count"
                    )
                )
                * 100,
                2,
            ),
        )
        .withColumn(
            "moderate_congestion_percentage",
            F.round(
                (
                    F.col(
                        "moderate_congestion_observations"
                    )
                    /
                    F.col(
                        "traffic_observation_count"
                    )
                )
                * 100,
                2,
            ),
        )
        .withColumn(
            "low_congestion_percentage",
            F.round(
                (
                    F.col(
                        "low_congestion_observations"
                    )
                    /
                    F.col(
                        "traffic_observation_count"
                    )
                )
                * 100,
                2,
            ),
        )
        .withColumn(
            "dominant_congestion_level",
            F.when(
                (
                    F.col(
                        "heavy_congestion_observations"
                    )
                    >=
                    F.col(
                        "moderate_congestion_observations"
                    )
                )
                &
                (
                    F.col(
                        "heavy_congestion_observations"
                    )
                    >=
                    F.col(
                        "low_congestion_observations"
                    )
                ),
                F.lit("heavy"),
            )
            .when(
                F.col(
                    "moderate_congestion_observations"
                )
                >=
                F.col(
                    "low_congestion_observations"
                ),
                F.lit("moderate"),
            )
            .otherwise(
                F.lit("low")
            ),
        )
    )

    # --------------------------------------------------------
    # Incident severity score
    #
    # High severity     = 3
    # Moderate severity = 2
    # Low severity      = 1
    #
    # Road closures receive an additional weight because
    # they represent a stronger operational disruption.
    # --------------------------------------------------------

    result = (
        result
        .withColumn(
            "incident_severity_score",
            (
                F.col(
                    "high_severity_incident_count"
                )
                * 3
            )
            +
            (
                F.col(
                    "moderate_severity_incident_count"
                )
                * 2
            )
            +
            F.col(
                "low_severity_incident_count"
            )
            +
            (
                F.col(
                    "road_closure_count"
                )
                * 2
            ),
        )
    )

    # --------------------------------------------------------
    # Traffic disruption score
    #
    # This is an UrbanPulse analytical indicator for the
    # synthetic portfolio dataset. It is NOT an official
    # Johannesburg congestion or transport metric.
    # --------------------------------------------------------

    result = (
        result
        .withColumn(
            "traffic_disruption_score",
            F.round(
                (
                    F.col(
                        "heavy_congestion_percentage"
                    )
                    * 0.05
                )
                +
                (
                    F.col(
                        "moderate_congestion_percentage"
                    )
                    * 0.02
                )
                +
                F.when(
                    F.col(
                        "avg_speed_kmh"
                    ) < 20,
                    3.0,
                )
                .when(
                    F.col(
                        "avg_speed_kmh"
                    ) < 35,
                    2.0,
                )
                .otherwise(
                    1.0
                ),
                2,
            ),
        )
    )

    result = (
        result
        .withColumn(
            "incident_impact_score",
            F.round(
                F.col(
                    "incident_severity_score"
                )
                +
                F.col(
                    "traffic_disruption_score"
                ),
                2,
            ),
        )
        .withColumn(
            "incident_impact_level",
            F.when(
                F.col(
                    "incident_impact_score"
                ) >= 10,
                F.lit("high"),
            )
            .when(
                F.col(
                    "incident_impact_score"
                ) >= 6,
                F.lit("moderate"),
            )
            .otherwise(
                F.lit("low")
            ),
        )
        .withColumn(
            "gold_model",
            F.lit(
                GOLD_MODEL_NAME
            ),
        )
        .withColumn(
            "aggregation_window",
            F.lit(
                AGGREGATION_WINDOW
            ),
        )
        .withColumn(
            "data_classification",
            F.lit(
                DATA_CLASSIFICATION
            ),
        )
        .withColumn(
            "generated_at",
            F.current_timestamp(),
        )
    )

    return result


# ============================================================
# Validate Gold dataset
# ============================================================


def validate_gold(gold):
    print_section(
        "URBANPULSE - GOLD INCIDENT IMPACT VALIDATION"
    )

    total_rows = gold.count()

    null_roads = (
        gold
        .filter(
            F.col("road_name").isNull()
        )
        .count()
    )

    null_window_starts = (
        gold
        .filter(
            F.col("window_start").isNull()
        )
        .count()
    )

    null_window_ends = (
        gold
        .filter(
            F.col("window_end").isNull()
        )
        .count()
    )

    invalid_incident_counts = (
        gold
        .filter(
            F.col("incident_count") <= 0
        )
        .count()
    )

    invalid_traffic_counts = (
        gold
        .filter(
            F.col(
                "traffic_observation_count"
            ) <= 0
        )
        .count()
    )

    duplicate_rows = (
        gold
        .groupBy(
            "normalized_road_name",
            "window_start",
            "window_end",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    invalid_impact_scores = (
        gold
        .filter(
            F.col(
                "incident_impact_score"
            ).isNull()
            |
            (
                F.col(
                    "incident_impact_score"
                ) < 0
            )
        )
        .count()
    )

    invalid_impact_levels = (
        gold
        .filter(
            ~F.col(
                "incident_impact_level"
            ).isin(
                "low",
                "moderate",
                "high",
            )
        )
        .count()
    )

    print(
        f"Gold incident-impact rows: {total_rows}"
    )

    print(
        f"Null road names: {null_roads}"
    )

    print(
        "Null window starts: "
        f"{null_window_starts}"
    )

    print(
        "Null window ends: "
        f"{null_window_ends}"
    )

    print(
        "Invalid incident counts: "
        f"{invalid_incident_counts}"
    )

    print(
        "Invalid traffic observation counts: "
        f"{invalid_traffic_counts}"
    )

    print(
        "Duplicate road-window combinations: "
        f"{duplicate_rows}"
    )

    print(
        "Invalid impact scores: "
        f"{invalid_impact_scores}"
    )

    print(
        "Invalid impact levels: "
        f"{invalid_impact_levels}"
    )

    validation_failures = (
        null_roads
        + null_window_starts
        + null_window_ends
        + invalid_incident_counts
        + invalid_traffic_counts
        + duplicate_rows
        + invalid_impact_scores
        + invalid_impact_levels
    )

    if validation_failures > 0:
        raise ValueError(
            "Gold incident impact dataset "
            "failed structural validation."
        )

    print()
    print(
        "PASS - Gold incident impact dataset "
        "passed structural validation."
    )

    return total_rows


# ============================================================
# Write Gold dataset
# ============================================================


def write_gold(gold):
    (
        gold
        .coalesce(1)
        .write
        .mode("overwrite")
        .format("parquet")
        .save(GOLD_OUTPUT_PATH)
    )


# ============================================================
# Main
# ============================================================


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:
        print_section(
            "URBANPULSE - GOLD INCIDENT IMPACT MODEL"
        )

        print(
            "Aggregation grain: "
            "road + 15-minute window"
        )

        print(
            "Data classification: "
            "SYNTHETIC / SIMULATED"
        )

        traffic, incidents = (
            read_silver_datasets(
                spark
            )
        )

        validate_source_schemas(
            traffic,
            incidents,
        )

        traffic_total = traffic.count()
        incident_total = incidents.count()

        valid_traffic = prepare_traffic(
            traffic
        )

        valid_incidents = prepare_incidents(
            incidents
        )

        valid_traffic_count = (
            valid_traffic.count()
        )

        valid_incident_count = (
            valid_incidents.count()
        )

        print_section(
            "SOURCE DATA SUMMARY"
        )

        print(
            "Traffic Silver records: "
            f"{traffic_total}"
        )

        print(
            "Eligible traffic records: "
            f"{valid_traffic_count}"
        )

        print(
            "Incident Silver records: "
            f"{incident_total}"
        )

        print(
            "Eligible incident records: "
            f"{valid_incident_count}"
        )

        traffic_metrics = (
            aggregate_traffic(
                valid_traffic
            )
        )

        incident_metrics = (
            aggregate_incidents(
                valid_incidents
            )
        )

        traffic_window_count = (
            traffic_metrics.count()
        )

        incident_window_count = (
            incident_metrics.count()
        )

        print_section(
            "AGGREGATION SUMMARY"
        )

        print(
            "Traffic road-window rows: "
            f"{traffic_window_count}"
        )

        print(
            "Incident road-window rows: "
            f"{incident_window_count}"
        )

        gold = build_incident_impact(
            traffic_metrics,
            incident_metrics,
        )

        gold = gold.cache()

        gold_count = validate_gold(
            gold
        )

        print_section(
            "SAMPLE GOLD INCIDENT IMPACT RECORDS"
        )

        if gold_count > 0:
            (
                gold
                .orderBy(
                    F.col(
                        "incident_impact_score"
                    ).desc(),
                    F.col(
                        "window_start"
                    ).asc(),
                )
                .select(
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
                )
                .show(
                    20,
                    truncate=False,
                )
            )

        else:
            print(
                "No matching road/time windows "
                "currently exist between Traffic "
                "Silver and Incident Silver."
            )

            print(
                "This is a valid analytical result. "
                "No synthetic relationship will be "
                "fabricated."
            )

        write_gold(
            gold
        )

        print_section(
            "GOLD INCIDENT IMPACT BUILD COMPLETE"
        )

        print(
            f"Published to: {GOLD_OUTPUT_PATH}"
        )

        print(
            f"Gold rows published: {gold_count}"
        )

        if gold_count == 0:
            print()
            print(
                "NOTE: The Gold dataset is currently "
                "empty because no road + 15-minute "
                "window exists in both source domains."
            )

        gold.unpersist()

    except Exception as exc:
        print_section(
            "GOLD INCIDENT IMPACT BUILD FAILED"
        )

        print(
            f"{type(exc).__name__}: {exc}"
        )

        raise

    finally:
        spark.stop()

        print()
        print(
            "Spark session stopped successfully."
        )


if __name__ == "__main__":
    main()