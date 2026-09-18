from datetime import datetime, timedelta
import os
import socket

from airflow.sdk import DAG, task
from airflow.providers.docker.operators.docker import DockerOperator


# ---------------------------------------------------------------------------
# UrbanPulse configuration
# ---------------------------------------------------------------------------

SPARK_IMAGE = "urbanpulse-spark:4.0.4"
DOCKER_URL = "unix:///var/run/docker.sock"
DOCKER_NETWORK = "urbanpulse_default"

MINIO_ENV = {
    "HOME": "/tmp",
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ROOT_USER": "urbanpulse",
    "MINIO_ROOT_PASSWORD": "urbanpulse123",
}

POSTGRES_ENV = {
    **MINIO_ENV,
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "urbanpulse",
    "POSTGRES_USER": "urbanpulse",
    "POSTGRES_PASSWORD": "urbanpulse123",
}

COMMON_SPARK_ARGS = [
    "--master",
    "local[2]",
    "--driver-memory",
    "768m",
    "--conf",
    "spark.sql.shuffle.partitions=2",
    "--conf",
    "spark.default.parallelism=2",
    "--conf",
    "spark.jars.ivy=/tmp/.ivy2",
    "--repositories",
    "https://repo1.maven.org/maven2",
]

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"
POSTGRES_PACKAGE = "org.postgresql:postgresql:42.7.7"


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="urbanpulse_gold_pipeline",
    description=(
        "Build UrbanPulse Gold analytical models and publish them "
        "to PostgreSQL."
    ),
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=[
        "urbanpulse",
        "data-engineering",
        "spark",
        "gold",
        "postgresql",
        "synthetic-data",
    ],
    default_args={
        "owner": "urbanpulse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    # -----------------------------------------------------------------------
    # 1. Infrastructure health check
    # -----------------------------------------------------------------------

    @task(task_id="check_infrastructure")
    def check_infrastructure():
        services = {
            "MinIO": ("minio", 9000),
            "PostgreSQL": ("postgres", 5432),
        }

        for service_name, (host, port) in services.items():
            try:
                with socket.create_connection((host, port), timeout=5):
                    print(
                        f"{service_name} connectivity check passed "
                        f"({host}:{port})"
                    )
            except OSError as exc:
                raise RuntimeError(
                    f"{service_name} is unavailable at "
                    f"{host}:{port}"
                ) from exc

        print("UrbanPulse infrastructure checks passed.")

    infrastructure_check = check_infrastructure()

    # -----------------------------------------------------------------------
    # 2. Gold: Route Performance
    # -----------------------------------------------------------------------

    build_route_performance = DockerOperator(
        task_id="build_route_performance",
        image=SPARK_IMAGE,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        entrypoint="/opt/spark/bin/spark-submit",
        command=[
            *COMMON_SPARK_ARGS,
            "--packages",
            HADOOP_AWS_PACKAGE,
            "/opt/urbanpulse/processing/batch/route_performance_gold.py",
        ],
        environment=MINIO_ENV,
        auto_remove="success",
        mount_tmp_dir=False,
        force_pull=False,
    )

    # -----------------------------------------------------------------------
    # 3. Gold: Congestion Summary
    # -----------------------------------------------------------------------

    build_congestion_summary = DockerOperator(
        task_id="build_congestion_summary",
        image=SPARK_IMAGE,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        entrypoint="/opt/spark/bin/spark-submit",
        command=[
            *COMMON_SPARK_ARGS,
            "--packages",
            HADOOP_AWS_PACKAGE,
            "/opt/urbanpulse/processing/batch/congestion_summary_gold.py",
        ],
        environment=MINIO_ENV,
        auto_remove="success",
        mount_tmp_dir=False,
        force_pull=False,
    )

    # -----------------------------------------------------------------------
    # 4. Gold: Incident Impact
    # -----------------------------------------------------------------------

    build_incident_impact = DockerOperator(
        task_id="build_incident_impact",
        image=SPARK_IMAGE,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        entrypoint="/opt/spark/bin/spark-submit",
        command=[
            *COMMON_SPARK_ARGS,
            "--packages",
            HADOOP_AWS_PACKAGE,
            "/opt/urbanpulse/processing/batch/incident_impact_gold.py",
        ],
        environment=MINIO_ENV,
        auto_remove="success",
        mount_tmp_dir=False,
        force_pull=False,
    )

    # -----------------------------------------------------------------------
    # 5. Publish Gold datasets to PostgreSQL
    # -----------------------------------------------------------------------

    load_postgres = DockerOperator(
        task_id="load_gold_to_postgres",
        image=SPARK_IMAGE,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        entrypoint="/opt/spark/bin/spark-submit",
        command=[
            *COMMON_SPARK_ARGS,
            "--packages",
            (
                f"{HADOOP_AWS_PACKAGE},"
                f"{POSTGRES_PACKAGE}"
            ),
            "/opt/urbanpulse/processing/serving/load_gold_to_postgres.py",
        ],
        environment=POSTGRES_ENV,
        auto_remove="success",
        mount_tmp_dir=False,
        force_pull=False,
    )

    # -----------------------------------------------------------------------
    # 6. Verify PostgreSQL serving layer
    # -----------------------------------------------------------------------

    @task(task_id="verify_serving")
    def verify_serving():
        import psycopg2

        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "urbanpulse"),
            user=os.getenv("POSTGRES_USER", "urbanpulse"),
            password=os.getenv("POSTGRES_PASSWORD", "urbanpulse123"),
        )

        tables = [
            "route_performance",
            "congestion_summary",
            "incident_impact",
        ]

        try:
            with connection.cursor() as cursor:
                for table in tables:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM analytics.{table}"
                    )

                    row_count = cursor.fetchone()[0]

                    if row_count <= 0:
                        raise RuntimeError(
                            f"analytics.{table} contains no rows."
                        )

                    print(
                        f"analytics.{table}: "
                        f"{row_count} rows available"
                    )

        finally:
            connection.close()

        print(
            "UrbanPulse PostgreSQL serving verification passed."
        )

    serving_check = verify_serving()

    # -----------------------------------------------------------------------
    # Pipeline dependency chain
    #
    # Sequential execution is intentional for the local environment to
    # prevent multiple Spark JVMs from exhausting WSL resources.
    # -----------------------------------------------------------------------

    (
        infrastructure_check
        >> build_route_performance
        >> build_congestion_summary
        >> build_incident_impact
        >> load_postgres
        >> serving_check
    )