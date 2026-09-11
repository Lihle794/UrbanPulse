from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "vehicle.telemetry"


vehicle_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_version", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("source", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("route_id", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("speed_kmh", DoubleType(), False),
        StructField("heading", IntegerType(), False),
        StructField("occupancy_status", StringType(), False),
        StructField("vehicle_status", StringType(), False),
    ]
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("UrbanPulseVehicleTelemetryStream")
        .master("local[*]")
        .getOrCreate()
    )


def read_vehicle_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )


def parse_vehicle_events(raw_stream):
    return (
        raw_stream
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_payload"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "event",
            from_json(col("raw_payload"), vehicle_schema),
        )
        .select(
            "kafka_key",
            "raw_payload",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "event.*",
        )
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = read_vehicle_stream(spark)

    parsed_stream = parse_vehicle_events(raw_stream)

    query = (
        parsed_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()