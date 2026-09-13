import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = "vehicle.telemetry"

SOURCE = "urbanpulse_vehicle_simulator"


def current_timestamp() -> str:
    """
    Return the current UTC timestamp in ISO-8601 format.
    """
    return datetime.now(
        timezone.utc
    ).isoformat()


def serialize_key(key: str) -> bytes:
    """
    Convert a Kafka message key into UTF-8 bytes.
    """
    return key.encode("utf-8")


def serialize_value(value: dict) -> bytes:
    """
    Convert a Python dictionary into JSON bytes.
    """
    return json.dumps(value).encode("utf-8")


def create_producer() -> KafkaProducer:
    """
    Create a Kafka producer for controlled
    UrbanPulse data-quality testing.

    This producer intentionally bypasses the normal
    JSON Schema validation used by the production
    telemetry producer.

    The purpose is to inject controlled invalid
    synthetic events so that downstream quarantine
    behaviour can be tested.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=serialize_key,
        value_serializer=serialize_value,
        acks="all",
    )


def base_event(
    vehicle_id: str = "JHB-BUS-999",
) -> dict:
    """
    Create an otherwise-valid synthetic vehicle
    telemetry event.

    Individual data-quality test scenarios modify
    one or more fields in this base event.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "vehicle_telemetry",
        "event_version": "1.0",
        "event_timestamp": current_timestamp(),
        "source": SOURCE,
        "vehicle_id": vehicle_id,
        "route_id": "ROUTE-01",
        "latitude": -26.2041,
        "longitude": 28.0473,
        "speed_kmh": 45.0,
        "heading": 180,
        "occupancy_status": "moderate",
        "vehicle_status": "in_service",
    }


def build_invalid_events() -> list[tuple[str, dict]]:
    """
    Build intentionally invalid synthetic records.

    These events are used only to test UrbanPulse
    downstream data-quality and quarantine behaviour.
    """

    events = []

    # -------------------------------------------------
    # 1. Impossible vehicle speed
    # -------------------------------------------------
    invalid_speed = base_event(
        "JHB-BUS-901"
    )

    invalid_speed["speed_kmh"] = 900.0

    events.append(
        (
            "invalid_speed",
            invalid_speed,
        )
    )

    # -------------------------------------------------
    # 2. Impossible Johannesburg latitude
    # -------------------------------------------------
    invalid_latitude = base_event(
        "JHB-BUS-902"
    )

    invalid_latitude["latitude"] = 400.0

    events.append(
        (
            "invalid_latitude",
            invalid_latitude,
        )
    )

    # -------------------------------------------------
    # 3. Heading outside valid 0-359 range
    # -------------------------------------------------
    invalid_heading = base_event(
        "JHB-BUS-903"
    )

    invalid_heading["heading"] = 999

    events.append(
        (
            "invalid_heading",
            invalid_heading,
        )
    )

    # -------------------------------------------------
    # 4. Invalid occupancy classification
    # -------------------------------------------------
    invalid_occupancy = base_event(
        "JHB-BUS-904"
    )

    invalid_occupancy[
        "occupancy_status"
    ] = "absolutely_packed"

    events.append(
        (
            "invalid_occupancy_status",
            invalid_occupancy,
        )
    )

    # -------------------------------------------------
    # 5. Invalid vehicle operating status
    # -------------------------------------------------
    invalid_vehicle_status = base_event(
        "JHB-BUS-905"
    )

    invalid_vehicle_status[
        "vehicle_status"
    ] = "teleporting"

    events.append(
        (
            "invalid_vehicle_status",
            invalid_vehicle_status,
        )
    )

    # -------------------------------------------------
    # 6. Missing required vehicle ID
    # -------------------------------------------------
    missing_vehicle_id = base_event(
        "JHB-BUS-906"
    )

    missing_vehicle_id.pop(
        "vehicle_id"
    )

    events.append(
        (
            "missing_vehicle_id",
            missing_vehicle_id,
        )
    )

    # -------------------------------------------------
    # 7. Multiple simultaneous quality failures
    # -------------------------------------------------
    multiple_failures = base_event(
        "JHB-BUS-907"
    )

    multiple_failures[
        "latitude"
    ] = 500.0

    multiple_failures[
        "longitude"
    ] = 600.0

    multiple_failures[
        "speed_kmh"
    ] = -50.0

    multiple_failures[
        "heading"
    ] = 800

    multiple_failures[
        "occupancy_status"
    ] = "unknown_level"

    multiple_failures[
        "vehicle_status"
    ] = "flying"

    events.append(
        (
            "multiple_quality_failures",
            multiple_failures,
        )
    )

    # -------------------------------------------------
    # 8. Missing fields that specifically test
    #    null-handling in the Silver DQ pipeline
    # -------------------------------------------------
    missing_status_fields = base_event(
        "JHB-BUS-908"
    )

    missing_status_fields.pop(
        "occupancy_status"
    )

    missing_status_fields.pop(
        "vehicle_status"
    )

    events.append(
        (
            "missing_status_fields",
            missing_status_fields,
        )
    )

    return events


def publish_invalid_events() -> None:
    """
    Publish all controlled invalid events to Kafka.
    """

    producer = create_producer()

    invalid_events = build_invalid_events()

    print()
    print(
        "UrbanPulse controlled "
        "invalid-event producer started."
    )

    print(
        f"Kafka broker: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Topic: {TOPIC}"
    )

    print(
        f"Test events to publish: "
        f"{len(invalid_events)}"
    )

    print()
    print(
        "IMPORTANT:"
    )

    print(
        "These are intentionally invalid "
        "synthetic test records."
    )

    print(
        "They exist only to test the "
        "UrbanPulse data-quality pipeline."
    )

    print()

    try:
        for (
            scenario_name,
            event,
        ) in invalid_events:

            vehicle_id = event.get(
                "vehicle_id",
                "MISSING-VEHICLE-ID",
            )

            kafka_key = event.get(
                "vehicle_id",
            )

            if kafka_key is None:
                kafka_key = (
                    f"dq-test-"
                    f"{uuid.uuid4()}"
                )

            future = producer.send(
                TOPIC,
                key=kafka_key,
                value=event,
            )

            metadata = future.get(
                timeout=10
            )

            print(
                f"[SENT] {scenario_name}"
            )

            print(
                f"       vehicle_id: "
                f"{vehicle_id}"
            )

            print(
                f"       partition: "
                f"{metadata.partition}"
            )

            print(
                f"       offset: "
                f"{metadata.offset}"
            )

            print(
                f"       event_id: "
                f"{event.get('event_id')}"
            )

            print()

            time.sleep(0.5)

        producer.flush()

        print(
            "All controlled invalid events "
            "were published successfully."
        )

        print()

        print(
            "Expected downstream path:"
        )

        print(
            "Kafka"
        )

        print(
            "  -> Bronze"
        )

        print(
            "  -> Spark data-quality rules"
        )

        print(
            "  -> Quarantine"
        )

    finally:
        producer.close()


if __name__ == "__main__":
    publish_invalid_events()