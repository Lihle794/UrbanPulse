import json
import os
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = "incidents.events"


def serialize_key(key):
    return key.encode("utf-8")


def serialize_value(value):
    return json.dumps(value).encode("utf-8")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=serialize_key,
        value_serializer=serialize_value,
        acks="all",
    )


def current_timestamp():
    return datetime.now(timezone.utc).isoformat()


def base_event(
    incident_id=None,
):
    """
    Create a valid synthetic incident event.

    Individual test cases modify this baseline to deliberately
    violate Silver-layer data-quality rules.
    """

    if incident_id is None:
        incident_id = (
            f"JHB-INC-TEST-{uuid.uuid4().hex[:8].upper()}"
        )

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "incident",
        "event_version": "1.0",
        "event_timestamp": current_timestamp(),
        "source": "urbanpulse_incident_quality_test",
        "incident_id": incident_id,
        "road_name": "Jan Smuts Avenue",
        "incident_type": "vehicle_breakdown",
        "severity": "moderate",
        "status": "active",
        "latitude": -26.1450,
        "longitude": 28.0340,
        "description": (
            "Synthetic incident generated specifically "
            "for UrbanPulse data-quality testing."
        ),
        "data_classification": "SYNTHETIC",
    }


def build_invalid_events():
    """
    Build controlled invalid incident events.

    These deliberately bypass the normal incident producer's
    schema validation so the downstream Spark Silver pipeline
    can demonstrate quarantine behaviour.
    """

    scenarios = []

    # ---------------------------------------------------------
    # 1. Invalid severity
    # ---------------------------------------------------------

    event = base_event()
    event["severity"] = "catastrophic"

    scenarios.append(
        (
            "invalid_severity",
            event,
        )
    )

    # ---------------------------------------------------------
    # 2. Invalid incident type
    # ---------------------------------------------------------

    event = base_event()
    event["incident_type"] = "alien_invasion"

    scenarios.append(
        (
            "invalid_incident_type",
            event,
        )
    )

    # ---------------------------------------------------------
    # 3. Invalid status
    # ---------------------------------------------------------

    event = base_event()
    event["status"] = "teleported"

    scenarios.append(
        (
            "invalid_status",
            event,
        )
    )

    # ---------------------------------------------------------
    # 4. Invalid latitude
    # ---------------------------------------------------------

    event = base_event()
    event["latitude"] = 400.0

    scenarios.append(
        (
            "invalid_latitude",
            event,
        )
    )

    # ---------------------------------------------------------
    # 5. Invalid longitude
    # ---------------------------------------------------------

    event = base_event()
    event["longitude"] = -500.0

    scenarios.append(
        (
            "invalid_longitude",
            event,
        )
    )

    # ---------------------------------------------------------
    # 6. Missing incident ID
    # ---------------------------------------------------------

    event = base_event()
    event["incident_id"] = None

    scenarios.append(
        (
            "missing_incident_id",
            event,
        )
    )

    # ---------------------------------------------------------
    # 7. Invalid event timestamp
    # ---------------------------------------------------------

    event = base_event()
    event["event_timestamp"] = "not-a-valid-timestamp"

    scenarios.append(
        (
            "invalid_event_timestamp",
            event,
        )
    )

    # ---------------------------------------------------------
    # 8. Multiple simultaneous failures
    # ---------------------------------------------------------

    event = base_event()

    event["severity"] = "impossible"
    event["status"] = "unknown_status"
    event["latitude"] = 999.0
    event["longitude"] = 999.0

    scenarios.append(
        (
            "multiple_quality_failures",
            event,
        )
    )

    return scenarios


def publish_invalid_events():
    producer = create_producer()

    scenarios = build_invalid_events()

    print()
    print("=" * 80)
    print("URBANPULSE - CONTROLLED INVALID INCIDENT PRODUCER")
    print("=" * 80)

    print(
        f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Publishing to topic: {TOPIC}"
    )

    print(
        "Data classification: SYNTHETIC / "
        "CONTROLLED QUALITY TEST"
    )

    print(
        f"Number of scenarios: {len(scenarios)}"
    )

    print()

    try:
        for scenario_name, event in scenarios:

            key = (
                event.get("incident_id")
                or event["event_id"]
            )

            future = producer.send(
                TOPIC,
                key=key,
                value=event,
            )

            metadata = future.get(
                timeout=30
            )

            print(
                f"[SENT] {scenario_name}"
            )

            print(
                "    incident_id: "
                f"{event.get('incident_id')}"
            )

            print(
                "    partition: "
                f"{metadata.partition}"
            )

            print(
                "    offset: "
                f"{metadata.offset}"
            )

            print(
                "    event_id: "
                f"{event['event_id']}"
            )

            print()

        producer.flush()

    finally:
        producer.close()

    print("=" * 80)

    print(
        "All controlled invalid incident events "
        "were published successfully."
    )

    print("=" * 80)

    print()

    print("Expected downstream path:")

    print(
        "Kafka"
    )

    print(
        "  -> Bronze"
    )

    print(
        "  -> Spark incident data-quality rules"
    )

    print(
        "  -> Quarantine"
    )

    print()

    print(
        "IMPORTANT: These records are intentionally "
        "invalid synthetic test data."
    )


if __name__ == "__main__":
    publish_invalid_events()