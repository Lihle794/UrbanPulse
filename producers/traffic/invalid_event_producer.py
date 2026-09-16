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

TOPIC = "traffic.observations"
SOURCE = "urbanpulse_traffic_simulator"


def serialize_key(key):
    if key is None:
        return None

    return str(key).encode("utf-8")


def serialize_value(value):
    return json.dumps(value).encode("utf-8")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=serialize_key,
        value_serializer=serialize_value,
        acks="all",
    )


def utc_timestamp():
    return datetime.now(
        timezone.utc
    ).isoformat()


def base_event(
    segment_id="JHB-SEG-900",
):
    """
    Create a structurally normal synthetic traffic
    observation that can be modified for controlled
    data-quality failure testing.

    These events intentionally bypass the normal
    producer-side JSON Schema validation.
    """

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "traffic_observation",
        "event_version": "1.0",
        "event_timestamp": utc_timestamp(),
        "source": SOURCE,
        "segment_id": segment_id,
        "road_name": "UrbanPulse Test Segment",
        "latitude": -26.2041,
        "longitude": 28.0473,
        "average_speed_kmh": 45.0,
        "vehicle_count": 80,
        "congestion_level": "moderate",
        "travel_time_seconds": 80,
    }


def build_invalid_events():
    """
    Build a controlled set of intentionally invalid
    synthetic traffic observations.

    Each scenario targets one or more Silver-layer
    data-quality rules.
    """

    events = []

    # 1. Impossible average speed
    event = base_event(
        "JHB-SEG-901"
    )

    event[
        "average_speed_kmh"
    ] = 900.0

    events.append(
        (
            "invalid_speed",
            event,
        )
    )

    # 2. Invalid latitude
    event = base_event(
        "JHB-SEG-902"
    )

    event["latitude"] = 400.0

    events.append(
        (
            "invalid_latitude",
            event,
        )
    )

    # 3. Invalid longitude
    event = base_event(
        "JHB-SEG-903"
    )

    event["longitude"] = -500.0

    events.append(
        (
            "invalid_longitude",
            event,
        )
    )

    # 4. Negative vehicle count
    event = base_event(
        "JHB-SEG-904"
    )

    event["vehicle_count"] = -25

    events.append(
        (
            "invalid_vehicle_count",
            event,
        )
    )

    # 5. Unsupported congestion category
    event = base_event(
        "JHB-SEG-905"
    )

    event[
        "congestion_level"
    ] = "gridlocked_forever"

    events.append(
        (
            "invalid_congestion_level",
            event,
        )
    )

    # 6. Missing segment ID
    event = base_event(
        "JHB-SEG-906"
    )

    event.pop(
        "segment_id"
    )

    events.append(
        (
            "missing_segment_id",
            event,
        )
    )

    # 7. Invalid travel time
    event = base_event(
        "JHB-SEG-907"
    )

    event[
        "travel_time_seconds"
    ] = 99999

    events.append(
        (
            "invalid_travel_time",
            event,
        )
    )

    # 8. Multiple simultaneous quality failures
    event = base_event(
        "JHB-SEG-908"
    )

    event[
        "average_speed_kmh"
    ] = -100.0

    event[
        "vehicle_count"
    ] = -500

    event[
        "congestion_level"
    ] = "impossible"

    event[
        "latitude"
    ] = 999.0

    event[
        "travel_time_seconds"
    ] = 0

    events.append(
        (
            "multiple_quality_failures",
            event,
        )
    )

    return events


def publish_invalid_events():
    """
    Publish the controlled invalid traffic events
    directly to Kafka.

    This deliberately bypasses the normal traffic
    producer's schema validation so the downstream
    Silver quality gate can be tested.
    """

    producer = create_producer()

    invalid_events = (
        build_invalid_events()
    )

    print()
    print(
        "=" * 72
    )

    print(
        "URBANPULSE — CONTROLLED INVALID "
        "TRAFFIC EVENT TEST"
    )

    print(
        "=" * 72
    )

    print(
        f"Kafka broker: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Topic: {TOPIC}"
    )

    print(
        "Data classification: "
        "SYNTHETIC / INTENTIONALLY INVALID"
    )

    print()

    try:
        for (
            scenario,
            event,
        ) in invalid_events:

            kafka_key = event.get(
                "segment_id",
                "INVALID-SEGMENT",
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
                f"[SENT] {scenario}"
            )

            print(
                f"       segment_id: "
                f"{event.get('segment_id')}"
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

            time.sleep(0.25)

        producer.flush()

        print(
            "All controlled invalid traffic "
            "events were published successfully."
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

        print()

        print(
            f"Expected quarantined records "
            f"from this test: "
            f"{len(invalid_events)}"
        )

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    publish_invalid_events()