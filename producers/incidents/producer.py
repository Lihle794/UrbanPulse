import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = "incidents.events"

SOURCE = "urbanpulse_incident_simulator"

SCHEMA_PATH = (
    Path(__file__).resolve().parents[2]
    / "schemas"
    / "incident.schema.json"
)


ROAD_SEGMENTS = [
    {
        "segment_id": "JHB-SEG-001",
        "road_name": "M1",
        "latitude": -26.1929,
        "longitude": 28.0305,
    },
    {
        "segment_id": "JHB-SEG-002",
        "road_name": "M2",
        "latitude": -26.2041,
        "longitude": 28.0473,
    },
    {
        "segment_id": "JHB-SEG-003",
        "road_name": "Jan Smuts Avenue",
        "latitude": -26.1455,
        "longitude": 28.0358,
    },
    {
        "segment_id": "JHB-SEG-004",
        "road_name": "Oxford Road",
        "latitude": -26.1372,
        "longitude": 28.0431,
    },
    {
        "segment_id": "JHB-SEG-005",
        "road_name": "Louis Botha Avenue",
        "latitude": -26.1598,
        "longitude": 28.0755,
    },
    {
        "segment_id": "JHB-SEG-006",
        "road_name": "Empire Road",
        "latitude": -26.1810,
        "longitude": 28.0265,
    },
]


INCIDENT_TYPES = [
    "collision",
    "road_closure",
    "vehicle_breakdown",
    "construction",
    "road_hazard",
]


SEVERITIES = [
    "low",
    "moderate",
    "high",
    "critical",
]


STATUSES = [
    "active",
    "monitoring",
    "cleared",
]


INCIDENT_DESCRIPTIONS = {
    "collision": [
        "Synthetic multi-vehicle collision reported.",
        "Synthetic minor collision affecting traffic flow.",
        "Synthetic collision partially obstructing the roadway.",
    ],
    "road_closure": [
        "Synthetic temporary road closure reported.",
        "Synthetic lane closure affecting the road segment.",
        "Synthetic full road closure currently active.",
    ],
    "vehicle_breakdown": [
        "Synthetic vehicle breakdown obstructing a lane.",
        "Synthetic disabled vehicle reported on the roadway.",
        "Synthetic breakdown causing local traffic disruption.",
    ],
    "construction": [
        "Synthetic roadworks affecting normal traffic flow.",
        "Synthetic construction activity reducing road capacity.",
        "Synthetic maintenance work affecting the segment.",
    ],
    "road_hazard": [
        "Synthetic road hazard reported.",
        "Synthetic obstruction detected on the roadway.",
        "Synthetic hazardous road condition affecting traffic.",
    ],
}


CLEARANCE_RANGES = {
    "low": (5, 30),
    "moderate": (20, 60),
    "high": (45, 180),
    "critical": (90, 360),
}


def load_schema():
    with open(
        SCHEMA_PATH,
        "r",
        encoding="utf-8",
    ) as schema_file:
        return json.load(schema_file)


SCHEMA = load_schema()

VALIDATOR = Draft202012Validator(
    SCHEMA,
    format_checker=FormatChecker(),
)


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


def generate_incident_id():
    return (
        f"JHB-INC-"
        f"{random.randint(0, 999999):06d}"
    )


def generate_clearance_minutes(
    severity,
):
    minimum, maximum = (
        CLEARANCE_RANGES[severity]
    )

    return random.randint(
        minimum,
        maximum,
    )


def generate_incident_event(
    road_segment=None,
):
    if road_segment is None:
        road_segment = random.choice(
            ROAD_SEGMENTS
        )

    incident_type = random.choice(
        INCIDENT_TYPES
    )

    severity = random.choices(
        population=SEVERITIES,
        weights=[
            40,
            35,
            20,
            5,
        ],
        k=1,
    )[0]

    status = random.choices(
        population=STATUSES,
        weights=[
            65,
            25,
            10,
        ],
        k=1,
    )[0]

    now = utc_timestamp()

    latitude = (
        road_segment["latitude"]
        + random.uniform(
            -0.002,
            0.002,
        )
    )

    longitude = (
        road_segment["longitude"]
        + random.uniform(
            -0.002,
            0.002,
        )
    )

    event = {
        "event_id": str(
            uuid.uuid4()
        ),
        "event_type": "incident",
        "event_version": "1.0",
        "event_timestamp": now,
        "source": SOURCE,
        "incident_id": (
            generate_incident_id()
        ),
        "incident_type": (
            incident_type
        ),
        "severity": severity,
        "status": status,
        "segment_id": (
            road_segment[
                "segment_id"
            ]
        ),
        "road_name": (
            road_segment[
                "road_name"
            ]
        ),
        "latitude": round(
            latitude,
            6,
        ),
        "longitude": round(
            longitude,
            6,
        ),
        "description": random.choice(
            INCIDENT_DESCRIPTIONS[
                incident_type
            ]
        ),
        "reported_at": now,
        "estimated_clearance_minutes": (
            generate_clearance_minutes(
                severity
            )
        ),
    }

    return event


def validate_event(event):
    errors = sorted(
        VALIDATOR.iter_errors(
            event
        ),
        key=lambda error: list(
            error.path
        ),
    )

    if errors:
        messages = []

        for error in errors:
            field = (
                ".".join(
                    str(part)
                    for part
                    in error.path
                )
                or "<root>"
            )

            messages.append(
                f"{field}: "
                f"{error.message}"
            )

        raise ValueError(
            "Incident event failed "
            "schema validation: "
            + "; ".join(messages)
        )


def publish_incident_events():
    producer = create_producer()

    print()
    print(
        "UrbanPulse synthetic incident "
        "simulator started."
    )

    print(
        f"Kafka broker: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Publishing to topic: "
        f"{TOPIC}"
    )

    print(
        "Data classification: "
        "SYNTHETIC / SIMULATED"
    )

    print(
        "Press Ctrl+C to stop."
    )

    print()

    try:
        while True:
            road_segment = (
                random.choice(
                    ROAD_SEGMENTS
                )
            )

            event = (
                generate_incident_event(
                    road_segment
                )
            )

            validate_event(
                event
            )

            future = producer.send(
                TOPIC,
                key=event[
                    "segment_id"
                ],
                value=event,
            )

            metadata = future.get(
                timeout=10
            )

            print(
                "Delivered "
                f"{event['incident_id']} "
                f"| {event['road_name']} "
                f"| {event['incident_type']} "
                f"| {event['severity']} "
                f"| {event['status']} "
                f"| partition "
                f"{metadata.partition} "
                f"| offset "
                f"{metadata.offset}"
            )

            time.sleep(
                random.uniform(
                    1.0,
                    2.5,
                )
            )

    except KeyboardInterrupt:
        print()

        print(
            "UrbanPulse incident "
            "simulator stopped."
        )

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    publish_incident_events()