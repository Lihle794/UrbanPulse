import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema import FormatChecker
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = "traffic.observations"

SOURCE = "urbanpulse_traffic_simulator"

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SCHEMA_PATH = (
    PROJECT_ROOT
    / "schemas"
    / "traffic_observation.schema.json"
)


ROAD_SEGMENTS = [
    {
        "segment_id": "JHB-SEG-001",
        "road_name": "M1",
        "latitude": -26.1767,
        "longitude": 28.0360,
        "normal_speed": 80,
    },
    {
        "segment_id": "JHB-SEG-002",
        "road_name": "M2",
        "latitude": -26.2110,
        "longitude": 28.0610,
        "normal_speed": 80,
    },
    {
        "segment_id": "JHB-SEG-003",
        "road_name": "Jan Smuts Avenue",
        "latitude": -26.1455,
        "longitude": 28.0342,
        "normal_speed": 60,
    },
    {
        "segment_id": "JHB-SEG-004",
        "road_name": "Oxford Road",
        "latitude": -26.1435,
        "longitude": 28.0430,
        "normal_speed": 60,
    },
    {
        "segment_id": "JHB-SEG-005",
        "road_name": "Louis Botha Avenue",
        "latitude": -26.1700,
        "longitude": 28.0740,
        "normal_speed": 60,
    },
    {
        "segment_id": "JHB-SEG-006",
        "road_name": "Empire Road",
        "latitude": -26.1830,
        "longitude": 28.0310,
        "normal_speed": 60,
    },
]


def load_schema() -> dict:
    """
    Load the UrbanPulse traffic observation
    JSON Schema.
    """
    with SCHEMA_PATH.open(
        "r",
        encoding="utf-8",
    ) as schema_file:
        return json.load(schema_file)


TRAFFIC_SCHEMA = load_schema()

VALIDATOR = Draft202012Validator(
    TRAFFIC_SCHEMA,
    format_checker=FormatChecker(),
)


def serialize_key(key: str) -> bytes:
    return key.encode("utf-8")


def serialize_value(value: dict) -> bytes:
    return json.dumps(value).encode("utf-8")


def create_producer() -> KafkaProducer:
    """
    Create the Kafka producer used by the
    synthetic traffic simulator.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=serialize_key,
        value_serializer=serialize_value,
        acks="all",
    )


def current_timestamp() -> str:
    """
    Return the current UTC timestamp.
    """
    return datetime.now(
        timezone.utc
    ).isoformat()


def determine_congestion(
    normal_speed: float,
    observed_speed: float,
) -> str:
    """
    Derive a congestion category from the ratio
    between observed and normal road speed.
    """
    if normal_speed <= 0:
        return "severe"

    ratio = observed_speed / normal_speed

    if ratio >= 0.75:
        return "free_flow"

    if ratio >= 0.50:
        return "moderate"

    if ratio >= 0.25:
        return "heavy"

    return "severe"


def generate_speed(
    normal_speed: int,
) -> float:
    """
    Generate a synthetic observed speed.

    Most observations represent normal or moderate
    conditions, while some simulate heavier
    congestion.
    """
    scenario = random.choices(
        population=[
            "normal",
            "moderate",
            "heavy",
            "severe",
        ],
        weights=[
            45,
            30,
            18,
            7,
        ],
        k=1,
    )[0]

    if scenario == "normal":
        lower = normal_speed * 0.75
        upper = normal_speed

    elif scenario == "moderate":
        lower = normal_speed * 0.50
        upper = normal_speed * 0.74

    elif scenario == "heavy":
        lower = normal_speed * 0.25
        upper = normal_speed * 0.49

    else:
        lower = 3
        upper = max(
            5,
            normal_speed * 0.24,
        )

    return round(
        random.uniform(
            lower,
            upper,
        ),
        1,
    )


def estimate_vehicle_count(
    congestion_level: str,
) -> int:
    """
    Generate a synthetic vehicle count associated
    with the current congestion level.
    """
    ranges = {
        "free_flow": (15, 80),
        "moderate": (60, 160),
        "heavy": (130, 280),
        "severe": (220, 420),
    }

    lower, upper = ranges[
        congestion_level
    ]

    return random.randint(
        lower,
        upper,
    )


def estimate_travel_time(
    average_speed_kmh: float,
) -> int:
    """
    Estimate travel time across a simulated
    one-kilometre road segment.

    A minimum speed is used only for the calculation
    to avoid division by zero.
    """
    calculation_speed = max(
        average_speed_kmh,
        1.0,
    )

    hours = 1.0 / calculation_speed

    seconds = int(
        hours * 3600
    )

    return min(
        max(seconds, 1),
        7200,
    )


def generate_traffic_event(
    segment: dict,
) -> dict:
    """
    Generate one valid synthetic traffic observation.
    """
    average_speed = generate_speed(
        segment["normal_speed"]
    )

    congestion_level = (
        determine_congestion(
            segment["normal_speed"],
            average_speed,
        )
    )

    vehicle_count = (
        estimate_vehicle_count(
            congestion_level
        )
    )

    travel_time = (
        estimate_travel_time(
            average_speed
        )
    )

    return {
        "event_id": str(
            uuid.uuid4()
        ),
        "event_type": (
            "traffic_observation"
        ),
        "event_version": "1.0",
        "event_timestamp": (
            current_timestamp()
        ),
        "source": SOURCE,
        "segment_id": (
            segment["segment_id"]
        ),
        "road_name": (
            segment["road_name"]
        ),
        "latitude": (
            segment["latitude"]
        ),
        "longitude": (
            segment["longitude"]
        ),
        "average_speed_kmh": (
            average_speed
        ),
        "vehicle_count": (
            vehicle_count
        ),
        "congestion_level": (
            congestion_level
        ),
        "travel_time_seconds": (
            travel_time
        ),
    }


def validate_event(
    event: dict,
) -> None:
    """
    Validate an event before it is allowed
    onto the Kafka topic.
    """
    errors = sorted(
        VALIDATOR.iter_errors(event),
        key=lambda error: list(
            error.absolute_path
        ),
    )

    if not errors:
        return

    messages = []

    for error in errors:
        location = ".".join(
            str(part)
            for part
            in error.absolute_path
        )

        if not location:
            location = "<root>"

        messages.append(
            f"{location}: "
            f"{error.message}"
        )

    raise ValueError(
        "Traffic event failed schema "
        "validation: "
        + " | ".join(messages)
    )


def publish_traffic_events() -> None:
    """
    Continuously generate, validate and publish
    synthetic Johannesburg traffic observations.
    """
    producer = create_producer()

    print()
    print(
        "UrbanPulse synthetic traffic "
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

    print()

    try:
        while True:
            for segment in ROAD_SEGMENTS:
                event = (
                    generate_traffic_event(
                        segment
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
                    f"Delivered "
                    f"{event['segment_id']} "
                    f"| {event['road_name']} "
                    f"| "
                    f"{event['average_speed_kmh']} "
                    f"km/h "
                    f"| "
                    f"{event['congestion_level']} "
                    f"| partition "
                    f"{metadata.partition} "
                    f"| offset "
                    f"{metadata.offset}"
                )

            producer.flush()

            time.sleep(5)

    except KeyboardInterrupt:
        print()
        print(
            "UrbanPulse traffic simulator "
            "stopped."
        )

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    publish_traffic_events()