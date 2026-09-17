"""
UrbanPulse - Correlated Incident + Traffic Scenario Producer

Publishes a controlled synthetic scenario to two Kafka topics:

    incidents.events
    traffic.observations

The incident and traffic observations deliberately share the same road
and 15-minute analytical window so that the UrbanPulse Gold
incident-impact model can correlate them.

IMPORTANT:
This is synthetic / simulated portfolio data. It does not represent
official or observed Johannesburg traffic conditions.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

INCIDENT_TOPIC = "incidents.events"
TRAFFIC_TOPIC = "traffic.observations"

ROAD_NAME = "Jan Smuts Avenue"
SEGMENT_ID = "JHB-SEG-SCENARIO-001"

DATA_CLASSIFICATION = "SYNTHETIC"

# These values intentionally conform to the existing
# UrbanPulse Silver data-quality contracts.
INCIDENT_SOURCE = "urbanpulse_incident_simulator"
TRAFFIC_SOURCE = "urbanpulse_traffic_simulator"


# ============================================================
# Kafka serialization
# ============================================================


def serialize_key(key):
    return key.encode("utf-8")


def serialize_value(value):
    return json.dumps(value).encode("utf-8")


# ============================================================
# Kafka producer
# ============================================================


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=serialize_key,
        value_serializer=serialize_value,
        acks="all",
        retries=5,
    )


# ============================================================
# Timestamp helpers
# ============================================================


def utc_now():
    return datetime.now(timezone.utc)


def timestamp_string(timestamp):
    return timestamp.isoformat()


# ============================================================
# Incident event
# ============================================================


def build_incident_event(scenario_time):
    """
    Build one valid synthetic incident event.

    The event timestamp is shared with the traffic observations so
    that both domains fall into the same 15-minute Gold aggregation
    window.

    The source value conforms to the Incident Silver data-quality
    contract.
    """

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "incident",
        "event_version": "1.0",
        "event_timestamp": timestamp_string(
            scenario_time
        ),
        "source": INCIDENT_SOURCE,
        "incident_id": (
            f"JHB-INC-SCENARIO-"
            f"{uuid.uuid4().hex[:8].upper()}"
        ),
        "segment_id": SEGMENT_ID,
        "road_name": ROAD_NAME,
        "incident_type": "vehicle_breakdown",
        "severity": "high",
        "status": "active",
        "latitude": -26.1450,
        "longitude": 28.0340,
        "description": (
            "Controlled synthetic vehicle breakdown "
            "used to test UrbanPulse cross-domain "
            "incident impact analytics."
        ),
        "reported_at": timestamp_string(
            scenario_time
        ),
        "estimated_clearance_minutes": 30,
        "data_classification": DATA_CLASSIFICATION,
    }


# ============================================================
# Traffic event
# ============================================================


def build_traffic_event(
    scenario_time,
    average_speed_kmh,
    vehicle_count,
    congestion_level,
    travel_time_seconds,
):
    """
    Build one valid synthetic traffic observation.

    The source and congestion values conform to the existing
    Traffic Silver data-quality contract.
    """

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "traffic_observation",
        "event_version": "1.0",
        "event_timestamp": timestamp_string(
            scenario_time
        ),
        "source": TRAFFIC_SOURCE,
        "segment_id": SEGMENT_ID,
        "road_name": ROAD_NAME,
        "latitude": -26.1450,
        "longitude": 28.0340,
        "average_speed_kmh": average_speed_kmh,
        "vehicle_count": vehicle_count,
        "congestion_level": congestion_level,
        "travel_time_seconds": travel_time_seconds,
        "data_classification": DATA_CLASSIFICATION,
    }


# ============================================================
# Controlled scenario
# ============================================================


def build_scenario():
    """
    Build a coordinated synthetic incident/traffic scenario.

    All records intentionally use the same event timestamp so they
    land in exactly the same 15-minute analytical window.

    The scenario represents:

        1. A synthetic high-severity vehicle breakdown.
        2. Moderate traffic conditions.
        3. Progressively lower average speeds.
        4. Increasing vehicle counts.
        5. Heavy congestion.
        6. Increasing travel times.

    This is designed to exercise the UrbanPulse cross-domain
    analytical pipeline. It does not make a causal claim about
    real-world Johannesburg traffic.
    """

    scenario_time = utc_now()

    incident = build_incident_event(
        scenario_time
    )

    traffic_observations = [
        build_traffic_event(
            scenario_time=scenario_time,
            average_speed_kmh=42.0,
            vehicle_count=105,
            congestion_level="moderate",
            travel_time_seconds=95,
        ),
        build_traffic_event(
            scenario_time=scenario_time,
            average_speed_kmh=31.0,
            vehicle_count=145,
            congestion_level="heavy",
            travel_time_seconds=135,
        ),
        build_traffic_event(
            scenario_time=scenario_time,
            average_speed_kmh=22.0,
            vehicle_count=185,
            congestion_level="heavy",
            travel_time_seconds=190,
        ),
        build_traffic_event(
            scenario_time=scenario_time,
            average_speed_kmh=14.0,
            vehicle_count=225,
            congestion_level="heavy",
            travel_time_seconds=280,
        ),
    ]

    return (
        scenario_time,
        incident,
        traffic_observations,
    )


# ============================================================
# Publishing helper
# ============================================================


def publish_event(
    producer,
    topic,
    key,
    event,
):
    future = producer.send(
        topic,
        key=key,
        value=event,
    )

    metadata = future.get(
        timeout=30
    )

    return metadata


# ============================================================
# Publish controlled scenario
# ============================================================


def publish_scenario():
    producer = create_producer()

    (
        scenario_time,
        incident,
        traffic_observations,
    ) = build_scenario()

    print()
    print("=" * 80)
    print(
        "URBANPULSE - CORRELATED INCIDENT + "
        "TRAFFIC SCENARIO"
    )
    print("=" * 80)

    print(
        f"Kafka broker: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Incident topic: "
        f"{INCIDENT_TOPIC}"
    )

    print(
        f"Traffic topic: "
        f"{TRAFFIC_TOPIC}"
    )

    print(
        f"Road: "
        f"{ROAD_NAME}"
    )

    print(
        f"Segment: "
        f"{SEGMENT_ID}"
    )

    print(
        f"Scenario timestamp: "
        f"{timestamp_string(scenario_time)}"
    )

    print(
        "Data classification: "
        "SYNTHETIC / CONTROLLED SCENARIO"
    )

    print()

    print(
        "Incident source: "
        f"{INCIDENT_SOURCE}"
    )

    print(
        "Traffic source: "
        f"{TRAFFIC_SOURCE}"
    )

    print()

    try:
        # ====================================================
        # Publish incident
        # ====================================================

        incident_metadata = publish_event(
            producer=producer,
            topic=INCIDENT_TOPIC,
            key=incident["incident_id"],
            event=incident,
        )

        print(
            "[SENT] INCIDENT"
        )

        print(
            f"    incident_id: "
            f"{incident['incident_id']}"
        )

        print(
            f"    source: "
            f"{incident['source']}"
        )

        print(
            f"    road_name: "
            f"{incident['road_name']}"
        )

        print(
            f"    segment_id: "
            f"{incident['segment_id']}"
        )

        print(
            f"    incident_type: "
            f"{incident['incident_type']}"
        )

        print(
            f"    severity: "
            f"{incident['severity']}"
        )

        print(
            f"    status: "
            f"{incident['status']}"
        )

        print(
            f"    estimated_clearance_minutes: "
            f"{incident['estimated_clearance_minutes']}"
        )

        print(
            f"    partition: "
            f"{incident_metadata.partition}"
        )

        print(
            f"    offset: "
            f"{incident_metadata.offset}"
        )

        print()

        # ====================================================
        # Publish correlated traffic observations
        # ====================================================

        for index, event in enumerate(
            traffic_observations,
            start=1,
        ):
            metadata = publish_event(
                producer=producer,
                topic=TRAFFIC_TOPIC,
                key=event["segment_id"],
                event=event,
            )

            print(
                f"[SENT] TRAFFIC OBSERVATION "
                f"{index}"
            )

            print(
                f"    event_id: "
                f"{event['event_id']}"
            )

            print(
                f"    source: "
                f"{event['source']}"
            )

            print(
                f"    segment_id: "
                f"{event['segment_id']}"
            )

            print(
                f"    road_name: "
                f"{event['road_name']}"
            )

            print(
                f"    speed: "
                f"{event['average_speed_kmh']} km/h"
            )

            print(
                f"    vehicles: "
                f"{event['vehicle_count']}"
            )

            print(
                f"    congestion: "
                f"{event['congestion_level']}"
            )

            print(
                f"    travel_time: "
                f"{event['travel_time_seconds']} seconds"
            )

            print(
                f"    partition: "
                f"{metadata.partition}"
            )

            print(
                f"    offset: "
                f"{metadata.offset}"
            )

            print()

            time.sleep(
                0.25
            )

        producer.flush()

    finally:
        producer.close()

    # ========================================================
    # Completion summary
    # ========================================================

    print("=" * 80)

    print(
        "CORRELATED SYNTHETIC SCENARIO "
        "PUBLISHED SUCCESSFULLY"
    )

    print("=" * 80)

    print()

    print(
        "Published records:"
    )

    print(
        "    Incidents: 1"
    )

    print(
        "    Traffic observations: 4"
    )

    print(
        "    Total: 5"
    )

    print()

    print(
        "Expected incident path:"
    )

    print(
        "incidents.events"
    )

    print(
        "  -> Incident Bronze"
    )

    print(
        "  -> Incident Silver"
    )

    print()

    print(
        "Expected traffic path:"
    )

    print(
        "traffic.observations"
    )

    print(
        "  -> Traffic Bronze"
    )

    print(
        "  -> Traffic Silver"
    )

    print()

    print(
        "Expected analytical relationship:"
    )

    print(
        ROAD_NAME
    )

    print(
        "  + matching 15-minute window"
    )

    print(
        "  + high-severity vehicle breakdown"
    )

    print(
        "  + degraded traffic observations"
    )

    print(
        "  -> Gold incident impact"
    )

    print()

    print(
        "Scenario trace identifier:"
    )

    print(
        f"    {SEGMENT_ID}"
    )

    print()

    print(
        "IMPORTANT:"
    )

    print(
        "All records generated by this scenario "
        "are synthetic controlled test data."
    )

    print(
        "They do not represent official or observed "
        "Johannesburg transport conditions."
    )


if __name__ == "__main__":
    publish_scenario()