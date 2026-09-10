import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "vehicle.telemetry"


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "urbanpulse-vehicle-simulator",
    }
)


VEHICLES = [
    {
        "vehicle_id": "JHB-BUS-001",
        "route_id": "ROUTE-01",
        "latitude": -26.2041,
        "longitude": 28.0473,
        "speed_kmh": 35.0,
        "heading": 90,
        "occupancy_status": "moderate",
        "vehicle_status": "in_service",
    },
    {
        "vehicle_id": "JHB-BUS-002",
        "route_id": "ROUTE-02",
        "latitude": -26.1929,
        "longitude": 28.0305,
        "speed_kmh": 28.0,
        "heading": 180,
        "occupancy_status": "low",
        "vehicle_status": "in_service",
    },
    {
        "vehicle_id": "JHB-BUS-003",
        "route_id": "ROUTE-03",
        "latitude": -26.1076,
        "longitude": 28.0567,
        "speed_kmh": 42.0,
        "heading": 270,
        "occupancy_status": "high",
        "vehicle_status": "in_service",
    },
]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered {msg.key().decode('utf-8')} "
            f"to {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"offset {msg.offset()}"
        )


def update_vehicle(vehicle):
    vehicle["speed_kmh"] = max(
        0,
        min(
            80,
            vehicle["speed_kmh"] + random.uniform(-5, 5),
        ),
    )

    vehicle["heading"] = (
        vehicle["heading"] + random.randint(-10, 10)
    ) % 360

    vehicle["latitude"] += random.uniform(-0.0005, 0.0005)
    vehicle["longitude"] += random.uniform(-0.0005, 0.0005)

    return vehicle


def build_event(vehicle):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "vehicle_telemetry",
        "event_version": "1.0",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "urbanpulse_vehicle_simulator",
        "vehicle_id": vehicle["vehicle_id"],
        "route_id": vehicle["route_id"],
        "latitude": round(vehicle["latitude"], 6),
        "longitude": round(vehicle["longitude"], 6),
        "speed_kmh": round(vehicle["speed_kmh"], 2),
        "heading": vehicle["heading"],
        "occupancy_status": vehicle["occupancy_status"],
        "vehicle_status": vehicle["vehicle_status"],
    }


def run_simulator():
    print("UrbanPulse vehicle telemetry simulator started.")
    print(f"Publishing to Kafka topic: {TOPIC}")

    try:
        while True:
            for vehicle in VEHICLES:
                update_vehicle(vehicle)

                event = build_event(vehicle)

                producer.produce(
                    topic=TOPIC,
                    key=vehicle["vehicle_id"],
                    value=json.dumps(event),
                    callback=delivery_report,
                )

                producer.poll(0)

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping vehicle telemetry simulator...")

    finally:
        producer.flush()
        print("Producer shutdown complete.")


if __name__ == "__main__":
    run_simulator()