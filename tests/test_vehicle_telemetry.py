from copy import deepcopy

from producers.vehicle_telemetry.producer import (
    VEHICLES,
    build_event,
    update_vehicle,
    validate_event,
)


def test_build_event_contains_required_fields():
    vehicle = deepcopy(VEHICLES[0])

    event = build_event(vehicle)

    expected_fields = {
        "event_id",
        "event_type",
        "event_version",
        "event_timestamp",
        "source",
        "vehicle_id",
        "route_id",
        "latitude",
        "longitude",
        "speed_kmh",
        "heading",
        "occupancy_status",
        "vehicle_status",
    }

    assert set(event.keys()) == expected_fields


def test_build_event_preserves_vehicle_identity():
    vehicle = deepcopy(VEHICLES[0])

    event = build_event(vehicle)

    assert event["vehicle_id"] == vehicle["vehicle_id"]
    assert event["route_id"] == vehicle["route_id"]


def test_build_event_generates_unique_event_ids():
    vehicle = deepcopy(VEHICLES[0])

    first_event = build_event(vehicle)
    second_event = build_event(vehicle)

    assert first_event["event_id"] != second_event["event_id"]


def test_valid_event_passes_validation():
    vehicle = deepcopy(VEHICLES[0])

    event = build_event(vehicle)

    assert validate_event(event) is True


def test_invalid_speed_fails_validation():
    vehicle = deepcopy(VEHICLES[0])

    event = build_event(vehicle)
    event["speed_kmh"] = 900

    assert validate_event(event) is False


def test_invalid_latitude_fails_validation():
    vehicle = deepcopy(VEHICLES[0])

    event = build_event(vehicle)
    event["latitude"] = 400

    assert validate_event(event) is False


def test_invalid_occupancy_status_fails_validation():
    vehicle = deepcopy(VEHICLES[0])

    event = build_event(vehicle)
    event["occupancy_status"] = "extremely_full"

    assert validate_event(event) is False


def test_missing_required_field_fails_validation():
    vehicle = deepcopy(VEHICLES[0])

    event = build_event(vehicle)
    del event["vehicle_id"]

    assert validate_event(event) is False

def test_invalid_timestamp_fails_validation():
    vehicle = deepcopy(VEHICLES[0])

    event = build_event(vehicle)
    event["event_timestamp"] = "not-a-valid-timestamp"

    assert validate_event(event) is False    


def test_update_vehicle_keeps_speed_within_bounds():
    vehicle = deepcopy(VEHICLES[0])

    for _ in range(100):
        update_vehicle(vehicle)

        assert 0 <= vehicle["speed_kmh"] <= 80


def test_update_vehicle_keeps_heading_within_bounds():
    vehicle = deepcopy(VEHICLES[0])

    for _ in range(100):
        update_vehicle(vehicle)

        assert 0 <= vehicle["heading"] <= 359