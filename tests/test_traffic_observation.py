import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from jsonschema import FormatChecker

from producers.traffic.producer import (
    ROAD_SEGMENTS,
    determine_congestion,
    estimate_travel_time,
    generate_traffic_event,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCHEMA_PATH = (
    PROJECT_ROOT
    / "schemas"
    / "traffic_observation.schema.json"
)


@pytest.fixture(scope="module")
def validator():
    with SCHEMA_PATH.open(
        "r",
        encoding="utf-8",
    ) as schema_file:
        schema = json.load(schema_file)

    return Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )


def test_generated_event_matches_schema(
    validator,
):
    segment = ROAD_SEGMENTS[0]

    event = generate_traffic_event(
        segment
    )

    errors = list(
        validator.iter_errors(event)
    )

    assert errors == []


def test_all_segments_generate_valid_events(
    validator,
):
    for segment in ROAD_SEGMENTS:
        event = generate_traffic_event(
            segment
        )

        errors = list(
            validator.iter_errors(event)
        )

        assert errors == []


def test_event_type_is_correct():
    event = generate_traffic_event(
        ROAD_SEGMENTS[0]
    )

    assert (
        event["event_type"]
        == "traffic_observation"
    )


def test_event_version_is_correct():
    event = generate_traffic_event(
        ROAD_SEGMENTS[0]
    )

    assert event["event_version"] == "1.0"


def test_source_is_correct():
    event = generate_traffic_event(
        ROAD_SEGMENTS[0]
    )

    assert (
        event["source"]
        == "urbanpulse_traffic_simulator"
    )


def test_segment_id_is_preserved():
    segment = ROAD_SEGMENTS[0]

    event = generate_traffic_event(
        segment
    )

    assert (
        event["segment_id"]
        == segment["segment_id"]
    )


def test_road_name_is_preserved():
    segment = ROAD_SEGMENTS[0]

    event = generate_traffic_event(
        segment
    )

    assert (
        event["road_name"]
        == segment["road_name"]
    )


def test_speed_is_within_valid_range():
    for _ in range(100):
        event = generate_traffic_event(
            ROAD_SEGMENTS[0]
        )

        assert (
            0
            <= event["average_speed_kmh"]
            <= 120
        )


def test_vehicle_count_is_within_valid_range():
    for _ in range(100):
        event = generate_traffic_event(
            ROAD_SEGMENTS[0]
        )

        assert (
            0
            <= event["vehicle_count"]
            <= 500
        )


def test_travel_time_is_within_valid_range():
    for _ in range(100):
        event = generate_traffic_event(
            ROAD_SEGMENTS[0]
        )

        assert (
            1
            <= event["travel_time_seconds"]
            <= 7200
        )


@pytest.mark.parametrize(
    (
        "normal_speed",
        "observed_speed",
        "expected",
    ),
    [
        (
            80,
            80,
            "free_flow",
        ),
        (
            80,
            60,
            "free_flow",
        ),
        (
            80,
            59,
            "moderate",
        ),
        (
            80,
            40,
            "moderate",
        ),
        (
            80,
            39,
            "heavy",
        ),
        (
            80,
            20,
            "heavy",
        ),
        (
            80,
            19,
            "severe",
        ),
        (
            80,
            5,
            "severe",
        ),
    ],
)
def test_congestion_calculation(
    normal_speed,
    observed_speed,
    expected,
):
    result = determine_congestion(
        normal_speed,
        observed_speed,
    )

    assert result == expected


def test_travel_time_increases_when_speed_drops():
    faster = estimate_travel_time(
        60
    )

    slower = estimate_travel_time(
        20
    )

    assert slower > faster


def test_event_contains_required_fields():
    event = generate_traffic_event(
        ROAD_SEGMENTS[0]
    )

    required_fields = {
        "event_id",
        "event_type",
        "event_version",
        "event_timestamp",
        "source",
        "segment_id",
        "road_name",
        "latitude",
        "longitude",
        "average_speed_kmh",
        "vehicle_count",
        "congestion_level",
        "travel_time_seconds",
    }

    assert required_fields.issubset(
        event.keys()
    )


def test_congestion_level_is_valid():
    allowed_levels = {
        "free_flow",
        "moderate",
        "heavy",
        "severe",
    }

    for _ in range(100):
        event = generate_traffic_event(
            ROAD_SEGMENTS[0]
        )

        assert (
            event["congestion_level"]
            in allowed_levels
        )