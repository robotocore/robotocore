"""Compatibility tests for IoT Data Plane service."""

import json

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def iot():
    return make_client("iot")


@pytest.fixture
def iotdata():
    return make_client("iot-data")


class TestIoTDataOperations:
    """Tests for IoT Data Plane operations."""

    def test_update_and_get_thing_shadow(self, iot, iotdata):
        thing_name = "test-thing-shadow"
        # Create the thing first via the IoT control plane
        iot.create_thing(thingName=thing_name)
        try:
            payload = {"state": {"desired": {"temperature": 72}}}
            iotdata.update_thing_shadow(
                thingName=thing_name,
                payload=json.dumps(payload),
            )
            resp = iotdata.get_thing_shadow(thingName=thing_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            shadow = json.loads(resp["payload"].read())
            assert "state" in shadow
        finally:
            iot.delete_thing(thingName=thing_name)

    def test_get_thing_shadow_not_found(self, iot, iotdata):
        thing_name = "nonexistent-thing-for-shadow"
        iot.create_thing(thingName=thing_name)
        try:
            with pytest.raises(iotdata.exceptions.ResourceNotFoundException):
                iotdata.get_thing_shadow(thingName=thing_name)
        finally:
            iot.delete_thing(thingName=thing_name)
