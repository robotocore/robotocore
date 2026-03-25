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

    def test_delete_thing_shadow(self, iot, iotdata):
        thing_name = "test-thing-delete-shadow"
        iot.create_thing(thingName=thing_name)
        try:
            payload = {"state": {"desired": {"color": "blue"}}}
            iotdata.update_thing_shadow(
                thingName=thing_name,
                payload=json.dumps(payload),
            )
            resp = iotdata.delete_thing_shadow(thingName=thing_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "payload" in resp
        finally:
            iot.delete_thing(thingName=thing_name)

    def test_list_named_shadows_for_thing(self, iot, iotdata):
        thing_name = "test-thing-named-shadows"
        iot.create_thing(thingName=thing_name)
        try:
            shadow_name = "my-shadow"
            payload = {"state": {"desired": {"level": 5}}}
            iotdata.update_thing_shadow(
                thingName=thing_name,
                shadowName=shadow_name,
                payload=json.dumps(payload),
            )
            resp = iotdata.list_named_shadows_for_thing(thingName=thing_name)
            assert "results" in resp
            assert shadow_name in resp["results"]
        finally:
            iot.delete_thing(thingName=thing_name)

    def test_publish(self, iotdata):
        resp = iotdata.publish(
            topic="test/topic",
            qos=0,
            payload=b"hello",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
