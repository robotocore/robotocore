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

    def test_delete_connection(self, iotdata):
        # DeleteConnection disconnects an MQTT client; in the emulator
        # connections are not tracked so any clientId succeeds.
        resp = iotdata.delete_connection(clientId="my-test-client")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_retained_messages_empty(self, iotdata):
        # ListRetainedMessages on a fresh account returns an empty list.
        resp = iotdata.list_retained_messages()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "retainedTopics" in resp

    def test_get_retained_message_not_found(self, iotdata):
        # GetRetainedMessage on a non-existent topic raises ResourceNotFoundException.
        with pytest.raises(iotdata.exceptions.ResourceNotFoundException):
            iotdata.get_retained_message(topic="no/such/topic")

    def test_publish_with_retain_then_list_and_get(self, iotdata):
        topic = "sensors/temp/retained-test"
        payload = b"25.5"
        # Publish with retain=True stores the message.
        iotdata.publish(topic=topic, qos=1, payload=payload, retain=True)
        # ListRetainedMessages should include the topic.
        list_resp = iotdata.list_retained_messages()
        assert "retainedTopics" in list_resp
        topics = [m["topic"] for m in list_resp["retainedTopics"]]
        assert topic in topics
        # GetRetainedMessage should return the stored message.
        get_resp = iotdata.get_retained_message(topic=topic)
        assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert get_resp["topic"] == topic
        assert get_resp["qos"] == 1
        assert "lastModifiedTime" in get_resp
