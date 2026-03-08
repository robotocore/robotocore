"""Compatibility tests for AWS MediaStore (mediastore)."""

import json
import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def mediastore_client():
    return make_client("mediastore")


@pytest.fixture
def container(mediastore_client):
    name = f"compat-{uuid.uuid4().hex[:8]}"
    resp = mediastore_client.create_container(ContainerName=name)
    yield resp["Container"]
    try:
        mediastore_client.delete_container(ContainerName=name)
    except Exception:
        pass


class TestMediaStoreContainers:
    def test_create_container(self, mediastore_client):
        name = f"create-{uuid.uuid4().hex[:8]}"
        resp = mediastore_client.create_container(ContainerName=name)
        container = resp["Container"]
        assert container["Name"] == name
        assert "ARN" in container
        assert container["Status"] in ("CREATING", "ACTIVE")
        # cleanup
        mediastore_client.delete_container(ContainerName=name)

    def test_list_containers(self, mediastore_client, container):
        resp = mediastore_client.list_containers()
        names = [c["Name"] for c in resp["Containers"]]
        assert container["Name"] in names

    def test_describe_container(self, mediastore_client, container):
        resp = mediastore_client.describe_container(ContainerName=container["Name"])
        described = resp["Container"]
        assert described["Name"] == container["Name"]
        assert described["ARN"] == container["ARN"]
        assert described["Status"] in ("CREATING", "ACTIVE")

    def test_delete_container(self, mediastore_client):
        name = f"delete-{uuid.uuid4().hex[:8]}"
        mediastore_client.create_container(ContainerName=name)
        mediastore_client.delete_container(ContainerName=name)
        # Verify it no longer exists
        with pytest.raises(Exception):
            mediastore_client.describe_container(ContainerName=name)

    def test_describe_nonexistent_container(self, mediastore_client):
        with pytest.raises(Exception):
            mediastore_client.describe_container(ContainerName=f"no-such-{uuid.uuid4().hex[:8]}")


class TestMediaStoreContainerPolicy:
    def test_put_and_get_container_policy(self, mediastore_client, container):
        arn = container["ARN"]
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "CompatTest",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "mediastore:*",
                        "Resource": f"{arn}/*",
                    }
                ],
            }
        )
        put_resp = mediastore_client.put_container_policy(
            ContainerName=container["Name"], Policy=policy
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_resp = mediastore_client.get_container_policy(ContainerName=container["Name"])
        returned_policy = json.loads(get_resp["Policy"])
        assert returned_policy["Version"] == "2012-10-17"
        assert len(returned_policy["Statement"]) == 1
        assert returned_policy["Statement"][0]["Sid"] == "CompatTest"
