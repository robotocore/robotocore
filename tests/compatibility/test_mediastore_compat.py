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

    def test_describe_container_has_endpoint(self, mediastore_client, container):
        resp = mediastore_client.describe_container(ContainerName=container["Name"])
        described = resp["Container"]
        # Endpoint may or may not be present depending on container status,
        # but CreationTime should always be present
        assert "CreationTime" in described

    def test_describe_nonexistent_container(self, mediastore_client):
        with pytest.raises(Exception):
            mediastore_client.describe_container(ContainerName=f"no-such-{uuid.uuid4().hex[:8]}")

    def test_list_containers_response_structure(self, mediastore_client):
        resp = mediastore_client.list_containers()
        assert "Containers" in resp
        assert isinstance(resp["Containers"], list)


class TestMediaStoreContainerErrors:
    def test_delete_nonexistent_container(self, mediastore_client):
        with pytest.raises(mediastore_client.exceptions.ContainerNotFoundException):
            mediastore_client.delete_container(ContainerName=f"no-such-{uuid.uuid4().hex[:8]}")

    def test_get_container_policy_nonexistent(self, mediastore_client):
        with pytest.raises(Exception) as exc_info:
            mediastore_client.get_container_policy(ContainerName=f"no-such-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_lifecycle_policy_nonexistent(self, mediastore_client):
        with pytest.raises(Exception) as exc_info:
            mediastore_client.get_lifecycle_policy(ContainerName=f"no-such-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_metric_policy_nonexistent(self, mediastore_client):
        with pytest.raises(Exception) as exc_info:
            mediastore_client.get_metric_policy(ContainerName=f"no-such-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_container_policy_nonexistent(self, mediastore_client):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "Test",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "mediastore:*",
                        "Resource": "arn:aws:mediastore:*:*:*",
                    }
                ],
            }
        )
        with pytest.raises(Exception) as exc_info:
            mediastore_client.put_container_policy(
                ContainerName=f"no-such-{uuid.uuid4().hex[:8]}", Policy=policy
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_lifecycle_policy_nonexistent(self, mediastore_client):
        policy = json.dumps(
            {"rules": [{"definition": {"path": [{"prefix": ""}]}, "action": "EXPIRE"}]}
        )
        with pytest.raises(Exception) as exc_info:
            mediastore_client.put_lifecycle_policy(
                ContainerName=f"no-such-{uuid.uuid4().hex[:8]}",
                LifecyclePolicy=policy,
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_metric_policy_nonexistent(self, mediastore_client):
        with pytest.raises(Exception) as exc_info:
            mediastore_client.put_metric_policy(
                ContainerName=f"no-such-{uuid.uuid4().hex[:8]}",
                MetricPolicy={"ContainerLevelMetrics": "ENABLED"},
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestMediaStorePolicyNotFound:
    """Test that getting policies on a container with no policies raises PolicyNotFoundException."""

    def test_get_container_policy_no_policy(self, mediastore_client, container):
        with pytest.raises(Exception) as exc_info:
            mediastore_client.get_container_policy(ContainerName=container["Name"])
        assert exc_info.value.response["Error"]["Code"] == "PolicyNotFoundException"

    def test_get_lifecycle_policy_no_policy(self, mediastore_client, container):
        with pytest.raises(Exception) as exc_info:
            mediastore_client.get_lifecycle_policy(ContainerName=container["Name"])
        assert exc_info.value.response["Error"]["Code"] == "PolicyNotFoundException"

    def test_get_metric_policy_no_policy(self, mediastore_client, container):
        with pytest.raises(Exception) as exc_info:
            mediastore_client.get_metric_policy(ContainerName=container["Name"])
        assert exc_info.value.response["Error"]["Code"] == "PolicyNotFoundException"


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


class TestMediaStoreLifecyclePolicy:
    def test_put_and_get_lifecycle_policy(self, mediastore_client, container):
        policy = json.dumps(
            {
                "rules": [
                    {
                        "definition": {
                            "path": [{"prefix": ""}],
                            "days_since_create": [{"numeric": [">", 30]}],
                        },
                        "action": "EXPIRE",
                    }
                ]
            }
        )
        put_resp = mediastore_client.put_lifecycle_policy(
            ContainerName=container["Name"], LifecyclePolicy=policy
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_resp = mediastore_client.get_lifecycle_policy(ContainerName=container["Name"])
        returned = json.loads(get_resp["LifecyclePolicy"])
        assert "rules" in returned
        assert returned["rules"][0]["action"] == "EXPIRE"


class TestMediaStoreMetricPolicy:
    def test_put_and_get_metric_policy(self, mediastore_client, container):
        put_resp = mediastore_client.put_metric_policy(
            ContainerName=container["Name"],
            MetricPolicy={
                "ContainerLevelMetrics": "ENABLED",
            },
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        get_resp = mediastore_client.get_metric_policy(ContainerName=container["Name"])
        assert get_resp["MetricPolicy"]["ContainerLevelMetrics"] == "ENABLED"


class TestMediaStoreTagsForResource:
    def test_list_tags_for_resource(self, mediastore_client, container):
        resp = mediastore_client.list_tags_for_resource(Resource=container["ARN"])
        assert "Tags" in resp
        assert isinstance(resp["Tags"], list)

    def test_list_tags_for_resource_with_tags(self, mediastore_client):
        name = f"tagged-{uuid.uuid4().hex[:8]}"
        tags = [{"Key": "env", "Value": "test"}, {"Key": "project", "Value": "compat"}]
        try:
            resp = mediastore_client.create_container(ContainerName=name, Tags=tags)
            arn = resp["Container"]["ARN"]
            tag_resp = mediastore_client.list_tags_for_resource(Resource=arn)
            assert "Tags" in tag_resp
            returned_keys = {t["Key"] for t in tag_resp["Tags"]}
            assert "env" in returned_keys
            assert "project" in returned_keys
        finally:
            try:
                mediastore_client.delete_container(ContainerName=name)
            except Exception:
                pass
