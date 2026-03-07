"""Compatibility tests for AWS Cloud Directory service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def clouddirectory_client():
    return make_client("clouddirectory")


@pytest.fixture
def schema(clouddirectory_client):
    """Create a development schema and clean it up after the test."""
    name = f"test-{uuid.uuid4().hex[:8]}"
    resp = clouddirectory_client.create_schema(Name=name)
    arn = resp["SchemaArn"]
    yield {"Name": name, "SchemaArn": arn}
    try:
        clouddirectory_client.delete_schema(SchemaArn=arn)
    except Exception:
        pass


class TestCloudDirectoryCompat:
    """Tests for Cloud Directory operations."""

    def test_create_schema(self, clouddirectory_client):
        """create_schema returns a valid SchemaArn."""
        name = f"test-{uuid.uuid4().hex[:8]}"
        resp = clouddirectory_client.create_schema(Name=name)
        arn = resp["SchemaArn"]
        assert "SchemaArn" in resp
        assert name in arn
        assert ":schema/development/" in arn
        # cleanup
        clouddirectory_client.delete_schema(SchemaArn=arn)

    def test_list_development_schema_arns(self, clouddirectory_client, schema):
        """list_development_schema_arns includes the created schema."""
        resp = clouddirectory_client.list_development_schema_arns()
        assert "SchemaArns" in resp
        assert schema["SchemaArn"] in resp["SchemaArns"]

    def test_delete_schema(self, clouddirectory_client):
        """delete_schema removes a development schema."""
        name = f"test-{uuid.uuid4().hex[:8]}"
        create_resp = clouddirectory_client.create_schema(Name=name)
        arn = create_resp["SchemaArn"]

        delete_resp = clouddirectory_client.delete_schema(SchemaArn=arn)
        assert delete_resp["SchemaArn"] == arn

        # Verify it no longer appears in development schemas
        list_resp = clouddirectory_client.list_development_schema_arns()
        assert arn not in list_resp["SchemaArns"]

    def test_list_published_schema_arns(self, clouddirectory_client):
        """list_published_schema_arns returns a list (possibly empty)."""
        resp = clouddirectory_client.list_published_schema_arns()
        assert "SchemaArns" in resp
        assert isinstance(resp["SchemaArns"], list)
