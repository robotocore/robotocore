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


@pytest.fixture
def published_schema(clouddirectory_client, schema):
    """Publish a schema and clean up after the test."""
    version = f"v{uuid.uuid4().hex[:6]}"
    resp = clouddirectory_client.publish_schema(
        DevelopmentSchemaArn=schema["SchemaArn"],
        Version=version,
    )
    arn = resp["PublishedSchemaArn"]
    yield {"PublishedSchemaArn": arn, "Version": version}
    try:
        clouddirectory_client.delete_schema(SchemaArn=arn)
    except Exception:
        pass


@pytest.fixture
def directory(clouddirectory_client, published_schema):
    """Create a directory with a published schema, clean up after."""
    name = f"test-dir-{uuid.uuid4().hex[:8]}"
    resp = clouddirectory_client.create_directory(
        Name=name,
        SchemaArn=published_schema["PublishedSchemaArn"],
    )
    directory_arn = resp["DirectoryArn"]
    yield {
        "Name": name,
        "DirectoryArn": directory_arn,
        "AppliedSchemaArn": resp.get("AppliedSchemaArn"),
    }
    try:
        clouddirectory_client.disable_directory(DirectoryArn=directory_arn)
    except Exception:
        pass
    try:
        clouddirectory_client.delete_directory(DirectoryArn=directory_arn)
    except Exception:
        pass


class TestClouddirectoryAutoCoverage:
    """Auto-generated coverage tests for clouddirectory."""

    @pytest.fixture
    def client(self):
        return make_client("clouddirectory")

    def test_list_directories(self, client):
        """ListDirectories returns a response."""
        resp = client.list_directories()
        assert "Directories" in resp


class TestCloudDirectoryDirectoryOps:
    """Tests for Directory operations in Cloud Directory."""

    def test_create_directory(self, clouddirectory_client, published_schema):
        """CreateDirectory creates a directory with a published schema."""
        name = f"test-dir-{uuid.uuid4().hex[:8]}"
        resp = clouddirectory_client.create_directory(
            Name=name,
            SchemaArn=published_schema["PublishedSchemaArn"],
        )
        assert "DirectoryArn" in resp
        assert "Name" in resp
        assert resp["Name"] == name
        assert "AppliedSchemaArn" in resp
        # cleanup
        try:
            clouddirectory_client.disable_directory(DirectoryArn=resp["DirectoryArn"])
        except Exception:
            pass
        clouddirectory_client.delete_directory(DirectoryArn=resp["DirectoryArn"])

    def test_get_directory(self, clouddirectory_client, directory):
        """GetDirectory returns details for an existing directory."""
        resp = clouddirectory_client.get_directory(
            DirectoryArn=directory["DirectoryArn"],
        )
        assert "Directory" in resp
        d = resp["Directory"]
        assert d["Name"] == directory["Name"]
        assert d["DirectoryArn"] == directory["DirectoryArn"]
        assert "State" in d


class TestCloudDirectorySchemaOps:
    """Tests for Schema publish/apply operations."""

    def test_publish_schema(self, clouddirectory_client, schema):
        """PublishSchema publishes a development schema."""
        version = f"v{uuid.uuid4().hex[:6]}"
        resp = clouddirectory_client.publish_schema(
            DevelopmentSchemaArn=schema["SchemaArn"],
            Version=version,
        )
        assert "PublishedSchemaArn" in resp
        assert ":schema/published/" in resp["PublishedSchemaArn"]
        # cleanup
        clouddirectory_client.delete_schema(SchemaArn=resp["PublishedSchemaArn"])

    def test_apply_schema(self, clouddirectory_client, directory, published_schema):
        """ApplySchema applies a published schema to a directory."""
        # The directory already has one applied schema from creation.
        # Applying the same one again should work or we can try a new one.
        # Let's create a second schema, publish it, and apply it.
        name2 = f"test-{uuid.uuid4().hex[:8]}"
        resp2 = clouddirectory_client.create_schema(Name=name2)
        arn2 = resp2["SchemaArn"]
        version2 = f"v{uuid.uuid4().hex[:6]}"
        pub_resp = clouddirectory_client.publish_schema(
            DevelopmentSchemaArn=arn2,
            Version=version2,
        )
        pub_arn2 = pub_resp["PublishedSchemaArn"]
        try:
            resp = clouddirectory_client.apply_schema(
                PublishedSchemaArn=pub_arn2,
                DirectoryArn=directory["DirectoryArn"],
            )
            assert "AppliedSchemaArn" in resp
            assert "DirectoryArn" in resp
        finally:
            try:
                clouddirectory_client.delete_schema(SchemaArn=pub_arn2)
            except Exception:
                pass
            try:
                clouddirectory_client.delete_schema(SchemaArn=arn2)
            except Exception:
                pass


class TestCloudDirectoryTagOps:
    """Tests for tag operations on Cloud Directory resources."""

    def test_tag_and_list_tags(self, clouddirectory_client, directory):
        """TagResource and ListTagsForResource work together."""
        arn = directory["DirectoryArn"]
        clouddirectory_client.tag_resource(
            ResourceArn=arn,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        resp = clouddirectory_client.list_tags_for_resource(ResourceArn=arn)
        assert "Tags" in resp
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags.get("env") == "test"

    def test_untag_resource(self, clouddirectory_client, directory):
        """UntagResource removes a tag from a directory."""
        arn = directory["DirectoryArn"]
        clouddirectory_client.tag_resource(
            ResourceArn=arn,
            Tags=[{"Key": "remove-me", "Value": "yes"}],
        )
        clouddirectory_client.untag_resource(
            ResourceArn=arn,
            TagKeys=["remove-me"],
        )
        resp = clouddirectory_client.list_tags_for_resource(ResourceArn=arn)
        tag_keys = [t["Key"] for t in resp["Tags"]]
        assert "remove-me" not in tag_keys
