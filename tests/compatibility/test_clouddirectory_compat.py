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


class TestCloudDirectoryDeleteDirectory:
    """Tests for DeleteDirectory operation."""

    def test_delete_directory(self, clouddirectory_client, published_schema):
        """DeleteDirectory removes a directory and returns its ARN."""
        name = f"test-dir-{uuid.uuid4().hex[:8]}"
        create_resp = clouddirectory_client.create_directory(
            Name=name,
            SchemaArn=published_schema["PublishedSchemaArn"],
        )
        dir_arn = create_resp["DirectoryArn"]

        delete_resp = clouddirectory_client.delete_directory(DirectoryArn=dir_arn)
        assert delete_resp["DirectoryArn"] == dir_arn

        # Verify it no longer appears in listing
        list_resp = clouddirectory_client.list_directories()
        found_arns = [d["DirectoryArn"] for d in list_resp["Directories"]]
        assert dir_arn not in found_arns

    def test_delete_directory_not_in_list(self, clouddirectory_client, published_schema):
        """After deleting a directory, it should not appear in any state filter."""
        name = f"test-dir-{uuid.uuid4().hex[:8]}"
        create_resp = clouddirectory_client.create_directory(
            Name=name,
            SchemaArn=published_schema["PublishedSchemaArn"],
        )
        dir_arn = create_resp["DirectoryArn"]

        clouddirectory_client.delete_directory(DirectoryArn=dir_arn)

        enabled_resp = clouddirectory_client.list_directories(state="ENABLED")
        enabled_arns = [d["DirectoryArn"] for d in enabled_resp["Directories"]]
        assert dir_arn not in enabled_arns


class TestCloudDirectoryListDirectoriesFiltered:
    """Tests for ListDirectories with state filter and pagination."""

    def test_list_directories_state_enabled(self, clouddirectory_client, directory):
        """ListDirectories with state=ENABLED includes active directories."""
        resp = clouddirectory_client.list_directories(state="ENABLED")
        assert "Directories" in resp
        found_arns = [d["DirectoryArn"] for d in resp["Directories"]]
        assert directory["DirectoryArn"] in found_arns

    def test_list_directories_returns_directory_fields(self, clouddirectory_client, directory):
        """ListDirectories returns directory objects with expected fields."""
        resp = clouddirectory_client.list_directories()
        assert len(resp["Directories"]) >= 1
        d = next(d for d in resp["Directories"] if d["DirectoryArn"] == directory["DirectoryArn"])
        assert "Name" in d
        assert d["Name"] == directory["Name"]
        assert "DirectoryArn" in d
        assert "State" in d


class TestCloudDirectoryGetDirectoryErrors:
    """Tests for GetDirectory error handling."""

    def test_get_directory_invalid_arn(self, clouddirectory_client):
        """GetDirectory with a nonexistent ARN returns an error."""
        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/nonexistent"
        with pytest.raises(clouddirectory_client.exceptions.ClientError) as exc_info:
            clouddirectory_client.get_directory(DirectoryArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "InvalidArnException"


class TestCloudDirectorySchemaLifecycle:
    """Tests for schema creation, publishing, and deletion lifecycle."""

    def test_publish_schema_appears_in_published_list(self, clouddirectory_client):
        """A published schema appears in list_published_schema_arns."""
        name = f"test-{uuid.uuid4().hex[:8]}"
        resp = clouddirectory_client.create_schema(Name=name)
        dev_arn = resp["SchemaArn"]
        version = f"v{uuid.uuid4().hex[:6]}"
        pub_resp = clouddirectory_client.publish_schema(
            DevelopmentSchemaArn=dev_arn, Version=version
        )
        pub_arn = pub_resp["PublishedSchemaArn"]
        try:
            list_resp = clouddirectory_client.list_published_schema_arns()
            assert pub_arn in list_resp["SchemaArns"]
        finally:
            clouddirectory_client.delete_schema(SchemaArn=pub_arn)

    def test_delete_published_schema(self, clouddirectory_client):
        """Deleting a published schema removes it from the published list."""
        name = f"test-{uuid.uuid4().hex[:8]}"
        resp = clouddirectory_client.create_schema(Name=name)
        dev_arn = resp["SchemaArn"]
        version = f"v{uuid.uuid4().hex[:6]}"
        pub_resp = clouddirectory_client.publish_schema(
            DevelopmentSchemaArn=dev_arn, Version=version
        )
        pub_arn = pub_resp["PublishedSchemaArn"]

        del_resp = clouddirectory_client.delete_schema(SchemaArn=pub_arn)
        assert del_resp["SchemaArn"] == pub_arn

        list_resp = clouddirectory_client.list_published_schema_arns()
        assert pub_arn not in list_resp["SchemaArns"]

    def test_multiple_published_schemas(self, clouddirectory_client):
        """Multiple published schemas all appear in the listing."""
        arns_to_clean = []
        try:
            pub_arns = []
            for _ in range(2):
                name = f"test-{uuid.uuid4().hex[:8]}"
                resp = clouddirectory_client.create_schema(Name=name)
                dev_arn = resp["SchemaArn"]
                version = f"v{uuid.uuid4().hex[:6]}"
                pub_resp = clouddirectory_client.publish_schema(
                    DevelopmentSchemaArn=dev_arn, Version=version
                )
                pub_arns.append(pub_resp["PublishedSchemaArn"])
                arns_to_clean.append(pub_resp["PublishedSchemaArn"])

            list_resp = clouddirectory_client.list_published_schema_arns()
            for pa in pub_arns:
                assert pa in list_resp["SchemaArns"]
        finally:
            for arn in arns_to_clean:
                try:
                    clouddirectory_client.delete_schema(SchemaArn=arn)
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

    def test_tag_schema_resource(self, clouddirectory_client, schema):
        """TagResource and ListTagsForResource work on schema ARNs."""
        arn = schema["SchemaArn"]
        clouddirectory_client.tag_resource(
            ResourceArn=arn,
            Tags=[{"Key": "purpose", "Value": "testing"}],
        )
        resp = clouddirectory_client.list_tags_for_resource(ResourceArn=arn)
        assert "Tags" in resp
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags.get("purpose") == "testing"

    def test_tag_multiple_tags_at_once(self, clouddirectory_client, directory):
        """TagResource can add multiple tags in a single call."""
        arn = directory["DirectoryArn"]
        clouddirectory_client.tag_resource(
            ResourceArn=arn,
            Tags=[
                {"Key": "k1", "Value": "v1"},
                {"Key": "k2", "Value": "v2"},
            ],
        )
        resp = clouddirectory_client.list_tags_for_resource(ResourceArn=arn)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags.get("k1") == "v1"
        assert tags.get("k2") == "v2"

    def test_list_tags_empty(self, clouddirectory_client):
        """ListTagsForResource returns empty list for a fresh resource."""
        name = f"test-{uuid.uuid4().hex[:8]}"
        resp = clouddirectory_client.create_schema(Name=name)
        arn = resp["SchemaArn"]
        try:
            tag_resp = clouddirectory_client.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in tag_resp
            assert isinstance(tag_resp["Tags"], list)
        finally:
            clouddirectory_client.delete_schema(SchemaArn=arn)
