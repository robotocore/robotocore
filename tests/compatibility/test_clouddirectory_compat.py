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


class TestCloudDirectorySchemaQueries:
    """Tests for schema query operations."""

    def test_list_managed_schema_arns(self, clouddirectory_client):
        """ListManagedSchemaArns returns a list."""
        resp = clouddirectory_client.list_managed_schema_arns()
        assert "SchemaArns" in resp
        assert isinstance(resp["SchemaArns"], list)

    def test_get_schema_as_json(self, clouddirectory_client, published_schema):
        """GetSchemaAsJson returns schema document for a published schema."""
        resp = clouddirectory_client.get_schema_as_json(
            SchemaArn=published_schema["PublishedSchemaArn"],
        )
        assert "Name" in resp
        assert "Document" in resp

    def test_list_applied_schema_arns(self, clouddirectory_client, directory):
        """ListAppliedSchemaArns returns a list for a directory."""
        resp = clouddirectory_client.list_applied_schema_arns(
            DirectoryArn=directory["DirectoryArn"],
        )
        assert "SchemaArns" in resp
        assert isinstance(resp["SchemaArns"], list)

    def test_list_facet_names(self, clouddirectory_client, published_schema):
        """ListFacetNames returns a list for a published schema."""
        resp = clouddirectory_client.list_facet_names(
            SchemaArn=published_schema["PublishedSchemaArn"],
        )
        assert "FacetNames" in resp
        assert isinstance(resp["FacetNames"], list)

    def test_list_typed_link_facet_names(self, clouddirectory_client, published_schema):
        """ListTypedLinkFacetNames returns a list for a published schema."""
        resp = clouddirectory_client.list_typed_link_facet_names(
            SchemaArn=published_schema["PublishedSchemaArn"],
        )
        assert "FacetNames" in resp
        assert isinstance(resp["FacetNames"], list)


class TestCloudDirectoryObjectQueries:
    """Tests for object query operations against a directory."""

    def test_get_object_information(self, clouddirectory_client, directory):
        """GetObjectInformation returns info for root object."""
        resp = clouddirectory_client.get_object_information(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
            ConsistencyLevel="EVENTUAL",
        )
        assert "ObjectIdentifier" in resp

    def test_list_object_children(self, clouddirectory_client, directory):
        """ListObjectChildren returns children of root object."""
        resp = clouddirectory_client.list_object_children(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
            ConsistencyLevel="EVENTUAL",
        )
        assert "Children" in resp
        assert isinstance(resp["Children"], dict)

    def test_list_object_parents(self, clouddirectory_client, directory):
        """ListObjectParents returns parents for root object."""
        resp = clouddirectory_client.list_object_parents(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
            ConsistencyLevel="EVENTUAL",
        )
        assert "Parents" in resp
        assert isinstance(resp["Parents"], dict)

    def test_list_object_parent_paths(self, clouddirectory_client, directory):
        """ListObjectParentPaths returns paths for root object."""
        resp = clouddirectory_client.list_object_parent_paths(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
        )
        assert "PathToObjectIdentifiersList" in resp
        assert isinstance(resp["PathToObjectIdentifiersList"], list)

    def test_list_object_attributes(self, clouddirectory_client, directory):
        """ListObjectAttributes returns attributes for root object."""
        resp = clouddirectory_client.list_object_attributes(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
            ConsistencyLevel="EVENTUAL",
        )
        assert "Attributes" in resp
        assert isinstance(resp["Attributes"], list)

    def test_list_object_policies(self, clouddirectory_client, directory):
        """ListObjectPolicies returns policies for root object."""
        resp = clouddirectory_client.list_object_policies(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
            ConsistencyLevel="EVENTUAL",
        )
        assert "AttachedPolicyIds" in resp
        assert isinstance(resp["AttachedPolicyIds"], list)

    def test_list_attached_indices(self, clouddirectory_client, directory):
        """ListAttachedIndices returns indices for root object."""
        resp = clouddirectory_client.list_attached_indices(
            DirectoryArn=directory["DirectoryArn"],
            TargetReference={"Selector": "/"},
            ConsistencyLevel="EVENTUAL",
        )
        assert "IndexAttachments" in resp
        assert isinstance(resp["IndexAttachments"], list)

    def test_list_incoming_typed_links(self, clouddirectory_client, directory):
        """ListIncomingTypedLinks returns typed links for root object."""
        resp = clouddirectory_client.list_incoming_typed_links(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
        )
        assert "LinkSpecifiers" in resp
        assert isinstance(resp["LinkSpecifiers"], list)

    def test_list_outgoing_typed_links(self, clouddirectory_client, directory):
        """ListOutgoingTypedLinks returns typed links for root object."""
        resp = clouddirectory_client.list_outgoing_typed_links(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
        )
        assert "TypedLinkSpecifiers" in resp
        assert isinstance(resp["TypedLinkSpecifiers"], list)

    def test_list_policy_attachments(self, clouddirectory_client, directory):
        """ListPolicyAttachments returns object IDs for root object."""
        resp = clouddirectory_client.list_policy_attachments(
            DirectoryArn=directory["DirectoryArn"],
            PolicyReference={"Selector": "/"},
            ConsistencyLevel="EVENTUAL",
        )
        assert "ObjectIdentifiers" in resp
        assert isinstance(resp["ObjectIdentifiers"], list)

    def test_get_link_attributes(self, clouddirectory_client, directory):
        """GetLinkAttributes returns attributes for a typed link specifier."""
        applied_arn = directory["AppliedSchemaArn"]
        resp = clouddirectory_client.get_link_attributes(
            DirectoryArn=directory["DirectoryArn"],
            TypedLinkSpecifier={
                "TypedLinkFacet": {
                    "SchemaArn": applied_arn,
                    "TypedLinkName": "fake",
                },
                "SourceObjectReference": {"Selector": "/"},
                "TargetObjectReference": {"Selector": "/"},
                "IdentityAttributeValues": [],
            },
            AttributeNames=["attr1"],
        )
        assert "Attributes" in resp

    def test_get_object_attributes(self, clouddirectory_client, directory):
        """GetObjectAttributes returns attributes for an object facet."""
        applied_arn = directory["AppliedSchemaArn"]
        resp = clouddirectory_client.get_object_attributes(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
            SchemaFacet={
                "SchemaArn": applied_arn,
                "FacetName": "fake",
            },
            AttributeNames=["attr1"],
        )
        assert "Attributes" in resp


class TestCloudDirectoryFacetOperations:
    """Tests for Facet CRUD operations."""

    def test_create_and_get_facet(self, clouddirectory_client, schema):
        """CreateFacet + GetFacet round-trip."""
        facet_name = f"Facet{uuid.uuid4().hex[:8]}"
        clouddirectory_client.create_facet(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
            ObjectType="NODE",
        )
        resp = clouddirectory_client.get_facet(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
        )
        assert "Facet" in resp
        assert resp["Facet"]["Name"] == facet_name

    def test_delete_facet(self, clouddirectory_client, schema):
        """DeleteFacet removes a facet."""
        facet_name = f"Facet{uuid.uuid4().hex[:8]}"
        clouddirectory_client.create_facet(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
            ObjectType="NODE",
        )
        clouddirectory_client.delete_facet(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
        )
        resp = clouddirectory_client.list_facet_names(SchemaArn=schema["SchemaArn"])
        assert facet_name not in resp["FacetNames"]

    def test_update_facet(self, clouddirectory_client, schema):
        """UpdateFacet modifies a facet."""
        facet_name = f"Facet{uuid.uuid4().hex[:8]}"
        clouddirectory_client.create_facet(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
            ObjectType="NODE",
            Attributes=[
                {
                    "Name": "attr1",
                    "AttributeDefinition": {
                        "Type": "STRING",
                    },
                    "RequiredBehavior": "NOT_REQUIRED",
                },
            ],
        )
        resp = clouddirectory_client.update_facet(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_facet_attributes(self, clouddirectory_client, schema):
        """ListFacetAttributes returns attributes for a facet."""
        facet_name = f"Facet{uuid.uuid4().hex[:8]}"
        clouddirectory_client.create_facet(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
            ObjectType="NODE",
            Attributes=[
                {
                    "Name": "myattr",
                    "AttributeDefinition": {"Type": "STRING"},
                    "RequiredBehavior": "NOT_REQUIRED",
                },
            ],
        )
        resp = clouddirectory_client.list_facet_attributes(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
        )
        assert "Attributes" in resp
        assert isinstance(resp["Attributes"], list)
        attr_names = [a["Name"] for a in resp["Attributes"]]
        assert "myattr" in attr_names


class TestCloudDirectoryDirectoryEnableDisable:
    """Tests for EnableDirectory/DisableDirectory."""

    def test_disable_and_enable_directory(self, clouddirectory_client, published_schema):
        """DisableDirectory + EnableDirectory toggle directory state."""
        name = f"test-dir-{uuid.uuid4().hex[:8]}"
        create_resp = clouddirectory_client.create_directory(
            Name=name,
            SchemaArn=published_schema["PublishedSchemaArn"],
        )
        dir_arn = create_resp["DirectoryArn"]
        try:
            # Disable
            dis_resp = clouddirectory_client.disable_directory(DirectoryArn=dir_arn)
            assert dis_resp["DirectoryArn"] == dir_arn

            # Enable
            en_resp = clouddirectory_client.enable_directory(DirectoryArn=dir_arn)
            assert en_resp["DirectoryArn"] == dir_arn
        finally:
            try:
                clouddirectory_client.disable_directory(DirectoryArn=dir_arn)
            except Exception:
                pass
            try:
                clouddirectory_client.delete_directory(DirectoryArn=dir_arn)
            except Exception:
                pass


class TestCloudDirectoryObjectCrud:
    """Tests for object CRUD operations."""

    def test_create_object(self, clouddirectory_client, directory):
        """CreateObject creates an object in a directory with empty facets."""
        resp = clouddirectory_client.create_object(
            DirectoryArn=directory["DirectoryArn"],
            SchemaFacets=[],
        )
        assert "ObjectIdentifier" in resp

    def test_lookup_policy(self, clouddirectory_client, directory):
        """LookupPolicy returns PolicyToPathList for root."""
        resp = clouddirectory_client.lookup_policy(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": "/"},
        )
        assert "PolicyToPathList" in resp
        assert isinstance(resp["PolicyToPathList"], list)


class TestCloudDirectorySchemaJson:
    """Tests for PutSchemaFromJson and UpdateSchema."""

    def test_put_schema_from_json(self, clouddirectory_client, schema):
        """PutSchemaFromJson sets schema from a JSON document."""
        json_doc = '{"facets":{},"typedLinkFacets":{}}'
        resp = clouddirectory_client.put_schema_from_json(
            SchemaArn=schema["SchemaArn"],
            Document=json_doc,
        )
        assert "Arn" in resp

    def test_update_schema(self, clouddirectory_client, schema):
        """UpdateSchema changes the schema name."""
        new_name = f"updated-{uuid.uuid4().hex[:8]}"
        resp = clouddirectory_client.update_schema(
            SchemaArn=schema["SchemaArn"],
            Name=new_name,
        )
        assert "SchemaArn" in resp

    def test_get_applied_schema_version_not_found(self, clouddirectory_client):
        """GetAppliedSchemaVersion for nonexistent raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:schema/published/fake/1"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.get_applied_schema_version(SchemaArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestCloudDirectoryTypedLinkFacetOps:
    """Tests for TypedLinkFacet operations."""

    def test_create_and_get_typed_link_facet(self, clouddirectory_client, schema):
        """CreateTypedLinkFacet + GetTypedLinkFacetInformation round-trip."""
        facet_name = f"TLFacet{uuid.uuid4().hex[:6]}"
        clouddirectory_client.create_typed_link_facet(
            SchemaArn=schema["SchemaArn"],
            Facet={
                "Name": facet_name,
                "Attributes": [
                    {
                        "Name": "linkattr",
                        "Type": "STRING",
                        "RequiredBehavior": "REQUIRED_ALWAYS",
                    },
                ],
                "IdentityAttributeOrder": ["linkattr"],
            },
        )
        resp = clouddirectory_client.get_typed_link_facet_information(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
        )
        assert "IdentityAttributeOrder" in resp
        assert "linkattr" in resp["IdentityAttributeOrder"]

    def test_delete_typed_link_facet(self, clouddirectory_client, schema):
        """DeleteTypedLinkFacet removes a typed link facet."""
        facet_name = f"TLFacet{uuid.uuid4().hex[:6]}"
        clouddirectory_client.create_typed_link_facet(
            SchemaArn=schema["SchemaArn"],
            Facet={
                "Name": facet_name,
                "Attributes": [
                    {
                        "Name": "linkattr",
                        "Type": "STRING",
                        "RequiredBehavior": "REQUIRED_ALWAYS",
                    },
                ],
                "IdentityAttributeOrder": ["linkattr"],
            },
        )
        clouddirectory_client.delete_typed_link_facet(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
        )
        resp = clouddirectory_client.list_typed_link_facet_names(
            SchemaArn=schema["SchemaArn"],
        )
        assert facet_name not in resp["FacetNames"]

    def test_list_typed_link_facet_attributes(self, clouddirectory_client, schema):
        """ListTypedLinkFacetAttributes returns attributes."""
        facet_name = f"TLFacet{uuid.uuid4().hex[:6]}"
        clouddirectory_client.create_typed_link_facet(
            SchemaArn=schema["SchemaArn"],
            Facet={
                "Name": facet_name,
                "Attributes": [
                    {
                        "Name": "mylink",
                        "Type": "STRING",
                        "RequiredBehavior": "REQUIRED_ALWAYS",
                    },
                ],
                "IdentityAttributeOrder": ["mylink"],
            },
        )
        resp = clouddirectory_client.list_typed_link_facet_attributes(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
        )
        assert "Attributes" in resp
        assert isinstance(resp["Attributes"], list)


class TestCloudDirectoryBatchOps:
    """Tests for BatchRead and BatchWrite operations."""

    def test_batch_read(self, clouddirectory_client, directory):
        """BatchRead with ListObjectChildren returns Responses."""
        resp = clouddirectory_client.batch_read(
            DirectoryArn=directory["DirectoryArn"],
            Operations=[
                {
                    "ListObjectChildren": {
                        "ObjectReference": {"Selector": "/"},
                    },
                },
            ],
        )
        assert "Responses" in resp
        assert len(resp["Responses"]) == 1

    def test_batch_write(self, clouddirectory_client, directory):
        """BatchWrite with empty operations returns Responses."""
        resp = clouddirectory_client.batch_write(
            DirectoryArn=directory["DirectoryArn"],
            Operations=[],
        )
        assert "Responses" in resp
        assert isinstance(resp["Responses"], list)


class TestCloudDirectoryObjectMutations:
    """Tests for object attach/detach/delete and facet mutations."""

    def test_delete_object(self, clouddirectory_client, directory):
        """DeleteObject removes an unattached object."""
        create_resp = clouddirectory_client.create_object(
            DirectoryArn=directory["DirectoryArn"],
            SchemaFacets=[],
        )
        obj_id = create_resp["ObjectIdentifier"]
        del_resp = clouddirectory_client.delete_object(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": f"${obj_id}"},
        )
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_attach_object(self, clouddirectory_client, directory):
        """AttachObject attaches a child to parent and returns AttachedObjectIdentifier."""
        child_resp = clouddirectory_client.create_object(
            DirectoryArn=directory["DirectoryArn"],
            SchemaFacets=[],
        )
        child_id = child_resp["ObjectIdentifier"]
        link_name = f"link-{uuid.uuid4().hex[:6]}"
        attach_resp = clouddirectory_client.attach_object(
            DirectoryArn=directory["DirectoryArn"],
            ParentReference={"Selector": "/"},
            ChildReference={"Selector": f"${child_id}"},
            LinkName=link_name,
        )
        assert "AttachedObjectIdentifier" in attach_resp
        # cleanup: detach then delete
        clouddirectory_client.detach_object(
            DirectoryArn=directory["DirectoryArn"],
            ParentReference={"Selector": "/"},
            LinkName=link_name,
        )
        clouddirectory_client.delete_object(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": f"${child_id}"},
        )

    def test_detach_object(self, clouddirectory_client, directory):
        """DetachObject detaches a child and returns DetachedObjectIdentifier."""
        child_resp = clouddirectory_client.create_object(
            DirectoryArn=directory["DirectoryArn"],
            SchemaFacets=[],
        )
        child_id = child_resp["ObjectIdentifier"]
        link_name = f"link-{uuid.uuid4().hex[:6]}"
        clouddirectory_client.attach_object(
            DirectoryArn=directory["DirectoryArn"],
            ParentReference={"Selector": "/"},
            ChildReference={"Selector": f"${child_id}"},
            LinkName=link_name,
        )
        detach_resp = clouddirectory_client.detach_object(
            DirectoryArn=directory["DirectoryArn"],
            ParentReference={"Selector": "/"},
            LinkName=link_name,
        )
        assert "DetachedObjectIdentifier" in detach_resp
        # cleanup
        clouddirectory_client.delete_object(
            DirectoryArn=directory["DirectoryArn"],
            ObjectReference={"Selector": f"${child_id}"},
        )

    def test_attach_policy_not_found(self, clouddirectory_client):
        """AttachPolicy with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.attach_policy(
                DirectoryArn=fake_arn,
                PolicyReference={"Selector": "/"},
                ObjectReference={"Selector": "/"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_detach_policy_not_found(self, clouddirectory_client):
        """DetachPolicy with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.detach_policy(
                DirectoryArn=fake_arn,
                PolicyReference={"Selector": "/"},
                ObjectReference={"Selector": "/"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_add_facet_to_object_not_found(self, clouddirectory_client):
        """AddFacetToObject with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.add_facet_to_object(
                DirectoryArn=fake_arn,
                SchemaFacet={"SchemaArn": fake_arn, "FacetName": "fake"},
                ObjectReference={"Selector": "/"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_remove_facet_from_object_not_found(self, clouddirectory_client):
        """RemoveFacetFromObject with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.remove_facet_from_object(
                DirectoryArn=fake_arn,
                SchemaFacet={"SchemaArn": fake_arn, "FacetName": "fake"},
                ObjectReference={"Selector": "/"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_object_attributes_not_found(self, clouddirectory_client):
        """UpdateObjectAttributes with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.update_object_attributes(
                DirectoryArn=fake_arn,
                ObjectReference={"Selector": "/"},
                AttributeUpdates=[],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestCloudDirectoryIndexOps:
    """Tests for index operations."""

    def test_create_index(self, clouddirectory_client, directory):
        """CreateIndex creates an index and returns ObjectIdentifier."""
        applied_arn = directory["AppliedSchemaArn"]
        resp = clouddirectory_client.create_index(
            DirectoryArn=directory["DirectoryArn"],
            OrderedIndexedAttributeList=[
                {
                    "SchemaArn": applied_arn,
                    "FacetName": "fake",
                    "Name": "attr1",
                },
            ],
            IsUnique=False,
        )
        assert "ObjectIdentifier" in resp

    def test_attach_to_index_not_found(self, clouddirectory_client):
        """AttachToIndex with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.attach_to_index(
                DirectoryArn=fake_arn,
                IndexReference={"Selector": "/"},
                TargetReference={"Selector": "/"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_detach_from_index_not_found(self, clouddirectory_client):
        """DetachFromIndex with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.detach_from_index(
                DirectoryArn=fake_arn,
                IndexReference={"Selector": "/"},
                TargetReference={"Selector": "/"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_index_not_found(self, clouddirectory_client):
        """ListIndex with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.list_index(
                DirectoryArn=fake_arn,
                IndexReference={"Selector": "/"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestCloudDirectoryTypedLinkMutations:
    """Tests for typed link attach/detach and update operations."""

    def test_attach_typed_link_not_found(self, clouddirectory_client):
        """AttachTypedLink with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.attach_typed_link(
                DirectoryArn=fake_arn,
                SourceObjectReference={"Selector": "/"},
                TargetObjectReference={"Selector": "/"},
                TypedLinkFacet={
                    "SchemaArn": fake_arn,
                    "TypedLinkName": "fake",
                },
                Attributes=[],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_detach_typed_link_not_found(self, clouddirectory_client):
        """DetachTypedLink with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.detach_typed_link(
                DirectoryArn=fake_arn,
                TypedLinkSpecifier={
                    "TypedLinkFacet": {
                        "SchemaArn": fake_arn,
                        "TypedLinkName": "fake",
                    },
                    "SourceObjectReference": {"Selector": "/"},
                    "TargetObjectReference": {"Selector": "/"},
                    "IdentityAttributeValues": [],
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_link_attributes_not_found(self, clouddirectory_client):
        """UpdateLinkAttributes with fake directory ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.update_link_attributes(
                DirectoryArn=fake_arn,
                TypedLinkSpecifier={
                    "TypedLinkFacet": {
                        "SchemaArn": fake_arn,
                        "TypedLinkName": "fake",
                    },
                    "SourceObjectReference": {"Selector": "/"},
                    "TargetObjectReference": {"Selector": "/"},
                    "IdentityAttributeValues": [],
                },
                AttributeUpdates=[],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_typed_link_facet(self, clouddirectory_client, schema):
        """UpdateTypedLinkFacet modifies a typed link facet."""
        facet_name = f"TLFacet{uuid.uuid4().hex[:6]}"
        clouddirectory_client.create_typed_link_facet(
            SchemaArn=schema["SchemaArn"],
            Facet={
                "Name": facet_name,
                "Attributes": [
                    {
                        "Name": "linkattr",
                        "Type": "STRING",
                        "RequiredBehavior": "REQUIRED_ALWAYS",
                    },
                ],
                "IdentityAttributeOrder": ["linkattr"],
            },
        )
        resp = clouddirectory_client.update_typed_link_facet(
            SchemaArn=schema["SchemaArn"],
            Name=facet_name,
            AttributeUpdates=[],
            IdentityAttributeOrder=["linkattr"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCloudDirectorySchemaUpgrade:
    """Tests for UpgradeAppliedSchema and UpgradePublishedSchema."""

    def test_upgrade_applied_schema_not_found(self, clouddirectory_client):
        """UpgradeAppliedSchema with fake ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_arn = "arn:aws:clouddirectory:us-east-1:123456789012:schema/published/fake/1"
        fake_dir = "arn:aws:clouddirectory:us-east-1:123456789012:directory/fakedir123"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.upgrade_applied_schema(
                PublishedSchemaArn=fake_arn,
                DirectoryArn=fake_dir,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_upgrade_published_schema_not_found(self, clouddirectory_client):
        """UpgradePublishedSchema with fake ARN raises ResourceNotFoundException."""
        from botocore.exceptions import ClientError

        fake_dev = "arn:aws:clouddirectory:us-east-1:123456789012:schema/development/fake"
        fake_pub = "arn:aws:clouddirectory:us-east-1:123456789012:schema/published/fake/1"
        with pytest.raises(ClientError) as exc:
            clouddirectory_client.upgrade_published_schema(
                DevelopmentSchemaArn=fake_dev,
                PublishedSchemaArn=fake_pub,
                MinorVersion="1",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
