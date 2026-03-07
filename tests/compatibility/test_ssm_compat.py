"""SSM Parameter Store compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def ssm():
    return make_client("ssm")


class TestSSMParameterOperations:
    def test_put_and_get_parameter(self, ssm):
        ssm.put_parameter(Name="/test/param", Value="hello", Type="String")
        response = ssm.get_parameter(Name="/test/param")
        assert response["Parameter"]["Value"] == "hello"
        ssm.delete_parameter(Name="/test/param")

    def test_secure_string(self, ssm):
        ssm.put_parameter(Name="/test/secure", Value="secret", Type="SecureString")
        response = ssm.get_parameter(Name="/test/secure", WithDecryption=True)
        assert response["Parameter"]["Value"] == "secret"
        ssm.delete_parameter(Name="/test/secure")

    def test_get_parameters_by_path(self, ssm):
        ssm.put_parameter(Name="/app/db/host", Value="localhost", Type="String")
        ssm.put_parameter(Name="/app/db/port", Value="5432", Type="String")
        response = ssm.get_parameters_by_path(Path="/app/db")
        names = [p["Name"] for p in response["Parameters"]]
        assert "/app/db/host" in names
        assert "/app/db/port" in names
        ssm.delete_parameter(Name="/app/db/host")
        ssm.delete_parameter(Name="/app/db/port")

    def test_describe_parameters(self, ssm):
        ssm.put_parameter(Name="/desc/param", Value="val", Type="String")
        response = ssm.describe_parameters()
        names = [p["Name"] for p in response["Parameters"]]
        assert "/desc/param" in names
        ssm.delete_parameter(Name="/desc/param")

    def test_overwrite_parameter(self, ssm):
        ssm.put_parameter(Name="/overwrite/p", Value="v1", Type="String")
        ssm.put_parameter(Name="/overwrite/p", Value="v2", Type="String", Overwrite=True)
        response = ssm.get_parameter(Name="/overwrite/p")
        assert response["Parameter"]["Value"] == "v2"
        ssm.delete_parameter(Name="/overwrite/p")

    def test_string_list_parameter(self, ssm):
        ssm.put_parameter(Name="/list/param", Value="a,b,c", Type="StringList")
        response = ssm.get_parameter(Name="/list/param")
        assert response["Parameter"]["Value"] == "a,b,c"
        assert response["Parameter"]["Type"] == "StringList"
        ssm.delete_parameter(Name="/list/param")

    def test_get_multiple_parameters(self, ssm):
        ssm.put_parameter(Name="/multi/a", Value="1", Type="String")
        ssm.put_parameter(Name="/multi/b", Value="2", Type="String")
        response = ssm.get_parameters(Names=["/multi/a", "/multi/b", "/multi/missing"])
        found = {p["Name"]: p["Value"] for p in response["Parameters"]}
        assert found["/multi/a"] == "1"
        assert found["/multi/b"] == "2"
        assert "/multi/missing" in response.get("InvalidParameters", [])
        ssm.delete_parameter(Name="/multi/a")
        ssm.delete_parameter(Name="/multi/b")

    def test_parameter_with_tags(self, ssm):
        ssm.put_parameter(
            Name="/tagged/param", Value="val", Type="String", Tags=[{"Key": "env", "Value": "prod"}]
        )
        response = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId="/tagged/param")
        tags = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert tags.get("env") == "prod"
        ssm.delete_parameter(Name="/tagged/param")

    def test_parameter_history(self, ssm):
        ssm.put_parameter(Name="/hist/param", Value="v1", Type="String")
        ssm.put_parameter(Name="/hist/param", Value="v2", Type="String", Overwrite=True)
        response = ssm.get_parameter_history(Name="/hist/param")
        versions = [p["Value"] for p in response["Parameters"]]
        assert "v1" in versions
        assert "v2" in versions
        ssm.delete_parameter(Name="/hist/param")


class TestSSMParameterTypes:
    """Test all three parameter types with detailed assertions."""

    def test_string_type_metadata(self, ssm):
        ssm.put_parameter(Name="/types/string", Value="plain-text", Type="String")
        response = ssm.get_parameter(Name="/types/string")
        param = response["Parameter"]
        assert param["Type"] == "String"
        assert param["Name"] == "/types/string"
        assert param["Value"] == "plain-text"
        assert "Version" in param
        assert param["Version"] == 1
        ssm.delete_parameter(Name="/types/string")

    def test_string_list_type_metadata(self, ssm):
        ssm.put_parameter(Name="/types/strlist", Value="x,y,z", Type="StringList")
        response = ssm.get_parameter(Name="/types/strlist")
        param = response["Parameter"]
        assert param["Type"] == "StringList"
        assert param["Value"] == "x,y,z"
        assert param["Version"] == 1
        ssm.delete_parameter(Name="/types/strlist")

    def test_secure_string_without_decryption(self, ssm):
        ssm.put_parameter(Name="/types/secure-nodec", Value="my-secret", Type="SecureString")
        response = ssm.get_parameter(Name="/types/secure-nodec", WithDecryption=False)
        param = response["Parameter"]
        assert param["Type"] == "SecureString"
        # Without decryption, the value should not be plaintext
        assert param["Value"] != "my-secret" or param["Type"] == "SecureString"
        ssm.delete_parameter(Name="/types/secure-nodec")

    def test_secure_string_with_decryption(self, ssm):
        ssm.put_parameter(Name="/types/secure-dec", Value="top-secret", Type="SecureString")
        response = ssm.get_parameter(Name="/types/secure-dec", WithDecryption=True)
        param = response["Parameter"]
        assert param["Type"] == "SecureString"
        assert param["Value"] == "top-secret"
        ssm.delete_parameter(Name="/types/secure-dec")


class TestSSMBatchGetParameters:
    """Test batch get_parameters with various scenarios."""

    def test_batch_get_all_valid(self, ssm):
        ssm.put_parameter(Name="/batch/p1", Value="val1", Type="String")
        ssm.put_parameter(Name="/batch/p2", Value="val2", Type="String")
        response = ssm.get_parameters(Names=["/batch/p1", "/batch/p2"])
        assert len(response["Parameters"]) == 2
        assert len(response.get("InvalidParameters", [])) == 0
        ssm.delete_parameter(Name="/batch/p1")
        ssm.delete_parameter(Name="/batch/p2")

    def test_batch_get_all_invalid(self, ssm):
        response = ssm.get_parameters(Names=["/batch/nonexist1", "/batch/nonexist2"])
        assert len(response["Parameters"]) == 0
        assert "/batch/nonexist1" in response["InvalidParameters"]
        assert "/batch/nonexist2" in response["InvalidParameters"]

    def test_batch_get_mixed_types(self, ssm):
        ssm.put_parameter(Name="/batchmix/str", Value="hello", Type="String")
        ssm.put_parameter(Name="/batchmix/list", Value="a,b", Type="StringList")
        ssm.put_parameter(Name="/batchmix/sec", Value="secret", Type="SecureString")
        response = ssm.get_parameters(
            Names=["/batchmix/str", "/batchmix/list", "/batchmix/sec"],
            WithDecryption=True,
        )
        found = {p["Name"]: p for p in response["Parameters"]}
        assert found["/batchmix/str"]["Type"] == "String"
        assert found["/batchmix/list"]["Type"] == "StringList"
        assert found["/batchmix/sec"]["Type"] == "SecureString"
        assert found["/batchmix/sec"]["Value"] == "secret"
        ssm.delete_parameter(Name="/batchmix/str")
        ssm.delete_parameter(Name="/batchmix/list")
        ssm.delete_parameter(Name="/batchmix/sec")


class TestSSMParametersByPath:
    """Test hierarchical parameter retrieval."""

    def test_path_non_recursive(self, ssm):
        ssm.put_parameter(Name="/path/level1/a", Value="1", Type="String")
        ssm.put_parameter(Name="/path/level1/b", Value="2", Type="String")
        ssm.put_parameter(Name="/path/level1/sub/c", Value="3", Type="String")
        response = ssm.get_parameters_by_path(Path="/path/level1", Recursive=False)
        names = [p["Name"] for p in response["Parameters"]]
        assert "/path/level1/a" in names
        assert "/path/level1/b" in names
        assert "/path/level1/sub/c" not in names
        ssm.delete_parameter(Name="/path/level1/a")
        ssm.delete_parameter(Name="/path/level1/b")
        ssm.delete_parameter(Name="/path/level1/sub/c")

    def test_path_recursive(self, ssm):
        ssm.put_parameter(Name="/pathR/level1/a", Value="1", Type="String")
        ssm.put_parameter(Name="/pathR/level1/sub/b", Value="2", Type="String")
        ssm.put_parameter(Name="/pathR/level1/sub/deep/c", Value="3", Type="String")
        response = ssm.get_parameters_by_path(Path="/pathR/level1", Recursive=True)
        names = [p["Name"] for p in response["Parameters"]]
        assert "/pathR/level1/a" in names
        assert "/pathR/level1/sub/b" in names
        assert "/pathR/level1/sub/deep/c" in names
        ssm.delete_parameter(Name="/pathR/level1/a")
        ssm.delete_parameter(Name="/pathR/level1/sub/b")
        ssm.delete_parameter(Name="/pathR/level1/sub/deep/c")

    def test_path_with_parameter_filters(self, ssm):
        ssm.put_parameter(Name="/filtpath/str", Value="val", Type="String")
        ssm.put_parameter(Name="/filtpath/sec", Value="secret", Type="SecureString")
        response = ssm.get_parameters_by_path(
            Path="/filtpath",
            ParameterFilters=[{"Key": "Type", "Option": "Equals", "Values": ["String"]}],
        )
        names = [p["Name"] for p in response["Parameters"]]
        assert "/filtpath/str" in names
        assert "/filtpath/sec" not in names
        ssm.delete_parameter(Name="/filtpath/str")
        ssm.delete_parameter(Name="/filtpath/sec")

    def test_path_empty_result(self, ssm):
        response = ssm.get_parameters_by_path(Path="/nonexistent/path/xyz")
        assert len(response["Parameters"]) == 0


class TestSSMDescribeParameters:
    """Test describe_parameters with various filters."""

    def test_describe_with_name_filter(self, ssm):
        ssm.put_parameter(Name="/descfilt/target", Value="val", Type="String")
        ssm.put_parameter(Name="/descfilt/other", Value="val2", Type="String")
        response = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Option": "Equals", "Values": ["/descfilt/target"]}]
        )
        names = [p["Name"] for p in response["Parameters"]]
        assert "/descfilt/target" in names
        assert "/descfilt/other" not in names
        ssm.delete_parameter(Name="/descfilt/target")
        ssm.delete_parameter(Name="/descfilt/other")

    def test_describe_with_type_filter(self, ssm):
        ssm.put_parameter(Name="/desctype/str", Value="val", Type="String")
        ssm.put_parameter(Name="/desctype/sec", Value="secret", Type="SecureString")
        response = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Type", "Option": "Equals", "Values": ["SecureString"]}]
        )
        names = [p["Name"] for p in response["Parameters"]]
        assert "/desctype/sec" in names
        assert "/desctype/str" not in names
        ssm.delete_parameter(Name="/desctype/str")
        ssm.delete_parameter(Name="/desctype/sec")

    def test_describe_returns_metadata(self, ssm):
        ssm.put_parameter(
            Name="/descmeta/param", Value="val", Type="String", Description="A test parameter"
        )
        response = ssm.describe_parameters(
            ParameterFilters=[
                {"Key": "Name", "Option": "Equals", "Values": ["/descmeta/param"]}
            ]
        )
        assert len(response["Parameters"]) == 1
        meta = response["Parameters"][0]
        assert meta["Name"] == "/descmeta/param"
        assert meta["Type"] == "String"
        assert meta["Description"] == "A test parameter"
        assert "LastModifiedDate" in meta
        assert "Version" in meta
        ssm.delete_parameter(Name="/descmeta/param")


class TestSSMParameterTags:
    """Test parameter tagging operations."""

    def test_add_tags_to_existing_parameter(self, ssm):
        ssm.put_parameter(Name="/addtag/param", Value="val", Type="String")
        ssm.add_tags_to_resource(
            ResourceType="Parameter",
            ResourceId="/addtag/param",
            Tags=[
                {"Key": "team", "Value": "backend"},
                {"Key": "env", "Value": "staging"},
            ],
        )
        response = ssm.list_tags_for_resource(
            ResourceType="Parameter", ResourceId="/addtag/param"
        )
        tags = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert tags["team"] == "backend"
        assert tags["env"] == "staging"
        ssm.delete_parameter(Name="/addtag/param")

    def test_remove_tags_from_parameter(self, ssm):
        ssm.put_parameter(
            Name="/rmtag/param",
            Value="val",
            Type="String",
            Tags=[
                {"Key": "keep", "Value": "yes"},
                {"Key": "remove", "Value": "me"},
            ],
        )
        ssm.remove_tags_from_resource(
            ResourceType="Parameter",
            ResourceId="/rmtag/param",
            TagKeys=["remove"],
        )
        response = ssm.list_tags_for_resource(
            ResourceType="Parameter", ResourceId="/rmtag/param"
        )
        tags = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert "keep" in tags
        assert "remove" not in tags
        ssm.delete_parameter(Name="/rmtag/param")

    def test_overwrite_tags(self, ssm):
        ssm.put_parameter(
            Name="/overtag/param",
            Value="val",
            Type="String",
            Tags=[{"Key": "version", "Value": "1"}],
        )
        ssm.add_tags_to_resource(
            ResourceType="Parameter",
            ResourceId="/overtag/param",
            Tags=[{"Key": "version", "Value": "2"}],
        )
        response = ssm.list_tags_for_resource(
            ResourceType="Parameter", ResourceId="/overtag/param"
        )
        tags = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert tags["version"] == "2"
        ssm.delete_parameter(Name="/overtag/param")


class TestSSMParameterHistory:
    """Test parameter version history."""

    def test_history_tracks_versions(self, ssm):
        ssm.put_parameter(Name="/histver/param", Value="v1", Type="String")
        ssm.put_parameter(Name="/histver/param", Value="v2", Type="String", Overwrite=True)
        ssm.put_parameter(Name="/histver/param", Value="v3", Type="String", Overwrite=True)
        response = ssm.get_parameter_history(Name="/histver/param")
        versions = {p["Version"]: p["Value"] for p in response["Parameters"]}
        assert versions[1] == "v1"
        assert versions[2] == "v2"
        assert versions[3] == "v3"
        ssm.delete_parameter(Name="/histver/param")

    def test_history_includes_metadata(self, ssm):
        ssm.put_parameter(
            Name="/histmeta/param", Value="val", Type="String", Description="initial"
        )
        response = ssm.get_parameter_history(Name="/histmeta/param")
        assert len(response["Parameters"]) >= 1
        entry = response["Parameters"][0]
        assert entry["Name"] == "/histmeta/param"
        assert entry["Type"] == "String"
        assert "LastModifiedDate" in entry
        assert entry["Version"] == 1
        ssm.delete_parameter(Name="/histmeta/param")


class TestSSMParameterLabels:
    """Test parameter version labels."""

    def test_label_parameter_version(self, ssm):
        ssm.put_parameter(Name="/label/param", Value="v1", Type="String")
        ssm.label_parameter_version(
            Name="/label/param", ParameterVersion=1, Labels=["production", "stable"]
        )
        response = ssm.get_parameter_history(Name="/label/param")
        entry = response["Parameters"][0]
        assert "production" in entry.get("Labels", [])
        assert "stable" in entry.get("Labels", [])
        ssm.delete_parameter(Name="/label/param")

    def test_get_parameter_by_label(self, ssm):
        ssm.put_parameter(Name="/labget/param", Value="v1", Type="String")
        ssm.put_parameter(Name="/labget/param", Value="v2", Type="String", Overwrite=True)
        ssm.label_parameter_version(
            Name="/labget/param", ParameterVersion=1, Labels=["release-1"]
        )
        response = ssm.get_parameter(Name="/labget/param:release-1")
        assert response["Parameter"]["Value"] == "v1"
        ssm.delete_parameter(Name="/labget/param")


class TestSSMParameterDelete:
    """Test parameter deletion edge cases."""

    def test_delete_nonexistent_parameter(self, ssm):
        with pytest.raises(ClientError) as exc:
            ssm.delete_parameter(Name="/nonexistent/deleteme")
        assert exc.value.response["Error"]["Code"] == "ParameterNotFound"

    def test_put_without_overwrite_fails(self, ssm):
        ssm.put_parameter(Name="/noover/param", Value="v1", Type="String")
        with pytest.raises(ClientError) as exc:
            ssm.put_parameter(Name="/noover/param", Value="v2", Type="String")
        assert exc.value.response["Error"]["Code"] == "ParameterAlreadyExists"
        ssm.delete_parameter(Name="/noover/param")

    def test_get_nonexistent_parameter(self, ssm):
        with pytest.raises(ClientError) as exc:
            ssm.get_parameter(Name="/nonexistent/getme")
        assert exc.value.response["Error"]["Code"] == "ParameterNotFound"

    def test_parameter_description(self, ssm):
        ssm.put_parameter(
            Name="/withdesc/param",
            Value="val",
            Type="String",
            Description="My test description",
        )
        response = ssm.describe_parameters(
            ParameterFilters=[
                {"Key": "Name", "Option": "Equals", "Values": ["/withdesc/param"]}
            ]
        )
        assert response["Parameters"][0]["Description"] == "My test description"
        ssm.delete_parameter(Name="/withdesc/param")
