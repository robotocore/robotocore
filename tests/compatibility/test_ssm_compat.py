"""SSM Parameter Store compatibility tests."""

import uuid

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

    def test_put_parameter_with_description(self, ssm):
        name = f"/desc/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(
            Name=name, Value="val", Type="String", Description="A test parameter"
        )
        response = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Values": [name]}]
        )
        param = response["Parameters"][0]
        assert param["Description"] == "A test parameter"
        ssm.delete_parameter(Name=name)

    def test_get_parameters_by_path_recursive(self, ssm):
        prefix = f"/recursive/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=f"{prefix}/a", Value="1", Type="String")
        ssm.put_parameter(Name=f"{prefix}/sub/b", Value="2", Type="String")

        # Non-recursive should only get direct children
        response = ssm.get_parameters_by_path(Path=prefix, Recursive=False)
        names = [p["Name"] for p in response["Parameters"]]
        assert f"{prefix}/a" in names
        assert f"{prefix}/sub/b" not in names

        # Recursive should get all descendants
        response = ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        names = [p["Name"] for p in response["Parameters"]]
        assert f"{prefix}/a" in names
        assert f"{prefix}/sub/b" in names

        ssm.delete_parameter(Name=f"{prefix}/a")
        ssm.delete_parameter(Name=f"{prefix}/sub/b")

    def test_get_parameters_by_path_with_decryption(self, ssm):
        prefix = f"/secpath/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=f"{prefix}/secret", Value="hidden", Type="SecureString")
        response = ssm.get_parameters_by_path(Path=prefix, WithDecryption=True)
        assert len(response["Parameters"]) == 1
        assert response["Parameters"][0]["Value"] == "hidden"
        ssm.delete_parameter(Name=f"{prefix}/secret")

    def test_describe_parameters_with_name_filter(self, ssm):
        name = f"/filtdesc/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="val", Type="String")
        response = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Values": [name]}]
        )
        assert len(response["Parameters"]) == 1
        assert response["Parameters"][0]["Name"] == name
        ssm.delete_parameter(Name=name)

    def test_describe_parameters_with_type_filter(self, ssm):
        name = f"/typefilt/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="val", Type="SecureString")
        response = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Type", "Values": ["SecureString"]}]
        )
        names = [p["Name"] for p in response["Parameters"]]
        assert name in names
        ssm.delete_parameter(Name=name)

    def test_add_tags_to_parameter(self, ssm):
        name = f"/addtag/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="val", Type="String")
        ssm.add_tags_to_resource(
            ResourceType="Parameter",
            ResourceId=name,
            Tags=[{"Key": "team", "Value": "backend"}, {"Key": "cost", "Value": "low"}],
        )
        response = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)
        tags = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert tags["team"] == "backend"
        assert tags["cost"] == "low"
        ssm.delete_parameter(Name=name)

    def test_remove_tags_from_parameter(self, ssm):
        name = f"/rmtag/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(
            Name=name,
            Value="val",
            Type="String",
            Tags=[{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}],
        )
        ssm.remove_tags_from_resource(
            ResourceType="Parameter", ResourceId=name, TagKeys=["a"]
        )
        response = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)
        tags = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert "a" not in tags
        assert tags["b"] == "2"
        ssm.delete_parameter(Name=name)

    def test_parameter_version_increments(self, ssm):
        name = f"/ver/{uuid.uuid4().hex[:8]}"
        r1 = ssm.put_parameter(Name=name, Value="v1", Type="String")
        assert r1["Version"] == 1
        r2 = ssm.put_parameter(Name=name, Value="v2", Type="String", Overwrite=True)
        assert r2["Version"] == 2
        ssm.delete_parameter(Name=name)

    def test_get_parameter_returns_version(self, ssm):
        name = f"/getver/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="v1", Type="String")
        ssm.put_parameter(Name=name, Value="v2", Type="String", Overwrite=True)
        response = ssm.get_parameter(Name=name)
        assert response["Parameter"]["Version"] == 2
        ssm.delete_parameter(Name=name)

    def test_get_parameter_history_versions(self, ssm):
        name = f"/histver/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="a", Type="String")
        ssm.put_parameter(Name=name, Value="b", Type="String", Overwrite=True)
        ssm.put_parameter(Name=name, Value="c", Type="String", Overwrite=True)
        response = ssm.get_parameter_history(Name=name)
        versions = [p["Version"] for p in response["Parameters"]]
        assert versions == [1, 2, 3]
        ssm.delete_parameter(Name=name)

    def test_label_parameter_version(self, ssm):
        name = f"/label/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="v1", Type="String")
        ssm.label_parameter_version(
            Name=name, ParameterVersion=1, Labels=["prod", "stable"]
        )
        response = ssm.get_parameter_history(Name=name)
        labels = response["Parameters"][0].get("Labels", [])
        assert "prod" in labels
        assert "stable" in labels
        ssm.delete_parameter(Name=name)

    def test_get_parameter_by_version(self, ssm):
        name = f"/byver/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="first", Type="String")
        ssm.put_parameter(Name=name, Value="second", Type="String", Overwrite=True)
        response = ssm.get_parameter(Name=f"{name}:1")
        assert response["Parameter"]["Value"] == "first"
        response = ssm.get_parameter(Name=f"{name}:2")
        assert response["Parameter"]["Value"] == "second"
        ssm.delete_parameter(Name=name)

    def test_get_parameter_by_label(self, ssm):
        name = f"/bylabel/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="val", Type="String")
        ssm.label_parameter_version(Name=name, ParameterVersion=1, Labels=["release"])
        response = ssm.get_parameter(Name=f"{name}:release")
        assert response["Parameter"]["Value"] == "val"
        ssm.delete_parameter(Name=name)

    def test_delete_nonexistent_parameter_raises(self, ssm):
        with pytest.raises(ClientError) as exc_info:
            ssm.delete_parameter(Name=f"/nonexistent/{uuid.uuid4().hex}")
        assert exc_info.value.response["Error"]["Code"] == "ParameterNotFound"

    def test_get_nonexistent_parameter_raises(self, ssm):
        with pytest.raises(ClientError) as exc_info:
            ssm.get_parameter(Name=f"/nonexistent/{uuid.uuid4().hex}")
        assert exc_info.value.response["Error"]["Code"] == "ParameterNotFound"

    def test_put_parameter_no_overwrite_raises(self, ssm):
        name = f"/noover/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="v1", Type="String")
        with pytest.raises(ClientError) as exc_info:
            ssm.put_parameter(Name=name, Value="v2", Type="String")
        assert exc_info.value.response["Error"]["Code"] == "ParameterAlreadyExists"
        ssm.delete_parameter(Name=name)

    def test_get_parameters_batch(self, ssm):
        prefix = f"/batch/{uuid.uuid4().hex[:8]}"
        names = [f"{prefix}/p{i}" for i in range(5)]
        for n in names:
            ssm.put_parameter(Name=n, Value=f"val-{n}", Type="String")
        response = ssm.get_parameters(Names=names)
        found = {p["Name"] for p in response["Parameters"]}
        assert found == set(names)
        assert len(response.get("InvalidParameters", [])) == 0
        for n in names:
            ssm.delete_parameter(Name=n)

    def test_get_parameters_with_and_without_decryption(self, ssm):
        name = f"/dectest/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="secret", Type="SecureString")
        response = ssm.get_parameters(Names=[name], WithDecryption=True)
        assert response["Parameters"][0]["Value"] == "secret"
        ssm.delete_parameter(Name=name)

    def test_parameter_type_in_response(self, ssm):
        prefix = f"/types/{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=f"{prefix}/str", Value="val", Type="String")
        ssm.put_parameter(Name=f"{prefix}/sl", Value="a,b", Type="StringList")
        ssm.put_parameter(Name=f"{prefix}/ss", Value="sec", Type="SecureString")

        for name, expected_type in [
            (f"{prefix}/str", "String"),
            (f"{prefix}/sl", "StringList"),
            (f"{prefix}/ss", "SecureString"),
        ]:
            resp = ssm.get_parameter(Name=name, WithDecryption=True)
            assert resp["Parameter"]["Type"] == expected_type

        ssm.delete_parameter(Name=f"{prefix}/str")
        ssm.delete_parameter(Name=f"{prefix}/sl")
        ssm.delete_parameter(Name=f"{prefix}/ss")
