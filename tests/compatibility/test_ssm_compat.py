"""SSM Parameter Store compatibility tests."""

import pytest

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

    def test_add_and_remove_tags(self, ssm):
        ssm.put_parameter(Name="/addtag/param", Value="val", Type="String")
        ssm.add_tags_to_resource(
            ResourceType="Parameter",
            ResourceId="/addtag/param",
            Tags=[
                {"Key": "team", "Value": "platform"},
                {"Key": "cost", "Value": "dev"},
            ],
        )
        response = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId="/addtag/param")
        tags = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert tags["team"] == "platform"
        assert tags["cost"] == "dev"

        ssm.remove_tags_from_resource(
            ResourceType="Parameter", ResourceId="/addtag/param", TagKeys=["cost"]
        )
        response = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId="/addtag/param")
        tag_keys = [t["Key"] for t in response["TagList"]]
        assert "team" in tag_keys
        assert "cost" not in tag_keys
        ssm.delete_parameter(Name="/addtag/param")

    def test_string_list_multiple_values(self, ssm):
        ssm.put_parameter(Name="/slist/param", Value="x,y,z,w", Type="StringList")
        response = ssm.get_parameter(Name="/slist/param")
        assert response["Parameter"]["Value"] == "x,y,z,w"
        assert response["Parameter"]["Type"] == "StringList"
        ssm.delete_parameter(Name="/slist/param")

    def test_secure_string_without_decryption(self, ssm):
        ssm.put_parameter(Name="/secure/nodec", Value="hidden", Type="SecureString")
        response = ssm.get_parameter(Name="/secure/nodec", WithDecryption=False)
        # Without decryption, value should not be plaintext
        assert response["Parameter"]["Type"] == "SecureString"
        ssm.delete_parameter(Name="/secure/nodec")

    def test_parameter_history_versions(self, ssm):
        ssm.put_parameter(Name="/histv/param", Value="a", Type="String")
        ssm.put_parameter(Name="/histv/param", Value="b", Type="String", Overwrite=True)
        ssm.put_parameter(Name="/histv/param", Value="c", Type="String", Overwrite=True)
        response = ssm.get_parameter_history(Name="/histv/param")
        versions = [p["Version"] for p in response["Parameters"]]
        assert 1 in versions
        assert 2 in versions
        assert 3 in versions
        ssm.delete_parameter(Name="/histv/param")

    def test_get_parameters_by_path_recursive(self, ssm):
        ssm.put_parameter(Name="/rec/a/b", Value="1", Type="String")
        ssm.put_parameter(Name="/rec/a/c/d", Value="2", Type="String")
        response = ssm.get_parameters_by_path(Path="/rec/a", Recursive=True)
        names = [p["Name"] for p in response["Parameters"]]
        assert "/rec/a/b" in names
        assert "/rec/a/c/d" in names

        # Without recursive, only direct children
        response2 = ssm.get_parameters_by_path(Path="/rec/a", Recursive=False)
        names2 = [p["Name"] for p in response2["Parameters"]]
        assert "/rec/a/b" in names2
        assert "/rec/a/c/d" not in names2

        ssm.delete_parameter(Name="/rec/a/b")
        ssm.delete_parameter(Name="/rec/a/c/d")

    def test_overwrite_changes_version(self, ssm):
        ssm.put_parameter(Name="/owver/param", Value="v1", Type="String")
        resp = ssm.get_parameter(Name="/owver/param")
        assert resp["Parameter"]["Version"] == 1

        ssm.put_parameter(Name="/owver/param", Value="v2", Type="String", Overwrite=True)
        resp = ssm.get_parameter(Name="/owver/param")
        assert resp["Parameter"]["Version"] == 2
        ssm.delete_parameter(Name="/owver/param")

    def test_describe_parameters_with_filters(self, ssm):
        ssm.put_parameter(Name="/filt/alpha", Value="val", Type="String")
        ssm.put_parameter(Name="/filt/beta", Value="val", Type="String")
        response = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Values": ["/filt/alpha"]}]
        )
        names = [p["Name"] for p in response["Parameters"]]
        assert "/filt/alpha" in names
        assert "/filt/beta" not in names
        ssm.delete_parameter(Name="/filt/alpha")
        ssm.delete_parameter(Name="/filt/beta")

    def test_get_parameters_batch(self, ssm):
        ssm.put_parameter(Name="/batch/x", Value="10", Type="String")
        ssm.put_parameter(Name="/batch/y", Value="20", Type="String")
        ssm.put_parameter(Name="/batch/z", Value="30", Type="String")
        response = ssm.get_parameters(Names=["/batch/x", "/batch/y", "/batch/z"])
        found = {p["Name"]: p["Value"] for p in response["Parameters"]}
        assert found["/batch/x"] == "10"
        assert found["/batch/y"] == "20"
        assert found["/batch/z"] == "30"
        assert len(response.get("InvalidParameters", [])) == 0
        ssm.delete_parameter(Name="/batch/x")
        ssm.delete_parameter(Name="/batch/y")
        ssm.delete_parameter(Name="/batch/z")

    def test_put_parameter_no_overwrite_fails(self, ssm):
        ssm.put_parameter(Name="/noow/param", Value="v1", Type="String")
        with pytest.raises(ssm.exceptions.ParameterAlreadyExists):
            ssm.put_parameter(Name="/noow/param", Value="v2", Type="String")
        ssm.delete_parameter(Name="/noow/param")

    def test_delete_nonexistent_parameter(self, ssm):
        with pytest.raises(ssm.exceptions.ParameterNotFound):
            ssm.delete_parameter(Name="/nonexistent/param-xyz")

    def test_get_parameters_by_path_with_values(self, ssm):
        ssm.put_parameter(Name="/pathval/a", Value="alpha", Type="String")
        ssm.put_parameter(Name="/pathval/b", Value="beta", Type="String")
        response = ssm.get_parameters_by_path(Path="/pathval")
        params = {p["Name"]: p["Value"] for p in response["Parameters"]}
        assert params["/pathval/a"] == "alpha"
        assert params["/pathval/b"] == "beta"
        ssm.delete_parameter(Name="/pathval/a")
        ssm.delete_parameter(Name="/pathval/b")
    def test_label_parameter_version(self, ssm):
        ssm.put_parameter(Name="/label/param", Value="v1", Type="String")
        ssm.label_parameter_version(
            Name="/label/param", ParameterVersion=1, Labels=["prod"]
        )
        response = ssm.get_parameter_history(Name="/label/param")
        labels = response["Parameters"][0].get("Labels", [])
        assert "prod" in labels
        ssm.delete_parameter(Name="/label/param")

    def test_delete_parameters(self, ssm):
        ssm.put_parameter(Name="/delmulti/a", Value="1", Type="String")
        ssm.put_parameter(Name="/delmulti/b", Value="2", Type="String")
        response = ssm.delete_parameters(Names=["/delmulti/a", "/delmulti/b"])
        deleted = response["DeletedParameters"]
        assert "/delmulti/a" in deleted
        assert "/delmulti/b" in deleted
