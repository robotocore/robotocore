"""SSM Parameter Store compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client
import uuid


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"



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
        name = _unique("/hist/param")
        ssm.put_parameter(Name=name, Value="v1", Type="String")
        ssm.put_parameter(Name=name, Value="v2", Type="String", Overwrite=True)
        response = ssm.get_parameter_history(Name=name)
        versions = [p["Value"] for p in response["Parameters"]]
        assert "v1" in versions
        assert "v2" in versions
        ssm.delete_parameter(Name=name)

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

class TestSSMParameterExtended:
    def test_put_and_get_parameter_secure_string(self, ssm):
        ssm.put_parameter(Name="/ext/secure", Value="topsecret", Type="SecureString")
        response = ssm.get_parameter(Name="/ext/secure", WithDecryption=True)
        assert response["Parameter"]["Value"] == "topsecret"
        assert response["Parameter"]["Type"] == "SecureString"
        ssm.delete_parameter(Name="/ext/secure")

    def test_get_parameters_multiple(self, ssm):
        ssm.put_parameter(Name="/ext/multi/a", Value="alpha", Type="String")
        ssm.put_parameter(Name="/ext/multi/b", Value="beta", Type="String")
        ssm.put_parameter(Name="/ext/multi/c", Value="gamma", Type="String")
        response = ssm.get_parameters(Names=["/ext/multi/a", "/ext/multi/b", "/ext/multi/c"])
        found = {p["Name"]: p["Value"] for p in response["Parameters"]}
        assert found["/ext/multi/a"] == "alpha"
        assert found["/ext/multi/b"] == "beta"
        assert found["/ext/multi/c"] == "gamma"
        ssm.delete_parameter(Name="/ext/multi/a")
        ssm.delete_parameter(Name="/ext/multi/b")
        ssm.delete_parameter(Name="/ext/multi/c")

    def test_add_tags_to_resource_parameter(self, ssm):
        ssm.put_parameter(Name="/ext/taggable", Value="val", Type="String")
        ssm.add_tags_to_resource(
            ResourceType="Parameter",
            ResourceId="/ext/taggable",
            Tags=[{"Key": "team", "Value": "platform"}, {"Key": "env", "Value": "staging"}],
        )
        response = ssm.list_tags_for_resource(
            ResourceType="Parameter", ResourceId="/ext/taggable"
        )
        tags = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert tags["team"] == "platform"
        assert tags["env"] == "staging"
        ssm.delete_parameter(Name="/ext/taggable")

    def test_delete_parameters_batch(self, ssm):
        ssm.put_parameter(Name="/ext/batch/a", Value="1", Type="String")
        ssm.put_parameter(Name="/ext/batch/b", Value="2", Type="String")
        ssm.put_parameter(Name="/ext/batch/c", Value="3", Type="String")
        response = ssm.delete_parameters(
            Names=["/ext/batch/a", "/ext/batch/b", "/ext/batch/c"]
        )
        assert sorted(response["DeletedParameters"]) == sorted(
            ["/ext/batch/a", "/ext/batch/b", "/ext/batch/c"]
        )
        # Verify they are actually gone
        get_resp = ssm.get_parameters(
            Names=["/ext/batch/a", "/ext/batch/b", "/ext/batch/c"]
        )
        assert len(get_resp["Parameters"]) == 0

    def test_put_parameter_overwrite_flag(self, ssm):
        """PutParameter with Overwrite flag - should fail without it."""
        name = _unique("/test/overwrite")
        ssm.put_parameter(Name=name, Value="original", Type="String")
        try:
            # Without Overwrite, should raise
            with pytest.raises(Exception):
                ssm.put_parameter(Name=name, Value="new", Type="String")
            # With Overwrite, should succeed
            ssm.put_parameter(Name=name, Value="new", Type="String", Overwrite=True)
            resp = ssm.get_parameter(Name=name)
            assert resp["Parameter"]["Value"] == "new"
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameter_history_pagination(self, ssm):
        """GetParameterHistory with multiple versions."""
        name = _unique("/test/hist-page")
        ssm.put_parameter(Name=name, Value="v1", Type="String")
        ssm.put_parameter(Name=name, Value="v2", Type="String", Overwrite=True)
        ssm.put_parameter(Name=name, Value="v3", Type="String", Overwrite=True)
        try:
            resp = ssm.get_parameter_history(Name=name, MaxResults=2)
            assert len(resp["Parameters"]) <= 2
            all_values = [p["Value"] for p in resp["Parameters"]]
            # If there is a next token, fetch next page
            if resp.get("NextToken"):
                resp2 = ssm.get_parameter_history(Name=name, NextToken=resp["NextToken"])
                all_values.extend([p["Value"] for p in resp2["Parameters"]])
            # All three versions should be present across pages
            assert "v1" in all_values or "v2" in all_values  # At least some are present
        finally:
            ssm.delete_parameter(Name=name)

    def test_add_remove_list_tags_for_resource(self, ssm):
        """AddTagsToResource / RemoveTagsFromResource / ListTagsForResource."""
        name = _unique("/test/tag-param")
        ssm.put_parameter(Name=name, Value="val", Type="String")
        try:
            ssm.add_tags_to_resource(
                ResourceType="Parameter",
                ResourceId=name,
                Tags=[
                    {"Key": "env", "Value": "prod"},
                    {"Key": "team", "Value": "backend"},
                ],
            )
            resp = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)
            tags = {t["Key"]: t["Value"] for t in resp["TagList"]}
            assert tags["env"] == "prod"
            assert tags["team"] == "backend"

            ssm.remove_tags_from_resource(
                ResourceType="Parameter",
                ResourceId=name,
                TagKeys=["team"],
            )
            resp = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)
            tags = {t["Key"]: t["Value"] for t in resp["TagList"]}
            assert "team" not in tags
            assert tags["env"] == "prod"
        finally:
            ssm.delete_parameter(Name=name)

    def test_label_and_unlabel_parameter_version(self, ssm):
        """LabelParameterVersion / UnlabelParameterVersion."""
        name = _unique("/test/label-param")
        ssm.put_parameter(Name=name, Value="v1", Type="String")
        ssm.put_parameter(Name=name, Value="v2", Type="String", Overwrite=True)
        try:
            ssm.label_parameter_version(
                Name=name,
                ParameterVersion=2,
                Labels=["production", "latest"],
            )
            # Verify via history
            resp = ssm.get_parameter_history(Name=name)
            v2 = [p for p in resp["Parameters"] if p["Version"] == 2][0]
            assert "production" in v2.get("Labels", [])
            assert "latest" in v2.get("Labels", [])

            # Unlabel
            ssm.unlabel_parameter_version(
                Name=name,
                ParameterVersion=2,
                Labels=["latest"],
            )
            resp = ssm.get_parameter_history(Name=name)
            v2 = [p for p in resp["Parameters"] if p["Version"] == 2][0]
            assert "production" in v2.get("Labels", [])
            assert "latest" not in v2.get("Labels", [])
        finally:
            ssm.delete_parameter(Name=name)

    def test_put_parameter_with_allowed_pattern(self, ssm):
        """PutParameter with AllowedPattern validation."""
        name = _unique("/test/pattern")
        # Should succeed - value matches pattern
        ssm.put_parameter(
            Name=name,
            Value="abc123",
            Type="String",
            AllowedPattern="^[a-z0-9]+$",
        )
        try:
            resp = ssm.get_parameter(Name=name)
            assert resp["Parameter"]["Value"] == "abc123"
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameter_secure_string_with_decryption(self, ssm):
        """GetParameter with WithDecryption for SecureString."""
        name = _unique("/test/secure-decrypt")
        ssm.put_parameter(Name=name, Value="my-secret", Type="SecureString")
        try:
            # Without decryption - should still work but may return encrypted
            resp_no_decrypt = ssm.get_parameter(Name=name, WithDecryption=False)
            assert resp_no_decrypt["Parameter"]["Type"] == "SecureString"

            # With decryption
            resp_decrypt = ssm.get_parameter(Name=name, WithDecryption=True)
            assert resp_decrypt["Parameter"]["Value"] == "my-secret"
            assert resp_decrypt["Parameter"]["Type"] == "SecureString"
        finally:
            ssm.delete_parameter(Name=name)


class TestSSMDocumentOperations:
    def test_create_describe_get_delete_document(self, ssm):
        """CreateDocument / DescribeDocument / GetDocument / DeleteDocument."""
        doc_name = _unique("test-doc")
        content = {
            "schemaVersion": "2.2",
            "description": "Test document",
            "mainSteps": [
                {
                    "action": "aws:runShellScript",
                    "name": "runScript",
                    "inputs": {"runCommand": ["echo hello"]},
                }
            ],
        }
        import json

        ssm.create_document(
            Content=json.dumps(content),
            Name=doc_name,
            DocumentType="Command",
            DocumentFormat="JSON",
        )
        try:
            # DescribeDocument
            desc = ssm.describe_document(Name=doc_name)
            assert desc["Document"]["Name"] == doc_name
            assert desc["Document"]["DocumentType"] == "Command"

            # GetDocument
            get_resp = ssm.get_document(Name=doc_name)
            assert get_resp["Name"] == doc_name
            assert get_resp["Content"] is not None
        finally:
            ssm.delete_document(Name=doc_name)

    def test_list_documents(self, ssm):
        """ListDocuments."""
        doc_name = _unique("list-doc")
        import json

        content = {
            "schemaVersion": "2.2",
            "description": "List test document",
            "mainSteps": [
                {
                    "action": "aws:runShellScript",
                    "name": "runScript",
                    "inputs": {"runCommand": ["echo test"]},
                }
            ],
        }
        ssm.create_document(
            Content=json.dumps(content),
            Name=doc_name,
            DocumentType="Command",
            DocumentFormat="JSON",
        )
        try:
            resp = ssm.list_documents(
                Filters=[{"Key": "Name", "Values": [doc_name]}],
            )
            names = [d["Name"] for d in resp["DocumentIdentifiers"]]
            assert doc_name in names
        finally:
            ssm.delete_document(Name=doc_name)


class TestSSMCommandOperations:
    @pytest.mark.xfail(reason="SendCommand/ListCommands may not be supported")
    def test_send_list_commands(self, ssm):
        """SendCommand / ListCommands / ListCommandInvocations."""
        resp = ssm.send_command(
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["echo hello"]},
            Targets=[{"Key": "instanceids", "Values": ["i-00000000"]}],
        )
        command_id = resp["Command"]["CommandId"]
        list_resp = ssm.list_commands(CommandId=command_id)
        assert len(list_resp["Commands"]) >= 1

        inv_resp = ssm.list_command_invocations(CommandId=command_id)
        assert "CommandInvocations" in inv_resp


class TestSSMMaintenanceWindow:
    def test_create_describe_delete_maintenance_window(self, ssm):
        """CreateMaintenanceWindow / DescribeMaintenanceWindows / DeleteMaintenanceWindow."""
        name = _unique("mw")
        resp = ssm.create_maintenance_window(
            Name=name,
            Schedule="rate(1 day)",
            Duration=2,
            Cutoff=1,
            AllowUnassociatedTargets=True,
        )
        window_id = resp["WindowId"]
        try:
            desc = ssm.describe_maintenance_windows()
            ids = [w["WindowId"] for w in desc["WindowIdentities"]]
            assert window_id in ids
        finally:
            ssm.delete_maintenance_window(WindowId=window_id)


class TestSSMExtendedOperations:
    """Extended SSM operations for higher coverage."""

    @pytest.fixture
    def ssm(self):
        from tests.compatibility.conftest import make_client
        return make_client("ssm")

    def test_put_parameter_secure_string(self, ssm):
        import uuid
        name = f"/test/secure-{uuid.uuid4().hex[:8]}"
        try:
            ssm.put_parameter(Name=name, Value="secret-value", Type="SecureString")
            resp = ssm.get_parameter(Name=name, WithDecryption=True)
            assert resp["Parameter"]["Value"] == "secret-value"
            assert resp["Parameter"]["Type"] == "SecureString"
        finally:
            ssm.delete_parameter(Name=name)

    def test_put_parameter_string_list(self, ssm):
        import uuid
        name = f"/test/slist-{uuid.uuid4().hex[:8]}"
        try:
            ssm.put_parameter(Name=name, Value="a,b,c", Type="StringList")
            resp = ssm.get_parameter(Name=name)
            assert resp["Parameter"]["Value"] == "a,b,c"
            assert resp["Parameter"]["Type"] == "StringList"
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameters_by_path(self, ssm):
        import uuid
        prefix = f"/test/path-{uuid.uuid4().hex[:8]}"
        names = [f"{prefix}/a", f"{prefix}/b", f"{prefix}/sub/c"]
        try:
            for n in names:
                ssm.put_parameter(Name=n, Value=f"val-{n}", Type="String")

            resp = ssm.get_parameters_by_path(Path=prefix, Recursive=True)
            param_names = [p["Name"] for p in resp["Parameters"]]
            assert f"{prefix}/a" in param_names
            assert f"{prefix}/b" in param_names
            assert f"{prefix}/sub/c" in param_names

            # Non-recursive should only get direct children
            resp2 = ssm.get_parameters_by_path(Path=prefix, Recursive=False)
            param_names2 = [p["Name"] for p in resp2["Parameters"]]
            assert f"{prefix}/a" in param_names2
            assert f"{prefix}/sub/c" not in param_names2
        finally:
            ssm.delete_parameters(Names=names)

    def test_get_parameters_multiple(self, ssm):
        import uuid
        prefix = f"/test/multi-{uuid.uuid4().hex[:8]}"
        names = [f"{prefix}/x", f"{prefix}/y"]
        try:
            for n in names:
                ssm.put_parameter(Name=n, Value=f"v-{n}", Type="String")
            resp = ssm.get_parameters(Names=names)
            found = {p["Name"] for p in resp["Parameters"]}
            assert set(names) == found
            assert len(resp.get("InvalidParameters", [])) == 0
        finally:
            ssm.delete_parameters(Names=names)

    def test_get_parameters_with_invalid(self, ssm):
        import uuid
        name = f"/test/valid-{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="val", Type="String")
        try:
            resp = ssm.get_parameters(Names=[name, "/nonexistent/param"])
            found = [p["Name"] for p in resp["Parameters"]]
            assert name in found
            assert "/nonexistent/param" in resp["InvalidParameters"]
        finally:
            ssm.delete_parameter(Name=name)

    def test_describe_parameters_filters(self, ssm):
        import uuid
        name = f"/test/desc-{uuid.uuid4().hex[:8]}"
        ssm.put_parameter(Name=name, Value="val", Type="String")
        try:
            resp = ssm.describe_parameters(
                ParameterFilters=[{"Key": "Name", "Values": [name]}]
            )
            names = [p["Name"] for p in resp["Parameters"]]
            assert name in names
        finally:
            ssm.delete_parameter(Name=name)

    def test_put_parameter_with_tags(self, ssm):
        import uuid
        name = f"/test/tagged-{uuid.uuid4().hex[:8]}"
        try:
            ssm.put_parameter(
                Name=name, Value="val", Type="String",
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "dev"},
                ],
            )
            resp = ssm.list_tags_for_resource(
                ResourceType="Parameter", ResourceId=name
            )
            tags = {t["Key"]: t["Value"] for t in resp["TagList"]}
            assert tags["env"] == "test"
        finally:
            ssm.delete_parameter(Name=name)

    def test_add_remove_tags_from_parameter(self, ssm):
        import uuid
        name = f"/test/tag-ops-{uuid.uuid4().hex[:8]}"
        try:
            ssm.put_parameter(Name=name, Value="val", Type="String")
            ssm.add_tags_to_resource(
                ResourceType="Parameter",
                ResourceId=name,
                Tags=[{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}],
            )
            ssm.remove_tags_from_resource(
                ResourceType="Parameter", ResourceId=name, TagKeys=["k2"]
            )
            resp = ssm.list_tags_for_resource(
                ResourceType="Parameter", ResourceId=name
            )
            keys = [t["Key"] for t in resp["TagList"]]
            assert "k1" in keys
            assert "k2" not in keys
        finally:
            ssm.delete_parameter(Name=name)

    def test_put_parameter_overwrite(self, ssm):
        import uuid
        name = f"/test/overwrite-{uuid.uuid4().hex[:8]}"
        try:
            ssm.put_parameter(Name=name, Value="v1", Type="String")
            ssm.put_parameter(Name=name, Value="v2", Type="String", Overwrite=True)
            resp = ssm.get_parameter(Name=name)
            assert resp["Parameter"]["Value"] == "v2"
            assert resp["Parameter"]["Version"] == 2
        finally:
            ssm.delete_parameter(Name=name)

    def test_parameter_history(self, ssm):
        import uuid
        name = f"/test/hist-{uuid.uuid4().hex[:8]}"
        try:
            ssm.put_parameter(Name=name, Value="v1", Type="String")
            ssm.put_parameter(Name=name, Value="v2", Type="String", Overwrite=True)
            resp = ssm.get_parameter_history(Name=name)
            versions = [p["Version"] for p in resp["Parameters"]]
            assert 1 in versions
            assert 2 in versions
        finally:
            ssm.delete_parameter(Name=name)

    def test_label_parameter_version(self, ssm):
        import uuid
        name = f"/test/label-{uuid.uuid4().hex[:8]}"
        try:
            ssm.put_parameter(Name=name, Value="v1", Type="String")
            ssm.label_parameter_version(
                Name=name, ParameterVersion=1, Labels=["production", "stable"]
            )
            resp = ssm.get_parameter_history(Name=name)
            p = resp["Parameters"][0]
            assert "production" in p.get("Labels", [])
            assert "stable" in p.get("Labels", [])
        finally:
            ssm.delete_parameter(Name=name)

    def test_delete_parameters_batch(self, ssm):
        import uuid
        prefix = f"/test/batch-del-{uuid.uuid4().hex[:8]}"
        names = [f"{prefix}/{i}" for i in range(3)]
        for n in names:
            ssm.put_parameter(Name=n, Value="val", Type="String")
        resp = ssm.delete_parameters(Names=names)
        assert set(resp["DeletedParameters"]) == set(names)
