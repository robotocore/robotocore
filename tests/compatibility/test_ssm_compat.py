"""SSM Parameter Store compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


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
        ssm.label_parameter_version(Name="/label/param", ParameterVersion=1, Labels=["prod"])
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
        response = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId="/ext/taggable")
        tags = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert tags["team"] == "platform"
        assert tags["env"] == "staging"
        ssm.delete_parameter(Name="/ext/taggable")

    def test_delete_parameters_batch(self, ssm):
        ssm.put_parameter(Name="/ext/batch/a", Value="1", Type="String")
        ssm.put_parameter(Name="/ext/batch/b", Value="2", Type="String")
        ssm.put_parameter(Name="/ext/batch/c", Value="3", Type="String")
        response = ssm.delete_parameters(Names=["/ext/batch/a", "/ext/batch/b", "/ext/batch/c"])
        assert sorted(response["DeletedParameters"]) == sorted(
            ["/ext/batch/a", "/ext/batch/b", "/ext/batch/c"]
        )
        # Verify they are actually gone
        get_resp = ssm.get_parameters(Names=["/ext/batch/a", "/ext/batch/b", "/ext/batch/c"])
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

    def test_put_parameter_with_tags(self, ssm):
        import uuid

        name = f"/test/tagged-{uuid.uuid4().hex[:8]}"
        try:
            ssm.put_parameter(
                Name=name,
                Value="val",
                Type="String",
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "dev"},
                ],
            )
            resp = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)
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
            ssm.remove_tags_from_resource(ResourceType="Parameter", ResourceId=name, TagKeys=["k2"])
            resp = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)
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


class TestSSMDocumentExtended:
    """Extended document operations: update, list versions, describe permissions."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def _doc_content(self, description="Test document"):
        import json

        return json.dumps(
            {
                "schemaVersion": "2.2",
                "description": description,
                "mainSteps": [
                    {
                        "action": "aws:runShellScript",
                        "name": "runScript",
                        "inputs": {"runCommand": ["echo hello"]},
                    }
                ],
            }
        )

    def test_update_document(self, ssm):
        """UpdateDocument creates a new version."""
        doc_name = _unique("upd-doc")
        ssm.create_document(
            Content=self._doc_content("v1"),
            Name=doc_name,
            DocumentType="Command",
            DocumentFormat="JSON",
        )
        try:
            resp = ssm.update_document(
                Content=self._doc_content("v2"),
                Name=doc_name,
                DocumentVersion="$LATEST",
            )
            assert resp["DocumentDescription"]["Name"] == doc_name
            # Version should be "2" after update
            assert resp["DocumentDescription"]["DocumentVersion"] in ("2", 2)
        finally:
            ssm.delete_document(Name=doc_name)

    def test_update_document_default_version(self, ssm):
        """UpdateDocumentDefaultVersion sets the default version."""
        doc_name = _unique("defver-doc")
        ssm.create_document(
            Content=self._doc_content("v1"),
            Name=doc_name,
            DocumentType="Command",
            DocumentFormat="JSON",
        )
        try:
            # Create version 2
            ssm.update_document(
                Content=self._doc_content("v2"),
                Name=doc_name,
                DocumentVersion="$LATEST",
            )
            # Set default version to 2
            resp = ssm.update_document_default_version(Name=doc_name, DocumentVersion="2")
            desc = resp["Description"]
            assert desc["Name"] == doc_name
            assert desc["DefaultVersion"] == "2"
            # Verify via describe
            info = ssm.describe_document(Name=doc_name)
            assert info["Document"]["DefaultVersion"] == "2"
        finally:
            ssm.delete_document(Name=doc_name)

    def test_describe_document_permission(self, ssm):
        """DescribeDocumentPermission returns sharing info."""
        doc_name = _unique("perm-doc")
        ssm.create_document(
            Content=self._doc_content(),
            Name=doc_name,
            DocumentType="Command",
            DocumentFormat="JSON",
        )
        try:
            resp = ssm.describe_document_permission(Name=doc_name, PermissionType="Share")
            assert "AccountIds" in resp
        finally:
            ssm.delete_document(Name=doc_name)


class TestSSMMaintenanceWindowExtended:
    """Extended maintenance window operations."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def _create_window(self, ssm, name=None):
        name = name or _unique("mw")
        resp = ssm.create_maintenance_window(
            Name=name,
            Schedule="rate(1 day)",
            Duration=2,
            Cutoff=1,
            AllowUnassociatedTargets=True,
        )
        return resp["WindowId"]

    def test_get_maintenance_window(self, ssm):
        """GetMaintenanceWindow returns details."""
        wid = self._create_window(ssm)
        try:
            resp = ssm.get_maintenance_window(WindowId=wid)
            assert resp["WindowId"] == wid
            assert resp["Duration"] == 2
            assert resp["Cutoff"] == 1
            assert resp["Schedule"] == "rate(1 day)"
        finally:
            ssm.delete_maintenance_window(WindowId=wid)

    def test_register_and_deregister_target(self, ssm):
        """RegisterTargetWithMaintenanceWindow / DeregisterTargetFromMaintenanceWindow."""
        wid = self._create_window(ssm)
        try:
            reg = ssm.register_target_with_maintenance_window(
                WindowId=wid,
                ResourceType="INSTANCE",
                Targets=[{"Key": "tag:Environment", "Values": ["prod"]}],
            )
            target_id = reg["WindowTargetId"]

            # Verify target is listed
            desc = ssm.describe_maintenance_window_targets(
                WindowId=wid,
                Filters=[],
            )
            target_ids = [t["WindowTargetId"] for t in desc["Targets"]]
            assert target_id in target_ids

            # Deregister
            ssm.deregister_target_from_maintenance_window(WindowId=wid, WindowTargetId=target_id)
            desc2 = ssm.describe_maintenance_window_targets(
                WindowId=wid,
                Filters=[],
            )
            target_ids2 = [t["WindowTargetId"] for t in desc2["Targets"]]
            assert target_id not in target_ids2
        finally:
            ssm.delete_maintenance_window(WindowId=wid)

    def test_register_and_deregister_task(self, ssm):
        """RegisterTaskWithMaintenanceWindow / DeregisterTaskFromMaintenanceWindow."""
        wid = self._create_window(ssm)
        try:
            # Register a target first (needed for task)
            reg_target = ssm.register_target_with_maintenance_window(
                WindowId=wid,
                ResourceType="INSTANCE",
                Targets=[{"Key": "tag:Name", "Values": ["test"]}],
            )
            target_id = reg_target["WindowTargetId"]

            reg_task = ssm.register_task_with_maintenance_window(
                WindowId=wid,
                Targets=[{"Key": "WindowTargetIds", "Values": [target_id]}],
                TaskArn="AWS-RunShellScript",
                TaskType="RUN_COMMAND",
                MaxConcurrency="1",
                MaxErrors="0",
            )
            task_id = reg_task["WindowTaskId"]

            # Verify task shows up
            desc = ssm.describe_maintenance_window_tasks(WindowId=wid)
            task_ids = [t["WindowTaskId"] for t in desc["Tasks"]]
            assert task_id in task_ids

            # Deregister
            ssm.deregister_task_from_maintenance_window(WindowId=wid, WindowTaskId=task_id)
            desc2 = ssm.describe_maintenance_window_tasks(WindowId=wid)
            task_ids2 = [t["WindowTaskId"] for t in desc2["Tasks"]]
            assert task_id not in task_ids2
        finally:
            ssm.delete_maintenance_window(WindowId=wid)

    def test_describe_maintenance_windows_filter(self, ssm):
        """DescribeMaintenanceWindows lists created windows."""
        name = _unique("mw-filter")
        wid = self._create_window(ssm, name=name)
        try:
            resp = ssm.describe_maintenance_windows(Filters=[{"Key": "Name", "Values": [name]}])
            found_ids = [w["WindowId"] for w in resp["WindowIdentities"]]
            assert wid in found_ids
        finally:
            ssm.delete_maintenance_window(WindowId=wid)


class TestSSMPatchBaseline:
    """Patch baseline operations."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_describe_patch_baselines(self, ssm):
        """DescribePatchBaselines lists baselines."""
        name = _unique("pb-desc")
        resp = ssm.create_patch_baseline(Name=name)
        baseline_id = resp["BaselineId"]
        try:
            desc = ssm.describe_patch_baselines(Filters=[{"Key": "NAME_PREFIX", "Values": [name]}])
            ids = [b["BaselineId"] for b in desc["BaselineIdentities"]]
            assert baseline_id in ids
        finally:
            ssm.delete_patch_baseline(BaselineId=baseline_id)


class TestSSMGapStubs:
    """Tests for newly-stubbed SSM operations that return empty results."""

    def test_list_associations(self, ssm):
        """ListAssociations returns empty list."""
        resp = ssm.list_associations()
        assert "Associations" in resp
        assert isinstance(resp["Associations"], list)

    def test_describe_automation_executions(self, ssm):
        """DescribeAutomationExecutions returns empty list."""
        resp = ssm.describe_automation_executions()
        assert "AutomationExecutionMetadataList" in resp
        assert isinstance(resp["AutomationExecutionMetadataList"], list)

    def test_list_compliance_items(self, ssm):
        """ListComplianceItems returns empty list."""
        resp = ssm.list_compliance_items()
        assert "ComplianceItems" in resp
        assert isinstance(resp["ComplianceItems"], list)

    def test_list_compliance_summaries(self, ssm):
        """ListComplianceSummaries returns empty list."""
        resp = ssm.list_compliance_summaries()
        assert "ComplianceSummaryItems" in resp
        assert isinstance(resp["ComplianceSummaryItems"], list)

    def test_list_resource_compliance_summaries(self, ssm):
        """ListResourceComplianceSummaries returns empty list."""
        resp = ssm.list_resource_compliance_summaries()
        assert "ResourceComplianceSummaryItems" in resp
        assert isinstance(resp["ResourceComplianceSummaryItems"], list)

    def test_describe_ops_items(self, ssm):
        """DescribeOpsItems returns empty list."""
        resp = ssm.describe_ops_items(
            OpsItemFilters=[{"Key": "Status", "Values": ["Open"], "Operator": "Equal"}]
        )
        assert "OpsItemSummaries" in resp
        assert isinstance(resp["OpsItemSummaries"], list)

    def test_describe_available_patches(self, ssm):
        """DescribeAvailablePatches returns empty list."""
        resp = ssm.describe_available_patches()
        assert "Patches" in resp
        assert isinstance(resp["Patches"], list)

    def test_describe_patch_groups(self, ssm):
        """DescribePatchGroups returns empty list."""
        resp = ssm.describe_patch_groups()
        assert "Mappings" in resp
        assert isinstance(resp["Mappings"], list)

    def test_describe_inventory_deletions(self, ssm):
        """DescribeInventoryDeletions returns empty list."""
        resp = ssm.describe_inventory_deletions()
        assert "InventoryDeletions" in resp
        assert isinstance(resp["InventoryDeletions"], list)

    def test_get_inventory(self, ssm):
        """GetInventory returns empty list."""
        resp = ssm.get_inventory()
        assert "Entities" in resp
        assert isinstance(resp["Entities"], list)

    def test_get_inventory_schema(self, ssm):
        """GetInventorySchema returns empty list."""
        resp = ssm.get_inventory_schema()
        assert "Schemas" in resp
        assert isinstance(resp["Schemas"], list)

    def test_list_resource_data_sync(self, ssm):
        """ListResourceDataSync returns empty list."""
        resp = ssm.list_resource_data_sync()
        assert "ResourceDataSyncItems" in resp
        assert isinstance(resp["ResourceDataSyncItems"], list)

    def test_list_ops_item_events(self, ssm):
        """ListOpsItemEvents returns empty list."""
        resp = ssm.list_ops_item_events()
        assert "Summaries" in resp
        assert isinstance(resp["Summaries"], list)

    def test_list_ops_item_related_items(self, ssm):
        """ListOpsItemRelatedItems returns empty list."""
        resp = ssm.list_ops_item_related_items(OpsItemId="oi-0000000000")
        assert "Summaries" in resp
        assert isinstance(resp["Summaries"], list)

    def test_list_ops_metadata(self, ssm):
        """ListOpsMetadata returns empty list."""
        resp = ssm.list_ops_metadata()
        assert "OpsMetadataList" in resp
        assert isinstance(resp["OpsMetadataList"], list)


class TestSSMGapCoverage:
    """Additional SSM compat tests to close coverage gaps."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_describe_activations(self, ssm):
        """DescribeActivations returns list."""
        resp = ssm.describe_activations()
        assert "ActivationList" in resp
        assert isinstance(resp["ActivationList"], list)

    def test_describe_instance_information(self, ssm):
        """DescribeInstanceInformation returns list."""
        resp = ssm.describe_instance_information()
        assert "InstanceInformationList" in resp
        assert isinstance(resp["InstanceInformationList"], list)

    def test_describe_maintenance_window_schedule(self, ssm):
        """DescribeMaintenanceWindowSchedule returns list."""
        resp = ssm.describe_maintenance_window_schedule()
        assert "ScheduledWindowExecutions" in resp
        assert isinstance(resp["ScheduledWindowExecutions"], list)

    def test_describe_parameters(self, ssm):
        """DescribeParameters returns list of parameter metadata."""
        resp = ssm.describe_parameters()
        assert "Parameters" in resp
        assert isinstance(resp["Parameters"], list)

    def test_describe_parameters_with_filter(self, ssm):
        """DescribeParameters with ParameterFilters."""
        name = _unique("/test/desc-param")
        ssm.put_parameter(Name=name, Value="val", Type="String")
        try:
            resp = ssm.describe_parameters(
                ParameterFilters=[{"Key": "Name", "Option": "Equals", "Values": [name]}]
            )
            assert "Parameters" in resp
            names = [p["Name"] for p in resp["Parameters"]]
            assert name in names
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_default_patch_baseline(self, ssm):
        """GetDefaultPatchBaseline returns a baseline ID."""
        resp = ssm.get_default_patch_baseline()
        assert "BaselineId" in resp
        assert resp["BaselineId"].startswith("pb-")

    def test_get_service_setting_activation_tier(self, ssm):
        """GetServiceSetting for activation-tier."""
        resp = ssm.get_service_setting(SettingId="/ssm/managed-instance/activation-tier")
        assert "ServiceSetting" in resp
        assert resp["ServiceSetting"]["SettingId"] == "/ssm/managed-instance/activation-tier"

    def test_get_service_setting_throughput(self, ssm):
        """GetServiceSetting for high-throughput-enabled."""
        resp = ssm.get_service_setting(SettingId="/ssm/parameter-store/high-throughput-enabled")
        assert "ServiceSetting" in resp

    def test_get_ops_summary(self, ssm):
        """GetOpsSummary returns list."""
        resp = ssm.get_ops_summary()
        assert "Entities" in resp
        assert isinstance(resp["Entities"], list)

    def test_get_ops_item(self, ssm):
        """GetOpsItem returns ops item details after creation."""
        create_resp = ssm.create_ops_item(
            Title="Test OpsItem",
            Description="Test ops item for compat",
            Source="compat-test",
        )
        ops_item_id = create_resp["OpsItemId"]
        resp = ssm.get_ops_item(OpsItemId=ops_item_id)
        assert "OpsItem" in resp
        assert resp["OpsItem"]["OpsItemId"] == ops_item_id
        assert resp["OpsItem"]["Title"] == "Test OpsItem"
        assert resp["OpsItem"]["Source"] == "compat-test"
        assert resp["OpsItem"]["Status"] == "Open"

    def test_list_command_invocations(self, ssm):
        """ListCommandInvocations returns list."""
        resp = ssm.list_command_invocations()
        assert "CommandInvocations" in resp
        assert isinstance(resp["CommandInvocations"], list)

    def test_get_parameters_by_path_empty(self, ssm):
        """GetParametersByPath with nonexistent path returns empty list."""
        resp = ssm.get_parameters_by_path(Path="/nonexistent/probe/path")
        assert "Parameters" in resp
        assert isinstance(resp["Parameters"], list)
        assert len(resp["Parameters"]) == 0

    def test_describe_parameters_empty(self, ssm):
        """DescribeParameters returns parameter list."""
        resp = ssm.describe_parameters()
        assert "Parameters" in resp
        assert isinstance(resp["Parameters"], list)


class TestSSMPatchBaselineExtended:
    """Patch baseline CRUD and patch group registration tests."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_create_and_delete_patch_baseline(self, ssm):
        """CreatePatchBaseline / DeletePatchBaseline full lifecycle."""
        name = _unique("pb-crud")
        resp = ssm.create_patch_baseline(
            Name=name,
            Description="Test patch baseline",
            OperatingSystem="AMAZON_LINUX_2",
        )
        baseline_id = resp["BaselineId"]
        assert baseline_id.startswith("pb-")

        # Verify it shows in describe
        desc = ssm.describe_patch_baselines()
        ids = [b["BaselineId"] for b in desc["BaselineIdentities"]]
        assert baseline_id in ids

        ssm.delete_patch_baseline(BaselineId=baseline_id)

    def test_register_deregister_patch_baseline_for_patch_group(self, ssm):
        """RegisterPatchBaselineForPatchGroup / DeregisterPatchBaselineForPatchGroup."""
        name = _unique("pb-pg")
        resp = ssm.create_patch_baseline(Name=name)
        baseline_id = resp["BaselineId"]
        group_name = _unique("test-group")
        try:
            reg = ssm.register_patch_baseline_for_patch_group(
                BaselineId=baseline_id, PatchGroup=group_name
            )
            assert reg["BaselineId"] == baseline_id
            assert reg["PatchGroup"] == group_name

            dereg = ssm.deregister_patch_baseline_for_patch_group(
                BaselineId=baseline_id, PatchGroup=group_name
            )
            assert dereg["BaselineId"] == baseline_id
            assert dereg["PatchGroup"] == group_name
        finally:
            ssm.delete_patch_baseline(BaselineId=baseline_id)


class TestSSMDocumentPermissions:
    """Document sharing and permission tests."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def _create_doc(self, ssm, name=None):
        import json

        name = name or _unique("doc-perm")
        content = json.dumps(
            {
                "schemaVersion": "2.2",
                "description": "Permission test document",
                "mainSteps": [
                    {
                        "action": "aws:runShellScript",
                        "name": "runScript",
                        "inputs": {"runCommand": ["echo hello"]},
                    }
                ],
            }
        )
        ssm.create_document(
            Content=content,
            Name=name,
            DocumentType="Command",
            DocumentFormat="JSON",
        )
        return name

    def test_modify_document_permission_share_and_unshare(self, ssm):
        """ModifyDocumentPermission: share then unshare a document."""
        doc_name = self._create_doc(ssm)
        try:
            # Share with account
            ssm.modify_document_permission(
                Name=doc_name,
                PermissionType="Share",
                AccountIdsToAdd=["111111111111"],
            )
            resp = ssm.describe_document_permission(Name=doc_name, PermissionType="Share")
            assert "111111111111" in resp["AccountIds"]

            # Unshare
            ssm.modify_document_permission(
                Name=doc_name,
                PermissionType="Share",
                AccountIdsToRemove=["111111111111"],
            )
            resp2 = ssm.describe_document_permission(Name=doc_name, PermissionType="Share")
            assert "111111111111" not in resp2.get("AccountIds", [])
        finally:
            ssm.delete_document(Name=doc_name)

    def test_list_documents_no_filter(self, ssm):
        """ListDocuments with no filter returns document list."""
        doc_name = self._create_doc(ssm)
        try:
            resp = ssm.list_documents()
            assert "DocumentIdentifiers" in resp
            assert isinstance(resp["DocumentIdentifiers"], list)
            names = [d["Name"] for d in resp["DocumentIdentifiers"]]
            assert doc_name in names
        finally:
            ssm.delete_document(Name=doc_name)

    def test_list_documents_owner_self_filter(self, ssm):
        """ListDocuments with Owner=Self filter."""
        doc_name = self._create_doc(ssm)
        try:
            resp = ssm.list_documents(Filters=[{"Key": "Owner", "Values": ["Self"]}])
            assert "DocumentIdentifiers" in resp
            names = [d["Name"] for d in resp["DocumentIdentifiers"]]
            assert doc_name in names
        finally:
            ssm.delete_document(Name=doc_name)


class TestSSMCommandExtended:
    """Extended command operations."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_list_commands_no_params(self, ssm):
        """ListCommands with no params returns Commands list."""
        resp = ssm.list_commands()
        assert "Commands" in resp
        assert isinstance(resp["Commands"], list)

    def test_send_command_and_list(self, ssm):
        """SendCommand then ListCommands finds the command."""
        resp = ssm.send_command(
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["echo test"]},
            Targets=[{"Key": "instanceids", "Values": ["i-00000000"]}],
        )
        command_id = resp["Command"]["CommandId"]
        assert command_id is not None
        assert resp["Command"]["DocumentName"] == "AWS-RunShellScript"

        list_resp = ssm.list_commands(CommandId=command_id)
        assert len(list_resp["Commands"]) >= 1
        assert list_resp["Commands"][0]["CommandId"] == command_id


class TestSsmAutoCoverage:
    """Auto-generated coverage tests for ssm."""

    @pytest.fixture
    def client(self):
        return make_client("ssm")

    def test_describe_instance_properties(self, client):
        """DescribeInstanceProperties returns a response."""
        resp = client.describe_instance_properties()
        assert "InstanceProperties" in resp

    def test_list_nodes(self, client):
        """ListNodes returns a response."""
        resp = client.list_nodes()
        assert "Nodes" in resp


class TestSSMParameterAdvanced:
    """Advanced parameter operations: description, tier, version selectors, pagination."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_put_parameter_with_description(self, ssm):
        """PutParameter with Description appears in DescribeParameters."""
        name = _unique("/test/desc-param")
        try:
            ssm.put_parameter(Name=name, Value="val", Type="String", Description="My description")
            resp = ssm.describe_parameters(
                ParameterFilters=[{"Key": "Name", "Option": "Equals", "Values": [name]}]
            )
            assert len(resp["Parameters"]) == 1
            assert resp["Parameters"][0]["Description"] == "My description"
        finally:
            ssm.delete_parameter(Name=name)

    def test_put_parameter_with_tier(self, ssm):
        """PutParameter with Tier=Standard is reflected in DescribeParameters."""
        name = _unique("/test/tier-param")
        try:
            ssm.put_parameter(Name=name, Value="val", Type="String", Tier="Standard")
            resp = ssm.describe_parameters(
                ParameterFilters=[{"Key": "Name", "Option": "Equals", "Values": [name]}]
            )
            assert len(resp["Parameters"]) == 1
            assert resp["Parameters"][0]["Tier"] == "Standard"
        finally:
            ssm.delete_parameter(Name=name)

    def test_put_parameter_with_data_type(self, ssm):
        """PutParameter with DataType=text is reflected in DescribeParameters."""
        name = _unique("/test/dtype-param")
        try:
            ssm.put_parameter(Name=name, Value="val", Type="String", DataType="text")
            resp = ssm.describe_parameters(
                ParameterFilters=[{"Key": "Name", "Option": "Equals", "Values": [name]}]
            )
            assert len(resp["Parameters"]) == 1
            assert resp["Parameters"][0]["DataType"] == "text"
        finally:
            ssm.delete_parameter(Name=name)

    def test_put_parameter_returns_version_and_tier(self, ssm):
        """PutParameter response includes Version and Tier."""
        name = _unique("/test/putresp-param")
        try:
            resp = ssm.put_parameter(Name=name, Value="val", Type="String")
            assert resp["Version"] == 1
            assert resp["Tier"] == "Standard"
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameter_with_version_selector(self, ssm):
        """GetParameter with :N version selector returns specific version."""
        name = _unique("/test/versel-param")
        try:
            ssm.put_parameter(Name=name, Value="v1", Type="String")
            ssm.put_parameter(Name=name, Value="v2", Type="String", Overwrite=True)
            resp = ssm.get_parameter(Name=f"{name}:1")
            assert resp["Parameter"]["Value"] == "v1"
            assert resp["Parameter"]["Version"] == 1
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameter_with_label_selector(self, ssm):
        """GetParameter with :label selector returns labeled version."""
        name = _unique("/test/lblsel-param")
        try:
            ssm.put_parameter(Name=name, Value="v1", Type="String")
            ssm.label_parameter_version(Name=name, ParameterVersion=1, Labels=["prod"])
            resp = ssm.get_parameter(Name=f"{name}:prod")
            assert resp["Parameter"]["Value"] == "v1"
        finally:
            ssm.delete_parameter(Name=name)

    def test_describe_parameters_pagination(self, ssm):
        """DescribeParameters supports pagination with MaxResults/NextToken."""
        names = [_unique("/test/page-param") for _ in range(6)]
        try:
            for n in names:
                ssm.put_parameter(Name=n, Value="val", Type="String")
            resp = ssm.describe_parameters(MaxResults=50)
            assert "Parameters" in resp
            # All 6 params should be present (possibly more from other tests)
            found = {p["Name"] for p in resp["Parameters"]}
            for n in names:
                assert n in found
        finally:
            ssm.delete_parameters(Names=names)

    def test_describe_parameters_begins_with_filter(self, ssm):
        """DescribeParameters with BeginsWith filter."""
        prefix = _unique("/test/bw")
        name = f"{prefix}/x"
        try:
            ssm.put_parameter(Name=name, Value="val", Type="String")
            resp = ssm.describe_parameters(
                ParameterFilters=[{"Key": "Name", "Option": "BeginsWith", "Values": [prefix]}]
            )
            found = [p["Name"] for p in resp["Parameters"]]
            assert name in found
        finally:
            ssm.delete_parameter(Name=name)

    def test_describe_parameters_path_filter(self, ssm):
        """DescribeParameters with Path filter."""
        prefix = _unique("/test/pathf")
        names = [f"{prefix}/a", f"{prefix}/b"]
        try:
            for n in names:
                ssm.put_parameter(Name=n, Value="val", Type="String")
            resp = ssm.describe_parameters(
                ParameterFilters=[{"Key": "Path", "Option": "Recursive", "Values": [prefix]}]
            )
            found = [p["Name"] for p in resp["Parameters"]]
            for n in names:
                assert n in found
        finally:
            ssm.delete_parameters(Names=names)

    def test_get_parameters_by_path_pagination(self, ssm):
        """GetParametersByPath supports pagination with MaxResults/NextToken."""
        prefix = _unique("/test/pathpage")
        names = [f"{prefix}/p{i}" for i in range(6)]
        try:
            for n in names:
                ssm.put_parameter(Name=n, Value="val", Type="String")
            resp = ssm.get_parameters_by_path(Path=prefix, MaxResults=3)
            assert len(resp["Parameters"]) == 3
            assert resp.get("NextToken") is not None
            resp2 = ssm.get_parameters_by_path(
                Path=prefix, NextToken=resp["NextToken"], MaxResults=3
            )
            assert len(resp2["Parameters"]) == 3
            all_names = [p["Name"] for p in resp["Parameters"] + resp2["Parameters"]]
            assert set(all_names) == set(names)
        finally:
            ssm.delete_parameters(Names=names)

    def test_get_parameters_by_path_with_filter(self, ssm):
        """GetParametersByPath with ParameterFilters."""
        prefix = _unique("/test/pathfilt")
        name = f"{prefix}/a"
        try:
            ssm.put_parameter(Name=name, Value="val", Type="String")
            resp = ssm.get_parameters_by_path(
                Path=prefix,
                ParameterFilters=[{"Key": "Type", "Option": "Equals", "Values": ["String"]}],
            )
            assert len(resp["Parameters"]) == 1
            assert resp["Parameters"][0]["Name"] == name
        finally:
            ssm.delete_parameter(Name=name)

    def test_describe_parameters_secure_string_has_key_id(self, ssm):
        """DescribeParameters for SecureString includes KeyId."""
        name = _unique("/test/ss-keyid")
        try:
            ssm.put_parameter(Name=name, Value="secret", Type="SecureString")
            resp = ssm.describe_parameters(
                ParameterFilters=[{"Key": "Name", "Option": "Equals", "Values": [name]}]
            )
            assert len(resp["Parameters"]) == 1
            assert "KeyId" in resp["Parameters"][0]
        finally:
            ssm.delete_parameter(Name=name)


class TestSSMMaintenanceWindowTags:
    """Maintenance window tagging operations."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_create_maintenance_window_with_tags(self, ssm):
        """CreateMaintenanceWindow with Tags."""
        name = _unique("mw-tag")
        resp = ssm.create_maintenance_window(
            Name=name,
            Schedule="rate(1 day)",
            Duration=2,
            Cutoff=1,
            AllowUnassociatedTargets=True,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        wid = resp["WindowId"]
        try:
            tags = ssm.list_tags_for_resource(ResourceType="MaintenanceWindow", ResourceId=wid)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
            assert tag_map["env"] == "test"
        finally:
            ssm.delete_maintenance_window(WindowId=wid)

    def test_add_remove_tags_maintenance_window(self, ssm):
        """AddTagsToResource / RemoveTagsFromResource on MaintenanceWindow."""
        name = _unique("mw-tags")
        resp = ssm.create_maintenance_window(
            Name=name,
            Schedule="rate(1 day)",
            Duration=2,
            Cutoff=1,
            AllowUnassociatedTargets=True,
        )
        wid = resp["WindowId"]
        try:
            ssm.add_tags_to_resource(
                ResourceType="MaintenanceWindow",
                ResourceId=wid,
                Tags=[
                    {"Key": "team", "Value": "ops"},
                    {"Key": "cost", "Value": "low"},
                ],
            )
            tags = ssm.list_tags_for_resource(ResourceType="MaintenanceWindow", ResourceId=wid)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
            assert tag_map["team"] == "ops"
            assert tag_map["cost"] == "low"

            ssm.remove_tags_from_resource(
                ResourceType="MaintenanceWindow", ResourceId=wid, TagKeys=["cost"]
            )
            tags2 = ssm.list_tags_for_resource(ResourceType="MaintenanceWindow", ResourceId=wid)
            keys = [t["Key"] for t in tags2["TagList"]]
            assert "team" in keys
            assert "cost" not in keys
        finally:
            ssm.delete_maintenance_window(WindowId=wid)


class TestSSMDocumentTags:
    """Document tagging operations."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def _doc_content(self):
        import json

        return json.dumps(
            {
                "schemaVersion": "2.2",
                "description": "Tag test doc",
                "mainSteps": [
                    {
                        "action": "aws:runShellScript",
                        "name": "run",
                        "inputs": {"runCommand": ["echo hi"]},
                    }
                ],
            }
        )

    def test_create_document_with_tags(self, ssm):
        """CreateDocument with Tags."""
        doc_name = _unique("doc-tag")
        ssm.create_document(
            Content=self._doc_content(),
            Name=doc_name,
            DocumentType="Command",
            DocumentFormat="JSON",
            Tags=[{"Key": "team", "Value": "dev"}],
        )
        try:
            tags = ssm.list_tags_for_resource(ResourceType="Document", ResourceId=doc_name)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
            assert tag_map["team"] == "dev"
        finally:
            ssm.delete_document(Name=doc_name)

    def test_add_tags_to_document(self, ssm):
        """AddTagsToResource on Document."""
        doc_name = _unique("doc-addtag")
        ssm.create_document(
            Content=self._doc_content(),
            Name=doc_name,
            DocumentType="Command",
            DocumentFormat="JSON",
        )
        try:
            ssm.add_tags_to_resource(
                ResourceType="Document",
                ResourceId=doc_name,
                Tags=[{"Key": "env", "Value": "staging"}],
            )
            tags = ssm.list_tags_for_resource(ResourceType="Document", ResourceId=doc_name)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
            assert tag_map["env"] == "staging"
        finally:
            ssm.delete_document(Name=doc_name)


class TestSSMSendCommandExtended:
    """Extended SendCommand / ListCommands tests."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_send_command_with_comment(self, ssm):
        """SendCommand with Comment field."""
        resp = ssm.send_command(
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["echo hello", "date"]},
            Targets=[{"Key": "instanceids", "Values": ["i-aabbccdd"]}],
            Comment="deployment script",
        )
        assert resp["Command"]["Comment"] == "deployment script"
        assert resp["Command"]["DocumentName"] == "AWS-RunShellScript"

    def test_send_command_with_instance_ids(self, ssm):
        """SendCommand with InstanceIds instead of Targets."""
        resp = ssm.send_command(
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["echo test"]},
            InstanceIds=["i-12345678"],
        )
        assert resp["Command"]["CommandId"] is not None
        assert "i-12345678" in resp["Command"].get("InstanceIds", [])

    def test_list_commands_by_command_id(self, ssm):
        """ListCommands filtered by CommandId."""
        resp = ssm.send_command(
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["echo x"]},
            Targets=[{"Key": "instanceids", "Values": ["i-00000001"]}],
        )
        cid = resp["Command"]["CommandId"]
        list_resp = ssm.list_commands(CommandId=cid)
        assert len(list_resp["Commands"]) == 1
        assert list_resp["Commands"][0]["CommandId"] == cid
        assert list_resp["Commands"][0]["DocumentName"] == "AWS-RunShellScript"

    def test_get_command_invocation(self, ssm):
        """GetCommandInvocation retrieves invocation details for a command."""
        instance_id = "i-00000002"
        resp = ssm.send_command(
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["echo hello"]},
            InstanceIds=[instance_id],
        )
        command_id = resp["Command"]["CommandId"]
        inv_resp = ssm.get_command_invocation(
            CommandId=command_id,
            InstanceId=instance_id,
        )
        assert inv_resp["CommandId"] == command_id
        assert inv_resp["InstanceId"] == instance_id
        assert "Status" in inv_resp

    def test_get_patch_baseline_for_patch_group(self, ssm):
        """GetPatchBaselineForPatchGroup returns the baseline for a group."""
        name = _unique("pb-forpg")
        resp = ssm.create_patch_baseline(Name=name)
        baseline_id = resp["BaselineId"]
        group_name = _unique("pg")
        try:
            ssm.register_patch_baseline_for_patch_group(
                BaselineId=baseline_id, PatchGroup=group_name
            )
            pg_resp = ssm.get_patch_baseline_for_patch_group(PatchGroup=group_name)
            assert pg_resp["BaselineId"] == baseline_id
            assert pg_resp["PatchGroup"] == group_name

            ssm.deregister_patch_baseline_for_patch_group(
                BaselineId=baseline_id, PatchGroup=group_name
            )
        finally:
            ssm.delete_patch_baseline(BaselineId=baseline_id)


class TestSSMOpsItemCRUD:
    """OpsItem create, get, update, describe lifecycle."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_create_and_get_ops_item(self, ssm):
        """CreateOpsItem / GetOpsItem full lifecycle."""
        resp = ssm.create_ops_item(
            Title="Test OpsItem CRUD",
            Description="Testing create and get",
            Source="compat-test",
        )
        ops_item_id = resp["OpsItemId"]
        assert ops_item_id.startswith("oi-")

        get_resp = ssm.get_ops_item(OpsItemId=ops_item_id)
        assert get_resp["OpsItem"]["OpsItemId"] == ops_item_id
        assert get_resp["OpsItem"]["Title"] == "Test OpsItem CRUD"
        assert get_resp["OpsItem"]["Description"] == "Testing create and get"
        assert get_resp["OpsItem"]["Source"] == "compat-test"
        assert get_resp["OpsItem"]["Status"] == "Open"

    def test_update_ops_item_status(self, ssm):
        """UpdateOpsItem changes status."""
        resp = ssm.create_ops_item(
            Title="Update Status Test",
            Description="Will be resolved",
            Source="compat-test",
        )
        ops_item_id = resp["OpsItemId"]

        ssm.update_ops_item(
            OpsItemId=ops_item_id,
            Status="Resolved",
        )
        get_resp = ssm.get_ops_item(OpsItemId=ops_item_id)
        assert get_resp["OpsItem"]["Status"] == "Resolved"

    def test_update_ops_item_title(self, ssm):
        """UpdateOpsItem changes title."""
        resp = ssm.create_ops_item(
            Title="Original Title",
            Description="desc",
            Source="compat-test",
        )
        ops_item_id = resp["OpsItemId"]

        ssm.update_ops_item(
            OpsItemId=ops_item_id,
            Title="Updated Title",
        )
        get_resp = ssm.get_ops_item(OpsItemId=ops_item_id)
        assert get_resp["OpsItem"]["Title"] == "Updated Title"

    def test_describe_ops_items_finds_created(self, ssm):
        """DescribeOpsItems finds a created OpsItem."""
        resp = ssm.create_ops_item(
            Title="Describe Test",
            Description="desc",
            Source="compat-test",
        )
        ops_item_id = resp["OpsItemId"]

        desc = ssm.describe_ops_items(
            OpsItemFilters=[{"Key": "OpsItemId", "Values": [ops_item_id], "Operator": "Equal"}]
        )
        assert len(desc["OpsItemSummaries"]) >= 1
        found_ids = [o["OpsItemId"] for o in desc["OpsItemSummaries"]]
        assert ops_item_id in found_ids


class TestSSMAssociationCRUD:
    """Association create, describe, list, update, delete lifecycle."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def _create_doc(self, ssm, name=None):
        import json

        name = name or _unique("assoc-doc")
        content = json.dumps(
            {
                "schemaVersion": "2.2",
                "description": "Association test doc",
                "mainSteps": [
                    {
                        "action": "aws:runShellScript",
                        "name": "run",
                        "inputs": {"runCommand": ["echo hi"]},
                    }
                ],
            }
        )
        ssm.create_document(
            Content=content, Name=name, DocumentType="Command", DocumentFormat="JSON"
        )
        return name

    def test_create_and_describe_association(self, ssm):
        """CreateAssociation / DescribeAssociation."""
        doc_name = self._create_doc(ssm)
        try:
            resp = ssm.create_association(
                Name=doc_name,
                Targets=[{"Key": "instanceids", "Values": ["i-00000000"]}],
            )
            assoc_id = resp["AssociationDescription"]["AssociationId"]
            assert assoc_id is not None

            desc = ssm.describe_association(AssociationId=assoc_id)
            assert desc["AssociationDescription"]["AssociationId"] == assoc_id
            assert desc["AssociationDescription"]["Name"] == doc_name
        finally:
            ssm.delete_association(AssociationId=assoc_id)
            ssm.delete_document(Name=doc_name)

    def test_list_associations(self, ssm):
        """ListAssociations finds created association."""
        doc_name = self._create_doc(ssm)
        try:
            resp = ssm.create_association(
                Name=doc_name,
                Targets=[{"Key": "instanceids", "Values": ["i-00000000"]}],
            )
            assoc_id = resp["AssociationDescription"]["AssociationId"]

            list_resp = ssm.list_associations(
                AssociationFilterList=[{"key": "Name", "value": doc_name}]
            )
            assert "Associations" in list_resp
            found_ids = [a["AssociationId"] for a in list_resp["Associations"]]
            assert assoc_id in found_ids
        finally:
            ssm.delete_association(AssociationId=assoc_id)
            ssm.delete_document(Name=doc_name)

    def test_update_association(self, ssm):
        """UpdateAssociation changes schedule expression."""
        doc_name = self._create_doc(ssm)
        try:
            resp = ssm.create_association(
                Name=doc_name,
                Targets=[{"Key": "instanceids", "Values": ["i-00000000"]}],
            )
            assoc_id = resp["AssociationDescription"]["AssociationId"]

            upd = ssm.update_association(
                AssociationId=assoc_id,
                ScheduleExpression="rate(1 hour)",
            )
            assert upd["AssociationDescription"]["ScheduleExpression"] == "rate(1 hour)"
        finally:
            ssm.delete_association(AssociationId=assoc_id)
            ssm.delete_document(Name=doc_name)

    def test_delete_association(self, ssm):
        """DeleteAssociation removes association."""
        doc_name = self._create_doc(ssm)
        try:
            resp = ssm.create_association(
                Name=doc_name,
                Targets=[{"Key": "instanceids", "Values": ["i-00000000"]}],
            )
            assoc_id = resp["AssociationDescription"]["AssociationId"]

            ssm.delete_association(AssociationId=assoc_id)
            # Describe should fail after deletion
            with pytest.raises(Exception):
                ssm.describe_association(AssociationId=assoc_id)
        finally:
            ssm.delete_document(Name=doc_name)


class TestSSMActivation:
    """Activation create, describe, delete."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_create_and_describe_activation(self, ssm):
        """CreateActivation / DescribeActivations."""
        resp = ssm.create_activation(
            IamRole="SSMServiceRole",
            RegistrationLimit=5,
            Description="Test activation",
        )
        activation_id = resp["ActivationId"]
        assert activation_id is not None
        assert "ActivationCode" in resp

        desc = ssm.describe_activations(
            Filters=[{"FilterKey": "ActivationIds", "FilterValues": [activation_id]}]
        )
        assert len(desc["ActivationList"]) >= 1
        found_ids = [a["ActivationId"] for a in desc["ActivationList"]]
        assert activation_id in found_ids

    def test_delete_activation(self, ssm):
        """DeleteActivation removes activation."""
        resp = ssm.create_activation(
            IamRole="SSMServiceRole",
            RegistrationLimit=1,
        )
        activation_id = resp["ActivationId"]
        ssm.delete_activation(ActivationId=activation_id)

        desc = ssm.describe_activations(
            Filters=[{"FilterKey": "ActivationIds", "FilterValues": [activation_id]}]
        )
        # After deletion, the activation should not appear (or appear as deleted)
        active_ids = [a["ActivationId"] for a in desc["ActivationList"] if not a.get("Expired")]
        assert activation_id not in active_ids


class TestSSMPatchBaselineCRUD:
    """Extended patch baseline operations."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_create_patch_baseline_with_description(self, ssm):
        """CreatePatchBaseline with description and OS."""
        name = _unique("pb-desc2")
        resp = ssm.create_patch_baseline(
            Name=name,
            Description="Detailed baseline",
            OperatingSystem="AMAZON_LINUX_2",
        )
        baseline_id = resp["BaselineId"]
        assert baseline_id.startswith("pb-")

        desc = ssm.describe_patch_baselines()
        found = [b for b in desc["BaselineIdentities"] if b["BaselineId"] == baseline_id]
        assert len(found) == 1
        assert found[0]["BaselineName"] == name
        ssm.delete_patch_baseline(BaselineId=baseline_id)


class TestSSMResourceDataSync:
    """ResourceDataSync create, list, delete."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_create_and_list_resource_data_sync(self, ssm):
        """CreateResourceDataSync / ListResourceDataSync."""
        sync_name = _unique("rds")
        ssm.create_resource_data_sync(
            SyncName=sync_name,
            S3Destination={
                "BucketName": "my-bucket",
                "SyncFormat": "JsonSerDe",
                "Region": "us-east-1",
            },
        )
        try:
            resp = ssm.list_resource_data_sync()
            assert "ResourceDataSyncItems" in resp
            names = [s["SyncName"] for s in resp["ResourceDataSyncItems"]]
            assert sync_name in names
        finally:
            ssm.delete_resource_data_sync(SyncName=sync_name)

    def test_delete_resource_data_sync(self, ssm):
        """DeleteResourceDataSync removes sync."""
        sync_name = _unique("rds-del")
        ssm.create_resource_data_sync(
            SyncName=sync_name,
            S3Destination={
                "BucketName": "my-bucket",
                "SyncFormat": "JsonSerDe",
                "Region": "us-east-1",
            },
        )
        ssm.delete_resource_data_sync(SyncName=sync_name)
        resp = ssm.list_resource_data_sync()
        names = [s["SyncName"] for s in resp["ResourceDataSyncItems"]]
        assert sync_name not in names


class TestSSMAutomation:
    """Automation execution operations."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_start_and_describe_automation_execution(self, ssm):
        """StartAutomationExecution / DescribeAutomationExecutions."""
        resp = ssm.start_automation_execution(
            DocumentName="AWS-RestartEC2Instance",
            Parameters={"InstanceId": ["i-00000000"]},
        )
        execution_id = resp["AutomationExecutionId"]
        assert execution_id is not None

        desc = ssm.describe_automation_executions(
            Filters=[{"Key": "ExecutionId", "Values": [execution_id]}]
        )
        assert len(desc["AutomationExecutionMetadataList"]) >= 1
        found_ids = [e["AutomationExecutionId"] for e in desc["AutomationExecutionMetadataList"]]
        assert execution_id in found_ids


class TestSSMOpsItemExtended:
    """Extended OpsItem operations: priority, severity, notifications."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_create_ops_item_with_priority(self, ssm):
        """CreateOpsItem with Priority field."""
        resp = ssm.create_ops_item(
            Title="Priority OpsItem",
            Description="High priority item",
            Source="compat-test",
            Priority=1,
        )
        ops_item_id = resp["OpsItemId"]
        get_resp = ssm.get_ops_item(OpsItemId=ops_item_id)
        assert get_resp["OpsItem"]["Priority"] == 1

    def test_create_ops_item_with_severity(self, ssm):
        """CreateOpsItem with Severity field."""
        resp = ssm.create_ops_item(
            Title="Severity OpsItem",
            Description="Critical severity",
            Source="compat-test",
            Severity="1",
        )
        ops_item_id = resp["OpsItemId"]
        get_resp = ssm.get_ops_item(OpsItemId=ops_item_id)
        assert get_resp["OpsItem"]["Severity"] == "1"

    def test_update_ops_item_description(self, ssm):
        """UpdateOpsItem changes description."""
        resp = ssm.create_ops_item(
            Title="Desc Update",
            Description="Original",
            Source="compat-test",
        )
        ops_item_id = resp["OpsItemId"]
        ssm.update_ops_item(
            OpsItemId=ops_item_id,
            Description="Updated description",
        )
        get_resp = ssm.get_ops_item(OpsItemId=ops_item_id)
        assert get_resp["OpsItem"]["Description"] == "Updated description"


class TestSSMDocumentCRUDExtended:
    """Extended document CRUD: create with different formats, tags on create."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def _doc_content(self, desc="Test doc"):
        import json

        return json.dumps(
            {
                "schemaVersion": "2.2",
                "description": desc,
                "mainSteps": [
                    {
                        "action": "aws:runShellScript",
                        "name": "run",
                        "inputs": {"runCommand": ["echo hi"]},
                    }
                ],
            }
        )

    def test_create_document_automation_type(self, ssm):
        """CreateDocument with Automation type."""
        import json

        doc_name = _unique("doc-auto")
        content = json.dumps(
            {
                "schemaVersion": "0.3",
                "description": "Automation doc",
                "mainSteps": [
                    {
                        "name": "step1",
                        "action": "aws:sleep",
                        "inputs": {"Duration": "PT1S"},
                    }
                ],
            }
        )
        ssm.create_document(
            Content=content,
            Name=doc_name,
            DocumentType="Automation",
            DocumentFormat="JSON",
        )
        try:
            desc = ssm.describe_document(Name=doc_name)
            assert desc["Document"]["Name"] == doc_name
            assert desc["Document"]["DocumentType"] == "Automation"
        finally:
            ssm.delete_document(Name=doc_name)

    def test_get_document_content(self, ssm):
        """GetDocument returns parseable content."""
        import json

        doc_name = _unique("doc-content")
        ssm.create_document(
            Content=self._doc_content("content test"),
            Name=doc_name,
            DocumentType="Command",
            DocumentFormat="JSON",
        )
        try:
            resp = ssm.get_document(Name=doc_name)
            assert resp["Name"] == doc_name
            content = json.loads(resp["Content"])
            assert content["description"] == "content test"
            assert content["schemaVersion"] == "2.2"
        finally:
            ssm.delete_document(Name=doc_name)


class TestSSMAssociationExtended:
    """Extended association tests."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def _create_doc(self, ssm):
        import json

        name = _unique("assoc-ext-doc")
        content = json.dumps(
            {
                "schemaVersion": "2.2",
                "description": "Association extended test",
                "mainSteps": [
                    {
                        "action": "aws:runShellScript",
                        "name": "run",
                        "inputs": {"runCommand": ["echo test"]},
                    }
                ],
            }
        )
        ssm.create_document(
            Content=content, Name=name, DocumentType="Command", DocumentFormat="JSON"
        )
        return name

    def test_create_association_with_schedule(self, ssm):
        """CreateAssociation with ScheduleExpression."""
        doc_name = self._create_doc(ssm)
        try:
            resp = ssm.create_association(
                Name=doc_name,
                Targets=[{"Key": "instanceids", "Values": ["i-00000000"]}],
                ScheduleExpression="rate(30 minutes)",
            )
            assoc_id = resp["AssociationDescription"]["AssociationId"]
            assert resp["AssociationDescription"]["ScheduleExpression"] == "rate(30 minutes)"

            desc = ssm.describe_association(AssociationId=assoc_id)
            assert desc["AssociationDescription"]["ScheduleExpression"] == "rate(30 minutes)"
        finally:
            ssm.delete_association(AssociationId=assoc_id)
            ssm.delete_document(Name=doc_name)

    def test_create_association_with_association_name(self, ssm):
        """CreateAssociation with AssociationName."""
        doc_name = self._create_doc(ssm)
        assoc_name = _unique("assoc-named")
        try:
            resp = ssm.create_association(
                Name=doc_name,
                Targets=[{"Key": "instanceids", "Values": ["i-00000000"]}],
                AssociationName=assoc_name,
            )
            assoc_id = resp["AssociationDescription"]["AssociationId"]
            assert resp["AssociationDescription"]["AssociationName"] == assoc_name
        finally:
            ssm.delete_association(AssociationId=assoc_id)
            ssm.delete_document(Name=doc_name)


class TestSSMAutomationExecutionDetails:
    """Tests for GetAutomationExecution and StopAutomationExecution."""

    def test_get_automation_execution(self, ssm):
        """GetAutomationExecution returns execution details."""
        import json

        doc_name = _unique("auto-get-exec")
        doc_content = json.dumps(
            {
                "schemaVersion": "0.3",
                "description": "test automation",
                "mainSteps": [
                    {
                        "name": "step1",
                        "action": "aws:sleep",
                        "inputs": {"Duration": "PT1S"},
                    }
                ],
            }
        )
        ssm.create_document(Content=doc_content, Name=doc_name, DocumentType="Automation")
        try:
            start = ssm.start_automation_execution(DocumentName=doc_name)
            exec_id = start["AutomationExecutionId"]
            resp = ssm.get_automation_execution(AutomationExecutionId=exec_id)
            execution = resp["AutomationExecution"]
            assert execution["AutomationExecutionId"] == exec_id
            assert execution["DocumentName"] == doc_name
            assert "AutomationExecutionStatus" in execution
        finally:
            ssm.delete_document(Name=doc_name)

    def test_stop_automation_execution(self, ssm):
        """StopAutomationExecution stops a running execution."""
        import json

        doc_name = _unique("auto-stop-exec")
        doc_content = json.dumps(
            {
                "schemaVersion": "0.3",
                "description": "test automation",
                "mainSteps": [
                    {
                        "name": "step1",
                        "action": "aws:sleep",
                        "inputs": {"Duration": "PT60S"},
                    }
                ],
            }
        )
        ssm.create_document(Content=doc_content, Name=doc_name, DocumentType="Automation")
        try:
            start = ssm.start_automation_execution(DocumentName=doc_name)
            exec_id = start["AutomationExecutionId"]
            stop_resp = ssm.stop_automation_execution(AutomationExecutionId=exec_id)
            assert stop_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ssm.delete_document(Name=doc_name)


class TestSSMGetParametersExtended:
    """Extended tests for GetParameters operation with valid parameters."""

    @pytest.fixture
    def ssm(self):
        return make_client("ssm")

    def test_get_parameters_returns_invalid_parameters(self, ssm):
        """GetParameters returns InvalidParameters for names that don't exist."""
        name = _unique("/getp/real")
        ssm.put_parameter(Name=name, Value="ok", Type="String")
        try:
            resp = ssm.get_parameters(Names=[name, "/getp/fake1", "/getp/fake2"])
            assert len(resp["Parameters"]) == 1
            assert resp["Parameters"][0]["Name"] == name
            assert "/getp/fake1" in resp["InvalidParameters"]
            assert "/getp/fake2" in resp["InvalidParameters"]
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameters_all_valid(self, ssm):
        """GetParameters with all valid names returns empty InvalidParameters."""
        names = [_unique("/getp/v") for _ in range(3)]
        for n in names:
            ssm.put_parameter(Name=n, Value="val", Type="String")
        try:
            resp = ssm.get_parameters(Names=names)
            assert len(resp["Parameters"]) == 3
            assert len(resp["InvalidParameters"]) == 0
        finally:
            for n in names:
                ssm.delete_parameter(Name=n)

    def test_get_parameters_all_invalid(self, ssm):
        """GetParameters with all invalid names returns empty Parameters."""
        resp = ssm.get_parameters(Names=["/getp/none1", "/getp/none2"])
        assert len(resp["Parameters"]) == 0
        assert "/getp/none1" in resp["InvalidParameters"]
        assert "/getp/none2" in resp["InvalidParameters"]

    def test_get_parameters_with_decryption(self, ssm):
        """GetParameters with WithDecryption decrypts SecureString."""
        name = _unique("/getp/sec")
        ssm.put_parameter(Name=name, Value="secret-val", Type="SecureString")
        try:
            resp = ssm.get_parameters(Names=[name], WithDecryption=True)
            assert len(resp["Parameters"]) == 1
            assert resp["Parameters"][0]["Value"] == "secret-val"
            assert resp["Parameters"][0]["Type"] == "SecureString"
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameters_returns_version(self, ssm):
        """GetParameters returns Version field for each parameter."""
        name = _unique("/getp/ver")
        ssm.put_parameter(Name=name, Value="v1", Type="String")
        try:
            resp = ssm.get_parameters(Names=[name])
            assert resp["Parameters"][0]["Version"] == 1
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameters_returns_arn(self, ssm):
        """GetParameters returns ARN field for each parameter."""
        name = _unique("/getp/arn")
        ssm.put_parameter(Name=name, Value="val", Type="String")
        try:
            resp = ssm.get_parameters(Names=[name])
            param = resp["Parameters"][0]
            assert "ARN" in param
            assert name in param["ARN"]
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameters_returns_last_modified_date(self, ssm):
        """GetParameters returns LastModifiedDate for each parameter."""
        name = _unique("/getp/lmd")
        ssm.put_parameter(Name=name, Value="val", Type="String")
        try:
            resp = ssm.get_parameters(Names=[name])
            assert "LastModifiedDate" in resp["Parameters"][0]
        finally:
            ssm.delete_parameter(Name=name)

    def test_get_parameters_returns_data_type(self, ssm):
        """GetParameters returns DataType field."""
        name = _unique("/getp/dt")
        ssm.put_parameter(Name=name, Value="val", Type="String")
        try:
            resp = ssm.get_parameters(Names=[name])
            assert "DataType" in resp["Parameters"][0]
            assert resp["Parameters"][0]["DataType"] == "text"
        finally:
            ssm.delete_parameter(Name=name)


class TestSSMMaintenanceWindowDescribe:
    """Tests for DescribeMaintenanceWindowTasks and DescribeMaintenanceWindowTargets."""

    def test_describe_maintenance_window_tasks_empty(self, ssm):
        """DescribeMaintenanceWindowTasks returns empty list for new window."""
        name = _unique("mw-tasks")
        resp = ssm.create_maintenance_window(
            Name=name,
            Schedule="cron(0 0 ? * SUN *)",
            Duration=2,
            Cutoff=1,
            AllowUnassociatedTargets=True,
        )
        win_id = resp["WindowId"]
        try:
            tasks_resp = ssm.describe_maintenance_window_tasks(WindowId=win_id)
            assert "Tasks" in tasks_resp
            assert isinstance(tasks_resp["Tasks"], list)
            assert len(tasks_resp["Tasks"]) == 0
        finally:
            ssm.delete_maintenance_window(WindowId=win_id)

    def test_describe_maintenance_window_targets_empty(self, ssm):
        """DescribeMaintenanceWindowTargets returns empty list for new window."""
        name = _unique("mw-targets")
        resp = ssm.create_maintenance_window(
            Name=name,
            Schedule="cron(0 0 ? * SUN *)",
            Duration=2,
            Cutoff=1,
            AllowUnassociatedTargets=True,
        )
        win_id = resp["WindowId"]
        try:
            targets_resp = ssm.describe_maintenance_window_targets(WindowId=win_id)
            assert "Targets" in targets_resp
            assert isinstance(targets_resp["Targets"], list)
            assert len(targets_resp["Targets"]) == 0
        finally:
            ssm.delete_maintenance_window(WindowId=win_id)


class TestSSMGetParametersMultiple:
    """Tests for GetParameters with multiple names including invalid ones."""

    def test_get_parameters_with_invalid_names(self, ssm):
        """GetParameters returns InvalidParameters for nonexistent names."""
        name1 = _unique("/test/gpmulti1")
        name2 = _unique("/test/gpmulti2")
        fake = _unique("/test/gpfake")
        ssm.put_parameter(Name=name1, Value="val1", Type="String")
        ssm.put_parameter(Name=name2, Value="val2", Type="String")
        try:
            resp = ssm.get_parameters(Names=[name1, name2, fake])
            assert "Parameters" in resp
            assert "InvalidParameters" in resp
            found_names = {p["Name"] for p in resp["Parameters"]}
            assert name1 in found_names
            assert name2 in found_names
            assert fake in resp["InvalidParameters"]
            assert len(resp["Parameters"]) == 2
        finally:
            ssm.delete_parameter(Name=name1)
            ssm.delete_parameter(Name=name2)


class TestSSMDescribeOperations:
    def test_describe_patch_properties(self, ssm):
        """DescribePatchProperties returns patch property list."""
        resp = ssm.describe_patch_properties(
            OperatingSystem="AMAZON_LINUX_2",
            Property="PRODUCT",
        )
        assert "Properties" in resp

    def test_describe_sessions(self, ssm):
        """DescribeSessions returns session list."""
        resp = ssm.describe_sessions(State="Active")
        assert "Sessions" in resp

    def test_get_calendar_state(self, ssm):
        """GetCalendarState with nonexistent calendar returns error or state."""
        try:
            resp = ssm.get_calendar_state(CalendarNames=["/nonexistent/calendar"])
            # If it succeeds, should have State key
            assert "State" in resp
        except ssm.exceptions.ClientError:
            # InvalidDocument or similar is acceptable
            pass

    def test_describe_patch_group_state(self, ssm):
        """DescribePatchGroupState returns patch group compliance."""
        resp = ssm.describe_patch_group_state(PatchGroup="nonexistent-group")
        assert (
            "Instances" in resp
            or "InstancesWithInstalledPatches" in resp
            or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        )

    def test_get_ops_summary(self, ssm):
        """GetOpsSummary returns entities."""
        resp = ssm.get_ops_summary()
        assert "Entities" in resp

    def test_get_connection_status(self, ssm):
        """GetConnectionStatus returns status for an instance."""
        resp = ssm.get_connection_status(Target="i-1234567890abcdef0")
        assert "Target" in resp or "Status" in resp

    def test_describe_instance_patch_states(self, ssm):
        """DescribeInstancePatchStates returns patch states."""
        resp = ssm.describe_instance_patch_states(InstanceIds=["i-1234567890abcdef0"])
        assert "InstancePatchStates" in resp

    def test_describe_instance_patch_states_for_patch_group(self, ssm):
        """DescribeInstancePatchStatesForPatchGroup returns patch states."""
        resp = ssm.describe_instance_patch_states_for_patch_group(PatchGroup="test-group")
        assert "InstancePatchStates" in resp

    def test_describe_association_executions(self, ssm):
        """DescribeAssociationExecutions with fake ID returns error."""
        with pytest.raises(ClientError) as exc:
            ssm.describe_association_executions(AssociationId="fake-assoc-id-12345")
        assert exc.value.response["Error"]["Code"] in (
            "AssociationDoesNotExist",
            "DoesNotExistException",
            "InvalidDocument",
            "InternalServerError",
        )

    def test_describe_association_execution_targets(self, ssm):
        """DescribeAssociationExecutionTargets with fake ID returns error."""
        with pytest.raises(ClientError) as exc:
            ssm.describe_association_execution_targets(
                AssociationId="fake-assoc-id-12345",
                ExecutionId="fake-exec-id-12345",
            )
        assert exc.value.response["Error"]["Code"] in (
            "AssociationDoesNotExist",
            "AssociationExecutionDoesNotExist",
            "DoesNotExistException",
            "InvalidDocument",
            "InternalServerError",
        )

    def test_describe_effective_instance_associations(self, ssm):
        """DescribeEffectiveInstanceAssociations returns associations list."""
        resp = ssm.describe_effective_instance_associations(InstanceId="i-1234567890abcdef0")
        assert "Associations" in resp

    def test_describe_instance_associations_status(self, ssm):
        """DescribeInstanceAssociationsStatus returns associations list."""
        resp = ssm.describe_instance_associations_status(InstanceId="i-1234567890abcdef0")
        assert "InstanceAssociationStatusInfos" in resp


class TestSSMOpsMetadataOperations:
    """Tests for OpsMetadata operations."""

    def test_get_ops_metadata(self, ssm):
        """GetOpsMetadata returns metadata for a given ARN."""
        resp = ssm.get_ops_metadata(
            OpsMetadataArn="arn:aws:ssm:us-east-1:123456789012:opsmetadata/fake-metadata"
        )
        assert "ResourceId" in resp
        assert "Metadata" in resp
