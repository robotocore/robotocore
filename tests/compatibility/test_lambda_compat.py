"""Lambda compatibility tests — CRUD and invocation."""

import io
import json
import uuid
import zipfile

import pytest

from tests.compatibility.conftest import make_client


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def role():
    iam = make_client("iam")
    trust = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    iam.create_role(RoleName="lambda-compat-role", AssumeRolePolicyDocument=trust)
    yield "arn:aws:iam::123456789012:role/lambda-compat-role"
    iam.delete_role(RoleName="lambda-compat-role")


class TestLambdaCRUDOperations:
    def test_create_function(self, lam, role):
        code = _make_zip("def handler(event, ctx): return 'ok'")
        response = lam.create_function(
            FunctionName="test-func",
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        assert response["FunctionName"] == "test-func"
        lam.delete_function(FunctionName="test-func")

    def test_list_functions(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName="list-func",
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.list_functions()
        names = [f["FunctionName"] for f in response["Functions"]]
        assert "list-func" in names
        lam.delete_function(FunctionName="list-func")

    def test_get_function(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName="get-func",
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.get_function(FunctionName="get-func")
        assert response["Configuration"]["FunctionName"] == "get-func"
        lam.delete_function(FunctionName="get-func")

    def test_update_function_configuration(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName="update-func",
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.update_function_configuration(
            FunctionName="update-func",
            Description="updated description",
        )
        assert response["Description"] == "updated description"
        lam.delete_function(FunctionName="update-func")

    def test_get_function_configuration(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName="getconfig-func",
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Description="config test",
            MemorySize=256,
            Timeout=30,
        )
        response = lam.get_function_configuration(FunctionName="getconfig-func")
        assert response["FunctionName"] == "getconfig-func"
        assert response["Runtime"] == "python3.12"
        assert response["Handler"] == "lambda_function.handler"
        assert response["Description"] == "config test"
        assert response["MemorySize"] == 256
        assert response["Timeout"] == 30
        lam.delete_function(FunctionName="getconfig-func")

    def test_delete_function(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName="delete-func",
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.delete_function(FunctionName="delete-func")
        response = lam.list_functions()
        names = [f["FunctionName"] for f in response["Functions"]]
        assert "delete-func" not in names


class TestLambdaInvocation:
    def test_invoke_simple(self, lam, role):
        """Test basic synchronous invocation."""
        code = _make_zip('def handler(event, ctx): return {"statusCode": 200, "body": "hello"}')
        fname = f"invoke-simple-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.invoke(FunctionName=fname)
        payload = json.loads(response["Payload"].read())
        assert payload["statusCode"] == 200
        assert payload["body"] == "hello"
        lam.delete_function(FunctionName=fname)

    def test_invoke_with_payload(self, lam, role):
        """Test invocation with input event."""
        code = _make_zip('def handler(event, ctx): return {"echo": event.get("msg", "none")}')
        fname = f"invoke-payload-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.invoke(
            FunctionName=fname,
            Payload=json.dumps({"msg": "test123"}),
        )
        payload = json.loads(response["Payload"].read())
        assert payload["echo"] == "test123"
        lam.delete_function(FunctionName=fname)

    def test_invoke_error(self, lam, role):
        """Test invocation that raises an error."""
        code = _make_zip('def handler(event, ctx): raise ValueError("boom")')
        fname = f"invoke-error-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.invoke(FunctionName=fname)
        assert response.get("FunctionError") is not None
        payload = json.loads(response["Payload"].read())
        assert "boom" in payload.get("errorMessage", "")
        lam.delete_function(FunctionName=fname)

    def test_invoke_event_type(self, lam, role):
        """Test async (Event) invocation returns 202."""
        code = _make_zip('def handler(event, ctx): return "ok"')
        fname = f"invoke-event-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.invoke(
            FunctionName=fname,
            InvocationType="Event",
        )
        assert response["StatusCode"] == 202
        lam.delete_function(FunctionName=fname)

    def test_invoke_dry_run(self, lam, role):
        """Test DryRun invocation returns 204."""
        code = _make_zip('def handler(event, ctx): return "ok"')
        fname = f"invoke-dry-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.invoke(
            FunctionName=fname,
            InvocationType="DryRun",
        )
        assert response["StatusCode"] == 204
        lam.delete_function(FunctionName=fname)


class TestLambdaAdvanced:
    def test_environment_variables(self, lam, role):
        """Test function with environment variables."""
        code = _make_zip(
            'import os\ndef handler(e, c): return {"env_val": os.environ.get("MY_VAR", "missing")}'
        )
        fname = f"env-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"MY_VAR": "hello123"}},
        )
        response = lam.invoke(FunctionName=fname)
        payload = json.loads(response["Payload"].read())
        assert payload["env_val"] == "hello123"
        lam.delete_function(FunctionName=fname)

    def test_publish_version(self, lam, role):
        """Test publishing a function version."""
        code = _make_zip('def handler(e, c): return "v1"')
        fname = f"version-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.publish_version(FunctionName=fname)
        assert response["Version"] == "1"
        lam.delete_function(FunctionName=fname)

    def test_list_versions(self, lam, role):
        """Test listing function versions."""
        code = _make_zip('def handler(e, c): return "v1"')
        fname = f"listver-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)
        response = lam.list_versions_by_function(FunctionName=fname)
        versions = [v["Version"] for v in response["Versions"]]
        assert "$LATEST" in versions
        assert "1" in versions
        lam.delete_function(FunctionName=fname)

    def test_function_tags(self, lam, role):
        """Test function tagging."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"tag-func-{uuid.uuid4().hex[:8]}"
        response = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Tags={"env": "test", "project": "robotocore"},
        )
        arn = response["FunctionArn"]
        tags = lam.list_tags(Resource=arn)["Tags"]
        assert tags["env"] == "test"
        assert tags["project"] == "robotocore"
        lam.delete_function(FunctionName=fname)


class TestLambdaConcurrency:
    def test_put_get_delete_function_concurrency(self, lam, role):
        """Test setting, reading, and removing reserved concurrency."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"concurrency-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Put concurrency
        response = lam.put_function_concurrency(
            FunctionName=fname,
            ReservedConcurrentExecutions=10,
        )
        assert response["ReservedConcurrentExecutions"] == 10

        # Get concurrency
        response = lam.get_function_concurrency(FunctionName=fname)
        assert response["ReservedConcurrentExecutions"] == 10

        # Delete concurrency
        lam.delete_function_concurrency(FunctionName=fname)

        # After deletion, get should return empty (no ReservedConcurrentExecutions key)
        response = lam.get_function_concurrency(FunctionName=fname)
        assert (
            "ReservedConcurrentExecutions" not in response
            or response.get("ReservedConcurrentExecutions") is None
        )

        lam.delete_function(FunctionName=fname)

    def test_get_account_settings(self, lam, role):
        """Test retrieving Lambda account settings."""
        response = lam.get_account_settings()

        # Verify the response has the expected top-level keys
        assert "AccountLimit" in response
        assert "AccountUsage" in response

        limits = response["AccountLimit"]
        assert "TotalCodeSize" in limits
        assert "CodeSizeUnzipped" in limits
        assert "CodeSizeZipped" in limits
        assert "ConcurrentExecutions" in limits
        assert "UnreservedConcurrentExecutions" in limits
        assert limits["ConcurrentExecutions"] >= 1

        usage = response["AccountUsage"]
        assert "TotalCodeSize" in usage
        assert "FunctionCount" in usage
        assert usage["FunctionCount"] >= 0


class TestLambdaLayers:
    def test_publish_layer_version(self, lam):
        """Test creating a Lambda layer."""
        layer_code = io.BytesIO()
        with zipfile.ZipFile(layer_code, "w") as zf:
            zf.writestr("python/helper.py", 'LAYER_VALUE = "from-layer"')
        layer_bytes = layer_code.getvalue()

        layer_name = f"test-layer-{uuid.uuid4().hex[:8]}"
        response = lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": layer_bytes},
            CompatibleRuntimes=["python3.12"],
        )
        assert response["Version"] == 1
        assert layer_name in response["LayerVersionArn"]

    def test_list_layers(self, lam):
        """Test listing Lambda layers."""
        layer_code = io.BytesIO()
        with zipfile.ZipFile(layer_code, "w") as zf:
            zf.writestr("python/util.py", "X = 1")

        layer_name = f"list-layer-{uuid.uuid4().hex[:8]}"
        lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": layer_code.getvalue()},
            CompatibleRuntimes=["python3.12"],
        )
        response = lam.list_layers()
        layer_names = [layer["LayerName"] for layer in response["Layers"]]
        assert layer_name in layer_names

    def test_list_layer_versions(self, lam):
        """Test listing versions of a layer."""
        layer_code = io.BytesIO()
        with zipfile.ZipFile(layer_code, "w") as zf:
            zf.writestr("python/mod.py", "Y = 2")

        layer_name = f"ver-layer-{uuid.uuid4().hex[:8]}"
        lam.publish_layer_version(LayerName=layer_name, Content={"ZipFile": layer_code.getvalue()})
        lam.publish_layer_version(LayerName=layer_name, Content={"ZipFile": layer_code.getvalue()})

        response = lam.list_layer_versions(LayerName=layer_name)
        versions = [v["Version"] for v in response["LayerVersions"]]
        assert 1 in versions
        assert 2 in versions


class TestLambdaVersions:
    @pytest.mark.xfail(reason="Not yet implemented")
    def test_publish_multiple_versions(self, lam, role):
        """Test publishing multiple versions of a function."""
        code = _make_zip('def handler(e, c): return "v1"')
        fname = f"multi-ver-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        v1 = lam.publish_version(FunctionName=fname, Description="first version")
        assert v1["Version"] == "1"
        assert v1["Description"] == "first version"

        v2 = lam.publish_version(FunctionName=fname, Description="second version")
        assert v2["Version"] == "2"
        assert v2["Description"] == "second version"

        lam.delete_function(FunctionName=fname)

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_list_versions_by_function(self, lam, role):
        """Test listing all versions including $LATEST."""
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"listver2-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)
        lam.publish_version(FunctionName=fname)

        response = lam.list_versions_by_function(FunctionName=fname)
        versions = [v["Version"] for v in response["Versions"]]
        assert "$LATEST" in versions
        assert "1" in versions
        assert "2" in versions
        assert len(versions) == 3

        lam.delete_function(FunctionName=fname)


class TestLambdaAliases:
    def test_create_and_get_alias(self, lam, role):
        """Test creating and retrieving a function alias."""
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"alias-cg-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)

        alias = lam.create_alias(
            FunctionName=fname,
            Name="prod",
            FunctionVersion="1",
            Description="production alias",
        )
        assert alias["Name"] == "prod"
        assert alias["FunctionVersion"] == "1"
        assert alias["Description"] == "production alias"

        got = lam.get_alias(FunctionName=fname, Name="prod")
        assert got["Name"] == "prod"
        assert got["FunctionVersion"] == "1"

        lam.delete_alias(FunctionName=fname, Name="prod")
        lam.delete_function(FunctionName=fname)

    def test_list_aliases(self, lam, role):
        """Test listing aliases for a function."""
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"alias-list-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)

        lam.create_alias(FunctionName=fname, Name="dev", FunctionVersion="1")
        lam.create_alias(FunctionName=fname, Name="staging", FunctionVersion="1")

        response = lam.list_aliases(FunctionName=fname)
        alias_names = [a["Name"] for a in response["Aliases"]]
        assert "dev" in alias_names
        assert "staging" in alias_names

        lam.delete_alias(FunctionName=fname, Name="dev")
        lam.delete_alias(FunctionName=fname, Name="staging")
        lam.delete_function(FunctionName=fname)

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_update_alias(self, lam, role):
        """Test updating an alias to point to a new version."""
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"alias-upd-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)
        lam.publish_version(FunctionName=fname)

        lam.create_alias(FunctionName=fname, Name="live", FunctionVersion="1")

        updated = lam.update_alias(
            FunctionName=fname,
            Name="live",
            FunctionVersion="2",
            Description="updated to v2",
        )
        assert updated["FunctionVersion"] == "2"
        assert updated["Description"] == "updated to v2"

        lam.delete_alias(FunctionName=fname, Name="live")
        lam.delete_function(FunctionName=fname)

    def test_delete_alias(self, lam, role):
        """Test deleting a function alias."""
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"alias-del-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)
        lam.create_alias(FunctionName=fname, Name="temp", FunctionVersion="1")
        lam.delete_alias(FunctionName=fname, Name="temp")

        response = lam.list_aliases(FunctionName=fname)
        alias_names = [a["Name"] for a in response["Aliases"]]
        assert "temp" not in alias_names

        lam.delete_function(FunctionName=fname)


class TestLambdaConfigurationUpdates:
    def test_update_handler(self, lam, role):
        """Test updating function handler."""
        code = _make_zip(
            'def handler(e, c): return "old"\ndef new_handler(e, c): return "new"'
        )
        fname = f"cfg-handler-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        response = lam.update_function_configuration(
            FunctionName=fname,
            Handler="lambda_function.new_handler",
        )
        assert response["Handler"] == "lambda_function.new_handler"
        lam.delete_function(FunctionName=fname)

    def test_update_timeout_and_memory(self, lam, role):
        """Test updating function timeout and memory size."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"cfg-tm-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Timeout=3,
            MemorySize=128,
        )
        response = lam.update_function_configuration(
            FunctionName=fname,
            Timeout=60,
            MemorySize=512,
        )
        assert response["Timeout"] == 60
        assert response["MemorySize"] == 512

        # Verify the change persists
        config = lam.get_function_configuration(FunctionName=fname)
        assert config["Timeout"] == 60
        assert config["MemorySize"] == 512

        lam.delete_function(FunctionName=fname)

    def test_update_environment_variables(self, lam, role):
        """Test updating function environment variables."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"cfg-env-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"KEY1": "val1"}},
        )
        response = lam.update_function_configuration(
            FunctionName=fname,
            Environment={"Variables": {"KEY1": "updated", "KEY2": "new"}},
        )
        env_vars = response["Environment"]["Variables"]
        assert env_vars["KEY1"] == "updated"
        assert env_vars["KEY2"] == "new"

        lam.delete_function(FunctionName=fname)


class TestLambdaTags:
    def test_tag_and_untag_resource(self, lam, role):
        """Test adding and removing tags from a function."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"tag-rw-{uuid.uuid4().hex[:8]}"
        response = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        arn = response["FunctionArn"]

        # Add tags
        lam.tag_resource(Resource=arn, Tags={"team": "platform", "cost-center": "eng"})
        tags = lam.list_tags(Resource=arn)["Tags"]
        assert tags["team"] == "platform"
        assert tags["cost-center"] == "eng"

        # Remove one tag
        lam.untag_resource(Resource=arn, TagKeys=["cost-center"])
        tags = lam.list_tags(Resource=arn)["Tags"]
        assert "team" in tags
        assert "cost-center" not in tags

        lam.delete_function(FunctionName=fname)

    def test_create_function_with_tags_then_add_more(self, lam, role):
        """Test creating a function with tags, then adding more."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"tag-add-{uuid.uuid4().hex[:8]}"
        response = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Tags={"initial": "tag"},
        )
        arn = response["FunctionArn"]

        lam.tag_resource(Resource=arn, Tags={"added": "later"})
        tags = lam.list_tags(Resource=arn)["Tags"]
        assert tags["initial"] == "tag"
        assert tags["added"] == "later"

        lam.delete_function(FunctionName=fname)


class TestLambdaFunctionUrl:
    def test_create_get_delete_function_url(self, lam, role):
        """Test function URL config lifecycle."""
        code = _make_zip('def handler(e, c): return {"statusCode": 200, "body": "hi"}')
        fname = f"furl-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Create function URL
        url_config = lam.create_function_url_config(
            FunctionName=fname,
            AuthType="NONE",
        )
        assert "FunctionUrl" in url_config
        assert url_config["AuthType"] == "NONE"

        # Get function URL
        got = lam.get_function_url_config(FunctionName=fname)
        assert got["AuthType"] == "NONE"
        assert "FunctionUrl" in got

        # Delete function URL
        lam.delete_function_url_config(FunctionName=fname)

        # After deletion, get should fail
        with pytest.raises(lam.exceptions.ResourceNotFoundException):
            lam.get_function_url_config(FunctionName=fname)

        lam.delete_function(FunctionName=fname)

    def test_function_url_with_cors(self, lam, role):
        """Test function URL with CORS configuration."""
        code = _make_zip('def handler(e, c): return {"statusCode": 200}')
        fname = f"furl-cors-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        url_config = lam.create_function_url_config(
            FunctionName=fname,
            AuthType="NONE",
            Cors={
                "AllowOrigins": ["https://example.com"],
                "AllowMethods": ["GET", "POST"],
                "AllowHeaders": ["Content-Type"],
                "MaxAge": 3600,
            },
        )
        assert url_config["Cors"]["AllowOrigins"] == ["https://example.com"]
        assert url_config["Cors"]["AllowMethods"] == ["GET", "POST"]

        lam.delete_function_url_config(FunctionName=fname)
        lam.delete_function(FunctionName=fname)


class TestLambdaPermissions:
    def test_add_and_get_policy(self, lam, role):
        """Test adding a permission and retrieving the policy."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"perm-add-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        lam.add_permission(
            FunctionName=fname,
            StatementId="allow-s3",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn="arn:aws:s3:::my-bucket",
        )

        policy_response = lam.get_policy(FunctionName=fname)
        policy = json.loads(policy_response["Policy"])
        statements = policy["Statement"]
        assert len(statements) >= 1
        stmt = next(s for s in statements if s["Sid"] == "allow-s3")
        assert stmt["Effect"] == "Allow"
        assert stmt["Action"] == "lambda:InvokeFunction"

        lam.delete_function(FunctionName=fname)

    def test_remove_permission(self, lam, role):
        """Test removing a permission from a function policy."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"perm-rm-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        lam.add_permission(
            FunctionName=fname,
            StatementId="stmt-to-remove",
            Action="lambda:InvokeFunction",
            Principal="sns.amazonaws.com",
        )

        lam.remove_permission(FunctionName=fname, StatementId="stmt-to-remove")

        # After removal, get_policy should either fail or return empty statements
        try:
            policy_response = lam.get_policy(FunctionName=fname)
            policy = json.loads(policy_response["Policy"])
            sids = [s["Sid"] for s in policy["Statement"]]
            assert "stmt-to-remove" not in sids
        except lam.exceptions.ResourceNotFoundException:
            pass  # No policy left is also valid

        lam.delete_function(FunctionName=fname)

    def test_add_multiple_permissions(self, lam, role):
        """Test adding multiple permission statements."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"perm-multi-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        lam.add_permission(
            FunctionName=fname,
            StatementId="allow-s3",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
        )
        lam.add_permission(
            FunctionName=fname,
            StatementId="allow-sns",
            Action="lambda:InvokeFunction",
            Principal="sns.amazonaws.com",
        )

        policy_response = lam.get_policy(FunctionName=fname)
        policy = json.loads(policy_response["Policy"])
        sids = [s["Sid"] for s in policy["Statement"]]
        assert "allow-s3" in sids
        assert "allow-sns" in sids

        lam.delete_function(FunctionName=fname)
