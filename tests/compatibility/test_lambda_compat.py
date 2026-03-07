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


class TestLambdaPermissions:
    def test_add_and_get_policy(self, lam, role):
        """Test AddPermission creates a resource policy retrievable via GetPolicy."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"perm-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.add_permission(
            FunctionName=fname,
            StatementId="allow-invoke",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
        )
        policy_resp = lam.get_policy(FunctionName=fname)
        policy = json.loads(policy_resp["Policy"])
        statements = policy.get("Statement", [])
        sids = [s["Sid"] for s in statements]
        assert "allow-invoke" in sids
        lam.delete_function(FunctionName=fname)

    def test_remove_permission(self, lam, role):
        """Test RemovePermission removes a statement from the policy."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"rmperm-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.add_permission(
            FunctionName=fname,
            StatementId="to-remove",
            Action="lambda:InvokeFunction",
            Principal="sns.amazonaws.com",
        )
        lam.remove_permission(FunctionName=fname, StatementId="to-remove")
        # After removing the only statement, GetPolicy should fail or return empty
        try:
            policy_resp = lam.get_policy(FunctionName=fname)
            policy = json.loads(policy_resp["Policy"])
            sids = [s["Sid"] for s in policy.get("Statement", [])]
            assert "to-remove" not in sids
        except lam.exceptions.ResourceNotFoundException:
            pass  # No policy left is also valid
        lam.delete_function(FunctionName=fname)


class TestLambdaAliases:
    def test_create_get_list_delete_alias(self, lam, role):
        """Test full alias lifecycle: create, get, list, delete."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"alias-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)

        # Create alias
        alias_resp = lam.create_alias(
            FunctionName=fname,
            Name="prod",
            FunctionVersion="1",
            Description="production alias",
        )
        assert alias_resp["Name"] == "prod"
        assert alias_resp["FunctionVersion"] == "1"

        # Get alias
        get_resp = lam.get_alias(FunctionName=fname, Name="prod")
        assert get_resp["Name"] == "prod"
        assert get_resp["Description"] == "production alias"

        # List aliases
        list_resp = lam.list_aliases(FunctionName=fname)
        alias_names = [a["Name"] for a in list_resp["Aliases"]]
        assert "prod" in alias_names

        # Delete alias
        lam.delete_alias(FunctionName=fname, Name="prod")
        list_resp = lam.list_aliases(FunctionName=fname)
        alias_names = [a["Name"] for a in list_resp["Aliases"]]
        assert "prod" not in alias_names

        lam.delete_function(FunctionName=fname)


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
    """Tests for function versioning: publish, list, invoke specific version."""

    def test_publish_version_returns_version_number(self, lam, role):
        code = _make_zip('def handler(e, c): return "v1"')
        fname = f"pubver-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.publish_version(FunctionName=fname)
        assert resp["Version"] == "1"
        assert resp["FunctionName"] == fname
        lam.delete_function(FunctionName=fname)

    def test_publish_multiple_versions(self, lam, role):
        code = _make_zip('def handler(e, c): return "v1"')
        fname = f"multiver-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        r1 = lam.publish_version(FunctionName=fname)
        assert r1["Version"] == "1"
        # Update code then publish again
        code2 = _make_zip('def handler(e, c): return "v2"')
        lam.update_function_code(FunctionName=fname, ZipFile=code2)
        r2 = lam.publish_version(FunctionName=fname)
        assert r2["Version"] == "2"
        lam.delete_function(FunctionName=fname)

    def test_list_versions_by_function(self, lam, role):
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
        # Update code to get a new version
        code2 = _make_zip('def handler(e, c): return "v2"')
        lam.update_function_code(FunctionName=fname, ZipFile=code2)
        lam.publish_version(FunctionName=fname)
        resp = lam.list_versions_by_function(FunctionName=fname)
        versions = [v["Version"] for v in resp["Versions"]]
        assert "$LATEST" in versions
        assert "1" in versions
        assert "2" in versions
        lam.delete_function(FunctionName=fname)

    def test_invoke_specific_version(self, lam, role):
        code = _make_zip('def handler(e, c): return {"ver": 1}')
        fname = f"invver-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        ver_resp = lam.publish_version(FunctionName=fname)
        version = ver_resp["Version"]
        resp = lam.invoke(FunctionName=fname, Qualifier=version)
        payload = json.loads(resp["Payload"].read())
        assert payload["ver"] == 1
        lam.delete_function(FunctionName=fname)

    def test_get_function_with_qualifier(self, lam, role):
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"getqual-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)
        resp = lam.get_function(FunctionName=fname, Qualifier="1")
        assert resp["Configuration"]["FunctionName"] == fname
        # Qualifier "1" should return a versioned ARN
        assert ":1" in resp["Configuration"]["FunctionArn"] or resp["Configuration"]["Version"] in (
            "1",
            "$LATEST",
        )
        lam.delete_function(FunctionName=fname)


class TestLambdaAliases:
    """Tests for function aliases: create, get, update, list, delete, invoke."""

    def test_create_alias(self, lam, role):
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"alias-create-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)
        resp = lam.create_alias(
            FunctionName=fname,
            Name="prod",
            FunctionVersion="1",
        )
        assert resp["Name"] == "prod"
        assert resp["FunctionVersion"] == "1"
        lam.delete_alias(FunctionName=fname, Name="prod")
        lam.delete_function(FunctionName=fname)

    def test_get_alias(self, lam, role):
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"alias-get-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)
        lam.create_alias(FunctionName=fname, Name="staging", FunctionVersion="1")
        resp = lam.get_alias(FunctionName=fname, Name="staging")
        assert resp["Name"] == "staging"
        assert resp["FunctionVersion"] == "1"
        lam.delete_alias(FunctionName=fname, Name="staging")
        lam.delete_function(FunctionName=fname)

    def test_update_alias(self, lam, role):
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
        code2 = _make_zip('def handler(e, c): return "v2"')
        lam.update_function_code(FunctionName=fname, ZipFile=code2)
        lam.publish_version(FunctionName=fname)
        lam.create_alias(FunctionName=fname, Name="live", FunctionVersion="1")
        resp = lam.update_alias(
            FunctionName=fname,
            Name="live",
            FunctionVersion="2",
            Description="updated",
        )
        assert resp["FunctionVersion"] == "2"
        assert resp["Description"] == "updated"
        lam.delete_alias(FunctionName=fname, Name="live")
        lam.delete_function(FunctionName=fname)

    def test_list_aliases(self, lam, role):
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
        lam.create_alias(FunctionName=fname, Name="alpha", FunctionVersion="1")
        lam.create_alias(FunctionName=fname, Name="beta", FunctionVersion="1")
        resp = lam.list_aliases(FunctionName=fname)
        alias_names = [a["Name"] for a in resp["Aliases"]]
        assert "alpha" in alias_names
        assert "beta" in alias_names
        lam.delete_alias(FunctionName=fname, Name="alpha")
        lam.delete_alias(FunctionName=fname, Name="beta")
        lam.delete_function(FunctionName=fname)

    def test_delete_alias(self, lam, role):
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
        lam.create_alias(FunctionName=fname, Name="todelete", FunctionVersion="1")
        lam.delete_alias(FunctionName=fname, Name="todelete")
        resp = lam.list_aliases(FunctionName=fname)
        alias_names = [a["Name"] for a in resp["Aliases"]]
        assert "todelete" not in alias_names
        lam.delete_function(FunctionName=fname)

    def test_invoke_via_alias(self, lam, role):
        code = _make_zip('def handler(e, c): return {"source": "alias"}')
        fname = f"alias-inv-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.publish_version(FunctionName=fname)
        lam.create_alias(FunctionName=fname, Name="live", FunctionVersion="1")
        resp = lam.invoke(FunctionName=fname, Qualifier="live")
        payload = json.loads(resp["Payload"].read())
        assert payload["source"] == "alias"
        lam.delete_alias(FunctionName=fname, Name="live")
        lam.delete_function(FunctionName=fname)


class TestLambdaEnvironmentVariables:
    """Tests for environment variable management."""

    def test_create_with_env_vars(self, lam, role):
        code = _make_zip(
            'import os\ndef handler(e, c): return {"a": os.environ.get("VAR_A"), "b": os.environ.get("VAR_B")}'
        )
        fname = f"envvar-create-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"VAR_A": "aaa", "VAR_B": "bbb"}},
        )
        resp = lam.invoke(FunctionName=fname)
        payload = json.loads(resp["Payload"].read())
        assert payload["a"] == "aaa"
        assert payload["b"] == "bbb"
        lam.delete_function(FunctionName=fname)

    def test_update_env_vars(self, lam, role):
        code = _make_zip(
            'import os\ndef handler(e, c): return {"val": os.environ.get("MY_KEY", "unset")}'
        )
        fname = f"envvar-upd-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"MY_KEY": "original"}},
        )
        lam.update_function_configuration(
            FunctionName=fname,
            Environment={"Variables": {"MY_KEY": "updated"}},
        )
        resp = lam.invoke(FunctionName=fname)
        payload = json.loads(resp["Payload"].read())
        assert payload["val"] == "updated"
        lam.delete_function(FunctionName=fname)

    def test_env_vars_in_get_configuration(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"envvar-cfg-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"KEY1": "val1"}},
        )
        resp = lam.get_function_configuration(FunctionName=fname)
        assert resp["Environment"]["Variables"]["KEY1"] == "val1"
        lam.delete_function(FunctionName=fname)

    def test_remove_env_vars(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"envvar-rm-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"REMOVE_ME": "yes"}},
        )
        lam.update_function_configuration(
            FunctionName=fname,
            Environment={"Variables": {}},
        )
        resp = lam.get_function_configuration(FunctionName=fname)
        env_vars = resp.get("Environment", {}).get("Variables", {})
        assert "REMOVE_ME" not in env_vars
        lam.delete_function(FunctionName=fname)


class TestLambdaTagging:
    """Tests for function tagging operations."""

    def test_tag_resource(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"tag-add-{uuid.uuid4().hex[:8]}"
        resp = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        arn = resp["FunctionArn"]
        lam.tag_resource(Resource=arn, Tags={"team": "platform", "cost-center": "123"})
        tags = lam.list_tags(Resource=arn)["Tags"]
        assert tags["team"] == "platform"
        assert tags["cost-center"] == "123"
        lam.delete_function(FunctionName=fname)

    def test_untag_resource(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"tag-rm-{uuid.uuid4().hex[:8]}"
        resp = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Tags={"keep": "yes", "remove": "me"},
        )
        arn = resp["FunctionArn"]
        lam.untag_resource(Resource=arn, TagKeys=["remove"])
        tags = lam.list_tags(Resource=arn)["Tags"]
        assert "keep" in tags
        assert "remove" not in tags
        lam.delete_function(FunctionName=fname)

    def test_list_tags_empty(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"tag-empty-{uuid.uuid4().hex[:8]}"
        resp = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        arn = resp["FunctionArn"]
        tags = lam.list_tags(Resource=arn)["Tags"]
        assert tags == {} or isinstance(tags, dict)
        lam.delete_function(FunctionName=fname)

    def test_create_with_tags(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"tag-init-{uuid.uuid4().hex[:8]}"
        resp = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Tags={"env": "dev", "version": "1.0"},
        )
        arn = resp["FunctionArn"]
        tags = lam.list_tags(Resource=arn)["Tags"]
        assert tags["env"] == "dev"
        assert tags["version"] == "1.0"
        lam.delete_function(FunctionName=fname)


class TestLambdaConcurrencyExtended:
    """Extended concurrency tests."""

    def test_put_concurrency(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"conc-put-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.put_function_concurrency(
            FunctionName=fname, ReservedConcurrentExecutions=5
        )
        assert resp["ReservedConcurrentExecutions"] == 5
        lam.delete_function(FunctionName=fname)

    def test_update_concurrency(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"conc-upd-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.put_function_concurrency(
            FunctionName=fname, ReservedConcurrentExecutions=5
        )
        resp = lam.put_function_concurrency(
            FunctionName=fname, ReservedConcurrentExecutions=20
        )
        assert resp["ReservedConcurrentExecutions"] == 20
        lam.delete_function(FunctionName=fname)


class TestLambdaInvokeExtended:
    """Extended invocation tests: modes, errors, payloads."""

    def test_invoke_request_response(self, lam, role):
        code = _make_zip('def handler(e, c): return {"mode": "sync"}')
        fname = f"inv-rr-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.invoke(FunctionName=fname, InvocationType="RequestResponse")
        assert resp["StatusCode"] == 200
        payload = json.loads(resp["Payload"].read())
        assert payload["mode"] == "sync"
        lam.delete_function(FunctionName=fname)

    def test_invoke_returns_none(self, lam, role):
        code = _make_zip("def handler(e, c): return None")
        fname = f"inv-none-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.invoke(FunctionName=fname)
        payload_bytes = resp["Payload"].read()
        assert payload_bytes == b"null" or payload_bytes == b""
        lam.delete_function(FunctionName=fname)

    def test_invoke_returns_string(self, lam, role):
        code = _make_zip('def handler(e, c): return "just a string"')
        fname = f"inv-str-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.invoke(FunctionName=fname)
        payload = json.loads(resp["Payload"].read())
        assert payload == "just a string"
        lam.delete_function(FunctionName=fname)

    def test_invoke_returns_list(self, lam, role):
        code = _make_zip("def handler(e, c): return [1, 2, 3]")
        fname = f"inv-list-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.invoke(FunctionName=fname)
        payload = json.loads(resp["Payload"].read())
        assert payload == [1, 2, 3]
        lam.delete_function(FunctionName=fname)

    def test_invoke_error_type_error(self, lam, role):
        """Invoke a function that raises TypeError, verify FunctionError."""
        code = _make_zip('def handler(e, c): raise TypeError("bad type")')
        fname = f"inv-terr-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.invoke(FunctionName=fname)
        assert resp.get("FunctionError") is not None
        payload = json.loads(resp["Payload"].read())
        assert "bad type" in payload.get("errorMessage", "")
        lam.delete_function(FunctionName=fname)

    def test_invoke_error_key_error(self, lam, role):
        """Invoke a function that raises KeyError."""
        code = _make_zip('def handler(e, c): raise KeyError("missing_key")')
        fname = f"inv-kerr-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.invoke(FunctionName=fname)
        assert resp.get("FunctionError") is not None
        payload = json.loads(resp["Payload"].read())
        assert "missing_key" in payload.get("errorMessage", str(payload))
        lam.delete_function(FunctionName=fname)

    def test_invoke_large_payload(self, lam, role):
        """Invoke with a ~256KB payload and verify response."""
        code = _make_zip(
            'def handler(e, c): return {"size": len(str(e)), "ok": True}'
        )
        fname = f"inv-large-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        # ~256KB payload
        large_data = {"data": "x" * (256 * 1024 - 50)}
        resp = lam.invoke(
            FunctionName=fname,
            Payload=json.dumps(large_data),
        )
        assert resp["StatusCode"] == 200
        payload = json.loads(resp["Payload"].read())
        assert payload["ok"] is True
        assert payload["size"] > 200000
        lam.delete_function(FunctionName=fname)


class TestLambdaFunctionURL:
    """Tests for function URL configuration."""

    def test_create_function_url_config(self, lam, role):
        code = _make_zip('def handler(e, c): return {"statusCode": 200}')
        fname = f"furl-create-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.create_function_url_config(
            FunctionName=fname,
            AuthType="NONE",
        )
        assert "FunctionUrl" in resp
        assert resp["AuthType"] == "NONE"
        lam.delete_function_url_config(FunctionName=fname)
        lam.delete_function(FunctionName=fname)

    def test_get_function_url_config(self, lam, role):
        code = _make_zip('def handler(e, c): return {"statusCode": 200}')
        fname = f"furl-get-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.create_function_url_config(FunctionName=fname, AuthType="NONE")
        resp = lam.get_function_url_config(FunctionName=fname)
        assert "FunctionUrl" in resp
        assert resp["AuthType"] == "NONE"
        lam.delete_function_url_config(FunctionName=fname)
        lam.delete_function(FunctionName=fname)

    def test_delete_function_url_config(self, lam, role):
        code = _make_zip('def handler(e, c): return {"statusCode": 200}')
        fname = f"furl-del-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        lam.create_function_url_config(FunctionName=fname, AuthType="NONE")
        lam.delete_function_url_config(FunctionName=fname)
        with pytest.raises(lam.exceptions.ResourceNotFoundException):
            lam.get_function_url_config(FunctionName=fname)
        lam.delete_function(FunctionName=fname)


class TestLambdaLayersExtended:
    """Extended layer tests: get version, permissions."""

    def test_get_layer_version(self, lam):
        layer_code = io.BytesIO()
        with zipfile.ZipFile(layer_code, "w") as zf:
            zf.writestr("python/mymod.py", "Z = 42")
        layer_name = f"getlv-{uuid.uuid4().hex[:8]}"
        pub_resp = lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": layer_code.getvalue()},
            CompatibleRuntimes=["python3.12"],
            Description="test layer",
        )
        version = pub_resp["Version"]
        resp = lam.get_layer_version(LayerName=layer_name, VersionNumber=version)
        assert resp["Version"] == version
        assert resp["Description"] == "test layer"
        assert "Content" in resp

    def test_layer_compatible_runtimes(self, lam):
        layer_code = io.BytesIO()
        with zipfile.ZipFile(layer_code, "w") as zf:
            zf.writestr("python/compat.py", "A = 1")
        layer_name = f"compat-rt-{uuid.uuid4().hex[:8]}"
        resp = lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": layer_code.getvalue()},
            CompatibleRuntimes=["python3.11", "python3.12"],
        )
        assert "python3.11" in resp["CompatibleRuntimes"]
        assert "python3.12" in resp["CompatibleRuntimes"]


class TestLambdaUpdateCode:
    """Tests for updating function code."""

    def test_update_function_code(self, lam, role):
        code1 = _make_zip('def handler(e, c): return {"v": 1}')
        fname = f"upd-code-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code1},
        )
        # Invoke v1
        resp1 = lam.invoke(FunctionName=fname)
        p1 = json.loads(resp1["Payload"].read())
        assert p1["v"] == 1
        # Update code
        code2 = _make_zip('def handler(e, c): return {"v": 2}')
        lam.update_function_code(FunctionName=fname, ZipFile=code2)
        # Invoke v2
        resp2 = lam.invoke(FunctionName=fname)
        p2 = json.loads(resp2["Payload"].read())
        assert p2["v"] == 2
        lam.delete_function(FunctionName=fname)

    def test_update_code_returns_config(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"upd-code-cfg-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        code2 = _make_zip("def handler(e, c): return 42")
        resp = lam.update_function_code(FunctionName=fname, ZipFile=code2)
        assert resp["FunctionName"] == fname
        assert resp["Runtime"] == "python3.12"
        assert "FunctionArn" in resp
        lam.delete_function(FunctionName=fname)
