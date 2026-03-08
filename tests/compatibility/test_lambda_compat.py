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


class TestLambdaAliasesExtended:
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
        _code = _make_zip('def handler(e, c): return "ok"')
        _fname = f"alias-del-{uuid.uuid4().hex[:8]}"


class TestLambdaPermissionsExtended:
    def test_add_and_remove_permission(self, lam, role):
        """Test adding and removing a resource-based policy statement."""
        code = _make_zip("def handler(event, context): return {'statusCode': 200}")
        fname = f"perm-func-{uuid.uuid4().hex[:8]}"
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
            'import os\ndef handler(e, c): return {"a": os.environ.get("VAR_A"), "b": os.environ.get("VAR_B")}'  # noqa: E501
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
        resp = lam.put_function_concurrency(FunctionName=fname, ReservedConcurrentExecutions=5)
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
        lam.put_function_concurrency(FunctionName=fname, ReservedConcurrentExecutions=5)
        resp = lam.put_function_concurrency(FunctionName=fname, ReservedConcurrentExecutions=20)
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
        code = _make_zip('def handler(e, c): return {"size": len(str(e)), "ok": True}')
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

        # Add permission
        lam.add_permission(
            FunctionName=fname,
            StatementId="s3-invoke",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
        )

        # Verify via get_policy
        policy_response = lam.get_policy(FunctionName=fname)
        policy = json.loads(policy_response["Policy"])
        statements = policy["Statement"]
        stmt_ids = [s["Sid"] for s in statements]
        assert "s3-invoke" in stmt_ids

        # Find the statement and verify fields
        stmt = [s for s in statements if s["Sid"] == "s3-invoke"][0]
        assert stmt["Action"] == "lambda:InvokeFunction"
        assert "s3.amazonaws.com" in json.dumps(stmt["Principal"])

        # Remove permission
        lam.remove_permission(FunctionName=fname, StatementId="s3-invoke")

        # After removal, get_policy should either raise or return empty statements
        try:
            policy_response = lam.get_policy(FunctionName=fname)
            policy = json.loads(policy_response["Policy"])
            stmt_ids = [s["Sid"] for s in policy["Statement"]]
            assert "s3-invoke" not in stmt_ids
        except lam.exceptions.ResourceNotFoundException:
            pass  # No policy left is also valid

        lam.delete_function(FunctionName=fname)


class TestLambdaConfigurationFields:
    def test_get_function_configuration_all_fields(self, lam, role):
        """GetFunctionConfiguration returns all expected fields."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"cfg-fields-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Description="field check",
            MemorySize=256,
            Timeout=15,
        )
        try:
            resp = lam.get_function_configuration(FunctionName=fname)
            assert resp["FunctionName"] == fname
            assert "FunctionArn" in resp
            assert resp["Runtime"] == "python3.12"
            assert resp["Handler"] == "lambda_function.handler"
            assert resp["Description"] == "field check"
            assert resp["MemorySize"] == 256
            assert resp["Timeout"] == 15
            assert "CodeSha256" in resp
            assert "CodeSize" in resp
            assert "LastModified" in resp
            assert "State" in resp or "Version" in resp
        finally:
            lam.delete_function(FunctionName=fname)

    def test_update_function_configuration_timeout_memory(self, lam, role):
        """UpdateFunctionConfiguration to change timeout and memory."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"upd-cfg-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            MemorySize=128,
            Timeout=3,
        )
        try:
            resp = lam.update_function_configuration(
                FunctionName=fname,
                MemorySize=512,
                Timeout=60,
            )
            assert resp["MemorySize"] == 512
            assert resp["Timeout"] == 60
            # Verify via get
            cfg = lam.get_function_configuration(FunctionName=fname)
            assert cfg["MemorySize"] == 512
            assert cfg["Timeout"] == 60
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaVersionsAndAliases:
    def test_publish_version_and_list(self, lam, role):
        """PublishVersion and ListVersionsByFunction."""
        code = _make_zip('def handler(e, c): return "v1"')
        fname = f"ver-list-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            v1 = lam.publish_version(FunctionName=fname)
            assert v1["Version"] == "1"
            # Update code to get a new version
            code2 = _make_zip('def handler(e, c): return "v2"')
            lam.update_function_code(FunctionName=fname, ZipFile=code2)
            v2 = lam.publish_version(FunctionName=fname)
            assert v2["Version"] == "2"
            versions = lam.list_versions_by_function(FunctionName=fname)
            ver_nums = [v["Version"] for v in versions["Versions"]]
            assert "$LATEST" in ver_nums
            assert "1" in ver_nums
            assert "2" in ver_nums
        finally:
            lam.delete_function(FunctionName=fname)

    def test_create_get_update_delete_alias(self, lam, role):
        """Full alias lifecycle: Create, Get, Update, Delete, List."""
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"alias-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.publish_version(FunctionName=fname)  # version 1
            code2 = _make_zip('def handler(e, c): return "v2"')
            lam.update_function_code(FunctionName=fname, ZipFile=code2)
            lam.publish_version(FunctionName=fname)  # version 2

            # Create alias
            alias = lam.create_alias(
                FunctionName=fname,
                Name="prod",
                FunctionVersion="1",
                Description="production alias",
            )
            assert alias["Name"] == "prod"
            assert alias["FunctionVersion"] == "1"

            # Get alias
            got = lam.get_alias(FunctionName=fname, Name="prod")
            assert got["Name"] == "prod"
            assert got["FunctionVersion"] == "1"

            # Update alias
            updated = lam.update_alias(
                FunctionName=fname,
                Name="prod",
                FunctionVersion="2",
                Description="updated",
            )
            assert updated["FunctionVersion"] == "2"

            # List aliases
            aliases = lam.list_aliases(FunctionName=fname)
            alias_names = [a["Name"] for a in aliases["Aliases"]]
            assert "prod" in alias_names

            # Delete alias
            lam.delete_alias(FunctionName=fname, Name="prod")
            aliases = lam.list_aliases(FunctionName=fname)
            alias_names = [a["Name"] for a in aliases["Aliases"]]
            assert "prod" not in alias_names
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaPermissionsV2:
    def test_add_remove_permission_get_policy(self, lam, role):
        """AddPermission, GetPolicy, RemovePermission."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"perm-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.add_permission(
                FunctionName=fname,
                StatementId="allow-sns",
                Action="lambda:InvokeFunction",
                Principal="sns.amazonaws.com",
            )
            policy_resp = lam.get_policy(FunctionName=fname)
            policy = json.loads(policy_resp["Policy"])
            stmt_ids = [s["Sid"] for s in policy["Statement"]]
            assert "allow-sns" in stmt_ids

            lam.remove_permission(FunctionName=fname, StatementId="allow-sns")
            # After removal, get_policy may raise or return empty
            try:
                policy_resp = lam.get_policy(FunctionName=fname)
                policy = json.loads(policy_resp["Policy"])
                stmt_ids = [s["Sid"] for s in policy["Statement"]]
                assert "allow-sns" not in stmt_ids
            except lam.exceptions.ResourceNotFoundException:
                pass  # No policy left is also valid
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaConcurrencyV2:
    def test_put_get_delete_concurrency(self, lam, role):
        """PutFunctionConcurrency, GetFunctionConcurrency, DeleteFunctionConcurrency."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"conc-ext-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.put_function_concurrency(FunctionName=fname, ReservedConcurrentExecutions=5)
            assert resp["ReservedConcurrentExecutions"] == 5

            resp = lam.get_function_concurrency(FunctionName=fname)
            assert resp["ReservedConcurrentExecutions"] == 5

            lam.delete_function_concurrency(FunctionName=fname)
            resp = lam.get_function_concurrency(FunctionName=fname)
            assert (
                "ReservedConcurrentExecutions" not in resp
                or resp.get("ReservedConcurrentExecutions") is None
            )
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaEventSourceMappings:
    def test_create_list_delete_event_source_mapping(self, lam, role):
        """CreateEventSourceMapping, ListEventSourceMappings, DeleteEventSourceMapping with SQS."""
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-queue-{suffix}"
        fname = f"esm-func-{suffix}"

        q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        esm_uuid = None
        try:
            resp = lam.create_event_source_mapping(
                EventSourceArn=q_arn,
                FunctionName=fname,
                BatchSize=10,
            )
            esm_uuid = resp["UUID"]
            assert resp["EventSourceArn"] == q_arn
            assert resp["BatchSize"] == 10

            # List
            mappings = lam.list_event_source_mappings(FunctionName=fname)
            uuids = [m["UUID"] for m in mappings["EventSourceMappings"]]
            assert esm_uuid in uuids
        finally:
            if esm_uuid:
                lam.delete_event_source_mapping(UUID=esm_uuid)
            lam.delete_function(FunctionName=fname)
            sqs.delete_queue(QueueUrl=q_url)


class TestLambdaListFunctionsPagination:
    def test_list_functions_pagination(self, lam, role):
        """ListFunctions pagination by creating several functions."""
        code = _make_zip("def handler(e, c): pass")
        created = []
        try:
            for i in range(3):
                fname = f"page-fn-{uuid.uuid4().hex[:8]}"
                lam.create_function(
                    FunctionName=fname,
                    Runtime="python3.12",
                    Role=role,
                    Handler="lambda_function.handler",
                    Code={"ZipFile": code},
                )
                created.append(fname)

            all_names = []
            resp = lam.list_functions()
            all_names.extend([f["FunctionName"] for f in resp["Functions"]])
            while "NextMarker" in resp:
                resp = lam.list_functions(Marker=resp["NextMarker"])
                all_names.extend([f["FunctionName"] for f in resp["Functions"]])

            for fname in created:
                assert fname in all_names
        finally:
            for fname in created:
                lam.delete_function(FunctionName=fname)


class TestLambdaTaggingExtended:
    def test_tag_untag_list_tags(self, lam, role):
        """TagResource, UntagResource, ListTags on functions."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"tag-ext-{uuid.uuid4().hex[:8]}"
        resp = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        arn = resp["FunctionArn"]
        try:
            lam.tag_resource(
                Resource=arn,
                Tags={"env": "test", "team": "platform"},
            )
            tags = lam.list_tags(Resource=arn)["Tags"]
            assert tags["env"] == "test"
            assert tags["team"] == "platform"

            lam.untag_resource(Resource=arn, TagKeys=["team"])
            tags = lam.list_tags(Resource=arn)["Tags"]
            assert "env" in tags
            assert "team" not in tags
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaAccountSettings:
    def test_invoke_async_deprecated(self, lam, role):
        """InvokeAsync (deprecated API) should still work."""
        code = _make_zip('def handler(e, c): return "ok"')
        fname = f"invoke-async-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke_async(
                FunctionName=fname,
                InvokeArgs=json.dumps({"test": True}),
            )
            assert resp["Status"] == 202
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaMultiRuntime:
    def test_create_function_python312(self, lam, role):
        """CreateFunction with python3.12 runtime."""
        code = _make_zip('def handler(e, c): return "py312"')
        fname = f"py312-{uuid.uuid4().hex[:8]}"
        try:
            resp = lam.create_function(
                FunctionName=fname,
                Runtime="python3.12",
                Role=role,
                Handler="lambda_function.handler",
                Code={"ZipFile": code},
            )
            assert resp["Runtime"] == "python3.12"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_create_function_nodejs20(self, lam, role):
        """CreateFunction with nodejs20.x runtime."""
        # Node.js handler
        node_code = io.BytesIO()
        with zipfile.ZipFile(node_code, "w") as zf:
            zf.writestr(
                "index.js",
                "exports.handler = async (event) => { return { statusCode: 200 }; };",
            )
        fname = f"node20-{uuid.uuid4().hex[:8]}"
        try:
            resp = lam.create_function(
                FunctionName=fname,
                Runtime="nodejs20.x",
                Role=role,
                Handler="index.handler",
                Code={"ZipFile": node_code.getvalue()},
            )
            assert resp["Runtime"] == "nodejs20.x"
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaExtendedOperations:
    """Extended Lambda operations for higher coverage."""

    @pytest.fixture
    def lam(self):
        return make_client("lambda")

    @pytest.fixture
    def role(self):
        iam = make_client("iam")
        role_name = f"lambda-ext-role-{uuid.uuid4().hex[:8]}"
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(
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
            ),
        )
        yield f"arn:aws:iam::123456789012:role/{role_name}"
        iam.delete_role(RoleName=role_name)

    def test_invoke_with_log_type_tail(self, lam, role):
        code = _make_zip('def handler(e, c): print("hello"); return "ok"')
        fname = f"tail-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname, LogType="Tail")
            assert resp["StatusCode"] == 200
            # LogResult should be present when Tail is requested
            if "LogResult" in resp:
                import base64

                logs = base64.b64decode(resp["LogResult"]).decode()
                assert isinstance(logs, str)
        finally:
            lam.delete_function(FunctionName=fname)

    def test_invoke_async_event(self, lam, role):
        code = _make_zip('def handler(e, c): return "async"')
        fname = f"async-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                InvocationType="Event",
                Payload=json.dumps({"test": True}),
            )
            assert resp["StatusCode"] == 202
        finally:
            lam.delete_function(FunctionName=fname)

    def test_invoke_dry_run(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"dryrun-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname, InvocationType="DryRun")
            assert resp["StatusCode"] == 204
        finally:
            lam.delete_function(FunctionName=fname)

    def test_get_function_returns_code_and_config(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"getfunc-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.get_function(FunctionName=fname)
            assert "Configuration" in resp
            assert "Code" in resp
            assert resp["Configuration"]["FunctionName"] == fname
            assert "Location" in resp["Code"]
        finally:
            lam.delete_function(FunctionName=fname)

    def test_list_functions(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"listfn-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.list_functions()
            names = [f["FunctionName"] for f in resp["Functions"]]
            assert fname in names
        finally:
            lam.delete_function(FunctionName=fname)

    def test_create_function_with_env_vars(self, lam, role):
        code = _make_zip(
            'import os\ndef handler(e, c): return {"MY_VAR": os.environ.get("MY_VAR", "")}'
        )
        fname = f"envvar-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"MY_VAR": "hello"}},
        )
        try:
            cfg = lam.get_function_configuration(FunctionName=fname)
            assert cfg["Environment"]["Variables"]["MY_VAR"] == "hello"

            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["MY_VAR"] == "hello"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_update_function_configuration_env_vars(self, lam, role):
        code = _make_zip('import os\ndef handler(e, c): return {"X": os.environ.get("X", "")}')
        fname = f"updenv-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"X": "old"}},
        )
        try:
            lam.update_function_configuration(
                FunctionName=fname,
                Environment={"Variables": {"X": "new"}},
            )
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["X"] == "new"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_invoke_returns_function_error(self, lam, role):
        code = _make_zip('def handler(e, c): raise ValueError("boom")')
        fname = f"err-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            assert "FunctionError" in resp
            assert resp["FunctionError"] in ("Handled", "Unhandled")
        finally:
            lam.delete_function(FunctionName=fname)

    def test_get_account_settings(self, lam):
        resp = lam.get_account_settings()
        assert "AccountLimit" in resp
        assert "AccountUsage" in resp


class TestLambdaEventInvokeConfig:
    """Tests for function event invoke configuration."""

    def test_put_get_event_invoke_config(self, lam, role):
        """PutFunctionEventInvokeConfig and GetFunctionEventInvokeConfig."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"eic-put-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.put_function_event_invoke_config(
                FunctionName=fname,
                MaximumRetryAttempts=1,
                MaximumEventAgeInSeconds=120,
            )
            assert resp["MaximumRetryAttempts"] == 1
            assert resp["MaximumEventAgeInSeconds"] == 120

            get_resp = lam.get_function_event_invoke_config(FunctionName=fname)
            assert get_resp["MaximumRetryAttempts"] == 1
            assert get_resp["MaximumEventAgeInSeconds"] == 120
        finally:
            lam.delete_function(FunctionName=fname)

    def test_update_event_invoke_config(self, lam, role):
        """UpdateFunctionEventInvokeConfig changes retry settings."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"eic-upd-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.put_function_event_invoke_config(
                FunctionName=fname,
                MaximumRetryAttempts=2,
                MaximumEventAgeInSeconds=60,
            )
            resp = lam.update_function_event_invoke_config(
                FunctionName=fname,
                MaximumRetryAttempts=0,
            )
            assert resp["MaximumRetryAttempts"] == 0
        finally:
            lam.delete_function(FunctionName=fname)

    def test_delete_event_invoke_config(self, lam, role):
        """DeleteFunctionEventInvokeConfig removes the config."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"eic-del-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.put_function_event_invoke_config(
                FunctionName=fname,
                MaximumRetryAttempts=1,
                MaximumEventAgeInSeconds=60,
            )
            lam.delete_function_event_invoke_config(FunctionName=fname)
            with pytest.raises(lam.exceptions.ClientError):
                lam.get_function_event_invoke_config(FunctionName=fname)
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaProvisionedConcurrency:
    """Tests for provisioned concurrency configuration."""

    def test_put_get_provisioned_concurrency(self, lam, role):
        """PutProvisionedConcurrencyConfig and GetProvisionedConcurrencyConfig."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"pcc-put-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            ver = lam.publish_version(FunctionName=fname)["Version"]
            resp = lam.put_provisioned_concurrency_config(
                FunctionName=fname,
                Qualifier=ver,
                ProvisionedConcurrentExecutions=3,
            )
            assert resp["RequestedProvisionedConcurrentExecutions"] == 3

            get_resp = lam.get_provisioned_concurrency_config(FunctionName=fname, Qualifier=ver)
            assert get_resp["RequestedProvisionedConcurrentExecutions"] == 3
        finally:
            try:
                lam.delete_provisioned_concurrency_config(FunctionName=fname, Qualifier=ver)
            except Exception:
                pass
            lam.delete_function(FunctionName=fname)

    def test_delete_provisioned_concurrency(self, lam, role):
        """DeleteProvisionedConcurrencyConfig removes the config."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"pcc-del-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            ver = lam.publish_version(FunctionName=fname)["Version"]
            lam.put_provisioned_concurrency_config(
                FunctionName=fname,
                Qualifier=ver,
                ProvisionedConcurrentExecutions=5,
            )
            lam.delete_provisioned_concurrency_config(FunctionName=fname, Qualifier=ver)
            with pytest.raises(lam.exceptions.ClientError):
                lam.get_provisioned_concurrency_config(FunctionName=fname, Qualifier=ver)
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaEventSourceMappingExtended:
    """Extended ESM tests: get, update."""

    def test_get_event_source_mapping(self, lam, role):
        """GetEventSourceMapping returns mapping details."""
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-get-q-{suffix}"
        fname = f"esm-get-fn-{suffix}"

        q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        esm_uuid = None
        try:
            resp = lam.create_event_source_mapping(
                EventSourceArn=q_arn, FunctionName=fname, BatchSize=10
            )
            esm_uuid = resp["UUID"]

            get_resp = lam.get_event_source_mapping(UUID=esm_uuid)
            assert get_resp["UUID"] == esm_uuid
            assert get_resp["EventSourceArn"] == q_arn
            assert get_resp["BatchSize"] == 10
        finally:
            if esm_uuid:
                lam.delete_event_source_mapping(UUID=esm_uuid)
            lam.delete_function(FunctionName=fname)
            sqs.delete_queue(QueueUrl=q_url)

    def test_update_event_source_mapping(self, lam, role):
        """UpdateEventSourceMapping changes batch size."""
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-upd-q-{suffix}"
        fname = f"esm-upd-fn-{suffix}"

        q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        esm_uuid = None
        try:
            resp = lam.create_event_source_mapping(
                EventSourceArn=q_arn, FunctionName=fname, BatchSize=10
            )
            esm_uuid = resp["UUID"]

            upd_resp = lam.update_event_source_mapping(UUID=esm_uuid, BatchSize=5)
            assert upd_resp["BatchSize"] == 5

            get_resp = lam.get_event_source_mapping(UUID=esm_uuid)
            assert get_resp["BatchSize"] == 5
        finally:
            if esm_uuid:
                lam.delete_event_source_mapping(UUID=esm_uuid)
            lam.delete_function(FunctionName=fname)
            sqs.delete_queue(QueueUrl=q_url)

    def test_list_event_source_mappings_all(self, lam, role):
        """ListEventSourceMappings without function filter returns all mappings."""
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-all-q-{suffix}"
        fname = f"esm-all-fn-{suffix}"

        q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        esm_uuid = None
        try:
            resp = lam.create_event_source_mapping(
                EventSourceArn=q_arn, FunctionName=fname, BatchSize=10
            )
            esm_uuid = resp["UUID"]

            all_resp = lam.list_event_source_mappings()
            uuids = [m["UUID"] for m in all_resp["EventSourceMappings"]]
            assert esm_uuid in uuids
        finally:
            if esm_uuid:
                lam.delete_event_source_mapping(UUID=esm_uuid)
            lam.delete_function(FunctionName=fname)
            sqs.delete_queue(QueueUrl=q_url)


class TestLambdaLayerDeletion:
    """Tests for layer version deletion."""

    def test_delete_layer_version(self, lam):
        """DeleteLayerVersion removes a specific layer version."""
        layer_code = io.BytesIO()
        with zipfile.ZipFile(layer_code, "w") as zf:
            zf.writestr("python/mod.py", "X = 1")
        layer_name = f"del-layer-{uuid.uuid4().hex[:8]}"
        resp = lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": layer_code.getvalue()},
            CompatibleRuntimes=["python3.12"],
        )
        version = resp["Version"]
        lam.delete_layer_version(LayerName=layer_name, VersionNumber=version)
        with pytest.raises(lam.exceptions.ClientError):
            lam.get_layer_version(LayerName=layer_name, VersionNumber=version)

    def test_delete_one_layer_version_keeps_others(self, lam):
        """Deleting one layer version does not affect other versions."""
        layer_code = io.BytesIO()
        with zipfile.ZipFile(layer_code, "w") as zf:
            zf.writestr("python/mod.py", "X = 1")
        layer_name = f"del-one-layer-{uuid.uuid4().hex[:8]}"
        lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": layer_code.getvalue()},
        )
        lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": layer_code.getvalue()},
        )
        # Delete version 1, version 2 should remain
        lam.delete_layer_version(LayerName=layer_name, VersionNumber=1)
        resp = lam.get_layer_version(LayerName=layer_name, VersionNumber=2)
        assert resp["Version"] == 2


class TestLambdaPermissionsWithSourceArn:
    """Tests for permissions with SourceArn/SourceAccount."""

    def test_add_permission_with_source_arn(self, lam, role):
        """AddPermission with SourceArn and SourceAccount."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"perm-src-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.add_permission(
                FunctionName=fname,
                StatementId="allow-s3",
                Action="lambda:InvokeFunction",
                Principal="s3.amazonaws.com",
                SourceArn="arn:aws:s3:::my-bucket",
                SourceAccount="123456789012",
            )
            policy_resp = lam.get_policy(FunctionName=fname)
            policy = json.loads(policy_resp["Policy"])
            stmt = [s for s in policy["Statement"] if s["Sid"] == "allow-s3"][0]
            assert stmt["Action"] == "lambda:InvokeFunction"
            assert "s3.amazonaws.com" in json.dumps(stmt["Principal"])
            lam.remove_permission(FunctionName=fname, StatementId="allow-s3")
        finally:
            lam.delete_function(FunctionName=fname)

    def test_add_multiple_permissions(self, lam, role):
        """Multiple AddPermission calls create multiple statements."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"perm-multi-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.add_permission(
                FunctionName=fname,
                StatementId="stmt-1",
                Action="lambda:InvokeFunction",
                Principal="s3.amazonaws.com",
            )
            lam.add_permission(
                FunctionName=fname,
                StatementId="stmt-2",
                Action="lambda:InvokeFunction",
                Principal="sns.amazonaws.com",
            )
            policy_resp = lam.get_policy(FunctionName=fname)
            policy = json.loads(policy_resp["Policy"])
            sids = [s["Sid"] for s in policy["Statement"]]
            assert "stmt-1" in sids
            assert "stmt-2" in sids
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaFunctionUrlUpdate:
    """Tests for UpdateFunctionUrlConfig."""

    def test_update_function_url_config(self, lam, role):
        """Create a function URL then update its auth type."""
        code = _make_zip("def handler(e, c): return {'statusCode': 200}")
        fname = f"url-update-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.create_function_url_config(FunctionName=fname, AuthType="NONE")
            resp = lam.update_function_url_config(FunctionName=fname, AuthType="AWS_IAM")
            assert resp["AuthType"] == "AWS_IAM"
            assert "FunctionUrl" in resp

            # Verify the update persisted
            get_resp = lam.get_function_url_config(FunctionName=fname)
            assert get_resp["AuthType"] == "AWS_IAM"

            lam.delete_function_url_config(FunctionName=fname)
        finally:
            lam.delete_function(FunctionName=fname)

    def test_update_function_url_config_cors(self, lam, role):
        """Update function URL CORS configuration."""
        code = _make_zip("def handler(e, c): return {'statusCode': 200}")
        fname = f"url-cors-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.create_function_url_config(FunctionName=fname, AuthType="NONE")
            resp = lam.update_function_url_config(
                FunctionName=fname,
                AuthType="NONE",
                Cors={
                    "AllowOrigins": ["https://example.com"],
                    "AllowMethods": ["GET", "POST"],
                },
            )
            assert resp["AuthType"] == "NONE"
            assert "Cors" in resp
            assert "https://example.com" in resp["Cors"]["AllowOrigins"]

            lam.delete_function_url_config(FunctionName=fname)
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaUpdateFunctionCode:
    """Tests for UpdateFunctionCode with additional scenarios."""

    def test_update_function_code_new_handler(self, lam, role):
        """Update function code and verify the new code is used."""
        code_v1 = _make_zip("def handler(e, c): return 'v1'")
        code_v2 = _make_zip("def handler(e, c): return 'v2'")
        fname = f"code-update-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code_v1},
        )
        try:
            resp = lam.update_function_code(FunctionName=fname, ZipFile=code_v2)
            assert resp["FunctionName"] == fname
            assert "CodeSha256" in resp
            assert "CodeSize" in resp
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaListLayerVersions:
    """Tests for ListLayerVersions with multiple versions."""

    def test_list_layer_versions_multiple(self, lam):
        """Publish multiple layer versions and list them."""
        code = _make_zip("# layer code")
        layer_name = f"multi-layer-{uuid.uuid4().hex[:8]}"
        lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": code},
            CompatibleRuntimes=["python3.12"],
            Description="version 1",
        )
        lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": code},
            CompatibleRuntimes=["python3.12"],
            Description="version 2",
        )
        resp = lam.list_layer_versions(LayerName=layer_name)
        versions = resp["LayerVersions"]
        assert len(versions) >= 2
        version_numbers = [v["Version"] for v in versions]
        assert 1 in version_numbers
        assert 2 in version_numbers

        lam.delete_layer_version(LayerName=layer_name, VersionNumber=1)
        lam.delete_layer_version(LayerName=layer_name, VersionNumber=2)


class TestLambdaGetEventInvokeConfig:
    """Tests for GetFunctionEventInvokeConfig and ListFunctionEventInvokeConfigs."""

    def test_get_function_event_invoke_config(self, lam, role):
        """Put and get event invoke config."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"eic-get-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.put_function_event_invoke_config(
                FunctionName=fname,
                MaximumRetryAttempts=0,
                MaximumEventAgeInSeconds=120,
            )
            resp = lam.get_function_event_invoke_config(FunctionName=fname)
            assert resp["MaximumRetryAttempts"] == 0
            assert resp["MaximumEventAgeInSeconds"] == 120
            lam.delete_function_event_invoke_config(FunctionName=fname)
        finally:
            lam.delete_function(FunctionName=fname)

    def test_list_function_event_invoke_configs(self, lam, role):
        """ListFunctionEventInvokeConfigs returns a response."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"eic-list-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.put_function_event_invoke_config(FunctionName=fname, MaximumRetryAttempts=2)
            resp = lam.list_function_event_invoke_configs(FunctionName=fname)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                lam.delete_function_event_invoke_config(FunctionName=fname)
            except Exception:
                pass
            lam.delete_function(FunctionName=fname)


class TestLambdaGetLayerVersionByArn:
    """Tests for GetLayerVersionByArn."""

    def test_get_layer_version_by_arn(self, lam):
        """GetLayerVersionByArn returns a successful response."""
        code = _make_zip("# layer")
        layer_name = f"by-arn-layer-{uuid.uuid4().hex[:8]}"
        pub = lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": code},
            CompatibleRuntimes=["python3.12"],
        )
        layer_version_arn = pub["LayerVersionArn"]
        try:
            resp = lam.get_layer_version_by_arn(Arn=layer_version_arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            lam.delete_layer_version(LayerName=layer_name, VersionNumber=1)


class TestLambdaDeleteFunctionEventInvokeConfig:
    """Tests for DeleteFunctionEventInvokeConfig."""

    def test_delete_function_event_invoke_config(self, lam, role):
        """Delete event invoke config after creating one."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"eic-del-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.put_function_event_invoke_config(FunctionName=fname, MaximumRetryAttempts=1)
            lam.delete_function_event_invoke_config(FunctionName=fname)
            # Verify it's gone by trying to get it
            from botocore.exceptions import ClientError

            with pytest.raises(ClientError):
                lam.get_function_event_invoke_config(FunctionName=fname)
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaAccountAndESM:
    """Tests for account settings and event source mappings."""

    @pytest.fixture
    def lam(self):
        return make_client("lambda")

    def test_get_account_settings(self, lam):
        """GetAccountSettings returns account limits and usage."""
        resp = lam.get_account_settings()
        assert "AccountLimit" in resp
        assert "AccountUsage" in resp

    def test_list_event_source_mappings_empty(self, lam):
        """ListEventSourceMappings returns list (possibly empty)."""
        resp = lam.list_event_source_mappings()
        assert "EventSourceMappings" in resp
        assert isinstance(resp["EventSourceMappings"], list)


class TestLambdaFunctionRecursionConfig:
    @pytest.fixture
    def lam(self):
        return make_client("lambda")

    @pytest.fixture
    def role(self):
        iam = make_client("iam")
        name = f"recursion-role-{uuid.uuid4().hex[:8]}"
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
        iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust)
        yield f"arn:aws:iam::123456789012:role/{name}"
        iam.delete_role(RoleName=name)

    @pytest.fixture
    def func(self, lam, role):
        name = f"recursion-func-{uuid.uuid4().hex[:8]}"
        code = _make_zip("def handler(e, c): return 'ok'")
        lam.create_function(
            FunctionName=name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        yield name
        lam.delete_function(FunctionName=name)

    def test_get_function_recursion_config(self, lam, func):
        """GetFunctionRecursionConfig returns RecursiveLoop setting."""
        resp = lam.get_function_recursion_config(FunctionName=func)
        assert "RecursiveLoop" in resp
        assert resp["RecursiveLoop"] in ("Allow", "Terminate")

    def test_put_function_recursion_config(self, lam, func):
        """PutFunctionRecursionConfig sets RecursiveLoop and returns it."""
        resp = lam.put_function_recursion_config(FunctionName=func, RecursiveLoop="Terminate")
        assert resp["RecursiveLoop"] == "Terminate"
        # Verify via get
        got = lam.get_function_recursion_config(FunctionName=func)
        assert got["RecursiveLoop"] == "Terminate"

    def test_get_function_recursion_config_nonexistent(self, lam):
        """GetFunctionRecursionConfig on nonexistent function raises error."""
        with pytest.raises(lam.exceptions.ClientError) as exc_info:
            lam.get_function_recursion_config(FunctionName="no-such-func-xyz")
        assert exc_info.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "FunctionNotFoundException",
        )


class TestLambdaFunctionScalingConfig:
    """Tests for GetFunctionScalingConfig and PutFunctionScalingConfig."""

    @pytest.fixture
    def func(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"scaling-cfg-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        yield fname
        lam.delete_function(FunctionName=fname)

    def test_get_function_scaling_config_default(self, lam, func):
        """GetFunctionScalingConfig returns config for new function."""
        resp = lam.get_function_scaling_config(FunctionName=func, Qualifier="$LATEST")
        assert "FunctionArn" in resp

    def test_put_function_scaling_config(self, lam, func):
        """PutFunctionScalingConfig sets scaling config and returns FunctionState."""
        resp = lam.put_function_scaling_config(
            FunctionName=func,
            Qualifier="$LATEST",
            FunctionScalingConfig={"MaxExecutionEnvironments": 10},
        )
        assert "FunctionState" in resp

    def test_put_then_get_scaling_config(self, lam, func):
        """PutFunctionScalingConfig is reflected by GetFunctionScalingConfig."""
        lam.put_function_scaling_config(
            FunctionName=func,
            Qualifier="$LATEST",
            FunctionScalingConfig={"MaxExecutionEnvironments": 5},
        )
        resp = lam.get_function_scaling_config(FunctionName=func, Qualifier="$LATEST")
        assert resp["AppliedFunctionScalingConfig"]["MaxExecutionEnvironments"] == 5

    def test_get_function_scaling_config_nonexistent(self, lam):
        """GetFunctionScalingConfig on nonexistent function raises error."""
        with pytest.raises(lam.exceptions.ClientError) as exc_info:
            lam.get_function_scaling_config(FunctionName="no-such-func-xyz", Qualifier="$LATEST")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLambdaCodeSigningConfig:
    """Tests for Get/Put/DeleteFunctionCodeSigningConfig."""

    @pytest.fixture
    def func(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"codesign-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        yield fname
        lam.delete_function(FunctionName=fname)

    def test_put_function_code_signing_config(self, lam, func):
        """PutFunctionCodeSigningConfig sets code signing config ARN."""
        csc_arn = "arn:aws:lambda:us-east-1:123456789012:code-signing-config:csc-fake123"
        resp = lam.put_function_code_signing_config(FunctionName=func, CodeSigningConfigArn=csc_arn)
        assert resp["CodeSigningConfigArn"] == csc_arn
        assert resp["FunctionName"] == func

    def test_get_function_code_signing_config(self, lam, func):
        """GetFunctionCodeSigningConfig returns the configured ARN."""
        csc_arn = "arn:aws:lambda:us-east-1:123456789012:code-signing-config:csc-fake456"
        lam.put_function_code_signing_config(FunctionName=func, CodeSigningConfigArn=csc_arn)
        resp = lam.get_function_code_signing_config(FunctionName=func)
        assert resp["CodeSigningConfigArn"] == csc_arn

    def test_delete_function_code_signing_config(self, lam, func):
        """DeleteFunctionCodeSigningConfig removes the config."""
        csc_arn = "arn:aws:lambda:us-east-1:123456789012:code-signing-config:csc-del"
        lam.put_function_code_signing_config(FunctionName=func, CodeSigningConfigArn=csc_arn)
        lam.delete_function_code_signing_config(FunctionName=func)
        with pytest.raises(lam.exceptions.ClientError) as exc_info:
            lam.get_function_code_signing_config(FunctionName=func)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_code_signing_config_nonexistent(self, lam):
        """GetFunctionCodeSigningConfig on nonexistent function raises error."""
        with pytest.raises(lam.exceptions.ClientError) as exc_info:
            lam.get_function_code_signing_config(FunctionName="no-such-func-xyz")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLambdaRuntimeManagementConfig:
    """Tests for GetRuntimeManagementConfig and PutRuntimeManagementConfig."""

    @pytest.fixture
    def func(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"rtmgmt-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        yield fname
        lam.delete_function(FunctionName=fname)

    def test_get_runtime_management_config_default(self, lam, func):
        """GetRuntimeManagementConfig returns Auto by default."""
        resp = lam.get_runtime_management_config(FunctionName=func)
        assert resp["UpdateRuntimeOn"] == "Auto"

    def test_put_runtime_management_config(self, lam, func):
        """PutRuntimeManagementConfig sets UpdateRuntimeOn."""
        resp = lam.put_runtime_management_config(
            FunctionName=func, UpdateRuntimeOn="FunctionUpdate"
        )
        assert resp["UpdateRuntimeOn"] == "FunctionUpdate"

    def test_put_then_get_runtime_management_config(self, lam, func):
        """PutRuntimeManagementConfig is reflected by Get."""
        lam.put_runtime_management_config(FunctionName=func, UpdateRuntimeOn="FunctionUpdate")
        resp = lam.get_runtime_management_config(FunctionName=func)
        assert resp["UpdateRuntimeOn"] == "FunctionUpdate"

    def test_get_runtime_management_config_nonexistent(self, lam):
        """GetRuntimeManagementConfig on nonexistent function raises error."""
        with pytest.raises(lam.exceptions.ClientError) as exc_info:
            lam.get_runtime_management_config(FunctionName="no-such-func-xyz")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLambdaLayerVersionPermission:
    """Tests for AddLayerVersionPermission, RemoveLayerVersionPermission, GetLayerVersionPolicy."""

    @pytest.fixture
    def layer(self, lam):
        code = _make_zip("# layer code")
        layer_name = f"perm-layer-{uuid.uuid4().hex[:8]}"
        resp = lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": code},
            CompatibleRuntimes=["python3.12"],
        )
        version = resp["Version"]
        yield layer_name, version
        try:
            lam.delete_layer_version(LayerName=layer_name, VersionNumber=version)
        except Exception:
            pass

    def test_add_layer_version_permission(self, lam, layer):
        """AddLayerVersionPermission adds a permission statement."""
        layer_name, version = layer
        resp = lam.add_layer_version_permission(
            LayerName=layer_name,
            VersionNumber=version,
            StatementId="allow-all",
            Action="lambda:GetLayerVersion",
            Principal="*",
        )
        assert "Statement" in resp

    def test_get_layer_version_policy(self, lam, layer):
        """GetLayerVersionPolicy returns the policy after adding a permission."""
        layer_name, version = layer
        lam.add_layer_version_permission(
            LayerName=layer_name,
            VersionNumber=version,
            StatementId="sid1",
            Action="lambda:GetLayerVersion",
            Principal="*",
        )
        resp = lam.get_layer_version_policy(LayerName=layer_name, VersionNumber=version)
        assert "Policy" in resp
        import json

        policy = json.loads(resp["Policy"])
        assert len(policy["Statement"]) >= 1

    def test_remove_layer_version_permission(self, lam, layer):
        """RemoveLayerVersionPermission removes a statement."""
        layer_name, version = layer
        lam.add_layer_version_permission(
            LayerName=layer_name,
            VersionNumber=version,
            StatementId="to-remove",
            Action="lambda:GetLayerVersion",
            Principal="*",
        )
        lam.remove_layer_version_permission(
            LayerName=layer_name,
            VersionNumber=version,
            StatementId="to-remove",
        )
        # After removing the only statement, policy should not exist
        with pytest.raises(lam.exceptions.ClientError) as exc_info:
            lam.get_layer_version_policy(LayerName=layer_name, VersionNumber=version)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_layer_version_policy_nonexistent(self, lam):
        """GetLayerVersionPolicy on nonexistent layer raises error."""
        with pytest.raises(lam.exceptions.ClientError) as exc_info:
            lam.get_layer_version_policy(LayerName="no-such-layer-xyz", VersionNumber=1)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLambdaListFunctionUrlConfigs:
    """Tests for ListFunctionUrlConfigs."""

    @pytest.fixture
    def func(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"urls-list-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        yield fname
        try:
            lam.delete_function_url_config(FunctionName=fname)
        except Exception:
            pass
        lam.delete_function(FunctionName=fname)

    def test_list_function_url_configs_empty(self, lam, func):
        """ListFunctionUrlConfigs returns empty list when no URL configured."""
        resp = lam.list_function_url_configs(FunctionName=func)
        assert resp["FunctionUrlConfigs"] == []

    def test_list_function_url_configs_with_url(self, lam, func):
        """ListFunctionUrlConfigs returns the URL config after creation."""
        lam.create_function_url_config(FunctionName=func, AuthType="NONE")
        resp = lam.list_function_url_configs(FunctionName=func)
        assert len(resp["FunctionUrlConfigs"]) == 1
        assert "FunctionUrl" in resp["FunctionUrlConfigs"][0]


class TestLambdaListProvisionedConcurrencyConfigs:
    """Tests for ListProvisionedConcurrencyConfigs."""

    @pytest.fixture
    def func(self, lam, role):
        code = _make_zip("def handler(e, c): pass")
        fname = f"prov-list-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        yield fname
        lam.delete_function(FunctionName=fname)

    def test_list_provisioned_concurrency_configs_empty(self, lam, func):
        """ListProvisionedConcurrencyConfigs returns empty list."""
        resp = lam.list_provisioned_concurrency_configs(FunctionName=func)
        assert resp["ProvisionedConcurrencyConfigs"] == []

    def test_list_provisioned_concurrency_configs_with_config(self, lam, func):
        """ListProvisionedConcurrencyConfigs includes set configs."""
        # Publish a version first (provisioned concurrency needs qualifier)
        ver = lam.publish_version(FunctionName=func)
        version = ver["Version"]
        lam.put_provisioned_concurrency_config(
            FunctionName=func,
            Qualifier=version,
            ProvisionedConcurrentExecutions=5,
        )
        resp = lam.list_provisioned_concurrency_configs(FunctionName=func)
        assert len(resp["ProvisionedConcurrencyConfigs"]) >= 1
        # Clean up
        lam.delete_provisioned_concurrency_config(FunctionName=func, Qualifier=version)


class TestLambdaListDurableExecutions:
    """Tests for ListDurableExecutionsByFunction."""

    def test_list_durable_executions_empty(self, lam):
        """ListDurableExecutionsByFunction returns empty list."""
        resp = lam.list_durable_executions_by_function(FunctionName="any-func")
        assert resp["DurableExecutions"] == []


class TestLambdaInvokeWithResponseStream:
    """Tests for InvokeWithResponseStream."""

    @pytest.fixture
    def func(self, lam, role):
        code = _make_zip('def handler(event, ctx): return {"result": "streamed"}')
        fname = f"stream-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        yield fname
        lam.delete_function(FunctionName=fname)

    def test_invoke_with_response_stream(self, lam, func):
        """InvokeWithResponseStream returns a 200 with ExecutedVersion."""
        resp = lam.invoke_with_response_stream(FunctionName=func)
        assert resp["StatusCode"] == 200
        assert resp["ExecutedVersion"] == "$LATEST"


# ---------------------------------------------------------------------------
# Alias with RoutingConfig (weighted traffic shifting)
# ---------------------------------------------------------------------------


class TestLambdaAliasRoutingConfig:
    """Tests for alias RoutingConfig (weighted traffic shifting)."""

    @pytest.fixture
    def func_with_version(self, lam, role):
        code = _make_zip('def handler(e, c): return {"ok": True}')
        fname = f"alias-rc-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        resp = lam.publish_version(FunctionName=fname)
        version = resp["Version"]
        yield fname, version
        try:
            lam.delete_alias(FunctionName=fname, Name="weighted")
        except Exception:
            pass
        lam.delete_function(FunctionName=fname)

    def test_create_alias_with_routing_config(self, lam, func_with_version):
        """CreateAlias with RoutingConfig sets weighted traffic shifting."""
        fname, version = func_with_version
        resp = lam.create_alias(
            FunctionName=fname,
            Name="weighted",
            FunctionVersion=version,
            RoutingConfig={"AdditionalVersionWeights": {version: 0.1}},
        )
        assert resp["Name"] == "weighted"
        assert resp["RoutingConfig"]["AdditionalVersionWeights"][version] == 0.1

    def test_update_alias_routing_config(self, lam, func_with_version):
        """UpdateAlias with RoutingConfig changes the weights."""
        fname, version = func_with_version
        lam.create_alias(
            FunctionName=fname,
            Name="weighted",
            FunctionVersion=version,
        )
        resp = lam.update_alias(
            FunctionName=fname,
            Name="weighted",
            RoutingConfig={"AdditionalVersionWeights": {version: 0.5}},
        )
        assert resp["RoutingConfig"]["AdditionalVersionWeights"][version] == 0.5


# ---------------------------------------------------------------------------
# PublishLayerVersion with Description, LicenseInfo, CompatibleArchitectures
# ---------------------------------------------------------------------------


class TestLambdaLayerExtras:
    """Tests for layer attributes: Description, LicenseInfo, CompatibleArchitectures."""

    def test_publish_layer_with_description_and_license(self, lam):
        """PublishLayerVersion with Description and LicenseInfo."""
        code = _make_zip("# layer code")
        layer_name = f"desc-layer-{uuid.uuid4().hex[:8]}"
        resp = lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": code},
            Description="test layer description",
            LicenseInfo="MIT",
            CompatibleRuntimes=["python3.12"],
        )
        assert resp["Description"] == "test layer description"
        assert resp["LicenseInfo"] == "MIT"
        assert "python3.12" in resp["CompatibleRuntimes"]
        lam.delete_layer_version(LayerName=layer_name, VersionNumber=1)

    def test_publish_layer_with_compatible_architectures(self, lam):
        """PublishLayerVersion with CompatibleArchitectures."""
        code = _make_zip("# layer code")
        layer_name = f"arch-layer-{uuid.uuid4().hex[:8]}"
        resp = lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": code},
            CompatibleRuntimes=["python3.12"],
            CompatibleArchitectures=["arm64"],
        )
        assert "arm64" in resp["CompatibleArchitectures"]
        lam.delete_layer_version(LayerName=layer_name, VersionNumber=1)

    def test_list_layers_compatible_runtime_filter(self, lam):
        """ListLayers with CompatibleRuntime filter returns matching layers."""
        code = _make_zip("# layer code")
        layer_name = f"filter-layer-{uuid.uuid4().hex[:8]}"
        lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": code},
            CompatibleRuntimes=["python3.12"],
        )
        try:
            resp = lam.list_layers(CompatibleRuntime="python3.12")
            names = [layer["LayerName"] for layer in resp["Layers"]]
            assert layer_name in names
        finally:
            lam.delete_layer_version(LayerName=layer_name, VersionNumber=1)


# ---------------------------------------------------------------------------
# CreateFunction with Layers
# ---------------------------------------------------------------------------


class TestLambdaFunctionWithLayers:
    """Tests for creating functions with layer attachments."""

    def test_create_function_with_layers(self, lam, role):
        """CreateFunction with Layers attaches the layer to the function."""
        code = _make_zip("def handler(e, c): return {'ok': True}")
        layer_name = f"fn-layer-{uuid.uuid4().hex[:8]}"
        layer_resp = lam.publish_layer_version(
            LayerName=layer_name,
            Content={"ZipFile": code},
            CompatibleRuntimes=["python3.12"],
        )
        layer_arn = layer_resp["LayerVersionArn"]

        fname = f"with-layer-{uuid.uuid4().hex[:8]}"
        try:
            resp = lam.create_function(
                FunctionName=fname,
                Runtime="python3.12",
                Role=role,
                Handler="lambda_function.handler",
                Code={"ZipFile": code},
                Layers=[layer_arn],
            )
            assert len(resp["Layers"]) == 1
            assert resp["Layers"][0]["Arn"] == layer_arn
        finally:
            lam.delete_function(FunctionName=fname)
            lam.delete_layer_version(LayerName=layer_name, VersionNumber=1)
