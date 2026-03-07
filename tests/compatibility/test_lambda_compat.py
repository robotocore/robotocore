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


class TestLambdaPagination:
    def test_list_functions_contains_created(self, lam, role):
        """Test listing functions returns all created functions."""
        code = _make_zip("def handler(e, c): pass")
        fnames = []
        for i in range(4):
            fname = f"page-func-{uuid.uuid4().hex[:8]}"
            fnames.append(fname)
            lam.create_function(
                FunctionName=fname,
                Runtime="python3.12",
                Role=role,
                Handler="lambda_function.handler",
                Code={"ZipFile": code},
            )

        try:
            response = lam.list_functions()
            listed_names = [f["FunctionName"] for f in response["Functions"]]
            for fname in fnames:
                assert fname in listed_names
        finally:
            for fname in fnames:
                lam.delete_function(FunctionName=fname)


class TestLambdaConfigurationUpdates:
    def test_update_timeout_and_memory(self, lam, role):
        """Test updating function timeout and memory size."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"upd-config-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Timeout=3,
            MemorySize=128,
        )
        try:
            response = lam.update_function_configuration(
                FunctionName=fname,
                Timeout=60,
                MemorySize=512,
            )
            assert response["Timeout"] == 60
            assert response["MemorySize"] == 512

            # Verify via get
            cfg = lam.get_function_configuration(FunctionName=fname)
            assert cfg["Timeout"] == 60
            assert cfg["MemorySize"] == 512
        finally:
            lam.delete_function(FunctionName=fname)

    def test_update_environment_variables(self, lam, role):
        """Test updating environment variables on a function."""
        code = _make_zip(
            'import os\ndef handler(e, c): return {"v": os.environ.get("KEY", "missing")}'
        )
        fname = f"upd-env-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"KEY": "original"}},
        )
        try:
            lam.update_function_configuration(
                FunctionName=fname,
                Environment={"Variables": {"KEY": "updated", "NEW_KEY": "new_val"}},
            )
            cfg = lam.get_function_configuration(FunctionName=fname)
            env_vars = cfg["Environment"]["Variables"]
            assert env_vars["KEY"] == "updated"
            assert env_vars["NEW_KEY"] == "new_val"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_update_description(self, lam, role):
        """Test updating function description."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"upd-desc-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Description="initial",
        )
        try:
            lam.update_function_configuration(
                FunctionName=fname, Description="updated description"
            )
            cfg = lam.get_function_configuration(FunctionName=fname)
            assert cfg["Description"] == "updated description"
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaTagging:
    def test_tag_and_untag_function(self, lam, role):
        """Test adding and removing tags from a Lambda function."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"tag-mgmt-{uuid.uuid4().hex[:8]}"
        resp = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Tags={"env": "test"},
        )
        arn = resp["FunctionArn"]
        try:
            # Add more tags
            lam.tag_resource(Resource=arn, Tags={"team": "platform", "version": "1.0"})
            tags = lam.list_tags(Resource=arn)["Tags"]
            assert tags["env"] == "test"
            assert tags["team"] == "platform"
            assert tags["version"] == "1.0"

            # Remove a tag
            lam.untag_resource(Resource=arn, TagKeys=["version"])
            tags = lam.list_tags(Resource=arn)["Tags"]
            assert "version" not in tags
            assert tags["env"] == "test"
            assert tags["team"] == "platform"
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaAliases:
    def test_create_get_alias(self, lam, role):
        """Test creating and getting a function alias."""
        code = _make_zip('def handler(e, c): return "v1"')
        fname = f"alias-func-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.publish_version(FunctionName=fname)
            resp = lam.create_alias(
                FunctionName=fname,
                Name="prod",
                FunctionVersion="1",
                Description="production alias",
            )
            assert resp["Name"] == "prod"
            assert resp["FunctionVersion"] == "1"

            alias = lam.get_alias(FunctionName=fname, Name="prod")
            assert alias["Name"] == "prod"
            assert alias["Description"] == "production alias"
        finally:
            try:
                lam.delete_alias(FunctionName=fname, Name="prod")
            except Exception:
                pass
            lam.delete_function(FunctionName=fname)

    def test_update_alias(self, lam, role):
        """Test updating an alias to point to a new version."""
        code_v1 = _make_zip('def handler(e, c): return "v1"')
        fname = f"alias-upd-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code_v1},
        )
        try:
            lam.publish_version(FunctionName=fname)  # version 1
            # Update code to create a distinct version 2
            code_v2 = _make_zip('def handler(e, c): return "v2"')
            lam.update_function_code(FunctionName=fname, ZipFile=code_v2)
            lam.publish_version(FunctionName=fname)  # version 2
            lam.create_alias(FunctionName=fname, Name="staging", FunctionVersion="1")
            resp = lam.update_alias(FunctionName=fname, Name="staging", FunctionVersion="2")
            assert resp["FunctionVersion"] == "2"

            alias = lam.get_alias(FunctionName=fname, Name="staging")
            assert alias["FunctionVersion"] == "2"
        finally:
            try:
                lam.delete_alias(FunctionName=fname, Name="staging")
            except Exception:
                pass
            lam.delete_function(FunctionName=fname)

    def test_list_aliases(self, lam, role):
        """Test listing aliases for a function."""
        code = _make_zip('def handler(e, c): return "v1"')
        fname = f"alias-list-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.publish_version(FunctionName=fname)
            lam.create_alias(FunctionName=fname, Name="dev", FunctionVersion="1")
            lam.create_alias(FunctionName=fname, Name="staging", FunctionVersion="1")

            response = lam.list_aliases(FunctionName=fname)
            alias_names = sorted(a["Name"] for a in response["Aliases"])
            assert "dev" in alias_names
            assert "staging" in alias_names
        finally:
            for alias in ["dev", "staging"]:
                try:
                    lam.delete_alias(FunctionName=fname, Name=alias)
                except Exception:
                    pass
            lam.delete_function(FunctionName=fname)

    def test_delete_alias(self, lam, role):
        """Test deleting an alias."""
        code = _make_zip('def handler(e, c): return "v1"')
        fname = f"alias-del-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.publish_version(FunctionName=fname)
            lam.create_alias(FunctionName=fname, Name="temp", FunctionVersion="1")
            lam.delete_alias(FunctionName=fname, Name="temp")

            response = lam.list_aliases(FunctionName=fname)
            alias_names = [a["Name"] for a in response["Aliases"]]
            assert "temp" not in alias_names
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaVersions:
    def test_publish_multiple_versions(self, lam, role):
        """Test publishing multiple versions and listing them."""
        code_v1 = _make_zip('def handler(e, c): return "v1"')
        fname = f"multi-ver-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code_v1},
        )
        try:
            v1 = lam.publish_version(FunctionName=fname)
            assert v1["Version"] == "1"
            # Update code so next publish creates a new version
            code_v2 = _make_zip('def handler(e, c): return "v2"')
            lam.update_function_code(FunctionName=fname, ZipFile=code_v2)
            v2 = lam.publish_version(FunctionName=fname)
            assert v2["Version"] == "2"

            response = lam.list_versions_by_function(FunctionName=fname)
            versions = sorted(v["Version"] for v in response["Versions"])
            assert "$LATEST" in versions
            assert "1" in versions
            assert "2" in versions
        finally:
            lam.delete_function(FunctionName=fname)

    def test_invoke_specific_version(self, lam, role):
        """Test invoking a specific published version."""
        code = _make_zip('def handler(e, c): return {"version": "original"}')
        fname = f"ver-invoke-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.publish_version(FunctionName=fname)  # version 1

            # Invoke version 1
            response = lam.invoke(FunctionName=fname, Qualifier="1")
            payload = json.loads(response["Payload"].read())
            assert payload["version"] == "original"
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaInvocationTypes:
    def test_invoke_request_response(self, lam, role):
        """Test explicit RequestResponse invocation type."""
        code = _make_zip('def handler(e, c): return {"result": "sync"}')
        fname = f"inv-rr-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            response = lam.invoke(
                FunctionName=fname, InvocationType="RequestResponse"
            )
            assert response["StatusCode"] == 200
            payload = json.loads(response["Payload"].read())
            assert payload["result"] == "sync"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_invoke_event_returns_202(self, lam, role):
        """Test Event invocation type returns 202 with no payload."""
        code = _make_zip('def handler(e, c): return "async-result"')
        fname = f"inv-ev-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            response = lam.invoke(FunctionName=fname, InvocationType="Event")
            assert response["StatusCode"] == 202
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaEnvironment:
    def test_create_with_multiple_env_vars(self, lam, role):
        """Test creating a function with multiple environment variables."""
        code = _make_zip(
            'import os\ndef handler(e, c): return {'
            '"a": os.environ.get("VAR_A", ""), '
            '"b": os.environ.get("VAR_B", ""), '
            '"c": os.environ.get("VAR_C", "")}'
        )
        fname = f"multi-env-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"VAR_A": "a1", "VAR_B": "b2", "VAR_C": "c3"}},
        )
        try:
            response = lam.invoke(FunctionName=fname)
            payload = json.loads(response["Payload"].read())
            assert payload["a"] == "a1"
            assert payload["b"] == "b2"
            assert payload["c"] == "c3"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_env_vars_in_configuration(self, lam, role):
        """Test environment variables appear in get_function_configuration."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"env-cfg-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"DB_HOST": "localhost", "DB_PORT": "5432"}},
        )
        try:
            cfg = lam.get_function_configuration(FunctionName=fname)
            env_vars = cfg["Environment"]["Variables"]
            assert env_vars["DB_HOST"] == "localhost"
            assert env_vars["DB_PORT"] == "5432"
        finally:
            lam.delete_function(FunctionName=fname)


class TestLambdaConcurrencyAdvanced:
    def test_update_reserved_concurrency(self, lam, role):
        """Test updating reserved concurrency from one value to another."""
        code = _make_zip("def handler(e, c): pass")
        fname = f"conc-upd-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        try:
            lam.put_function_concurrency(
                FunctionName=fname, ReservedConcurrentExecutions=5
            )
            resp = lam.get_function_concurrency(FunctionName=fname)
            assert resp["ReservedConcurrentExecutions"] == 5

            lam.put_function_concurrency(
                FunctionName=fname, ReservedConcurrentExecutions=20
            )
            resp = lam.get_function_concurrency(FunctionName=fname)
            assert resp["ReservedConcurrentExecutions"] == 20
        finally:
            lam.delete_function(FunctionName=fname)
