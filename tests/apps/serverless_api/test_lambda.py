"""
Lambda function lifecycle and invocation tests.

Tests deployment, invocation with payloads, code updates, environment
variables, listing, versioning, and deletion.
"""

import json

import pytest

from .handlers import HELLO_HANDLER
from .models import LambdaConfig


class TestLambdaOperations:
    """Lambda compute layer tests."""

    @pytest.fixture
    def hello_function(self, serverless_app, unique_name):
        """Deploy a simple hello-world Lambda function."""
        role_arn = serverless_app.create_lambda_role(f"lambda-role-{unique_name}")
        config = LambdaConfig(
            function_name=f"hello-{unique_name}",
            handler="index.handler",
            runtime="python3.12",
            code=HELLO_HANDLER,
        )
        fn_arn = serverless_app.deploy_function(config, role_arn)
        yield serverless_app, config.function_name, fn_arn

    def test_create_and_invoke(self, hello_function):
        """Create a function and invoke it with no payload."""
        app, fn_name, _ = hello_function
        result = app.invoke_function(fn_name)
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert "Hello" in body["message"]

    def test_invoke_with_payload(self, hello_function):
        """Invoke with a payload and verify handler receives the event."""
        app, fn_name, _ = hello_function
        result = app.invoke_function(fn_name, payload={"name": "Alice"})
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["message"] == "Hello, Alice!"

    def test_update_function_code(self, hello_function):
        """Update function code and verify the new behavior."""
        app, fn_name, _ = hello_function

        new_code = """
import json

def handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Updated handler!", "version": 2}),
    }
"""
        app.update_function_code(fn_name, new_code)
        result = app.invoke_function(fn_name)
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["message"] == "Updated handler!"
        assert body["version"] == 2

    def test_function_with_env_vars(self, serverless_app, unique_name):
        """Deploy a function with environment variables."""
        role_arn = serverless_app.create_lambda_role(f"env-role-{unique_name}")

        env_handler = """
import json
import os

def handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({
            "app_name": os.environ.get("APP_NAME", "unknown"),
            "stage": os.environ.get("STAGE", "unknown"),
        }),
    }
"""
        config = LambdaConfig(
            function_name=f"env-fn-{unique_name}",
            handler="index.handler",
            runtime="python3.12",
            code=env_handler,
            env_vars={"APP_NAME": "my-api", "STAGE": "test"},
        )
        serverless_app.deploy_function(config, role_arn)
        result = serverless_app.invoke_function(config.function_name)
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["app_name"] == "my-api"
        assert body["stage"] == "test"

    def test_list_functions(self, hello_function):
        """List functions and verify the created function appears."""
        app, fn_name, _ = hello_function
        functions = app.list_functions()
        names = [f["FunctionName"] for f in functions]
        assert fn_name in names

    def test_delete_function(self, serverless_app, unique_name):
        """Delete a function and verify it is gone."""
        role_arn = serverless_app.create_lambda_role(f"del-role-{unique_name}")
        config = LambdaConfig(
            function_name=f"to-delete-{unique_name}",
            handler="index.handler",
            runtime="python3.12",
            code=HELLO_HANDLER,
        )
        serverless_app.deploy_function(config, role_arn)

        # Verify it exists
        functions = serverless_app.list_functions()
        names = [f["FunctionName"] for f in functions]
        assert config.function_name in names

        # Delete it (remove from tracking so cleanup doesn't double-delete)
        serverless_app.delete_function(config.function_name)

        # Verify it is gone
        functions = serverless_app.list_functions()
        names = [f["FunctionName"] for f in functions]
        assert config.function_name not in names
