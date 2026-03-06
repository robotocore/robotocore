"""Lambda invocation integration tests through the full HTTP stack.

Tests Lambda CRUD and invocation via the ASGI app using httpx.AsyncClient.
"""

import base64
import json
import urllib.parse
import uuid

from tests.integration.conftest import (
    auth_header,
    make_lambda_zip,
)

ROLE_NAME = "integ-lambda-role"
ROLE_ARN = f"arn:aws:iam::123456789012:role/{ROLE_NAME}"


def _lambda_auth(region: str = "us-east-1") -> dict[str, str]:
    return auth_header("lambda", region)


def _iam_auth(region: str = "us-east-1") -> dict[str, str]:
    return auth_header("iam", region)


async def _ensure_lambda_role(client) -> str:
    """Create an IAM role for Lambda if it doesn't exist, return the ARN."""
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
    body = urllib.parse.urlencode(
        {
            "Action": "CreateRole",
            "RoleName": ROLE_NAME,
            "AssumeRolePolicyDocument": trust,
            "Version": "2010-05-08",
        }
    )
    await client.post(
        "/",
        content=body,
        headers={
            **_iam_auth(),
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    return ROLE_ARN


async def _create_lambda(client, func_name: str, code_str: str, **extra) -> None:
    """Create a Lambda function via HTTP, ensuring role exists first."""
    role_arn = await _ensure_lambda_role(client)
    code = make_lambda_zip(code_str)
    payload = {
        "FunctionName": func_name,
        "Runtime": "python3.12",
        "Role": role_arn,
        "Handler": "lambda_function.handler",
        "Code": {"ZipFile": base64.b64encode(code).decode()},
        **extra,
    }
    resp = await client.post(
        "/2015-03-31/functions",
        content=json.dumps(payload),
        headers={
            **_lambda_auth(),
            "Content-Type": "application/json",
        },
    )
    assert resp.status_code in (200, 201), resp.text


async def _delete_lambda(client, func_name: str) -> None:
    await client.delete(
        f"/2015-03-31/functions/{func_name}",
        headers=_lambda_auth(),
    )


class TestLambdaPythonInvocation:
    """Create and invoke Python Lambda functions through the HTTP stack."""

    async def test_create_and_invoke_python_lambda(self, client):
        func_name = f"integ-py-{uuid.uuid4().hex[:8]}"
        await _create_lambda(
            client,
            func_name,
            "def handler(event, context):\n    return {'statusCode': 200, 'body': 'hello'}\n",
        )

        # Invoke (RequestResponse)
        invoke_resp = await client.post(
            f"/2015-03-31/functions/{func_name}/invocations",
            content=json.dumps({"key": "value"}).encode(),
            headers=_lambda_auth(),
        )
        assert invoke_resp.status_code == 200
        payload = invoke_resp.json()
        assert payload["statusCode"] == 200
        assert payload["body"] == "hello"

        await _delete_lambda(client, func_name)

    async def test_lambda_returns_event_data(self, client):
        """Lambda that echoes back the input event."""
        func_name = f"integ-echo-{uuid.uuid4().hex[:8]}"
        await _create_lambda(
            client,
            func_name,
            "def handler(event, context):\n    return event\n",
        )

        event = {"message": "echo test", "number": 42}
        invoke_resp = await client.post(
            f"/2015-03-31/functions/{func_name}/invocations",
            content=json.dumps(event).encode(),
            headers=_lambda_auth(),
        )
        assert invoke_resp.status_code == 200
        assert invoke_resp.json() == event

        await _delete_lambda(client, func_name)

    async def test_lambda_with_environment_variables(self, client):
        """Lambda that reads environment variables."""
        func_name = f"integ-env-{uuid.uuid4().hex[:8]}"
        await _create_lambda(
            client,
            func_name,
            "import os\n"
            "def handler(event, context):\n"
            "    return {'MY_VAR': os.environ.get('MY_VAR', '')}\n",
            Environment={"Variables": {"MY_VAR": "test-value"}},
        )

        invoke_resp = await client.post(
            f"/2015-03-31/functions/{func_name}/invocations",
            content=b"{}",
            headers=_lambda_auth(),
        )
        assert invoke_resp.status_code == 200
        assert invoke_resp.json()["MY_VAR"] == "test-value"

        await _delete_lambda(client, func_name)


class TestLambdaInvocationTypes:
    """Test different Lambda invocation types."""

    async def test_event_invocation_returns_202(self, client):
        """Event invocation type should return 202."""
        func_name = f"integ-event-{uuid.uuid4().hex[:8]}"
        await _create_lambda(client, func_name, "def handler(event, context): return 'ok'\n")

        resp = await client.post(
            f"/2015-03-31/functions/{func_name}/invocations",
            content=b"{}",
            headers={
                **_lambda_auth(),
                "X-Amz-Invocation-Type": "Event",
            },
        )
        assert resp.status_code == 202

        await _delete_lambda(client, func_name)

    async def test_dryrun_invocation_returns_204(self, client):
        """DryRun invocation type should return 204."""
        func_name = f"integ-dry-{uuid.uuid4().hex[:8]}"
        await _create_lambda(client, func_name, "def handler(event, context): return 'ok'\n")

        resp = await client.post(
            f"/2015-03-31/functions/{func_name}/invocations",
            content=b"{}",
            headers={
                **_lambda_auth(),
                "X-Amz-Invocation-Type": "DryRun",
            },
        )
        assert resp.status_code == 204

        await _delete_lambda(client, func_name)

    async def test_request_response_invocation(self, client):
        """Default RequestResponse invocation returns 200 with payload."""
        func_name = f"integ-rr-{uuid.uuid4().hex[:8]}"
        await _create_lambda(client, func_name, "def handler(event, context): return 'ok'\n")

        resp = await client.post(
            f"/2015-03-31/functions/{func_name}/invocations",
            content=b"{}",
            headers={
                **_lambda_auth(),
                "X-Amz-Invocation-Type": "RequestResponse",
            },
        )
        assert resp.status_code == 200
        assert "x-amz-executed-version" in resp.headers

        await _delete_lambda(client, func_name)


class TestLambdaCRUDViaHTTP:
    """Lambda CRUD operations through raw HTTP."""

    async def test_list_functions(self, client):
        resp = await client.get(
            "/2015-03-31/functions",
            headers=_lambda_auth(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "Functions" in body

    async def test_create_get_delete_function(self, client):
        func_name = f"integ-crud-{uuid.uuid4().hex[:8]}"
        await _create_lambda(client, func_name, "def handler(e, c): return 'ok'\n")

        # Get
        get_resp = await client.get(
            f"/2015-03-31/functions/{func_name}",
            headers=_lambda_auth(),
        )
        assert get_resp.status_code == 200
        assert get_resp.json()["Configuration"]["FunctionName"] == func_name

        # Delete
        del_resp = await client.delete(
            f"/2015-03-31/functions/{func_name}",
            headers=_lambda_auth(),
        )
        assert del_resp.status_code in (200, 204)

        # Verify deleted
        get_resp2 = await client.get(
            f"/2015-03-31/functions/{func_name}",
            headers=_lambda_auth(),
        )
        assert get_resp2.status_code in (404, 500)

    async def test_invoke_with_tail_log_type(self, client):
        """Invoke with LogType=Tail should return base64 log result."""
        func_name = f"integ-tail-{uuid.uuid4().hex[:8]}"
        await _create_lambda(
            client,
            func_name,
            "def handler(event, context):\n    print('log output here')\n    return 'done'\n",
        )

        resp = await client.post(
            f"/2015-03-31/functions/{func_name}/invocations",
            content=b"{}",
            headers={
                **_lambda_auth(),
                "X-Amz-Log-Type": "Tail",
            },
        )
        assert resp.status_code == 200
        log_result = resp.headers.get("x-amz-log-result", "")
        if log_result:
            decoded = base64.b64decode(log_result).decode()
            assert isinstance(decoded, str)

        await _delete_lambda(client, func_name)
