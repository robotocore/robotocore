"""Tests for correctness bugs in the DynamoDB native provider."""

import json
from unittest.mock import AsyncMock, patch

import pytest
from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.dynamodb.provider import handle_dynamodb_request

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(target: str, body: dict) -> Request:
    """Build a fake Starlette Request with the given X-Amz-Target and JSON body."""
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(b"x-amz-target", target.encode())],
    }
    body_bytes = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": body_bytes}

    return Request(scope, receive)


def _mock_moto_response(status_code: int = 200, body: bytes = b"{}") -> Response:
    return Response(content=body, status_code=status_code)


# ---------------------------------------------------------------------------
# PutItem stream event is INSERT for new items, MODIFY for overwrites
# ---------------------------------------------------------------------------


class TestPutItemStreamEventType:
    """PutItem should fire MODIFY stream event when overwriting an existing item,
    and INSERT only for new items.
    """

    @pytest.mark.asyncio
    async def test_put_item_overwrite_should_fire_modify(self):
        """When PutItem overwrites an existing item the stream event must be MODIFY,
        not INSERT."""
        body = {"TableName": "mytable", "Item": {"id": {"S": "1"}, "val": {"S": "new"}}}
        req = _make_request("DynamoDB_20120810.PutItem", body)
        # Simulate Moto returning a successful PutItem with Attributes (indicating overwrite)
        moto_resp_body = json.dumps(
            {"Attributes": {"id": {"S": "1"}, "val": {"S": "old"}}}
        ).encode()
        mock_resp = _mock_moto_response(200, moto_resp_body)

        with (
            patch(
                "robotocore.services.dynamodb.provider.forward_to_moto",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ),
            patch("robotocore.services.dynamodbstreams.hooks.notify_table_change") as mock_notify,
        ):
            await handle_dynamodb_request(req, "us-east-1", "123456789012")

        call_kw = mock_notify.call_args[1]
        assert call_kw["event_name"] == "MODIFY", (
            f"Expected MODIFY for overwrite, got {call_kw['event_name']}"
        )
