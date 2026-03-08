"""Failing tests for bugs in the SSM native provider.

Each test documents a specific correctness bug. All tests should FAIL against
the current implementation, proving the bug exists.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.ssm.provider import _commands, handle_ssm_request


def _make_request(action: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {"x-amz-target": f"AmazonSSM.{action}"}
    req.method = "POST"
    req.url = MagicMock()
    req.url.path = "/"
    req.query_params = {}
    payload = json.dumps(body or {}).encode()
    req.body = AsyncMock(return_value=payload)
    return req


@pytest.mark.asyncio
class TestCommandRegionIsolationBug:
    """Bug: SSM _commands store is not region-scoped.

    Commands created in us-east-1 can be listed from us-west-2 because the
    module-level _commands dict is keyed only by account_id, not by
    (account_id, region). AWS SSM commands are regional resources.
    """

    async def test_command_should_not_be_visible_in_other_regions(self):
        # Clear module state
        _commands.clear()

        # Create a command in us-east-1
        send_req = _make_request(
            "SendCommand",
            {
                "DocumentName": "AWS-RunShellScript",
                "Targets": [{"Key": "instanceids", "Values": ["i-abc123"]}],
            },
        )
        send_resp = await handle_ssm_request(send_req, "us-east-1", "123456789012")
        assert send_resp.status_code == 200
        cmd_id = json.loads(send_resp.body)["Command"]["CommandId"]

        # Try to list it from us-west-2 — should NOT find it
        list_req = _make_request("ListCommands", {"CommandId": cmd_id})
        list_resp = await handle_ssm_request(list_req, "us-west-2", "123456789012")

        # The bug: the command is found because _commands ignores region
        body = json.loads(list_resp.body)
        assert "Commands" not in body or len(body.get("Commands", [])) == 0, (
            f"Command created in us-east-1 should not be visible in us-west-2. Got: {body}"
        )

    async def test_command_invocations_should_not_be_visible_in_other_regions(self):
        """Same bug but for ListCommandInvocations."""
        _commands.clear()

        # Create a command in eu-west-1
        send_req = _make_request(
            "SendCommand",
            {
                "DocumentName": "AWS-RunShellScript",
                "Targets": [{"Key": "instanceids", "Values": ["i-xyz789"]}],
            },
        )
        send_resp = await handle_ssm_request(send_req, "eu-west-1", "123456789012")
        cmd_id = json.loads(send_resp.body)["Command"]["CommandId"]

        # Query from ap-southeast-1 — should fall through to Moto (not found natively)
        inv_req = _make_request("ListCommandInvocations", {"CommandId": cmd_id})
        inv_resp = await handle_ssm_request(inv_req, "ap-southeast-1", "123456789012")

        # The bug: it returns 200 with empty invocations from the native store
        # because _commands lookup doesn't check region. It should fall through
        # to Moto for this region since no command was created there.
        # We verify by checking the response doesn't come from our native handler
        body = json.loads(inv_resp.body)
        # If the native handler returned this, it has CommandInvocations key with []
        # A proper implementation would not find it in the native store for this region
        # and would fall through to Moto.
        # For now, we assert the command shouldn't be findable cross-region:
        assert cmd_id not in _commands.get("123456789012", {}), (
            "This assertion will pass — the real bug is that the ListCommandInvocations "
            "lookup at line 48 finds it because it only checks account_id"
        )
        # The actual failing assertion: native handler should not have handled this
        # Since it did handle it (returning CommandInvocations), this proves the bug
        assert "CommandInvocations" not in body or inv_resp.status_code != 200, (
            "Command created in eu-west-1 should not be found via native handler "
            "when querying from ap-southeast-1"
        )
