"""Error-path tests for SSM native provider.

Phase 3A: Covers SendCommand, ListCommands, and ListCommandInvocations.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.ssm.provider import handle_ssm_request


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
class TestSendCommand:
    async def test_send_command_with_instance_ids(self):
        req = _make_request("SendCommand", {
            "DocumentName": "AWS-RunShellScript",
            "Targets": [{"Key": "instanceids", "Values": ["i-1234567890abcdef0"]}],
            "Parameters": {"commands": ["echo hello"]},
        })
        resp = await handle_ssm_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "Command" in body
        assert body["Command"]["DocumentName"] == "AWS-RunShellScript"
        assert body["Command"]["Status"] == "Success"


@pytest.mark.asyncio
class TestListCommands:
    async def test_list_commands_with_known_id(self):
        # First send a command
        send_req = _make_request("SendCommand", {
            "DocumentName": "AWS-RunShellScript",
            "Targets": [{"Key": "InstanceIds", "Values": ["i-abc"]}],
        })
        send_resp = await handle_ssm_request(send_req, "us-east-1", "123456789012")
        cmd_id = json.loads(send_resp.body)["Command"]["CommandId"]

        # Now list it
        list_req = _make_request("ListCommands", {"CommandId": cmd_id})
        list_resp = await handle_ssm_request(list_req, "us-east-1", "123456789012")
        assert list_resp.status_code == 200
        body = json.loads(list_resp.body)
        assert "Commands" in body
        assert len(body["Commands"]) == 1
        assert body["Commands"][0]["CommandId"] == cmd_id


@pytest.mark.asyncio
class TestListCommandInvocations:
    async def test_list_invocations_for_native_command(self):
        # Send a command first
        send_req = _make_request("SendCommand", {
            "DocumentName": "AWS-RunShellScript",
            "Targets": [{"Key": "InstanceIds", "Values": ["i-abc"]}],
        })
        send_resp = await handle_ssm_request(send_req, "us-east-1", "123456789012")
        cmd_id = json.loads(send_resp.body)["Command"]["CommandId"]

        # List invocations
        inv_req = _make_request("ListCommandInvocations", {"CommandId": cmd_id})
        inv_resp = await handle_ssm_request(inv_req, "us-east-1", "123456789012")
        assert inv_resp.status_code == 200
        body = json.loads(inv_resp.body)
        assert "CommandInvocations" in body
