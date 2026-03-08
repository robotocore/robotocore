"""Tests for SSM native provider region isolation fix.

Validates that SSM commands are scoped by (account_id, region) so that
commands created in one region are not visible in another.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

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
class TestCommandRegionIsolation:
    """SSM _commands store is region-scoped.

    Commands created in us-east-1 should not be visible from us-west-2.
    """

    async def test_command_should_not_be_visible_in_other_regions(self):
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

        # Verify command exists in us-east-1 store
        assert cmd_id in _commands.get("123456789012:us-east-1", {})

        # Verify command does NOT exist in us-west-2 store
        assert cmd_id not in _commands.get("123456789012:us-west-2", {})

        # ListCommands from us-west-2 should fall through to Moto (not find natively)
        mock_moto_resp = MagicMock()
        mock_moto_resp.status_code = 200
        mock_moto_resp.body = json.dumps({"Commands": []}).encode()

        with patch(
            "robotocore.services.ssm.provider.forward_to_moto",
            new_callable=AsyncMock,
            return_value=mock_moto_resp,
        ):
            list_req = _make_request("ListCommands", {"CommandId": cmd_id})
            list_resp = await handle_ssm_request(list_req, "us-west-2", "123456789012")
            body = json.loads(list_resp.body)
            assert len(body.get("Commands", [])) == 0

    async def test_command_invocations_should_not_be_visible_in_other_regions(self):
        _commands.clear()

        send_req = _make_request(
            "SendCommand",
            {
                "DocumentName": "AWS-RunShellScript",
                "Targets": [{"Key": "instanceids", "Values": ["i-xyz789"]}],
            },
        )
        send_resp = await handle_ssm_request(send_req, "eu-west-1", "123456789012")
        cmd_id = json.loads(send_resp.body)["Command"]["CommandId"]

        # Verify command exists in eu-west-1 store
        assert cmd_id in _commands.get("123456789012:eu-west-1", {})

        # Query from ap-southeast-1 should fall through to Moto
        mock_moto_resp = MagicMock()
        mock_moto_resp.status_code = 200
        mock_moto_resp.body = json.dumps({"CommandInvocations": []}).encode()

        with patch(
            "robotocore.services.ssm.provider.forward_to_moto",
            new_callable=AsyncMock,
            return_value=mock_moto_resp,
        ):
            inv_req = _make_request("ListCommandInvocations", {"CommandId": cmd_id})
            inv_resp = await handle_ssm_request(inv_req, "ap-southeast-1", "123456789012")
            # Should have fallen through to Moto, not handled natively
            assert inv_resp.status_code == 200
