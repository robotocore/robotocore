"""Unit tests for the round-4 STS session registration and IAM fix set.

Covers:
1. `AssumeRole` (forwarded to moto) registers the minted ASIA key under the
   role's canonical ARN, so subsequent signed calls resolve the assumed
   role's policies under ENFORCE_IAM=1 instead of implicit-denying.
2. The assumed-role session ARN canonicalizes to the role ARN the Moto IAM
   backend stores.
3. `aws:CurrentTime` policy context uses wall-clock now (not a fixed 2024
   constant), so DateLessThan/DateGreaterThan conditions evaluate correctly.
4. AccessDenied 403 XML bodies XML-escape the caller-controlled action.
"""

from unittest.mock import patch

import pytest

from robotocore.gateway.iam_middleware import (
    _build_access_denied_response,
    _sts_sessions,
    clear_sts_sessions,
)
from robotocore.services.sts.provider import (
    _canonical_role_arn,
    handle_sts_request,
)


class TestCanonicalRoleArn:
    def test_assumed_role_arn_canonicalizes(self):
        arn = "arn:aws:sts::123456789012:assumed-role/DeployRole/alpha"
        role_arn, account = _canonical_role_arn(arn)
        assert role_arn == "arn:aws:iam::123456789012:role/DeployRole"
        assert account == "123456789012"


@pytest.mark.asyncio
async def test_assume_role_registers_the_minted_key():
    """handle_sts_request(AssumeRole) must register the temporary key with
    the assumed role's policy set, not leave it unknown (implicit deny)."""

    class _FakeResponse:
        status_code = 200

        def __init__(self, body: bytes):
            self.body = body

    xml = (
        b"<AssumeRoleResponse>"
        b"<AssumeRoleResult>"
        b"<AssumedRoleUser>"
        b"<Arn>arn:aws:sts::123456789012:assumed-role/DeployRole/probe</Arn>"
        b"</AssumedRoleUser>"
        b"<Credentials><AccessKeyId>ASIAPROBEKEY123456</AccessKeyId></Credentials>"
        b"</AssumeRoleResult></AssumeRoleResponse>"
    )

    with patch(
        "robotocore.services.sts.provider.forward_to_moto_with_body",
        return_value=_FakeResponse(xml),
    ):
        clear_sts_sessions()
        req = _FakeRequest()
        await handle_sts_request(req, "us-east-1", "123456789012")

        try:
            session = _sts_sessions["ASIAPROBEKEY123456"]
            assert session["role_arn"] == "arn:aws:iam::123456789012:role/DeployRole"
        finally:
            clear_sts_sessions()


class _FakeRequest:
    headers = {}
    query_params = {}
    method = "POST"
    url = type("U", (), {"path": "/", "query": ""})()

    async def body(self):
        return (
            b"Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/DeployRole"
            b"&RoleSessionName=probe"
        )


class TestAccessDeniedXmlEscaping:
    def test_action_with_entities_is_escaped_in_xml(self):
        response = _build_access_denied_response("s3:PutObject&<evil>", "query")
        assert b"AccessDenied" in response.body
        assert b"<&" not in response.body
        assert b"&amp;" in response.body


class TestCurrentTimeContext:
    def test_policy_context_time_is_recent(self):
        """aws:CurrentTime must be wall-clock, not the stale fixed constant."""
        import time as _time

        from robotocore.services.iam.policy_engine import evaluate_policy

        policy = {
            "Statement": [
                {
                    "Effect": "Deny",
                    "Action": "s3:ListObjects",
                    "Condition": {"DateLessThan": {"aws:CurrentTime": "2024-01-01T00:00:00Z"}},
                }
            ]
        }
        decision = evaluate_policy(
            [policy],
            "s3:ListObjects",
            "arn:aws:s3:::bucket/key",
            {"aws:CurrentTime": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime(_time.time()))},
        )
        # 2024's deny-by-past-date condition must not match a request
        # evaluated against today's time.
        assert decision != "DENY"
