"""Unit tests for the STS native provider."""

import asyncio
import json
import re
from unittest.mock import AsyncMock, MagicMock, patch
from xml.etree import ElementTree as ET

from robotocore.services.sts.provider import (
    _assume_role_with_saml,
    _decode_authorization_message,
    _get_access_key_info,
    _pack_policy,
    handle_sts_request,
)


def _make_request(body: bytes = b"", headers: dict | None = None):
    request = MagicMock()
    request.body = AsyncMock(return_value=body)
    request.headers = headers or {}
    request.method = "POST"
    request.url = MagicMock()
    request.url.path = "/"
    request.url.query = None
    return request


class TestSTSProvider:
    def test_get_access_key_info(self):
        body = b"Action=GetAccessKeyInfo&AccessKeyId=AKIAIOSFODNN7EXAMPLE"
        request = _make_request(body)
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        assert response.status_code == 200
        assert b"<Account>123456789012</Account>" in response.body

    def test_get_access_key_info_different_account(self):
        body = b"Action=GetAccessKeyInfo&AccessKeyId=AKIAIOSFODNN7EXAMPLE"
        request = _make_request(body)
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "999888777666")
        )
        assert b"<Account>999888777666</Account>" in response.body

    @patch("robotocore.services.sts.provider.forward_to_moto_with_body")
    def test_non_intercepted_action_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        body = b"Action=GetCallerIdentity"
        request = _make_request(body)
        asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        mock_forward.assert_called_once_with(request, "sts", body)

    @patch("robotocore.services.sts.provider.forward_to_moto_with_body")
    def test_assume_role_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        body = b"Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=s"
        request = _make_request(body)
        asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        mock_forward.assert_called_once()

    def test_assume_role_packed_policy_too_large(self):
        """AssumeRole with an oversized packed policy returns PackedPolicyTooLarge error."""
        # Build a policy that exceeds 2048 bytes when packed
        large_policy = (
            '{"Version":"2012-10-17","Statement":['
            + ",".join(
                [
                    '{"Effect":"Allow","Action":"s3:GetObject",'
                    f'"Resource":"arn:aws:s3:::bucket-{i}/*"}}'
                    for i in range(100)
                ]
            )
            + "]}"
        )
        body = (
            b"Action=AssumeRole"
            b"&RoleArn=arn:aws:iam::123456789012:role/test"
            b"&RoleSessionName=s"
            b"&Policy=" + large_policy.encode()
        )
        request = _make_request(body)
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        assert response.status_code == 400
        assert b"PackedPolicyTooLarge" in response.body

    def test_assume_role_with_saml_returns_credentials(self):
        """AssumeRoleWithSAML bypasses SAML validation and returns credentials."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::123456789012:role/SAMLRole"
            b"&PrincipalArn=arn:aws:iam::123456789012:saml-provider/MyIdP"
            b"&SAMLAssertion=not-real-saml"
        )
        request = _make_request(body)
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        assert response.status_code == 200
        assert b"<Credentials>" in response.body
        assert b"<AccessKeyId>" in response.body

    def test_decode_authorization_message(self):
        """DecodeAuthorizationMessage returns the encoded message as-is."""
        body = b"Action=DecodeAuthorizationMessage&EncodedMessage=test-encoded-msg"
        request = _make_request(body)
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        assert response.status_code == 200
        assert b"<DecodedMessage>test-encoded-msg</DecodedMessage>" in response.body

    def test_empty_action_forwards_to_moto(self):
        """A request with no Action parameter forwards to Moto."""
        with patch(
            "robotocore.services.sts.provider.forward_to_moto_with_body",
            new_callable=AsyncMock,
        ) as mock_forward:
            mock_forward.return_value = MagicMock(status_code=200)
            body = b""
            request = _make_request(body)
            asyncio.get_event_loop().run_until_complete(
                handle_sts_request(request, "us-east-1", "123456789012")
            )
            mock_forward.assert_called_once()


# ─── Bug-revealing tests below ───────────────────────────────────────────
# Each test targets a specific correctness bug and is expected to FAIL.

NS = "https://sts.amazonaws.com/doc/2011-06-15/"


def _parse_xml(response) -> ET.Element:
    body = response.body if isinstance(response.body, bytes) else response.body.encode()
    return ET.fromstring(body)


class TestGetAccessKeyInfoBugs:
    def test_missing_access_key_id_should_error(self):
        """Bug: GetAccessKeyInfo returns 200 even when AccessKeyId is missing.

        AWS returns MissingParameter error for missing required AccessKeyId.
        """
        params = {}
        response = _get_access_key_info(params, "123456789012")
        assert response.status_code == 400, (
            "GetAccessKeyInfo should reject requests missing the required AccessKeyId parameter"
        )


class TestDecodeAuthorizationMessageBugs:
    def test_empty_encoded_message_should_error(self):
        """Bug: DecodeAuthorizationMessage returns 200 for empty EncodedMessage.

        AWS returns InvalidAuthorizationMessageException when EncodedMessage is empty.
        """
        params = {"EncodedMessage": ""}
        response = _decode_authorization_message(params)
        assert response.status_code == 400, (
            "DecodeAuthorizationMessage should reject empty EncodedMessage"
        )

    def test_missing_encoded_message_should_error(self):
        """Bug: DecodeAuthorizationMessage returns 200 when EncodedMessage is absent.

        AWS returns MissingParameter error.
        """
        params = {}
        response = _decode_authorization_message(params)
        assert response.status_code == 400, (
            "DecodeAuthorizationMessage should reject missing EncodedMessage"
        )


class TestAssumeRoleWithSAMLValidationBugs:
    def test_missing_saml_assertion_should_error(self):
        """Bug: AssumeRoleWithSAML succeeds without SAMLAssertion.

        AWS requires SAMLAssertion and returns MissingParameter if absent.
        """
        params = {
            "RoleArn": "arn:aws:iam::123456789012:role/TestRole",
            "PrincipalArn": "arn:aws:iam::123456789012:saml-provider/TestProvider",
        }
        response = _assume_role_with_saml(params, "123456789012")
        assert response.status_code == 400, (
            "AssumeRoleWithSAML should reject requests missing SAMLAssertion"
        )

    def test_missing_role_arn_should_error(self):
        """Bug: AssumeRoleWithSAML succeeds without RoleArn.

        AWS requires RoleArn. The provider defaults to "" and generates a
        broken assumed-role ARN like 'arn:aws:sts::123:assumed-role//saml-session-xxx'.
        """
        params = {
            "PrincipalArn": "arn:aws:iam::123456789012:saml-provider/TestProvider",
            "SAMLAssertion": "PHNhbWw+PC9zYW1sPg==",
        }
        response = _assume_role_with_saml(params, "123456789012")
        assert response.status_code == 400, (
            "AssumeRoleWithSAML should reject requests missing RoleArn"
        )

    def test_duration_below_minimum_should_error(self):
        """Bug: AssumeRoleWithSAML accepts DurationSeconds=1.

        AWS minimum is 900 seconds. Values below should return ValidationError.
        """
        params = {
            "RoleArn": "arn:aws:iam::123456789012:role/TestRole",
            "PrincipalArn": "arn:aws:iam::123456789012:saml-provider/TestProvider",
            "SAMLAssertion": "PHNhbWw+PC9zYW1sPg==",
            "DurationSeconds": "1",
        }
        response = _assume_role_with_saml(params, "123456789012")
        assert response.status_code == 400, "AssumeRoleWithSAML should reject DurationSeconds < 900"

    def test_duration_above_maximum_should_error(self):
        """Bug: AssumeRoleWithSAML accepts DurationSeconds=99999.

        AWS maximum is 43200 seconds. Values above should return ValidationError.
        """
        params = {
            "RoleArn": "arn:aws:iam::123456789012:role/TestRole",
            "PrincipalArn": "arn:aws:iam::123456789012:saml-provider/TestProvider",
            "SAMLAssertion": "PHNhbWw+PC9zYW1sPg==",
            "DurationSeconds": "99999",
        }
        response = _assume_role_with_saml(params, "123456789012")
        assert response.status_code == 400, (
            "AssumeRoleWithSAML should reject DurationSeconds > 43200"
        )


class TestAssumeRoleWithSAMLResponseBugs:
    def test_packed_policy_size_reflects_actual_policy(self):
        """Bug: PackedPolicySize is hardcoded to 0 even when Policy is provided.

        When a session policy is attached, PackedPolicySize should reflect
        the percentage of the allowed size that was consumed.
        """
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
            }
        )
        params = {
            "RoleArn": "arn:aws:iam::123456789012:role/TestRole",
            "PrincipalArn": "arn:aws:iam::123456789012:saml-provider/TestProvider",
            "SAMLAssertion": "PHNhbWw+PC9zYW1sPg==",
            "Policy": policy,
        }
        response = _assume_role_with_saml(params, "123456789012")
        root = _parse_xml(response)
        packed_size_el = root.find(f".//{{{NS}}}PackedPolicySize")
        assert packed_size_el is not None
        size_val = int(packed_size_el.text)
        assert size_val > 0, (
            "PackedPolicySize should reflect actual policy size, not be hardcoded to 0"
        )

    def test_access_key_uses_valid_aws_charset(self):
        """Bug: Access key suffix uses hex (0-9,A-F) instead of base32 (A-Z,2-7).

        Real AWS temporary access keys with ASIA prefix use base32 encoding
        for the remaining characters, not hexadecimal. Hex produces chars
        like '0', '1', '8', '9' that never appear in real AWS keys.
        """
        params = {
            "RoleArn": "arn:aws:iam::123456789012:role/TestRole",
            "PrincipalArn": "arn:aws:iam::123456789012:saml-provider/TestProvider",
            "SAMLAssertion": "PHNhbWw+PC9zYW1sPg==",
        }
        response = _assume_role_with_saml(params, "123456789012")
        root = _parse_xml(response)
        access_key = root.find(f".//{{{NS}}}AccessKeyId").text
        suffix = access_key[4:]  # strip ASIA prefix
        # Base32 charset: A-Z and 2-7. Hex would include 0,1,8,9.
        assert re.match(r"^[A-Z2-7]+$", suffix), (
            f"Access key suffix '{suffix}' uses hex charset instead of base32. "
            f"Real AWS keys use A-Z and 2-7 only."
        )


class TestAssumeRolePolicyValidationBugs:
    @patch("robotocore.services.sts.provider.forward_to_moto_with_body", new_callable=AsyncMock)
    def test_invalid_json_policy_should_error(self, mock_forward):
        """Bug: AssumeRole forwards invalid JSON policy to Moto without validation.

        AWS returns MalformedPolicyDocument for non-JSON policy strings.
        The provider's _pack_policy silently returns the raw string.
        """
        mock_forward.return_value = MagicMock(status_code=200)
        body = (
            b"Action=AssumeRole"
            b"&RoleArn=arn:aws:iam::123456789012:role/Test"
            b"&RoleSessionName=test"
            b"&Policy=not-valid-json"
        )
        request = _make_request(body)
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        assert response.status_code == 400, (
            "AssumeRole should reject invalid JSON in Policy parameter"
        )


class TestPackPolicyBugs:
    def test_invalid_json_should_not_pass_through(self):
        """Bug: _pack_policy returns invalid JSON unchanged instead of raising.

        This means size validation uses the raw string length, which differs
        from AWS behavior (AWS rejects invalid JSON before checking size).
        """
        result = _pack_policy("this is not json")
        assert result != "this is not json", (
            "_pack_policy should not silently return invalid JSON unchanged"
        )
