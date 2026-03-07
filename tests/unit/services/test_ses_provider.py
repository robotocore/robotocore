"""Unit tests for the SES native provider."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robotocore.services.ses.provider import (
    SesError,
    _delete_receipt_rule,
    _get_account_sending_enabled,
    _list_identities,
    handle_ses_request,
)


def _make_request(body: bytes = b"", content_type: str = "application/x-www-form-urlencoded"):
    request = MagicMock()
    request.body = AsyncMock(return_value=body)
    request.headers = {"content-type": content_type}
    request.method = "POST"
    request.url = MagicMock()
    request.url.path = "/"
    request.url.query = None
    return request


class TestSESProvider:
    @patch("robotocore.services.ses.provider._get_ses_backend")
    def test_list_identities_with_max_items(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.list_identities.return_value = [
            "a@example.com",
            "b@example.com",
            "c@example.com",
        ]
        mock_backend_fn.return_value = mock_backend

        result = _list_identities({"MaxItems": "2"}, "us-east-1", "123456789012")
        assert "<member>a@example.com</member>" in result
        assert "<member>b@example.com</member>" in result
        assert "c@example.com" not in result

    @patch("robotocore.services.ses.provider._get_ses_backend")
    def test_list_identities_no_max(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.list_identities.return_value = ["a@example.com"]
        mock_backend_fn.return_value = mock_backend

        result = _list_identities({}, "us-east-1", "123456789012")
        assert "<member>a@example.com</member>" in result

    def test_get_account_sending_enabled(self):
        result = _get_account_sending_enabled({}, "us-east-1", "123456789012")
        assert "<Enabled>true</Enabled>" in result

    @patch("robotocore.services.ses.provider._get_ses_backend")
    def test_delete_receipt_rule_success(self, mock_backend_fn):
        mock_rule_set = MagicMock()
        mock_rule_set.rules = [{"Name": "rule1"}, {"Name": "rule2"}]
        mock_backend = MagicMock()
        mock_backend.receipt_rule_set = {"my-set": mock_rule_set}
        mock_backend_fn.return_value = mock_backend

        result = _delete_receipt_rule(
            {"RuleSetName": "my-set", "RuleName": "rule1"}, "us-east-1", "123456789012"
        )
        assert result == ""
        assert len(mock_rule_set.rules) == 1

    @patch("robotocore.services.ses.provider._get_ses_backend")
    def test_delete_receipt_rule_nonexistent_set(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_backend.receipt_rule_set = {}
        mock_backend_fn.return_value = mock_backend

        with pytest.raises(SesError) as exc:
            _delete_receipt_rule(
                {"RuleSetName": "missing-set", "RuleName": "rule1"}, "us-east-1", "123456789012"
            )
        assert exc.value.code == "RuleSetDoesNotExist"

    def test_delete_receipt_rule_requires_params(self):
        with pytest.raises(SesError) as exc:
            _delete_receipt_rule({}, "us-east-1", "123456789012")
        assert exc.value.code == "ValidationError"

    @patch("robotocore.services.ses.provider.forward_to_moto")
    def test_unknown_action_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        body = b"Action=VerifyEmailIdentity&EmailAddress=test@example.com"
        request = _make_request(body)
        asyncio.get_event_loop().run_until_complete(
            handle_ses_request(request, "us-east-1", "123456789012")
        )
        mock_forward.assert_called_once()
