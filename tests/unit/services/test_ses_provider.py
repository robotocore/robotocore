"""Unit tests for the SES native provider."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robotocore.services.ses.provider import (
    SesError,
    _delete_receipt_rule,
    _get_account_sending_enabled,
    _list_identities,
    _set_identity_dkim_enabled,
    _set_identity_notification_topic,
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
        asyncio.run(handle_ses_request(request, "us-east-1", "123456789012"))
        mock_forward.assert_called_once()

    # -----------------------------------------------------------------------
    # Categorical bug: return type mismatch (dict vs str)
    # All action handlers must return str for _xml_response, not dict.
    # -----------------------------------------------------------------------

    def test_set_identity_dkim_enabled_returns_str(self):
        """Handlers in _ACTION_MAP must return str, not dict.

        _xml_response uses f-string interpolation, so returning {} produces
        literal '{}' in the XML body instead of empty content.
        """
        result = _set_identity_dkim_enabled({}, "us-east-1", "123456789012")
        assert isinstance(result, str), f"Handler returned {type(result).__name__}, expected str"
        assert "{}" not in result, "Dict was stringified into XML body"

    @patch("robotocore.services.ses.provider._get_ses_backend")
    def test_set_identity_dkim_enabled_via_dispatch_produces_valid_xml(self, mock_backend_fn):
        """End-to-end: SetIdentityDkimEnabled should produce clean XML, not '{}' in body."""
        mock_backend_fn.return_value = MagicMock()
        body = b"Action=SetIdentityDkimEnabled&Identity=example.com&DkimEnabled=true"
        request = _make_request(body)
        response = asyncio.run(handle_ses_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 200
        # The response body should NOT contain literal '{}'
        content = response.body.decode() if hasattr(response, "body") else ""
        assert "{}" not in content, f"Dict stringified in XML: {content}"

    # -----------------------------------------------------------------------
    # Categorical bug: notification topic set on nonexistent identity
    # AWS returns InvalidParameterValue; our provider blindly writes.
    # -----------------------------------------------------------------------

    @patch("robotocore.services.ses.provider._get_ses_backend")
    def test_set_notification_topic_nonexistent_identity_errors(self, mock_backend_fn):
        """Setting notification topic for identity that doesn't exist should error."""
        mock_backend = MagicMock()
        mock_backend.sns_topics = {}
        # Simulate: identity not verified (not in addresses or domains)
        mock_backend.email_identities = {}
        mock_backend.domains = {}
        mock_backend_fn.return_value = mock_backend

        with pytest.raises(SesError) as exc:
            _set_identity_notification_topic(
                {
                    "Identity": "nonexistent@example.com",
                    "NotificationType": "Bounce",
                    "SnsTopic": "arn:aws:sns:us-east-1:123456789012:my-topic",
                },
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "InvalidParameterValue"

    @patch("robotocore.services.ses.provider._get_ses_backend")
    def test_set_notification_topic_verified_identity_succeeds(self, mock_backend_fn):
        """Setting notification topic for a verified identity should work."""
        mock_backend = MagicMock()
        mock_backend.sns_topics = {}
        mock_backend.email_identities = {"user@example.com": MagicMock()}
        mock_backend.domains = {}
        mock_backend_fn.return_value = mock_backend

        result = _set_identity_notification_topic(
            {
                "Identity": "user@example.com",
                "NotificationType": "Bounce",
                "SnsTopic": "arn:aws:sns:us-east-1:123456789012:my-topic",
            },
            "us-east-1",
            "123456789012",
        )
        assert result == ""
        assert mock_backend.sns_topics["user@example.com"]["Bounce"] == (
            "arn:aws:sns:us-east-1:123456789012:my-topic"
        )

    @patch("robotocore.services.ses.provider._get_ses_backend")
    def test_set_notification_topic_domain_identity_succeeds(self, mock_backend_fn):
        """Domain identities should also be accepted."""
        mock_backend = MagicMock()
        mock_backend.sns_topics = {}
        mock_backend.email_identities = {}
        mock_backend.domains = {"example.com": MagicMock()}
        mock_backend_fn.return_value = mock_backend

        result = _set_identity_notification_topic(
            {
                "Identity": "example.com",
                "NotificationType": "Complaint",
                "SnsTopic": "arn:aws:sns:us-east-1:123456789012:complaints",
            },
            "us-east-1",
            "123456789012",
        )
        assert result == ""

    # -----------------------------------------------------------------------
    # Categorical bug: orphaned state on resource deletion
    # When an identity is deleted, sns_topics and mail_from configs linger.
    # -----------------------------------------------------------------------

    @patch("robotocore.services.ses.provider._get_ses_backend")
    def test_notification_topic_cleared_on_identity_clear(self, mock_backend_fn):
        """Clearing a notification topic (SnsTopic='') should remove the key."""
        mock_backend = MagicMock()
        mock_backend.sns_topics = {
            "user@example.com": {"Bounce": "arn:aws:sns:us-east-1:123:topic"}
        }
        mock_backend.email_identities = {"user@example.com": MagicMock()}
        mock_backend.domains = {}
        mock_backend_fn.return_value = mock_backend

        _set_identity_notification_topic(
            {
                "Identity": "user@example.com",
                "NotificationType": "Bounce",
                "SnsTopic": "",
            },
            "us-east-1",
            "123456789012",
        )
        # The Bounce key should be removed, not set to empty string
        assert "Bounce" not in mock_backend.sns_topics["user@example.com"]
