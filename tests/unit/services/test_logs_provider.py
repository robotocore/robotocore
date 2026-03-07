"""Unit tests for the CloudWatch Logs provider."""

import json
from unittest.mock import MagicMock, patch

import pytest
from starlette.requests import Request

from robotocore.services.cloudwatch.logs_provider import (
    _VALID_RETENTION_DAYS,
    LogsError,
    _associate_kms_key,
    _disassociate_kms_key,
    handle_logs_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(action: str, body: dict):
    target = f"Logs_20140328.{action}"
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


# ---------------------------------------------------------------------------
# PutRetentionPolicy validation
# ---------------------------------------------------------------------------


class TestPutRetentionPolicyValidation:
    @pytest.mark.asyncio
    async def test_invalid_retention_value_returns_400(self):
        """An invalid retentionInDays value (e.g. 15) should return an error."""
        req = _make_request("PutRetentionPolicy", {
            "logGroupName": "/test/group",
            "retentionInDays": 15,
        })
        resp = await handle_logs_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert "InvalidParameterException" in data["__type"]

    @pytest.mark.asyncio
    async def test_valid_retention_value_forwards_to_moto(self):
        """A valid retentionInDays value (e.g. 7) should be forwarded to Moto."""
        req = _make_request("PutRetentionPolicy", {
            "logGroupName": "/test/group",
            "retentionInDays": 7,
        })
        with patch("robotocore.services.cloudwatch.logs_provider.forward_to_moto") as mock_fwd:
            mock_fwd.return_value = MagicMock(status_code=200, body=b"{}")
            resp = await handle_logs_request(req, "us-east-1", "123456789012")
        mock_fwd.assert_called_once()
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_zero_is_invalid(self):
        req = _make_request("PutRetentionPolicy", {
            "logGroupName": "/test/group",
            "retentionInDays": 0,
        })
        resp = await handle_logs_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    def test_valid_retention_days_set(self):
        """Verify the set contains expected values."""
        assert 1 in _VALID_RETENTION_DAYS
        assert 7 in _VALID_RETENTION_DAYS
        assert 30 in _VALID_RETENTION_DAYS
        assert 365 in _VALID_RETENTION_DAYS
        assert 3653 in _VALID_RETENTION_DAYS
        assert 15 not in _VALID_RETENTION_DAYS
        assert 0 not in _VALID_RETENTION_DAYS
        assert 999 not in _VALID_RETENTION_DAYS


# ---------------------------------------------------------------------------
# FilterLogEvents with logStreamNamePrefix
# ---------------------------------------------------------------------------


class TestFilterLogEventsStreamNamePrefix:
    @pytest.mark.asyncio
    async def test_prefix_calls_native_handler(self):
        """FilterLogEvents with logStreamNamePrefix should not just forward to Moto."""
        req = _make_request("FilterLogEvents", {
            "logGroupName": "/test/group",
            "logStreamNamePrefix": "web-",
        })
        mock_backend = MagicMock()
        mock_backend.groups = {"/test/group": MagicMock()}
        mock_backend.groups["/test/group"].streams = {
            "web-1": MagicMock(),
            "web-2": MagicMock(),
            "db-1": MagicMock(),
        }
        mock_backend.filter_log_events.return_value = ([], None, [])

        with patch("robotocore.services.cloudwatch.logs_provider.forward_to_moto") as mock_fwd:
            with patch("moto.backends.get_backend") as mock_get:
                mock_get.return_value.__getitem__ = MagicMock(
                    return_value=MagicMock(__getitem__=MagicMock(return_value=mock_backend))
                )
                resp = await handle_logs_request(req, "us-east-1", "123456789012")

        # Should NOT have forwarded to Moto
        mock_fwd.assert_not_called()
        assert resp.status_code == 200
        # Should have called filter_log_events with resolved stream names
        mock_backend.filter_log_events.assert_called_once()
        call_args = mock_backend.filter_log_events.call_args
        stream_names = call_args[0][1]
        assert "web-1" in stream_names
        assert "web-2" in stream_names
        assert "db-1" not in stream_names

    @pytest.mark.asyncio
    async def test_no_prefix_forwards_to_moto(self):
        """FilterLogEvents without logStreamNamePrefix should forward to Moto."""
        req = _make_request("FilterLogEvents", {
            "logGroupName": "/test/group",
        })
        with patch("robotocore.services.cloudwatch.logs_provider.forward_to_moto") as mock_fwd:
            mock_fwd.return_value = MagicMock(status_code=200, body=b'{"events":[]}')
            await handle_logs_request(req, "us-east-1", "123456789012")
        mock_fwd.assert_called_once()


# ---------------------------------------------------------------------------
# AssociateKmsKey / DisassociateKmsKey
# ---------------------------------------------------------------------------


class TestAssociateKmsKey:
    def test_associates_kms_key(self):
        mock_group = MagicMock()
        mock_backend = MagicMock()
        mock_backend.groups = {"/test/group": mock_group}

        with patch("moto.backends.get_backend") as mock_get:
            mock_get.return_value.__getitem__ = MagicMock(
                return_value=MagicMock(__getitem__=MagicMock(return_value=mock_backend))
            )
            result = _associate_kms_key(
                {"logGroupName": "/test/group", "kmsKeyId": "arn:aws:kms:us-east-1:123:key/abc"},
                "us-east-1",
                "123456789012",
            )
        assert result == {}
        assert mock_group.kms_key_id == "arn:aws:kms:us-east-1:123:key/abc"

    def test_not_found_raises(self):
        mock_backend = MagicMock()
        mock_backend.groups = {}

        with patch("moto.backends.get_backend") as mock_get:
            mock_get.return_value.__getitem__ = MagicMock(
                return_value=MagicMock(__getitem__=MagicMock(return_value=mock_backend))
            )
            with pytest.raises(LogsError) as exc:
                _associate_kms_key(
                    {"logGroupName": "/nope", "kmsKeyId": "arn:aws:kms:us-east-1:123:key/abc"},
                    "us-east-1",
                    "123456789012",
                )
        assert exc.value.code == "ResourceNotFoundException"

    def test_missing_kms_key_id_raises(self):
        with pytest.raises(LogsError) as exc:
            _associate_kms_key(
                {"logGroupName": "/test/group"},
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "InvalidParameterException"

    def test_missing_log_group_name_raises(self):
        with pytest.raises(LogsError) as exc:
            _associate_kms_key(
                {"kmsKeyId": "arn:aws:kms:us-east-1:123:key/abc"},
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "InvalidParameterException"


class TestDisassociateKmsKey:
    def test_disassociates_kms_key(self):
        mock_group = MagicMock()
        mock_group.kms_key_id = "arn:aws:kms:us-east-1:123:key/abc"
        mock_backend = MagicMock()
        mock_backend.groups = {"/test/group": mock_group}

        with patch("moto.backends.get_backend") as mock_get:
            mock_get.return_value.__getitem__ = MagicMock(
                return_value=MagicMock(__getitem__=MagicMock(return_value=mock_backend))
            )
            result = _disassociate_kms_key(
                {"logGroupName": "/test/group"},
                "us-east-1",
                "123456789012",
            )
        assert result == {}
        assert mock_group.kms_key_id is None

    def test_not_found_raises(self):
        mock_backend = MagicMock()
        mock_backend.groups = {}

        with patch("moto.backends.get_backend") as mock_get:
            mock_get.return_value.__getitem__ = MagicMock(
                return_value=MagicMock(__getitem__=MagicMock(return_value=mock_backend))
            )
            with pytest.raises(LogsError) as exc:
                _disassociate_kms_key(
                    {"logGroupName": "/nope"},
                    "us-east-1",
                    "123456789012",
                )
        assert exc.value.code == "ResourceNotFoundException"

    def test_missing_log_group_name_raises(self):
        with pytest.raises(LogsError) as exc:
            _disassociate_kms_key({}, "us-east-1", "123456789012")
        assert exc.value.code == "InvalidParameterException"
