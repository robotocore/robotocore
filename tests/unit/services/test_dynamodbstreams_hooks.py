"""Tests for robotocore.services.dynamodbstreams.hooks."""

from unittest.mock import MagicMock, patch

import robotocore.services.dynamodbstreams.hooks as hooks_module
from robotocore.services.dynamodbstreams.hooks import get_store, notify_table_change
from robotocore.services.dynamodbstreams.models import DynamoDBStreamsStore


class TestGetStore:
    def setup_method(self):
        hooks_module._stores.clear()

    def test_returns_store_instance(self):
        store = get_store("us-east-1")
        assert isinstance(store, DynamoDBStreamsStore)

    def test_same_region_returns_same_store(self):
        s1 = get_store("us-east-1")
        s2 = get_store("us-east-1")
        assert s1 is s2

    def test_different_regions_return_different_stores(self):
        s1 = get_store("us-east-1")
        s2 = get_store("eu-west-1")
        assert s1 is not s2

    def test_default_region(self):
        store = get_store()
        assert store is get_store("us-east-1")


class TestNotifyTableChange:
    def setup_method(self):
        hooks_module._stores.clear()

    @patch("robotocore.services.dynamodbstreams.hooks.get_store")
    @patch("moto.backends.get_backend")
    def test_records_change_for_streaming_table(self, mock_get_backend, mock_get_store):
        # Set up mock Moto table with streaming enabled
        mock_table = MagicMock()
        mock_table.latest_stream_label = "2024-01-01T00:00:00.000"
        mock_table.table_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Orders"
        mock_table.stream_specification = {"StreamViewType": "NEW_AND_OLD_IMAGES"}

        mock_backend = MagicMock()
        mock_backend.get_table.return_value = mock_table
        mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}

        mock_store = MagicMock()
        mock_get_store.return_value = mock_store

        notify_table_change(
            table_name="Orders",
            event_name="INSERT",
            keys={"pk": {"S": "order-1"}},
            new_image={"pk": {"S": "order-1"}, "status": {"S": "new"}},
            old_image=None,
            region="us-east-1",
            account_id="123456789012",
        )

        mock_store.record_change.assert_called_once_with(
            table_name="Orders",
            event_name="INSERT",
            keys={"pk": {"S": "order-1"}},
            new_image={"pk": {"S": "order-1"}, "status": {"S": "new"}},
            old_image=None,
            region="us-east-1",
            account_id="123456789012",
            stream_arn="arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2024-01-01T00:00:00.000",
            view_type="NEW_AND_OLD_IMAGES",
        )

    @patch("moto.backends.get_backend")
    def test_no_stream_enabled_does_nothing(self, mock_get_backend):
        mock_table = MagicMock()
        mock_table.latest_stream_label = None  # No stream

        mock_backend = MagicMock()
        mock_backend.get_table.return_value = mock_table
        mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}

        # Should not raise
        notify_table_change(
            table_name="t",
            event_name="INSERT",
            keys={},
            new_image=None,
            old_image=None,
            region="us-east-1",
            account_id="123456789012",
        )

    @patch("moto.backends.get_backend")
    def test_no_table_does_nothing(self, mock_get_backend):
        mock_backend = MagicMock()
        mock_backend.get_table.return_value = None

        mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}

        # Should not raise
        notify_table_change(
            table_name="nonexistent",
            event_name="INSERT",
            keys={},
            new_image=None,
            old_image=None,
            region="us-east-1",
            account_id="123456789012",
        )

    @patch("moto.backends.get_backend", side_effect=Exception("boom"))
    def test_exception_is_caught(self, mock_get_backend):
        # Should not raise, exception is caught
        notify_table_change(
            table_name="t",
            event_name="INSERT",
            keys={},
            new_image=None,
            old_image=None,
            region="us-east-1",
            account_id="123",
        )

    @patch("robotocore.services.dynamodbstreams.hooks.get_store")
    @patch("moto.backends.get_backend")
    def test_uses_default_account_id(self, mock_get_backend, mock_get_store):
        """When account_id is 123456789012, it should use DEFAULT_ACCOUNT_ID from moto."""
        from moto.core import DEFAULT_ACCOUNT_ID

        mock_table = MagicMock()
        mock_table.latest_stream_label = "lbl"
        mock_table.table_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/t"
        mock_table.stream_specification = {"StreamViewType": "KEYS_ONLY"}

        mock_backend = MagicMock()
        mock_backend.get_table.return_value = mock_table

        # The hooks code maps "123456789012" -> DEFAULT_ACCOUNT_ID for moto lookup
        mock_get_backend.return_value = {DEFAULT_ACCOUNT_ID: {"us-east-1": mock_backend}}

        mock_store = MagicMock()
        mock_get_store.return_value = mock_store

        notify_table_change(
            table_name="t",
            event_name="MODIFY",
            keys={"pk": {"S": "1"}},
            new_image=None,
            old_image=None,
            region="us-east-1",
            account_id="123456789012",
        )

        mock_store.record_change.assert_called_once()
        call_kwargs = mock_store.record_change.call_args[1]
        assert call_kwargs["view_type"] == "KEYS_ONLY"
