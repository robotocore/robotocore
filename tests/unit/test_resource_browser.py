"""Unit tests for the resource browser."""

from unittest.mock import MagicMock, patch

from robotocore.resources.browser import (
    RESOURCE_ATTRS,
    get_service_resources,
)


class TestResourceAttrs:
    def test_known_services_have_attrs(self):
        """Verify the RESOURCE_ATTRS map covers core services."""
        assert "s3" in RESOURCE_ATTRS
        assert "sqs" in RESOURCE_ATTRS
        assert "dynamodb" in RESOURCE_ATTRS
        assert "lambda" in RESOURCE_ATTRS
        assert "iam" in RESOURCE_ATTRS

    def test_attrs_are_tuples(self):
        for service, attrs in RESOURCE_ATTRS.items():
            for attr, type_name in attrs:
                assert isinstance(attr, str), f"{service}.{attr} must be str"
                assert isinstance(type_name, str), f"{service}.{type_name} must be str"

    def test_s3_has_buckets(self):
        attrs = dict(RESOURCE_ATTRS["s3"])
        assert "buckets" in attrs
        assert attrs["buckets"] == "Buckets"

    def test_iam_has_roles_users_policies(self):
        attrs = dict(RESOURCE_ATTRS["iam"])
        assert "roles" in attrs
        assert "users" in attrs
        assert "policies" in attrs

    def test_all_services_have_at_least_one_attr(self):
        for service, attrs in RESOURCE_ATTRS.items():
            assert len(attrs) >= 1, f"{service} has no resource attrs"


class TestGetServiceResources:
    @patch("robotocore.resources.browser._get_backend")
    def test_returns_resources_with_names(self, mock_backend_fn):
        item1 = MagicMock()
        item1.name = "my-bucket"
        item1.arn = "arn:aws:s3:::my-bucket"
        backend = MagicMock()
        backend.buckets = {"my-bucket": item1}
        mock_backend_fn.return_value = backend

        resources = get_service_resources("s3")
        assert len(resources) == 1
        assert resources[0]["type"] == "Buckets"
        assert resources[0]["name"] == "my-bucket"
        assert resources[0]["arn"] == "arn:aws:s3:::my-bucket"

    @patch("robotocore.resources.browser._get_backend")
    def test_no_backend_returns_empty(self, mock_backend_fn):
        mock_backend_fn.return_value = None
        assert get_service_resources("nonexistent") == []

    @patch("robotocore.resources.browser._get_backend")
    def test_multiple_resource_types(self, mock_backend_fn):
        role = MagicMock()
        role.arn = "arn:aws:iam::123:role/test"
        role.name = "test-role"
        user = MagicMock()
        user.arn = "arn:aws:iam::123:user/admin"
        user.name = "admin"
        policy = MagicMock()
        policy.arn = "arn:aws:iam::123:policy/ReadOnly"
        policy.name = "ReadOnly"

        backend = MagicMock()
        backend.roles = {"test-role": role}
        backend.users = {"admin": user}
        backend.policies = {"ReadOnly": policy}
        mock_backend_fn.return_value = backend

        resources = get_service_resources("iam")
        types = {r["type"] for r in resources}
        assert "Roles" in types
        assert "Users" in types
        assert "Policies" in types

    @patch("robotocore.resources.browser._get_backend")
    def test_empty_collection_yields_no_resources(self, mock_backend_fn):
        backend = MagicMock()
        backend.buckets = {}
        mock_backend_fn.return_value = backend

        resources = get_service_resources("s3")
        assert resources == []

    @patch("robotocore.resources.browser._get_backend")
    def test_item_without_arn(self, mock_backend_fn):
        item = MagicMock(spec=[])
        item.name = "test-queue"
        backend = MagicMock()
        backend.queues = {"test-queue": item}
        mock_backend_fn.return_value = backend

        resources = get_service_resources("sqs")
        assert len(resources) == 1
        assert resources[0]["name"] == "test-queue"
        assert "arn" not in resources[0]

    @patch("robotocore.resources.browser._get_backend")
    def test_item_with_physical_resource_id(self, mock_backend_fn):
        item = MagicMock(spec=[])
        item.physical_resource_id = "arn:aws:s3:::bucket"
        item.name = "bucket"
        backend = MagicMock()
        backend.buckets = {"bucket": item}
        mock_backend_fn.return_value = backend

        resources = get_service_resources("s3")
        assert resources[0]["arn"] == "arn:aws:s3:::bucket"
