"""Advanced tests for the resource browser: get_resource_counts, generic fallback,
list-type collections, error handling in resource iteration."""

from unittest.mock import MagicMock, patch

from robotocore.resources.browser import (
    RESOURCE_ATTRS,
    _get_backend,
    get_resource_counts,
    get_service_resources,
)


class TestGetResourceCounts:
    @patch("robotocore.resources.browser._get_backend")
    def test_counts_resources_across_services(self, mock_backend_fn):
        """Verify get_resource_counts aggregates counts from mapped services."""
        bucket1 = MagicMock()
        bucket2 = MagicMock()
        s3_backend = MagicMock()
        s3_backend.buckets = {"b1": bucket1, "b2": bucket2}

        queue = MagicMock()
        sqs_backend = MagicMock()
        sqs_backend.queues = {"q1": queue}

        def _backend(svc, account_id="123456789012"):
            if svc == "s3":
                return s3_backend
            if svc == "sqs":
                return sqs_backend
            return None

        mock_backend_fn.side_effect = _backend

        counts = get_resource_counts()
        assert counts.get("s3") == 2
        assert counts.get("sqs") == 1

    @patch("robotocore.resources.browser._get_backend")
    def test_zero_count_services_excluded(self, mock_backend_fn):
        """Services with zero resources should not appear in counts."""
        backend = MagicMock()
        backend.buckets = {}  # empty
        mock_backend_fn.return_value = backend

        counts = get_resource_counts()
        assert "s3" not in counts

    @patch("robotocore.resources.browser._get_backend")
    def test_none_backend_skipped(self, mock_backend_fn):
        mock_backend_fn.return_value = None
        counts = get_resource_counts()
        assert counts == {}

    @patch("robotocore.resources.browser._get_backend")
    def test_multiple_attrs_per_service_summed(self, mock_backend_fn):
        """IAM has roles, users, policies — all should be summed."""
        backend = MagicMock()
        backend.roles = {"r1": MagicMock()}
        backend.users = {"u1": MagicMock(), "u2": MagicMock()}
        backend.policies = {"p1": MagicMock()}
        mock_backend_fn.return_value = backend

        counts = get_resource_counts()
        assert counts.get("iam") == 4  # 1 role + 2 users + 1 policy

    @patch("robotocore.resources.browser._get_backend")
    def test_attr_with_no_len_skipped(self, mock_backend_fn):
        """If an attribute doesn't support len(), it should be skipped gracefully."""
        backend = MagicMock()
        backend.buckets = MagicMock()
        backend.buckets.__len__ = MagicMock(side_effect=TypeError("no len"))
        mock_backend_fn.return_value = backend

        # Should not raise
        counts = get_resource_counts()
        assert "s3" not in counts


class TestGetServiceResourcesGenericFallback:
    @patch("robotocore.resources.browser._get_backend")
    def test_unmapped_service_uses_generic_heuristic(self, mock_backend_fn):
        """Services not in RESOURCE_ATTRS should use the generic dict-scanning fallback."""

        class FakeBackend:
            widgets = {"w1": "widget1", "w2": "widget2"}
            _private = {"hidden": True}

        mock_backend_fn.return_value = FakeBackend()

        # Use a service name not in RESOURCE_ATTRS
        assert "acm" not in RESOURCE_ATTRS
        resources = get_service_resources("acm")
        # The generic fallback should find the "widgets" dict
        assert any(r["type"] == "widgets" for r in resources)
        widget_entry = [r for r in resources if r["type"] == "widgets"][0]
        assert widget_entry["count"] == 2

    @patch("robotocore.resources.browser._get_backend")
    def test_list_type_collection(self, mock_backend_fn):
        """Collections that are lists (not dicts) should also be iterable."""
        item1 = MagicMock()
        item1.name = "item-1"
        item1.arn = "arn:aws:sqs:us-east-1:123:item-1"
        item2 = MagicMock()
        item2.name = "item-2"
        item2.arn = "arn:aws:sqs:us-east-1:123:item-2"
        backend = MagicMock()
        backend.queues = [item1, item2]  # list, not dict
        mock_backend_fn.return_value = backend

        resources = get_service_resources("sqs")
        assert len(resources) == 2
        names = {r["name"] for r in resources}
        assert "item-1" in names
        assert "item-2" in names


class TestGetBackend:
    @patch("moto.backends.get_backend")
    def test_returns_none_on_exception(self, mock_get_backend):
        mock_get_backend.side_effect = Exception("backend error")
        result = _get_backend("nonexistent")
        assert result is None

    @patch("moto.backends.get_backend")
    def test_returns_none_for_unknown_account(self, mock_get_backend):
        from moto.core.base_backend import BackendDict

        bd = MagicMock(spec=BackendDict)
        bd.__contains__ = MagicMock(return_value=False)
        mock_get_backend.return_value = bd
        result = _get_backend("s3", account_id="999999999999")
        assert result is None


class TestResourceAttrsCoverage:
    def test_all_listed_services_have_string_attrs(self):
        for service, attrs in RESOURCE_ATTRS.items():
            assert isinstance(service, str)
            for attr_name, type_name in attrs:
                assert isinstance(attr_name, str)
                assert isinstance(type_name, str)
                assert len(type_name) > 0

    def test_core_services_covered(self):
        core = ["s3", "sqs", "dynamodb", "lambda", "iam", "ec2", "ecs", "kinesis"]
        for svc in core:
            assert svc in RESOURCE_ATTRS, f"Core service {svc} missing from RESOURCE_ATTRS"

    def test_ec2_has_instances_and_vpcs(self):
        attrs = dict(RESOURCE_ATTRS["ec2"])
        assert "instances" in attrs
        assert "vpcs" in attrs

    def test_rds_has_databases_and_clusters(self):
        attrs = dict(RESOURCE_ATTRS["rds"])
        assert "databases" in attrs
        assert "clusters" in attrs
