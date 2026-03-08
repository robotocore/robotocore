"""Tests for CloudWatch and CloudWatch Logs provider bug fixes.

Each test targets a specific bug that has been fixed.
"""

import json

import pytest

from robotocore.services.cloudwatch.filters import matches_filter_pattern
from robotocore.services.cloudwatch.insights import (
    execute_pipeline,
    parse_query,
)
from robotocore.services.cloudwatch.metric_math import (
    MetricMathError,
    evaluate_expression,
)
from robotocore.services.cloudwatch.provider import (
    CloudWatchError,
    _dict_to_xml,
    put_dashboard,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clear_dashboard_store(region: str = REGION) -> None:
    from robotocore.services.cloudwatch.provider import _dashboard_lock, _dashboards

    with _dashboard_lock:
        _dashboards.pop(region, None)


@pytest.fixture(autouse=True)
def _cleanup():
    _clear_dashboard_store()
    yield
    _clear_dashboard_store()


# ===================================================================
# Bug 1: _dict_to_xml omits parent key tag around list items
# ===================================================================


class TestDictToXmlListWrapping:
    def test_list_values_wrapped_in_parent_key(self):
        """List values in XML responses must be wrapped with the dict key name."""
        data = {"AlarmActions": ["arn:aws:sns:us-east-1:123:topic1"]}
        xml = _dict_to_xml(data)
        assert "<AlarmActions>" in xml
        assert "</AlarmActions>" in xml
        assert "<AlarmActions><member>" in xml or "<AlarmActions>\n<member>" in xml

    def test_multiple_list_items_wrapped(self):
        """Multiple list items should all be inside the parent key."""
        data = {"OKActions": ["arn:1", "arn:2"]}
        xml = _dict_to_xml(data)
        assert xml.count("<member>") == 2
        assert "<OKActions>" in xml
        assert "</OKActions>" in xml


# ===================================================================
# Bug 5: JSON filter pattern doesn't handle array access or hyphens
# ===================================================================


class TestFilterPatternJsonEdgeCases:
    def test_json_field_with_hyphen(self):
        """Filter patterns should handle field names with hyphens."""
        msg = json.dumps({"request-id": "abc-123"})
        assert matches_filter_pattern('{ $.request-id = "abc-123" }', msg) is True

    def test_json_array_access(self):
        """Filter patterns should handle array index access."""
        msg = json.dumps({"items": ["first", "second"]})
        assert matches_filter_pattern('{ $.items[0] = "first" }', msg) is True


# ===================================================================
# Bug 6: Dashboard validation accepts non-list 'widgets' value
# ===================================================================


class TestDashboardValidation:
    def test_widgets_must_be_a_list(self):
        """DashboardBody.widgets must be a list, not a string or number."""
        with pytest.raises(CloudWatchError):
            put_dashboard(
                {
                    "DashboardName": "bad-widgets",
                    "DashboardBody": json.dumps({"widgets": "not-a-list"}),
                },
                REGION,
                ACCOUNT,
            )

    def test_widgets_must_not_be_empty(self):
        """An empty widgets list should be rejected."""
        with pytest.raises(CloudWatchError):
            put_dashboard(
                {
                    "DashboardName": "empty-widgets",
                    "DashboardBody": json.dumps({"widgets": []}),
                },
                REGION,
                ACCOUNT,
            )


# ===================================================================
# Bug 7: Metric math doesn't handle empty function arguments
# ===================================================================


class TestMetricMathEmptyArgs:
    def test_metrics_function_parses(self):
        """METRICS() with no arguments should parse without crashing on empty parens."""
        try:
            evaluate_expression(
                "SUM(METRICS())",
                {"m1": [1.0, 2.0], "m2": [3.0, 4.0]},
            )
        except MetricMathError as e:
            assert "Unknown function" in str(e), f"Got unexpected parse error: {e}"


# ===================================================================
# Bug 8: Insights sort with mixed types causes TypeError
# ===================================================================


class TestInsightsSortMixedTypes:
    def test_sort_with_mixed_types(self):
        """Sorting rows with mix of numeric and non-numeric values shouldn't crash."""
        events = [
            {"timestamp": 1, "message": "value=100"},
            {"timestamp": "abc", "message": "no timestamp"},
            {"timestamp": 2, "message": "value=200"},
        ]
        cmds = parse_query("sort @timestamp asc")
        result = execute_pipeline(cmds, events)
        assert len(result) == 3
