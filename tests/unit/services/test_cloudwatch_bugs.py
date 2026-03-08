"""Failing tests exposing bugs in the CloudWatch and CloudWatch Logs providers.

Each test targets a specific bug identified during code audit.
These tests are expected to FAIL against the current implementation.
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
    evaluate_alarm_rule,
    parse_alarm_rule,
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
#
# When a dict value is a list, _dict_to_xml emits bare <member> tags
# without wrapping them in a <ParentKey> tag. This produces malformed
# XML that AWS SDKs cannot parse.
#
# Expected: <AlarmActions><member>arn:...</member></AlarmActions>
# Actual:   <member>arn:...</member>


class TestDictToXmlListWrapping:
    def test_list_values_wrapped_in_parent_key(self):
        """List values in XML responses must be wrapped with the dict key name."""
        data = {"AlarmActions": ["arn:aws:sns:us-east-1:123:topic1"]}
        xml = _dict_to_xml(data)
        # The XML must contain the parent key wrapping the member elements
        assert "<AlarmActions>" in xml
        assert "</AlarmActions>" in xml
        assert "<AlarmActions><member>" in xml or "<AlarmActions>\n<member>" in xml

    def test_multiple_list_items_wrapped(self):
        """Multiple list items should all be inside the parent key."""
        data = {"OKActions": ["arn:1", "arn:2"]}
        xml = _dict_to_xml(data)
        # Both members should be inside OKActions tags
        assert xml.count("<member>") == 2
        assert "<OKActions>" in xml
        assert "</OKActions>" in xml


# ===================================================================
# Bug 2: evaluate_alarm_rule never returns INSUFFICIENT_DATA
# ===================================================================
#
# AWS composite alarms should evaluate to INSUFFICIENT_DATA when
# any referenced child alarm has INSUFFICIENT_DATA and the overall
# rule cannot be definitively resolved to ALARM or OK.
#
# e.g. ALARM("a") AND ALARM("b") where b is INSUFFICIENT_DATA
# should return INSUFFICIENT_DATA, not OK.


class TestCompositeAlarmInsufficientData:
    def test_and_with_insufficient_data_child_returns_insufficient(self):
        """If one child is ALARM and another is INSUFFICIENT_DATA, AND should
        return INSUFFICIENT_DATA (not OK), because the result is indeterminate."""
        ast = parse_alarm_rule('ALARM("a") AND ALARM("b")')
        # a is in ALARM, b is INSUFFICIENT_DATA
        result = evaluate_alarm_rule(ast, {"a": "ALARM", "b": "INSUFFICIENT_DATA"})
        assert result == "INSUFFICIENT_DATA"

    def test_or_with_insufficient_data_returns_insufficient(self):
        """If one child is OK and another is INSUFFICIENT_DATA, OR should
        return INSUFFICIENT_DATA (not OK), since the result is indeterminate."""
        ast = parse_alarm_rule('ALARM("a") OR ALARM("b")')
        # a is OK, b is INSUFFICIENT_DATA -> outcome depends on b
        result = evaluate_alarm_rule(ast, {"a": "OK", "b": "INSUFFICIENT_DATA"})
        assert result == "INSUFFICIENT_DATA"


# ===================================================================
# Bug 3: Alarm rule tokenizer doesn't enforce word boundaries
# ===================================================================
#
# The tokenizer checks startswith("AND") without ensuring the next
# character is a space or paren. This means inputs like "TRUEANDFALSE"
# are parsed as "TRUE AND FALSE" instead of raising an error.


class TestAlarmRuleTokenizerWordBoundaries:
    def test_and_keyword_requires_word_boundary(self):
        """'TRUEANDFALSE' should not be parsed as 'TRUE AND FALSE'."""
        # If word boundaries are enforced, this should raise an error
        # because 'TRUEANDFALSE' is not a valid token
        with pytest.raises(CloudWatchError):
            parse_alarm_rule("TRUEANDFALSE")

    def test_or_keyword_requires_word_boundary(self):
        """'TRUEORFALSE' should not be parsed as 'TRUE OR FALSE'."""
        with pytest.raises(CloudWatchError):
            parse_alarm_rule("TRUEORFALSE")

    def test_not_keyword_requires_word_boundary(self):
        """'NOTALARM(\"x\")' without space should not parse as 'NOT ALARM(\"x\")'."""
        # There's no space between NOT and ALARM, but the current tokenizer
        # accepts it. This is ambiguous and should require a space.
        # Actually 'NOTALARM' should fail since NOTALARM is not a valid token.
        # But the tokenizer matches NOT, advances 3, then matches ALARM("x")
        # This test documents the expectation that "NOT" requires a word boundary.
        with pytest.raises(CloudWatchError):
            parse_alarm_rule('NOTALARM("x")')


# ===================================================================
# Bug 4: Insights filter doesn't support boolean combinators
# ===================================================================
#
# CloudWatch Logs Insights supports `and`, `or` in filter expressions:
#   filter @message like /ERROR/ and @logStream = "web-1"
#
# The current _evaluate_filter only handles single comparisons.


class TestInsightsFilterBooleanCombinators:
    def test_filter_with_and(self):
        """filter expressions with 'and' should combine conditions."""
        events = [
            {"timestamp": 1, "message": "ERROR in web", "logStreamName": "web-1"},
            {"timestamp": 2, "message": "ERROR in db", "logStreamName": "db-1"},
            {"timestamp": 3, "message": "INFO in web", "logStreamName": "web-1"},
        ]
        cmds = parse_query('filter @message like /ERROR/ and @logStream = "web-1"')
        # The filter should match only event 1 (ERROR + web-1)
        # but not event 2 (ERROR but db-1) or event 3 (web-1 but INFO)
        result = execute_pipeline(cmds, events)
        # We expect only the first event to match
        assert len(result) == 1
        assert "ERROR" in result[0]["message"]

    def test_filter_with_or(self):
        """filter expressions with 'or' should match either condition."""
        events = [
            {"timestamp": 1, "message": "ERROR occurred"},
            {"timestamp": 2, "message": "WARNING detected"},
            {"timestamp": 3, "message": "INFO ok"},
        ]
        cmds = parse_query("filter @message like /ERROR/ or @message like /WARNING/")
        result = execute_pipeline(cmds, events)
        assert len(result) == 2


# ===================================================================
# Bug 5: JSON filter pattern doesn't handle array access or hyphens
# ===================================================================
#
# CloudWatch filter patterns support array access like { $.items[0] = "x" }
# and field names with hyphens like { $.request-id = "123" }.
# The current regex only captures \w characters (no hyphens or brackets).


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
#
# put_dashboard validates that 'widgets' key exists but doesn't check
# that it's actually a list. A string or number value passes validation.


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
        """An empty widgets list should be rejected (AWS requires at least one widget)."""
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
# Bug 7: Metric math doesn't handle METRICS() function
# ===================================================================
#
# AWS CloudWatch metric math supports METRICS() to reference all
# metric queries. The current implementation doesn't support it.
# The tokenizer can't even parse "METRICS()" — it fails at the
# empty argument list.


class TestMetricMathMissingFunctions:
    def test_metrics_function_parses(self):
        """METRICS() with no arguments should at least parse without error.
        Even if unsupported, the tokenizer should handle an empty-argument
        function call rather than crashing."""
        # This should not raise a parse/tokenize error.
        # It can raise MetricMathError("Unknown function") if METRICS is
        # not supported, but it should NOT raise on the empty parens.
        try:
            evaluate_expression(
                "SUM(METRICS())",
                {"m1": [1.0, 2.0], "m2": [3.0, 4.0]},
            )
        except MetricMathError as e:
            # Acceptable: "Unknown function: METRICS"
            # NOT acceptable: parse/token errors from empty parens
            assert "Unknown function" in str(e), f"Got unexpected parse error: {e}"


# ===================================================================
# Bug 8: Insights sort with mixed types causes TypeError
# ===================================================================
#
# When sorting rows where some values are numeric strings and others
# are non-numeric, the sort_key function returns mixed types (float
# vs str), which causes TypeError in Python 3.


class TestInsightsSortMixedTypes:
    def test_sort_with_mixed_types(self):
        """Sorting rows with mix of numeric and non-numeric values shouldn't crash."""
        events = [
            {"timestamp": 1, "message": "value=100"},
            {"timestamp": "abc", "message": "no timestamp"},
            {"timestamp": 2, "message": "value=200"},
        ]
        cmds = parse_query("sort @timestamp asc")
        # Should not raise TypeError when comparing float and str
        result = execute_pipeline(cmds, events)
        assert len(result) == 3
