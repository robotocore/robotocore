"""Tests for CloudWatch and CloudWatch Logs provider bug fixes.

Each test targets a specific bug that has been fixed.
"""

import json

import pytest

from robotocore.services.cloudwatch.filters import (
    get_filter_store,
    matches_filter_pattern,
    process_log_events,
)
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


# ===================================================================
# Bug 9: Metric filters silently drop wildcard matches, dimensions,
# and extracted metric values (found reviewing the Bedrock invocation-
# logging terraform module, which relies on all three).
# ===================================================================


class TestMetricFilterExtraction:
    def test_wildcard_json_pattern_matches_present_field(self):
        """`{ $.field = * }` means "field is present", not the literal string "*"."""
        msg = json.dumps({"input": {"inputTokenCount": 1500}})
        assert matches_filter_pattern("{ $.input.inputTokenCount = * }", msg) is True

    def test_wildcard_json_pattern_does_not_match_missing_field(self):
        msg = json.dumps({"output": {"outputTokenCount": 400}})
        assert matches_filter_pattern("{ $.input.inputTokenCount = * }", msg) is False

    def test_metric_value_extracted_from_message_not_hardcoded_to_one(self):
        """metricValue may be a `$.field.path` extractor, not just a literal."""
        region, account = "us-east-1", "999999999999"
        store = get_filter_store(region, account)
        store.put_metric_filter(
            "/aws/bedrock/engineer-inference",
            "BedrockInputTokensByModel",
            "{ $.input.inputTokenCount = * }",
            [
                {
                    "metricName": "InputTokens",
                    "metricNamespace": "BedrockEngineerUsage",
                    "metricValue": "$.input.inputTokenCount",
                    "dimensions": {"ModelId": "$.modelId"},
                }
            ],
        )
        msg = json.dumps({"modelId": "moonshotai.kimi-k2.5", "input": {"inputTokenCount": 1500}})
        process_log_events(
            "/aws/bedrock/engineer-inference", "stream1", [{"message": msg}], region, account
        )

        from moto.backends import get_backend

        cw_backend = get_backend("cloudwatch")[account][region]
        matching = [d for d in cw_backend.metric_data if d.name == "InputTokens"]
        assert len(matching) == 1
        assert matching[0].value == 1500.0
        dim_pairs = {(d.name, d.value) for d in matching[0].dimensions}
        assert ("ModelId", "moonshotai.kimi-k2.5") in dim_pairs

    def test_metric_filter_without_dimensions_still_works(self):
        """A metric filter with no `dimensions` key (the common case) is unaffected."""
        region, account = "us-east-1", "999999999998"
        store = get_filter_store(region, account)
        store.put_metric_filter(
            "/some/group",
            "PlainCount",
            '{ $.status = "ERROR" }',
            [{"metricName": "Errors", "metricNamespace": "MyApp", "metricValue": "1"}],
        )
        msg = json.dumps({"status": "ERROR"})
        process_log_events("/some/group", "stream1", [{"message": msg}], region, account)

        from moto.backends import get_backend

        cw_backend = get_backend("cloudwatch")[account][region]
        matching = [d for d in cw_backend.metric_data if d.name == "Errors"]
        assert len(matching) == 1
        assert matching[0].value == 1.0
        assert list(matching[0].dimensions) == []

    def test_metric_not_emitted_when_extracted_value_missing(self):
        """If metricValue references a field the message doesn't have, skip cleanly."""
        region, account = "us-east-1", "999999999997"
        store = get_filter_store(region, account)
        store.put_metric_filter(
            "/some/group",
            "MissingField",
            "",
            [
                {
                    "metricName": "Whatever",
                    "metricNamespace": "MyApp",
                    "metricValue": "$.does.not.exist",
                }
            ],
        )
        msg = json.dumps({"other": "field"})
        process_log_events("/some/group", "stream1", [{"message": msg}], region, account)

        from moto.backends import get_backend

        cw_backend = get_backend("cloudwatch")[account][region]
        matching = [d for d in cw_backend.metric_data if d.name == "Whatever"]
        assert matching == []


# ===================================================================
# Bug 10: Logs Insights never auto-discovers JSON message fields, `parse`
# rejects dotted source names and glob wildcards, and filter/stats field
# regexes reject dotted paths and single-quoted string literals — found
# alongside Bug 9, reviewing the same Bedrock invocation-logging module.
# ===================================================================


class TestInsightsJsonAutoDiscovery:
    def test_nested_json_fields_are_queryable_without_parse(self):
        """A JSON log message's nested fields are auto-discovered as dotted-path
        fields, matching real Logs Insights — no `parse` needed for JSON bodies."""
        msg = json.dumps({"input": {"inputTokenCount": 1500}, "modelId": "kimi"})
        cmds = parse_query(
            "stats sum(input.inputTokenCount) as input_tokens, count(*) as calls by modelId"
        )
        result = execute_pipeline(cmds, [{"message": msg}])
        assert result == [{"modelId": "kimi", "input_tokens": "1500.0", "calls": "1.0"}]

    def test_parse_with_dotted_source_and_glob_wildcards(self):
        """`parse identity.arn "*literal*" as a, b` — the real AWS glob idiom for ARN
        extraction — must resolve dotted source fields and treat `*` as a wildcard,
        not a literal regex quantifier."""
        msg = json.dumps(
            {
                "identity": {
                    "arn": "arn:aws:sts::222222222222:assumed-role/"
                    "AWSReservedSSO_BedrockEngineer_abc123/jack@ld.com"
                }
            }
        )
        cmds = parse_query(
            'parse identity.arn "*assumed-role/AWSReservedSSO_*_*/*" '
            "as arn_prefix, permission_set, sso_id, engineer"
        )
        result = execute_pipeline(cmds, [{"message": msg}])
        assert result[0]["permission_set"] == "BedrockEngineer"
        assert result[0]["engineer"] == "jack@ld.com"

    def test_regex_mode_parse_still_supported(self):
        """Slash-delimited `parse` patterns remain real regexes with explicit groups."""
        cmds = parse_query("parse @message /user=(\\w+)/ as @user")
        result = execute_pipeline(cmds, [{"message": "user=alice"}])
        assert result[0]["user"] == "alice"

    def test_filter_supports_single_quoted_string_literal(self):
        """`filter field = 'value'` (single quotes) must work, not just double quotes."""
        cmds = parse_query("filter permission_set = 'Administrator'")
        rows = [
            {"message": json.dumps({"permission_set": "Administrator"})},
            {"message": json.dumps({"permission_set": "BedrockEngineer"})},
        ]
        result = execute_pipeline(cmds, rows)
        assert len(result) == 1
        assert result[0]["permission_set"] == "Administrator"

    def test_stats_alias_honored_without_group_by(self):
        """`stats count(*) as alias` must use the alias even with no `by` clause —
        the grouped branch honored it, the non-grouped branch previously didn't."""
        cmds = parse_query("stats count(*) as error_count")
        result = execute_pipeline(cmds, [{"message": "x"}, {"message": "y"}])
        assert result == [{"error_count": "2.0"}]
