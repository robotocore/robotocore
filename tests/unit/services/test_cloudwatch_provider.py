"""Tests for CloudWatch deep features: composite alarms, metric math, insights,
filters, dashboards, and alarm actions.

89+ tests covering:
- 14 composite alarm rule evaluation tests
- 16 metric math expression tests
- 22 Logs Insights tests (parser + executor + lifecycle)
- 11 filter pattern matching and metric filter CRUD tests
- 5 subscription filter CRUD and delivery tests
- 12 dashboard CRUD tests
- 4 alarm action dispatch tests
- 5 additional tests
"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.cloudwatch.filters import (
    FilterStore,
    matches_filter_pattern,
)
from robotocore.services.cloudwatch.insights import (
    InsightsError,
    clear_queries,
    execute_pipeline,
    get_query_results,
    parse_query,
    start_query,
    stop_query,
)
from robotocore.services.cloudwatch.metric_math import (
    MetricMathError,
    aggregate_values,
    evaluate_expression,
)
from robotocore.services.cloudwatch.provider import (
    CloudWatchError,
    delete_composite_alarms,
    delete_dashboards,
    describe_composite_alarms,
    dispatch_alarm_actions,
    evaluate_alarm_rule,
    get_dashboard,
    get_metric_data,
    list_dashboards,
    parse_alarm_rule,
    put_composite_alarm,
    put_dashboard,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REGION = "us-east-1"
ACCOUNT = "123456789012"


def _clear_composite_store(region: str = REGION) -> None:
    """Clear composite alarm store for test isolation."""
    from robotocore.services.cloudwatch.provider import _composite_alarms, _composite_lock

    with _composite_lock:
        _composite_alarms.pop(region, None)


def _clear_dashboard_store(region: str = REGION) -> None:
    """Clear dashboard store for test isolation."""
    from robotocore.services.cloudwatch.provider import _dashboard_lock, _dashboards

    with _dashboard_lock:
        _dashboards.pop(region, None)


@pytest.fixture(autouse=True)
def _cleanup():
    """Clean up stores before each test."""
    _clear_composite_store()
    _clear_dashboard_store()
    clear_queries()
    yield
    _clear_composite_store()
    _clear_dashboard_store()
    clear_queries()


# ===================================================================
# COMPOSITE ALARM RULE EVALUATION TESTS (14 tests)
# ===================================================================


class TestCompositeAlarmRuleParsing:
    def test_parse_simple_alarm_state(self):
        ast = parse_alarm_rule('ALARM("my-alarm")')
        assert ast["type"] == "STATE"
        assert ast["state"] == "ALARM"
        assert ast["alarm_name"] == "my-alarm"

    def test_parse_ok_state(self):
        ast = parse_alarm_rule('OK("my-alarm")')
        assert ast["type"] == "STATE"
        assert ast["state"] == "OK"

    def test_parse_insufficient_data_state(self):
        ast = parse_alarm_rule('INSUFFICIENT_DATA("my-alarm")')
        assert ast["type"] == "STATE"
        assert ast["state"] == "INSUFFICIENT_DATA"

    def test_parse_and_expression(self):
        ast = parse_alarm_rule('ALARM("a1") AND ALARM("a2")')
        assert ast["type"] == "AND"
        assert ast["left"]["alarm_name"] == "a1"
        assert ast["right"]["alarm_name"] == "a2"

    def test_parse_or_expression(self):
        ast = parse_alarm_rule('ALARM("a1") OR ALARM("a2")')
        assert ast["type"] == "OR"

    def test_parse_not_expression(self):
        ast = parse_alarm_rule('NOT ALARM("a1")')
        assert ast["type"] == "NOT"
        assert ast["child"]["alarm_name"] == "a1"

    def test_parse_complex_nested(self):
        ast = parse_alarm_rule('(ALARM("a1") AND ALARM("a2")) OR NOT OK("a3")')
        assert ast["type"] == "OR"
        assert ast["left"]["type"] == "AND"
        assert ast["right"]["type"] == "NOT"

    def test_evaluate_alarm_true(self):
        ast = parse_alarm_rule('ALARM("cpu-high")')
        result = evaluate_alarm_rule(ast, {"cpu-high": "ALARM"})
        assert result == "ALARM"

    def test_evaluate_alarm_false(self):
        ast = parse_alarm_rule('ALARM("cpu-high")')
        result = evaluate_alarm_rule(ast, {"cpu-high": "OK"})
        assert result == "OK"

    def test_evaluate_and_both_true(self):
        ast = parse_alarm_rule('ALARM("a1") AND ALARM("a2")')
        result = evaluate_alarm_rule(ast, {"a1": "ALARM", "a2": "ALARM"})
        assert result == "ALARM"

    def test_evaluate_and_one_false(self):
        ast = parse_alarm_rule('ALARM("a1") AND ALARM("a2")')
        result = evaluate_alarm_rule(ast, {"a1": "ALARM", "a2": "OK"})
        assert result == "OK"

    def test_evaluate_or_one_true(self):
        ast = parse_alarm_rule('ALARM("a1") OR ALARM("a2")')
        result = evaluate_alarm_rule(ast, {"a1": "OK", "a2": "ALARM"})
        assert result == "ALARM"

    def test_evaluate_not(self):
        ast = parse_alarm_rule('NOT ALARM("a1")')
        result = evaluate_alarm_rule(ast, {"a1": "OK"})
        assert result == "ALARM"

    def test_evaluate_missing_alarm_defaults_insufficient(self):
        ast = parse_alarm_rule('INSUFFICIENT_DATA("unknown")')
        result = evaluate_alarm_rule(ast, {})
        assert result == "ALARM"


# ===================================================================
# METRIC MATH EXPRESSION TESTS (16 tests)
# ===================================================================


class TestMetricMath:
    def test_simple_metric_ref(self):
        result = evaluate_expression("m1", {"m1": [1.0, 2.0, 3.0]})
        assert result == [1.0, 2.0, 3.0]

    def test_sum_function(self):
        result = evaluate_expression("SUM(m1)", {"m1": [1.0, 2.0, 3.0]})
        assert result == [6.0]

    def test_avg_function(self):
        result = evaluate_expression("AVG(m1)", {"m1": [2.0, 4.0, 6.0]})
        assert result == [4.0]

    def test_min_function(self):
        result = evaluate_expression("MIN(m1)", {"m1": [5.0, 1.0, 3.0]})
        assert result == [1.0]

    def test_max_function(self):
        result = evaluate_expression("MAX(m1)", {"m1": [5.0, 1.0, 3.0]})
        assert result == [5.0]

    def test_ceil_function(self):
        result = evaluate_expression("CEIL(m1)", {"m1": [1.2, 2.7]})
        assert result == [2.0, 3.0]

    def test_floor_function(self):
        result = evaluate_expression("FLOOR(m1)", {"m1": [1.8, 2.2]})
        assert result == [1.0, 2.0]

    def test_abs_function(self):
        result = evaluate_expression("ABS(m1)", {"m1": [-5.0, 3.0]})
        assert result == [5.0, 3.0]

    def test_addition(self):
        result = evaluate_expression("m1 + m2", {"m1": [1.0, 2.0], "m2": [3.0, 4.0]})
        assert result == [4.0, 6.0]

    def test_subtraction(self):
        result = evaluate_expression("m1 - m2", {"m1": [5.0, 10.0], "m2": [1.0, 3.0]})
        assert result == [4.0, 7.0]

    def test_multiplication(self):
        result = evaluate_expression("m1 * m2", {"m1": [2.0, 3.0], "m2": [4.0, 5.0]})
        assert result == [8.0, 15.0]

    def test_division(self):
        result = evaluate_expression("m1 / m2", {"m1": [10.0, 6.0], "m2": [2.0, 3.0]})
        assert result == [5.0, 2.0]

    def test_division_by_zero(self):
        result = evaluate_expression("m1 / m2", {"m1": [10.0], "m2": [0.0]})
        assert result == [0.0]

    def test_scalar_broadcast(self):
        result = evaluate_expression("m1 + 10", {"m1": [1.0, 2.0, 3.0]})
        assert result == [11.0, 12.0, 13.0]

    def test_complex_expression(self):
        result = evaluate_expression("AVG(m1) / 100", {"m1": [200.0, 400.0]})
        assert result == [3.0]

    def test_unknown_metric_raises(self):
        with pytest.raises(MetricMathError, match="Unknown metric"):
            evaluate_expression("m_unknown", {})

    def test_aggregate_values_sum(self):
        result = aggregate_values([1.0, 2.0, 3.0], 60, "Sum")
        assert result == [6.0]

    def test_aggregate_values_average(self):
        result = aggregate_values([2.0, 4.0], 60, "Average")
        assert result == [3.0]

    def test_aggregate_values_empty(self):
        result = aggregate_values([], 60, "Average")
        assert result == []


# ===================================================================
# LOGS INSIGHTS TESTS (22 tests)
# ===================================================================


class TestInsightsParser:
    def test_parse_fields_command(self):
        cmds = parse_query("fields @timestamp, @message")
        assert len(cmds) == 1
        assert cmds[0]["type"] == "fields"
        assert "timestamp" in cmds[0]["fields"]
        assert "message" in cmds[0]["fields"]

    def test_parse_filter_like(self):
        cmds = parse_query('filter @message like /ERROR/')
        assert len(cmds) == 1
        assert cmds[0]["type"] == "filter"
        assert "like" in cmds[0]["expression"]

    def test_parse_filter_exact(self):
        cmds = parse_query('filter @message = "hello"')
        assert cmds[0]["type"] == "filter"

    def test_parse_filter_numeric(self):
        cmds = parse_query("filter @duration > 100")
        assert cmds[0]["type"] == "filter"

    def test_parse_stats_count(self):
        cmds = parse_query("stats count(*)")
        assert cmds[0]["type"] == "stats"
        assert cmds[0]["aggregations"][0]["func"] == "count"

    def test_parse_stats_with_group_by(self):
        cmds = parse_query("stats count(*) by @logStream")
        assert cmds[0]["type"] == "stats"
        assert cmds[0]["group_by"] == ["logStream"]

    def test_parse_stats_avg(self):
        cmds = parse_query("stats avg(@duration)")
        assert cmds[0]["aggregations"][0]["func"] == "avg"
        assert cmds[0]["aggregations"][0]["field"] == "duration"

    def test_parse_sort(self):
        cmds = parse_query("sort @timestamp desc")
        assert cmds[0]["type"] == "sort"
        assert cmds[0]["field"] == "timestamp"
        assert cmds[0]["order"] == "desc"

    def test_parse_limit(self):
        cmds = parse_query("limit 50")
        assert cmds[0]["type"] == "limit"
        assert cmds[0]["count"] == 50

    def test_parse_parse_regex(self):
        cmds = parse_query('parse @message /user=(\\w+)/ as @user')
        assert cmds[0]["type"] == "parse"
        assert cmds[0]["source"] == "message"
        assert cmds[0]["fields"] == ["user"]

    def test_parse_pipeline(self):
        cmds = parse_query(
            'fields @timestamp, @message | filter @message like /ERROR/ | limit 10'
        )
        assert len(cmds) == 3
        assert cmds[0]["type"] == "fields"
        assert cmds[1]["type"] == "filter"
        assert cmds[2]["type"] == "limit"


class TestInsightsExecutor:
    def _events(self, messages: list[str]) -> list[dict]:
        return [{"timestamp": i, "message": m} for i, m in enumerate(messages)]

    def test_fields_projection(self):
        events = self._events(["hello", "world"])
        cmds = parse_query("fields @timestamp, @message")
        result = execute_pipeline(cmds, events)
        assert len(result) == 2
        assert "timestamp" in result[0]
        assert "message" in result[0]

    def test_filter_like_regex(self):
        events = self._events(["ERROR: something", "INFO: ok", "ERROR: another"])
        cmds = parse_query('filter @message like /ERROR/')
        result = execute_pipeline(cmds, events)
        assert len(result) == 2

    def test_filter_exact_match(self):
        events = self._events(["hello", "world"])
        cmds = parse_query('filter @message = "hello"')
        result = execute_pipeline(cmds, events)
        assert len(result) == 1

    def test_filter_numeric_comparison(self):
        events = [{"timestamp": i, "message": f"duration={i * 100}"} for i in range(5)]
        # Add a 'duration' field to rows
        cmds = [{"type": "filter", "expression": "@timestamp > 2"}]
        result = execute_pipeline(cmds, events)
        assert len(result) == 2  # timestamps 3 and 4

    def test_stats_count(self):
        events = self._events(["a", "b", "c"])
        cmds = parse_query("stats count(*)")
        result = execute_pipeline(cmds, events)
        assert len(result) == 1
        assert result[0]["count(*)"] == "3.0"

    def test_stats_group_by(self):
        events = [
            {"timestamp": 1, "message": "x", "logStreamName": "s1"},
            {"timestamp": 2, "message": "y", "logStreamName": "s1"},
            {"timestamp": 3, "message": "z", "logStreamName": "s2"},
        ]
        cmds = parse_query("stats count(*) by @logStream")
        result = execute_pipeline(cmds, events)
        assert len(result) == 2

    def test_sort_desc(self):
        events = self._events(["a", "b", "c"])
        cmds = parse_query("sort @timestamp desc")
        result = execute_pipeline(cmds, events)
        assert result[0]["timestamp"] == "2"
        assert result[2]["timestamp"] == "0"

    def test_limit(self):
        events = self._events(["a", "b", "c", "d", "e"])
        cmds = parse_query("limit 3")
        result = execute_pipeline(cmds, events)
        assert len(result) == 3

    def test_parse_regex_extraction(self):
        events = self._events(["user=alice action=read", "user=bob action=write"])
        cmds = parse_query('parse @message /user=(\\w+)/ as @user')
        result = execute_pipeline(cmds, events)
        assert result[0]["user"] == "alice"
        assert result[1]["user"] == "bob"

    def test_full_pipeline(self):
        events = self._events([
            "ERROR: disk full",
            "INFO: ok",
            "ERROR: timeout",
            "WARNING: slow",
            "ERROR: crash",
        ])
        cmds = parse_query(
            'fields @timestamp, @message | filter @message like /ERROR/ | limit 2'
        )
        result = execute_pipeline(cmds, events)
        assert len(result) == 2
        assert "ERROR" in result[0]["message"]


class TestInsightsLifecycle:
    @patch("moto.backends.get_backend")
    def test_start_query_returns_id(self, mock_backend):
        mock_backend.return_value = {ACCOUNT: {REGION: MagicMock(groups={})}}
        qid = start_query(
            ["/aws/test"], "fields @message", 0, 9999, REGION, ACCOUNT
        )
        assert qid

    @patch("moto.backends.get_backend")
    def test_get_query_results_complete(self, mock_backend):
        mock_backend.return_value = {ACCOUNT: {REGION: MagicMock(groups={})}}
        qid = start_query(
            ["/aws/test"], "fields @message", 0, 9999, REGION, ACCOUNT
        )
        result = get_query_results(qid)
        assert result["status"] == "Complete"

    def test_get_query_results_not_found(self):
        with pytest.raises(InsightsError, match="not found"):
            get_query_results("nonexistent-id")

    @patch("moto.backends.get_backend")
    def test_stop_query(self, mock_backend):
        mock_backend.return_value = {ACCOUNT: {REGION: MagicMock(groups={})}}
        qid = start_query(
            ["/aws/test"], "fields @message", 0, 9999, REGION, ACCOUNT
        )
        result = stop_query(qid)
        assert result is True

    def test_stop_nonexistent_query(self):
        result = stop_query("nonexistent")
        assert result is False


# ===================================================================
# FILTER PATTERN MATCHING AND METRIC FILTER CRUD TESTS (11 tests)
# ===================================================================


class TestFilterPatternMatching:
    def test_empty_pattern_matches_all(self):
        assert matches_filter_pattern("", "anything") is True

    def test_single_term_match(self):
        assert matches_filter_pattern("ERROR", "This is an ERROR message") is True

    def test_single_term_no_match(self):
        assert matches_filter_pattern("ERROR", "This is fine") is False

    def test_multiple_terms_and(self):
        assert matches_filter_pattern("ERROR disk", "ERROR: disk full") is True

    def test_multiple_terms_partial_fail(self):
        assert matches_filter_pattern("ERROR memory", "ERROR: disk full") is False

    def test_quoted_string(self):
        assert matches_filter_pattern('"disk full"', "ERROR: disk full detected") is True

    def test_json_pattern_match(self):
        msg = json.dumps({"level": "ERROR", "code": 500})
        assert matches_filter_pattern('{ $.level = "ERROR" }', msg) is True

    def test_json_pattern_no_match(self):
        msg = json.dumps({"level": "INFO", "code": 200})
        assert matches_filter_pattern('{ $.level = "ERROR" }', msg) is False

    def test_json_numeric_comparison(self):
        msg = json.dumps({"level": "ERROR", "code": 500})
        assert matches_filter_pattern("{ $.code > 400 }", msg) is True

    def test_json_nested_field(self):
        msg = json.dumps({"request": {"status": 404}})
        assert matches_filter_pattern("{ $.request.status = 404 }", msg) is True


class TestMetricFilterCRUD:
    def test_put_and_describe(self):
        store = FilterStore()
        store.put_metric_filter(
            "/aws/test",
            "my-filter",
            "ERROR",
            [{"metricName": "ErrorCount", "metricNamespace": "MyApp", "metricValue": "1"}],
        )
        filters = store.describe_metric_filters("/aws/test")
        assert len(filters) == 1
        assert filters[0].filter_name == "my-filter"


# ===================================================================
# SUBSCRIPTION FILTER CRUD TESTS (5 tests)
# ===================================================================


class TestSubscriptionFilterCRUD:
    def test_put_subscription_filter(self):
        store = FilterStore()
        sf = store.put_subscription_filter(
            "/aws/test", "sub1", "ERROR", "arn:aws:lambda:us-east-1:123:function:my-fn"
        )
        assert sf.filter_name == "sub1"

    def test_describe_subscription_filters(self):
        store = FilterStore()
        store.put_subscription_filter(
            "/aws/test", "sub1", "ERROR", "arn:aws:lambda:us-east-1:123:function:my-fn"
        )
        store.put_subscription_filter(
            "/aws/test", "sub2", "WARN", "arn:aws:kinesis:us-east-1:123:stream/my-stream"
        )
        filters = store.describe_subscription_filters("/aws/test")
        assert len(filters) == 2

    def test_delete_subscription_filter(self):
        store = FilterStore()
        store.put_subscription_filter(
            "/aws/test", "sub1", "ERROR", "arn:aws:lambda:us-east-1:123:function:my-fn"
        )
        assert store.delete_subscription_filter("/aws/test", "sub1") is True
        assert store.describe_subscription_filters("/aws/test") == []

    def test_delete_nonexistent_subscription_filter(self):
        store = FilterStore()
        assert store.delete_subscription_filter("/aws/test", "nope") is False

    def test_get_subscription_filters_for_group(self):
        store = FilterStore()
        store.put_subscription_filter(
            "/aws/test", "sub1", "", "arn:aws:lambda:us-east-1:123:function:fn1"
        )
        filters = store.get_subscription_filters_for_group("/aws/test")
        assert len(filters) == 1


# ===================================================================
# DASHBOARD CRUD TESTS (12 tests)
# ===================================================================


class TestDashboardCRUD:
    def _valid_body(self) -> str:
        return json.dumps({"widgets": [{"type": "metric", "properties": {}}]})

    def test_put_dashboard(self):
        result = put_dashboard(
            {"DashboardName": "test-dash", "DashboardBody": self._valid_body()},
            REGION,
            ACCOUNT,
        )
        assert "DashboardValidationMessages" in result

    def test_get_dashboard(self):
        body = self._valid_body()
        put_dashboard(
            {"DashboardName": "test-dash", "DashboardBody": body}, REGION, ACCOUNT
        )
        result = get_dashboard({"DashboardName": "test-dash"}, REGION, ACCOUNT)
        assert result["DashboardName"] == "test-dash"
        assert result["DashboardBody"] == body

    def test_get_dashboard_not_found(self):
        with pytest.raises(CloudWatchError, match="does not exist"):
            get_dashboard({"DashboardName": "nope"}, REGION, ACCOUNT)

    def test_list_dashboards(self):
        put_dashboard(
            {"DashboardName": "dash-a", "DashboardBody": self._valid_body()},
            REGION,
            ACCOUNT,
        )
        put_dashboard(
            {"DashboardName": "dash-b", "DashboardBody": self._valid_body()},
            REGION,
            ACCOUNT,
        )
        result = list_dashboards({}, REGION, ACCOUNT)
        assert len(result) == 2

    def test_list_dashboards_with_prefix(self):
        put_dashboard(
            {"DashboardName": "prod-dash", "DashboardBody": self._valid_body()},
            REGION,
            ACCOUNT,
        )
        put_dashboard(
            {"DashboardName": "dev-dash", "DashboardBody": self._valid_body()},
            REGION,
            ACCOUNT,
        )
        result = list_dashboards({"DashboardNamePrefix": "prod"}, REGION, ACCOUNT)
        assert len(result) == 1
        assert result[0]["DashboardName"] == "prod-dash"

    def test_delete_dashboards(self):
        put_dashboard(
            {"DashboardName": "to-delete", "DashboardBody": self._valid_body()},
            REGION,
            ACCOUNT,
        )
        delete_dashboards({"DashboardNames": ["to-delete"]}, REGION, ACCOUNT)
        with pytest.raises(CloudWatchError):
            get_dashboard({"DashboardName": "to-delete"}, REGION, ACCOUNT)

    def test_delete_nonexistent_dashboard(self):
        with pytest.raises(CloudWatchError, match="does not exist"):
            delete_dashboards({"DashboardNames": ["nope"]}, REGION, ACCOUNT)

    def test_put_dashboard_invalid_json(self):
        with pytest.raises(CloudWatchError, match="Invalid JSON"):
            put_dashboard(
                {"DashboardName": "bad", "DashboardBody": "not json"},
                REGION,
                ACCOUNT,
            )

    def test_put_dashboard_missing_widgets(self):
        with pytest.raises(CloudWatchError, match="widgets"):
            put_dashboard(
                {"DashboardName": "bad", "DashboardBody": "{}"},
                REGION,
                ACCOUNT,
            )

    def test_put_dashboard_missing_name(self):
        with pytest.raises(CloudWatchError, match="DashboardName"):
            put_dashboard({"DashboardBody": self._valid_body()}, REGION, ACCOUNT)

    def test_put_dashboard_missing_body(self):
        with pytest.raises(CloudWatchError, match="DashboardBody"):
            put_dashboard({"DashboardName": "test"}, REGION, ACCOUNT)

    def test_update_dashboard(self):
        body1 = json.dumps({"widgets": [{"type": "metric"}]})
        body2 = json.dumps({"widgets": [{"type": "text"}]})
        put_dashboard(
            {"DashboardName": "test", "DashboardBody": body1}, REGION, ACCOUNT
        )
        put_dashboard(
            {"DashboardName": "test", "DashboardBody": body2}, REGION, ACCOUNT
        )
        result = get_dashboard({"DashboardName": "test"}, REGION, ACCOUNT)
        assert result["DashboardBody"] == body2


# ===================================================================
# ALARM ACTION DISPATCH TESTS (4 tests)
# ===================================================================


class TestAlarmActionDispatch:
    @patch("moto.backends.get_backend")
    def test_dispatch_sns_action(self, mock_get_backend):
        mock_sns = MagicMock()
        mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_sns}}

        alarm_data = {
            "AlarmActions": ["arn:aws:sns:us-east-1:123456789012:my-topic"],
            "OKActions": [],
            "InsufficientDataActions": [],
        }
        dispatched = dispatch_alarm_actions(
            "test-alarm", alarm_data, "OK", "ALARM", "threshold crossed", REGION, ACCOUNT
        )
        assert len(dispatched) == 1
        mock_sns.publish.assert_called_once()

    @patch("robotocore.services.lambda_.invoke.invoke_lambda_async")
    def test_dispatch_lambda_action(self, mock_invoke):
        alarm_data = {
            "AlarmActions": ["arn:aws:lambda:us-east-1:123456789012:function:my-func"],
            "OKActions": [],
            "InsufficientDataActions": [],
        }
        dispatched = dispatch_alarm_actions(
            "test-alarm", alarm_data, "OK", "ALARM", "threshold crossed", REGION, ACCOUNT
        )
        assert len(dispatched) == 1
        mock_invoke.assert_called_once()

    def test_dispatch_ec2_action(self):
        alarm_data = {
            "AlarmActions": [
                "arn:aws:automate:us-east-1:ec2:stop"
            ],
            "OKActions": [],
            "InsufficientDataActions": [],
        }
        dispatched = dispatch_alarm_actions(
            "test-alarm", alarm_data, "OK", "ALARM", "threshold crossed", REGION, ACCOUNT
        )
        assert len(dispatched) == 1

    def test_dispatch_ok_actions(self):
        alarm_data = {
            "AlarmActions": [],
            "OKActions": ["arn:aws:automate:us-east-1:ec2:reboot"],
            "InsufficientDataActions": [],
        }
        dispatched = dispatch_alarm_actions(
            "test-alarm", alarm_data, "ALARM", "OK", "recovered", REGION, ACCOUNT
        )
        assert len(dispatched) == 1


# ===================================================================
# ADDITIONAL TESTS (5+ tests)
# ===================================================================


class TestCompositeAlarmCRUD:
    def test_put_and_describe_composite(self):
        put_composite_alarm(
            {
                "AlarmName": "composite-1",
                "AlarmRule": 'ALARM("a1") OR ALARM("a2")',
            },
            REGION,
            ACCOUNT,
        )
        alarms = describe_composite_alarms({}, REGION, ACCOUNT)
        assert len(alarms) == 1
        assert alarms[0]["AlarmName"] == "composite-1"

    def test_delete_composite_alarm(self):
        put_composite_alarm(
            {
                "AlarmName": "composite-del",
                "AlarmRule": 'ALARM("a1")',
            },
            REGION,
            ACCOUNT,
        )
        delete_composite_alarms(["composite-del"], REGION)
        alarms = describe_composite_alarms({}, REGION, ACCOUNT)
        assert len(alarms) == 0

    def test_put_composite_alarm_invalid_rule(self):
        with pytest.raises(CloudWatchError):
            put_composite_alarm(
                {
                    "AlarmName": "bad",
                    "AlarmRule": "INVALID SYNTAX %%%",
                },
                REGION,
                ACCOUNT,
            )

    def test_describe_composite_with_prefix(self):
        put_composite_alarm(
            {"AlarmName": "prod-alarm", "AlarmRule": 'ALARM("a1")'},
            REGION,
            ACCOUNT,
        )
        put_composite_alarm(
            {"AlarmName": "dev-alarm", "AlarmRule": 'ALARM("a2")'},
            REGION,
            ACCOUNT,
        )
        alarms = describe_composite_alarms(
            {"AlarmNamePrefix": "prod"}, REGION, ACCOUNT
        )
        assert len(alarms) == 1


class TestGetMetricData:
    @patch("moto.backends.get_backend")
    def test_get_metric_data_with_expression(self, mock_get_backend):
        # Create a mock backend with metric data
        mock_datum = SimpleNamespace(
            namespace="AWS/EC2",
            name="CPUUtilization",
            dimensions=[],
            value=50.0,
        )
        mock_backend = MagicMock()
        mock_backend.metric_data = [mock_datum]
        mock_backend.aws_metric_data = []
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        result = get_metric_data(
            {
                "MetricDataQueries": [
                    {
                        "Id": "m1",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/EC2",
                                "MetricName": "CPUUtilization",
                            },
                            "Stat": "Average",
                            "Period": 60,
                        },
                    },
                    {
                        "Id": "e1",
                        "Expression": "m1 * 2",
                    },
                ],
            },
            REGION,
            ACCOUNT,
        )
        assert len(result["MetricDataResults"]) == 2
        # e1 should have doubled value
        e1_result = [r for r in result["MetricDataResults"] if r["Id"] == "e1"][0]
        assert e1_result["Values"] == [100.0]


class TestBooleanRuleTokens:
    def test_parse_true_literal(self):
        ast = parse_alarm_rule("TRUE")
        assert ast["type"] == "BOOL"
        assert ast["value"] is True

    def test_parse_false_literal(self):
        ast = parse_alarm_rule("FALSE")
        assert ast["type"] == "BOOL"
        assert ast["value"] is False

    def test_evaluate_true_literal(self):
        ast = parse_alarm_rule("TRUE")
        assert evaluate_alarm_rule(ast, {}) == "ALARM"

    def test_evaluate_false_literal(self):
        ast = parse_alarm_rule("FALSE")
        assert evaluate_alarm_rule(ast, {}) == "OK"


class TestCloudWatchJsonProtocol:
    """Test JSON protocol handling for modern boto3."""

    @pytest.fixture
    def app(self):
        from robotocore.services.cloudwatch.provider import handle_cloudwatch_request
        return handle_cloudwatch_request

    def _make_json_request(self, action, body=None):
        """Create a mock request with JSON protocol headers."""
        req = MagicMock()
        req.headers = {
            "content-type": "application/x-amz-json-1.0",
            "x-amz-target": f"GraniteServiceVersion20100801.{action}",
        }
        req.url = MagicMock()
        req.url.query = ""

        async def mock_body():
            return json.dumps(body or {}).encode()

        req.body = mock_body
        return req

    @pytest.mark.asyncio
    async def test_disable_alarm_actions_json(self, app):
        """DisableAlarmActions via JSON protocol."""
        with patch("moto.backends.get_backend") as mock_gb:
            mock_backend = MagicMock()
            mock_alarm = MagicMock()
            mock_alarm.actions_enabled = True
            mock_backend.alarms = {"test-alarm": mock_alarm}
            mock_gb.return_value = {"123456789012": {"us-east-1": mock_backend}}

            req = self._make_json_request(
                "DisableAlarmActions", {"AlarmNames": ["test-alarm"]}
            )
            resp = await app(req, "us-east-1", "123456789012")
            assert resp.status_code == 200
            assert mock_alarm.actions_enabled is False
            assert "application/x-amz-json-1.0" in resp.media_type

    @pytest.mark.asyncio
    async def test_enable_alarm_actions_json(self, app):
        """EnableAlarmActions via JSON protocol."""
        with patch("moto.backends.get_backend") as mock_gb:
            mock_backend = MagicMock()
            mock_alarm = MagicMock()
            mock_alarm.actions_enabled = False
            mock_backend.alarms = {"test-alarm": mock_alarm}
            mock_gb.return_value = {"123456789012": {"us-east-1": mock_backend}}

            req = self._make_json_request(
                "EnableAlarmActions", {"AlarmNames": ["test-alarm"]}
            )
            resp = await app(req, "us-east-1", "123456789012")
            assert resp.status_code == 200
            assert mock_alarm.actions_enabled is True

    @pytest.mark.asyncio
    async def test_describe_alarm_history_json(self, app):
        """DescribeAlarmHistory via JSON protocol returns empty list."""
        req = self._make_json_request(
            "DescribeAlarmHistory", {"AlarmName": "test"}
        )
        resp = await app(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["AlarmHistoryItems"] == []

    @pytest.mark.asyncio
    async def test_json_fallback_to_moto(self, app):
        """Unknown JSON actions fall through to Moto."""
        with patch("robotocore.services.cloudwatch.provider.forward_to_moto") as mock_moto:
            mock_moto.return_value = MagicMock(status_code=200)
            req = self._make_json_request(
                "ListMetrics", {}
            )
            await app(req, "us-east-1", "123456789012")
            mock_moto.assert_called_once()
