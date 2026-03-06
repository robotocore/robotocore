"""Tests for CloudWatch alarm scheduler."""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robotocore.services.cloudwatch.alarm_scheduler import (
    COMPARISON_OPS,
    AlarmScheduler,
    get_alarm_scheduler,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dimension(name, value):
    return SimpleNamespace(name=name, value=value)


def _make_alarm(
    name="test-alarm",
    metric_name="CPUUtilization",
    namespace="AWS/EC2",
    statistic="Average",
    comparison_operator="GreaterThanThreshold",
    threshold=80.0,
    period=60,
    evaluation_periods=1,
    datapoints_to_alarm=None,
    actions_enabled=True,
    state_value="OK",
    alarm_actions=None,
    ok_actions=None,
    insufficient_data_actions=None,
    treat_missing_data=None,
    dimensions=None,
    description="",
):
    return SimpleNamespace(
        name=name,
        metric_name=metric_name,
        namespace=namespace,
        statistic=statistic,
        comparison_operator=comparison_operator,
        threshold=threshold,
        period=period,
        evaluation_periods=evaluation_periods,
        datapoints_to_alarm=datapoints_to_alarm,
        actions_enabled=actions_enabled,
        state_value=state_value,
        alarm_actions=alarm_actions or [],
        ok_actions=ok_actions or [],
        insufficient_data_actions=insufficient_data_actions or [],
        treat_missing_data=treat_missing_data,
        dimensions=dimensions or [],
        description=description,
    )


def _make_metric_datum(namespace, name, value, timestamp, dimensions=None):
    return SimpleNamespace(
        namespace=namespace,
        name=name,
        value=value,
        timestamp=timestamp,
        dimensions=dimensions or [],
    )


# ---------------------------------------------------------------------------
# COMPARISON_OPS
# ---------------------------------------------------------------------------


class TestComparisonOps:
    def test_greater_than_or_equal(self):
        op = COMPARISON_OPS["GreaterThanOrEqualToThreshold"]
        assert op(10, 10) is True
        assert op(11, 10) is True
        assert op(9, 10) is False

    def test_greater_than(self):
        op = COMPARISON_OPS["GreaterThanThreshold"]
        assert op(11, 10) is True
        assert op(10, 10) is False

    def test_less_than(self):
        op = COMPARISON_OPS["LessThanThreshold"]
        assert op(9, 10) is True
        assert op(10, 10) is False

    def test_less_than_or_equal(self):
        op = COMPARISON_OPS["LessThanOrEqualToThreshold"]
        assert op(10, 10) is True
        assert op(9, 10) is True
        assert op(11, 10) is False


# ---------------------------------------------------------------------------
# AlarmScheduler start / stop
# ---------------------------------------------------------------------------


class TestAlarmSchedulerStartStop:
    def test_start_creates_daemon_thread(self):
        scheduler = AlarmScheduler()
        with patch.object(scheduler, "_run_loop"):
            scheduler.start()
            assert scheduler._running is True
            assert scheduler._thread is not None
            assert scheduler._thread.daemon is True
            assert scheduler._thread.name == "cloudwatch-alarms"
            scheduler.stop()

    def test_start_is_idempotent(self):
        scheduler = AlarmScheduler()
        with patch.object(scheduler, "_run_loop"):
            scheduler.start()
            thread1 = scheduler._thread
            scheduler.start()  # second call
            thread2 = scheduler._thread
            assert thread1 is thread2
            scheduler.stop()

    def test_stop_sets_running_false(self):
        scheduler = AlarmScheduler()
        scheduler._running = True
        scheduler.stop()
        assert scheduler._running is False


# ---------------------------------------------------------------------------
# _compute_statistic
# ---------------------------------------------------------------------------


class TestComputeStatistic:
    def test_average(self):
        assert AlarmScheduler._compute_statistic([10, 20, 30], "average") == 20.0

    def test_sum(self):
        assert AlarmScheduler._compute_statistic([10, 20, 30], "sum") == 60.0

    def test_minimum(self):
        assert AlarmScheduler._compute_statistic([10, 20, 30], "minimum") == 10.0

    def test_maximum(self):
        assert AlarmScheduler._compute_statistic([10, 20, 30], "maximum") == 30.0

    def test_samplecount(self):
        assert AlarmScheduler._compute_statistic([1, 2, 3], "samplecount") == 3.0

    def test_empty_returns_zero(self):
        assert AlarmScheduler._compute_statistic([], "average") == 0.0

    def test_unknown_stat_defaults_to_average(self):
        assert AlarmScheduler._compute_statistic([10, 20], "p99") == 15.0


# ---------------------------------------------------------------------------
# _dimensions_match
# ---------------------------------------------------------------------------


class TestDimensionsMatch:
    def test_no_alarm_dims_matches_anything(self):
        assert AlarmScheduler._dimensions_match([_make_dimension("a", "b")], []) is True

    def test_matching_dims(self):
        alarm_dims = [_make_dimension("InstanceId", "i-123")]
        datum_dims = [_make_dimension("InstanceId", "i-123")]
        assert AlarmScheduler._dimensions_match(datum_dims, alarm_dims) is True

    def test_mismatched_dims(self):
        alarm_dims = [_make_dimension("InstanceId", "i-123")]
        datum_dims = [_make_dimension("InstanceId", "i-456")]
        assert AlarmScheduler._dimensions_match(datum_dims, alarm_dims) is False

    def test_different_length_dims(self):
        alarm_dims = [_make_dimension("A", "1"), _make_dimension("B", "2")]
        datum_dims = [_make_dimension("A", "1")]
        assert AlarmScheduler._dimensions_match(datum_dims, alarm_dims) is False


# ---------------------------------------------------------------------------
# _determine_state
# ---------------------------------------------------------------------------


class TestDetermineState:
    def setup_method(self):
        self.scheduler = AlarmScheduler()

    def test_all_breaching_returns_alarm(self):
        alarm = _make_alarm(comparison_operator="GreaterThanThreshold", threshold=50)
        values = [60.0, 70.0, 80.0]
        result = self.scheduler._determine_state(values, alarm, 3, 3)
        assert result == "ALARM"

    def test_none_breaching_returns_ok(self):
        alarm = _make_alarm(comparison_operator="GreaterThanThreshold", threshold=50)
        values = [10.0, 20.0, 30.0]
        result = self.scheduler._determine_state(values, alarm, 3, 3)
        assert result == "OK"

    def test_partial_breach_below_threshold_returns_ok(self):
        alarm = _make_alarm(comparison_operator="GreaterThanThreshold", threshold=50)
        values = [60.0, 10.0, 20.0]
        result = self.scheduler._determine_state(values, alarm, 3, 3)
        assert result == "OK"

    def test_partial_breach_at_threshold_returns_alarm(self):
        alarm = _make_alarm(comparison_operator="GreaterThanThreshold", threshold=50)
        values = [60.0, 70.0, 10.0]
        result = self.scheduler._determine_state(values, alarm, 3, 2)
        assert result == "ALARM"

    # --- TreatMissingData modes ---

    def test_all_missing_treat_missing_returns_insufficient_data(self):
        alarm = _make_alarm(treat_missing_data="missing")
        values = [None, None, None]
        result = self.scheduler._determine_state(values, alarm, 3, 3)
        assert result == "INSUFFICIENT_DATA"

    def test_all_missing_treat_breaching_returns_alarm(self):
        alarm = _make_alarm(treat_missing_data="breaching")
        values = [None, None, None]
        result = self.scheduler._determine_state(values, alarm, 3, 3)
        assert result == "ALARM"

    def test_all_missing_treat_not_breaching_returns_ok(self):
        alarm = _make_alarm(treat_missing_data="notBreaching")
        values = [None, None, None]
        result = self.scheduler._determine_state(values, alarm, 3, 3)
        assert result == "OK"

    def test_all_missing_treat_ignore_returns_none(self):
        alarm = _make_alarm(treat_missing_data="ignore")
        values = [None, None, None]
        result = self.scheduler._determine_state(values, alarm, 3, 3)
        assert result is None

    def test_some_missing_treat_breaching_counts_as_breach(self):
        alarm = _make_alarm(
            comparison_operator="GreaterThanThreshold",
            threshold=50,
            treat_missing_data="breaching",
        )
        # 1 real breach + 2 missing (treated as breaching) = 3 >= 3
        values = [60.0, None, None]
        result = self.scheduler._determine_state(values, alarm, 3, 3)
        assert result == "ALARM"

    def test_some_missing_treat_not_breaching_does_not_count(self):
        alarm = _make_alarm(
            comparison_operator="GreaterThanThreshold",
            threshold=50,
            treat_missing_data="notBreaching",
        )
        # 1 real breach + 2 missing (not breaching) = 1 < 3
        values = [60.0, None, None]
        result = self.scheduler._determine_state(values, alarm, 3, 3)
        assert result == "OK"

    def test_default_treat_missing_is_missing(self):
        alarm = _make_alarm(treat_missing_data=None)
        values = [None, None]
        result = self.scheduler._determine_state(values, alarm, 2, 2)
        assert result == "INSUFFICIENT_DATA"


# ---------------------------------------------------------------------------
# _build_reason
# ---------------------------------------------------------------------------


class TestBuildReason:
    def test_alarm_reason_greater(self):
        alarm = _make_alarm(
            comparison_operator="GreaterThanThreshold", threshold=80.0, evaluation_periods=3
        )
        reason = AlarmScheduler._build_reason("ALARM", alarm, [90.0, 85.0, None])
        assert "greater" in reason
        assert "80.0" in reason
        assert "2 datapoint(s)" in reason

    def test_alarm_reason_less(self):
        alarm = _make_alarm(comparison_operator="LessThanThreshold", threshold=10.0)
        reason = AlarmScheduler._build_reason("ALARM", alarm, [5.0])
        assert "less" in reason

    def test_ok_reason(self):
        alarm = _make_alarm(threshold=80.0)
        reason = AlarmScheduler._build_reason("OK", alarm, [50.0, 60.0])
        assert "not breaching" in reason.lower() or "not" in reason.lower()

    def test_insufficient_data_reason(self):
        alarm = _make_alarm(evaluation_periods=5)
        reason = AlarmScheduler._build_reason("INSUFFICIENT_DATA", alarm, [None, None])
        assert "Insufficient Data" in reason
        assert "5 period(s)" in reason


# ---------------------------------------------------------------------------
# _evaluate_alarm — full flow with mocked backend
# ---------------------------------------------------------------------------


class TestEvaluateAlarm:
    def test_skips_alarm_without_metric_name(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(metric_name=None)
        backend = MagicMock()
        # Should not raise; set_alarm_state should not be called
        scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")
        backend.set_alarm_state.assert_not_called()

    def test_skips_alarm_without_comparison_operator(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(comparison_operator=None)
        backend = MagicMock()
        scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")
        backend.set_alarm_state.assert_not_called()

    def test_skips_alarm_with_unsupported_operator(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(comparison_operator="GreaterThanUpperThreshold")
        backend = MagicMock()
        scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")
        backend.set_alarm_state.assert_not_called()

    def test_skips_alarm_with_none_threshold(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(threshold=None)
        backend = MagicMock()
        scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")
        backend.set_alarm_state.assert_not_called()

    def test_skips_alarm_without_statistic(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(statistic=None)
        backend = MagicMock()
        scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")
        backend.set_alarm_state.assert_not_called()

    def test_skips_alarm_with_actions_disabled(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(actions_enabled=False)
        backend = MagicMock()
        scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")
        backend.set_alarm_state.assert_not_called()

    def test_transitions_ok_to_alarm(self):
        scheduler = AlarmScheduler()
        now = datetime.now(tz=UTC)
        alarm = _make_alarm(
            state_value="OK",
            comparison_operator="GreaterThanThreshold",
            threshold=50.0,
            period=60,
            evaluation_periods=1,
            alarm_actions=["arn:aws:sns:us-east-1:123456789012:my-topic"],
        )
        datum = _make_metric_datum(
            "AWS/EC2",
            "CPUUtilization",
            90.0,
            now - timedelta(seconds=30),
        )
        backend = MagicMock()
        backend.metric_data = [datum]
        backend.aws_metric_data = []

        with patch.object(scheduler, "_dispatch_actions"):
            scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")

        backend.set_alarm_state.assert_called_once()
        call_args = backend.set_alarm_state.call_args
        assert call_args[0][0] == "test-alarm"
        assert call_args[0][3] == "ALARM"

    def test_no_transition_when_state_unchanged(self):
        scheduler = AlarmScheduler()
        now = datetime.now(tz=UTC)
        alarm = _make_alarm(
            state_value="ALARM",
            comparison_operator="GreaterThanThreshold",
            threshold=50.0,
            period=60,
            evaluation_periods=1,
        )
        datum = _make_metric_datum(
            "AWS/EC2",
            "CPUUtilization",
            90.0,
            now - timedelta(seconds=30),
        )
        backend = MagicMock()
        backend.metric_data = [datum]
        backend.aws_metric_data = []

        scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")
        backend.set_alarm_state.assert_not_called()

    def test_transitions_alarm_to_ok(self):
        scheduler = AlarmScheduler()
        now = datetime.now(tz=UTC)
        alarm = _make_alarm(
            state_value="ALARM",
            comparison_operator="GreaterThanThreshold",
            threshold=50.0,
            period=60,
            evaluation_periods=1,
            ok_actions=["arn:aws:sns:us-east-1:123456789012:ok-topic"],
        )
        # Value below threshold
        datum = _make_metric_datum(
            "AWS/EC2",
            "CPUUtilization",
            10.0,
            now - timedelta(seconds=30),
        )
        backend = MagicMock()
        backend.metric_data = [datum]
        backend.aws_metric_data = []

        with patch.object(scheduler, "_dispatch_actions"):
            scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")

        backend.set_alarm_state.assert_called_once()
        assert backend.set_alarm_state.call_args[0][3] == "OK"


# ---------------------------------------------------------------------------
# _dispatch_actions
# ---------------------------------------------------------------------------


class TestDispatchActions:
    def test_alarm_dispatches_alarm_actions(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            alarm_actions=["arn:aws:sns:us-east-1:123456789012:my-topic"],
            ok_actions=["arn:aws:sns:us-east-1:123456789012:ok-topic"],
        )
        with patch.object(scheduler, "_publish_to_sns") as mock_publish:
            scheduler._dispatch_actions(alarm, "OK", "ALARM", "reason", "123456789012", "us-east-1")
        mock_publish.assert_called_once()
        assert mock_publish.call_args[0][0] == "arn:aws:sns:us-east-1:123456789012:my-topic"

    def test_ok_dispatches_ok_actions(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            ok_actions=["arn:aws:sns:us-east-1:123456789012:ok-topic"],
        )
        with patch.object(scheduler, "_publish_to_sns") as mock_publish:
            scheduler._dispatch_actions(alarm, "ALARM", "OK", "reason", "123456789012", "us-east-1")
        mock_publish.assert_called_once()
        assert mock_publish.call_args[0][0] == "arn:aws:sns:us-east-1:123456789012:ok-topic"

    def test_insufficient_data_dispatches_insufficient_actions(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            insufficient_data_actions=["arn:aws:sns:us-east-1:123456789012:insuf-topic"],
        )
        with patch.object(scheduler, "_publish_to_sns") as mock_publish:
            scheduler._dispatch_actions(
                alarm, "OK", "INSUFFICIENT_DATA", "reason", "123456789012", "us-east-1"
            )
        mock_publish.assert_called_once()

    def test_no_actions_does_not_publish(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(alarm_actions=[])
        with patch.object(scheduler, "_publish_to_sns") as mock_publish:
            scheduler._dispatch_actions(alarm, "OK", "ALARM", "reason", "123456789012", "us-east-1")
        mock_publish.assert_not_called()


# ---------------------------------------------------------------------------
# _build_alarm_message
# ---------------------------------------------------------------------------


class TestBuildAlarmMessage:
    def test_message_contains_expected_fields(self):
        alarm = _make_alarm(
            name="test-alarm",
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            comparison_operator="GreaterThanThreshold",
            threshold=80.0,
            statistic="Average",
            period=60,
            evaluation_periods=3,
            description="Test alarm",
        )
        msg_str = AlarmScheduler._build_alarm_message(
            alarm, "OK", "ALARM", "some reason", "123456789012", "us-east-1"
        )
        msg = json.loads(msg_str)
        assert msg["AlarmName"] == "test-alarm"
        assert msg["NewStateValue"] == "ALARM"
        assert msg["OldStateValue"] == "OK"
        assert msg["NewStateReason"] == "some reason"
        assert msg["AWSAccountId"] == "123456789012"
        assert msg["Region"] == "us-east-1"
        assert msg["AlarmDescription"] == "Test alarm"
        trigger = msg["Trigger"]
        assert trigger["MetricName"] == "CPUUtilization"
        assert trigger["Namespace"] == "AWS/EC2"
        assert trigger["Threshold"] == 80.0
        assert trigger["ComparisonOperator"] == "GreaterThanThreshold"
        assert trigger["Period"] == 60
        assert trigger["EvaluationPeriods"] == 3


# ---------------------------------------------------------------------------
# _publish_to_sns
# ---------------------------------------------------------------------------


class TestPublishToSns:
    def test_publishes_to_correct_sns_backend(self):
        mock_sns_backend = MagicMock()
        mock_backend_dict = {"123456789012": {"us-east-1": mock_sns_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            AlarmScheduler._publish_to_sns(
                "arn:aws:sns:us-east-1:123456789012:my-topic",
                "message body",
                "ALARM: test",
                "123456789012",
                "us-east-1",
            )

        mock_sns_backend.publish.assert_called_once_with(
            message="message body",
            arn="arn:aws:sns:us-east-1:123456789012:my-topic",
            subject="ALARM: test",
        )

    def test_invalid_arn_does_not_raise(self):
        # Should log a warning but not raise
        AlarmScheduler._publish_to_sns("not-an-arn", "msg", "subj", "123", "us-east-1")

    def test_missing_sns_backend_does_not_raise(self):
        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value={},
        ):
            AlarmScheduler._publish_to_sns(
                "arn:aws:sns:us-east-1:123456789012:topic",
                "msg",
                "subj",
                "123456789012",
                "us-east-1",
            )


# ---------------------------------------------------------------------------
# _evaluate_all_alarms
# ---------------------------------------------------------------------------


class TestEvaluateAllAlarms:
    def test_iterates_accounts_and_regions(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm()

        mock_backend = MagicMock()
        mock_backend.alarms = {"test-alarm": alarm}

        mock_cw_backends = {
            "111111111111": {
                "us-east-1": mock_backend,
            }
        }

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_cw_backends,
        ):
            with patch.object(scheduler, "_evaluate_alarm") as mock_eval:
                scheduler._evaluate_all_alarms()

        mock_eval.assert_called_once_with(mock_backend, alarm, "111111111111", "us-east-1")

    def test_handles_get_backend_exception(self):
        scheduler = AlarmScheduler()
        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            side_effect=Exception("no backend"),
        ):
            # Should not raise
            scheduler._evaluate_all_alarms()


# ---------------------------------------------------------------------------
# get_alarm_scheduler singleton
# ---------------------------------------------------------------------------


class TestGetAlarmScheduler:
    def test_returns_same_instance(self):
        import robotocore.services.cloudwatch.alarm_scheduler as mod

        old = mod._scheduler
        try:
            mod._scheduler = None
            s1 = get_alarm_scheduler()
            s2 = get_alarm_scheduler()
            assert s1 is s2
            assert isinstance(s1, AlarmScheduler)
        finally:
            mod._scheduler = old


# ---------------------------------------------------------------------------
# _collect_metric_values (direct tests)
# ---------------------------------------------------------------------------


class TestCollectMetricValues:
    def test_returns_values_for_matching_metric(self):
        scheduler = AlarmScheduler()
        now = datetime.now(tz=UTC)
        alarm = _make_alarm(
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            statistic="Average",
            period=60,
            evaluation_periods=2,
        )
        d1 = _make_metric_datum("AWS/EC2", "CPUUtilization", 80.0, now - timedelta(seconds=30))
        d2 = _make_metric_datum("AWS/EC2", "CPUUtilization", 60.0, now - timedelta(seconds=90))
        backend = MagicMock()
        backend.metric_data = [d1, d2]
        backend.aws_metric_data = []

        values = scheduler._collect_metric_values(backend, alarm, 60, 2)
        # Oldest first after reverse
        assert len(values) == 2
        assert values[0] == 60.0
        assert values[1] == 80.0

    def test_returns_none_for_missing_periods(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            statistic="Average",
            period=60,
            evaluation_periods=3,
        )
        backend = MagicMock()
        backend.metric_data = []
        backend.aws_metric_data = []

        values = scheduler._collect_metric_values(backend, alarm, 60, 3)
        assert values == [None, None, None]

    def test_filters_by_namespace(self):
        scheduler = AlarmScheduler()
        now = datetime.now(tz=UTC)
        alarm = _make_alarm(
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            statistic="Average",
            period=60,
            evaluation_periods=1,
        )
        wrong_ns = _make_metric_datum(
            "AWS/Lambda", "CPUUtilization", 99.0, now - timedelta(seconds=30)
        )
        backend = MagicMock()
        backend.metric_data = [wrong_ns]
        backend.aws_metric_data = []

        values = scheduler._collect_metric_values(backend, alarm, 60, 1)
        assert values == [None]

    def test_filters_by_dimensions(self):
        scheduler = AlarmScheduler()
        now = datetime.now(tz=UTC)
        alarm = _make_alarm(
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            statistic="Sum",
            period=60,
            evaluation_periods=1,
            dimensions=[_make_dimension("InstanceId", "i-123")],
        )
        matching = _make_metric_datum(
            "AWS/EC2",
            "CPUUtilization",
            50.0,
            now - timedelta(seconds=30),
            dimensions=[_make_dimension("InstanceId", "i-123")],
        )
        non_matching = _make_metric_datum(
            "AWS/EC2",
            "CPUUtilization",
            99.0,
            now - timedelta(seconds=30),
            dimensions=[_make_dimension("InstanceId", "i-456")],
        )
        backend = MagicMock()
        backend.metric_data = [matching, non_matching]
        backend.aws_metric_data = []

        values = scheduler._collect_metric_values(backend, alarm, 60, 1)
        assert values == [50.0]

    def test_uses_aws_metric_data_too(self):
        scheduler = AlarmScheduler()
        now = datetime.now(tz=UTC)
        alarm = _make_alarm(
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            statistic="Average",
            period=60,
            evaluation_periods=1,
        )
        d = _make_metric_datum("AWS/EC2", "CPUUtilization", 42.0, now - timedelta(seconds=30))
        backend = MagicMock()
        backend.metric_data = []
        backend.aws_metric_data = [d]

        values = scheduler._collect_metric_values(backend, alarm, 60, 1)
        assert values == [42.0]


# ---------------------------------------------------------------------------
# _dispatch_actions — exception handling
# ---------------------------------------------------------------------------


class TestDispatchActionsErrorHandling:
    def test_publish_failure_does_not_propagate(self):
        """If _publish_to_sns raises, _dispatch_actions logs but does not raise."""
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            alarm_actions=[
                "arn:aws:sns:us-east-1:123456789012:topic1",
                "arn:aws:sns:us-east-1:123456789012:topic2",
            ],
        )
        call_count = 0

        def failing_publish(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("SNS unavailable")

        with patch.object(scheduler, "_publish_to_sns", side_effect=failing_publish):
            # Should not raise even though first publish fails
            scheduler._dispatch_actions(alarm, "OK", "ALARM", "reason", "123456789012", "us-east-1")
        # Both topics were attempted
        assert call_count == 2

    def test_unknown_state_dispatches_nothing(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(alarm_actions=["arn:aws:sns:us-east-1:123:t"])
        with patch.object(scheduler, "_publish_to_sns") as mock_pub:
            scheduler._dispatch_actions(alarm, "OK", "UNKNOWN_STATE", "reason", "123", "us-east-1")
        mock_pub.assert_not_called()


# ---------------------------------------------------------------------------
# _evaluate_alarm — determine_state returns None
# ---------------------------------------------------------------------------


class TestEvaluateAlarmIgnoreMissing:
    def test_no_state_change_when_determine_state_returns_none(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            state_value="OK",
            treat_missing_data="ignore",
        )
        backend = MagicMock()
        backend.metric_data = []
        backend.aws_metric_data = []

        scheduler._evaluate_alarm(backend, alarm, "123456789012", "us-east-1")
        backend.set_alarm_state.assert_not_called()


# ---------------------------------------------------------------------------
# _run_loop — single iteration test
# ---------------------------------------------------------------------------


class TestRunLoop:
    def test_run_loop_calls_evaluate_and_stops(self):
        scheduler = AlarmScheduler()
        call_count = 0

        def fake_evaluate():
            nonlocal call_count
            call_count += 1
            scheduler._running = False  # stop after first iteration

        with patch.object(scheduler, "_evaluate_all_alarms", side_effect=fake_evaluate):
            with patch("robotocore.services.cloudwatch.alarm_scheduler.time.sleep"):
                scheduler._running = True
                scheduler._run_loop()

        assert call_count == 1

    def test_run_loop_continues_after_exception(self):
        scheduler = AlarmScheduler()
        call_count = 0

        def exploding_evaluate():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("boom")
            scheduler._running = False

        with patch.object(scheduler, "_evaluate_all_alarms", side_effect=exploding_evaluate):
            with patch("robotocore.services.cloudwatch.alarm_scheduler.time.sleep"):
                scheduler._running = True
                scheduler._run_loop()

        assert call_count == 2


# ---------------------------------------------------------------------------
# _evaluate_all_alarms — error handling per alarm
# ---------------------------------------------------------------------------


class TestEvaluateAllAlarmsPerAlarmError:
    def test_continues_evaluating_after_single_alarm_failure(self):
        scheduler = AlarmScheduler()
        alarm1 = _make_alarm(name="alarm1")
        alarm2 = _make_alarm(name="alarm2")

        mock_backend = MagicMock()
        mock_backend.alarms = {"alarm1": alarm1, "alarm2": alarm2}

        mock_cw_backends = {"111": {"us-east-1": mock_backend}}

        eval_calls = []

        def tracking_eval(backend, alarm, acct, region):
            eval_calls.append(alarm.name)
            if alarm.name == "alarm1":
                raise RuntimeError("eval failed")

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_cw_backends,
        ):
            with patch.object(scheduler, "_evaluate_alarm", side_effect=tracking_eval):
                scheduler._evaluate_all_alarms()

        assert "alarm1" in eval_calls
        assert "alarm2" in eval_calls
