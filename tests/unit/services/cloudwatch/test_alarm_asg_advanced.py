"""Advanced tests for CloudWatch alarm ASG actions and state transitions."""

import json
from unittest.mock import MagicMock, call, patch

from robotocore.services.cloudwatch.alarm_scheduler import AlarmScheduler


def _make_alarm(
    name="test-alarm",
    metric_name="CPUUtilization",
    comparison_operator="GreaterThanThreshold",
    threshold=80.0,
    statistic="Average",
    period=60,
    evaluation_periods=1,
    actions_enabled=True,
    alarm_actions=None,
    ok_actions=None,
    insufficient_data_actions=None,
    state_value="OK",
    namespace="AWS/EC2",
    dimensions=None,
    treat_missing_data="missing",
    datapoints_to_alarm=None,
    description="",
):
    alarm = MagicMock()
    alarm.name = name
    alarm.metric_name = metric_name
    alarm.comparison_operator = comparison_operator
    alarm.threshold = threshold
    alarm.statistic = statistic
    alarm.period = period
    alarm.evaluation_periods = evaluation_periods
    alarm.actions_enabled = actions_enabled
    alarm.alarm_actions = alarm_actions or []
    alarm.ok_actions = ok_actions or []
    alarm.insufficient_data_actions = insufficient_data_actions or []
    alarm.state_value = state_value
    alarm.namespace = namespace
    alarm.dimensions = dimensions or []
    alarm.treat_missing_data = treat_missing_data
    alarm.datapoints_to_alarm = datapoints_to_alarm
    alarm.description = description
    return alarm


class TestMultipleAlarmsTriggringDifferentASGPolicies:
    """Multiple alarms triggering different ASG policies."""

    def test_two_alarms_trigger_two_different_policies(self):
        scheduler = AlarmScheduler()
        asg_arn_1 = (
            "arn:aws:autoscaling:us-east-1:123456789012:"
            "scalingPolicy:uuid1:autoScalingGroupName/my-asg:policyName/scale-up"
        )
        asg_arn_2 = (
            "arn:aws:autoscaling:us-east-1:123456789012:"
            "scalingPolicy:uuid2:autoScalingGroupName/my-asg:policyName/scale-down"
        )
        alarm1 = _make_alarm(
            name="high-cpu",
            alarm_actions=[asg_arn_1],
            state_value="OK",
        )
        alarm2 = _make_alarm(
            name="low-cpu",
            comparison_operator="LessThanThreshold",
            threshold=20.0,
            alarm_actions=[asg_arn_2],
            state_value="OK",
        )

        mock_asg_backend = MagicMock()
        with patch("robotocore.services.cloudwatch.alarm_scheduler.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = (
                mock_asg_backend
            )

            # Dispatch alarm1 -> ALARM
            scheduler._dispatch_actions(
                alarm1, "OK", "ALARM", "reason1", "123456789012", "us-east-1"
            )
            # Dispatch alarm2 -> ALARM
            scheduler._dispatch_actions(
                alarm2, "OK", "ALARM", "reason2", "123456789012", "us-east-1"
            )

        assert mock_asg_backend.execute_policy.call_count == 2
        calls = mock_asg_backend.execute_policy.call_args_list
        assert calls[0] == call("my-asg", "scale-up")
        assert calls[1] == call("my-asg", "scale-down")


class TestAlarmWithBothSNSAndASGActions:
    """Alarm with both SNS and ASG actions - both should fire."""

    def test_both_sns_and_asg_fire(self):
        scheduler = AlarmScheduler()
        sns_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        asg_arn = (
            "arn:aws:autoscaling:us-east-1:123456789012:"
            "scalingPolicy:uuid1:autoScalingGroupName/asg1:policyName/scale-out"
        )
        alarm = _make_alarm(
            name="mixed-actions",
            alarm_actions=[sns_arn, asg_arn],
            state_value="OK",
        )

        mock_asg_backend = MagicMock()
        mock_topic = MagicMock()
        mock_sub = MagicMock()
        mock_sub.confirmed = True
        mock_topic.subscriptions = [mock_sub]
        mock_store = MagicMock()
        mock_store.get_topic.return_value = mock_topic
        mock_deliver = MagicMock()

        def mock_get_backend_fn(service):
            backends = MagicMock()
            if service == "autoscaling":
                backends.__getitem__ = lambda s, k: MagicMock(
                    __getitem__=lambda s2, k2: mock_asg_backend
                )
            return backends

        with (
            patch(
                "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
                side_effect=mock_get_backend_fn,
            ),
            patch("robotocore.services.sns.provider._get_store", return_value=mock_store),
            patch("robotocore.services.sns.provider._deliver_to_subscriber", mock_deliver),
            patch("robotocore.services.sns.provider._new_id", return_value="test-id"),
        ):
            scheduler._dispatch_actions(
                alarm, "OK", "ALARM", "threshold crossed", "123456789012", "us-east-1"
            )

        mock_deliver.assert_called_once()
        mock_asg_backend.execute_policy.assert_called_once_with("asg1", "scale-out")


class TestAlarmStateTransitions:
    """Alarm state transitions: OK -> ALARM -> OK, actions fire on each transition."""

    def _mock_sns_provider(self):
        """Create mock patches for the SNS provider used by _publish_to_sns."""
        mock_topic = MagicMock()
        mock_sub = MagicMock()
        mock_sub.confirmed = True
        mock_topic.subscriptions = [mock_sub]
        mock_store = MagicMock()
        mock_store.get_topic.return_value = mock_topic
        mock_deliver = MagicMock()
        return mock_store, mock_deliver, mock_sub

    def test_ok_to_alarm_fires_alarm_actions(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            alarm_actions=["arn:aws:sns:us-east-1:123456789012:alarm-topic"],
            ok_actions=["arn:aws:sns:us-east-1:123456789012:ok-topic"],
        )
        mock_store, mock_deliver, _ = self._mock_sns_provider()
        with (
            patch("robotocore.services.sns.provider._get_store", return_value=mock_store),
            patch("robotocore.services.sns.provider._deliver_to_subscriber", mock_deliver),
            patch("robotocore.services.sns.provider._new_id", return_value="test-id"),
        ):
            scheduler._dispatch_actions(
                alarm, "OK", "ALARM", "breaching", "123456789012", "us-east-1"
            )
        mock_deliver.assert_called_once()
        mock_store.get_topic.assert_called_with("arn:aws:sns:us-east-1:123456789012:alarm-topic")

    def test_alarm_to_ok_fires_ok_actions(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            alarm_actions=["arn:aws:sns:us-east-1:123456789012:alarm-topic"],
            ok_actions=["arn:aws:sns:us-east-1:123456789012:ok-topic"],
        )
        mock_store, mock_deliver, _ = self._mock_sns_provider()
        with (
            patch("robotocore.services.sns.provider._get_store", return_value=mock_store),
            patch("robotocore.services.sns.provider._deliver_to_subscriber", mock_deliver),
            patch("robotocore.services.sns.provider._new_id", return_value="test-id"),
        ):
            scheduler._dispatch_actions(
                alarm, "ALARM", "OK", "not breaching", "123456789012", "us-east-1"
            )
        mock_deliver.assert_called_once()
        mock_store.get_topic.assert_called_with("arn:aws:sns:us-east-1:123456789012:ok-topic")

    def test_full_cycle_ok_alarm_ok(self):
        """Full cycle: OK -> ALARM -> OK. Each transition dispatches appropriate actions."""
        scheduler = AlarmScheduler()
        asg_arn = (
            "arn:aws:autoscaling:us-east-1:123456789012:"
            "scalingPolicy:uuid1:autoScalingGroupName/asg:policyName/scale-up"
        )
        alarm = _make_alarm(
            alarm_actions=[asg_arn],
            ok_actions=["arn:aws:sns:us-east-1:123456789012:ok-topic"],
        )
        mock_asg = MagicMock()
        mock_store, mock_deliver, _ = self._mock_sns_provider()

        def mock_get_backend_fn(service):
            backends = MagicMock()
            if service == "autoscaling":
                backends.__getitem__ = lambda s, k: MagicMock(__getitem__=lambda s2, k2: mock_asg)
            return backends

        with (
            patch(
                "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
                side_effect=mock_get_backend_fn,
            ),
            patch("robotocore.services.sns.provider._get_store", return_value=mock_store),
            patch("robotocore.services.sns.provider._deliver_to_subscriber", mock_deliver),
            patch("robotocore.services.sns.provider._new_id", return_value="test-id"),
        ):
            # OK -> ALARM (fires ASG action)
            scheduler._dispatch_actions(
                alarm, "OK", "ALARM", "breaching", "123456789012", "us-east-1"
            )
            # ALARM -> OK (fires SNS action)
            scheduler._dispatch_actions(
                alarm, "ALARM", "OK", "not breaching", "123456789012", "us-east-1"
            )

        mock_asg.execute_policy.assert_called_once()
        mock_deliver.assert_called_once()

    def test_insufficient_data_fires_insufficient_actions(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            insufficient_data_actions=["arn:aws:sns:us-east-1:123456789012:insuff-topic"],
        )
        mock_store, mock_deliver, _ = self._mock_sns_provider()
        with (
            patch("robotocore.services.sns.provider._get_store", return_value=mock_store),
            patch("robotocore.services.sns.provider._deliver_to_subscriber", mock_deliver),
            patch("robotocore.services.sns.provider._new_id", return_value="test-id"),
        ):
            scheduler._dispatch_actions(
                alarm, "OK", "INSUFFICIENT_DATA", "no data", "123456789012", "us-east-1"
            )
        mock_deliver.assert_called_once()


class TestAlarmMessageFormat:
    """Alarm messages match AWS format."""

    def test_alarm_message_contains_trigger_info(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            name="cpu-high",
            metric_name="CPUUtilization",
            namespace="AWS/EC2",
            threshold=90.0,
            comparison_operator="GreaterThanThreshold",
            description="CPU is too high",
        )
        msg = scheduler._build_alarm_message(
            alarm, "OK", "ALARM", "Threshold crossed", "123456789012", "us-east-1"
        )
        parsed = json.loads(msg)
        assert parsed["AlarmName"] == "cpu-high"
        assert parsed["AlarmDescription"] == "CPU is too high"
        assert parsed["NewStateValue"] == "ALARM"
        assert parsed["OldStateValue"] == "OK"
        assert parsed["Trigger"]["MetricName"] == "CPUUtilization"
        assert parsed["Trigger"]["Namespace"] == "AWS/EC2"
        assert parsed["Trigger"]["Threshold"] == 90.0
        assert parsed["AWSAccountId"] == "123456789012"
        assert parsed["Region"] == "us-east-1"


class TestDetermineState:
    """State determination logic."""

    def test_all_breaching_returns_alarm(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(threshold=50.0, comparison_operator="GreaterThanThreshold")
        state = scheduler._determine_state([60.0, 70.0, 80.0], alarm, 3, 3)
        assert state == "ALARM"

    def test_none_breaching_returns_ok(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(threshold=50.0, comparison_operator="GreaterThanThreshold")
        state = scheduler._determine_state([10.0, 20.0, 30.0], alarm, 3, 3)
        assert state == "OK"

    def test_all_missing_treat_as_breaching(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            threshold=50.0,
            comparison_operator="GreaterThanThreshold",
            treat_missing_data="breaching",
        )
        state = scheduler._determine_state([None, None, None], alarm, 3, 3)
        assert state == "ALARM"

    def test_all_missing_treat_as_not_breaching(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            threshold=50.0,
            comparison_operator="GreaterThanThreshold",
            treat_missing_data="notBreaching",
        )
        state = scheduler._determine_state([None, None, None], alarm, 3, 3)
        assert state == "OK"

    def test_all_missing_treat_as_ignore(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            threshold=50.0,
            comparison_operator="GreaterThanThreshold",
            treat_missing_data="ignore",
        )
        state = scheduler._determine_state([None, None, None], alarm, 3, 3)
        assert state is None

    def test_partial_breach_below_datapoints_to_alarm(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            threshold=50.0,
            comparison_operator="GreaterThanThreshold",
        )
        # Only 1 of 3 is breaching, need 3
        state = scheduler._determine_state([60.0, 10.0, 10.0], alarm, 3, 3)
        assert state == "OK"

    def test_partial_breach_meets_datapoints_to_alarm(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(
            threshold=50.0,
            comparison_operator="GreaterThanThreshold",
        )
        # 2 of 3 breaching, need 2
        state = scheduler._determine_state([60.0, 70.0, 10.0], alarm, 3, 2)
        assert state == "ALARM"


class TestASGActionParsing:
    """Auto Scaling action ARN parsing."""

    def test_scaling_policy_arn_parsed_correctly(self):
        scheduler = AlarmScheduler()
        mock_asg_backend = MagicMock()
        with patch("robotocore.services.cloudwatch.alarm_scheduler.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = (
                mock_asg_backend
            )
            scheduler._execute_autoscaling_action(
                "arn:aws:autoscaling:us-east-1:123456789012:"
                "scalingPolicy:abc:autoScalingGroupName/web-asg:policyName/scale-out",
                "123456789012",
                "us-east-1",
            )
        mock_asg_backend.execute_policy.assert_called_once_with("web-asg", "scale-out")

    def test_asg_group_arn_increments_desired_capacity(self):
        scheduler = AlarmScheduler()
        mock_group = MagicMock()
        mock_group.desired_capacity = 2
        mock_group.max_size = 10
        mock_asg_backend = MagicMock()
        mock_asg_backend.describe_auto_scaling_groups.return_value = [mock_group]
        with patch("robotocore.services.cloudwatch.alarm_scheduler.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = (
                mock_asg_backend
            )
            scheduler._execute_autoscaling_action(
                "arn:aws:autoscaling:us-east-1:123456789012:"
                "autoScalingGroup:uuid:autoScalingGroupName/my-asg",
                "123456789012",
                "us-east-1",
            )
        mock_asg_backend.set_desired_capacity.assert_called_once_with("my-asg", 3)

    def test_asg_group_respects_max_size(self):
        scheduler = AlarmScheduler()
        mock_group = MagicMock()
        mock_group.desired_capacity = 5
        mock_group.max_size = 5
        mock_asg_backend = MagicMock()
        mock_asg_backend.describe_auto_scaling_groups.return_value = [mock_group]
        with patch("robotocore.services.cloudwatch.alarm_scheduler.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = (
                mock_asg_backend
            )
            scheduler._execute_autoscaling_action(
                "arn:aws:autoscaling:us-east-1:123456789012:"
                "autoScalingGroup:uuid:autoScalingGroupName/at-max",
                "123456789012",
                "us-east-1",
            )
        mock_asg_backend.set_desired_capacity.assert_called_once_with("at-max", 5)

    def test_unknown_asg_action_type_logs_warning(self, caplog):
        import logging

        scheduler = AlarmScheduler()
        with patch("robotocore.services.cloudwatch.alarm_scheduler.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = MagicMock()
            with caplog.at_level(
                logging.WARNING, logger="robotocore.services.cloudwatch.alarm_scheduler"
            ):
                scheduler._execute_autoscaling_action(
                    "arn:aws:autoscaling:us-east-1:123456789012:unknownType:blah",
                    "123456789012",
                    "us-east-1",
                )
        assert any("Unknown Auto Scaling action type" in rec.message for rec in caplog.records)
