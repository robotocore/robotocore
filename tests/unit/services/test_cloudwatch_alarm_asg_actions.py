"""Tests for CloudWatch alarm Auto Scaling action dispatch."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robotocore.services.cloudwatch.alarm_scheduler import AlarmScheduler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


POLICY_ARN = (
    "arn:aws:autoscaling:us-east-1:123456789012:scalingPolicy:"
    "abcd-1234:autoScalingGroupName/my-asg:policyName/scale-up"
)

ASG_ARN = (
    "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:"
    "abcd-1234:autoScalingGroupName/my-asg"
)

SNS_ARN = "arn:aws:sns:us-east-1:123456789012:my-topic"


# ---------------------------------------------------------------------------
# _dispatch_actions routes Auto Scaling ARNs correctly
# ---------------------------------------------------------------------------


class TestDispatchActionsRoutesAutoScaling:
    def test_autoscaling_arn_calls_execute_autoscaling(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(alarm_actions=[POLICY_ARN])
        with (
            patch.object(scheduler, "_execute_autoscaling_action") as mock_asg,
            patch.object(scheduler, "_publish_to_sns") as mock_sns,
        ):
            scheduler._dispatch_actions(alarm, "OK", "ALARM", "reason", "123456789012", "us-east-1")
        mock_asg.assert_called_once_with(POLICY_ARN, "123456789012", "us-east-1")
        mock_sns.assert_not_called()

    def test_sns_arn_still_publishes_to_sns(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(alarm_actions=[SNS_ARN])
        with (
            patch.object(scheduler, "_execute_autoscaling_action") as mock_asg,
            patch.object(scheduler, "_publish_to_sns") as mock_sns,
        ):
            scheduler._dispatch_actions(alarm, "OK", "ALARM", "reason", "123456789012", "us-east-1")
        mock_sns.assert_called_once()
        mock_asg.assert_not_called()

    def test_mixed_actions_dispatch_correctly(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(alarm_actions=[SNS_ARN, POLICY_ARN])
        with (
            patch.object(scheduler, "_execute_autoscaling_action") as mock_asg,
            patch.object(scheduler, "_publish_to_sns") as mock_sns,
        ):
            scheduler._dispatch_actions(alarm, "OK", "ALARM", "reason", "123456789012", "us-east-1")
        mock_sns.assert_called_once()
        mock_asg.assert_called_once()

    def test_autoscaling_failure_does_not_block_other_actions(self):
        scheduler = AlarmScheduler()
        alarm = _make_alarm(alarm_actions=[POLICY_ARN, SNS_ARN])
        with (
            patch.object(
                scheduler,
                "_execute_autoscaling_action",
                side_effect=RuntimeError("asg fail"),
            ),
            patch.object(scheduler, "_publish_to_sns") as mock_sns,
        ):
            scheduler._dispatch_actions(alarm, "OK", "ALARM", "reason", "123456789012", "us-east-1")
        # SNS action should still be attempted after ASG failure
        mock_sns.assert_called_once()


# ---------------------------------------------------------------------------
# _execute_autoscaling_action — scaling policy
# ---------------------------------------------------------------------------


class TestExecuteScalingPolicy:
    def test_executes_policy_via_moto_backend(self):
        mock_backend = MagicMock()
        mock_backend_dict = {"123456789012": {"us-east-1": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            AlarmScheduler._execute_autoscaling_action(POLICY_ARN, "123456789012", "us-east-1")

        mock_backend.execute_policy.assert_called_once_with("my-asg", "scale-up")

    def test_handles_invalid_arn(self):
        # Should log warning but not raise
        AlarmScheduler._execute_autoscaling_action("not-an-arn", "123456789012", "us-east-1")

    def test_handles_missing_backend(self):
        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value={},
        ):
            # Should not raise
            AlarmScheduler._execute_autoscaling_action(POLICY_ARN, "123456789012", "us-east-1")

    def test_handles_missing_policy_name(self):
        # ARN without policyName segment
        bad_arn = (
            "arn:aws:autoscaling:us-east-1:123456789012:scalingPolicy:"
            "abcd-1234:autoScalingGroupName/my-asg"
        )
        mock_backend = MagicMock()
        mock_backend_dict = {"123456789012": {"us-east-1": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            # Should not raise, should not call execute_policy
            AlarmScheduler._execute_autoscaling_action(bad_arn, "123456789012", "us-east-1")

        mock_backend.execute_policy.assert_not_called()

    def test_execute_policy_error_propagates(self):
        mock_backend = MagicMock()
        mock_backend.execute_policy.side_effect = RuntimeError("policy error")
        mock_backend_dict = {"123456789012": {"us-east-1": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            try:
                AlarmScheduler._execute_autoscaling_action(POLICY_ARN, "123456789012", "us-east-1")
                raised = False
            except RuntimeError:
                raised = True
        assert raised is True


# ---------------------------------------------------------------------------
# _execute_autoscaling_action — ASG (SetDesiredCapacity)
# ---------------------------------------------------------------------------


class TestExecuteAsgSetDesiredCapacity:
    def test_increments_desired_capacity(self):
        mock_group = MagicMock()
        mock_group.desired_capacity = 2
        mock_group.max_size = 10

        mock_backend = MagicMock()
        mock_backend.describe_auto_scaling_groups.return_value = [mock_group]
        mock_backend_dict = {"123456789012": {"us-east-1": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            AlarmScheduler._execute_autoscaling_action(ASG_ARN, "123456789012", "us-east-1")

        mock_backend.describe_auto_scaling_groups.assert_called_once_with(["my-asg"])
        mock_backend.set_desired_capacity.assert_called_once_with("my-asg", 3)

    def test_respects_max_size(self):
        mock_group = MagicMock()
        mock_group.desired_capacity = 5
        mock_group.max_size = 5

        mock_backend = MagicMock()
        mock_backend.describe_auto_scaling_groups.return_value = [mock_group]
        mock_backend_dict = {"123456789012": {"us-east-1": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            AlarmScheduler._execute_autoscaling_action(ASG_ARN, "123456789012", "us-east-1")

        # Should not exceed max_size
        mock_backend.set_desired_capacity.assert_called_once_with("my-asg", 5)

    def test_group_not_found_does_not_raise(self):
        mock_backend = MagicMock()
        mock_backend.describe_auto_scaling_groups.return_value = []
        mock_backend_dict = {"123456789012": {"us-east-1": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            # Should not raise
            AlarmScheduler._execute_autoscaling_action(ASG_ARN, "123456789012", "us-east-1")

        mock_backend.set_desired_capacity.assert_not_called()

    def test_handles_missing_asg_name_in_arn(self):
        bad_arn = "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:abcd-1234"
        mock_backend = MagicMock()
        mock_backend_dict = {"123456789012": {"us-east-1": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            # Should not raise
            AlarmScheduler._execute_autoscaling_action(bad_arn, "123456789012", "us-east-1")

        mock_backend.describe_auto_scaling_groups.assert_not_called()

    def test_set_desired_capacity_error_propagates(self):
        mock_group = MagicMock()
        mock_group.desired_capacity = 2
        mock_group.max_size = 10

        mock_backend = MagicMock()
        mock_backend.describe_auto_scaling_groups.return_value = [mock_group]
        mock_backend.set_desired_capacity.side_effect = RuntimeError("fail")
        mock_backend_dict = {"123456789012": {"us-east-1": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            try:
                AlarmScheduler._execute_autoscaling_action(ASG_ARN, "123456789012", "us-east-1")
                raised = False
            except RuntimeError:
                raised = True
        assert raised is True


# ---------------------------------------------------------------------------
# _execute_autoscaling_action — unknown resource type
# ---------------------------------------------------------------------------


class TestExecuteUnknownAutoScalingResource:
    def test_unknown_resource_type_does_not_raise(self):
        unknown_arn = "arn:aws:autoscaling:us-east-1:123456789012:unknownType:abcd-1234"
        mock_backend = MagicMock()
        mock_backend_dict = {"123456789012": {"us-east-1": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            # Should not raise
            AlarmScheduler._execute_autoscaling_action(unknown_arn, "123456789012", "us-east-1")

    def test_parses_region_from_arn(self):
        """Verify that the region is parsed from the ARN, not just using the provided one."""
        policy_arn_west = (
            "arn:aws:autoscaling:us-west-2:123456789012:scalingPolicy:"
            "abcd-1234:autoScalingGroupName/my-asg:policyName/scale-up"
        )
        mock_backend = MagicMock()
        mock_backend_dict = {"123456789012": {"us-west-2": mock_backend}}

        with patch(
            "robotocore.services.cloudwatch.alarm_scheduler.get_backend",
            return_value=mock_backend_dict,
        ):
            AlarmScheduler._execute_autoscaling_action(policy_arn_west, "123456789012", "us-east-1")

        mock_backend.execute_policy.assert_called_once_with("my-asg", "scale-up")
