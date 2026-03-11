"""Tests for EventBridge Scheduler Executor."""

import json
import time
from unittest.mock import patch

from robotocore.services.scheduler.provider import ScheduleExecutor, _schedules


class TestScheduleExecutor:
    def _create_schedule(
        self,
        name="test-sched",
        account_id="123456789012",
        region="us-east-1",
        expr="rate(1 minute)",
        state="ENABLED",
        target_arn="arn:aws:lambda:us-east-1:123456789012:function:my-func",
        target_input=None,
    ):
        """Create a schedule in the global _schedules dict."""
        key = (account_id, region)
        if key not in _schedules:
            _schedules[key] = {}
        _schedules[key][name] = {
            "Name": name,
            "Arn": f"arn:aws:scheduler:{region}:{account_id}:schedule/default/{name}",
            "GroupName": "default",
            "ScheduleExpression": expr,
            "State": state,
            "Target": {
                "Arn": target_arn,
                "Input": target_input,
            },
            "CreationDate": time.time(),
            "LastModificationDate": time.time(),
        }

    def _cleanup(self):
        _schedules.clear()

    @patch("robotocore.services.scheduler.provider.ScheduleExecutor._invoke_lambda")
    def test_fires_lambda_target(self, mock_lambda):
        """An ENABLED schedule with a Lambda target should fire."""
        try:
            self._create_schedule()
            executor = ScheduleExecutor()
            executor._check_all_schedules()

            assert mock_lambda.called
            call_args = mock_lambda.call_args
            assert "arn:aws:lambda:us-east-1:123456789012:function:my-func" in call_args[0][0]
        finally:
            self._cleanup()

    @patch("robotocore.services.scheduler.provider.ScheduleExecutor._invoke_lambda")
    def test_disabled_schedule_not_fired(self, mock_lambda):
        """DISABLED schedules should not fire."""
        try:
            self._create_schedule(state="DISABLED")
            executor = ScheduleExecutor()
            executor._check_all_schedules()

            assert not mock_lambda.called
        finally:
            self._cleanup()

    @patch("robotocore.services.scheduler.provider.ScheduleExecutor._invoke_lambda")
    def test_interval_tracking_prevents_double_fire(self, mock_lambda):
        """Schedule should not fire again until interval elapses."""
        try:
            self._create_schedule(expr="rate(1 minute)")
            executor = ScheduleExecutor()

            executor._check_all_schedules()
            assert mock_lambda.call_count == 1

            # Immediate re-check should NOT fire
            executor._check_all_schedules()
            assert mock_lambda.call_count == 1
        finally:
            self._cleanup()

    @patch("robotocore.services.scheduler.provider.ScheduleExecutor._invoke_lambda")
    def test_fires_after_interval_elapsed(self, mock_lambda):
        """Schedule should fire again after interval."""
        try:
            self._create_schedule(expr="rate(1 minute)")
            executor = ScheduleExecutor()

            executor._check_all_schedules()
            assert mock_lambda.call_count == 1

            # Set last-fired to >60s ago
            key = ("123456789012", "us-east-1", "test-sched")
            executor._last_fired[key] = time.monotonic() - 61

            executor._check_all_schedules()
            assert mock_lambda.call_count == 2
        finally:
            self._cleanup()

    @patch("robotocore.services.scheduler.provider.ScheduleExecutor._send_sqs")
    def test_fires_sqs_target(self, mock_sqs):
        """An ENABLED schedule with an SQS target should dispatch to SQS."""
        try:
            self._create_schedule(
                target_arn="arn:aws:sqs:us-east-1:123456789012:my-queue",
            )
            executor = ScheduleExecutor()
            executor._check_all_schedules()

            assert mock_sqs.called
        finally:
            self._cleanup()

    @patch("robotocore.services.scheduler.provider.ScheduleExecutor._publish_sns")
    def test_fires_sns_target(self, mock_sns):
        """An ENABLED schedule with an SNS target should dispatch to SNS."""
        try:
            self._create_schedule(
                target_arn="arn:aws:sns:us-east-1:123456789012:my-topic",
            )
            executor = ScheduleExecutor()
            executor._check_all_schedules()

            assert mock_sns.called
        finally:
            self._cleanup()

    @patch("robotocore.services.scheduler.provider.ScheduleExecutor._invoke_lambda")
    def test_custom_input_used(self, mock_lambda):
        """When Target.Input is set, it should be passed as the payload."""
        try:
            custom_input = json.dumps({"foo": "bar"})
            self._create_schedule(target_input=custom_input)
            executor = ScheduleExecutor()
            executor._check_all_schedules()

            assert mock_lambda.called
            payload = mock_lambda.call_args[0][1]
            assert payload == custom_input
        finally:
            self._cleanup()

    @patch("robotocore.services.scheduler.provider.ScheduleExecutor._invoke_lambda")
    def test_default_event_payload_when_no_input(self, mock_lambda):
        """When Target.Input is not set, a default scheduled event payload is used."""
        try:
            self._create_schedule(target_input=None)
            executor = ScheduleExecutor()
            executor._check_all_schedules()

            assert mock_lambda.called
            payload = json.loads(mock_lambda.call_args[0][1])
            assert payload["source"] == "aws.scheduler"
            assert payload["detail-type"] == "Scheduled Event"
            assert payload["detail"] == {}
        finally:
            self._cleanup()

    def test_start_stop(self):
        """Executor can be started and stopped."""
        executor = ScheduleExecutor()
        assert not executor.is_running()
        executor.start()
        assert executor.is_running()
        executor.stop()
        time.sleep(0.1)
        assert not executor.is_running()
