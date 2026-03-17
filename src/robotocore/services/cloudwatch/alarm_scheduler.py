"""CloudWatch Alarm Scheduler — evaluates metric alarms and dispatches SNS actions.

Background daemon thread that periodically evaluates all CloudWatch metric alarms
stored in Moto's backend. When an alarm transitions state (OK -> ALARM, ALARM -> OK,
etc.), the scheduler updates the alarm state via Moto and publishes notifications
to the configured SNS action ARNs.
"""

import json
import logging
import re
import threading
import time
from datetime import UTC, datetime, timedelta

from moto.backends import get_backend

logger = logging.getLogger(__name__)

# Evaluation interval in seconds
EVALUATION_INTERVAL = 10

COMPARISON_OPS = {
    "GreaterThanOrEqualToThreshold": lambda v, t: v >= t,
    "GreaterThanThreshold": lambda v, t: v > t,
    "LessThanThreshold": lambda v, t: v < t,
    "LessThanOrEqualToThreshold": lambda v, t: v <= t,
}

# Singleton scheduler
_scheduler: "AlarmScheduler | None" = None
_scheduler_lock = threading.Lock()


def get_alarm_scheduler() -> "AlarmScheduler":
    """Return the global AlarmScheduler singleton."""
    global _scheduler
    with _scheduler_lock:
        if _scheduler is None:
            _scheduler = AlarmScheduler()
        return _scheduler


class AlarmScheduler:
    """Periodically evaluates all CloudWatch metric alarms and dispatches actions."""

    def __init__(self) -> None:
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(
                target=self._run_loop, daemon=True, name="cloudwatch-alarms"
            )
            self._thread.start()
            logger.info("CloudWatch alarm scheduler started (interval=%ds)", EVALUATION_INTERVAL)

    def stop(self) -> None:
        with self._lock:
            self._running = False

    def _run_loop(self) -> None:
        """Main evaluation loop."""
        while self._running:
            try:
                self._evaluate_all_alarms()
            except Exception:
                logger.exception("Error in CloudWatch alarm evaluation loop")
            time.sleep(EVALUATION_INTERVAL)

    def _evaluate_all_alarms(self) -> None:
        """Iterate over all accounts/regions and evaluate every alarm."""
        try:
            cw_backends = get_backend("cloudwatch")
        except Exception:  # noqa: BLE001
            return

        # Iterate all accounts and regions
        for account_id in list(cw_backends.keys()):
            account_backends = cw_backends[account_id]
            for region_name in list(account_backends.keys()):
                try:
                    backend = account_backends[region_name]
                except (KeyError, TypeError):
                    continue
                for alarm in list(backend.alarms.values()):
                    try:
                        self._evaluate_alarm(backend, alarm, account_id, region_name)
                    except Exception:
                        logger.exception(
                            "Error evaluating alarm %s in %s/%s",
                            alarm.name,
                            account_id,
                            region_name,
                        )

    def _evaluate_alarm(self, backend, alarm, account_id: str, region_name: str) -> None:
        """Evaluate a single alarm against its metric data."""
        # Only evaluate alarms that have the required fields for simple metric alarms
        if not alarm.metric_name or not alarm.comparison_operator:
            return
        if alarm.comparison_operator not in COMPARISON_OPS:
            return
        if alarm.threshold is None:
            return
        if not alarm.statistic:
            return

        period = alarm.period or 60
        evaluation_periods = alarm.evaluation_periods or 1
        datapoints_to_alarm = alarm.datapoints_to_alarm or evaluation_periods

        # Collect metric values for the evaluation window
        metric_values = self._collect_metric_values(backend, alarm, period, evaluation_periods)

        # Determine new state
        old_state = alarm.state_value
        new_state = self._determine_state(
            metric_values, alarm, evaluation_periods, datapoints_to_alarm
        )

        if new_state is None:
            # No state change (e.g. treat_missing_data == "ignore")
            return

        if old_state == new_state:
            return

        # Build reason string
        reason = self._build_reason(new_state, alarm, metric_values)

        # Update alarm state in Moto
        backend.set_alarm_state(
            alarm.name,
            reason,
            "{}",
            new_state,
        )

        # Dispatch actions based on the new state (only if actions are enabled)
        if alarm.actions_enabled:
            self._dispatch_actions(alarm, old_state, new_state, reason, account_id, region_name)

    def _collect_metric_values(
        self, backend, alarm, period: int, evaluation_periods: int
    ) -> list[float | None]:
        """Collect metric data points from Moto's in-memory store for the evaluation window."""
        now = datetime.now(tz=UTC)
        values: list[float | None] = []

        namespace = alarm.namespace
        metric_name = alarm.metric_name
        alarm_dimensions = alarm.dimensions  # list of Dimension objects
        statistic = alarm.statistic.lower() if alarm.statistic else "average"

        # Map statistic names
        stat_map = {
            "average": "average",
            "sum": "sum",
            "minimum": "minimum",
            "maximum": "maximum",
            "samplecount": "samplecount",
        }
        stat_key = stat_map.get(statistic, "average")

        for i in range(evaluation_periods):
            end_time = now - timedelta(seconds=period * i)
            start_time = end_time - timedelta(seconds=period)

            # Filter metric_data from the backend for this period
            matching_values = []
            all_metric_data = backend.metric_data + backend.aws_metric_data
            for datum in all_metric_data:
                # Check namespace and metric name
                if datum.namespace != namespace:
                    continue
                if datum.name != metric_name:
                    continue
                # Check dimensions match
                if not self._dimensions_match(datum.dimensions, alarm_dimensions):
                    continue
                # Check timestamp is within the period
                # Moto may store timestamps as naive (UTC-assumed) datetimes
                ts = datum.timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
                if ts < start_time or ts >= end_time:
                    continue
                # Get the value
                if hasattr(datum, "value"):
                    matching_values.append(datum.value)

            if matching_values:
                value = self._compute_statistic(matching_values, stat_key)
                values.append(value)
            else:
                values.append(None)

        # Reverse so oldest is first
        values.reverse()
        return values

    @staticmethod
    def _dimensions_match(datum_dims: list, alarm_dims: list) -> bool:
        """Check if metric datum dimensions match alarm dimensions."""
        if not alarm_dims:
            return True
        if len(datum_dims) != len(alarm_dims):
            return False
        alarm_dim_set = {(d.name, d.value) for d in alarm_dims}
        datum_dim_set = {(d.name, d.value) for d in datum_dims}
        return alarm_dim_set == datum_dim_set

    @staticmethod
    def _compute_statistic(values: list[float], statistic: str) -> float:
        """Compute the requested statistic from a list of values."""
        if not values:
            return 0.0
        if statistic == "sum":
            return sum(values)
        if statistic == "minimum":
            return min(values)
        if statistic == "maximum":
            return max(values)
        if statistic == "samplecount":
            return float(len(values))
        # default: average
        return sum(values) / len(values)

    def _determine_state(
        self,
        metric_values: list[float | None],
        alarm,
        evaluation_periods: int,
        datapoints_to_alarm: int,
    ) -> str | None:
        """Determine the new alarm state based on metric values and threshold."""
        treat_missing = alarm.treat_missing_data or "missing"
        threshold = alarm.threshold
        comparison_op = COMPARISON_OPS[alarm.comparison_operator]

        # Count non-None values
        null_count = metric_values.count(None)

        # All datapoints missing
        if null_count == len(metric_values):
            if treat_missing == "missing":
                return "INSUFFICIENT_DATA"
            elif treat_missing == "breaching":
                return "ALARM"
            elif treat_missing == "notBreaching":
                return "OK"
            else:  # "ignore"
                return None

        # Evaluate each datapoint
        breaching_count = 0
        for val in metric_values:
            if val is None:
                if treat_missing == "breaching":
                    breaching_count += 1
                elif treat_missing == "notBreaching":
                    pass  # counts as not breaching
                # "missing"/"ignore": skip
            else:
                if comparison_op(val, threshold):
                    breaching_count += 1

        if breaching_count >= datapoints_to_alarm:
            return "ALARM"
        return "OK"

    @staticmethod
    def _build_reason(new_state: str, alarm, metric_values: list[float | None]) -> str:
        """Build a human-readable state reason string."""
        non_null = [v for v in metric_values if v is not None]
        if new_state == "ALARM":
            return (
                f"Threshold Crossed: {len(non_null)} datapoint(s) were "
                f"{'greater' if 'Greater' in alarm.comparison_operator else 'less'} than "
                f"the threshold ({alarm.threshold})."
            )
        elif new_state == "INSUFFICIENT_DATA":
            return f"Insufficient Data: {alarm.evaluation_periods} period(s) with no datapoints."
        else:
            return (
                f"Threshold Crossed: {len(non_null)} datapoint(s) were not "
                f"breaching the threshold ({alarm.threshold})."
            )

    def _dispatch_actions(
        self,
        alarm,
        old_state: str,
        new_state: str,
        reason: str,
        account_id: str,
        region_name: str,
    ) -> None:
        """Dispatch alarm actions (SNS, Auto Scaling, EC2, Lambda) on state transitions."""
        if new_state == "ALARM":
            action_arns = alarm.alarm_actions or []
        elif new_state == "OK":
            action_arns = alarm.ok_actions or []
        elif new_state == "INSUFFICIENT_DATA":
            action_arns = alarm.insufficient_data_actions or []
        else:
            return

        if not action_arns:
            return

        message = self._build_alarm_message(
            alarm, old_state, new_state, reason, account_id, region_name
        )
        subject = f"ALARM: {alarm.name}"

        for action_arn in action_arns:
            try:
                if ":autoscaling:" in action_arn:
                    self._execute_autoscaling_action(action_arn, account_id, region_name)
                elif ":lambda:" in action_arn:
                    self._invoke_lambda_action(action_arn, message, region_name, account_id)
                elif ":automate:" in action_arn:
                    logger.info("EC2 automate action (no-op): %s", action_arn)
                else:
                    self._publish_to_sns(action_arn, message, subject, account_id, region_name)
            except Exception:
                logger.exception(
                    "Failed to dispatch alarm action to %s for alarm %s",
                    action_arn,
                    alarm.name,
                )

    @staticmethod
    def _build_alarm_message(
        alarm,
        old_state: str,
        new_state: str,
        reason: str,
        account_id: str,
        region_name: str,
    ) -> str:
        """Build a JSON alarm notification message matching the AWS format."""
        now = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f+0000")
        message = {
            "AlarmName": alarm.name,
            "AlarmDescription": alarm.description or "",
            "AWSAccountId": account_id,
            "NewStateValue": new_state,
            "NewStateReason": reason,
            "StateChangeTime": now,
            "Region": region_name,
            "OldStateValue": old_state,
            "Trigger": {
                "MetricName": alarm.metric_name,
                "Namespace": alarm.namespace or "",
                "Statistic": alarm.statistic or "",
                "Period": alarm.period or 60,
                "EvaluationPeriods": alarm.evaluation_periods or 1,
                "ComparisonOperator": alarm.comparison_operator,
                "Threshold": alarm.threshold,
            },
        }
        return json.dumps(message)

    @staticmethod
    def _invoke_lambda_action(
        function_arn: str,
        message: str,
        region: str,
        account_id: str,
    ) -> None:
        """Invoke a Lambda function as a CloudWatch alarm action."""
        try:
            import json as _json

            from robotocore.services.lambda_.invoke import invoke_lambda_async

            payload = _json.loads(message)
            # Parse region/account from the Lambda ARN
            arn_parts = function_arn.split(":")
            target_region = arn_parts[3] if len(arn_parts) >= 7 else region
            target_account = arn_parts[4] if len(arn_parts) >= 7 else account_id
            invoke_lambda_async(function_arn, payload, target_region, target_account)
            logger.info("CloudWatch alarm -> Lambda: %s", function_arn)
        except Exception:
            logger.exception("Failed to invoke Lambda action: %s", function_arn)

    @staticmethod
    def _publish_to_sns(
        topic_arn: str,
        message: str,
        subject: str,
        account_id: str,
        region_name: str,
    ) -> None:
        """Publish a message to an SNS topic via our native SNS provider.

        Uses our provider's delivery path so that subscriptions (SQS, Lambda, etc.)
        are properly triggered, rather than going through Moto's backend directly.
        """
        # Parse region from the topic ARN (arn:aws:sns:REGION:ACCOUNT:TOPIC)
        arn_match = re.match(r"arn:aws:sns:([^:]+):([^:]+):(.+)", topic_arn)
        if not arn_match:
            logger.warning("Cannot parse SNS topic ARN: %s", topic_arn)
            return

        sns_region = arn_match.group(1)
        sns_account = arn_match.group(2)

        try:
            from robotocore.services.sns.provider import (
                _deliver_to_subscriber,
                _get_store,
                _new_id,
            )

            store = _get_store(sns_region, sns_account)
            topic = store.get_topic(topic_arn)
            if not topic:
                logger.warning("SNS topic not found: %s", topic_arn)
                return

            message_id = _new_id()
            for sub in topic.subscriptions:
                if sub.confirmed:
                    _deliver_to_subscriber(
                        sub,
                        message,
                        subject,
                        {},
                        message_id,
                        topic_arn,
                        sns_region,
                    )
        except Exception:
            logger.exception("Failed to publish to SNS topic %s", topic_arn)

    @staticmethod
    def _execute_autoscaling_action(
        action_arn: str,
        account_id: str,
        region_name: str,
    ) -> None:
        """Execute an Auto Scaling action (scaling policy or SetDesiredCapacity).

        Supports two ARN formats:
        - Scaling policy: arn:aws:autoscaling:REGION:ACCOUNT:scalingPolicy:UUID:
              autoScalingGroupName/GROUP:policyName/POLICY
        - Auto Scaling group (SetDesiredCapacity):
              arn:aws:autoscaling:REGION:ACCOUNT:autoScalingGroup:UUID:
              autoScalingGroupName/GROUP
        """
        # Parse region from the ARN
        arn_match = re.match(r"arn:aws:autoscaling:([^:]+):([^:]+):(.*)", action_arn)
        if not arn_match:
            logger.warning("Cannot parse Auto Scaling action ARN: %s", action_arn)
            return

        asg_region = arn_match.group(1)
        asg_account = arn_match.group(2)
        resource_part = arn_match.group(3)

        try:
            asg_backend = get_backend("autoscaling")[asg_account][asg_region]
        except (KeyError, TypeError):
            logger.warning("Auto Scaling backend not found for %s/%s", asg_account, asg_region)
            return

        if resource_part.startswith("scalingPolicy:"):
            # Extract policy name from the ARN
            # Format: scalingPolicy:UUID:autoScalingGroupName/GROUP:policyName/POLICY
            policy_match = re.search(r"policyName/(.+)$", resource_part)
            if not policy_match:
                logger.warning("Cannot parse scaling policy name from ARN: %s", action_arn)
                return
            policy_name = policy_match.group(1)

            # Extract ASG name
            group_match = re.search(r"autoScalingGroupName/([^:]+)", resource_part)
            group_name = group_match.group(1) if group_match else None

            try:
                asg_backend.execute_policy(group_name or "", policy_name)
                logger.info(
                    "CloudWatch alarm -> Auto Scaling ExecutePolicy: %s",
                    policy_name,
                )
            except Exception:
                logger.exception("Failed to execute Auto Scaling policy: %s", policy_name)
                raise

        elif resource_part.startswith("autoScalingGroup:"):
            # SetDesiredCapacity-style action on an ASG
            # Format: autoScalingGroup:UUID:autoScalingGroupName/GROUP
            group_match = re.search(r"autoScalingGroupName/(.+)$", resource_part)
            if not group_match:
                logger.warning("Cannot parse ASG name from ARN: %s", action_arn)
                return
            group_name = group_match.group(1)

            try:
                # Find the ASG and increment desired capacity by 1
                # (AWS behavior for simple alarm-triggered scaling)
                groups = asg_backend.describe_auto_scaling_groups([group_name])
                if groups:
                    group = groups[0]
                    new_desired = min(group.desired_capacity + 1, group.max_size)
                    asg_backend.set_desired_capacity(group_name, new_desired)
                    logger.info(
                        "CloudWatch alarm -> Auto Scaling SetDesiredCapacity: %s -> %d",
                        group_name,
                        new_desired,
                    )
                else:
                    logger.warning("Auto Scaling group not found: %s", group_name)
            except Exception:
                logger.exception("Failed to set desired capacity for ASG: %s", group_name)
                raise
        else:
            logger.warning("Unknown Auto Scaling action type in ARN: %s", action_arn)
