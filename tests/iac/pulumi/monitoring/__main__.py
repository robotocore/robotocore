"""Pulumi program: CloudWatch alarm + SNS topic + Log group."""

import pulumi
import pulumi_aws as aws

sns_topic = aws.sns.Topic(
    "monitoring-alerts",
    name="monitoring-alerts",
)

alarm = aws.cloudwatch.MetricAlarm(
    "high-cpu-alarm",
    name="high-cpu-alarm",
    metric_name="CPUUtilization",
    namespace="AWS/EC2",
    statistic="Average",
    period=300,
    evaluation_periods=2,
    threshold=80.0,
    comparison_operator="GreaterThanThreshold",
    alarm_actions=[sns_topic.arn],
)

log_group = aws.cloudwatch.LogGroup(
    "app-log-group",
    name="/app/monitoring",
    retention_in_days=7,
)

pulumi.export("topic_arn", sns_topic.arn)
pulumi.export("alarm_name", alarm.name)
pulumi.export("log_group_name", log_group.name)
