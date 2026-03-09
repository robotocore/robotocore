"""CDK app: CloudWatch alarm + SNS topic + Log group."""

import aws_cdk as cdk
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sns as sns


class MonitoringStack(cdk.Stack):
    def __init__(self, scope, construct_id, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        topic = sns.Topic(
            self,
            "AlarmTopic",
            topic_name="monitoring-alarm-topic",
        )

        alarm = cloudwatch.Alarm(
            self,
            "CpuAlarm",
            alarm_name="monitoring-cpu-alarm",
            metric=cloudwatch.Metric(
                namespace="AWS/EC2",
                metric_name="CPUUtilization",
                statistic="Average",
                period=cdk.Duration.seconds(300),
            ),
            evaluation_periods=2,
            threshold=80,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        alarm.add_alarm_action(cw_actions.SnsAction(topic))

        log_group = logs.LogGroup(
            self,
            "AppLogGroup",
            log_group_name="monitoring-app-logs",
            retention=logs.RetentionDays.ONE_WEEK,
        )

        cdk.CfnOutput(self, "TopicArn", value=topic.topic_arn)
        cdk.CfnOutput(self, "AlarmName", value=alarm.alarm_name)
        cdk.CfnOutput(self, "LogGroupName", value=log_group.log_group_name)


app = cdk.App()
MonitoringStack(app, "MonitoringStack")
app.synth()
