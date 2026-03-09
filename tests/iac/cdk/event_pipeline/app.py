"""CDK app: Event pipeline with EventBridge, SQS, and SNS."""

from __future__ import annotations

import aws_cdk as cdk
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sns_subscriptions as subs
import aws_cdk.aws_sqs as sqs


class EventPipelineStack(cdk.Stack):
    """Stack creating an EventBridge rule, SQS queue, SNS topic, and SNS->SQS sub."""

    def __init__(self, scope: cdk.App, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # SQS queue
        queue = sqs.Queue(
            self,
            "EventQueue",
            queue_name=f"{construct_id}-queue",
        )

        # SNS topic
        topic = sns.Topic(
            self,
            "EventTopic",
            topic_name=f"{construct_id}-topic",
        )

        # SNS -> SQS subscription
        topic.add_subscription(subs.SqsSubscription(queue))

        # EventBridge rule with a custom event pattern targeting the SQS queue
        rule = events.Rule(
            self,
            "EventRule",
            rule_name=f"{construct_id}-rule",
            event_pattern=events.EventPattern(
                source=["robotocore.test"],
                detail_type=["TestEvent"],
            ),
        )
        rule.add_target(targets.SqsQueue(queue))

        # Outputs
        cdk.CfnOutput(self, "QueueUrl", value=queue.queue_url)
        cdk.CfnOutput(self, "QueueName", value=queue.queue_name)
        cdk.CfnOutput(self, "TopicArn", value=topic.topic_arn)
        cdk.CfnOutput(self, "RuleName", value=rule.rule_name)


app = cdk.App()
EventPipelineStack(
    app,
    "CdkEventPipelineStack",
    env=cdk.Environment(account="123456789012", region="us-east-1"),
)
app.synth()
