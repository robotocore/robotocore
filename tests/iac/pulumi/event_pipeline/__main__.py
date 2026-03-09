"""Pulumi program: EventBridge rule + SQS queue target + SNS topic + subscription."""

import json

import pulumi
import pulumi_aws as aws

# SQS queue (target for EventBridge rule)
queue = aws.sqs.Queue(
    "event-queue",
    name="event-pipeline-queue",
)

# SNS topic
topic = aws.sns.Topic(
    "event-topic",
    name="event-pipeline-topic",
)

# SNS subscription: SQS queue subscribes to topic
subscription = aws.sns.TopicSubscription(
    "queue-subscription",
    topic=topic.arn,
    protocol="sqs",
    endpoint=queue.arn,
)

# EventBridge rule: match a custom event pattern
rule = aws.cloudwatch.EventRule(
    "pipeline-rule",
    name="event-pipeline-rule",
    description="Captures custom app events",
    event_pattern=json.dumps(
        {
            "source": ["my.app"],
            "detail-type": ["AppEvent"],
        }
    ),
)

# EventBridge target: send matched events to the SQS queue
target = aws.cloudwatch.EventTarget(
    "queue-target",
    rule=rule.name,
    arn=queue.arn,
)

# Exports
pulumi.export("queue_url", queue.id)
pulumi.export("queue_arn", queue.arn)
pulumi.export("topic_arn", topic.arn)
pulumi.export("rule_name", rule.name)
pulumi.export("subscription_arn", subscription.arn)
