"""
EventChainOrchestrator — Wires AWS resources with real event triggers.

Manages the lifecycle of multi-service event chains:
S3 → Lambda → DynamoDB → Streams → Lambda → SNS → SQS

Architecture:
    S3 PutObject ──► Lambda A (writes DDB) ──► DDB Stream ──► Lambda B (SNS) ──► SQS
    EventBridge PutEvents ──► Rule ──► SQS/Lambda targets

Services used: S3, Lambda, DynamoDB, DynamoDB Streams, SNS, SQS, EventBridge,
               CloudWatch, IAM
"""

from __future__ import annotations

import json
from typing import Any

from .models import ChainStage


class EventChainOrchestrator:
    """Orchestrates event-driven chains across multiple AWS services.

    Creates resources, wires triggers (S3 notifications, event source mappings,
    EventBridge rules, SNS subscriptions), and provides helpers for verification.
    """

    def __init__(
        self,
        s3_client: Any,
        lambda_client: Any,
        dynamodb_client: Any,
        sqs_client: Any,
        sns_client: Any,
        events_client: Any,
        cloudwatch_client: Any,
        iam_client: Any,
        endpoint_url: str = "http://localhost:4566",
        region: str = "us-east-1",
        account_id: str = "123456789012",
    ) -> None:
        self.s3 = s3_client
        self.lam = lambda_client
        self.dynamodb = dynamodb_client
        self.sqs = sqs_client
        self.sns = sns_client
        self.events = events_client
        self.cloudwatch = cloudwatch_client
        self.iam = iam_client
        self.endpoint_url = endpoint_url
        self.region = region
        self.account_id = account_id

        # Track resources for cleanup
        self._buckets: list[str] = []
        self._functions: list[str] = []
        self._tables: list[str] = []
        self._queues: list[str] = []
        self._topics: list[str] = []
        self._subscriptions: list[str] = []
        self._event_source_mappings: list[str] = []
        self._rules: list[tuple[str, str]] = []  # (rule_name, bus_name)
        self._stages: list[ChainStage] = []

    # ─── S3 Notifications ──────────────────────────────────────────────

    def configure_s3_to_lambda(
        self,
        bucket: str,
        function_arn: str,
        events: list[str] | None = None,
        prefix: str = "",
        suffix: str = "",
    ) -> None:
        """Configure S3 bucket notification to invoke a Lambda function."""
        if events is None:
            events = ["s3:ObjectCreated:*"]

        filter_rules = []
        if prefix:
            filter_rules.append({"Name": "prefix", "Value": prefix})
        if suffix:
            filter_rules.append({"Name": "suffix", "Value": suffix})

        config: dict[str, Any] = {
            "LambdaFunctionConfigurations": [
                {
                    "LambdaFunctionArn": function_arn,
                    "Events": events,
                    **({"Filter": {"Key": {"FilterRules": filter_rules}}} if filter_rules else {}),
                }
            ]
        }
        self.s3.put_bucket_notification_configuration(
            Bucket=bucket, NotificationConfiguration=config
        )

    def configure_s3_to_sqs(
        self,
        bucket: str,
        queue_arn: str,
        events: list[str] | None = None,
        prefix: str = "",
        suffix: str = "",
    ) -> None:
        """Configure S3 bucket notification to send to an SQS queue."""
        if events is None:
            events = ["s3:ObjectCreated:*"]

        filter_rules = []
        if prefix:
            filter_rules.append({"Name": "prefix", "Value": prefix})
        if suffix:
            filter_rules.append({"Name": "suffix", "Value": suffix})

        config: dict[str, Any] = {
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": events,
                    **({"Filter": {"Key": {"FilterRules": filter_rules}}} if filter_rules else {}),
                }
            ]
        }
        self.s3.put_bucket_notification_configuration(
            Bucket=bucket, NotificationConfiguration=config
        )

    # ─── Event Source Mappings ─────────────────────────────────────────

    def create_sqs_esm(
        self,
        queue_arn: str,
        function_name: str,
        batch_size: int = 1,
    ) -> str:
        """Create an SQS → Lambda event source mapping. Returns UUID."""
        resp = self.lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=function_name,
            BatchSize=batch_size,
            Enabled=True,
        )
        uuid = resp["UUID"]
        self._event_source_mappings.append(uuid)
        return uuid

    def create_dynamodb_stream_esm(
        self,
        stream_arn: str,
        function_name: str,
        starting_position: str = "LATEST",
        batch_size: int = 1,
    ) -> str:
        """Create a DynamoDB Streams → Lambda event source mapping. Returns UUID."""
        resp = self.lam.create_event_source_mapping(
            EventSourceArn=stream_arn,
            FunctionName=function_name,
            StartingPosition=starting_position,
            BatchSize=batch_size,
            Enabled=True,
        )
        uuid = resp["UUID"]
        self._event_source_mappings.append(uuid)
        return uuid

    def create_kinesis_esm(
        self,
        stream_arn: str,
        function_name: str,
        starting_position: str = "LATEST",
        batch_size: int = 1,
    ) -> str:
        """Create a Kinesis → Lambda event source mapping. Returns UUID."""
        resp = self.lam.create_event_source_mapping(
            EventSourceArn=stream_arn,
            FunctionName=function_name,
            StartingPosition=starting_position,
            BatchSize=batch_size,
            Enabled=True,
        )
        uuid = resp["UUID"]
        self._event_source_mappings.append(uuid)
        return uuid

    # ─── EventBridge Rules ─────────────────────────────────────────────

    def create_eb_rule_to_sqs(
        self,
        rule_name: str,
        queue_arn: str,
        event_pattern: dict[str, Any],
        bus_name: str = "default",
    ) -> str:
        """Create an EventBridge rule that sends matching events to SQS."""
        resp = self.events.put_rule(
            Name=rule_name,
            EventBusName=bus_name,
            EventPattern=json.dumps(event_pattern),
            State="ENABLED",
        )
        self.events.put_targets(
            Rule=rule_name,
            EventBusName=bus_name,
            Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
        )
        self._rules.append((rule_name, bus_name))
        return resp["RuleArn"]

    def create_eb_rule_to_lambda(
        self,
        rule_name: str,
        function_arn: str,
        event_pattern: dict[str, Any],
        bus_name: str = "default",
    ) -> str:
        """Create an EventBridge rule that invokes a Lambda function."""
        resp = self.events.put_rule(
            Name=rule_name,
            EventBusName=bus_name,
            EventPattern=json.dumps(event_pattern),
            State="ENABLED",
        )
        self.events.put_targets(
            Rule=rule_name,
            EventBusName=bus_name,
            Targets=[{"Id": "lambda-target", "Arn": function_arn}],
        )
        self._rules.append((rule_name, bus_name))
        return resp["RuleArn"]

    def create_scheduled_rule_to_sqs(
        self,
        rule_name: str,
        queue_arn: str,
        schedule: str,
        bus_name: str = "default",
    ) -> str:
        """Create an EventBridge scheduled rule that sends to SQS."""
        resp = self.events.put_rule(
            Name=rule_name,
            EventBusName=bus_name,
            ScheduleExpression=schedule,
            State="ENABLED",
        )
        self.events.put_targets(
            Rule=rule_name,
            EventBusName=bus_name,
            Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
        )
        self._rules.append((rule_name, bus_name))
        return resp["RuleArn"]

    # ─── SNS → SQS ────────────────────────────────────────────────────

    def subscribe_sqs_to_sns(self, topic_arn: str, queue_arn: str) -> str:
        """Subscribe an SQS queue to an SNS topic. Returns subscription ARN."""
        resp = self.sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        sub_arn = resp["SubscriptionArn"]
        self._subscriptions.append(sub_arn)
        return sub_arn

    # ─── CloudWatch Alarms ─────────────────────────────────────────────

    def create_alarm_to_sns(
        self,
        alarm_name: str,
        namespace: str,
        metric_name: str,
        threshold: float,
        topic_arn: str,
        comparison: str = "GreaterThanThreshold",
        period: int = 60,
        evaluation_periods: int = 1,
        statistic: str = "Average",
    ) -> None:
        """Create a CloudWatch alarm that notifies an SNS topic."""
        self.cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace=namespace,
            MetricName=metric_name,
            Threshold=threshold,
            ComparisonOperator=comparison,
            Period=period,
            EvaluationPeriods=evaluation_periods,
            Statistic=statistic,
            AlarmActions=[topic_arn],
            OKActions=[topic_arn],
            TreatMissingData="notBreaching",
        )

    # ─── Resource Creation Helpers ─────────────────────────────────────

    def create_bucket(self, name: str) -> str:
        """Create an S3 bucket. Returns name."""
        self.s3.create_bucket(Bucket=name)
        self._buckets.append(name)
        return name

    def create_table(
        self,
        name: str,
        stream: bool = False,
    ) -> dict[str, str]:
        """Create a DynamoDB table with pk/sk keys.

        Returns {"table_name": name, "stream_arn": arn_or_empty}.
        """
        kwargs: dict[str, Any] = {
            "TableName": name,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        if stream:
            kwargs["StreamSpecification"] = {
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            }
        resp = self.dynamodb.create_table(**kwargs)
        self._tables.append(name)
        stream_arn = resp.get("TableDescription", {}).get("LatestStreamArn", "")
        return {"table_name": name, "stream_arn": stream_arn}

    def create_queue(self, name: str) -> tuple[str, str]:
        """Create an SQS queue. Returns (queue_url, queue_arn)."""
        resp = self.sqs.create_queue(QueueName=name)
        url = resp["QueueUrl"]
        self._queues.append(url)
        arn = self.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        return url, arn

    def create_topic(self, name: str) -> str:
        """Create an SNS topic. Returns ARN."""
        resp = self.sns.create_topic(Name=name)
        arn = resp["TopicArn"]
        self._topics.append(arn)
        return arn

    # ─── Verification Helpers ──────────────────────────────────────────

    def get_ddb_item(self, table_name: str, pk: str, sk: str) -> dict[str, Any] | None:
        """Get a DynamoDB item by pk/sk."""
        resp = self.dynamodb.get_item(
            TableName=table_name,
            Key={"pk": {"S": pk}, "sk": {"S": sk}},
        )
        return resp.get("Item")

    def scan_table(self, table_name: str) -> list[dict[str, Any]]:
        """Scan all items from a DynamoDB table."""
        resp = self.dynamodb.scan(TableName=table_name)
        return resp.get("Items", [])

    # ─── Cleanup ───────────────────────────────────────────────────────

    def cleanup(self) -> None:
        """Clean up all managed resources in dependency order."""
        # ESMs first (they reference functions and streams)
        for esm_uuid in self._event_source_mappings:
            try:
                self.lam.delete_event_source_mapping(UUID=esm_uuid)
            except Exception:
                pass

        # EventBridge rules and targets
        for rule_name, bus_name in self._rules:
            try:
                targets = self.events.list_targets_by_rule(
                    Rule=rule_name, EventBusName=bus_name
                ).get("Targets", [])
                if targets:
                    self.events.remove_targets(
                        Rule=rule_name,
                        EventBusName=bus_name,
                        Ids=[t["Id"] for t in targets],
                    )
                self.events.delete_rule(Name=rule_name, EventBusName=bus_name)
            except Exception:
                pass

        # SNS subscriptions
        for sub_arn in self._subscriptions:
            try:
                if sub_arn != "PendingConfirmation":
                    self.sns.unsubscribe(SubscriptionArn=sub_arn)
            except Exception:
                pass

        # Lambda functions
        for fn in self._functions:
            try:
                self.lam.delete_function(FunctionName=fn)
            except Exception:
                pass

        # S3 buckets (empty then delete)
        for bucket in self._buckets:
            try:
                objs = self.s3.list_objects_v2(Bucket=bucket).get("Contents", [])
                for obj in objs:
                    self.s3.delete_object(Bucket=bucket, Key=obj["Key"])
                self.s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass

        # DynamoDB tables
        for table in self._tables:
            try:
                self.dynamodb.delete_table(TableName=table)
            except Exception:
                pass

        # SNS topics
        for topic in self._topics:
            try:
                self.sns.delete_topic(TopicArn=topic)
            except Exception:
                pass

        # SQS queues
        for url in self._queues:
            try:
                self.sqs.delete_queue(QueueUrl=url)
            except Exception:
                pass
