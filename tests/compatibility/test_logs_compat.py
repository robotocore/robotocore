"""CloudWatch Logs compatibility tests."""

import time

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def logs():
    return make_client("logs")


@pytest.fixture
def log_group(logs):
    name = "/test/compat-group"
    logs.create_log_group(logGroupName=name)
    yield name
    logs.delete_log_group(logGroupName=name)


class TestLogsOperations:
    def test_create_log_group(self, logs):
        logs.create_log_group(logGroupName="/test/group")
        response = logs.describe_log_groups(logGroupNamePrefix="/test/group")
        names = [g["logGroupName"] for g in response["logGroups"]]
        assert "/test/group" in names
        logs.delete_log_group(logGroupName="/test/group")

    def test_create_log_stream(self, logs, log_group):
        logs.create_log_stream(logGroupName=log_group, logStreamName="stream-1")
        response = logs.describe_log_streams(logGroupName=log_group)
        names = [s["logStreamName"] for s in response["logStreams"]]
        assert "stream-1" in names

    def test_put_and_get_log_events(self, logs, log_group):
        logs.create_log_stream(logGroupName=log_group, logStreamName="events-stream")
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="events-stream",
            logEvents=[
                {"timestamp": int(time.time() * 1000), "message": "hello log"},
            ],
        )
        response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName="events-stream",
        )
        messages = [e["message"] for e in response["events"]]
        assert "hello log" in messages

    def test_describe_log_groups(self, logs, log_group):
        response = logs.describe_log_groups()
        assert len(response["logGroups"]) >= 1

    def test_filter_log_events(self, logs, log_group):
        logs.create_log_stream(logGroupName=log_group, logStreamName="filter-stream")
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="filter-stream",
            logEvents=[
                {"timestamp": int(time.time() * 1000), "message": "ERROR something broke"},
                {"timestamp": int(time.time() * 1000), "message": "INFO all good"},
            ],
        )
        response = logs.filter_log_events(
            logGroupName=log_group,
            filterPattern="ERROR",
        )
        messages = [e["message"] for e in response["events"]]
        assert any("ERROR" in m for m in messages)

    def test_put_retention_policy(self, logs, log_group):
        logs.put_retention_policy(logGroupName=log_group, retentionInDays=7)
        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
        assert group["retentionInDays"] == 7

    def test_delete_log_stream(self, logs, log_group):
        logs.create_log_stream(logGroupName=log_group, logStreamName="del-stream")
        logs.delete_log_stream(logGroupName=log_group, logStreamName="del-stream")
        response = logs.describe_log_streams(logGroupName=log_group)
        names = [s["logStreamName"] for s in response["logStreams"]]
        assert "del-stream" not in names

    def test_filter_log_events_multiple_streams(self, logs, log_group):
        """FilterLogEvents across multiple streams in a log group."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="multi-a")
        logs.create_log_stream(logGroupName=log_group, logStreamName="multi-b")
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="multi-a",
            logEvents=[{"timestamp": now, "message": "stream-a event"}],
        )
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="multi-b",
            logEvents=[{"timestamp": now, "message": "stream-b event"}],
        )
        response = logs.filter_log_events(
            logGroupName=log_group,
            logStreamNames=["multi-a", "multi-b"],
        )
        messages = [e["message"] for e in response["events"]]
        assert any("stream-a" in m for m in messages)
        assert any("stream-b" in m for m in messages)

    def test_describe_log_streams_prefix_filter(self, logs, log_group):
        """Describe log streams filtered by prefix."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="prefix-alpha")
        logs.create_log_stream(logGroupName=log_group, logStreamName="prefix-beta")
        logs.create_log_stream(logGroupName=log_group, logStreamName="other-gamma")
        response = logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix="prefix-",
        )
        names = [s["logStreamName"] for s in response["logStreams"]]
        assert "prefix-alpha" in names
        assert "prefix-beta" in names
        assert "other-gamma" not in names

    def test_put_retention_policy_update(self, logs, log_group):
        """Put retention policy and then update it."""
        logs.put_retention_policy(logGroupName=log_group, retentionInDays=14)
        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
        assert group["retentionInDays"] == 14
        logs.put_retention_policy(logGroupName=log_group, retentionInDays=30)
        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
        assert group["retentionInDays"] == 30

    def test_delete_retention_policy(self, logs, log_group):
        """Setting and then deleting a retention policy."""
        logs.put_retention_policy(logGroupName=log_group, retentionInDays=7)
        logs.delete_retention_policy(logGroupName=log_group)
        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
        assert "retentionInDays" not in group

    def test_tag_log_group(self, logs, log_group):
        logs.tag_log_group(logGroupName=log_group, tags={"env": "test", "app": "roboto"})
        response = logs.list_tags_log_group(logGroupName=log_group)
        assert response["tags"]["env"] == "test"
        assert response["tags"]["app"] == "roboto"

    def test_untag_log_group(self, logs, log_group):
        logs.tag_log_group(logGroupName=log_group, tags={"k1": "v1", "k2": "v2"})
        logs.untag_log_group(logGroupName=log_group, tags=["k1"])
        response = logs.list_tags_log_group(logGroupName=log_group)
        assert "k1" not in response["tags"]
        assert response["tags"]["k2"] == "v2"

    def test_put_metric_filter(self, logs, log_group):
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName="ErrorCount",
            filterPattern="ERROR",
            metricTransformations=[{
                "metricName": "ErrorCount",
                "metricNamespace": "TestApp",
                "metricValue": "1",
            }],
        )
        response = logs.describe_metric_filters(logGroupName=log_group)
        names = [f["filterName"] for f in response["metricFilters"]]
        assert "ErrorCount" in names

    def test_delete_metric_filter(self, logs, log_group):
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName="ToDelete",
            filterPattern="WARN",
            metricTransformations=[{
                "metricName": "WarnCount",
                "metricNamespace": "TestApp",
                "metricValue": "1",
            }],
        )
        logs.delete_metric_filter(logGroupName=log_group, filterName="ToDelete")
        response = logs.describe_metric_filters(logGroupName=log_group)
        names = [f["filterName"] for f in response["metricFilters"]]
        assert "ToDelete" not in names

    def test_put_subscription_filter(self, logs, log_group):
        logs.put_subscription_filter(
            logGroupName=log_group,
            filterName="SubFilter",
            filterPattern="",
            destinationArn="arn:aws:lambda:us-east-1:123456789012:function:log-processor",
        )
        response = logs.describe_subscription_filters(logGroupName=log_group)
        names = [f["filterName"] for f in response["subscriptionFilters"]]
        assert "SubFilter" in names

    def test_delete_subscription_filter(self, logs, log_group):
        logs.put_subscription_filter(
            logGroupName=log_group,
            filterName="TempSub",
            filterPattern="",
            destinationArn="arn:aws:lambda:us-east-1:123456789012:function:temp",
        )
        logs.delete_subscription_filter(logGroupName=log_group, filterName="TempSub")
        response = logs.describe_subscription_filters(logGroupName=log_group)
        names = [f["filterName"] for f in response["subscriptionFilters"]]
        assert "TempSub" not in names

    def test_put_log_events_multiple_batches(self, logs, log_group):
        """Put multiple batches of log events to the same stream."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="batch-stream")
        now = int(time.time() * 1000)
        resp1 = logs.put_log_events(
            logGroupName=log_group,
            logStreamName="batch-stream",
            logEvents=[{"timestamp": now, "message": "batch-1"}],
        )
        token = resp1.get("nextSequenceToken")
        kwargs = {}
        if token:
            kwargs["sequenceToken"] = token
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="batch-stream",
            logEvents=[{"timestamp": now + 1, "message": "batch-2"}],
            **kwargs,
        )
        response = logs.get_log_events(
            logGroupName=log_group, logStreamName="batch-stream"
        )
        messages = [e["message"] for e in response["events"]]
        assert "batch-1" in messages
        assert "batch-2" in messages
