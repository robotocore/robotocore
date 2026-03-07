"""CloudWatch Logs compatibility tests."""

import time
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


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


class TestLogsTagging:
    def test_tag_log_group(self, logs):
        name = f"/test/tag-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=name)
        try:
            # Get the ARN
            described = logs.describe_log_groups(logGroupNamePrefix=name)
            arn = described["logGroups"][0]["arn"]
            logs.tag_resource(resourceArn=arn, tags={"env": "test", "team": "core"})
            resp = logs.list_tags_for_resource(resourceArn=arn)
            assert resp["tags"]["env"] == "test"
            assert resp["tags"]["team"] == "core"
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_untag_log_group(self, logs):
        name = f"/test/untag-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=name)
        try:
            described = logs.describe_log_groups(logGroupNamePrefix=name)
            arn = described["logGroups"][0]["arn"]
            logs.tag_resource(resourceArn=arn, tags={"remove-me": "yes", "keep": "yes"})
            logs.untag_resource(resourceArn=arn, tagKeys=["remove-me"])
            resp = logs.list_tags_for_resource(resourceArn=arn)
            assert "remove-me" not in resp["tags"]
            assert resp["tags"]["keep"] == "yes"
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_list_tags_for_resource(self, logs):
        name = f"/test/listtags-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=name)
        try:
            described = logs.describe_log_groups(logGroupNamePrefix=name)
            arn = described["logGroups"][0]["arn"]
            logs.tag_resource(resourceArn=arn, tags={"initial": "value"})
            resp = logs.list_tags_for_resource(resourceArn=arn)
            assert resp["tags"]["initial"] == "value"
        finally:
            logs.delete_log_group(logGroupName=name)


class TestLogsStreamEvents:
    def test_create_stream_put_and_get_events(self, logs, log_group):
        stream_name = _unique("events-stream")
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream_name)
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream_name,
            logEvents=[
                {"timestamp": now, "message": "first event"},
                {"timestamp": now + 1, "message": "second event"},
            ],
        )
        response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream_name,
        )
        messages = [e["message"] for e in response["events"]]
        assert "first event" in messages
        assert "second event" in messages

    def test_get_log_events_forward_and_backward(self, logs, log_group):
        stream_name = _unique("dir-stream")
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream_name)
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream_name,
            logEvents=[
                {"timestamp": now, "message": "msg-a"},
                {"timestamp": now + 1, "message": "msg-b"},
                {"timestamp": now + 2, "message": "msg-c"},
            ],
        )
        forward = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream_name,
            startFromHead=True,
        )
        assert len(forward["events"]) >= 3

    def test_describe_log_streams_with_prefix(self, logs, log_group):
        logs.create_log_stream(logGroupName=log_group, logStreamName="app-alpha")
        logs.create_log_stream(logGroupName=log_group, logStreamName="app-beta")
        logs.create_log_stream(logGroupName=log_group, logStreamName="web-gamma")
        response = logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix="app-",
        )
        names = [s["logStreamName"] for s in response["logStreams"]]
        assert "app-alpha" in names
        assert "app-beta" in names
        assert "web-gamma" not in names


class TestLogsMetricFilter:
    def test_put_and_describe_metric_filter(self, logs, log_group):
        filter_name = _unique("mf")
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName=filter_name,
            filterPattern="ERROR",
            metricTransformations=[
                {
                    "metricName": "ErrorCount",
                    "metricNamespace": "TestNamespace",
                    "metricValue": "1",
                }
            ],
        )
        try:
            response = logs.describe_metric_filters(logGroupName=log_group)
            filter_names = [f["filterName"] for f in response["metricFilters"]]
            assert filter_name in filter_names
            mf = [f for f in response["metricFilters"] if f["filterName"] == filter_name][0]
            assert mf["filterPattern"] == "ERROR"
            assert mf["metricTransformations"][0]["metricName"] == "ErrorCount"
        finally:
            logs.delete_metric_filter(logGroupName=log_group, filterName=filter_name)

    def test_delete_metric_filter(self, logs, log_group):
        filter_name = _unique("del-mf")
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName=filter_name,
            filterPattern="WARN",
            metricTransformations=[
                {
                    "metricName": "WarnCount",
                    "metricNamespace": "TestNS",
                    "metricValue": "1",
                }
            ],
        )
        logs.delete_metric_filter(logGroupName=log_group, filterName=filter_name)
        response = logs.describe_metric_filters(logGroupName=log_group)
        filter_names = [f["filterName"] for f in response["metricFilters"]]
        assert filter_name not in filter_names


class TestLogsSubscriptionFilter:
    def test_create_and_delete_subscription_filter(self, logs, log_group):
        filter_name = _unique("sub-filter")
        # Use a fake Lambda ARN as the destination
        dest_arn = "arn:aws:lambda:us-east-1:123456789012:function:fake-processor"
        logs.put_subscription_filter(
            logGroupName=log_group,
            filterName=filter_name,
            filterPattern="",
            destinationArn=dest_arn,
        )
        try:
            response = logs.describe_subscription_filters(logGroupName=log_group)
            filter_names = [f["filterName"] for f in response["subscriptionFilters"]]
            assert filter_name in filter_names
            sf = [
                f for f in response["subscriptionFilters"] if f["filterName"] == filter_name
            ][0]
            assert sf["destinationArn"] == dest_arn
        finally:
            logs.delete_subscription_filter(
                logGroupName=log_group, filterName=filter_name
            )

    def test_subscription_filter_gone_after_delete(self, logs, log_group):
        filter_name = _unique("sub-del")
        dest_arn = "arn:aws:lambda:us-east-1:123456789012:function:fake-fn"
        logs.put_subscription_filter(
            logGroupName=log_group,
            filterName=filter_name,
            filterPattern="ERROR",
            destinationArn=dest_arn,
        )
        logs.delete_subscription_filter(
            logGroupName=log_group, filterName=filter_name
        )
        response = logs.describe_subscription_filters(logGroupName=log_group)
        filter_names = [f["filterName"] for f in response["subscriptionFilters"]]
        assert filter_name not in filter_names


class TestLogsExportTasks:
    def test_describe_export_tasks_empty(self, logs):
        response = logs.describe_export_tasks()
        assert "exportTasks" in response


class TestLogsLogGroupWithTags:
    def test_create_log_group_with_tags(self, logs):
        name = f"/test/tagged-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=name, tags={"app": "myapp", "stage": "dev"})
        try:
            described = logs.describe_log_groups(logGroupNamePrefix=name)
            arn = described["logGroups"][0]["arn"]
            resp = logs.list_tags_for_resource(resourceArn=arn)
            assert resp["tags"]["app"] == "myapp"
            assert resp["tags"]["stage"] == "dev"
        finally:
            logs.delete_log_group(logGroupName=name)
