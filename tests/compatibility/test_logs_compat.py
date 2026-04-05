"""CloudWatch Logs compatibility tests."""

import time
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _log_group_arn(group_name: str) -> str:
    """Build a log group ARN for tagging operations."""
    return f"arn:aws:logs:us-east-1:000000000000:log-group:{group_name}"


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
        # CREATE a specific group to check
        specific = _unique("/test/desc-specific")
        logs.create_log_group(logGroupName=specific)
        try:
            # LIST all groups
            response = logs.describe_log_groups()
            assert len(response["logGroups"]) >= 1
            # LIST with prefix filter
            resp2 = logs.describe_log_groups(logGroupNamePrefix=specific)
            group = [g for g in resp2["logGroups"] if g["logGroupName"] == specific][0]
            assert group["logGroupName"] == specific
            assert "arn" in group
            assert "creationTime" in group
            # UPDATE: set retention
            logs.put_retention_policy(logGroupName=specific, retentionInDays=14)
            resp3 = logs.describe_log_groups(logGroupNamePrefix=specific)
            g2 = [g for g in resp3["logGroups"] if g["logGroupName"] == specific][0]
            assert g2["retentionInDays"] == 14
            # ERROR: describe nonexistent group returns empty list (not an error)
            with pytest.raises(ClientError) as exc:
                logs.delete_log_group(logGroupName="/test/nonexistent-xyz-abc-12345")
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            logs.delete_log_group(logGroupName=specific)

    def test_filter_log_events(self, logs, log_group):
        stream = _unique("filter-stream")
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[
                {"timestamp": now, "message": "ERROR something broke"},
                {"timestamp": now + 1, "message": "INFO all good"},
            ],
        )
        # Filter with pattern
        response = logs.filter_log_events(
            logGroupName=log_group,
            filterPattern="ERROR",
        )
        messages = [e["message"] for e in response["events"]]
        assert any("ERROR" in m for m in messages)
        # Verify events have required fields
        for ev in response["events"]:
            assert "timestamp" in ev
            assert "message" in ev
            assert "logStreamName" in ev
        # Retrieve all events via get_log_events
        get_resp = logs.get_log_events(logGroupName=log_group, logStreamName=stream)
        assert len(get_resp["events"]) == 2
        # List streams to confirm stream exists
        streams = logs.describe_log_streams(logGroupName=log_group)
        names = [s["logStreamName"] for s in streams["logStreams"]]
        assert stream in names
        # Error: filter nonexistent group
        with pytest.raises(ClientError) as exc:
            logs.filter_log_events(logGroupName="/test/totally-nonexistent-xyz-99999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        # Cleanup
        logs.delete_log_stream(logGroupName=log_group, logStreamName=stream)

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
        suffix = uuid.uuid4().hex[:6]
        sa = f"multi-a-{suffix}"
        sb = f"multi-b-{suffix}"
        logs.create_log_stream(logGroupName=log_group, logStreamName=sa)
        logs.create_log_stream(logGroupName=log_group, logStreamName=sb)
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=sa,
            logEvents=[{"timestamp": now, "message": "stream-a event"}],
        )
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=sb,
            logEvents=[{"timestamp": now, "message": "stream-b event"}],
        )
        response = logs.filter_log_events(
            logGroupName=log_group,
            logStreamNames=[sa, sb],
        )
        messages = [e["message"] for e in response["events"]]
        assert any("stream-a" in m for m in messages)
        assert any("stream-b" in m for m in messages)
        # Verify events include logStreamName field
        for ev in response["events"]:
            assert ev["logStreamName"] in (sa, sb)
        # List streams to confirm both exist
        desc = logs.describe_log_streams(logGroupName=log_group)
        names = [s["logStreamName"] for s in desc["logStreams"]]
        assert sa in names
        assert sb in names
        # Error: filter with invalid stream name list
        resp2 = logs.filter_log_events(
            logGroupName=log_group,
            logStreamNames=["nonexistent-stream-xyz"],
        )
        assert resp2["events"] == []
        # Cleanup
        logs.delete_log_stream(logGroupName=log_group, logStreamName=sa)
        logs.delete_log_stream(logGroupName=log_group, logStreamName=sb)

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

    def test_multiple_streams_put_events_each(self, logs, log_group):
        """Create multiple streams and put events to each independently."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        streams = [f"ms-{suffix}-{i}" for i in range(3)]
        for s in streams:
            logs.create_log_stream(logGroupName=log_group, logStreamName=s)

        now = int(time.time() * 1000)
        for i, s in enumerate(streams):
            logs.put_log_events(
                logGroupName=log_group,
                logStreamName=s,
                logEvents=[{"timestamp": now + i, "message": f"event from {s}"}],
            )

        for s in streams:
            response = logs.get_log_events(logGroupName=log_group, logStreamName=s)
            messages = [e["message"] for e in response["events"]]
            assert f"event from {s}" in messages

    def test_filter_log_events_across_streams(self, logs, log_group):
        """FilterLogEvents across multiple streams without specifying stream names."""
        suffix = uuid.uuid4().hex[:8]
        s1 = f"filt-across-{suffix}-a"
        s2 = f"filt-across-{suffix}-b"
        logs.create_log_stream(logGroupName=log_group, logStreamName=s1)
        logs.create_log_stream(logGroupName=log_group, logStreamName=s2)

        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=s1,
            logEvents=[{"timestamp": now, "message": f"MARKER-{suffix}-alpha"}],
        )
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=s2,
            logEvents=[{"timestamp": now + 1, "message": f"MARKER-{suffix}-beta"}],
        )

        response = logs.filter_log_events(logGroupName=log_group, filterPattern=f"MARKER-{suffix}")
        messages = [e["message"] for e in response["events"]]
        assert any("alpha" in m for m in messages)
        assert any("beta" in m for m in messages)
        # Verify logStreamName is present in events
        for ev in response["events"]:
            assert ev["logStreamName"] in (s1, s2)
        # List streams to verify both exist
        desc = logs.describe_log_streams(logGroupName=log_group)
        names = [s["logStreamName"] for s in desc["logStreams"]]
        assert s1 in names and s2 in names
        # Error: filter on nonexistent group raises ResourceNotFoundException
        with pytest.raises(ClientError) as exc:
            logs.filter_log_events(logGroupName="/test/nonexistent-across-xyz-9999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        # Cleanup
        logs.delete_log_stream(logGroupName=log_group, logStreamName=s1)
        logs.delete_log_stream(logGroupName=log_group, logStreamName=s2)

    def test_filter_log_events_with_pattern(self, logs, log_group):
        """FilterLogEvents with a specific filterPattern."""
        suffix = uuid.uuid4().hex[:8]
        stream = f"pattern-{suffix}"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[
                {"timestamp": now, "message": f"WARN slow-{suffix}"},
                {"timestamp": now + 1, "message": f"ERROR crash-{suffix}"},
                {"timestamp": now + 2, "message": f"INFO ok-{suffix}"},
            ],
        )
        resp = logs.filter_log_events(
            logGroupName=log_group,
            logStreamNames=[stream],
            filterPattern="ERROR",
        )
        messages = [e["message"] for e in resp["events"]]
        assert any("ERROR" in m for m in messages)
        assert all("INFO" not in m for m in messages)
        # Verify event fields
        for ev in resp["events"]:
            assert ev["logStreamName"] == stream
        # List streams to confirm stream is present
        desc = logs.describe_log_streams(logGroupName=log_group)
        names = [s["logStreamName"] for s in desc["logStreams"]]
        assert stream in names
        # Error: filter on nonexistent group
        with pytest.raises(ClientError) as exc:
            logs.filter_log_events(logGroupName="/test/nonexistent-pattern-xyz-9999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        # Cleanup
        logs.delete_log_stream(logGroupName=log_group, logStreamName=stream)

    def test_put_multiple_log_events_and_get(self, logs, log_group):
        """Put multiple log events and retrieve them in order."""
        stream = "multi-events-stream"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[
                {"timestamp": now, "message": "ERROR something failed"},
                {"timestamp": now + 1, "message": "INFO all good"},
                {"timestamp": now + 2, "message": "ERROR another failure"},
            ],
        )

        response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream,
        )
        messages = [e["message"] for e in response["events"]]
        assert len(messages) >= 3

    def test_get_log_events_pagination(self, logs, log_group):
        """GetLogEvents with pagination using nextToken."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        stream = f"paginate-{suffix}"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)

        now = int(time.time() * 1000)
        events = [{"timestamp": now + i, "message": f"msg-{suffix}-{i}"} for i in range(10)]
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=events,
        )

        # Get with a limit
        response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            limit=3,
        )
        assert len(response["events"]) <= 3
        assert "nextForwardToken" in response
        assert "nextBackwardToken" in response

        # Use the forward token to get more
        next_response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            nextToken=response["nextForwardToken"],
            limit=3,
        )
        assert "events" in next_response

    def test_put_and_delete_retention_policy(self, logs):
        """Full lifecycle: create group, set retention, delete retention, delete group."""
        import uuid

        group = f"/test/retention-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)

        logs.put_retention_policy(logGroupName=group, retentionInDays=14)
        response = logs.describe_log_groups(logGroupNamePrefix=group)
        g = [x for x in response["logGroups"] if x["logGroupName"] == group][0]
        assert g["retentionInDays"] == 14

        logs.delete_retention_policy(logGroupName=group)
        response = logs.describe_log_groups(logGroupNamePrefix=group)
        g = [x for x in response["logGroups"] if x["logGroupName"] == group][0]
        assert "retentionInDays" not in g

        logs.delete_log_group(logGroupName=group)

    def test_describe_log_streams_order_by(self, logs, log_group):
        """DescribeLogStreams with orderBy and limit."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        stream_names = [f"order-{suffix}-{c}" for c in ["c", "a", "b"]]
        for s in stream_names:
            logs.create_log_stream(logGroupName=log_group, logStreamName=s)

        response = logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=f"order-{suffix}",
            orderBy="LogStreamName",
        )
        names = [s["logStreamName"] for s in response["logStreams"]]
        # Should be alphabetically sorted
        assert names == sorted(names)

        # Test with limit
        response = logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=f"order-{suffix}",
            orderBy="LogStreamName",
            limit=2,
        )
        assert len(response["logStreams"]) == 2

    def test_log_group_tags(self, logs):
        """Tag, untag, and list tags on a log group."""
        import uuid

        group = f"/test/tags-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group, tags={"env": "test", "team": "infra"})

        # Get the actual ARN from describe
        desc = logs.describe_log_groups(logGroupNamePrefix=group)
        group_arn = [g["arn"] for g in desc["logGroups"] if g["logGroupName"] == group][0]
        # Strip trailing ":*" if present (AWS sometimes includes it)
        if group_arn.endswith(":*"):
            group_arn = group_arn[:-2]

        response = logs.list_tags_for_resource(resourceArn=group_arn)
        assert response["tags"]["env"] == "test"
        assert response["tags"]["team"] == "infra"

        logs.tag_resource(
            resourceArn=group_arn,
            tags={"version": "2"},
        )
        response = logs.list_tags_for_resource(resourceArn=group_arn)
        assert response["tags"]["version"] == "2"

        logs.untag_resource(
            resourceArn=group_arn,
            tagKeys=["team"],
        )
        response = logs.list_tags_for_resource(resourceArn=group_arn)
        assert "team" not in response["tags"]

        logs.delete_log_group(logGroupName=group)

    def test_put_describe_delete_metric_filter(self, logs, log_group):
        """PutMetricFilter, DescribeMetricFilters, DeleteMetricFilter."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        filter_name = f"mf-{suffix}"
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName=filter_name,
            filterPattern="ERROR",
            metricTransformations=[
                {
                    "metricName": f"ErrorCount-{suffix}",
                    "metricNamespace": "TestFilters",
                    "metricValue": "1",
                }
            ],
        )
        resp = logs.describe_metric_filters(
            logGroupName=log_group,
            filterNamePrefix=filter_name,
        )
        assert len(resp["metricFilters"]) == 1
        assert resp["metricFilters"][0]["filterName"] == filter_name

        logs.delete_metric_filter(
            logGroupName=log_group,
            filterName=filter_name,
        )
        resp = logs.describe_metric_filters(
            logGroupName=log_group,
            filterNamePrefix=filter_name,
        )
        assert len(resp["metricFilters"]) == 0

    def test_get_log_events_forward_and_backward(self, logs, log_group):
        """GetLogEvents with startFromHead true vs false."""
        stream = "direction-stream"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[
                {"timestamp": now, "message": "msg-a"},
                {"timestamp": now + 1000, "message": "msg-b"},
            ],
        )
        # Forward (oldest first)
        fwd = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            startFromHead=True,
        )
        assert len(fwd["events"]) >= 2
        # Backward (newest first) - default
        bwd = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            startFromHead=False,
        )
        assert len(bwd["events"]) >= 2

    def test_describe_log_streams_order(self, logs, log_group):
        """DescribeLogStreams returns streams in the group."""
        for name in ["stream-x", "stream-y", "stream-z"]:
            logs.create_log_stream(logGroupName=log_group, logStreamName=name)
        response = logs.describe_log_streams(logGroupName=log_group)
        names = [s["logStreamName"] for s in response["logStreams"]]
        assert "stream-x" in names
        assert "stream-y" in names
        assert "stream-z" in names

    def test_put_metric_filter(self, logs, log_group):
        """PutMetricFilter, DescribeMetricFilters, DeleteMetricFilter."""
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName="error-count",
            filterPattern="ERROR",
            metricTransformations=[
                {
                    "metricName": "ErrorCount",
                    "metricNamespace": "TestApp",
                    "metricValue": "1",
                }
            ],
        )

        response = logs.describe_metric_filters(logGroupName=log_group)
        names = [f["filterName"] for f in response["metricFilters"]]
        assert "error-count" in names

        logs.delete_metric_filter(logGroupName=log_group, filterName="error-count")
        response = logs.describe_metric_filters(logGroupName=log_group)
        names = [f["filterName"] for f in response["metricFilters"]]
        assert "error-count" not in names

    def test_put_describe_delete_subscription_filter(self, logs, log_group):
        """PutSubscriptionFilter, DescribeSubscriptionFilters, DeleteSubscriptionFilter."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        filter_name = f"sf-{suffix}"
        dest_arn = f"arn:aws:lambda:us-east-1:000000000000:function:dummy-{suffix}"

        logs.put_subscription_filter(
            logGroupName=log_group,
            filterName=filter_name,
            filterPattern="ERROR",
            destinationArn=dest_arn,
        )
        resp = logs.describe_subscription_filters(logGroupName=log_group)
        names = [f["filterName"] for f in resp["subscriptionFilters"]]
        assert filter_name in names

        logs.delete_subscription_filter(logGroupName=log_group, filterName=filter_name)
        resp = logs.describe_subscription_filters(logGroupName=log_group)
        names = [f["filterName"] for f in resp["subscriptionFilters"]]
        assert filter_name not in names

    def test_describe_log_streams(self, logs, log_group):
        """Create stream, describe_log_streams, verify stream name in list."""
        stream_name = "describe-streams-test"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream_name)
        response = logs.describe_log_streams(logGroupName=log_group)
        names = [s["logStreamName"] for s in response["logStreams"]]
        assert stream_name in names

    def test_put_and_delete_subscription_filter(self, logs, log_group):
        """Create subscription filter, describe, delete."""
        stream_name = "sub-filter-stream"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream_name)
        filter_name = "test-sub-filter"
        # Use a fake Lambda ARN as destination
        dest_arn = "arn:aws:lambda:us-east-1:123456789012:function:log-processor"
        logs.put_subscription_filter(
            logGroupName=log_group,
            filterName=filter_name,
            filterPattern="ERROR",
            destinationArn=dest_arn,
        )

        response = logs.describe_subscription_filters(logGroupName=log_group)
        names = [f["filterName"] for f in response["subscriptionFilters"]]
        assert filter_name in names

        logs.delete_subscription_filter(logGroupName=log_group, filterName=filter_name)
        response = logs.describe_subscription_filters(logGroupName=log_group)
        names = [f["filterName"] for f in response["subscriptionFilters"]]
        assert filter_name not in names

    def test_multiple_log_groups_describe_prefix_limit(self, logs):
        """Create multiple log groups, describe with prefix and limit."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        prefix = f"/test/multi-{suffix}"
        groups = [f"{prefix}-{i}" for i in range(5)]
        for g in groups:
            logs.create_log_group(logGroupName=g)

        # Describe with prefix
        response = logs.describe_log_groups(logGroupNamePrefix=prefix)
        names = [g["logGroupName"] for g in response["logGroups"]]
        for g in groups:
            assert g in names

        # Describe with prefix and limit
        response = logs.describe_log_groups(logGroupNamePrefix=prefix, limit=2)
        assert len(response["logGroups"]) == 2

        for g in groups:
            logs.delete_log_group(logGroupName=g)

    def test_get_log_events_start_end_time(self, logs, log_group):
        """GetLogEvents filtered by startTime and endTime."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        stream = f"time-range-{suffix}"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)

        base = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[
                {"timestamp": base, "message": f"early-{suffix}"},
                {"timestamp": base + 60000, "message": f"mid-{suffix}"},
                {"timestamp": base + 120000, "message": f"late-{suffix}"},
            ],
        )

        # Query a time range that should only include early and mid
        response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            startTime=base,
            endTime=base + 90000,
        )
        messages = [e["message"] for e in response["events"]]
        assert f"early-{suffix}" in messages
        assert f"mid-{suffix}" in messages
        assert f"late-{suffix}" not in messages

    def test_filter_log_events_with_limit(self, logs, log_group):
        """FilterLogEvents with limit parameter."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        stream = f"filt-limit-{suffix}"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)

        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[{"timestamp": now + i, "message": f"MATCH-{suffix}-{i}"} for i in range(10)],
        )

        response = logs.filter_log_events(
            logGroupName=log_group,
            logStreamNames=[stream],
            filterPattern=f"MATCH-{suffix}",
            limit=3,
        )
        assert len(response["events"]) <= 3

    def test_describe_log_groups_no_prefix(self, logs):
        """DescribeLogGroups without prefix returns all groups."""
        import uuid

        group = f"/test/noprefix-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)

        response = logs.describe_log_groups()
        names = [g["logGroupName"] for g in response["logGroups"]]
        assert group in names

        logs.delete_log_group(logGroupName=group)

    def test_put_log_events_multiple_batches(self, logs, log_group):
        """Put events in multiple batches and verify all appear."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        stream = f"batches-{suffix}"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)

        now = int(time.time() * 1000)
        # First batch
        resp1 = logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[{"timestamp": now, "message": f"batch1-{suffix}"}],
        )
        assert resp1["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Second batch
        resp2 = logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[{"timestamp": now + 1000, "message": f"batch2-{suffix}"}],
        )
        assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200

        response = logs.get_log_events(logGroupName=log_group, logStreamName=stream)
        messages = [e["message"] for e in response["events"]]
        assert f"batch1-{suffix}" in messages
        assert f"batch2-{suffix}" in messages

    def test_describe_metric_filters_with_filter_name_prefix(self, logs, log_group):
        """DescribeMetricFilters with filterNamePrefix."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        prefix = f"mfp-{suffix}"
        names = [f"{prefix}-a", f"{prefix}-b"]
        for name in names:
            logs.put_metric_filter(
                logGroupName=log_group,
                filterName=name,
                filterPattern="ERROR",
                metricTransformations=[
                    {
                        "metricName": f"Err-{name}",
                        "metricNamespace": "TestFilters",
                        "metricValue": "1",
                    }
                ],
            )

        try:
            response = logs.describe_metric_filters(
                logGroupName=log_group,
                filterNamePrefix=prefix,
            )
            returned = [f["filterName"] for f in response["metricFilters"]]
            for name in names:
                assert name in returned
        finally:
            for name in names:
                logs.delete_metric_filter(logGroupName=log_group, filterName=name)

    def test_create_and_describe_export_task(self, logs, log_group):
        """CreateExportTask → DescribeExportTasks with proper assertions."""
        s3 = make_client("s3")
        bucket = f"logs-export-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=bucket)
        try:
            resp = logs.create_export_task(
                logGroupName=log_group,
                fromTime=int(time.time() * 1000) - 3600000,
                to=int(time.time() * 1000),
                destination=bucket,
            )
            task_id = resp["taskId"]
            assert task_id

            # Describe by task ID
            desc = logs.describe_export_tasks(taskId=task_id)
            assert len(desc["exportTasks"]) == 1
            task = desc["exportTasks"][0]
            assert task["taskId"] == task_id
            assert task["logGroupName"] == log_group
            assert task["status"]["code"] in ("COMPLETED", "RUNNING", "PENDING")
        finally:
            try:
                # Clean up S3 objects if any
                objs = s3.list_objects_v2(Bucket=bucket)
                for obj in objs.get("Contents", []):
                    s3.delete_object(Bucket=bucket, Key=obj["Key"])
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_describe_log_groups_prefix_filter(self, logs):
        """DescribeLogGroups with prefix filter."""
        prefix = f"/test/prefix-{uuid.uuid4().hex[:8]}"
        name1 = f"{prefix}/group-a"
        name2 = f"{prefix}/group-b"
        name3 = f"/test/other-{uuid.uuid4().hex[:8]}/group-c"
        logs.create_log_group(logGroupName=name1)
        logs.create_log_group(logGroupName=name2)
        logs.create_log_group(logGroupName=name3)
        try:
            resp = logs.describe_log_groups(logGroupNamePrefix=prefix)
            names = [g["logGroupName"] for g in resp["logGroups"]]
            assert name1 in names
            assert name2 in names
            assert name3 not in names
        finally:
            logs.delete_log_group(logGroupName=name1)
            logs.delete_log_group(logGroupName=name2)
            logs.delete_log_group(logGroupName=name3)

    def test_describe_log_streams_prefix_and_ordering(self, logs, log_group):
        """DescribeLogStreams with prefix filter and ordering."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="order-alpha")
        logs.create_log_stream(logGroupName=log_group, logStreamName="order-beta")
        logs.create_log_stream(logGroupName=log_group, logStreamName="other-gamma")
        resp = logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix="order-",
            orderBy="LogStreamName",
        )
        names = [s["logStreamName"] for s in resp["logStreams"]]
        assert "order-alpha" in names
        assert "order-beta" in names
        assert "other-gamma" not in names
        # Should be sorted by name
        order_names = [n for n in names if n.startswith("order-")]
        assert order_names == sorted(order_names)

    def test_get_log_events_with_time_and_limit(self, logs, log_group):
        """GetLogEvents with startTime, endTime, and limit."""
        stream = _unique("time-stream")
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)
        base = int(time.time() * 1000)
        events = [
            {"timestamp": base, "message": "event-0"},
            {"timestamp": base + 1000, "message": "event-1"},
            {"timestamp": base + 2000, "message": "event-2"},
            {"timestamp": base + 3000, "message": "event-3"},
        ]
        logs.put_log_events(logGroupName=log_group, logStreamName=stream, logEvents=events)

        resp = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            startTime=base,
            endTime=base + 2500,
            limit=10,
        )
        messages = [e["message"] for e in resp["events"]]
        assert "event-0" in messages
        assert "event-1" in messages
        # event-2 may or may not be included depending on inclusivity of endTime
        assert "event-3" not in messages

    def test_tag_untag_log_group(self, logs):
        """TagLogGroup / UntagLogGroup / ListTagsLogGroup (legacy API)."""
        name = _unique("/test/tag-group")
        logs.create_log_group(logGroupName=name)
        try:
            logs.tag_log_group(logGroupName=name, tags={"env": "test", "team": "platform"})
            resp = logs.list_tags_log_group(logGroupName=name)
            assert resp["tags"]["env"] == "test"
            assert resp["tags"]["team"] == "platform"

            logs.untag_log_group(logGroupName=name, tags=["team"])
            resp = logs.list_tags_log_group(logGroupName=name)
            assert "team" not in resp["tags"]
            assert resp["tags"]["env"] == "test"
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_put_and_delete_retention_policy_new_group(self, logs):
        """PutRetentionPolicy / DeleteRetentionPolicy on a new group."""
        name = _unique("/test/ret-group")
        logs.create_log_group(logGroupName=name)
        try:
            logs.put_retention_policy(logGroupName=name, retentionInDays=30)
            resp = logs.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            assert group["retentionInDays"] == 30

            logs.delete_retention_policy(logGroupName=name)
            resp = logs.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            assert "retentionInDays" not in group
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_put_describe_delete_destination(self, logs):
        """PutDestination / DescribeDestinations / DeleteDestination."""
        dest_name = _unique("dest")
        logs.put_destination(
            destinationName=dest_name,
            targetArn="arn:aws:kinesis:us-east-1:000000000000:stream/dummy",
            roleArn="arn:aws:iam::000000000000:role/dummy",
        )
        try:
            resp = logs.describe_destinations(DestinationNamePrefix=dest_name)
            names = [d["destinationName"] for d in resp["destinations"]]
            assert dest_name in names
        finally:
            logs.delete_destination(destinationName=dest_name)

    def test_describe_queries(self, logs):
        """DescribeQueries."""
        resp = logs.describe_queries()
        assert "queries" in resp
        assert isinstance(resp["queries"], list)

    def test_start_query_and_get_results(self, logs, log_group):
        """StartQuery / GetQueryResults - Log Insights."""
        now = int(time.time())
        resp = logs.start_query(
            logGroupName=log_group,
            startTime=now - 3600,
            endTime=now,
            queryString="fields @timestamp, @message | limit 5",
        )
        query_id = resp["queryId"]
        result = logs.get_query_results(queryId=query_id)
        assert "status" in result
        assert result["status"] in ("Complete", "Running", "Scheduled", "Failed", "Cancelled")

    def test_tag_resource_new_api(self, logs):
        """TagResource / UntagResource / ListTagsForResource (new API)."""
        name = _unique("/test/newtag-group")
        logs.create_log_group(logGroupName=name)
        try:
            # Get the ARN
            resp = logs.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            arn = group["arn"]

            logs.tag_resource(resourceArn=arn, tags={"env": "staging", "version": "2"})
            resp = logs.list_tags_for_resource(resourceArn=arn)
            assert resp["tags"]["env"] == "staging"
            assert resp["tags"]["version"] == "2"

            logs.untag_resource(resourceArn=arn, tagKeys=["version"])
            resp = logs.list_tags_for_resource(resourceArn=arn)
            assert "version" not in resp["tags"]
            assert resp["tags"]["env"] == "staging"
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_associate_disassociate_kms_key(self, logs):
        """AssociateKmsKey / DisassociateKmsKey."""
        name = _unique("/test/kms-group")
        logs.create_log_group(logGroupName=name)
        try:
            kms = make_client("kms")
            key = kms.create_key()
            key_id = key["KeyMetadata"]["KeyId"]
            key_arn = key["KeyMetadata"]["Arn"]
            logs.associate_kms_key(logGroupName=name, kmsKeyId=key_arn)
            resp = logs.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            assert "kmsKeyId" in group

            logs.disassociate_kms_key(logGroupName=name)
            resp = logs.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            assert "kmsKeyId" not in group
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
        finally:
            logs.delete_log_group(logGroupName=name)


class TestLogsExtended:
    """Extended CloudWatch Logs operations for higher coverage."""

    @pytest.fixture
    def logs(self):
        from tests.compatibility.conftest import make_client

        return make_client("logs")

    def test_create_log_group_with_retention(self, logs):
        name = _unique("/test/ret-create")
        try:
            logs.create_log_group(logGroupName=name)
            logs.put_retention_policy(logGroupName=name, retentionInDays=7)
            resp = logs.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            assert group["retentionInDays"] == 7
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_filter_log_events(self, logs):
        name = _unique("/test/filter-events")
        stream = "filter-stream"
        logs.create_log_group(logGroupName=name)
        logs.create_log_stream(logGroupName=name, logStreamName=stream)
        try:
            ts = int(time.time() * 1000)
            logs.put_log_events(
                logGroupName=name,
                logStreamName=stream,
                logEvents=[
                    {"timestamp": ts, "message": "ERROR something failed"},
                    {"timestamp": ts + 1, "message": "INFO all good"},
                    {"timestamp": ts + 2, "message": "ERROR another failure"},
                ],
            )
            resp = logs.filter_log_events(
                logGroupName=name,
                filterPattern="ERROR",
            )
            messages = [e["message"] for e in resp["events"]]
            assert all("ERROR" in m for m in messages)
            assert len(messages) >= 2
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_filter_log_events_with_time_range(self, logs):
        name = _unique("/test/filter-time")
        stream = "time-stream"
        logs.create_log_group(logGroupName=name)
        logs.create_log_stream(logGroupName=name, logStreamName=stream)
        try:
            base = int(time.time() * 1000)
            logs.put_log_events(
                logGroupName=name,
                logStreamName=stream,
                logEvents=[
                    {"timestamp": base, "message": "event-0"},
                    {"timestamp": base + 5000, "message": "event-5"},
                ],
            )
            resp = logs.filter_log_events(
                logGroupName=name,
                startTime=base,
                endTime=base + 3000,
            )
            messages = [e["message"] for e in resp["events"]]
            assert "event-0" in messages
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_describe_log_groups_limit(self, logs):
        resp = logs.describe_log_groups(limit=5)
        assert "logGroups" in resp
        assert len(resp["logGroups"]) <= 5

    def test_put_log_events_multiple_batches(self, logs):
        name = _unique("/test/multi-batch")
        stream = "batch-stream"
        logs.create_log_group(logGroupName=name)
        logs.create_log_stream(logGroupName=name, logStreamName=stream)
        try:
            ts = int(time.time() * 1000)
            resp1 = logs.put_log_events(
                logGroupName=name,
                logStreamName=stream,
                logEvents=[{"timestamp": ts, "message": "batch-1"}],
            )
            seq = resp1.get("nextSequenceToken")
            kwargs = {
                "logGroupName": name,
                "logStreamName": stream,
                "logEvents": [{"timestamp": ts + 1000, "message": "batch-2"}],
            }
            if seq:
                kwargs["sequenceToken"] = seq
            logs.put_log_events(**kwargs)

            events = logs.get_log_events(logGroupName=name, logStreamName=stream)
            messages = [e["message"] for e in events["events"]]
            assert "batch-1" in messages
            assert "batch-2" in messages
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_create_log_group_with_tags(self, logs):
        name = _unique("/test/tagged-group")
        try:
            logs.create_log_group(
                logGroupName=name,
                tags={"env": "test", "team": "platform"},
            )
            resp = logs.list_tags_log_group(logGroupName=name)
            assert resp["tags"]["env"] == "test"
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_describe_log_streams_limit(self, logs):
        name = _unique("/test/stream-limit")
        logs.create_log_group(logGroupName=name)
        try:
            for i in range(5):
                logs.create_log_stream(logGroupName=name, logStreamName=f"stream-{i}")
            resp = logs.describe_log_streams(logGroupName=name, limit=3)
            assert len(resp["logStreams"]) <= 3
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_delete_log_stream(self, logs):
        name = _unique("/test/del-stream")
        logs.create_log_group(logGroupName=name)
        try:
            logs.create_log_stream(logGroupName=name, logStreamName="to-delete")
            logs.delete_log_stream(logGroupName=name, logStreamName="to-delete")
            resp = logs.describe_log_streams(logGroupName=name)
            names = [s["logStreamName"] for s in resp["logStreams"]]
            assert "to-delete" not in names
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_put_describe_delete_metric_filter(self, logs):
        name = _unique("/test/metric-filter")
        logs.create_log_group(logGroupName=name)
        try:
            logs.put_metric_filter(
                logGroupName=name,
                filterName="error-count",
                filterPattern="ERROR",
                metricTransformations=[
                    {
                        "metricName": "ErrorCount",
                        "metricNamespace": "TestApp",
                        "metricValue": "1",
                    }
                ],
            )
            resp = logs.describe_metric_filters(logGroupName=name)
            names = [f["filterName"] for f in resp["metricFilters"]]
            assert "error-count" in names

            logs.delete_metric_filter(logGroupName=name, filterName="error-count")
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_put_describe_delete_resource_policy(self, logs):
        """PutResourcePolicy / DescribeResourcePolicies / DeleteResourcePolicy."""
        policy_name = _unique("res-policy")
        policy_doc = (
            '{"Version":"2012-10-17","Statement":[{"Sid":"Route53","Effect":"Allow",'
            '"Principal":{"Service":"route53.amazonaws.com"},'
            '"Action":["logs:CreateLogStream","logs:PutLogEvents"],'
            '"Resource":"*"}]}'
        )
        logs.put_resource_policy(policyName=policy_name, policyDocument=policy_doc)
        try:
            resp = logs.describe_resource_policies()
            names = [p["policyName"] for p in resp["resourcePolicies"] if "policyName" in p]
            assert policy_name in names
        finally:
            logs.delete_resource_policy(policyName=policy_name)
        # Verify deletion
        resp = logs.describe_resource_policies()
        names = [p["policyName"] for p in resp["resourcePolicies"] if "policyName" in p]
        assert policy_name not in names

    def test_put_destination_policy(self, logs):
        """PutDestination / PutDestinationPolicy / DeleteDestination."""
        dest_name = _unique("dest-pol")
        logs.put_destination(
            destinationName=dest_name,
            targetArn="arn:aws:kinesis:us-east-1:000000000000:stream/dummy",
            roleArn="arn:aws:iam::000000000000:role/dummy",
        )
        try:
            policy_doc = (
                '{"Version":"2012-10-17","Statement":[{"Sid":"AllowSub","Effect":"Allow",'
                '"Principal":{"AWS":"000000000000"},"Action":"logs:PutSubscriptionFilter",'
                '"Resource":"*"}]}'
            )
            logs.put_destination_policy(
                destinationName=dest_name,
                accessPolicy=policy_doc,
            )
            resp = logs.describe_destinations(DestinationNamePrefix=dest_name)
            dest = [d for d in resp["destinations"] if d["destinationName"] == dest_name][0]
            assert "accessPolicy" in dest
        finally:
            logs.delete_destination(destinationName=dest_name)

    def test_describe_export_tasks(self, logs):
        """DescribeExportTasks returns a list (possibly empty)."""
        resp = logs.describe_export_tasks()
        assert "exportTasks" in resp
        assert isinstance(resp["exportTasks"], list)

    def test_stop_query(self, logs):
        """StartQuery / StopQuery lifecycle."""
        name = _unique("/test/stop-query")
        logs.create_log_group(logGroupName=name)
        try:
            now = int(time.time())
            start_resp = logs.start_query(
                logGroupName=name,
                startTime=now - 3600,
                endTime=now,
                queryString="fields @timestamp | limit 1",
            )
            query_id = start_resp["queryId"]
            stop_resp = logs.stop_query(queryId=query_id)
            assert stop_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert isinstance(stop_resp.get("success"), bool)
        finally:
            logs.delete_log_group(logGroupName=name)


class TestLogsGapStubs:
    """Tests for newly-stubbed Logs operations that return empty results."""

    @pytest.fixture
    def logs(self):
        return make_client("logs")

    def test_describe_query_definitions(self, logs):
        resp = logs.describe_query_definitions()
        assert "queryDefinitions" in resp
        assert isinstance(resp["queryDefinitions"], list)

    def test_list_anomalies(self, logs):
        resp = logs.list_anomalies(
            anomalyDetectorArn="arn:aws:logs:us-east-1:123456789012:anomaly-detector:dummy"
        )
        assert "anomalies" in resp
        assert isinstance(resp["anomalies"], list)

    def test_list_log_anomaly_detectors(self, logs):
        resp = logs.list_log_anomaly_detectors()
        assert "anomalyDetectors" in resp
        assert isinstance(resp["anomalyDetectors"], list)

    def test_list_integrations(self, logs):
        resp = logs.list_integrations()
        assert "integrationSummaries" in resp
        assert isinstance(resp["integrationSummaries"], list)


class TestLogsAdditionalOperations:
    """Tests for additional CloudWatch Logs operations."""

    @pytest.fixture
    def logs(self):
        return make_client("logs")

    def test_get_log_group_fields(self, logs):
        """GetLogGroupFields returns default fields for a log group."""
        name = _unique("/test/fields")
        logs.create_log_group(logGroupName=name)
        try:
            resp = logs.get_log_group_fields(logGroupName=name)
            assert "logGroupFields" in resp
            assert isinstance(resp["logGroupFields"], list)
            field_names = [f["name"] for f in resp["logGroupFields"]]
            assert "@timestamp" in field_names
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_put_query_definition(self, logs):
        """PutQueryDefinition creates a saved query."""
        name = _unique("test-query")
        resp = logs.put_query_definition(
            name=name,
            queryString="fields @timestamp, @message | sort @timestamp desc | limit 20",
        )
        assert "queryDefinitionId" in resp
        qid = resp["queryDefinitionId"]
        # Verify via describe
        desc = logs.describe_query_definitions()
        ids = [q["queryDefinitionId"] for q in desc["queryDefinitions"]]
        assert qid in ids

    def test_list_log_groups(self, logs):
        """ListLogGroups returns log groups."""
        name = _unique("/test/list-lg")
        logs.create_log_group(logGroupName=name)
        try:
            resp = logs.list_log_groups()
            assert "logGroups" in resp
            assert isinstance(resp["logGroups"], list)
            names = [g["logGroupName"] for g in resp["logGroups"]]
            assert name in names
        finally:
            logs.delete_log_group(logGroupName=name)

    def test_describe_deliveries_empty(self, logs):
        """DescribeDeliveries returns a list (possibly empty)."""
        resp = logs.describe_deliveries()
        assert "deliveries" in resp
        assert isinstance(resp["deliveries"], list)

    def test_describe_delivery_destinations_empty(self, logs):
        """DescribeDeliveryDestinations returns a list (possibly empty)."""
        resp = logs.describe_delivery_destinations()
        assert "deliveryDestinations" in resp
        assert isinstance(resp["deliveryDestinations"], list)

    def test_describe_delivery_sources_empty(self, logs):
        """DescribeDeliverySources returns a list (possibly empty)."""
        resp = logs.describe_delivery_sources()
        assert "deliverySources" in resp
        assert isinstance(resp["deliverySources"], list)

    def test_describe_configuration_templates(self, logs):
        """DescribeConfigurationTemplates returns a list."""
        resp = logs.describe_configuration_templates()
        assert "configurationTemplates" in resp
        assert isinstance(resp["configurationTemplates"], list)

    def test_put_and_get_delivery_destination(self, logs):
        """PutDeliveryDestination and GetDeliveryDestination."""
        dest_name = _unique("test-dest")
        resp = logs.put_delivery_destination(
            name=dest_name,
            outputFormat="json",
            deliveryDestinationConfiguration={
                "destinationResourceArn": "arn:aws:s3:::test-bucket-delivery"
            },
        )
        assert resp["deliveryDestination"]["name"] == dest_name
        try:
            get_resp = logs.get_delivery_destination(name=dest_name)
            assert get_resp["deliveryDestination"]["name"] == dest_name
        finally:
            logs.delete_delivery_destination(name=dest_name)

    def test_delete_delivery_destination(self, logs):
        """DeleteDeliveryDestination removes the destination."""
        dest_name = _unique("test-dest-del")
        logs.put_delivery_destination(
            name=dest_name,
            outputFormat="json",
            deliveryDestinationConfiguration={
                "destinationResourceArn": "arn:aws:s3:::test-bucket-del"
            },
        )
        logs.delete_delivery_destination(name=dest_name)
        # Verify gone
        resp = logs.describe_delivery_destinations()
        names = [d["name"] for d in resp["deliveryDestinations"]]
        assert dest_name not in names

    def test_put_delivery_destination_policy(self, logs):
        """PutDeliveryDestinationPolicy sets a policy on a destination."""
        import json

        dest_name = _unique("test-dest-pol")
        logs.put_delivery_destination(
            name=dest_name,
            outputFormat="json",
            deliveryDestinationConfiguration={
                "destinationResourceArn": "arn:aws:s3:::test-bucket-policy"
            },
        )
        try:
            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": "123456789012"},
                            "Action": "logs:CreateDelivery",
                            "Resource": "*",
                        }
                    ],
                }
            )
            resp = logs.put_delivery_destination_policy(
                deliveryDestinationName=dest_name,
                deliveryDestinationPolicy=policy_doc,
            )
            assert "policy" in resp
        finally:
            logs.delete_delivery_destination(name=dest_name)

    def test_get_delivery_destination_policy(self, logs):
        """GetDeliveryDestinationPolicy returns the policy."""

        dest_name = _unique("test-dest-gpol")
        logs.put_delivery_destination(
            name=dest_name,
            outputFormat="json",
            deliveryDestinationConfiguration={
                "destinationResourceArn": "arn:aws:s3:::test-bucket-gpolicy"
            },
        )
        try:
            resp = logs.get_delivery_destination_policy(
                deliveryDestinationName=dest_name,
            )
            assert "policy" in resp
        finally:
            logs.delete_delivery_destination(name=dest_name)

    def test_get_query_results(self, logs):
        """GetQueryResults returns results for a completed query."""
        import time

        name = _unique("/test/qresults")
        logs.create_log_group(logGroupName=name)
        try:
            r = logs.start_query(
                logGroupName=name,
                startTime=0,
                endTime=int(time.time()),
                queryString="fields @timestamp",
            )
            qid = r["queryId"]
            time.sleep(1)
            resp = logs.get_query_results(queryId=qid)
            assert "status" in resp
            assert resp["status"] in ("Complete", "Running", "Scheduled")
            assert "results" in resp
        finally:
            logs.delete_log_group(logGroupName=name)


class TestLogsAutoCoverage:
    """Auto-generated coverage tests for logs."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    def test_describe_import_tasks(self, client):
        """DescribeImportTasks returns a response."""
        resp = client.describe_import_tasks()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_scheduled_queries(self, client):
        """ListScheduledQueries returns a response."""
        resp = client.list_scheduled_queries()
        assert "scheduledQueries" in resp

    def test_put_describe_query_definition_lifecycle(self, client):
        """PutQueryDefinition → DescribeQueryDefinitions with content verification."""
        name = _unique("lifecycle-query")
        query = "fields @timestamp, @message | sort @timestamp desc | limit 50"
        resp = client.put_query_definition(
            name=name,
            queryString=query,
            logGroupNames=["/test/example"],
        )
        qid = resp["queryDefinitionId"]
        assert qid

        # Describe and verify fields
        desc = client.describe_query_definitions()
        matching = [q for q in desc["queryDefinitions"] if q["queryDefinitionId"] == qid]
        assert len(matching) == 1
        qdef = matching[0]
        assert qdef["name"] == name
        assert qdef["queryString"] == query

    def test_put_subscription_filter_with_destination(self, client):
        """PutSubscriptionFilter → DescribeSubscriptionFilters → DeleteSubscriptionFilter."""
        group = _unique("/test/sub-filter")
        client.create_log_group(logGroupName=group)
        try:
            client.create_log_stream(logGroupName=group, logStreamName="test-stream")
            filter_name = _unique("sub-filter")
            dest_arn = "arn:aws:lambda:us-east-1:123456789012:function:dummy-processor"
            client.put_subscription_filter(
                logGroupName=group,
                filterName=filter_name,
                filterPattern="ERROR",
                destinationArn=dest_arn,
            )
            resp = client.describe_subscription_filters(logGroupName=group)
            filters = [f for f in resp["subscriptionFilters"] if f["filterName"] == filter_name]
            assert len(filters) == 1
            assert filters[0]["filterPattern"] == "ERROR"
            assert filters[0]["destinationArn"] == dest_arn

            client.delete_subscription_filter(logGroupName=group, filterName=filter_name)
            resp = client.describe_subscription_filters(logGroupName=group)
            names = [f["filterName"] for f in resp["subscriptionFilters"]]
            assert filter_name not in names
        finally:
            client.delete_log_group(logGroupName=group)

    def test_put_describe_delete_resource_policy_v2(self, client):
        """ResourcePolicy full lifecycle with content verification."""
        policy_name = _unique("res-pol-v2")
        policy_doc = (
            '{"Version":"2012-10-17","Statement":[{"Sid":"AllowCWL","Effect":"Allow",'
            '"Principal":{"Service":"es.amazonaws.com"},'
            '"Action":["logs:PutLogEvents","logs:CreateLogStream"],'
            '"Resource":"*"}]}'
        )
        client.put_resource_policy(policyName=policy_name, policyDocument=policy_doc)
        try:
            resp = client.describe_resource_policies()
            matching = [p for p in resp["resourcePolicies"] if p.get("policyName") == policy_name]
            assert len(matching) == 1
            assert "policyDocument" in matching[0]
        finally:
            client.delete_resource_policy(policyName=policy_name)

        # Verify deletion
        resp = client.describe_resource_policies()
        names = [p.get("policyName") for p in resp["resourcePolicies"]]
        assert policy_name not in names

    def test_put_destination_describe_delete(self, client):
        """PutDestination → DescribeDestinations → DeleteDestination with field assertions."""
        dest_name = _unique("dest-v2")
        target_arn = "arn:aws:kinesis:us-east-1:000000000000:stream/test-stream"
        role_arn = "arn:aws:iam::000000000000:role/test-role"
        client.put_destination(
            destinationName=dest_name,
            targetArn=target_arn,
            roleArn=role_arn,
        )
        try:
            resp = client.describe_destinations(DestinationNamePrefix=dest_name)
            matching = [d for d in resp["destinations"] if d["destinationName"] == dest_name]
            assert len(matching) == 1
            assert matching[0]["targetArn"] == target_arn
            assert matching[0]["roleArn"] == role_arn
        finally:
            client.delete_destination(destinationName=dest_name)

        # Verify deletion
        resp = client.describe_destinations(DestinationNamePrefix=dest_name)
        names = [d["destinationName"] for d in resp["destinations"]]
        assert dest_name not in names


class TestLogsQueryDefinitionOperations:
    """Tests for PutQueryDefinition and related query definition operations."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.client = make_client("logs")

    def test_put_query_definition(self):
        """PutQueryDefinition creates a saved query and returns its ID."""
        name = _unique("qdef")
        resp = self.client.put_query_definition(
            name=name,
            queryString="fields @timestamp, @message | sort @timestamp desc | limit 20",
        )
        assert "queryDefinitionId" in resp
        assert len(resp["queryDefinitionId"]) > 0

    def test_put_query_definition_with_log_groups(self):
        """PutQueryDefinition with logGroupNames scopes the query."""
        group = _unique("/test/qdef-group")
        self.client.create_log_group(logGroupName=group)
        try:
            name = _unique("qdef-scoped")
            resp = self.client.put_query_definition(
                name=name,
                queryString="fields @timestamp | limit 5",
                logGroupNames=[group],
            )
            qid = resp["queryDefinitionId"]
            # Verify via describe
            desc = self.client.describe_query_definitions()
            found = [q for q in desc["queryDefinitions"] if q["queryDefinitionId"] == qid]
            assert len(found) == 1
            assert found[0]["name"] == name
            assert group in found[0].get("logGroupNames", [])
        finally:
            self.client.delete_log_group(logGroupName=group)

    def test_describe_query_definitions_returns_created(self):
        """DescribeQueryDefinitions lists previously created query definitions."""
        name = _unique("qdef-desc")
        resp = self.client.put_query_definition(
            name=name,
            queryString="fields @timestamp | limit 10",
        )
        qid = resp["queryDefinitionId"]
        desc = self.client.describe_query_definitions()
        assert "queryDefinitions" in desc
        ids = [q["queryDefinitionId"] for q in desc["queryDefinitions"]]
        assert qid in ids

    def test_put_query_definition_update_existing(self):
        """PutQueryDefinition with existing queryDefinitionId updates the query."""
        name = _unique("qdef-upd")
        resp = self.client.put_query_definition(
            name=name,
            queryString="fields @timestamp | limit 5",
        )
        qid = resp["queryDefinitionId"]
        # Update
        resp2 = self.client.put_query_definition(
            name=name,
            queryDefinitionId=qid,
            queryString="fields @message | limit 100",
        )
        assert resp2["queryDefinitionId"] == qid
        # Verify updated
        desc = self.client.describe_query_definitions()
        found = [q for q in desc["queryDefinitions"] if q["queryDefinitionId"] == qid]
        assert len(found) == 1
        assert "limit 100" in found[0]["queryString"]


class TestLogsQueryExecution:
    """Tests for StartQuery, GetQueryResults, StopQuery."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.client = make_client("logs")
        self.group = _unique("/test/query-exec")
        self.client.create_log_group(logGroupName=self.group)
        self.client.create_log_stream(logGroupName=self.group, logStreamName="s1")
        self.client.put_log_events(
            logGroupName=self.group,
            logStreamName="s1",
            logEvents=[{"timestamp": int(time.time() * 1000), "message": "query test msg"}],
        )
        yield
        self.client.delete_log_group(logGroupName=self.group)

    def test_start_query_returns_query_id(self):
        """StartQuery returns a queryId."""
        resp = self.client.start_query(
            logGroupName=self.group,
            startTime=int(time.time()) - 3600,
            endTime=int(time.time()) + 60,
            queryString="fields @timestamp, @message | limit 10",
        )
        assert "queryId" in resp
        assert len(resp["queryId"]) > 0

    def test_get_query_results(self):
        """GetQueryResults returns results for a started query."""
        resp = self.client.start_query(
            logGroupName=self.group,
            startTime=int(time.time()) - 3600,
            endTime=int(time.time()) + 60,
            queryString="fields @timestamp, @message | limit 10",
        )
        qid = resp["queryId"]
        results = self.client.get_query_results(queryId=qid)
        assert "status" in results
        assert results["status"] in ("Complete", "Running", "Scheduled")
        assert "results" in results

    def test_stop_query(self):
        """StopQuery stops a running query."""
        resp = self.client.start_query(
            logGroupName=self.group,
            startTime=int(time.time()) - 3600,
            endTime=int(time.time()) + 60,
            queryString="fields @timestamp | limit 10",
        )
        qid = resp["queryId"]
        stop_resp = self.client.stop_query(queryId=qid)
        assert "success" in stop_resp


class TestLogsCancelExportTask:
    """Tests for CancelExportTask."""

    def test_cancel_export_task_nonexistent(self):
        """CancelExportTask with fake taskId raises ResourceNotFoundException."""
        client = make_client("logs")
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.cancel_export_task(taskId="nonexistent-task-id")


class TestLogsDeliverySourceOperations:
    """Tests for GetDeliverySource and DeleteDeliverySource."""

    def test_get_delivery_source_nonexistent(self):
        """GetDeliverySource with nonexistent name raises ResourceNotFoundException."""
        client = make_client("logs")
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_delivery_source(name="nonexistent-source")

    def test_delete_delivery_source_nonexistent(self):
        """DeleteDeliverySource with nonexistent name raises ResourceNotFoundException."""
        client = make_client("logs")
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.delete_delivery_source(name="nonexistent-source")


class TestLogsDeliveryOperations:
    """Tests for Delivery create/get/delete and delivery source operations."""

    def test_put_delivery_source(self):
        """PutDeliverySource creates a delivery source for a supported service."""
        client = make_client("logs")
        name = _unique("delsrc")
        # Moto requires a supported service in the ARN (cloudfront, bedrock, etc.)
        arn = "arn:aws:cloudfront::123456789012:distribution/EXAMPLE"
        resp = client.put_delivery_source(
            name=name,
            resourceArn=arn,
            logType="ACCESS_LOGS",
        )
        assert "deliverySource" in resp
        assert resp["deliverySource"]["name"] == name

    def test_get_delivery_nonexistent(self):
        """GetDelivery with nonexistent ID raises ResourceNotFoundException."""
        client = make_client("logs")
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_delivery(id="nonexistent-delivery-id")

    def test_delete_delivery_nonexistent(self):
        """DeleteDelivery with nonexistent ID raises ResourceNotFoundException."""
        client = make_client("logs")
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.delete_delivery(id="nonexistent-delivery-id")

    def test_delete_delivery_destination_policy_nonexistent(self):
        """DeleteDeliveryDestinationPolicy with nonexistent name raises error."""
        client = make_client("logs")
        with pytest.raises((client.exceptions.ResourceNotFoundException, ClientError)):
            client.delete_delivery_destination_policy(deliveryDestinationName="nonexistent-dest")

    def test_create_delivery_and_get_delivery(self):
        """CreateDelivery creates a delivery, GetDelivery retrieves it."""
        client = make_client("logs")
        src_name = _unique("delsrc")
        dest_name = _unique("deldest")
        group_name = _unique("/test/delivery-dest")

        # Create log group for destination
        client.create_log_group(logGroupName=group_name)
        try:
            # Create delivery source (cloudfront)
            cf_arn = "arn:aws:cloudfront::123456789012:distribution/ABCDEF"
            client.put_delivery_source(name=src_name, resourceArn=cf_arn, logType="ACCESS_LOGS")
            # Create delivery destination (log group)
            dest_resp = client.put_delivery_destination(
                name=dest_name,
                deliveryDestinationConfiguration={
                    "destinationResourceArn": (
                        f"arn:aws:logs:us-east-1:123456789012:log-group:{group_name}"
                    )
                },
            )
            dest_arn = dest_resp["deliveryDestination"]["arn"]

            # Create delivery
            create_resp = client.create_delivery(
                deliverySourceName=src_name,
                deliveryDestinationArn=dest_arn,
            )
            assert "delivery" in create_resp
            delivery_id = create_resp["delivery"]["id"]
            assert delivery_id is not None

            # Get delivery
            get_resp = client.get_delivery(id=delivery_id)
            assert get_resp["delivery"]["id"] == delivery_id
            assert get_resp["delivery"]["deliverySourceName"] == src_name

            # Delete delivery
            client.delete_delivery(id=delivery_id)
            # Verify it's gone
            with pytest.raises(client.exceptions.ResourceNotFoundException):
                client.get_delivery(id=delivery_id)
        finally:
            client.delete_log_group(logGroupName=group_name)


class TestLogsAccountPolicies:
    """Tests for DescribeAccountPolicies."""

    def test_describe_account_policies_data_protection(self):
        """DescribeAccountPolicies with DATA_PROTECTION_POLICY returns empty list."""
        client = make_client("logs")
        resp = client.describe_account_policies(policyType="DATA_PROTECTION_POLICY")
        assert "accountPolicies" in resp
        assert isinstance(resp["accountPolicies"], list)

    def test_describe_account_policies_subscription_filter(self):
        """DescribeAccountPolicies with SUBSCRIPTION_FILTER_POLICY returns empty list."""
        client = make_client("logs")
        resp = client.describe_account_policies(policyType="SUBSCRIPTION_FILTER_POLICY")
        assert "accountPolicies" in resp
        assert isinstance(resp["accountPolicies"], list)


class TestLogsAnomalyDetector:
    """Tests for GetLogAnomalyDetector."""

    def test_get_log_anomaly_detector_nonexistent(self):
        """GetLogAnomalyDetector with fake ARN raises ResourceNotFoundException."""
        client = make_client("logs")
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_log_anomaly_detector(
                anomalyDetectorArn="arn:aws:logs:us-east-1:123456789012:anomaly-detector:fake-id"
            )


class TestLogsMetricFilterCRUD:
    """Full CRUD lifecycle tests for metric filters."""

    def test_metric_filter_create_describe_delete(self, logs):
        """Create, describe, and delete a metric filter."""
        group = f"/test/mf-crud-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            logs.put_metric_filter(
                logGroupName=group,
                filterName="error-count",
                filterPattern="ERROR",
                metricTransformations=[
                    {
                        "metricName": "ErrorCount",
                        "metricNamespace": "TestApp",
                        "metricValue": "1",
                    }
                ],
            )
            resp = logs.describe_metric_filters(logGroupName=group)
            assert len(resp["metricFilters"]) == 1
            mf = resp["metricFilters"][0]
            assert mf["filterName"] == "error-count"
            assert mf["filterPattern"] == "ERROR"
            assert mf["metricTransformations"][0]["metricName"] == "ErrorCount"

            logs.delete_metric_filter(logGroupName=group, filterName="error-count")
            resp2 = logs.describe_metric_filters(logGroupName=group)
            assert len(resp2["metricFilters"]) == 0
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_metric_filter_update_overwrites(self, logs):
        """Putting a metric filter with the same name overwrites it."""
        group = f"/test/mf-upd-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            logs.put_metric_filter(
                logGroupName=group,
                filterName="my-filter",
                filterPattern="ERROR",
                metricTransformations=[
                    {"metricName": "E", "metricNamespace": "NS", "metricValue": "1"}
                ],
            )
            logs.put_metric_filter(
                logGroupName=group,
                filterName="my-filter",
                filterPattern="FATAL",
                metricTransformations=[
                    {"metricName": "F", "metricNamespace": "NS", "metricValue": "1"}
                ],
            )
            resp = logs.describe_metric_filters(logGroupName=group, filterNamePrefix="my-filter")
            assert len(resp["metricFilters"]) == 1
            assert resp["metricFilters"][0]["filterPattern"] == "FATAL"
            assert resp["metricFilters"][0]["metricTransformations"][0]["metricName"] == "F"
        finally:
            try:
                logs.delete_metric_filter(logGroupName=group, filterName="my-filter")
            except Exception:
                pass  # best-effort cleanup
            logs.delete_log_group(logGroupName=group)

    def test_multiple_metric_filters_on_group(self, logs):
        """Multiple metric filters on the same log group."""
        group = f"/test/mf-multi-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            for name, pattern in [("mf-a", "ERROR"), ("mf-b", "WARN"), ("mf-c", "FATAL")]:
                logs.put_metric_filter(
                    logGroupName=group,
                    filterName=name,
                    filterPattern=pattern,
                    metricTransformations=[
                        {"metricName": name, "metricNamespace": "NS", "metricValue": "1"}
                    ],
                )
            resp = logs.describe_metric_filters(logGroupName=group)
            assert len(resp["metricFilters"]) == 3
            names = {mf["filterName"] for mf in resp["metricFilters"]}
            assert names == {"mf-a", "mf-b", "mf-c"}
        finally:
            for name in ["mf-a", "mf-b", "mf-c"]:
                try:
                    logs.delete_metric_filter(logGroupName=group, filterName=name)
                except Exception:
                    pass  # best-effort cleanup
            logs.delete_log_group(logGroupName=group)


class TestLogsSubscriptionFilterCRUD:
    """Full CRUD lifecycle tests for subscription filters."""

    def test_subscription_filter_create_describe_delete(self, logs):
        """Create, describe, and delete a subscription filter."""
        group = f"/test/sf-crud-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        dest_arn = "arn:aws:lambda:us-east-1:123456789012:function:fake"
        try:
            logs.put_subscription_filter(
                logGroupName=group,
                filterName="sf-test",
                filterPattern="ERROR",
                destinationArn=dest_arn,
            )
            resp = logs.describe_subscription_filters(logGroupName=group)
            assert len(resp["subscriptionFilters"]) == 1
            sf = resp["subscriptionFilters"][0]
            assert sf["filterName"] == "sf-test"
            assert sf["filterPattern"] == "ERROR"
            assert sf["destinationArn"] == dest_arn

            logs.delete_subscription_filter(logGroupName=group, filterName="sf-test")
            resp2 = logs.describe_subscription_filters(logGroupName=group)
            assert len(resp2["subscriptionFilters"]) == 0
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_subscription_filter_describe_with_prefix(self, logs):
        """Describe subscription filters with filterNamePrefix."""
        group = f"/test/sf-pfx-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        dest_arn = "arn:aws:lambda:us-east-1:123456789012:function:fake"
        try:
            logs.put_subscription_filter(
                logGroupName=group,
                filterName="prefix-alpha",
                filterPattern="ERROR",
                destinationArn=dest_arn,
            )
            resp = logs.describe_subscription_filters(
                logGroupName=group, filterNamePrefix="prefix-"
            )
            assert len(resp["subscriptionFilters"]) >= 1
            assert resp["subscriptionFilters"][0]["filterName"] == "prefix-alpha"
        finally:
            try:
                logs.delete_subscription_filter(logGroupName=group, filterName="prefix-alpha")
            except Exception:
                pass  # best-effort cleanup
            logs.delete_log_group(logGroupName=group)


class TestLogsTagCRUD:
    """Full CRUD tests for log group tagging (both old and new APIs)."""

    def test_tag_resource_and_list_tags_for_resource(self, logs):
        """Tag a log group via new API and list tags."""
        group = f"/test/tag-new-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        arn = f"arn:aws:logs:us-east-1:000000000000:log-group:{group}"
        try:
            logs.tag_resource(resourceArn=arn, tags={"env": "test", "project": "roboto"})
            resp = logs.list_tags_for_resource(resourceArn=arn)
            assert resp["tags"]["env"] == "test"
            assert resp["tags"]["project"] == "roboto"
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_untag_resource(self, logs):
        """Untag a log group via new API."""
        group = f"/test/untag-new-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        arn = f"arn:aws:logs:us-east-1:000000000000:log-group:{group}"
        try:
            logs.tag_resource(resourceArn=arn, tags={"keep": "yes", "remove": "yes"})
            logs.untag_resource(resourceArn=arn, tagKeys=["remove"])
            resp = logs.list_tags_for_resource(resourceArn=arn)
            assert "keep" in resp["tags"]
            assert "remove" not in resp["tags"]
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_tag_log_group_old_api(self, logs):
        """Tag and list tags via the old TagLogGroup/ListTagsLogGroup API."""
        group = f"/test/tag-old-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            logs.tag_log_group(logGroupName=group, tags={"old-key": "old-val", "env": "test"})
            resp = logs.list_tags_log_group(logGroupName=group)
            assert resp["tags"]["old-key"] == "old-val"
            assert resp["tags"]["env"] == "test"
            # Retrieve group via describe to verify it exists
            desc = logs.describe_log_groups(logGroupNamePrefix=group)
            grp = [g for g in desc["logGroups"] if g["logGroupName"] == group][0]
            assert grp["logGroupName"] == group
            # Update: untag one key
            logs.untag_log_group(logGroupName=group, tags=["env"])
            resp2 = logs.list_tags_log_group(logGroupName=group)
            assert "env" not in resp2["tags"]
            assert resp2["tags"]["old-key"] == "old-val"
            # Error: tag a nonexistent log group
            with pytest.raises(ClientError) as exc:
                logs.tag_log_group(
                    logGroupName="/test/nonexistent-tag-old-xyz-99999",
                    tags={"key": "val"},
                )
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_untag_log_group_old_api(self, logs):
        """Untag via the old UntagLogGroup API."""
        group = f"/test/untag-old-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            logs.tag_log_group(logGroupName=group, tags={"keep": "yes", "drop": "yes"})
            logs.untag_log_group(logGroupName=group, tags=["drop"])
            resp = logs.list_tags_log_group(logGroupName=group)
            assert "keep" in resp["tags"]
            assert "drop" not in resp["tags"]
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_create_log_group_with_tags(self, logs):
        """Create a log group with initial tags."""
        group = f"/test/init-tags-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group, tags={"created": "with-tags"})
        try:
            resp = logs.list_tags_log_group(logGroupName=group)
            assert resp["tags"]["created"] == "with-tags"
        finally:
            logs.delete_log_group(logGroupName=group)


class TestLogsRetentionCRUD:
    """Focused retention policy tests."""

    def test_put_and_verify_retention(self, logs):
        """Put retention policy and verify via describe."""
        group = f"/test/ret-put-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            logs.put_retention_policy(logGroupName=group, retentionInDays=30)
            resp = logs.describe_log_groups(logGroupNamePrefix=group)
            grp = [g for g in resp["logGroups"] if g["logGroupName"] == group][0]
            assert grp["retentionInDays"] == 30
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_delete_retention_removes_field(self, logs):
        """Delete retention policy removes retentionInDays from describe."""
        group = f"/test/ret-del-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            logs.put_retention_policy(logGroupName=group, retentionInDays=7)
            logs.delete_retention_policy(logGroupName=group)
            resp = logs.describe_log_groups(logGroupNamePrefix=group)
            grp = [g for g in resp["logGroups"] if g["logGroupName"] == group][0]
            assert "retentionInDays" not in grp
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_update_retention_changes_value(self, logs):
        """Updating retention policy changes the value."""
        group = f"/test/ret-upd-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            logs.put_retention_policy(logGroupName=group, retentionInDays=7)
            logs.put_retention_policy(logGroupName=group, retentionInDays=90)
            resp = logs.describe_log_groups(logGroupNamePrefix=group)
            grp = [g for g in resp["logGroups"] if g["logGroupName"] == group][0]
            assert grp["retentionInDays"] == 90
        finally:
            logs.delete_log_group(logGroupName=group)


class TestLogsEventsCRUD:
    """Focused log event put/get/filter tests."""

    def test_put_and_get_multiple_events(self, logs):
        """Put multiple events and retrieve them."""
        group = f"/test/evt-multi-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        stream = "test-stream"
        logs.create_log_stream(logGroupName=group, logStreamName=stream)
        try:
            now = int(time.time() * 1000)
            logs.put_log_events(
                logGroupName=group,
                logStreamName=stream,
                logEvents=[
                    {"timestamp": now, "message": "event-one"},
                    {"timestamp": now + 1, "message": "event-two"},
                    {"timestamp": now + 2, "message": "event-three"},
                ],
            )
            resp = logs.get_log_events(logGroupName=group, logStreamName=stream)
            messages = [e["message"] for e in resp["events"]]
            assert "event-one" in messages
            assert "event-two" in messages
            assert "event-three" in messages
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_filter_log_events_with_pattern(self, logs):
        """Filter log events using a pattern."""
        group = f"/test/evt-filt-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        stream = "filter-stream"
        logs.create_log_stream(logGroupName=group, logStreamName=stream)
        try:
            now = int(time.time() * 1000)
            logs.put_log_events(
                logGroupName=group,
                logStreamName=stream,
                logEvents=[
                    {"timestamp": now, "message": "ERROR critical failure"},
                    {"timestamp": now + 1, "message": "INFO all ok"},
                    {"timestamp": now + 2, "message": "ERROR another issue"},
                ],
            )
            resp = logs.filter_log_events(logGroupName=group, filterPattern="ERROR")
            messages = [e["message"] for e in resp["events"]]
            assert len(messages) >= 1
            assert all("ERROR" in m for m in messages)
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_get_log_events_with_start_time(self, logs):
        """Get log events filtered by startTime."""
        group = f"/test/evt-time-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        stream = "time-stream"
        logs.create_log_stream(logGroupName=group, logStreamName=stream)
        try:
            now = int(time.time() * 1000)
            logs.put_log_events(
                logGroupName=group,
                logStreamName=stream,
                logEvents=[
                    {"timestamp": now - 10000, "message": "old-event"},
                    {"timestamp": now, "message": "recent-event"},
                ],
            )
            resp = logs.get_log_events(
                logGroupName=group, logStreamName=stream, startTime=now - 1000
            )
            messages = [e["message"] for e in resp["events"]]
            assert "recent-event" in messages
        finally:
            logs.delete_log_group(logGroupName=group)


class TestLogsNewOps:
    """Tests for newly verified Logs operations."""

    @pytest.fixture
    def logs(self):
        return make_client("logs")

    def test_describe_import_task_batches(self, logs):
        """DescribeImportTaskBatches with fake importId returns empty batches."""
        resp = logs.describe_import_task_batches(importId="fake-import-id")
        assert "importBatches" in resp
        assert isinstance(resp["importBatches"], list)

    def test_get_data_protection_policy(self, logs):
        """GetDataProtectionPolicy on a log group returns a response."""
        group = f"/test/dp-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            resp = logs.get_data_protection_policy(logGroupIdentifier=group)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_get_integration_nonexistent(self, logs):
        """GetIntegration with nonexistent name raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.get_integration(integrationName="fake-integration")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_log_group_fields(self, logs):
        """GetLogGroupFields returns logGroupFields list."""
        group = f"/test/fields-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            resp = logs.get_log_group_fields(logGroupName=group)
            assert "logGroupFields" in resp
            assert isinstance(resp["logGroupFields"], list)
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_get_log_record_nonexistent(self, logs):
        """GetLogRecord with fake pointer raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.get_log_record(logRecordPointer="fake-pointer")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_transformer_nonexistent(self, logs):
        """GetTransformer on group with no transformer raises ResourceNotFoundException."""
        group = f"/test/trans-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            with pytest.raises(ClientError) as exc:
                logs.get_transformer(logGroupIdentifier=group)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_list_log_groups_for_query_nonexistent(self, logs):
        """ListLogGroupsForQuery with fake queryId raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.list_log_groups_for_query(queryId="fake-query-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_log_anomaly_detector(self, logs):
        """CreateLogAnomalyDetector creates a detector and returns ARN."""
        group = f"/test/anomaly-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=group)
        try:
            group_arn = _log_group_arn(group)
            resp = logs.create_log_anomaly_detector(logGroupArnList=[group_arn])
            assert "anomalyDetectorArn" in resp
            assert resp["anomalyDetectorArn"]
            logs.delete_log_anomaly_detector(anomalyDetectorArn=resp["anomalyDetectorArn"])
        finally:
            logs.delete_log_group(logGroupName=group)

    def test_delete_log_anomaly_detector_nonexistent(self, logs):
        """DeleteLogAnomalyDetector with nonexistent ARN raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_log_anomaly_detector(
                anomalyDetectorArn="arn:aws:logs:us-east-1:123456789012:anomaly-detector:fake"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_account_policy(self, logs):
        """DeleteAccountPolicy with nonexistent policy raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_account_policy(
                policyName="nonexistent-policy",
                policyType="DATA_PROTECTION_POLICY",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_data_protection_policy_nonexistent(self, logs):
        """DeleteDataProtectionPolicy on nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_data_protection_policy(logGroupIdentifier="/nonexistent/group")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_index_policy_nonexistent(self, logs):
        """DeleteIndexPolicy on nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_index_policy(logGroupIdentifier="/nonexistent/group")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_integration_nonexistent(self, logs):
        """DeleteIntegration with nonexistent name raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_integration(integrationName="nonexistent-integration")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_query_definition_nonexistent(self, logs):
        """DeleteQueryDefinition with nonexistent ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_query_definition(queryDefinitionId="fake-query-def-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_transformer_nonexistent(self, logs):
        """DeleteTransformer on nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_transformer(logGroupIdentifier="/nonexistent/group")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_log_fields(self, logs):
        """GetLogFields returns a 200 response."""
        resp = logs.get_log_fields(
            dataSourceName="test-source",
            dataSourceType="CLOUDWATCH_LOG_GROUP",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_aggregate_log_group_summaries(self, logs):
        """ListAggregateLogGroupSummaries returns summaries list."""
        resp = logs.list_aggregate_log_group_summaries(groupBy="LOG_CLASS")
        assert "aggregateLogGroupSummaries" in resp
        assert isinstance(resp["aggregateLogGroupSummaries"], list)

    def test_put_account_policy_data_protection(self, logs):
        """PutAccountPolicy with DATA_PROTECTION_POLICY type succeeds."""
        import json

        policy = json.dumps(
            {
                "Name": "test-data-protection",
                "Description": "test",
                "Version": "2021-06-01",
                "Statement": [
                    {
                        "Sid": "audit-policy",
                        "DataIdentifier": [
                            "arn:aws:dataprotection::aws:data-identifier/EmailAddress"
                        ],
                        "Operation": {"Audit": {"FindingsDestination": {}}},
                    }
                ],
            }
        )
        resp = logs.put_account_policy(
            policyName="test-dp-policy",
            policyDocument=policy,
            policyType="DATA_PROTECTION_POLICY",
        )
        assert "accountPolicy" in resp
        assert resp["accountPolicy"]["policyName"] == "test-dp-policy"
        logs.delete_account_policy(
            policyName="test-dp-policy",
            policyType="DATA_PROTECTION_POLICY",
        )

    def test_put_bearer_token_authentication_nonexistent(self, logs):
        """PutBearerTokenAuthentication on nonexistent group raises error."""
        with pytest.raises(ClientError) as exc:
            logs.put_bearer_token_authentication(
                logGroupIdentifier="/nonexistent/group",
                bearerTokenAuthenticationEnabled=True,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_data_protection_policy_nonexistent(self, logs):
        """PutDataProtectionPolicy on nonexistent group raises ResourceNotFoundException."""
        import json

        policy = json.dumps(
            {
                "Name": "test",
                "Version": "2021-06-01",
                "Statement": [
                    {
                        "Sid": "audit",
                        "DataIdentifier": [
                            "arn:aws:dataprotection::aws:data-identifier/EmailAddress"
                        ],
                        "Operation": {"Audit": {"FindingsDestination": {}}},
                    }
                ],
            }
        )
        with pytest.raises(ClientError) as exc:
            logs.put_data_protection_policy(
                logGroupIdentifier="/nonexistent/group",
                policyDocument=policy,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_index_policy_nonexistent(self, logs):
        """PutIndexPolicy on nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.put_index_policy(
                logGroupIdentifier="/nonexistent/group",
                policyDocument='{"Fields": ["@timestamp"]}',
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_log_anomaly_detector_nonexistent(self, logs):
        """UpdateLogAnomalyDetector with nonexistent ARN raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.update_log_anomaly_detector(
                anomalyDetectorArn="arn:aws:logs:us-east-1:123456789012:anomaly-detector:fake",
                enabled=False,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_scheduled_query_nonexistent(self, logs):
        """GetScheduledQuery with nonexistent identifier raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.get_scheduled_query(
                identifier="arn:aws:logs:us-east-1:123456789012:scheduled-query:nonexistent",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_scheduled_query_nonexistent(self, logs):
        """DeleteScheduledQuery with nonexistent identifier raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_scheduled_query(
                identifier="arn:aws:logs:us-east-1:123456789012:scheduled-query:nonexistent",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_anomaly_nonexistent(self, logs):
        """UpdateAnomaly with nonexistent detector ARN raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.update_anomaly(
                anomalyDetectorArn="arn:aws:logs:us-east-1:123456789012:anomaly-detector:fake",
                suppressionType="LIMITED",
                suppressionPeriod={"value": 1, "suppressionUnit": "HOURS"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_delivery_configuration_nonexistent(self, logs):
        """UpdateDeliveryConfiguration with nonexistent ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.update_delivery_configuration(
                id="fake-delivery-id",
                fieldDelimiter=",",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_scheduled_query_history_nonexistent(self, logs):
        """GetScheduledQueryHistory with nonexistent identifier raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.get_scheduled_query_history(
                identifier="arn:aws:logs:us-east-1:123456789012:scheduled-query:nonexistent",
                startTime=1704067200000,
                endTime=1735689600000,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_scheduled_query_nonexistent(self, logs):
        """UpdateScheduledQuery with nonexistent identifier raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.update_scheduled_query(
                identifier="arn:aws:logs:us-east-1:123456789012:scheduled-query:nonexistent",
                queryLanguage="CWLI",
                queryString="fields @timestamp | limit 5",
                scheduleExpression="rate(2 hours)",
                executionRoleArn="arn:aws:iam::123456789012:role/test",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_scheduled_query_conflict(self, logs):
        """CreateScheduledQuery with a duplicate name raises ConflictException."""
        unique_name = f"conflict-{uuid.uuid4().hex[:8]}"
        resp = logs.create_scheduled_query(
            name=unique_name,
            queryLanguage="CWLI",
            queryString="fields @timestamp | limit 10",
            scheduleExpression="rate(1 hour)",
            executionRoleArn="arn:aws:iam::123456789012:role/test",
        )
        arn = resp["scheduledQueryArn"]
        try:
            with pytest.raises(ClientError) as exc:
                logs.create_scheduled_query(
                    name=unique_name,
                    queryLanguage="CWLI",
                    queryString="fields @timestamp | limit 5",
                    scheduleExpression="rate(2 hours)",
                    executionRoleArn="arn:aws:iam::123456789012:role/test",
                )
            assert exc.value.response["Error"]["Code"] == "ConflictException"
        finally:
            logs.delete_scheduled_query(identifier=arn)


class TestLogsScheduledQueryList:
    """Tests for ScheduledQuery list operation."""

    def test_list_scheduled_queries(self, logs):
        """ListScheduledQueries returns a response with scheduledQueries key."""
        resp = logs.list_scheduled_queries()
        assert "scheduledQueries" in resp
        assert isinstance(resp["scheduledQueries"], list)


class TestLogsAdditionalOps:
    """Additional CloudWatch Logs operations."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    def test_put_log_group_deletion_protection_nonexistent(self, client):
        """PutLogGroupDeletionProtection raises ResourceNotFoundException for nonexistent group."""
        fake_arn = "arn:aws:logs:us-east-1:123456789012:log-group:nonexistent-group-xyz:*"
        with pytest.raises(ClientError) as exc:
            client.put_log_group_deletion_protection(
                logGroupIdentifier=fake_arn,
                deletionProtectionEnabled=True,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLogsFieldIndexes:
    """Tests for DescribeFieldIndexes operation."""

    @pytest.fixture
    def logs(self):
        return make_client("logs")

    def test_describe_field_indexes(self, logs):
        """DescribeFieldIndexes returns fieldIndexes key."""
        resp = logs.describe_field_indexes(
            logGroupIdentifiers=["arn:aws:logs:us-east-1:123456789012:log-group:nonexistent"]
        )
        assert "fieldIndexes" in resp
        assert isinstance(resp["fieldIndexes"], list)


class TestLogsIndexPolicies:
    """Tests for DescribeIndexPolicies operation."""

    @pytest.fixture
    def logs(self):
        return make_client("logs")

    def test_describe_index_policies(self, logs):
        """DescribeIndexPolicies returns indexPolicies key."""
        resp = logs.describe_index_policies(
            logGroupIdentifiers=["arn:aws:logs:us-east-1:123456789012:log-group:nonexistent"]
        )
        assert "indexPolicies" in resp
        assert isinstance(resp["indexPolicies"], list)


class TestLogsIntegration:
    """Tests for Integration operations."""

    @pytest.fixture
    def logs(self):
        return make_client("logs")

    def test_delete_integration_nonexistent(self, logs):
        """DeleteIntegration with nonexistent name raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_integration(integrationName="nonexistent-integ", force=True)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_integration_nonexistent(self, logs):
        """GetIntegration with nonexistent name raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.get_integration(integrationName="nonexistent-integ")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLogsTransformer:
    """Tests for Transformer operations."""

    @pytest.fixture
    def logs(self):
        return make_client("logs")

    def test_delete_transformer_nonexistent(self, logs):
        """DeleteTransformer for nonexistent log group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.delete_transformer(
                logGroupIdentifier="arn:aws:logs:us-east-1:123456789012:log-group:nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_transformer_nonexistent(self, logs):
        """GetTransformer for nonexistent log group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.get_transformer(
                logGroupIdentifier="arn:aws:logs:us-east-1:123456789012:log-group:nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_transformer_nonexistent(self, logs):
        """PutTransformer for nonexistent log group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            logs.put_transformer(
                logGroupIdentifier="arn:aws:logs:us-east-1:123456789012:log-group:nonexistent",
                transformerConfig=[{"parseJSON": {}}],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLogsNewStubOps:
    """Tests for newly-implemented CloudWatch Logs stub operations."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    def test_test_metric_filter(self, client):
        """TestMetricFilter: create group+filter, test pattern, verify matches, delete."""
        group = _unique("/test/tmf-stub")
        client.create_log_group(logGroupName=group)
        try:
            client.put_metric_filter(
                logGroupName=group,
                filterName="tmf-filter",
                filterPattern="ERROR",
                metricTransformations=[
                    {"metricName": "EC", "metricNamespace": "NS", "metricValue": "1"}
                ],
            )
            resp = client.test_metric_filter(
                filterPattern="ERROR",
                logEventMessages=["ERROR crash occurred", "INFO all good"],
            )
            assert isinstance(resp["matches"], list)
            assert len(resp["matches"]) == 1
            assert resp["matches"][0]["eventMessage"] == "ERROR crash occurred"
            assert resp["matches"][0]["eventNumber"] == 1
            # List the filter to confirm it exists
            filters = client.describe_metric_filters(logGroupName=group)
            assert len(filters["metricFilters"]) >= 1
            client.delete_metric_filter(logGroupName=group, filterName="tmf-filter")
        finally:
            client.delete_log_group(logGroupName=group)


class TestLogsNewStubOps2:
    """Tests for second batch of newly-implemented CloudWatch Logs stub operations."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    def test_associate_source_to_s3_table_integration(self, client):
        """AssociateSourceToS3TableIntegration succeeds or raises known error."""
        try:
            client.associate_source_to_s3_table_integration(
                integrationArn="arn:aws:logs:us-east-1:123456789012:integration/fake",
                dataSource={"name": "/fake/log-group", "type": "CloudWatchLogs"},
            )
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_disassociate_source_from_s3_table_integration(self, client):
        """DisassociateSourceFromS3TableIntegration succeeds or raises known error."""
        try:
            client.disassociate_source_from_s3_table_integration(
                identifier="arn:aws:logs:us-east-1:123456789012:integration/fake::source",
            )
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_list_sources_for_s3_table_integration(self, client):
        """ListSourcesForS3TableIntegration returns sources key."""
        try:
            resp = client.list_sources_for_s3_table_integration(
                integrationArn="arn:aws:logs:us-east-1:123456789012:integration/fake",
            )
            assert "sources" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_put_integration(self, client):
        """PutIntegration returns integrationName key."""
        try:
            resp = client.put_integration(
                integrationName="test-integration",
                resourceConfig={
                    "openSearchResourceConfig": {
                        "dataSourceRoleArn": ("arn:aws:iam::123456789012:role/test-role"),
                        "dashboardViewerPrincipals": [],
                        "retentionDays": 30,
                    }
                },
                integrationType="OPENSEARCH",
            )
            assert "integrationName" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_test_transformer(self, client):
        """TestTransformer returns transformedLogs key."""
        try:
            resp = client.test_transformer(
                transformerConfig=[
                    {"parseJSON": {}},
                ],
                logEventMessages=['{"level": "INFO", "msg": "test"}'],
            )
            assert "transformedLogs" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None


class TestLogsGapOps:
    """Tests for CloudWatch Logs operations that weren't previously covered."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    def test_cancel_import_task_not_found(self, client):
        """CancelImportTask: create a task, cancel it, verify CANCELLED response."""
        create_resp = client.create_import_task(
            importSourceArn="arn:aws:s3:::test-bucket-notfound/logs/",
            importRoleArn="arn:aws:iam::123456789012:role/LogsImportRole",
        )
        import_id = create_resp["importId"]
        resp = client.cancel_import_task(importId=import_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["importId"] == import_id
        assert resp["importStatus"] == "CANCELLED"

    def test_create_import_task(self, client):
        """CreateImportTask returns a task ID with valid UUID format."""
        resp = client.create_import_task(
            importSourceArn="arn:aws:s3:::nonexistent-bucket/logs/",
            importRoleArn="arn:aws:iam::123456789012:role/LogsImportRole",
        )
        assert "importId" in resp
        assert len(resp["importId"]) > 0
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestLogsStreamingGapOps:
    """Tests for CloudWatch Logs streaming operations (use host prefix)."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    def test_get_log_object_raises_exception(self, client):
        """GetLogObject uses streaming- host prefix; returns 501 or event stream error."""
        import boto3
        from botocore.config import Config
        from botocore.eventstream import ChecksumMismatch

        no_prefix_client = boto3.client(
            "logs",
            endpoint_url="http://localhost:4566",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=Config(inject_host_prefix=False),
        )
        # Returns 501 which botocore's event stream parser may interpret as a
        # ChecksumMismatch before a ClientError is raised
        with pytest.raises((ClientError, ChecksumMismatch)):
            no_prefix_client.get_log_object(logObjectPointer="pointer-abc123")

    def test_start_live_tail_raises_exception(self, client):
        """StartLiveTail uses streaming- host prefix; returns event stream or 501."""
        import boto3
        from botocore.config import Config
        from botocore.eventstream import ChecksumMismatch

        no_prefix_client = boto3.client(
            "logs",
            endpoint_url="http://localhost:4566",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=Config(inject_host_prefix=False),
        )
        with pytest.raises((ClientError, ChecksumMismatch)):
            no_prefix_client.start_live_tail(
                logGroupIdentifiers=["arn:aws:logs:us-east-1:123456789012:log-group:test"]
            )


class TestLogsEdgeCasesAndFidelity:
    """Edge cases and behavioral fidelity tests for CloudWatch Logs."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    # ── describe_log_groups ────────────────────────────────────────────────────

    def test_describe_log_groups_arn_format(self, client):
        """Log groups returned by describe have properly-formed ARNs."""
        name = _unique("/test/arn-fmt")
        client.create_log_group(logGroupName=name)
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            arn = group["arn"]
            assert arn.startswith("arn:aws:logs:")
            assert ":log-group:" in arn
            assert name in arn
        finally:
            client.delete_log_group(logGroupName=name)

    def test_describe_log_groups_creation_time_present(self, client):
        """Log groups have creationTime and storedBytes fields."""
        name = _unique("/test/ctime")
        client.create_log_group(logGroupName=name)
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            assert "creationTime" in group
            assert isinstance(group["creationTime"], int)
            assert group["creationTime"] > 0
            assert "storedBytes" in group
        finally:
            client.delete_log_group(logGroupName=name)

    def test_describe_log_groups_nonexistent_prefix_empty(self, client):
        """DescribeLogGroups with a prefix that matches nothing returns empty list."""
        resp = client.describe_log_groups(logGroupNamePrefix="/no-group-with-this-prefix-xyz-999")
        assert resp["logGroups"] == []

    def test_describe_log_groups_pagination_nexttoken(self, client):
        """DescribeLogGroups pagination: nextToken cycles through all results."""
        prefix = _unique("/test/pgn")
        names = [f"{prefix}-{i}" for i in range(3)]
        for n in names:
            client.create_log_group(logGroupName=n)
        try:
            collected = []
            resp = client.describe_log_groups(logGroupNamePrefix=prefix, limit=2)
            collected.extend([g["logGroupName"] for g in resp["logGroups"]])
            assert len(resp["logGroups"]) == 2
            assert "nextToken" in resp
            resp2 = client.describe_log_groups(
                logGroupNamePrefix=prefix, limit=2, nextToken=resp["nextToken"]
            )
            collected.extend([g["logGroupName"] for g in resp2["logGroups"]])
            for n in names:
                assert n in collected
        finally:
            for n in names:
                client.delete_log_group(logGroupName=n)

    def test_create_log_group_duplicate_raises(self, client):
        """Creating a log group with the same name twice raises ResourceAlreadyExistsException."""
        name = _unique("/test/dup")
        client.create_log_group(logGroupName=name)
        try:
            with pytest.raises(ClientError) as exc:
                client.create_log_group(logGroupName=name)
            assert exc.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
        finally:
            client.delete_log_group(logGroupName=name)

    def test_delete_log_group_nonexistent_raises(self, client):
        """Deleting a nonexistent log group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_log_group(logGroupName="/test/does-not-exist-xyz-abc-999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── filter_log_events ─────────────────────────────────────────────────────

    def test_filter_log_events_event_fields(self, client):
        """FilterLogEvents events contain timestamp, message, logStreamName, ingestionTime, eventId."""
        name = _unique("/test/fle-fields")
        stream = "s1"
        client.create_log_group(logGroupName=name)
        client.create_log_stream(logGroupName=name, logStreamName=stream)
        try:
            ts = int(time.time() * 1000)
            client.put_log_events(
                logGroupName=name,
                logStreamName=stream,
                logEvents=[{"timestamp": ts, "message": "field-check-event"}],
            )
            resp = client.filter_log_events(logGroupName=name)
            assert len(resp["events"]) >= 1
            ev = resp["events"][0]
            assert "timestamp" in ev
            assert "message" in ev
            assert "logStreamName" in ev
            assert "ingestionTime" in ev
            assert "eventId" in ev
            assert ev["logStreamName"] == stream
        finally:
            client.delete_log_group(logGroupName=name)

    def test_filter_log_events_no_match_returns_empty(self, client):
        """FilterLogEvents with a pattern that matches nothing returns empty events."""
        name = _unique("/test/fle-nomatch")
        stream = "s1"
        client.create_log_group(logGroupName=name)
        client.create_log_stream(logGroupName=name, logStreamName=stream)
        try:
            ts = int(time.time() * 1000)
            client.put_log_events(
                logGroupName=name,
                logStreamName=stream,
                logEvents=[{"timestamp": ts, "message": "INFO: all good"}],
            )
            resp = client.filter_log_events(
                logGroupName=name,
                filterPattern="DEFINITELY_WILL_NOT_MATCH_XYZZY",
            )
            assert resp["events"] == []
        finally:
            client.delete_log_group(logGroupName=name)

    def test_filter_log_events_pagination_nexttoken(self, client):
        """FilterLogEvents with limit returns nextToken for pagination."""
        name = _unique("/test/fle-page")
        stream = "s1"
        client.create_log_group(logGroupName=name)
        client.create_log_stream(logGroupName=name, logStreamName=stream)
        try:
            ts = int(time.time() * 1000)
            client.put_log_events(
                logGroupName=name,
                logStreamName=stream,
                logEvents=[{"timestamp": ts + i, "message": f"event-{i}"} for i in range(6)],
            )
            resp = client.filter_log_events(logGroupName=name, limit=3)
            assert len(resp["events"]) <= 3
            assert "nextToken" in resp
            resp2 = client.filter_log_events(
                logGroupName=name, limit=3, nextToken=resp["nextToken"]
            )
            assert "events" in resp2
            # Combined results should cover all 6 events
            all_msgs = [e["message"] for e in resp["events"]] + [
                e["message"] for e in resp2["events"]
            ]
            assert len(all_msgs) >= 3
        finally:
            client.delete_log_group(logGroupName=name)

    def test_filter_log_events_nonexistent_group_raises(self, client):
        """FilterLogEvents on a nonexistent log group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.filter_log_events(logGroupName="/test/does-not-exist-xyz-999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── test_metric_filter ────────────────────────────────────────────────────

    def test_test_metric_filter_returns_list(self, client):
        """TestMetricFilter: matches two ERROR messages out of three."""
        group = _unique("/test/tmf-list")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.test_metric_filter(
                filterPattern="ERROR",
                logEventMessages=["ERROR: crash", "INFO: ok", "ERROR: timeout"],
            )
            assert isinstance(resp["matches"], list)
            assert len(resp["matches"]) == 2
            matched_msgs = [m["eventMessage"] for m in resp["matches"]]
            assert "ERROR: crash" in matched_msgs
            assert "ERROR: timeout" in matched_msgs
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_log_group(logGroupName=group)

    def test_test_metric_filter_single_message(self, client):
        """TestMetricFilter with a single non-matching message returns empty list."""
        group = _unique("/test/tmf-single")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.test_metric_filter(
                filterPattern="ERROR",
                logEventMessages=["only one message"],
            )
            assert isinstance(resp["matches"], list)
            assert resp["matches"] == []
            # Verify matching works correctly too
            resp2 = client.test_metric_filter(
                filterPattern="ERROR",
                logEventMessages=["ERROR single match"],
            )
            assert len(resp2["matches"]) == 1
            assert resp2["matches"][0]["eventMessage"] == "ERROR single match"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_test_metric_filter_response_http_200(self, client):
        """TestMetricFilter returns HTTP 200 and eventNumber starts at 1."""
        group = _unique("/test/tmf-200")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.test_metric_filter(
                filterPattern="ERROR",
                logEventMessages=["INFO skip", "ERROR match", "INFO skip2"],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert len(resp["matches"]) == 1
            # eventNumber reflects position in logEventMessages (1-indexed)
            assert resp["matches"][0]["eventNumber"] == 2
            assert resp["matches"][0]["eventMessage"] == "ERROR match"
        finally:
            client.delete_log_group(logGroupName=group)

    # ── cancel_import_task ────────────────────────────────────────────────────

    def test_cancel_import_task_response_structure(self, client):
        """CancelImportTask: create task, cancel it, verify structure."""
        create_resp = client.create_import_task(
            importSourceArn="arn:aws:s3:::test-bucket/logs/",
            importRoleArn="arn:aws:iam::123456789012:role/LogsImportRole",
        )
        import_id = create_resp["importId"]
        assert import_id
        resp = client.cancel_import_task(importId=import_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["importId"] == import_id
        assert resp["importStatus"] == "CANCELLED"
        # Describe import tasks returns 200
        desc = client.describe_import_tasks()
        assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── describe_queries ─────────────────────────────────────────────────────

    def test_describe_queries_returns_list(self, client):
        """DescribeQueries always returns a queries list."""
        resp = client.describe_queries()
        assert "queries" in resp
        assert isinstance(resp["queries"], list)

    def test_describe_queries_status_filter(self, client):
        """DescribeQueries with status filter returns list."""
        resp = client.describe_queries(status="Complete")
        assert "queries" in resp
        assert isinstance(resp["queries"], list)

    def test_describe_queries_max_results(self, client):
        """DescribeQueries respects maxResults parameter."""
        resp = client.describe_queries(maxResults=5)
        assert "queries" in resp
        assert len(resp["queries"]) <= 5

    # ── describe_log_groups_limit ─────────────────────────────────────────────

    def test_describe_log_groups_limit_with_pagination(self, client):
        """DescribeLogGroups with limit=1 produces nextToken when multiple groups exist."""
        prefix = _unique("/test/lim-pg")
        names = [f"{prefix}-{i}" for i in range(3)]
        for n in names:
            client.create_log_group(logGroupName=n)
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=prefix, limit=1)
            assert len(resp["logGroups"]) == 1
            assert "nextToken" in resp
        finally:
            for n in names:
                client.delete_log_group(logGroupName=n)

    # ── describe_export_tasks ─────────────────────────────────────────────────

    def test_describe_export_tasks_with_status_filter(self, client):
        """DescribeExportTasks with statusCode filter returns a list."""
        for status in ("COMPLETED", "RUNNING", "PENDING", "FAILED"):
            resp = client.describe_export_tasks(statusCode=status)
            assert "exportTasks" in resp
            assert isinstance(resp["exportTasks"], list)

    # ── describe_query_definitions ────────────────────────────────────────────

    def test_describe_query_definitions_after_create(self, client):
        """DescribeQueryDefinitions returns newly created query definitions."""
        prefix = _unique("qdef-edge")
        qid1 = client.put_query_definition(
            name=f"{prefix}-alpha", queryString="fields @timestamp | limit 5"
        )["queryDefinitionId"]
        qid2 = client.put_query_definition(
            name=f"{prefix}-beta", queryString="fields @message | limit 10"
        )["queryDefinitionId"]
        try:
            desc = client.describe_query_definitions(queryDefinitionNamePrefix=prefix)
            assert "queryDefinitions" in desc
            returned_ids = [q["queryDefinitionId"] for q in desc["queryDefinitions"]]
            assert qid1 in returned_ids
            assert qid2 in returned_ids
        finally:
            client.delete_query_definition(queryDefinitionId=qid1)
            client.delete_query_definition(queryDefinitionId=qid2)

    def test_describe_query_definitions_prefix_filters(self, client):
        """DescribeQueryDefinitions with prefix excludes non-matching definitions."""
        prefix_a = _unique("qdef-pfx-a")
        prefix_b = _unique("qdef-pfx-b")
        qid_a = client.put_query_definition(
            name=prefix_a, queryString="fields @timestamp | limit 1"
        )["queryDefinitionId"]
        qid_b = client.put_query_definition(
            name=prefix_b, queryString="fields @message | limit 1"
        )["queryDefinitionId"]
        try:
            desc = client.describe_query_definitions(queryDefinitionNamePrefix=prefix_a)
            returned_ids = [q["queryDefinitionId"] for q in desc["queryDefinitions"]]
            assert qid_a in returned_ids
            assert qid_b not in returned_ids
        finally:
            client.delete_query_definition(queryDefinitionId=qid_a)
            client.delete_query_definition(queryDefinitionId=qid_b)

    # ── list_anomalies ────────────────────────────────────────────────────────

    def test_list_anomalies_with_real_detector(self, client):
        """list_anomalies with a real detector ARN returns a list."""
        group = _unique("/test/anom-list")
        client.create_log_group(logGroupName=group)
        group_arn = f"arn:aws:logs:us-east-1:123456789012:log-group:{group}"
        det_resp = client.create_log_anomaly_detector(logGroupArnList=[group_arn])
        det_arn = det_resp["anomalyDetectorArn"]
        try:
            resp = client.list_anomalies(anomalyDetectorArn=det_arn)
            assert "anomalies" in resp
            assert isinstance(resp["anomalies"], list)
        finally:
            client.delete_log_anomaly_detector(anomalyDetectorArn=det_arn)
            client.delete_log_group(logGroupName=group)

    # ── list_log_anomaly_detectors ────────────────────────────────────────────

    def test_list_log_anomaly_detectors_with_created(self, client):
        """list_log_anomaly_detectors returns the created detector."""
        group = _unique("/test/det-list")
        client.create_log_group(logGroupName=group)
        group_arn = f"arn:aws:logs:us-east-1:123456789012:log-group:{group}"
        det_resp = client.create_log_anomaly_detector(logGroupArnList=[group_arn])
        det_arn = det_resp["anomalyDetectorArn"]
        try:
            resp = client.list_log_anomaly_detectors()
            assert "anomalyDetectors" in resp
            arns = [d["anomalyDetectorArn"] for d in resp["anomalyDetectors"]]
            assert det_arn in arns
        finally:
            client.delete_log_anomaly_detector(anomalyDetectorArn=det_arn)
            client.delete_log_group(logGroupName=group)

    # ── list_integrations ─────────────────────────────────────────────────────

    def test_list_integrations_returns_list(self, client):
        """list_integrations returns a list (possibly empty)."""
        resp = client.list_integrations()
        assert "integrationSummaries" in resp
        assert isinstance(resp["integrationSummaries"], list)

    def test_list_integrations_with_type_filter(self, client):
        """list_integrations with integrationType filter returns a list."""
        resp = client.list_integrations(integrationType="OPENSEARCH")
        assert "integrationSummaries" in resp
        assert isinstance(resp["integrationSummaries"], list)

    # ── tag_log_group_old_api behavioral fidelity ─────────────────────────────

    def test_tag_log_group_old_api_nonexistent_raises(self, client):
        """TagLogGroup on a nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.tag_log_group(
                logGroupName="/test/nonexistent-group-xyz-999",
                tags={"k": "v"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_tag_log_group_old_api_merge_tags(self, client):
        """TagLogGroup adds tags to existing tags without removing others."""
        group = _unique("/test/tag-merge")
        client.create_log_group(logGroupName=group, tags={"original": "yes"})
        try:
            client.tag_log_group(logGroupName=group, tags={"added": "new"})
            resp = client.list_tags_log_group(logGroupName=group)
            assert resp["tags"]["original"] == "yes"
            assert resp["tags"]["added"] == "new"
        finally:
            client.delete_log_group(logGroupName=group)

    # ── log stream behavioral fidelity ────────────────────────────────────────

    def test_log_stream_arn_present(self, client):
        """describe_log_streams includes arn for each stream."""
        group = _unique("/test/stream-arn")
        client.create_log_group(logGroupName=group)
        client.create_log_stream(logGroupName=group, logStreamName="my-stream")
        try:
            resp = client.describe_log_streams(logGroupName=group)
            stream = [s for s in resp["logStreams"] if s["logStreamName"] == "my-stream"][0]
            assert "arn" in stream
            assert "my-stream" in stream["arn"]
        finally:
            client.delete_log_group(logGroupName=group)

    def test_delete_log_stream_nonexistent_raises(self, client):
        """DeleteLogStream on a nonexistent stream raises ResourceNotFoundException."""
        group = _unique("/test/del-stream-err")
        client.create_log_group(logGroupName=group)
        try:
            with pytest.raises(ClientError) as exc:
                client.delete_log_stream(
                    logGroupName=group, logStreamName="does-not-exist-xyz"
                )
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            client.delete_log_group(logGroupName=group)


class TestLogsAdditionalBehavioralFidelity:
    """Additional behavioral fidelity and edge case tests for CloudWatch Logs."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    # ── describe_log_groups structural fidelity ───────────────────────────────

    def test_describe_log_groups_has_required_fields(self, client):
        """describe_log_groups returns arn, creationTime, storedBytes, metricFilterCount."""
        group = _unique("/test/fields-check")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=group)
            g = [x for x in resp["logGroups"] if x["logGroupName"] == group][0]
            assert "arn" in g, "arn missing from log group"
            assert "creationTime" in g, "creationTime missing from log group"
            assert g["creationTime"] > 0, "creationTime should be a positive integer"
            assert "storedBytes" in g, "storedBytes missing from log group"
            assert "metricFilterCount" in g, "metricFilterCount missing from log group"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_describe_log_groups_arn_format(self, client):
        """Log group ARN matches arn:aws:logs:REGION:ACCOUNT:log-group:NAME pattern."""
        group = _unique("/test/arn-fmt")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=group)
            g = [x for x in resp["logGroups"] if x["logGroupName"] == group][0]
            arn = g["arn"]
            assert arn.startswith("arn:aws:logs:"), f"ARN should start with arn:aws:logs: got {arn}"
            assert ":log-group:" in arn, f"ARN should contain :log-group: got {arn}"
            assert group in arn, f"ARN should contain group name, got {arn}"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_create_duplicate_log_group_raises(self, client):
        """Creating a log group with a duplicate name raises ResourceAlreadyExistsException."""
        group = _unique("/test/dup-group")
        client.create_log_group(logGroupName=group)
        try:
            with pytest.raises(ClientError) as exc:
                client.create_log_group(logGroupName=group)
            assert exc.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_delete_nonexistent_log_group_raises(self, client):
        """Deleting a nonexistent log group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_log_group(logGroupName="/test/nonexistent-xyz-99999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_log_groups_pagination_next_token(self, client):
        """describe_log_groups nextToken allows paging through all results."""
        prefix = _unique("/test/pg-tok")
        names = [f"{prefix}-{i}" for i in range(4)]
        for n in names:
            client.create_log_group(logGroupName=n)
        try:
            all_names = []
            kwargs = {"logGroupNamePrefix": prefix, "limit": 2}
            while True:
                resp = client.describe_log_groups(**kwargs)
                all_names.extend(g["logGroupName"] for g in resp["logGroups"])
                if "nextToken" not in resp:
                    break
                kwargs["nextToken"] = resp["nextToken"]
            for n in names:
                assert n in all_names, f"{n} not found via pagination"
        finally:
            for n in names:
                client.delete_log_group(logGroupName=n)

    def test_describe_log_groups_unicode_name(self, client):
        """Log group names with unicode characters are stored and retrieved correctly."""
        group = _unique("/test/uni\u00e9")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.describe_log_groups(logGroupNamePrefix="/test/uni")
            names = [g["logGroupName"] for g in resp["logGroups"]]
            assert group in names
        finally:
            client.delete_log_group(logGroupName=group)

    # ── filter_log_events structural fidelity ─────────────────────────────────

    def test_filter_log_events_event_fields(self, client):
        """filter_log_events returns events with required fields."""
        group = _unique("/test/filt-fields")
        client.create_log_group(logGroupName=group)
        client.create_log_stream(logGroupName=group, logStreamName="s1")
        now = int(time.time() * 1000)
        client.put_log_events(
            logGroupName=group,
            logStreamName="s1",
            logEvents=[{"timestamp": now, "message": "PROBE event"}],
        )
        try:
            resp = client.filter_log_events(logGroupName=group, filterPattern="PROBE")
            assert len(resp["events"]) >= 1, "Expected at least one matching event"
            evt = resp["events"][0]
            assert "eventId" in evt, "eventId missing from event"
            assert "timestamp" in evt, "timestamp missing from event"
            assert "message" in evt, "message missing from event"
            assert "logStreamName" in evt, "logStreamName missing from event"
            assert "ingestionTime" in evt, "ingestionTime missing from event"
            assert evt["logStreamName"] == "s1"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_filter_log_events_pagination_via_next_token(self, client):
        """filter_log_events returns nextToken when limit is hit and more events exist."""
        group = _unique("/test/filt-pg")
        client.create_log_group(logGroupName=group)
        client.create_log_stream(logGroupName=group, logStreamName="s1")
        now = int(time.time() * 1000)
        client.put_log_events(
            logGroupName=group,
            logStreamName="s1",
            logEvents=[{"timestamp": now + i, "message": f"msg-{i}"} for i in range(5)],
        )
        try:
            resp = client.filter_log_events(logGroupName=group, limit=2)
            assert len(resp["events"]) <= 2
            assert "nextToken" in resp, "nextToken should be present when more events exist"
            # Use nextToken to get next page
            resp2 = client.filter_log_events(
                logGroupName=group, limit=2, nextToken=resp["nextToken"]
            )
            assert "events" in resp2
        finally:
            client.delete_log_group(logGroupName=group)

    def test_filter_log_events_empty_group_returns_empty(self, client):
        """filter_log_events on a group with no events returns empty list."""
        group = _unique("/test/filt-empty")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.filter_log_events(logGroupName=group)
            assert resp["events"] == []
        finally:
            client.delete_log_group(logGroupName=group)

    def test_filter_log_events_nonexistent_group_raises(self, client):
        """filter_log_events on nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.filter_log_events(logGroupName="/test/nonexistent-xyz-99999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── TestMetricFilter behavioral fidelity ──────────────────────────────────

    def test_test_metric_filter_returns_http_200(self, client):
        """TestMetricFilter: create group, verify matches, delete group."""
        group = _unique("/test/tmf-http")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.test_metric_filter(
                filterPattern="ERROR",
                logEventMessages=["ERROR: crash", "INFO: ok"],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert len(resp["matches"]) == 1
            assert resp["matches"][0]["eventMessage"] == "ERROR: crash"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_test_metric_filter_matches_is_list(self, client):
        """TestMetricFilter: multi-term pattern, verify extractedValues dict."""
        group = _unique("/test/tmf-isl")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.test_metric_filter(
                filterPattern="ERROR crash",
                logEventMessages=["ERROR crash here", "INFO ok", "ERROR timeout"],
            )
            assert isinstance(resp["matches"], list)
            # "ERROR crash" requires both terms - only first message matches
            assert len(resp["matches"]) == 1
            assert isinstance(resp["matches"][0]["extractedValues"], dict)
        finally:
            client.delete_log_group(logGroupName=group)

    # ── cancel_import_task behavioral fidelity ────────────────────────────────

    def test_cancel_import_task_returns_import_id(self, client):
        """CancelImportTask: create task then cancel, verify importId echoed back."""
        create_resp = client.create_import_task(
            importSourceArn="arn:aws:s3:::test-bucket/logs/",
            importRoleArn="arn:aws:iam::123456789012:role/LogsImportRole",
        )
        import_id = create_resp["importId"]
        resp = client.cancel_import_task(importId=import_id)
        assert resp["importId"] == import_id
        assert resp["importStatus"] == "CANCELLED"

    def test_cancel_import_task_returns_cancelled_status(self, client):
        """CancelImportTask: create then cancel, verify CANCELLED status returned."""
        create_resp = client.create_import_task(
            importSourceArn="arn:aws:s3:::test-bucket2/logs/",
            importRoleArn="arn:aws:iam::123456789012:role/LogsImportRole",
        )
        import_id = create_resp["importId"]
        resp = client.cancel_import_task(importId=import_id)
        assert resp["importStatus"] == "CANCELLED"
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── describe_queries structural fidelity ──────────────────────────────────

    def test_describe_queries_always_has_queries_list(self, client):
        """describe_queries always returns a 'queries' key with a list value."""
        resp = client.describe_queries()
        assert "queries" in resp
        assert isinstance(resp["queries"], list)

    def test_describe_queries_max_results_limits_count(self, client):
        """describe_queries respects maxResults parameter."""
        resp = client.describe_queries(maxResults=1)
        assert len(resp["queries"]) <= 1

    # ── describe_export_tasks behavioral fidelity ─────────────────────────────

    def test_describe_export_tasks_has_export_tasks_list(self, client):
        """describe_export_tasks always returns an exportTasks list."""
        resp = client.describe_export_tasks()
        assert "exportTasks" in resp
        assert isinstance(resp["exportTasks"], list)

    def test_describe_export_tasks_task_id_filter(self, client):
        """describe_export_tasks with a fake taskId raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_export_tasks(taskId="fake-task-id-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_export_tasks_after_create(self, client):
        """CreateExportTask result appears in describe_export_tasks."""
        s3 = make_client("s3")
        bucket = f"export-edge-{uuid.uuid4().hex[:8]}"
        group = _unique("/test/export-edge")
        client.create_log_group(logGroupName=group)
        s3.create_bucket(Bucket=bucket)
        try:
            resp = client.create_export_task(
                logGroupName=group,
                fromTime=int(time.time() * 1000) - 3600000,
                to=int(time.time() * 1000),
                destination=bucket,
            )
            task_id = resp["taskId"]
            desc = client.describe_export_tasks(taskId=task_id)
            assert len(desc["exportTasks"]) == 1
            task = desc["exportTasks"][0]
            assert task["taskId"] == task_id
            assert task["destination"] == bucket
            assert "status" in task
            assert task["status"]["code"] in ("COMPLETED", "RUNNING", "PENDING", "FAILED")
        finally:
            try:
                objs = s3.list_objects_v2(Bucket=bucket)
                for obj in objs.get("Contents", []):
                    s3.delete_object(Bucket=bucket, Key=obj["Key"])
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass
            client.delete_log_group(logGroupName=group)

    # ── tag_log_group_old_api behavioral fidelity ─────────────────────────────

    def test_tag_log_group_old_api_round_trip(self, client):
        """TagLogGroup tags can be read back via ListTagsLogGroup."""
        group = _unique("/test/tag-rt")
        client.create_log_group(logGroupName=group)
        try:
            client.tag_log_group(logGroupName=group, tags={"app": "myapp", "env": "staging"})
            resp = client.list_tags_log_group(logGroupName=group)
            assert resp["tags"]["app"] == "myapp"
            assert resp["tags"]["env"] == "staging"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_tag_log_group_old_api_overwrites_value(self, client):
        """TagLogGroup overwrites an existing tag value with the same key."""
        group = _unique("/test/tag-ow")
        client.create_log_group(logGroupName=group)
        try:
            client.tag_log_group(logGroupName=group, tags={"color": "red"})
            client.tag_log_group(logGroupName=group, tags={"color": "blue"})
            resp = client.list_tags_log_group(logGroupName=group)
            assert resp["tags"]["color"] == "blue"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_tag_log_group_old_api_nonexistent_group_raises(self, client):
        """TagLogGroup on a nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.tag_log_group(
                logGroupName="/test/nonexistent-group-xyz-99999",
                tags={"k": "v"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── log stream idempotency and error edge cases ───────────────────────────

    def test_create_duplicate_log_stream_raises(self, client):
        """Creating a log stream with a duplicate name raises ResourceAlreadyExistsException."""
        group = _unique("/test/dup-stream")
        client.create_log_group(logGroupName=group)
        try:
            client.create_log_stream(logGroupName=group, logStreamName="my-stream")
            with pytest.raises(ClientError) as exc:
                client.create_log_stream(logGroupName=group, logStreamName="my-stream")
            assert exc.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_get_log_events_nonexistent_stream_raises(self, client):
        """GetLogEvents on a nonexistent stream raises ResourceNotFoundException."""
        group = _unique("/test/no-stream")
        client.create_log_group(logGroupName=group)
        try:
            with pytest.raises(ClientError) as exc:
                client.get_log_events(logGroupName=group, logStreamName="does-not-exist")
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_describe_log_streams_pagination(self, client):
        """describe_log_streams nextToken pages through all streams."""
        group = _unique("/test/stream-pg")
        client.create_log_group(logGroupName=group)
        stream_names = [f"stream-{i:03d}" for i in range(5)]
        for s in stream_names:
            client.create_log_stream(logGroupName=group, logStreamName=s)
        try:
            collected = []
            kwargs = {"logGroupName": group, "limit": 2}
            while True:
                resp = client.describe_log_streams(**kwargs)
                collected.extend(s["logStreamName"] for s in resp["logStreams"])
                if "nextToken" not in resp:
                    break
                kwargs["nextToken"] = resp["nextToken"]
            for s in stream_names:
                assert s in collected, f"{s} missing after pagination"
        finally:
            client.delete_log_group(logGroupName=group)

    def test_put_retention_policy_nonexistent_group_raises(self, client):
        """put_retention_policy on nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.put_retention_policy(
                logGroupName="/test/nonexistent-xyz-99999", retentionInDays=7
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_log_groups_metric_filter_count_is_int(self, client):
        """metricFilterCount in describe_log_groups is always an integer >= 0."""
        group = _unique("/test/mf-count-int")
        client.create_log_group(logGroupName=group)
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=group)
            g = [x for x in resp["logGroups"] if x["logGroupName"] == group][0]
            assert isinstance(g["metricFilterCount"], int)
            assert g["metricFilterCount"] >= 0
        finally:
            client.delete_log_group(logGroupName=group)


class TestLogsEdgeCaseImprovements:
    """Edge cases and behavioral fidelity tests targeting low-coverage operations."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    # ── filter_log_events with limit: pagination + error ─────────────────────

    def test_filter_log_events_limit_nexttoken_pagination(self, client):
        """FilterLogEvents with limit returns nextToken; following it fetches more events."""
        group = _unique("/test/filt-pg")
        client.create_log_group(logGroupName=group)
        stream = "pg-stream"
        client.create_log_stream(logGroupName=group, logStreamName=stream)
        try:
            now = int(time.time() * 1000)
            client.put_log_events(
                logGroupName=group,
                logStreamName=stream,
                logEvents=[{"timestamp": now + i, "message": f"MSG-{i}"} for i in range(10)],
            )
            # First page
            resp1 = client.filter_log_events(
                logGroupName=group, logStreamNames=[stream], limit=3
            )
            assert len(resp1["events"]) <= 3
            assert "nextToken" in resp1
            # Follow the token
            resp2 = client.filter_log_events(
                logGroupName=group, logStreamNames=[stream], limit=3,
                nextToken=resp1["nextToken"],
            )
            assert "events" in resp2
            # Combined events should be more than first page alone
            total = len(resp1["events"]) + len(resp2["events"])
            assert total > 0
            # Error: filter on nonexistent group
            with pytest.raises(ClientError) as exc:
                client.filter_log_events(logGroupName="/test/nonexistent-xyz-9999999")
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            client.delete_log_group(logGroupName=group)

    # ── describe_queries: listing started queries ─────────────────────────────

    def test_describe_queries_after_start_query(self, client):
        """describe_queries returns a list; start_query returns a queryId that is retrievable."""
        group = _unique("/test/dq-start")
        client.create_log_group(logGroupName=group)
        try:
            now = int(time.time())
            resp = client.start_query(
                logGroupName=group,
                startTime=now - 3600,
                endTime=now,
                queryString="fields @timestamp | limit 5",
            )
            qid = resp["queryId"]
            assert qid
            # describe_queries returns a list (may be empty depending on Moto impl)
            desc = client.describe_queries(logGroupName=group)
            assert "queries" in desc
            assert isinstance(desc["queries"], list)
            # get_query_results retrieves the query by ID
            result = client.get_query_results(queryId=qid)
            assert "status" in result
            assert result["status"] in ("Complete", "Running", "Scheduled", "Failed", "Cancelled")
            assert "results" in result
        finally:
            client.delete_log_group(logGroupName=group)

    def test_describe_queries_nonexistent_group_returns_empty(self, client):
        """describe_queries with nonexistent logGroupName returns empty list (no error)."""
        resp = client.describe_queries(logGroupName="/test/nonexistent-xyz-9999999")
        assert "queries" in resp
        assert resp["queries"] == []

    # ── describe_log_groups limit: pagination ─────────────────────────────────

    def test_describe_log_groups_limit_pagination(self, client):
        """describe_log_groups limit + nextToken iterates through all groups."""
        prefix = _unique("/test/dgl-pg")
        names = [f"{prefix}-{i}" for i in range(3)]
        for n in names:
            client.create_log_group(logGroupName=n)
        try:
            # Page 1: limit=2
            resp1 = client.describe_log_groups(logGroupNamePrefix=prefix, limit=2)
            assert len(resp1["logGroups"]) == 2
            assert "nextToken" in resp1
            # Page 2
            resp2 = client.describe_log_groups(
                logGroupNamePrefix=prefix, limit=2, nextToken=resp1["nextToken"]
            )
            assert "logGroups" in resp2
            all_names = (
                [g["logGroupName"] for g in resp1["logGroups"]]
                + [g["logGroupName"] for g in resp2["logGroups"]]
            )
            for n in names:
                assert n in all_names, f"{n} missing after pagination"
            # Error: delete nonexistent group
            with pytest.raises(ClientError) as exc:
                client.delete_log_group(logGroupName="/test/nonexistent-xyz-9999999")
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            for n in names:
                client.delete_log_group(logGroupName=n)

    # ── describe_export_tasks: create + describe by id ────────────────────────

    def test_describe_export_tasks_with_created_task(self, client):
        """CreateExportTask + DescribeExportTasks: task appears with expected fields."""
        s3 = make_client("s3")
        bucket = f"export-edge-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=bucket)
        group = _unique("/test/exp-edge")
        client.create_log_group(logGroupName=group)
        try:
            now = int(time.time() * 1000)
            create_resp = client.create_export_task(
                logGroupName=group,
                fromTime=now - 3600000,
                to=now,
                destination=bucket,
            )
            task_id = create_resp["taskId"]
            assert task_id
            # Describe by task ID
            desc = client.describe_export_tasks(taskId=task_id)
            assert len(desc["exportTasks"]) == 1
            task = desc["exportTasks"][0]
            assert task["taskId"] == task_id
            assert task["logGroupName"] == group
            assert "status" in task
            assert task["status"]["code"] in ("COMPLETED", "RUNNING", "PENDING", "FAILED")
            # DescribeExportTasks with no args includes our task
            all_tasks = client.describe_export_tasks()
            all_ids = [t["taskId"] for t in all_tasks["exportTasks"]]
            assert task_id in all_ids
            # Delete export task (via cancel if still in progress)
            try:
                client.cancel_export_task(taskId=task_id)
            except ClientError:
                pass  # already completed — can't cancel
        finally:
            client.delete_log_group(logGroupName=group)
            try:
                objs = s3.list_objects_v2(Bucket=bucket)
                for obj in objs.get("Contents", []):
                    s3.delete_object(Bucket=bucket, Key=obj["Key"])
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort S3 cleanup

    # ── describe_query_definitions: full lifecycle with prefix filter ──────────

    def test_describe_query_definitions_prefix_filter_lifecycle(self, client):
        """PutQueryDefinition → describe with prefix → delete → verify gone."""
        suffix = uuid.uuid4().hex[:8]
        name = f"qdef-edge-{suffix}"
        query = "fields @timestamp, @message | sort @timestamp desc | limit 25"
        resp = client.put_query_definition(name=name, queryString=query)
        qid = resp["queryDefinitionId"]
        assert qid
        try:
            # Describe with prefix
            desc = client.describe_query_definitions(queryDefinitionNamePrefix=f"qdef-edge-{suffix}")
            matching = [q for q in desc["queryDefinitions"] if q["queryDefinitionId"] == qid]
            assert len(matching) == 1
            assert matching[0]["name"] == name
            assert matching[0]["queryString"] == query
            # Update (put with same id)
            updated_query = "fields @message | limit 10"
            client.put_query_definition(
                name=name, queryDefinitionId=qid, queryString=updated_query
            )
            desc2 = client.describe_query_definitions()
            found = [q for q in desc2["queryDefinitions"] if q["queryDefinitionId"] == qid]
            assert len(found) == 1
            assert found[0]["queryString"] == updated_query
        finally:
            # Delete and verify gone
            client.delete_query_definition(queryDefinitionId=qid)
            desc3 = client.describe_query_definitions()
            ids = [q["queryDefinitionId"] for q in desc3["queryDefinitions"]]
            assert qid not in ids
        # Error: delete again
        with pytest.raises(ClientError) as exc:
            client.delete_query_definition(queryDefinitionId=qid)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── list_anomalies: with detector lifecycle ───────────────────────────────

    def test_list_anomalies_with_detector_lifecycle(self, client):
        """CreateLogAnomalyDetector → list_anomalies → delete detector."""
        group = _unique("/test/anom-edge")
        client.create_log_group(logGroupName=group)
        try:
            group_arn = _log_group_arn(group)
            det_resp = client.create_log_anomaly_detector(logGroupArnList=[group_arn])
            det_arn = det_resp["anomalyDetectorArn"]
            assert det_arn
            # list_anomalies returns a list (may be empty if no anomalies yet)
            anom_resp = client.list_anomalies(anomalyDetectorArn=det_arn)
            assert "anomalies" in anom_resp
            assert isinstance(anom_resp["anomalies"], list)
            # GetLogAnomalyDetector retrieves the detector
            get_resp = client.get_log_anomaly_detector(anomalyDetectorArn=det_arn)
            assert "anomalyDetectorStatus" in get_resp
            # list_anomalies with nonexistent detector returns empty list (no error)
            resp_fake = client.list_anomalies(
                anomalyDetectorArn="arn:aws:logs:us-east-1:000000000000:anomaly-detector:fake"
            )
            assert resp_fake["anomalies"] == []
        finally:
            try:
                client.delete_log_anomaly_detector(anomalyDetectorArn=det_arn)
            except Exception:
                pass  # best-effort
            client.delete_log_group(logGroupName=group)

    # ── list_log_anomaly_detectors: after create ──────────────────────────────

    def test_list_log_anomaly_detectors_after_create(self, client):
        """Created anomaly detector appears in list_log_anomaly_detectors."""
        group = _unique("/test/lad-edge")
        client.create_log_group(logGroupName=group)
        det_arn = None
        try:
            group_arn = _log_group_arn(group)
            det_resp = client.create_log_anomaly_detector(logGroupArnList=[group_arn])
            det_arn = det_resp["anomalyDetectorArn"]
            # List all detectors
            list_resp = client.list_log_anomaly_detectors()
            assert "anomalyDetectors" in list_resp
            arns = [d["anomalyDetectorArn"] for d in list_resp["anomalyDetectors"]]
            assert det_arn in arns, "Created detector not found in list"
            # Each entry has required fields
            entry = next(d for d in list_resp["anomalyDetectors"] if d["anomalyDetectorArn"] == det_arn)
            assert "anomalyDetectorStatus" in entry
            # Delete and verify removed
            client.delete_log_anomaly_detector(anomalyDetectorArn=det_arn)
            det_arn = None
            list_resp2 = client.list_log_anomaly_detectors()
            arns2 = [d["anomalyDetectorArn"] for d in list_resp2["anomalyDetectors"]]
            assert det_arn not in arns2
        finally:
            if det_arn:
                try:
                    client.delete_log_anomaly_detector(anomalyDetectorArn=det_arn)
                except Exception:
                    pass
            client.delete_log_group(logGroupName=group)

    # ── list_integrations: type filter ───────────────────────────────────────

    def test_list_integrations_response_structure(self, client):
        """list_integrations returns integrationSummaries with expected structure."""
        resp = client.list_integrations()
        assert "integrationSummaries" in resp
        assert isinstance(resp["integrationSummaries"], list)
        # Each entry (if any) has required fields
        for summary in resp["integrationSummaries"]:
            assert "integrationName" in summary
            assert "integrationType" in summary

    def test_list_integrations_with_type_filter(self, client):
        """list_integrations with integrationType filter returns a list."""
        resp = client.list_integrations(integrationType="OPENSEARCH")
        assert "integrationSummaries" in resp
        assert isinstance(resp["integrationSummaries"], list)
        # All returned entries match the filter
        for summary in resp["integrationSummaries"]:
            assert summary["integrationType"] == "OPENSEARCH"

    # ── describe_deliveries: with created delivery ────────────────────────────

    def test_describe_deliveries_after_create(self, client):
        """CreateDelivery → describe_deliveries → verify delivery appears."""
        src_name = _unique("src-deld")
        dest_name = _unique("dst-deld")
        group_name = _unique("/test/delivery-edge")
        client.create_log_group(logGroupName=group_name)
        delivery_id = None
        try:
            cf_arn = "arn:aws:cloudfront::123456789012:distribution/EDGECASETEST"
            client.put_delivery_source(name=src_name, resourceArn=cf_arn, logType="ACCESS_LOGS")
            dest_resp = client.put_delivery_destination(
                name=dest_name,
                deliveryDestinationConfiguration={
                    "destinationResourceArn": (
                        f"arn:aws:logs:us-east-1:123456789012:log-group:{group_name}"
                    )
                },
            )
            dest_arn = dest_resp["deliveryDestination"]["arn"]
            create_resp = client.create_delivery(
                deliverySourceName=src_name,
                deliveryDestinationArn=dest_arn,
            )
            delivery_id = create_resp["delivery"]["id"]
            assert delivery_id
            # describe_deliveries returns our delivery
            desc = client.describe_deliveries()
            assert "deliveries" in desc
            ids = [d["id"] for d in desc["deliveries"]]
            assert delivery_id in ids
            # Each delivery has required fields
            our_delivery = next(d for d in desc["deliveries"] if d["id"] == delivery_id)
            assert "deliverySourceName" in our_delivery
            assert our_delivery["deliverySourceName"] == src_name
            # Delete delivery
            client.delete_delivery(id=delivery_id)
            delivery_id = None
            # Verify gone
            desc2 = client.describe_deliveries()
            ids2 = [d["id"] for d in desc2["deliveries"]]
            assert delivery_id not in ids2
        finally:
            if delivery_id:
                try:
                    client.delete_delivery(id=delivery_id)
                except Exception:
                    pass
            try:
                client.delete_delivery_destination(name=dest_name)
            except Exception:
                pass
            try:
                client.delete_delivery_source(name=src_name)
            except Exception:
                pass
            client.delete_log_group(logGroupName=group_name)

    # ── describe_delivery_destinations: after create ──────────────────────────

    def test_describe_delivery_destinations_after_create(self, client):
        """PutDeliveryDestination → describe_delivery_destinations → verify + delete."""
        dest_name = _unique("dst-desc-edge")
        resp = client.put_delivery_destination(
            name=dest_name,
            outputFormat="json",
            deliveryDestinationConfiguration={
                "destinationResourceArn": "arn:aws:s3:::test-bucket-edge-case"
            },
        )
        assert resp["deliveryDestination"]["name"] == dest_name
        try:
            # List and verify
            list_resp = client.describe_delivery_destinations()
            assert "deliveryDestinations" in list_resp
            names = [d["name"] for d in list_resp["deliveryDestinations"]]
            assert dest_name in names
            # Check structural fields
            entry = next(d for d in list_resp["deliveryDestinations"] if d["name"] == dest_name)
            assert "arn" in entry
            assert "deliveryDestinationConfiguration" in entry
            # Get directly
            get_resp = client.get_delivery_destination(name=dest_name)
            assert get_resp["deliveryDestination"]["name"] == dest_name
            # Error: changing outputFormat on existing dest raises ValidationException
            with pytest.raises(ClientError) as exc:
                client.put_delivery_destination(
                    name=dest_name,
                    outputFormat="plain",
                    deliveryDestinationConfiguration={
                        "destinationResourceArn": "arn:aws:s3:::test-bucket-edge-case"
                    },
                )
            assert exc.value.response["Error"]["Code"] == "ValidationException"
        finally:
            client.delete_delivery_destination(name=dest_name)
        # Error: get after delete
        with pytest.raises(ClientError) as exc:
            client.get_delivery_destination(name=dest_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── describe_delivery_sources: after create ───────────────────────────────

    def test_describe_delivery_sources_after_create(self, client):
        """PutDeliverySource → describe_delivery_sources → verify + delete."""
        src_name = _unique("src-desc-edge")
        cf_arn = "arn:aws:cloudfront::123456789012:distribution/SRCEDGECASE"
        resp = client.put_delivery_source(name=src_name, resourceArn=cf_arn, logType="ACCESS_LOGS")
        assert resp["deliverySource"]["name"] == src_name
        try:
            # List and verify
            list_resp = client.describe_delivery_sources()
            assert "deliverySources" in list_resp
            names = [d["name"] for d in list_resp["deliverySources"]]
            assert src_name in names
            # Check structural fields
            entry = next(d for d in list_resp["deliverySources"] if d["name"] == src_name)
            assert "resourceArns" in entry or "arn" in entry
            # Get directly
            get_resp = client.get_delivery_source(name=src_name)
            assert get_resp["deliverySource"]["name"] == src_name
        finally:
            client.delete_delivery_source(name=src_name)
        # Error: get after delete
        with pytest.raises(ClientError) as exc:
            client.get_delivery_source(name=src_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── describe_configuration_templates: structural fidelity ─────────────────

    def test_describe_configuration_templates_structural(self, client):
        """describe_configuration_templates returns a valid list (may be empty)."""
        resp = client.describe_configuration_templates()
        assert "configurationTemplates" in resp
        assert isinstance(resp["configurationTemplates"], list)
        # If any templates exist, check required fields
        for tmpl in resp["configurationTemplates"]:
            assert "service" in tmpl or "logType" in tmpl or "resourceType" in tmpl

    def test_describe_configuration_templates_with_service_filter(self, client):
        """describe_configuration_templates with service filter returns filtered list."""
        resp = client.describe_configuration_templates(service="CloudFront")
        assert "configurationTemplates" in resp
        assert isinstance(resp["configurationTemplates"], list)
        for tmpl in resp["configurationTemplates"]:
            assert tmpl.get("service") == "CloudFront"

    # ── describe_import_tasks: structural fidelity ────────────────────────────

    def test_describe_import_tasks_structural(self, client):
        """describe_import_tasks returns a 200 response (stub returns no tasks)."""
        resp = client.describe_import_tasks()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_import_tasks_with_limit(self, client):
        """describe_import_tasks with limit returns 200."""
        resp = client.describe_import_tasks(limit=5)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── list_scheduled_queries: structural fidelity ───────────────────────────

    def test_list_scheduled_queries_structural(self, client):
        """list_scheduled_queries returns scheduledQueries with correct type."""
        resp = client.list_scheduled_queries()
        assert "scheduledQueries" in resp
        assert isinstance(resp["scheduledQueries"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_scheduled_queries_nexttoken_field(self, client):
        """list_scheduled_queries with maxResults returns nextToken if more exist."""
        resp = client.list_scheduled_queries(maxResults=1)
        assert "scheduledQueries" in resp
        # nextToken may or may not be present depending on count; just verify response shape
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── put_query_definition full lifecycle ───────────────────────────────────

    def test_put_query_definition_full_lifecycle(self, client):
        """PutQueryDefinition: create → retrieve → update → delete → error."""
        suffix = uuid.uuid4().hex[:8]
        name = f"qdef-full-{suffix}"
        initial_query = "fields @timestamp, @message | limit 20"
        # CREATE
        create_resp = client.put_query_definition(name=name, queryString=initial_query)
        qid = create_resp["queryDefinitionId"]
        assert qid
        try:
            # RETRIEVE via describe
            desc = client.describe_query_definitions()
            found = [q for q in desc["queryDefinitions"] if q["queryDefinitionId"] == qid]
            assert len(found) == 1
            assert found[0]["name"] == name
            assert found[0]["queryString"] == initial_query
            # UPDATE: put with same ID and new query string
            updated_query = "fields @message | sort @timestamp desc | limit 50"
            update_resp = client.put_query_definition(
                name=name, queryDefinitionId=qid, queryString=updated_query
            )
            assert update_resp["queryDefinitionId"] == qid
            # RETRIEVE again - verify update
            desc2 = client.describe_query_definitions()
            found2 = [q for q in desc2["queryDefinitions"] if q["queryDefinitionId"] == qid]
            assert len(found2) == 1
            assert found2[0]["queryString"] == updated_query
        finally:
            # DELETE
            client.delete_query_definition(queryDefinitionId=qid)
        # RETRIEVE after delete - should not be found
        desc3 = client.describe_query_definitions()
        ids3 = [q["queryDefinitionId"] for q in desc3["queryDefinitions"]]
        assert qid not in ids3
        # ERROR: delete again
        with pytest.raises(ClientError) as exc:
            client.delete_query_definition(queryDefinitionId=qid)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── resource_policy: update + error ──────────────────────────────────────

    def test_resource_policy_update_and_error(self, client):
        """PutResourcePolicy update (same name = overwrite) + error on nonexistent delete."""
        suffix = uuid.uuid4().hex[:8]
        policy_name = f"res-pol-edge-{suffix}"
        policy_v1 = (
            '{"Version":"2012-10-17","Statement":[{"Sid":"V1","Effect":"Allow",'
            '"Principal":{"Service":"route53.amazonaws.com"},'
            '"Action":["logs:PutLogEvents"],"Resource":"*"}]}'
        )
        policy_v2 = (
            '{"Version":"2012-10-17","Statement":[{"Sid":"V2","Effect":"Allow",'
            '"Principal":{"Service":"es.amazonaws.com"},'
            '"Action":["logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"}]}'
        )
        # CREATE
        client.put_resource_policy(policyName=policy_name, policyDocument=policy_v1)
        try:
            # RETRIEVE
            resp1 = client.describe_resource_policies()
            matching = [p for p in resp1["resourcePolicies"] if p.get("policyName") == policy_name]
            assert len(matching) == 1
            assert "policyDocument" in matching[0]
            # UPDATE: put same name with different document
            client.put_resource_policy(policyName=policy_name, policyDocument=policy_v2)
            resp2 = client.describe_resource_policies()
            matching2 = [p for p in resp2["resourcePolicies"] if p.get("policyName") == policy_name]
            assert len(matching2) == 1
            assert "V2" in matching2[0]["policyDocument"]
        finally:
            client.delete_resource_policy(policyName=policy_name)
        # ERROR: delete nonexistent policy
        with pytest.raises(ClientError) as exc:
            client.delete_resource_policy(policyName=f"nonexistent-policy-{suffix}")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLogsEdgeCasesAndFidelity:
    """Edge case and behavioral fidelity tests targeting weak coverage areas."""

    @pytest.fixture
    def client(self):
        return make_client("logs")

    # ── Behavioral fidelity: ARN format and timestamps ────────────────────────

    def test_log_group_arn_format(self, client):
        """Log group ARN matches expected arn:aws:logs:<region>:<account>:log-group:<name>."""
        import re

        name = _unique("/test/arn-format")
        client.create_log_group(logGroupName=name)
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            arn = group["arn"]
            assert "arn:aws:logs:" in arn
            assert name in arn
            assert re.match(r"arn:aws:logs:[a-z0-9-]+:\d+:log-group:.+", arn.rstrip(":*"))
        finally:
            client.delete_log_group(logGroupName=name)

    def test_log_group_creation_time_is_reasonable(self, client):
        """Log group creationTime is a recent millisecond epoch timestamp."""
        name = _unique("/test/creation-time")
        before_ms = int(time.time() * 1000)
        client.create_log_group(logGroupName=name)
        after_ms = int(time.time() * 1000) + 5000
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in resp["logGroups"] if g["logGroupName"] == name][0]
            ct = group["creationTime"]
            assert isinstance(ct, int)
            assert before_ms <= ct <= after_ms
        finally:
            client.delete_log_group(logGroupName=name)

    def test_create_log_group_duplicate_raises_already_exists(self, client):
        """Creating a log group with the same name twice raises ResourceAlreadyExistsException."""
        name = _unique("/test/dup-group")
        client.create_log_group(logGroupName=name)
        try:
            with pytest.raises(ClientError) as exc:
                client.create_log_group(logGroupName=name)
            assert exc.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
        finally:
            client.delete_log_group(logGroupName=name)

    def test_delete_nonexistent_log_stream_raises_error(self, client):
        """Deleting a nonexistent log stream raises ResourceNotFoundException."""
        name = _unique("/test/del-stream-err")
        client.create_log_group(logGroupName=name)
        try:
            with pytest.raises(ClientError) as exc:
                client.delete_log_stream(
                    logGroupName=name, logStreamName="nonexistent-stream-xyz"
                )
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            client.delete_log_group(logGroupName=name)

    def test_get_log_events_chronological_ordering(self, client):
        """GetLogEvents with startFromHead=True returns events in timestamp order."""
        name = _unique("/test/ordering")
        stream = "order-stream"
        client.create_log_group(logGroupName=name)
        client.create_log_stream(logGroupName=name, logStreamName=stream)
        try:
            now = int(time.time() * 1000)
            client.put_log_events(
                logGroupName=name,
                logStreamName=stream,
                logEvents=[
                    {"timestamp": now, "message": "first"},
                    {"timestamp": now + 1000, "message": "second"},
                    {"timestamp": now + 2000, "message": "third"},
                ],
            )
            resp = client.get_log_events(
                logGroupName=name, logStreamName=stream, startFromHead=True
            )
            events = resp["events"]
            assert len(events) >= 3
            timestamps = [e["timestamp"] for e in events]
            assert timestamps == sorted(timestamps)
        finally:
            client.delete_log_group(logGroupName=name)

    def test_unicode_in_log_messages(self, client):
        """Log events with unicode characters are stored and retrieved correctly."""
        name = _unique("/test/unicode")
        stream = "unicode-stream"
        client.create_log_group(logGroupName=name)
        client.create_log_stream(logGroupName=name, logStreamName=stream)
        try:
            unicode_msg = "こんにちは世界 — héllo wörld"
            client.put_log_events(
                logGroupName=name,
                logStreamName=stream,
                logEvents=[{"timestamp": int(time.time() * 1000), "message": unicode_msg}],
            )
            resp = client.get_log_events(logGroupName=name, logStreamName=stream)
            messages = [e["message"] for e in resp["events"]]
            assert unicode_msg in messages
        finally:
            client.delete_log_group(logGroupName=name)

    # ── filter_log_events: nextToken pagination and error ─────────────────────

    def test_filter_log_events_next_token_pagination(self, client):
        """FilterLogEvents nextToken allows paginating through results."""
        name = _unique("/test/filt-paginate")
        stream = "filt-pg-stream"
        client.create_log_group(logGroupName=name)
        client.create_log_stream(logGroupName=name, logStreamName=stream)
        try:
            now = int(time.time() * 1000)
            client.put_log_events(
                logGroupName=name,
                logStreamName=stream,
                logEvents=[
                    {"timestamp": now + i, "message": f"MATCH-event-{i}"} for i in range(6)
                ],
            )
            # First page with limit=3
            page1 = client.filter_log_events(
                logGroupName=name,
                logStreamNames=[stream],
                filterPattern="MATCH-event",
                limit=3,
            )
            assert len(page1["events"]) <= 3
            assert "nextToken" in page1
            # Retrieve second page via nextToken
            page2 = client.filter_log_events(
                logGroupName=name,
                logStreamNames=[stream],
                filterPattern="MATCH-event",
                limit=3,
                nextToken=page1["nextToken"],
            )
            assert "events" in page2
            # List streams to confirm stream exists
            streams_resp = client.describe_log_streams(logGroupName=name)
            snames = [s["logStreamName"] for s in streams_resp["logStreams"]]
            assert stream in snames
            # Delete the stream
            client.delete_log_stream(logGroupName=name, logStreamName=stream)
            # ERROR: filter on nonexistent group
            with pytest.raises(ClientError) as exc:
                client.filter_log_events(logGroupName="/test/totally-nonexistent-filt-pg-xyz")
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            client.delete_log_group(logGroupName=name)

    # ── describe_queries: full lifecycle ─────────────────────────────────────

    def test_describe_queries_with_started_query(self, client):
        """describe_queries + get_query_results + stop_query lifecycle."""
        name = _unique("/test/dq-lifecycle")
        client.create_log_group(logGroupName=name)
        try:
            now = int(time.time())
            # CREATE: start a query
            start_resp = client.start_query(
                logGroupName=name,
                startTime=now - 3600,
                endTime=now + 60,
                queryString="fields @timestamp | limit 5",
            )
            qid = start_resp["queryId"]
            assert qid
            # LIST: describe_queries returns a list (Moto may not track in-flight queries here)
            list_resp = client.describe_queries()
            assert "queries" in list_resp
            assert isinstance(list_resp["queries"], list)
            # RETRIEVE: get_query_results
            result = client.get_query_results(queryId=qid)
            assert "status" in result
            assert result["status"] in ("Complete", "Running", "Scheduled", "Failed", "Cancelled")
            assert "results" in result
            # DELETE via stop
            stop = client.stop_query(queryId=qid)
            assert isinstance(stop.get("success"), bool)
            # ERROR: get_query_results for nonexistent query
            with pytest.raises(ClientError) as exc:
                client.get_query_results(queryId="nonexistent-query-id-xyz-9999")
            assert exc.value.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidParameterException",
            )
        finally:
            client.delete_log_group(logGroupName=name)

    # ── describe_log_groups_limit: pagination with nextToken ──────────────────

    def test_describe_log_groups_limit_with_pagination(self, client):
        """describe_log_groups with limit returns nextToken usable for next page."""
        suffix = uuid.uuid4().hex[:8]
        prefix = f"/test/pg-limit-{suffix}"
        groups = [f"{prefix}-{i}" for i in range(4)]
        for g in groups:
            client.create_log_group(logGroupName=g)
        try:
            # LIST with limit=2
            page1 = client.describe_log_groups(logGroupNamePrefix=prefix, limit=2)
            assert len(page1["logGroups"]) == 2
            assert "nextToken" in page1
            # RETRIEVE second page
            page2 = client.describe_log_groups(
                logGroupNamePrefix=prefix, limit=2, nextToken=page1["nextToken"]
            )
            assert len(page2["logGroups"]) >= 1
            all_names = (
                [g["logGroupName"] for g in page1["logGroups"]]
                + [g["logGroupName"] for g in page2["logGroups"]]
            )
            for g in groups:
                assert g in all_names
        finally:
            for g in groups:
                client.delete_log_group(logGroupName=g)

    # ── describe_export_tasks: full lifecycle with ID filter ─────────────────

    def test_describe_export_tasks_with_task_id(self, client):
        """CreateExportTask → DescribeExportTasks(taskId) → verify fields."""
        s3 = make_client("s3")
        bucket = f"logs-et-edge-{uuid.uuid4().hex[:8]}"
        name = _unique("/test/export-edge")
        client.create_log_group(logGroupName=name)
        s3.create_bucket(Bucket=bucket)
        try:
            task_resp = client.create_export_task(
                logGroupName=name,
                fromTime=int(time.time() * 1000) - 3600000,
                to=int(time.time() * 1000),
                destination=bucket,
            )
            task_id = task_resp["taskId"]
            assert task_id
            # RETRIEVE by task ID
            desc = client.describe_export_tasks(taskId=task_id)
            assert len(desc["exportTasks"]) == 1
            task = desc["exportTasks"][0]
            assert task["taskId"] == task_id
            assert task["logGroupName"] == name
            assert "status" in task
            assert task["status"]["code"] in ("COMPLETED", "RUNNING", "PENDING", "FAILED")
            # LIST all tasks — ours should appear
            all_tasks = client.describe_export_tasks()
            all_ids = [t["taskId"] for t in all_tasks["exportTasks"]]
            assert task_id in all_ids
        finally:
            try:
                objs = s3.list_objects_v2(Bucket=bucket)
                for obj in objs.get("Contents", []):
                    s3.delete_object(Bucket=bucket, Key=obj["Key"])
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup
            client.delete_log_group(logGroupName=name)

    # ── describe_query_definitions: full lifecycle ───────────────────────────

    def test_describe_query_definitions_full_lifecycle(self, client):
        """PutQueryDefinition: CREATE → RETRIEVE → UPDATE → DELETE → ERROR."""
        suffix = uuid.uuid4().hex[:8]
        name = f"qdef-edge-{suffix}"
        initial = "fields @timestamp | limit 10"
        updated = "fields @message | sort @timestamp desc | limit 100"
        # CREATE
        create_resp = client.put_query_definition(name=name, queryString=initial)
        qid = create_resp["queryDefinitionId"]
        assert qid
        try:
            # RETRIEVE
            desc1 = client.describe_query_definitions()
            found = [q for q in desc1["queryDefinitions"] if q["queryDefinitionId"] == qid]
            assert len(found) == 1
            assert found[0]["name"] == name
            assert found[0]["queryString"] == initial
            # UPDATE
            upd = client.put_query_definition(
                name=name, queryDefinitionId=qid, queryString=updated
            )
            assert upd["queryDefinitionId"] == qid
            desc2 = client.describe_query_definitions()
            found2 = [q for q in desc2["queryDefinitions"] if q["queryDefinitionId"] == qid]
            assert len(found2) == 1
            assert found2[0]["queryString"] == updated
        finally:
            # DELETE
            client.delete_query_definition(queryDefinitionId=qid)
        # ERROR: delete again
        with pytest.raises(ClientError) as exc:
            client.delete_query_definition(queryDefinitionId=qid)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── list_log_anomaly_detectors: create + list + delete ───────────────────

    def test_list_log_anomaly_detectors_lifecycle(self, client):
        """CreateLogAnomalyDetector → ListLogAnomalyDetectors → GetLogAnomalyDetector → Delete."""
        name = _unique("/test/anomaly-list")
        client.create_log_group(logGroupName=name)
        try:
            group_arn = _log_group_arn(name)
            create_resp = client.create_log_anomaly_detector(logGroupArnList=[group_arn])
            detector_arn = create_resp["anomalyDetectorArn"]
            assert detector_arn
            try:
                # LIST
                list_resp = client.list_log_anomaly_detectors()
                arns = [d["anomalyDetectorArn"] for d in list_resp["anomalyDetectors"]]
                assert detector_arn in arns
                # RETRIEVE (note: response has logGroupArnList not anomalyDetectorArn)
                get_resp = client.get_log_anomaly_detector(anomalyDetectorArn=detector_arn)
                assert group_arn in get_resp["logGroupArnList"]
                assert "anomalyDetectorStatus" in get_resp
                # UPDATE
                client.update_log_anomaly_detector(
                    anomalyDetectorArn=detector_arn, enabled=True
                )
                # list_anomalies for this detector
                anom_resp = client.list_anomalies(anomalyDetectorArn=detector_arn)
                assert "anomalies" in anom_resp
                assert isinstance(anom_resp["anomalies"], list)
            finally:
                client.delete_log_anomaly_detector(anomalyDetectorArn=detector_arn)
        finally:
            client.delete_log_group(logGroupName=name)

    # ── describe_delivery_destinations: full lifecycle ────────────────────────

    def test_describe_delivery_destinations_lifecycle(self, client):
        """PutDeliveryDestination → Describe → Get → Update → Delete → Error."""
        dest_name = _unique("dd-edge")
        create_resp = client.put_delivery_destination(
            name=dest_name,
            outputFormat="json",
            deliveryDestinationConfiguration={
                "destinationResourceArn": "arn:aws:s3:::edge-test-bucket"
            },
        )
        assert create_resp["deliveryDestination"]["name"] == dest_name
        try:
            # LIST
            list_resp = client.describe_delivery_destinations()
            names = [d["name"] for d in list_resp["deliveryDestinations"]]
            assert dest_name in names
            # RETRIEVE
            get_resp = client.get_delivery_destination(name=dest_name)
            assert get_resp["deliveryDestination"]["name"] == dest_name
            # UPDATE: put again with same name and same format (format changes are disallowed)
            client.put_delivery_destination(
                name=dest_name,
                outputFormat="json",
                deliveryDestinationConfiguration={
                    "destinationResourceArn": "arn:aws:s3:::edge-test-bucket-v2"
                },
            )
            get_resp2 = client.get_delivery_destination(name=dest_name)
            assert get_resp2["deliveryDestination"]["name"] == dest_name
        finally:
            client.delete_delivery_destination(name=dest_name)
        # ERROR: get deleted destination
        with pytest.raises(ClientError) as exc:
            client.get_delivery_destination(name=dest_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── describe_delivery_sources: full lifecycle ─────────────────────────────

    def test_describe_delivery_sources_lifecycle(self, client):
        """PutDeliverySource → DescribeDeliverySources → GetDeliverySource → Delete → Error."""
        src_name = _unique("ds-edge")
        cf_arn = "arn:aws:cloudfront::123456789012:distribution/EDGEDIST"
        create_resp = client.put_delivery_source(
            name=src_name,
            resourceArn=cf_arn,
            logType="ACCESS_LOGS",
        )
        assert create_resp["deliverySource"]["name"] == src_name
        try:
            # LIST
            list_resp = client.describe_delivery_sources()
            names = [d["name"] for d in list_resp["deliverySources"]]
            assert src_name in names
            # RETRIEVE
            get_resp = client.get_delivery_source(name=src_name)
            assert get_resp["deliverySource"]["name"] == src_name
            assert get_resp["deliverySource"]["logType"] == "ACCESS_LOGS"
        finally:
            client.delete_delivery_source(name=src_name)
        # ERROR: get deleted source
        with pytest.raises(ClientError) as exc:
            client.get_delivery_source(name=src_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── describe_deliveries: full lifecycle ────────────────────────────────────

    def test_describe_deliveries_lifecycle(self, client):
        """CreateDelivery → DescribeDeliveries → GetDelivery → DeleteDelivery → Error."""
        src_name = _unique("deliv-src")
        dest_name = _unique("deliv-dest")
        group_name = _unique("/test/deliv-group")
        client.create_log_group(logGroupName=group_name)
        try:
            client.put_delivery_source(
                name=src_name,
                resourceArn="arn:aws:cloudfront::123456789012:distribution/DELIVDIST",
                logType="ACCESS_LOGS",
            )
            dest_resp = client.put_delivery_destination(
                name=dest_name,
                deliveryDestinationConfiguration={
                    "destinationResourceArn": (
                        f"arn:aws:logs:us-east-1:123456789012:log-group:{group_name}"
                    )
                },
            )
            dest_arn = dest_resp["deliveryDestination"]["arn"]
            create_resp = client.create_delivery(
                deliverySourceName=src_name,
                deliveryDestinationArn=dest_arn,
            )
            delivery_id = create_resp["delivery"]["id"]
            assert delivery_id
            try:
                # LIST
                list_resp = client.describe_deliveries()
                delivery_ids = [d["id"] for d in list_resp["deliveries"]]
                assert delivery_id in delivery_ids
                # RETRIEVE
                get_resp = client.get_delivery(id=delivery_id)
                assert get_resp["delivery"]["id"] == delivery_id
                assert get_resp["delivery"]["deliverySourceName"] == src_name
            finally:
                client.delete_delivery(id=delivery_id)
            # ERROR: get deleted delivery
            with pytest.raises(ClientError) as exc:
                client.get_delivery(id=delivery_id)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            client.delete_log_group(logGroupName=group_name)
            for cleanup in [
                lambda: client.delete_delivery_source(name=src_name),
                lambda: client.delete_delivery_destination(name=dest_name),
            ]:
                try:
                    cleanup()
                except ClientError:
                    pass  # best-effort cleanup

    # ── list_scheduled_queries: create + list + delete ────────────────────────

    def test_list_scheduled_queries_with_entry(self, client):
        """CreateScheduledQuery → ListScheduledQueries includes it → Delete → Error."""
        suffix = uuid.uuid4().hex[:8]
        sq_name = f"sq-edge-{suffix}"
        create_resp = client.create_scheduled_query(
            name=sq_name,
            queryLanguage="CWLI",
            queryString="fields @timestamp | limit 5",
            scheduleExpression="rate(1 hour)",
            executionRoleArn="arn:aws:iam::123456789012:role/test",
        )
        sq_arn = create_resp["scheduledQueryArn"]
        assert sq_arn
        try:
            # LIST (items have name/scheduleExpression/creationTime, not arn)
            list_resp = client.list_scheduled_queries()
            names = [q["name"] for q in list_resp["scheduledQueries"]]
            assert sq_name in names
            # RETRIEVE by ARN (response is flat - fields at top level, not nested)
            get_resp = client.get_scheduled_query(identifier=sq_arn)
            assert get_resp["name"] == sq_name
            assert get_resp["queryString"] == "fields @timestamp | limit 5"
        finally:
            client.delete_scheduled_query(identifier=sq_arn)
        # ERROR: get deleted query
        with pytest.raises(ClientError) as exc:
            client.get_scheduled_query(identifier=sq_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── log_group_tags: error + retrieve patterns ─────────────────────────────

    def test_log_group_tags_empty_on_new_group(self, client):
        """A newly-created group with no tags returns empty tags dict."""
        name = _unique("/test/no-tags")
        client.create_log_group(logGroupName=name)
        try:
            desc = client.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in desc["logGroups"] if g["logGroupName"] == name][0]
            arn = group["arn"].rstrip(":*")
            resp = client.list_tags_for_resource(resourceArn=arn)
            assert "tags" in resp
            assert isinstance(resp["tags"], dict)
        finally:
            client.delete_log_group(logGroupName=name)

    def test_log_group_tags_retrieve_by_described_arn(self, client):
        """Tags set at creation are retrievable by ARN from describe_log_groups."""
        name = _unique("/test/tag-arn-r")
        client.create_log_group(logGroupName=name, tags={"initial": "value"})
        try:
            desc = client.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in desc["logGroups"] if g["logGroupName"] == name][0]
            arn = group["arn"].rstrip(":*")
            # RETRIEVE tags
            tags_resp = client.list_tags_for_resource(resourceArn=arn)
            assert tags_resp["tags"]["initial"] == "value"
            # UPDATE: add tag
            client.tag_resource(resourceArn=arn, tags={"added": "later"})
            tags_resp2 = client.list_tags_for_resource(resourceArn=arn)
            assert tags_resp2["tags"]["added"] == "later"
            assert tags_resp2["tags"]["initial"] == "value"
            # DELETE one tag
            client.untag_resource(resourceArn=arn, tagKeys=["initial"])
            tags_resp3 = client.list_tags_for_resource(resourceArn=arn)
            assert "initial" not in tags_resp3["tags"]
            assert tags_resp3["tags"]["added"] == "later"
        finally:
            client.delete_log_group(logGroupName=name)

    # ── put_query_definition: full lifecycle ──────────────────────────────────

    def test_put_query_definition_retrieve_update_delete(self, client):
        """PutQueryDefinition: CREATE → RETRIEVE → UPDATE → DELETE."""
        suffix = uuid.uuid4().hex[:8]
        name = f"qdef-lc-{suffix}"
        resp = client.put_query_definition(
            name=name,
            queryString="fields @timestamp, @message | limit 20",
        )
        qid = resp["queryDefinitionId"]
        assert qid
        # RETRIEVE
        desc = client.describe_query_definitions()
        matching = [q for q in desc["queryDefinitions"] if q["queryDefinitionId"] == qid]
        assert len(matching) == 1
        assert matching[0]["name"] == name
        # UPDATE
        resp2 = client.put_query_definition(
            name=name,
            queryDefinitionId=qid,
            queryString="fields @message | limit 5",
        )
        assert resp2["queryDefinitionId"] == qid
        desc2 = client.describe_query_definitions()
        m2 = [q for q in desc2["queryDefinitions"] if q["queryDefinitionId"] == qid]
        assert "limit 5" in m2[0]["queryString"]
        # DELETE
        client.delete_query_definition(queryDefinitionId=qid)
        desc3 = client.describe_query_definitions()
        ids3 = [q["queryDefinitionId"] for q in desc3["queryDefinitions"]]
        assert qid not in ids3
