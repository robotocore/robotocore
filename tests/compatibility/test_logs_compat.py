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
        import uuid

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

    def test_filter_log_events_with_pattern(self, logs, log_group):
        """FilterLogEvents with a specific filterPattern."""
        import uuid

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
        # Just verify we can call get_query_results
        result = logs.get_query_results(queryId=query_id)
        assert "status" in result

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

    def test_list_anomalies(self, logs):
        resp = logs.list_anomalies(
            anomalyDetectorArn="arn:aws:logs:us-east-1:123456789012:anomaly-detector:dummy"
        )
        assert "anomalies" in resp

    def test_list_log_anomaly_detectors(self, logs):
        resp = logs.list_log_anomaly_detectors()
        assert "anomalyDetectors" in resp

    def test_list_integrations(self, logs):
        resp = logs.list_integrations()
        assert "integrationSummaries" in resp


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
            logs.tag_log_group(logGroupName=group, tags={"old-key": "old-val"})
            resp = logs.list_tags_log_group(logGroupName=group)
            assert resp["tags"]["old-key"] == "old-val"
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
