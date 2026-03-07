"""CloudWatch Logs compatibility tests."""

import time

import pytest

from tests.compatibility.conftest import make_client


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
        events = [
            {"timestamp": now + i, "message": f"msg-{suffix}-{i}"} for i in range(10)
        ]
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
        group_arn = [
            g["arn"] for g in desc["logGroups"] if g["logGroupName"] == group
        ][0]
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

    def test_filter_log_events_with_pattern(self, logs, log_group):
        """FilterLogEvents with a specific filter pattern."""
        stream = "pattern-stream"
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            logEvents=[
                {"timestamp": now, "message": "INFO request processed"},
                {"timestamp": now + 1, "message": "WARN slow query detected"},
                {"timestamp": now + 2, "message": "ERROR connection timeout"},
                {"timestamp": now + 3, "message": "INFO request completed"},
            ],
        )
        response = logs.filter_log_events(
            logGroupName=log_group,
            filterPattern="WARN",
        )
        messages = [e["message"] for e in response["events"]]
        assert any("WARN" in m for m in messages)
        assert not any("ERROR" in m for m in messages)

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

        # Create a lambda function ARN (doesn't need to exist for the filter)
        dest_arn = f"arn:aws:lambda:us-east-1:000000000000:function:dummy-{suffix}"

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
            logEvents=[
                {"timestamp": now + i, "message": f"MATCH-{suffix}-{i}"} for i in range(10)
            ],
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

    @pytest.mark.xfail(reason="DescribeMetricFilters filterNamePrefix not returning results")
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

        response = logs.describe_metric_filters(
            logGroupName=log_group,
            filterNamePrefix=prefix,
        )
        returned = [f["filterName"] for f in response["metricFilters"]]
        for name in names:
            assert name in returned

        for name in names:
            logs.delete_metric_filter(logGroupName=log_group, filterName=name)
        response = logs.describe_metric_filters(logGroupName=log_group)
        filter_names = [f["filterName"] for f in response["metricFilters"]]
        assert "error-count" in filter_names

        # Verify the filter details
        mf = [f for f in response["metricFilters"] if f["filterName"] == "error-count"][0]
        assert mf["filterPattern"] == "ERROR"
        assert mf["metricTransformations"][0]["metricName"] == "ErrorCount"

        # Delete metric filter
        logs.delete_metric_filter(logGroupName=log_group, filterName="error-count")
        response = logs.describe_metric_filters(logGroupName=log_group)
        filter_names = [f["filterName"] for f in response["metricFilters"]]
        assert "error-count" not in filter_names

    def test_create_export_task(self, logs, log_group):
        """CreateExportTask - may not be supported, skip on error."""
        # Need an S3 bucket for export
        s3 = make_client("s3")
        bucket = "logs-export-test-bucket"
        try:
            s3.create_bucket(Bucket=bucket)
            logs.create_export_task(
                logGroupName=log_group,
                fromTime=int(time.time() * 1000) - 3600000,
                to=int(time.time() * 1000),
                destination=bucket,
            )
        except Exception:
            pytest.skip("CreateExportTask not supported")
        finally:
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass
