"""CloudWatch Logs compatibility tests."""

import time
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def logs():
    return make_client("logs")


@pytest.fixture
def lambda_client():
    return make_client("lambda")


@pytest.fixture
def iam_client():
    return make_client("iam")


@pytest.fixture
def kinesis_client():
    return make_client("kinesis")


@pytest.fixture
def log_group(logs):
    name = f"/test/compat-group-{uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=name)
    yield name
    try:
        logs.delete_log_group(logGroupName=name)
    except ClientError:
        pass


@pytest.fixture
def log_group_with_stream(logs, log_group):
    """Log group with a single stream already created."""
    stream_name = "default-stream"
    logs.create_log_stream(logGroupName=log_group, logStreamName=stream_name)
    return log_group, stream_name


class TestLogsOperations:
    def test_create_log_group(self, logs):
        name = f"/test/group-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=name)
        response = logs.describe_log_groups(logGroupNamePrefix=name)
        names = [g["logGroupName"] for g in response["logGroups"]]
        assert name in names
        logs.delete_log_group(logGroupName=name)

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


class TestLogsRetentionPolicy:
    def test_put_retention_valid_values(self, logs, log_group):
        """Retention policy accepts all valid day values."""
        for days in [1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653]:
            logs.put_retention_policy(logGroupName=log_group, retentionInDays=days)
            response = logs.describe_log_groups(logGroupNamePrefix=log_group)
            group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
            assert group["retentionInDays"] == days

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_put_retention_invalid_value(self, logs, log_group):
        """Retention policy rejects invalid day values."""
        with pytest.raises(ClientError) as exc_info:
            logs.put_retention_policy(logGroupName=log_group, retentionInDays=2)
        assert exc_info.value.response["Error"]["Code"] == "InvalidParameterException"

    def test_delete_retention_no_prior_policy(self, logs, log_group):
        """Deleting retention policy when none is set should succeed (idempotent)."""
        logs.delete_retention_policy(logGroupName=log_group)
        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
        assert "retentionInDays" not in group


class TestLogsTagging:
    def test_tag_log_group(self, logs, log_group):
        """Tag a log group and verify tags via list_tags_for_resource."""
        # Get the ARN first
        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
        arn = group["arn"]
        # Remove trailing :* if present
        if arn.endswith(":*"):
            arn = arn[:-2]

        logs.tag_resource(resourceArn=arn, tags={"env": "test", "team": "platform"})
        tag_response = logs.list_tags_for_resource(resourceArn=arn)
        assert tag_response["tags"]["env"] == "test"
        assert tag_response["tags"]["team"] == "platform"

    def test_untag_log_group(self, logs, log_group):
        """Untag specific keys from a log group."""
        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
        arn = group["arn"]
        if arn.endswith(":*"):
            arn = arn[:-2]

        logs.tag_resource(resourceArn=arn, tags={"env": "test", "team": "platform", "foo": "bar"})
        logs.untag_resource(resourceArn=arn, tagKeys=["foo"])
        tag_response = logs.list_tags_for_resource(resourceArn=arn)
        assert "foo" not in tag_response["tags"]
        assert tag_response["tags"]["env"] == "test"
        assert tag_response["tags"]["team"] == "platform"

    def test_tag_overwrite(self, logs, log_group):
        """Tagging with an existing key overwrites the value."""
        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
        arn = group["arn"]
        if arn.endswith(":*"):
            arn = arn[:-2]

        logs.tag_resource(resourceArn=arn, tags={"env": "dev"})
        logs.tag_resource(resourceArn=arn, tags={"env": "prod"})
        tag_response = logs.list_tags_for_resource(resourceArn=arn)
        assert tag_response["tags"]["env"] == "prod"

    def test_create_log_group_with_tags(self, logs):
        """Create a log group with tags inline."""
        name = f"/test/tagged-group-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=name, tags={"created": "inline"})
        response = logs.describe_log_groups(logGroupNamePrefix=name)
        group = [g for g in response["logGroups"] if g["logGroupName"] == name][0]
        arn = group["arn"]
        if arn.endswith(":*"):
            arn = arn[:-2]
        tag_response = logs.list_tags_for_resource(resourceArn=arn)
        assert tag_response["tags"]["created"] == "inline"
        logs.delete_log_group(logGroupName=name)


class TestLogsFilterEvents:
    def test_filter_log_events_with_time_range(self, logs, log_group):
        """Filter log events by start and end time."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="time-stream")
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="time-stream",
            logEvents=[
                {"timestamp": now - 60000, "message": "old event"},
                {"timestamp": now, "message": "new event"},
            ],
        )
        response = logs.filter_log_events(
            logGroupName=log_group,
            startTime=now - 1000,
            endTime=now + 1000,
        )
        messages = [e["message"] for e in response["events"]]
        assert "new event" in messages

    def test_filter_log_events_limit(self, logs, log_group):
        """Filter log events with a limit on results."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="limit-stream")
        now = int(time.time() * 1000)
        events = [
            {"timestamp": now + i, "message": f"event-{i}"}
            for i in range(10)
        ]
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="limit-stream",
            logEvents=events,
        )
        response = logs.filter_log_events(
            logGroupName=log_group,
            limit=3,
        )
        assert len(response["events"]) <= 3

    def test_filter_log_events_interleaved(self, logs, log_group):
        """Filter across multiple streams returns interleaved results by timestamp."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="interleave-a")
        logs.create_log_stream(logGroupName=log_group, logStreamName="interleave-b")
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="interleave-a",
            logEvents=[
                {"timestamp": now, "message": "a-first"},
                {"timestamp": now + 200, "message": "a-third"},
            ],
        )
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="interleave-b",
            logEvents=[
                {"timestamp": now + 100, "message": "b-second"},
                {"timestamp": now + 300, "message": "b-fourth"},
            ],
        )
        response = logs.filter_log_events(
            logGroupName=log_group,
            logStreamNames=["interleave-a", "interleave-b"],
            interleaved=True,
        )
        messages = [e["message"] for e in response["events"]]
        assert len(messages) == 4
        # Events should be ordered by timestamp
        assert messages == ["a-first", "b-second", "a-third", "b-fourth"]

    def test_filter_log_events_no_match(self, logs, log_group):
        """Filter with a pattern that matches nothing returns empty."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="nomatch-stream")
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="nomatch-stream",
            logEvents=[{"timestamp": now, "message": "hello world"}],
        )
        response = logs.filter_log_events(
            logGroupName=log_group,
            filterPattern="DOESNOTEXIST999",
        )
        assert len(response["events"]) == 0

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_filter_log_events_stream_name_prefix(self, logs, log_group):
        """Filter log events using logStreamNamePrefix."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="app-stream-1")
        logs.create_log_stream(logGroupName=log_group, logStreamName="app-stream-2")
        logs.create_log_stream(logGroupName=log_group, logStreamName="sys-stream-1")
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="app-stream-1",
            logEvents=[{"timestamp": now, "message": "app1 msg"}],
        )
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="app-stream-2",
            logEvents=[{"timestamp": now, "message": "app2 msg"}],
        )
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="sys-stream-1",
            logEvents=[{"timestamp": now, "message": "sys1 msg"}],
        )
        response = logs.filter_log_events(
            logGroupName=log_group,
            logStreamNamePrefix="app-",
        )
        messages = [e["message"] for e in response["events"]]
        assert any("app1" in m for m in messages)
        assert any("app2" in m for m in messages)
        assert not any("sys1" in m for m in messages)


class TestLogsDescribeStreams:
    def test_describe_log_streams_order_by_name(self, logs, log_group):
        """Describe log streams ordered by LogStreamName."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="charlie")
        logs.create_log_stream(logGroupName=log_group, logStreamName="alpha")
        logs.create_log_stream(logGroupName=log_group, logStreamName="bravo")
        response = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LogStreamName",
        )
        names = [s["logStreamName"] for s in response["logStreams"]]
        assert names == sorted(names)

    def test_describe_log_streams_order_by_last_event(self, logs, log_group):
        """Describe log streams ordered by LastEventTime."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="old-stream")
        logs.create_log_stream(logGroupName=log_group, logStreamName="new-stream")
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="old-stream",
            logEvents=[{"timestamp": now - 60000, "message": "old"}],
        )
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="new-stream",
            logEvents=[{"timestamp": now, "message": "new"}],
        )
        response = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
        )
        names = [s["logStreamName"] for s in response["logStreams"]]
        # new-stream should appear before old-stream when descending
        if "new-stream" in names and "old-stream" in names:
            assert names.index("new-stream") < names.index("old-stream")

    def test_describe_log_streams_descending(self, logs, log_group):
        """Describe log streams in descending name order."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="aaa")
        logs.create_log_stream(logGroupName=log_group, logStreamName="bbb")
        logs.create_log_stream(logGroupName=log_group, logStreamName="ccc")
        response = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LogStreamName",
            descending=True,
        )
        names = [s["logStreamName"] for s in response["logStreams"]]
        assert names == sorted(names, reverse=True)

    def test_describe_log_streams_limit(self, logs, log_group):
        """Describe log streams with a limit."""
        for i in range(5):
            logs.create_log_stream(logGroupName=log_group, logStreamName=f"lim-stream-{i}")
        response = logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix="lim-stream-",
            limit=2,
        )
        assert len(response["logStreams"]) <= 2

    def test_describe_log_streams_pagination(self, logs, log_group):
        """Describe log streams with pagination token."""
        for i in range(5):
            logs.create_log_stream(logGroupName=log_group, logStreamName=f"page-stream-{i}")
        first = logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix="page-stream-",
            limit=2,
        )
        assert len(first["logStreams"]) == 2
        if "nextToken" in first:
            second = logs.describe_log_streams(
                logGroupName=log_group,
                logStreamNamePrefix="page-stream-",
                limit=2,
                nextToken=first["nextToken"],
            )
            assert len(second["logStreams"]) >= 1
            first_names = {s["logStreamName"] for s in first["logStreams"]}
            second_names = {s["logStreamName"] for s in second["logStreams"]}
            assert first_names.isdisjoint(second_names)


class TestLogsSubscriptionFilters:
    def test_put_subscription_filter_lambda(self, logs, log_group, lambda_client, iam_client):
        """Put a subscription filter that targets a Lambda function."""
        # Create a minimal Lambda function as the destination
        func_name = f"log-sub-fn-{uuid.uuid4().hex[:8]}"
        role_name = f"log-sub-role-{uuid.uuid4().hex[:8]}"

        role_resp = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument='{"Version":"2012-10-17","Statement":[]}',
            Path="/",
        )
        role_arn = role_resp["Role"]["Arn"]

        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("index.py", "def handler(event, context): return event")
        buf.seek(0)

        fn_resp = lambda_client.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="index.handler",
            Code={"ZipFile": buf.read()},
        )
        fn_arn = fn_resp["FunctionArn"]

        try:
            logs.put_subscription_filter(
                logGroupName=log_group,
                filterName="my-sub-filter",
                filterPattern="ERROR",
                destinationArn=fn_arn,
            )
            response = logs.describe_subscription_filters(logGroupName=log_group)
            filters = response["subscriptionFilters"]
            assert len(filters) == 1
            assert filters[0]["filterName"] == "my-sub-filter"
            assert filters[0]["filterPattern"] == "ERROR"
            assert filters[0]["destinationArn"] == fn_arn
        finally:
            try:
                logs.delete_subscription_filter(
                    logGroupName=log_group, filterName="my-sub-filter"
                )
            except ClientError:
                pass
            try:
                lambda_client.delete_function(FunctionName=func_name)
            except ClientError:
                pass
            try:
                iam_client.delete_role(RoleName=role_name)
            except ClientError:
                pass

    def test_put_subscription_filter_kinesis(self, logs, log_group, kinesis_client):
        """Put a subscription filter that targets a Kinesis stream."""
        stream_name = f"log-sub-stream-{uuid.uuid4().hex[:8]}"
        kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
        try:
            # Wait for stream to be active
            waiter = kinesis_client.get_waiter("stream_exists")
            waiter.wait(StreamName=stream_name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})

            desc = kinesis_client.describe_stream(StreamName=stream_name)
            stream_arn = desc["StreamDescription"]["StreamARN"]

            logs.put_subscription_filter(
                logGroupName=log_group,
                filterName="kinesis-sub-filter",
                filterPattern="",
                destinationArn=stream_arn,
            )
            response = logs.describe_subscription_filters(logGroupName=log_group)
            filters = response["subscriptionFilters"]
            assert len(filters) == 1
            assert filters[0]["filterName"] == "kinesis-sub-filter"
            assert filters[0]["destinationArn"] == stream_arn
        finally:
            try:
                logs.delete_subscription_filter(
                    logGroupName=log_group, filterName="kinesis-sub-filter"
                )
            except ClientError:
                pass
            try:
                kinesis_client.delete_stream(
                    StreamName=stream_name, EnforceConsumerDeletion=True
                )
            except ClientError:
                pass

    def test_describe_subscription_filters_empty(self, logs, log_group):
        """Describe subscription filters on a group with none."""
        response = logs.describe_subscription_filters(logGroupName=log_group)
        assert response["subscriptionFilters"] == []

    def test_delete_subscription_filter_nonexistent(self, logs, log_group):
        """Deleting a nonexistent subscription filter raises an error."""
        with pytest.raises(ClientError) as exc_info:
            logs.delete_subscription_filter(
                logGroupName=log_group, filterName="nonexistent-filter"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLogsMetricFilters:
    def test_put_metric_filter(self, logs, log_group):
        """Create a metric filter on a log group."""
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName="error-count",
            filterPattern="ERROR",
            metricTransformations=[
                {
                    "metricName": "ErrorCount",
                    "metricNamespace": "TestApp",
                    "metricValue": "1",
                },
            ],
        )
        response = logs.describe_metric_filters(logGroupName=log_group)
        filters = response["metricFilters"]
        assert len(filters) == 1
        assert filters[0]["filterName"] == "error-count"
        assert filters[0]["filterPattern"] == "ERROR"
        transformations = filters[0]["metricTransformations"]
        assert transformations[0]["metricName"] == "ErrorCount"
        assert transformations[0]["metricNamespace"] == "TestApp"

    def test_put_metric_filter_update(self, logs, log_group):
        """Updating a metric filter replaces it."""
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName="my-filter",
            filterPattern="WARN",
            metricTransformations=[
                {
                    "metricName": "WarnCount",
                    "metricNamespace": "TestApp",
                    "metricValue": "1",
                },
            ],
        )
        # Update with new pattern
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName="my-filter",
            filterPattern="CRITICAL",
            metricTransformations=[
                {
                    "metricName": "CritCount",
                    "metricNamespace": "TestApp",
                    "metricValue": "1",
                },
            ],
        )
        response = logs.describe_metric_filters(logGroupName=log_group)
        filters = response["metricFilters"]
        assert len(filters) == 1
        assert filters[0]["filterPattern"] == "CRITICAL"
        assert filters[0]["metricTransformations"][0]["metricName"] == "CritCount"

    def test_describe_metric_filters_empty(self, logs, log_group):
        """Describe metric filters on a group with none."""
        response = logs.describe_metric_filters(logGroupName=log_group)
        assert response["metricFilters"] == []

    def test_describe_metric_filters_by_name(self, logs, log_group):
        """Describe metric filters filtered by filter name prefix."""
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName="app-errors",
            filterPattern="ERROR",
            metricTransformations=[
                {"metricName": "Errors", "metricNamespace": "App", "metricValue": "1"},
            ],
        )
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName="app-warnings",
            filterPattern="WARN",
            metricTransformations=[
                {"metricName": "Warnings", "metricNamespace": "App", "metricValue": "1"},
            ],
        )
        response = logs.describe_metric_filters(
            logGroupName=log_group,
            filterNamePrefix="app-err",
        )
        filters = response["metricFilters"]
        assert len(filters) == 1
        assert filters[0]["filterName"] == "app-errors"

    def test_delete_metric_filter(self, logs, log_group):
        """Delete a metric filter."""
        logs.put_metric_filter(
            logGroupName=log_group,
            filterName="to-delete",
            filterPattern="DELETE",
            metricTransformations=[
                {"metricName": "DelCount", "metricNamespace": "Test", "metricValue": "1"},
            ],
        )
        logs.delete_metric_filter(logGroupName=log_group, filterName="to-delete")
        response = logs.describe_metric_filters(logGroupName=log_group)
        names = [f["filterName"] for f in response["metricFilters"]]
        assert "to-delete" not in names

    def test_delete_metric_filter_nonexistent(self, logs, log_group):
        """Deleting a nonexistent metric filter raises an error."""
        with pytest.raises(ClientError) as exc_info:
            logs.delete_metric_filter(logGroupName=log_group, filterName="ghost-filter")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestLogsExportTask:
    def test_create_export_task(self, logs, log_group):
        """Create an export task for a log group."""
        s3 = make_client("s3")
        bucket_name = f"log-export-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=bucket_name)
        try:
            logs.create_log_stream(logGroupName=log_group, logStreamName="export-stream")
            now = int(time.time() * 1000)
            logs.put_log_events(
                logGroupName=log_group,
                logStreamName="export-stream",
                logEvents=[{"timestamp": now, "message": "export me"}],
            )
            response = logs.create_export_task(
                logGroupName=log_group,
                fromTime=now - 60000,
                to=now + 60000,
                destination=bucket_name,
            )
            assert "taskId" in response
        finally:
            try:
                s3.delete_bucket(Bucket=bucket_name)
            except ClientError:
                pass

    def test_create_export_task_with_prefix(self, logs, log_group):
        """Create an export task with a destination prefix."""
        s3 = make_client("s3")
        bucket_name = f"log-export-pfx-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=bucket_name)
        try:
            now = int(time.time() * 1000)
            response = logs.create_export_task(
                logGroupName=log_group,
                fromTime=now - 60000,
                to=now + 60000,
                destination=bucket_name,
                destinationPrefix="logs/exported",
            )
            assert "taskId" in response
        finally:
            try:
                s3.delete_bucket(Bucket=bucket_name)
            except ClientError:
                pass


class TestLogsKmsEncryption:
    def test_create_log_group_with_kms(self, logs):
        """Create a log group with KMS encryption."""
        kms = make_client("kms")
        key_resp = kms.create_key(Description="logs-test-key")
        key_id = key_resp["KeyMetadata"]["KeyId"]
        key_arn = key_resp["KeyMetadata"]["Arn"]

        name = f"/test/kms-group-{uuid.uuid4().hex[:8]}"
        try:
            logs.create_log_group(logGroupName=name, kmsKeyId=key_arn)
            response = logs.describe_log_groups(logGroupNamePrefix=name)
            group = [g for g in response["logGroups"] if g["logGroupName"] == name][0]
            assert group.get("kmsKeyId") == key_arn
        finally:
            try:
                logs.delete_log_group(logGroupName=name)
            except ClientError:
                pass
            try:
                kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
            except ClientError:
                pass

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_associate_kms_key(self, logs, log_group):
        """Associate a KMS key with an existing log group."""
        kms = make_client("kms")
        key_resp = kms.create_key(Description="logs-assoc-key")
        key_id = key_resp["KeyMetadata"]["KeyId"]
        key_arn = key_resp["KeyMetadata"]["Arn"]

        try:
            logs.associate_kms_key(logGroupName=log_group, kmsKeyId=key_arn)
            response = logs.describe_log_groups(logGroupNamePrefix=log_group)
            group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
            assert group.get("kmsKeyId") == key_arn
        finally:
            try:
                logs.disassociate_kms_key(logGroupName=log_group)
            except ClientError:
                pass
            try:
                kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
            except ClientError:
                pass

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_disassociate_kms_key(self, logs, log_group):
        """Disassociate a KMS key from a log group."""
        kms = make_client("kms")
        key_resp = kms.create_key(Description="logs-disassoc-key")
        key_id = key_resp["KeyMetadata"]["KeyId"]
        key_arn = key_resp["KeyMetadata"]["Arn"]

        try:
            logs.associate_kms_key(logGroupName=log_group, kmsKeyId=key_arn)
            logs.disassociate_kms_key(logGroupName=log_group)
            response = logs.describe_log_groups(logGroupNamePrefix=log_group)
            group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
            assert "kmsKeyId" not in group
        finally:
            try:
                kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
            except ClientError:
                pass


class TestLogsPutLogEvents:
    def test_put_log_events_returns_next_sequence_token(self, logs, log_group):
        """PutLogEvents returns a nextSequenceToken."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="seq-stream")
        response = logs.put_log_events(
            logGroupName=log_group,
            logStreamName="seq-stream",
            logEvents=[{"timestamp": int(time.time() * 1000), "message": "msg1"}],
        )
        # nextSequenceToken may or may not be present depending on implementation
        # but the call should succeed
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_log_events_multiple_batches(self, logs, log_group):
        """Put multiple batches of log events to the same stream."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="batch-stream")
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="batch-stream",
            logEvents=[{"timestamp": now, "message": "batch1-msg"}],
        )
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="batch-stream",
            logEvents=[{"timestamp": now + 1000, "message": "batch2-msg"}],
        )
        response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName="batch-stream",
        )
        messages = [e["message"] for e in response["events"]]
        assert "batch1-msg" in messages
        assert "batch2-msg" in messages

    def test_put_log_events_to_nonexistent_stream(self, logs, log_group):
        """PutLogEvents to a nonexistent stream raises an error."""
        with pytest.raises(ClientError) as exc_info:
            logs.put_log_events(
                logGroupName=log_group,
                logStreamName="does-not-exist",
                logEvents=[{"timestamp": int(time.time() * 1000), "message": "fail"}],
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_log_events_start_from_head(self, logs, log_group):
        """GetLogEvents with startFromHead=True returns events in chronological order."""
        logs.create_log_stream(logGroupName=log_group, logStreamName="head-stream")
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName="head-stream",
            logEvents=[
                {"timestamp": now, "message": "first"},
                {"timestamp": now + 1000, "message": "second"},
                {"timestamp": now + 2000, "message": "third"},
            ],
        )
        response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName="head-stream",
            startFromHead=True,
        )
        messages = [e["message"] for e in response["events"]]
        assert messages == ["first", "second", "third"]


class TestLogsDescribeLogGroups:
    def test_describe_log_groups_prefix(self, logs):
        """Describe log groups filtered by prefix."""
        name1 = f"/test/desc-aaa-{uuid.uuid4().hex[:8]}"
        name2 = f"/test/desc-bbb-{uuid.uuid4().hex[:8]}"
        logs.create_log_group(logGroupName=name1)
        logs.create_log_group(logGroupName=name2)
        try:
            response = logs.describe_log_groups(logGroupNamePrefix="/test/desc-aaa")
            names = [g["logGroupName"] for g in response["logGroups"]]
            assert name1 in names
            assert name2 not in names
        finally:
            logs.delete_log_group(logGroupName=name1)
            logs.delete_log_group(logGroupName=name2)

    def test_describe_log_groups_limit(self, logs):
        """Describe log groups with a limit."""
        created = []
        for i in range(3):
            name = f"/test/limit-grp-{uuid.uuid4().hex[:8]}"
            logs.create_log_group(logGroupName=name)
            created.append(name)
        try:
            response = logs.describe_log_groups(limit=1)
            assert len(response["logGroups"]) == 1
        finally:
            for name in created:
                logs.delete_log_group(logGroupName=name)

    def test_describe_log_groups_fields(self, logs, log_group):
        """Log group response contains expected fields."""
        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = [g for g in response["logGroups"] if g["logGroupName"] == log_group][0]
        assert "logGroupName" in group
        assert "arn" in group
        assert "creationTime" in group
        assert isinstance(group["creationTime"], int)

    def test_create_duplicate_log_group(self, logs, log_group):
        """Creating a duplicate log group raises an error."""
        with pytest.raises(ClientError) as exc_info:
            logs.create_log_group(logGroupName=log_group)
        assert exc_info.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"

    def test_delete_nonexistent_log_group(self, logs):
        """Deleting a nonexistent log group raises an error."""
        with pytest.raises(ClientError) as exc_info:
            logs.delete_log_group(logGroupName="/test/does-not-exist-ever")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
