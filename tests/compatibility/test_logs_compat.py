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
