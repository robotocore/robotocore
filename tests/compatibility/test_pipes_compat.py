"""Compatibility tests for EventBridge Pipes service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def pipes_client():
    return make_client("pipes")


@pytest.fixture
def pipe_name():
    return f"test-pipe-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def created_pipe(pipes_client, pipe_name):
    """Create a pipe and clean it up after the test."""
    resp = pipes_client.create_pipe(
        Name=pipe_name,
        Source="arn:aws:sqs:us-east-1:123456789012:test-source",
        Target="arn:aws:sqs:us-east-1:123456789012:test-target",
        RoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    yield resp
    try:
        pipes_client.delete_pipe(Name=pipe_name)
    except Exception:
        pass


class TestPipesCompat:
    """Tests for EventBridge Pipes operations."""

    def test_list_pipes_empty(self, pipes_client):
        """list_pipes returns a Pipes list."""
        resp = pipes_client.list_pipes()
        assert "Pipes" in resp

    def test_create_pipe(self, pipes_client, pipe_name):
        """create_pipe creates a pipe and returns its name and ARN."""
        resp = pipes_client.create_pipe(
            Name=pipe_name,
            Source="arn:aws:sqs:us-east-1:123456789012:test-source",
            Target="arn:aws:sqs:us-east-1:123456789012:test-target",
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        assert resp["Name"] == pipe_name
        assert "Arn" in resp
        assert pipe_name in resp["Arn"]

        # Cleanup
        pipes_client.delete_pipe(Name=pipe_name)

    def test_describe_pipe(self, pipes_client, created_pipe, pipe_name):
        """describe_pipe returns full details of a created pipe."""
        resp = pipes_client.describe_pipe(Name=pipe_name)
        assert resp["Name"] == pipe_name
        assert resp["Source"] == "arn:aws:sqs:us-east-1:123456789012:test-source"
        assert resp["Target"] == "arn:aws:sqs:us-east-1:123456789012:test-target"
        assert resp["RoleArn"] == "arn:aws:iam::123456789012:role/test-role"
        assert "Arn" in resp

    def test_delete_pipe(self, pipes_client):
        """delete_pipe removes a pipe."""
        name = f"test-pipe-{uuid.uuid4().hex[:8]}"
        pipes_client.create_pipe(
            Name=name,
            Source="arn:aws:sqs:us-east-1:123456789012:test-source",
            Target="arn:aws:sqs:us-east-1:123456789012:test-target",
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        resp = pipes_client.delete_pipe(Name=name)
        assert resp["Name"] == name

        # Verify it no longer appears in list
        pipes = pipes_client.list_pipes(NamePrefix=name)
        names = [p["Name"] for p in pipes["Pipes"]]
        assert name not in names

    def test_list_pipes_shows_created(self, pipes_client, created_pipe, pipe_name):
        """list_pipes includes a newly created pipe."""
        resp = pipes_client.list_pipes()
        names = [p["Name"] for p in resp["Pipes"]]
        assert pipe_name in names

    def test_list_pipes_name_prefix_filter(self, pipes_client, created_pipe, pipe_name):
        """list_pipes filters by NamePrefix."""
        # Use the unique pipe name as prefix -- should match exactly one
        resp = pipes_client.list_pipes(NamePrefix=pipe_name)
        names = [p["Name"] for p in resp["Pipes"]]
        assert pipe_name in names

        # Use a prefix that should match nothing
        resp = pipes_client.list_pipes(NamePrefix="nonexistent-prefix-xyz")
        assert len(resp["Pipes"]) == 0
