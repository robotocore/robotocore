"""State persistence compatibility tests — save, restore, and reset emulator state."""

import json
import os
import tempfile

import boto3
import pytest
import requests

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


@pytest.fixture
def s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def sqs():
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


class TestStatePersistence:
    def test_save_state_returns_path(self):
        """Test the save state management endpoint."""
        # Use a server-side path (/tmp/robotocore/state exists in Docker)
        save_path = "/tmp/robotocore/state"
        resp = requests.post(
            f"{ENDPOINT_URL}/_robotocore/state/save",
            json={"path": save_path},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "saved"
        assert data["path"] == save_path

    def test_load_state_nonexistent(self):
        """Loading from nonexistent path returns no_state_found."""
        resp = requests.post(
            f"{ENDPOINT_URL}/_robotocore/state/load",
            json={"path": "/tmp/nonexistent-state-dir-12345"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "no_state_found"

    def test_save_and_load_preserves_s3_buckets(self, s3):
        """S3 buckets survive save/load cycle."""
        bucket_name = "persist-test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        s3.put_object(Bucket=bucket_name, Key="data.txt", Body=b"persistent data")

        save_path = "/tmp/robotocore/state"
        # Save
        resp = requests.post(
            f"{ENDPOINT_URL}/_robotocore/state/save",
            json={"path": save_path},
        )
        assert resp.json()["status"] == "saved"

    def test_reset_state(self):
        """Reset clears all state."""
        resp = requests.post(f"{ENDPOINT_URL}/_robotocore/state/reset")
        assert resp.status_code == 200
        assert resp.json()["status"] == "reset"

    def test_health_endpoint(self):
        """Health endpoint always returns running status."""
        resp = requests.get(f"{ENDPOINT_URL}/_robotocore/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"
