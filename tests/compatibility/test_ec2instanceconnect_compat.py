"""Compatibility tests for EC2 Instance Connect service."""

import base64

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ec2ic():
    return make_client("ec2-instance-connect")


# Generate a fake SSH public key that meets the minimum length requirement
_FAKE_SSH_KEY = "ssh-rsa " + base64.b64encode(b"x" * 300).decode() + " test@test"


class TestEC2InstanceConnect:
    """Tests for EC2 Instance Connect operations."""

    def test_send_ssh_public_key(self, ec2ic):
        resp = ec2ic.send_ssh_public_key(
            InstanceId="i-1234567890abcdef0",
            InstanceOSUser="ec2-user",
            SSHPublicKey=_FAKE_SSH_KEY,
            AvailabilityZone="us-east-1a",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["Success"] is True
        assert "RequestId" in resp
