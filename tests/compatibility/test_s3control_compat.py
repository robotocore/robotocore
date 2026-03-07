"""S3 Control compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def s3control():
    return make_client("s3control")


class TestS3ControlOperations:
    def test_put_public_access_block(self, s3control):
        response = s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)
        s3control.delete_public_access_block(AccountId="123456789012")

    def test_get_public_access_block(self, s3control):
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": False,
            },
        )
        response = s3control.get_public_access_block(AccountId="123456789012")
        config = response["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is True
        assert config["IgnorePublicAcls"] is False
        s3control.delete_public_access_block(AccountId="123456789012")

    def test_delete_public_access_block(self, s3control):
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        response = s3control.delete_public_access_block(AccountId="123456789012")
        assert response["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_get_public_access_block_not_found(self, s3control):
        """Getting public access block when not set should error."""
        # Delete any existing config first
        try:
            s3control.delete_public_access_block(AccountId="123456789012")
        except ClientError:
            pass
        with pytest.raises(ClientError) as exc:
            s3control.get_public_access_block(AccountId="123456789012")
        assert exc.value.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration"

    def test_put_public_access_block_partial(self, s3control):
        """Set only some fields in public access block."""
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": False,
                "RestrictPublicBuckets": True,
            },
        )
        response = s3control.get_public_access_block(AccountId="123456789012")
        config = response["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is True
        assert config["IgnorePublicAcls"] is False
        assert config["BlockPublicPolicy"] is False
        assert config["RestrictPublicBuckets"] is True
        s3control.delete_public_access_block(AccountId="123456789012")

    def test_overwrite_public_access_block(self, s3control):
        """Overwriting public access block replaces all fields."""
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": False,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": False,
                "RestrictPublicBuckets": False,
            },
        )
        response = s3control.get_public_access_block(AccountId="123456789012")
        config = response["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is False
        assert config["IgnorePublicAcls"] is False
        s3control.delete_public_access_block(AccountId="123456789012")
