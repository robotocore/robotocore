"""S3 behaviours Terraform's aws_s3_bucket refresh and lifecycle read-back depend on."""

import os
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")
_CRED = "123456789012/20240101/us-east-1/s3/aws4_request"


def _client(service: str, account: str = "123456789012", **kw):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=kw.pop("region_name", "us-east-1"),
        aws_access_key_id=kw.pop("aws_access_key_id", account),
        aws_secret_access_key="test",
        **kw,
    )


@pytest.fixture
def bucket():
    s3 = _client("s3")
    name = f"tf-compat-{uuid.uuid4().hex[:10]}"
    s3.create_bucket(Bucket=name)
    yield name
    try:
        s3.delete_bucket_lifecycle(Bucket=name)
        s3.delete_bucket(Bucket=name)
    except ClientError as exc:
        print(f"cleanup: {exc}")


class TestS3BucketRefresh:
    """aws_s3_bucket refresh reads every sub-resource; unconfigured ones must 404, not 400."""

    def test_get_bucket_cors_unconfigured_is_404(self, bucket):
        with pytest.raises(ClientError) as exc:
            _client("s3").get_bucket_cors(Bucket=bucket)
        assert exc.value.response["Error"]["Code"] == "NoSuchCORSConfiguration"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_trailing_slash_subresource_is_bucket_level(self, bucket):
        import requests

        # aws-sdk-go-v2 (Terraform) sends path-style bucket requests as "/bucket/?cors="
        resp = requests.get(
            f"{ENDPOINT_URL}/{bucket}/?cors=",
            headers={"Authorization": f"AWS4-HMAC-SHA256 Credential={_CRED}"},
            timeout=10,
        )
        assert resp.status_code == 404
        assert "NoSuchCORSConfiguration" in resp.text


class TestS3LifecycleReadBack:
    def test_noncurrent_transition_and_and_filter_round_trip(self, bucket):
        s3 = _client("s3")
        rules = [
            {
                "ID": "logs",
                "Status": "Enabled",
                "Filter": {
                    "And": {
                        "Prefix": "logs/",
                        "ObjectSizeGreaterThan": 1024,
                        "Tags": [{"Key": "class", "Value": "log"}],
                    }
                },
                "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}],
            },
            {
                "ID": "noncurrent",
                "Status": "Enabled",
                "Filter": {},
                "NoncurrentVersionTransitions": [
                    {
                        "NoncurrentDays": 30,
                        "NewerNoncurrentVersions": 2,
                        "StorageClass": "INTELLIGENT_TIERING",
                    }
                ],
            },
        ]
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={"Rules": rules},
            TransitionDefaultMinimumObjectSize="varies_by_storage_class",
        )
        got = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        assert got["TransitionDefaultMinimumObjectSize"] == "varies_by_storage_class"
        by_id = {r["ID"]: r for r in got["Rules"]}
        assert by_id["logs"]["Filter"]["And"]["ObjectSizeGreaterThan"] == 1024
        nvt = by_id["noncurrent"]["NoncurrentVersionTransitions"][0]
        assert nvt == {
            "NoncurrentDays": 30,
            "NewerNoncurrentVersions": 2,
            "StorageClass": "INTELLIGENT_TIERING",
        }

    def test_default_minimum_object_size(self, bucket):
        s3 = _client("s3")
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "r", "Status": "Enabled", "Filter": {}, "Expiration": {"Days": 1}}]
            },
        )
        got = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        assert got["TransitionDefaultMinimumObjectSize"] == "all_storage_classes_128K"
