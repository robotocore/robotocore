"""Firehose compatibility tests."""

import os

import boto3
import pytest

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


@pytest.fixture
def firehose():
    return boto3.client(
        "firehose",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


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
def delivery_stream(firehose, s3):
    bucket = "firehose-dest-bucket"
    s3.create_bucket(Bucket=bucket)
    name = "test-delivery-stream"
    firehose.create_delivery_stream(
        DeliveryStreamName=name,
        ExtendedS3DestinationConfiguration={
            "BucketARN": f"arn:aws:s3:::{bucket}",
            "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
            "Prefix": "data/",
        },
    )
    yield name
    firehose.delete_delivery_stream(DeliveryStreamName=name)
    # Clean up bucket
    try:
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket)
    except Exception:
        pass


class TestFirehoseOperations:
    def test_create_delivery_stream(self, firehose, s3):
        s3.create_bucket(Bucket="fh-create-test")
        response = firehose.create_delivery_stream(
            DeliveryStreamName="create-test-stream",
            ExtendedS3DestinationConfiguration={
                "BucketARN": "arn:aws:s3:::fh-create-test",
                "RoleARN": "arn:aws:iam::123456789012:role/test",
            },
        )
        assert "DeliveryStreamARN" in response
        firehose.delete_delivery_stream(DeliveryStreamName="create-test-stream")
        s3.delete_bucket(Bucket="fh-create-test")

    def test_describe_delivery_stream(self, firehose, delivery_stream):
        response = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)
        desc = response["DeliveryStreamDescription"]
        assert desc["DeliveryStreamName"] == delivery_stream
        assert desc["DeliveryStreamStatus"] == "ACTIVE"

    def test_list_delivery_streams(self, firehose, delivery_stream):
        response = firehose.list_delivery_streams()
        assert delivery_stream in response["DeliveryStreamNames"]

    def test_put_record(self, firehose, delivery_stream):
        response = firehose.put_record(
            DeliveryStreamName=delivery_stream,
            Record={"Data": b"hello firehose\n"},
        )
        assert "RecordId" in response

    def test_put_record_batch(self, firehose, delivery_stream):
        response = firehose.put_record_batch(
            DeliveryStreamName=delivery_stream,
            Records=[
                {"Data": b"record 1\n"},
                {"Data": b"record 2\n"},
                {"Data": b"record 3\n"},
            ],
        )
        assert response["FailedPutCount"] == 0
        assert len(response["RequestResponses"]) == 3

    def test_delete_delivery_stream(self, firehose, s3):
        s3.create_bucket(Bucket="fh-delete-test")
        firehose.create_delivery_stream(
            DeliveryStreamName="delete-test-stream",
            ExtendedS3DestinationConfiguration={
                "BucketARN": "arn:aws:s3:::fh-delete-test",
                "RoleARN": "arn:aws:iam::123456789012:role/test",
            },
        )
        firehose.delete_delivery_stream(DeliveryStreamName="delete-test-stream")
        streams = firehose.list_delivery_streams()["DeliveryStreamNames"]
        assert "delete-test-stream" not in streams
        s3.delete_bucket(Bucket="fh-delete-test")


class TestFirehoseTagging:
    def test_tag_delivery_stream(self, firehose, delivery_stream):
        """Tag a delivery stream and list tags."""
        firehose.tag_delivery_stream(
            DeliveryStreamName=delivery_stream,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "data"},
            ],
        )
        response = firehose.list_tags_for_delivery_stream(
            DeliveryStreamName=delivery_stream
        )
        tag_map = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "data"

    def test_untag_delivery_stream(self, firehose, delivery_stream):
        """Untag a delivery stream."""
        firehose.tag_delivery_stream(
            DeliveryStreamName=delivery_stream,
            Tags=[{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}],
        )
        firehose.untag_delivery_stream(
            DeliveryStreamName=delivery_stream,
            TagKeys=["k1"],
        )
        response = firehose.list_tags_for_delivery_stream(
            DeliveryStreamName=delivery_stream
        )
        tag_map = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert "k1" not in tag_map
        assert tag_map["k2"] == "v2"


class TestFirehoseDescribeDetails:
    def test_describe_stream_has_destinations(self, firehose, delivery_stream):
        """Verify describe stream includes destination configuration."""
        response = firehose.describe_delivery_stream(
            DeliveryStreamName=delivery_stream
        )
        desc = response["DeliveryStreamDescription"]
        assert "Destinations" in desc
        assert len(desc["Destinations"]) >= 1
        assert "DeliveryStreamARN" in desc

    def test_list_delivery_streams_with_type(self, firehose, delivery_stream):
        """List streams filtered by type."""
        response = firehose.list_delivery_streams(
            DeliveryStreamType="DirectPut"
        )
        assert delivery_stream in response["DeliveryStreamNames"]
