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


class TestFirehoseExtended:
    """Extended Firehose compatibility tests."""

    def _make_stream(self, firehose, s3, name, bucket):
        """Helper: create bucket + delivery stream, return (name, bucket)."""
        s3.create_bucket(Bucket=bucket)
        firehose.create_delivery_stream(
            DeliveryStreamName=name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": f"arn:aws:s3:::{bucket}",
                "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
                "Prefix": "data/",
            },
        )

    def _cleanup_stream(self, firehose, s3, name, bucket):
        """Helper: delete delivery stream and bucket."""
        try:
            firehose.delete_delivery_stream(DeliveryStreamName=name)
        except Exception:
            pass
        try:
            objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
            for obj in objects:
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
            s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass

    def test_list_delivery_streams_with_limit(self, firehose, s3):
        """ListDeliveryStreams with Limit parameter."""
        import uuid as _uuid

        suffix = _uuid.uuid4().hex[:6]
        names = [f"lim-{suffix}-{i}" for i in range(3)]
        bucket = f"fh-lim-{suffix}"
        s3.create_bucket(Bucket=bucket)

        for n in names:
            firehose.create_delivery_stream(
                DeliveryStreamName=n,
                ExtendedS3DestinationConfiguration={
                    "BucketARN": f"arn:aws:s3:::{bucket}",
                    "RoleARN": "arn:aws:iam::123456789012:role/test",
                },
            )

        resp = firehose.list_delivery_streams(Limit=2)
        # Should return at most 2
        assert len(resp["DeliveryStreamNames"]) <= 2

        for n in names:
            firehose.delete_delivery_stream(DeliveryStreamName=n)
        s3.delete_bucket(Bucket=bucket)

    def test_list_delivery_streams_with_start_name(self, firehose, s3):
        """ListDeliveryStreams with ExclusiveStartDeliveryStreamName."""
        import uuid as _uuid

        suffix = _uuid.uuid4().hex[:6]
        # Use names that sort predictably
        names = sorted([f"start-{suffix}-a", f"start-{suffix}-b", f"start-{suffix}-c"])
        bucket = f"fh-start-{suffix}"
        s3.create_bucket(Bucket=bucket)

        for n in names:
            firehose.create_delivery_stream(
                DeliveryStreamName=n,
                ExtendedS3DestinationConfiguration={
                    "BucketARN": f"arn:aws:s3:::{bucket}",
                    "RoleARN": "arn:aws:iam::123456789012:role/test",
                },
            )

        # Start after the first name
        resp = firehose.list_delivery_streams(
            ExclusiveStartDeliveryStreamName=names[0]
        )
        listed = resp["DeliveryStreamNames"]
        assert names[0] not in listed
        # The remaining names should be present
        for n in names[1:]:
            assert n in listed

        for n in names:
            firehose.delete_delivery_stream(DeliveryStreamName=n)
        s3.delete_bucket(Bucket=bucket)

    def test_put_record_batch_multiple(self, firehose, delivery_stream):
        """PutRecordBatch with 5 records."""
        records = [{"Data": f"batch-record-{i}\n".encode()} for i in range(5)]
        resp = firehose.put_record_batch(
            DeliveryStreamName=delivery_stream,
            Records=records,
        )
        assert resp["FailedPutCount"] == 0
        assert len(resp["RequestResponses"]) == 5
        for rr in resp["RequestResponses"]:
            assert "RecordId" in rr

    def test_describe_delivery_stream_has_destinations(self, firehose, delivery_stream):
        """DescribeDeliveryStream includes destination information."""
        resp = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)
        desc = resp["DeliveryStreamDescription"]
        assert desc["DeliveryStreamName"] == delivery_stream
        assert "DeliveryStreamARN" in desc
        assert desc["DeliveryStreamStatus"] == "ACTIVE"
        assert "Destinations" in desc
        assert len(desc["Destinations"]) >= 1

    def test_describe_delivery_stream_has_type(self, firehose, delivery_stream):
        """DescribeDeliveryStream includes delivery stream type."""
        resp = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)
        desc = resp["DeliveryStreamDescription"]
        assert desc["DeliveryStreamType"] == "DirectPut"

    def test_describe_delivery_stream_not_found(self, firehose):
        """DescribeDeliveryStream for nonexistent stream raises error."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            firehose.describe_delivery_stream(DeliveryStreamName="nonexistent-stream-xyz")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_put_record_single(self, firehose, delivery_stream):
        """PutRecord with a single record returns RecordId."""
        resp = firehose.put_record(
            DeliveryStreamName=delivery_stream,
            Record={"Data": b"single-record-data\n"},
        )
        assert "RecordId" in resp

    def test_put_record_batch_empty_data(self, firehose, delivery_stream):
        """PutRecordBatch with minimal data."""
        resp = firehose.put_record_batch(
            DeliveryStreamName=delivery_stream,
            Records=[{"Data": b"x"}],
        )
        assert resp["FailedPutCount"] == 0
        assert len(resp["RequestResponses"]) == 1

    def test_create_delivery_stream_arn_format(self, firehose, s3):
        """CreateDeliveryStream returns correctly formatted ARN."""
        import uuid as _uuid

        suffix = _uuid.uuid4().hex[:6]
        name = f"arn-test-{suffix}"
        bucket = f"fh-arn-{suffix}"
        s3.create_bucket(Bucket=bucket)

        resp = firehose.create_delivery_stream(
            DeliveryStreamName=name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": f"arn:aws:s3:::{bucket}",
                "RoleARN": "arn:aws:iam::123456789012:role/test",
            },
        )
        arn = resp["DeliveryStreamARN"]
        assert arn.startswith("arn:aws:firehose:")
        assert name in arn

        firehose.delete_delivery_stream(DeliveryStreamName=name)
        s3.delete_bucket(Bucket=bucket)

    def test_list_delivery_streams_has_more_flag(self, firehose, delivery_stream):
        """ListDeliveryStreams response includes HasMoreDeliveryStreams."""
        resp = firehose.list_delivery_streams()
        assert "HasMoreDeliveryStreams" in resp
        assert isinstance(resp["HasMoreDeliveryStreams"], bool)

    def test_describe_destination_s3_config(self, firehose, delivery_stream):
        """Destination includes S3 configuration details."""
        resp = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)
        dests = resp["DeliveryStreamDescription"]["Destinations"]
        assert len(dests) >= 1
        dest = dests[0]
        assert "DestinationId" in dest
        s3_desc = dest.get("ExtendedS3DestinationDescription", {})
        assert "BucketARN" in s3_desc

    def test_create_and_delete_stream_lifecycle(self, firehose, s3):
        """Full create-describe-delete lifecycle."""
        import uuid as _uuid

        suffix = _uuid.uuid4().hex[:6]
        name = f"lifecycle-{suffix}"
        bucket = f"fh-life-{suffix}"
        s3.create_bucket(Bucket=bucket)

        firehose.create_delivery_stream(
            DeliveryStreamName=name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": f"arn:aws:s3:::{bucket}",
                "RoleARN": "arn:aws:iam::123456789012:role/test",
            },
        )

        # Verify exists
        desc = firehose.describe_delivery_stream(DeliveryStreamName=name)
        assert desc["DeliveryStreamDescription"]["DeliveryStreamName"] == name

        # Delete
        firehose.delete_delivery_stream(DeliveryStreamName=name)

        # Verify gone
        streams = firehose.list_delivery_streams()["DeliveryStreamNames"]
        assert name not in streams

        s3.delete_bucket(Bucket=bucket)

    def test_put_record_large_data(self, firehose, delivery_stream):
        """PutRecord with a larger payload."""
        data = ("x" * 1000 + "\n").encode()
        resp = firehose.put_record(
            DeliveryStreamName=delivery_stream,
            Record={"Data": data},
        )
        assert "RecordId" in resp

    def test_delete_nonexistent_stream(self, firehose):
        """Deleting a nonexistent stream raises error."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            firehose.delete_delivery_stream(DeliveryStreamName="does-not-exist-xyz")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_put_record_to_nonexistent_stream(self, firehose):
        """PutRecord to nonexistent stream raises error."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            firehose.put_record(
                DeliveryStreamName="nonexistent-xyz",
                Record={"Data": b"data\n"},
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_put_record_batch_to_nonexistent_stream(self, firehose):
        """PutRecordBatch to nonexistent stream raises error."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            firehose.put_record_batch(
                DeliveryStreamName="nonexistent-xyz",
                Records=[{"Data": b"data\n"}],
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_create_duplicate_stream(self, firehose, s3, delivery_stream):
        """Creating a stream with the same name raises error."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            firehose.create_delivery_stream(
                DeliveryStreamName=delivery_stream,
                ExtendedS3DestinationConfiguration={
                    "BucketARN": "arn:aws:s3:::fh-dup-bucket",
                    "RoleARN": "arn:aws:iam::123456789012:role/test",
                },
            )
        assert "ResourceInUseException" in str(exc_info.value)

    def test_list_delivery_streams_empty(self, firehose):
        """ListDeliveryStreams returns proper structure even with no matching streams."""
        resp = firehose.list_delivery_streams(
            ExclusiveStartDeliveryStreamName="zzz-nonexistent-zzz"
        )
        assert "DeliveryStreamNames" in resp
        assert isinstance(resp["DeliveryStreamNames"], list)
        assert "HasMoreDeliveryStreams" in resp

    def test_describe_delivery_stream_create_timestamp(self, firehose, delivery_stream):
        """DescribeDeliveryStream includes CreateTimestamp."""
        resp = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)
        desc = resp["DeliveryStreamDescription"]
        assert "CreateTimestamp" in desc

    def test_put_record_batch_single_record(self, firehose, delivery_stream):
        """PutRecordBatch with exactly one record."""
        resp = firehose.put_record_batch(
            DeliveryStreamName=delivery_stream,
            Records=[{"Data": b"single-batch\n"}],
        )
        assert resp["FailedPutCount"] == 0
        assert len(resp["RequestResponses"]) == 1

    def test_multiple_put_records(self, firehose, delivery_stream):
        """Multiple sequential PutRecord calls."""
        for i in range(3):
            resp = firehose.put_record(
                DeliveryStreamName=delivery_stream,
                Record={"Data": f"record-{i}\n".encode()},
            )
            assert "RecordId" in resp

    def test_describe_has_more_destinations_flag(self, firehose, delivery_stream):
        """DescribeDeliveryStream has HasMoreDestinations field."""
        resp = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)
        desc = resp["DeliveryStreamDescription"]
        assert "HasMoreDestinations" in desc
        assert desc["HasMoreDestinations"] is False
