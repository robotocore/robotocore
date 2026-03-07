"""Firehose compatibility tests."""

import base64
import json
import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def firehose():
    return make_client("firehose")


@pytest.fixture
def s3():
    return make_client("s3")


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def delivery_stream(firehose, s3):
    bucket = f"firehose-dest-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    name = f"test-stream-{_uid()}"
    firehose.create_delivery_stream(
        DeliveryStreamName=name,
        ExtendedS3DestinationConfiguration={
            "BucketARN": f"arn:aws:s3:::{bucket}",
            "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
            "Prefix": "data/",
        },
    )
    yield name, bucket
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


class TestFirehoseOperations:
    def test_create_delivery_stream(self, firehose, s3):
        bucket = f"fh-create-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        name = f"create-stream-{_uid()}"
        response = firehose.create_delivery_stream(
            DeliveryStreamName=name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": f"arn:aws:s3:::{bucket}",
                "RoleARN": "arn:aws:iam::123456789012:role/test",
            },
        )
        assert "DeliveryStreamARN" in response
        firehose.delete_delivery_stream(DeliveryStreamName=name)
        s3.delete_bucket(Bucket=bucket)

    def test_describe_delivery_stream(self, firehose, delivery_stream):
        name, _ = delivery_stream
        response = firehose.describe_delivery_stream(DeliveryStreamName=name)
        desc = response["DeliveryStreamDescription"]
        assert desc["DeliveryStreamName"] == name
        assert desc["DeliveryStreamStatus"] == "ACTIVE"

    def test_list_delivery_streams(self, firehose, delivery_stream):
        name, _ = delivery_stream
        response = firehose.list_delivery_streams()
        assert name in response["DeliveryStreamNames"]

    def test_put_record(self, firehose, delivery_stream):
        name, _ = delivery_stream
        response = firehose.put_record(
            DeliveryStreamName=name,
            Record={"Data": b"hello firehose\n"},
        )
        assert "RecordId" in response

    def test_put_record_batch(self, firehose, delivery_stream):
        name, _ = delivery_stream
        response = firehose.put_record_batch(
            DeliveryStreamName=name,
            Records=[
                {"Data": b"record 1\n"},
                {"Data": b"record 2\n"},
                {"Data": b"record 3\n"},
            ],
        )
        assert response["FailedPutCount"] == 0
        assert len(response["RequestResponses"]) == 3

    def test_delete_delivery_stream(self, firehose, s3):
        bucket = f"fh-delete-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        name = f"delete-stream-{_uid()}"
        firehose.create_delivery_stream(
            DeliveryStreamName=name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": f"arn:aws:s3:::{bucket}",
                "RoleARN": "arn:aws:iam::123456789012:role/test",
            },
        )
        firehose.delete_delivery_stream(DeliveryStreamName=name)
        streams = firehose.list_delivery_streams()["DeliveryStreamNames"]
        assert name not in streams
        s3.delete_bucket(Bucket=bucket)


class TestFirehoseUpdateDestination:
    @pytest.mark.xfail(reason="Not yet implemented")
    def test_update_destination_prefix(self, firehose, delivery_stream):
        """Update the S3 prefix on an existing delivery stream destination."""
        name, bucket = delivery_stream
        desc = firehose.describe_delivery_stream(DeliveryStreamName=name)
        stream_desc = desc["DeliveryStreamDescription"]
        version_id = stream_desc["VersionId"]
        dest = stream_desc["Destinations"][0]
        dest_id = dest["DestinationId"]

        firehose.update_destination(
            DeliveryStreamName=name,
            CurrentDeliveryStreamVersionId=version_id,
            DestinationId=dest_id,
            ExtendedS3DestinationUpdate={
                "BucketARN": f"arn:aws:s3:::{bucket}",
                "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
                "Prefix": "updated-prefix/",
            },
        )
        updated = firehose.describe_delivery_stream(DeliveryStreamName=name)
        updated_dest = updated["DeliveryStreamDescription"]["Destinations"][0]
        s3_dest = updated_dest.get("ExtendedS3DestinationDescription", {})
        assert s3_dest.get("Prefix") == "updated-prefix/"

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_update_destination_buffering_hints(self, firehose, delivery_stream):
        """Update buffering hints on an existing delivery stream."""
        name, bucket = delivery_stream
        desc = firehose.describe_delivery_stream(DeliveryStreamName=name)
        stream_desc = desc["DeliveryStreamDescription"]
        version_id = stream_desc["VersionId"]
        dest_id = stream_desc["Destinations"][0]["DestinationId"]

        firehose.update_destination(
            DeliveryStreamName=name,
            CurrentDeliveryStreamVersionId=version_id,
            DestinationId=dest_id,
            ExtendedS3DestinationUpdate={
                "BucketARN": f"arn:aws:s3:::{bucket}",
                "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
                "BufferingHints": {
                    "SizeInMBs": 10,
                    "IntervalInSeconds": 120,
                },
            },
        )
        updated = firehose.describe_delivery_stream(DeliveryStreamName=name)
        updated_dest = updated["DeliveryStreamDescription"]["Destinations"][0]
        hints = updated_dest.get("ExtendedS3DestinationDescription", {}).get(
            "BufferingHints", {}
        )
        assert hints.get("SizeInMBs") == 10
        assert hints.get("IntervalInSeconds") == 120


class TestFirehoseEncryption:
    @pytest.mark.xfail(reason="Not yet implemented")
    def test_start_delivery_stream_encryption(self, firehose, delivery_stream):
        """Start encryption on a delivery stream."""
        name, _ = delivery_stream
        firehose.start_delivery_stream_encryption(
            DeliveryStreamName=name,
            DeliveryStreamEncryptionInput={
                "KeyType": "AWS_OWNED_CMK",
            },
        )
        desc = firehose.describe_delivery_stream(DeliveryStreamName=name)
        encryption = desc["DeliveryStreamDescription"].get(
            "DeliveryStreamEncryptionConfiguration", {}
        )
        assert encryption.get("KeyType") == "AWS_OWNED_CMK"

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_stop_delivery_stream_encryption(self, firehose, delivery_stream):
        """Start and then stop encryption on a delivery stream."""
        name, _ = delivery_stream
        firehose.start_delivery_stream_encryption(
            DeliveryStreamName=name,
            DeliveryStreamEncryptionInput={
                "KeyType": "AWS_OWNED_CMK",
            },
        )
        firehose.stop_delivery_stream_encryption(DeliveryStreamName=name)
        desc = firehose.describe_delivery_stream(DeliveryStreamName=name)
        encryption = desc["DeliveryStreamDescription"].get(
            "DeliveryStreamEncryptionConfiguration", {}
        )
        status = encryption.get("Status", "DISABLED")
        assert status in ("DISABLED", "DISABLING")


class TestFirehosePutRecordFormats:
    def test_put_record_json_data(self, firehose, delivery_stream):
        """Put a record containing JSON-formatted data."""
        name, _ = delivery_stream
        data = json.dumps({"event": "click", "user_id": 42}).encode("utf-8")
        response = firehose.put_record(
            DeliveryStreamName=name,
            Record={"Data": data},
        )
        assert "RecordId" in response

    def test_put_record_base64_binary_data(self, firehose, delivery_stream):
        """Put a record with base64-encoded binary content."""
        name, _ = delivery_stream
        raw_bytes = b"\x00\x01\x02\xff\xfe\xfd"
        data = base64.b64encode(raw_bytes)
        response = firehose.put_record(
            DeliveryStreamName=name,
            Record={"Data": data},
        )
        assert "RecordId" in response

    def test_put_record_csv_data(self, firehose, delivery_stream):
        """Put a record containing CSV-formatted data."""
        name, _ = delivery_stream
        csv_line = b"timestamp,event_type,user_id\n2026-01-01T00:00:00Z,login,123\n"
        response = firehose.put_record(
            DeliveryStreamName=name,
            Record={"Data": csv_line},
        )
        assert "RecordId" in response

    def test_put_record_batch_mixed_formats(self, firehose, delivery_stream):
        """Put a batch of records with different data formats."""
        name, _ = delivery_stream
        records = [
            {"Data": b"plain text record\n"},
            {"Data": json.dumps({"key": "value"}).encode("utf-8")},
            {"Data": b"col1,col2\nval1,val2\n"},
        ]
        response = firehose.put_record_batch(
            DeliveryStreamName=name,
            Records=records,
        )
        assert response["FailedPutCount"] == 0
        assert len(response["RequestResponses"]) == 3
        for r in response["RequestResponses"]:
            assert "RecordId" in r
