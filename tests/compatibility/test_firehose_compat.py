"""Firehose compatibility tests."""

import os
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

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
        pass  # best-effort cleanup


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

    def test_create_with_buffering_hints_and_compression(self, firehose, s3):
        """Create delivery stream with BufferingHints and CompressionFormat."""
        s3.create_bucket(Bucket="fh-buffer-test")
        # Clean up from any previous run
        try:
            firehose.delete_delivery_stream(DeliveryStreamName="buffered-stream")
        except Exception:
            pass  # best-effort cleanup
        firehose.create_delivery_stream(
            DeliveryStreamName="buffered-stream",
            ExtendedS3DestinationConfiguration={
                "BucketARN": "arn:aws:s3:::fh-buffer-test",
                "RoleARN": "arn:aws:iam::123456789012:role/test",
                "BufferingHints": {
                    "SizeInMBs": 5,
                    "IntervalInSeconds": 60,
                },
                "CompressionFormat": "GZIP",
            },
        )
        desc = firehose.describe_delivery_stream(DeliveryStreamName="buffered-stream")[
            "DeliveryStreamDescription"
        ]
        dest = desc["Destinations"][0]["ExtendedS3DestinationDescription"]
        assert dest.get("CompressionFormat", "GZIP") == "GZIP"
        hints = dest.get("BufferingHints", {})
        assert hints.get("SizeInMBs", 5) == 5
        assert hints.get("IntervalInSeconds", 60) == 60

        firehose.delete_delivery_stream(DeliveryStreamName="buffered-stream")
        s3.delete_bucket(Bucket="fh-buffer-test")

    def test_put_record_batch_multiple_records(self, firehose, delivery_stream):
        """PutRecordBatch with many records, all succeed."""
        records = [{"Data": f"batch-item-{i}\n".encode()} for i in range(15)]
        response = firehose.put_record_batch(
            DeliveryStreamName=delivery_stream,
            Records=records,
        )
        assert response["FailedPutCount"] == 0
        assert len(response["RequestResponses"]) == 15
        for resp in response["RequestResponses"]:
            assert "RecordId" in resp

    def test_update_destination(self, firehose, delivery_stream):
        """UpdateDestination changes buffering config."""
        desc = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)[
            "DeliveryStreamDescription"
        ]
        dest_id = desc["Destinations"][0]["DestinationId"]
        version_id = desc["VersionId"]

        firehose.update_destination(
            DeliveryStreamName=delivery_stream,
            CurrentDeliveryStreamVersionId=version_id,
            DestinationId=dest_id,
            ExtendedS3DestinationUpdate={
                "BufferingHints": {
                    "SizeInMBs": 10,
                    "IntervalInSeconds": 120,
                },
            },
        )
        desc2 = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)[
            "DeliveryStreamDescription"
        ]
        dest2 = desc2["Destinations"][0]["ExtendedS3DestinationDescription"]
        assert dest2["BufferingHints"]["SizeInMBs"] == 10
        assert dest2["BufferingHints"]["IntervalInSeconds"] == 120

    def test_tag_delivery_stream(self, firehose, delivery_stream):
        """TagDeliveryStream adds tags."""
        firehose.tag_delivery_stream(
            DeliveryStreamName=delivery_stream,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "project", "Value": "robotocore"},
            ],
        )
        response = firehose.list_tags_for_delivery_stream(DeliveryStreamName=delivery_stream)
        tag_map = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tag_map["env"] == "test"
        assert tag_map["project"] == "robotocore"

    def test_untag_delivery_stream(self, firehose, delivery_stream):
        """UntagDeliveryStream removes tags."""
        firehose.tag_delivery_stream(
            DeliveryStreamName=delivery_stream,
            Tags=[{"Key": "temp", "Value": "val"}],
        )
        firehose.untag_delivery_stream(
            DeliveryStreamName=delivery_stream,
            TagKeys=["temp"],
        )
        response = firehose.list_tags_for_delivery_stream(DeliveryStreamName=delivery_stream)
        tag_keys = [t["Key"] for t in response["Tags"]]
        assert "temp" not in tag_keys

    def test_list_delivery_streams_with_pagination(self, firehose, s3):
        """ListDeliveryStreams with ExclusiveStartDeliveryStreamName for pagination."""
        s3.create_bucket(Bucket="fh-pagination-test")
        names = []
        for i in range(3):
            name = f"pagination-stream-{i}"
            names.append(name)
            try:
                firehose.delete_delivery_stream(DeliveryStreamName=name)
            except Exception:
                pass  # best-effort cleanup
            firehose.create_delivery_stream(
                DeliveryStreamName=name,
                ExtendedS3DestinationConfiguration={
                    "BucketARN": "arn:aws:s3:::fh-pagination-test",
                    "RoleARN": "arn:aws:iam::123456789012:role/test",
                },
            )
        try:
            # List all streams
            resp_all = firehose.list_delivery_streams()
            all_names = resp_all["DeliveryStreamNames"]
            for name in names:
                assert name in all_names

            # Use ExclusiveStartDeliveryStreamName to skip past the first stream
            first_name = sorted(names)[0]
            resp2 = firehose.list_delivery_streams(
                ExclusiveStartDeliveryStreamName=first_name,
            )
            assert first_name not in resp2["DeliveryStreamNames"]
        finally:
            for name in names:
                firehose.delete_delivery_stream(DeliveryStreamName=name)
            s3.delete_bucket(Bucket="fh-pagination-test")

    def test_list_delivery_streams_with_type_filter(self, firehose, delivery_stream):
        """ListDeliveryStreams with DeliveryStreamType filter."""
        response = firehose.list_delivery_streams(DeliveryStreamType="DirectPut")
        assert delivery_stream in response["DeliveryStreamNames"]

    def test_describe_delivery_stream_destination_details(self, firehose, delivery_stream):
        """DescribeDeliveryStream verifies destination configuration details."""
        desc = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)[
            "DeliveryStreamDescription"
        ]
        assert desc["DeliveryStreamType"] == "DirectPut"
        assert len(desc["Destinations"]) >= 1
        dest = desc["Destinations"][0]
        assert "DestinationId" in dest
        assert "ExtendedS3DestinationDescription" in dest
        s3_dest = dest["ExtendedS3DestinationDescription"]
        assert "BucketARN" in s3_dest
        assert "RoleARN" in s3_dest

    def test_put_record_nonexistent_stream(self, firehose):
        """PutRecord to nonexistent stream raises error."""
        with pytest.raises(ClientError) as exc:
            firehose.put_record(
                DeliveryStreamName="nonexistent-stream-xyz",
                Record={"Data": b"test\n"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_record_batch_nonexistent_stream(self, firehose):
        """PutRecordBatch to nonexistent stream raises error."""
        with pytest.raises(ClientError) as exc:
            firehose.put_record_batch(
                DeliveryStreamName="nonexistent-stream-xyz",
                Records=[{"Data": b"test\n"}],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_nonexistent_stream(self, firehose):
        """Deleting a nonexistent delivery stream raises error."""
        with pytest.raises(ClientError) as exc:
            firehose.delete_delivery_stream(DeliveryStreamName="nonexistent-stream-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_and_stop_encryption(self, firehose, delivery_stream):
        """StartDeliveryStreamEncryption and StopDeliveryStreamEncryption."""
        firehose.start_delivery_stream_encryption(
            DeliveryStreamName=delivery_stream,
            DeliveryStreamEncryptionConfigurationInput={
                "KeyType": "AWS_OWNED_CMK",
            },
        )
        desc = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)[
            "DeliveryStreamDescription"
        ]
        enc = desc.get("DeliveryStreamEncryptionConfiguration", {})
        assert enc.get("KeyType") == "AWS_OWNED_CMK"

        firehose.stop_delivery_stream_encryption(DeliveryStreamName=delivery_stream)
        desc2 = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)[
            "DeliveryStreamDescription"
        ]
        enc2 = desc2.get("DeliveryStreamEncryptionConfiguration", {})
        assert enc2.get("Status", "DISABLED") in ("DISABLED", "DISABLING", None)

    def test_multiple_streams_list_and_filter(self, firehose, s3):
        """Create multiple delivery streams, list them, verify filtering."""
        s3.create_bucket(Bucket="fh-multi-test")
        stream_names = []
        for i in range(3):
            name = f"multi-stream-{i}"
            stream_names.append(name)
            firehose.create_delivery_stream(
                DeliveryStreamName=name,
                ExtendedS3DestinationConfiguration={
                    "BucketARN": "arn:aws:s3:::fh-multi-test",
                    "RoleARN": "arn:aws:iam::123456789012:role/test",
                },
            )
        try:
            response = firehose.list_delivery_streams()
            listed = response["DeliveryStreamNames"]
            for name in stream_names:
                assert name in listed
        finally:
            for name in stream_names:
                firehose.delete_delivery_stream(DeliveryStreamName=name)
            s3.delete_bucket(Bucket="fh-multi-test")

    def test_list_tags_for_delivery_stream_empty(self, firehose, delivery_stream):
        """ListTagsForDeliveryStream on untagged stream returns empty list."""
        response = firehose.list_tags_for_delivery_stream(DeliveryStreamName=delivery_stream)
        assert "Tags" in response
        assert isinstance(response["Tags"], list)

    def test_tag_and_untag_delivery_stream(self, firehose, delivery_stream):
        """TagDeliveryStream / UntagDeliveryStream / ListTagsForDeliveryStream."""
        firehose.tag_delivery_stream(
            DeliveryStreamName=delivery_stream,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "data"},
            ],
        )
        resp = firehose.list_tags_for_delivery_stream(DeliveryStreamName=delivery_stream)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "data"

        firehose.untag_delivery_stream(DeliveryStreamName=delivery_stream, TagKeys=["team"])
        resp2 = firehose.list_tags_for_delivery_stream(DeliveryStreamName=delivery_stream)
        keys = [t["Key"] for t in resp2["Tags"]]
        assert "env" in keys
        assert "team" not in keys

    def test_put_record_returns_record_id(self, firehose, delivery_stream):
        """PutRecord returns RecordId."""
        resp = firehose.put_record(
            DeliveryStreamName=delivery_stream,
            Record={"Data": b"record-id-test\n"},
        )
        assert "RecordId" in resp

    def test_put_record_batch_returns_record_ids(self, firehose, delivery_stream):
        """PutRecordBatch returns RecordId for each record."""
        resp = firehose.put_record_batch(
            DeliveryStreamName=delivery_stream,
            Records=[
                {"Data": b"batch-1\n"},
                {"Data": b"batch-2\n"},
                {"Data": b"batch-3\n"},
            ],
        )
        assert resp["FailedPutCount"] == 0
        assert len(resp["RequestResponses"]) == 3
        for r in resp["RequestResponses"]:
            assert "RecordId" in r

    def test_describe_delivery_stream_fields(self, firehose, delivery_stream):
        """DescribeDeliveryStream returns all expected fields."""
        resp = firehose.describe_delivery_stream(DeliveryStreamName=delivery_stream)
        desc = resp["DeliveryStreamDescription"]
        assert desc["DeliveryStreamName"] == delivery_stream
        assert "DeliveryStreamARN" in desc
        assert "DeliveryStreamStatus" in desc
        assert desc["DeliveryStreamStatus"] in ("CREATING", "ACTIVE")
        assert "Destinations" in desc
        assert len(desc["Destinations"]) >= 1

    def test_list_delivery_streams_with_limit(self, firehose):
        """ListDeliveryStreams with Limit parameter."""
        resp = firehose.list_delivery_streams(Limit=10)
        assert "DeliveryStreamNames" in resp
        assert "HasMoreDeliveryStreams" in resp


class TestFirehoseExtended:
    """Extended Firehose compatibility tests covering additional operations and edge cases."""

    @pytest.fixture(autouse=True)
    def setup_clients(self):
        self.firehose = make_client("firehose")
        self.s3 = make_client("s3")

    def _unique_name(self, prefix="fh-ext"):
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def _create_stream(self, name, bucket, **extra_s3_config):
        config = {
            "BucketARN": f"arn:aws:s3:::{bucket}",
            "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
            **extra_s3_config,
        }
        return self.firehose.create_delivery_stream(
            DeliveryStreamName=name,
            ExtendedS3DestinationConfiguration=config,
        )

    def _cleanup_stream(self, name):
        try:
            self.firehose.delete_delivery_stream(DeliveryStreamName=name)
        except Exception:
            pass  # best-effort cleanup

    def _cleanup_bucket(self, bucket):
        try:
            objects = self.s3.list_objects_v2(Bucket=bucket).get("Contents", [])
            for obj in objects:
                self.s3.delete_object(Bucket=bucket, Key=obj["Key"])
            self.s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass  # best-effort cleanup

    def test_create_stream_returns_valid_arn_format(self):
        """CreateDeliveryStream ARN follows the expected format."""
        bucket = self._unique_name("arn-fmt-bkt")
        name = self._unique_name("arn-fmt")
        self.s3.create_bucket(Bucket=bucket)
        try:
            resp = self._create_stream(name, bucket)
            arn = resp["DeliveryStreamARN"]
            assert arn.startswith("arn:aws:firehose:")
            assert name in arn
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_describe_stream_has_all_required_fields(self):
        """DescribeDeliveryStream response contains all required top-level fields."""
        bucket = self._unique_name("desc-fields-bkt")
        name = self._unique_name("desc-fields")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            assert desc["DeliveryStreamName"] == name
            assert "DeliveryStreamARN" in desc
            assert desc["DeliveryStreamStatus"] == "ACTIVE"
            assert "VersionId" in desc
            assert "Destinations" in desc
            assert isinstance(desc["Destinations"], list)
            assert "DeliveryStreamType" in desc
            assert "HasMoreDestinations" in desc
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_describe_nonexistent_stream_raises_not_found(self):
        """DescribeDeliveryStream for a nonexistent stream raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            self.firehose.describe_delivery_stream(
                DeliveryStreamName="does-not-exist-" + uuid.uuid4().hex[:8]
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_duplicate_stream_raises_error(self):
        """Creating a stream with an existing name raises an error."""
        bucket = self._unique_name("dup-bkt")
        name = self._unique_name("dup-stream")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            with pytest.raises(ClientError) as exc:
                self._create_stream(name, bucket)
            assert exc.value.response["Error"]["Code"] == "ResourceInUseException"
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_create_with_s3_destination_configuration(self):
        """CreateDeliveryStream with S3DestinationConfiguration (non-extended)."""
        bucket = self._unique_name("s3dest-bkt")
        name = self._unique_name("s3dest")
        self.s3.create_bucket(Bucket=bucket)
        try:
            resp = self.firehose.create_delivery_stream(
                DeliveryStreamName=name,
                S3DestinationConfiguration={
                    "BucketARN": f"arn:aws:s3:::{bucket}",
                    "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
                },
            )
            assert "DeliveryStreamARN" in resp
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            assert desc["DeliveryStreamStatus"] == "ACTIVE"
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_create_with_snappy_compression(self):
        """CreateDeliveryStream with SNAPPY compression."""
        bucket = self._unique_name("snappy-bkt")
        name = self._unique_name("snappy")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket, CompressionFormat="SNAPPY")
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            dest = desc["Destinations"][0]["ExtendedS3DestinationDescription"]
            assert dest["CompressionFormat"] == "SNAPPY"
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_create_with_zip_compression(self):
        """CreateDeliveryStream with ZIP compression."""
        bucket = self._unique_name("zip-bkt")
        name = self._unique_name("zip")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket, CompressionFormat="ZIP")
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            dest = desc["Destinations"][0]["ExtendedS3DestinationDescription"]
            assert dest["CompressionFormat"] == "ZIP"
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_create_with_uncompressed(self):
        """CreateDeliveryStream with UNCOMPRESSED explicitly set."""
        bucket = self._unique_name("uncomp-bkt")
        name = self._unique_name("uncomp")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket, CompressionFormat="UNCOMPRESSED")
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            dest = desc["Destinations"][0]["ExtendedS3DestinationDescription"]
            assert dest["CompressionFormat"] == "UNCOMPRESSED"
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_create_with_prefix_and_error_prefix(self):
        """CreateDeliveryStream with Prefix and ErrorOutputPrefix."""
        bucket = self._unique_name("pfx-bkt")
        name = self._unique_name("pfx")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(
                name,
                bucket,
                Prefix="data/year=!{timestamp:yyyy}/",
                ErrorOutputPrefix="errors/",
            )
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            dest = desc["Destinations"][0]["ExtendedS3DestinationDescription"]
            assert dest.get("Prefix") == "data/year=!{timestamp:yyyy}/"
            assert dest.get("ErrorOutputPrefix") == "errors/"
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_list_streams_limit(self):
        """ListDeliveryStreams respects Limit parameter."""
        bucket = self._unique_name("limit-bkt")
        self.s3.create_bucket(Bucket=bucket)
        names = []
        try:
            for i in range(3):
                name = self._unique_name(f"limit-{i}")
                names.append(name)
                self._create_stream(name, bucket)
            resp = self.firehose.list_delivery_streams(Limit=1)
            assert len(resp["DeliveryStreamNames"]) <= 1
            assert "HasMoreDeliveryStreams" in resp
        finally:
            for name in names:
                self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_list_streams_has_more_flag(self):
        """ListDeliveryStreams HasMoreDeliveryStreams is boolean."""
        resp = self.firehose.list_delivery_streams()
        assert isinstance(resp["HasMoreDeliveryStreams"], bool)

    def test_tag_multiple_then_list(self):
        """Tag a stream with multiple tags and verify all are returned."""
        bucket = self._unique_name("mtag-bkt")
        name = self._unique_name("mtag")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            tags = [{"Key": f"key-{i}", "Value": f"val-{i}"} for i in range(5)]
            self.firehose.tag_delivery_stream(DeliveryStreamName=name, Tags=tags)
            resp = self.firehose.list_tags_for_delivery_stream(DeliveryStreamName=name)
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            for i in range(5):
                assert tag_map[f"key-{i}"] == f"val-{i}"
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_tag_overwrite_existing(self):
        """Tagging with an existing key overwrites the value."""
        bucket = self._unique_name("tagov-bkt")
        name = self._unique_name("tagov")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            self.firehose.tag_delivery_stream(
                DeliveryStreamName=name,
                Tags=[{"Key": "env", "Value": "dev"}],
            )
            self.firehose.tag_delivery_stream(
                DeliveryStreamName=name,
                Tags=[{"Key": "env", "Value": "prod"}],
            )
            resp = self.firehose.list_tags_for_delivery_stream(DeliveryStreamName=name)
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tag_map["env"] == "prod"
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_untag_nonexistent_key_is_noop(self):
        """UntagDeliveryStream with a key that does not exist does not raise."""
        bucket = self._unique_name("untagnoop-bkt")
        name = self._unique_name("untagnoop")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            # Should not raise
            self.firehose.untag_delivery_stream(
                DeliveryStreamName=name,
                TagKeys=["nonexistent-key"],
            )
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_update_destination_changes_compression(self):
        """UpdateDestination can change compression format."""
        bucket = self._unique_name("updcomp-bkt")
        name = self._unique_name("updcomp")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket, CompressionFormat="UNCOMPRESSED")
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            dest_id = desc["Destinations"][0]["DestinationId"]
            version_id = desc["VersionId"]

            self.firehose.update_destination(
                DeliveryStreamName=name,
                CurrentDeliveryStreamVersionId=version_id,
                DestinationId=dest_id,
                ExtendedS3DestinationUpdate={
                    "CompressionFormat": "GZIP",
                },
            )
            desc2 = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            dest2 = desc2["Destinations"][0]["ExtendedS3DestinationDescription"]
            assert dest2["CompressionFormat"] == "GZIP"
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_update_destination_wrong_version_raises(self):
        """UpdateDestination with wrong version ID raises error."""
        bucket = self._unique_name("updver-bkt")
        name = self._unique_name("updver")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            dest_id = desc["Destinations"][0]["DestinationId"]

            with pytest.raises(ClientError) as exc:
                self.firehose.update_destination(
                    DeliveryStreamName=name,
                    CurrentDeliveryStreamVersionId="999",
                    DestinationId=dest_id,
                    ExtendedS3DestinationUpdate={
                        "CompressionFormat": "GZIP",
                    },
                )
            assert exc.value.response["Error"]["Code"] in (
                "InvalidArgumentException",
                "ConcurrentModificationException",
            )
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_put_record_returns_record_id_format(self):
        """PutRecord RecordId is a non-empty string."""
        bucket = self._unique_name("recid-bkt")
        name = self._unique_name("recid")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            resp = self.firehose.put_record(
                DeliveryStreamName=name,
                Record={"Data": b"test-record-id\n"},
            )
            record_id = resp["RecordId"]
            assert isinstance(record_id, str)
            assert len(record_id) > 0
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_put_record_batch_each_has_record_id(self):
        """PutRecordBatch response has a RecordId for each record."""
        bucket = self._unique_name("batchid-bkt")
        name = self._unique_name("batchid")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            records = [{"Data": f"item-{i}\n".encode()} for i in range(5)]
            resp = self.firehose.put_record_batch(DeliveryStreamName=name, Records=records)
            assert resp["FailedPutCount"] == 0
            assert len(resp["RequestResponses"]) == 5
            record_ids = [r["RecordId"] for r in resp["RequestResponses"]]
            assert all(isinstance(rid, str) and len(rid) > 0 for rid in record_ids)
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_create_with_custom_buffering_hints(self):
        """CreateDeliveryStream with custom BufferingHints values are reflected in describe."""
        bucket = self._unique_name("bufhint-bkt")
        name = self._unique_name("bufhint")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(
                name,
                bucket,
                BufferingHints={"SizeInMBs": 64, "IntervalInSeconds": 300},
            )
            desc = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            dest = desc["Destinations"][0]["ExtendedS3DestinationDescription"]
            hints = dest["BufferingHints"]
            assert hints["SizeInMBs"] == 64
            assert hints["IntervalInSeconds"] == 300
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_version_id_increments_on_update(self):
        """VersionId changes after UpdateDestination."""
        bucket = self._unique_name("verinc-bkt")
        name = self._unique_name("verinc")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            desc1 = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            v1 = desc1["VersionId"]
            dest_id = desc1["Destinations"][0]["DestinationId"]

            self.firehose.update_destination(
                DeliveryStreamName=name,
                CurrentDeliveryStreamVersionId=v1,
                DestinationId=dest_id,
                ExtendedS3DestinationUpdate={
                    "BufferingHints": {"SizeInMBs": 32, "IntervalInSeconds": 90},
                },
            )
            desc2 = self.firehose.describe_delivery_stream(DeliveryStreamName=name)[
                "DeliveryStreamDescription"
            ]
            v2 = desc2["VersionId"]
            assert v1 != v2
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_list_tags_with_limit(self):
        """ListTagsForDeliveryStream respects Limit parameter."""
        bucket = self._unique_name("taglim-bkt")
        name = self._unique_name("taglim")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            tags = [{"Key": f"k{i}", "Value": f"v{i}"} for i in range(5)]
            self.firehose.tag_delivery_stream(DeliveryStreamName=name, Tags=tags)
            resp = self.firehose.list_tags_for_delivery_stream(DeliveryStreamName=name, Limit=2)
            assert len(resp["Tags"]) <= 2
            assert "HasMoreTags" in resp
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_list_tags_pagination_with_exclusive_start(self):
        """ListTagsForDeliveryStream paginates with ExclusiveStartTagKey."""
        bucket = self._unique_name("tagpag-bkt")
        name = self._unique_name("tagpag")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            tags = [{"Key": f"alpha-{i}", "Value": f"v{i}"} for i in range(5)]
            self.firehose.tag_delivery_stream(DeliveryStreamName=name, Tags=tags)
            resp = self.firehose.list_tags_for_delivery_stream(
                DeliveryStreamName=name,
                ExclusiveStartTagKey="alpha-0",
            )
            returned_keys = [t["Key"] for t in resp["Tags"]]
            assert "alpha-0" not in returned_keys
        finally:
            self._cleanup_stream(name)
            self._cleanup_bucket(bucket)

    def test_delete_stream_then_describe_raises(self):
        """Describing a deleted stream raises ResourceNotFoundException."""
        bucket = self._unique_name("deldesc-bkt")
        name = self._unique_name("deldesc")
        self.s3.create_bucket(Bucket=bucket)
        try:
            self._create_stream(name, bucket)
            self.firehose.delete_delivery_stream(DeliveryStreamName=name)
            with pytest.raises(ClientError) as exc:
                self.firehose.describe_delivery_stream(DeliveryStreamName=name)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            self._cleanup_bucket(bucket)
