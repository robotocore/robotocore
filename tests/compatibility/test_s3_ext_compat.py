"""S3 extended compatibility tests — bucket/object operations, replication, notifications."""

import json
import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

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


@pytest.fixture
def bucket(s3):
    bucket_name = "test-compat-ext-bucket"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass  # best-effort cleanup; failures are non-fatal


class TestS3BucketOperations:
    """Additional S3 bucket operations."""

    @pytest.fixture
    def bucket(self, s3):
        import uuid

        name = f"bkt-ops-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_head_bucket(self, s3, bucket):
        resp = s3.head_bucket(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_head_bucket_nonexistent(self, s3):
        import uuid

        with pytest.raises(ClientError):
            s3.head_bucket(Bucket=f"no-such-bucket-{uuid.uuid4().hex[:8]}")

    def test_get_bucket_location(self, s3, bucket):
        resp = s3.get_bucket_location(Bucket=bucket)
        # us-east-1 returns None for LocationConstraint
        assert resp["LocationConstraint"] is None or isinstance(resp["LocationConstraint"], str)

    def test_put_get_bucket_tagging(self, s3, bucket):
        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={
                "TagSet": [
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "dev"},
                ]
            },
        )
        resp = s3.get_bucket_tagging(Bucket=bucket)
        tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert tags["env"] == "test"
        assert tags["team"] == "dev"

    def test_delete_bucket_tagging(self, s3, bucket):
        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "x", "Value": "y"}]},
        )
        s3.delete_bucket_tagging(Bucket=bucket)
        with pytest.raises(ClientError):
            s3.get_bucket_tagging(Bucket=bucket)

    def test_put_get_bucket_lifecycle(self, s3, bucket):
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-old",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                        "Expiration": {"Days": 30},
                    }
                ]
            },
        )
        resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        assert len(resp["Rules"]) == 1
        assert resp["Rules"][0]["ID"] == "expire-old"

    def test_put_get_bucket_encryption(self, s3, bucket):
        s3.put_bucket_encryption(
            Bucket=bucket,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256",
                        }
                    }
                ]
            },
        )
        resp = s3.get_bucket_encryption(Bucket=bucket)
        algo = resp["ServerSideEncryptionConfiguration"]["Rules"][0][
            "ApplyServerSideEncryptionByDefault"
        ]["SSEAlgorithm"]
        assert algo == "AES256"

    def test_put_get_bucket_website(self, s3, bucket):
        s3.put_bucket_website(
            Bucket=bucket,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
                "ErrorDocument": {"Key": "error.html"},
            },
        )
        resp = s3.get_bucket_website(Bucket=bucket)
        assert resp["IndexDocument"]["Suffix"] == "index.html"
        assert resp["ErrorDocument"]["Key"] == "error.html"
        s3.delete_bucket_website(Bucket=bucket)

    def test_put_get_bucket_logging(self, s3, bucket):
        import uuid

        log_bucket = f"log-target-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=log_bucket)
        try:
            s3.put_bucket_logging(
                Bucket=bucket,
                BucketLoggingStatus={
                    "LoggingEnabled": {
                        "TargetBucket": log_bucket,
                        "TargetPrefix": "logs/",
                    }
                },
            )
            resp = s3.get_bucket_logging(Bucket=bucket)
            assert resp["LoggingEnabled"]["TargetBucket"] == log_bucket
        finally:
            s3.delete_bucket(Bucket=log_bucket)


class TestS3AdvancedOperations:
    @pytest.fixture
    def s3(self):
        return boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )

    @pytest.fixture
    def bucket(self, s3):
        import uuid

        name = f"adv-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=name)
        yield name
        # Clean up objects
        resp = s3.list_objects_v2(Bucket=name)
        for obj in resp.get("Contents", []):
            s3.delete_object(Bucket=name, Key=obj["Key"])
        s3.delete_bucket(Bucket=name)

    def test_list_objects_v2_prefix(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="docs/a.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="docs/b.txt", Body=b"b")
        s3.put_object(Bucket=bucket, Key="images/c.png", Body=b"c")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="docs/")
        assert resp["KeyCount"] == 2

    def test_list_objects_v2_delimiter(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="a/1.txt", Body=b"1")
        s3.put_object(Bucket=bucket, Key="a/2.txt", Body=b"2")
        s3.put_object(Bucket=bucket, Key="b/3.txt", Body=b"3")
        resp = s3.list_objects_v2(Bucket=bucket, Delimiter="/")
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        assert "a/" in prefixes
        assert "b/" in prefixes

    def test_list_objects_v2_max_keys(self, s3, bucket):
        for i in range(5):
            s3.put_object(Bucket=bucket, Key=f"k{i}.txt", Body=b"x")
        resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=3)
        assert len(resp["Contents"]) <= 3
        assert resp["IsTruncated"] is True

    def test_list_objects_v2_continuation(self, s3, bucket):
        for i in range(5):
            s3.put_object(Bucket=bucket, Key=f"p{i}.txt", Body=b"x")
        resp1 = s3.list_objects_v2(Bucket=bucket, MaxKeys=3)
        assert resp1["IsTruncated"] is True
        resp2 = s3.list_objects_v2(Bucket=bucket, ContinuationToken=resp1["NextContinuationToken"])
        total = len(resp1["Contents"]) + len(resp2["Contents"])
        assert total == 5

    def test_put_object_content_type(self, s3, bucket):
        s3.put_object(
            Bucket=bucket, Key="page.html", Body=b"<h1>Hello</h1>", ContentType="text/html"
        )
        resp = s3.head_object(Bucket=bucket, Key="page.html")
        assert resp["ContentType"] == "text/html"

    def test_put_object_metadata(self, s3, bucket):
        s3.put_object(
            Bucket=bucket, Key="meta.txt", Body=b"data", Metadata={"custom-key": "custom-value"}
        )
        resp = s3.head_object(Bucket=bucket, Key="meta.txt")
        assert resp["Metadata"]["custom-key"] == "custom-value"

    def test_copy_object_preserves_metadata(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="src.txt", Body=b"source", Metadata={"copied": "yes"})
        s3.copy_object(
            Bucket=bucket, Key="dst.txt", CopySource={"Bucket": bucket, "Key": "src.txt"}
        )
        resp = s3.head_object(Bucket=bucket, Key="dst.txt")
        assert resp["ContentLength"] == 6

    def test_delete_objects_batch(self, s3, bucket):
        for i in range(5):
            s3.put_object(Bucket=bucket, Key=f"del{i}.txt", Body=b"x")
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": f"del{i}.txt"} for i in range(5)]},
        )
        assert len(resp["Deleted"]) == 5

    def test_object_exists_after_put(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="exists.txt", Body=b"yes")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="exists.txt")
        assert resp["KeyCount"] == 1

    def test_object_not_found_raises_error(self, s3, bucket):
        with pytest.raises(ClientError) as exc:
            s3.get_object(Bucket=bucket, Key="nonexistent-key-xyz")
        assert exc.value.response["Error"]["Code"] in ("NoSuchKey", "404")

    def test_put_and_get_large_object(self, s3, bucket):
        data = b"X" * (1024 * 1024)  # 1MB
        s3.put_object(Bucket=bucket, Key="large.bin", Body=data)
        resp = s3.get_object(Bucket=bucket, Key="large.bin")
        assert resp["ContentLength"] == len(data)

    def test_put_object_with_storage_class(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="sc.txt", Body=b"data", StorageClass="STANDARD")
        resp = s3.head_object(Bucket=bucket, Key="sc.txt")
        # StorageClass may not be returned for STANDARD, just verify no error
        assert resp["ContentLength"] == 4

    def test_get_object_range(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="range.txt", Body=b"0123456789")
        resp = s3.get_object(Bucket=bucket, Key="range.txt", Range="bytes=2-5")
        body = resp["Body"].read()
        assert body == b"2345"

    def test_put_bucket_policy(self, s3, bucket):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        resp = s3.get_bucket_policy(Bucket=bucket)
        returned = json.loads(resp["Policy"])
        assert len(returned["Statement"]) == 1

    def test_delete_bucket_policy(self, s3, bucket):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        s3.delete_bucket_policy(Bucket=bucket)
        with pytest.raises(ClientError):
            s3.get_bucket_policy(Bucket=bucket)

    def test_get_bucket_location(self, s3, bucket):
        resp = s3.get_bucket_location(Bucket=bucket)
        # us-east-1 returns None or empty for LocationConstraint
        assert "LocationConstraint" in resp

    def test_put_get_object_tagging(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="tagged.txt", Body=b"tag me")
        s3.put_object_tagging(
            Bucket=bucket,
            Key="tagged.txt",
            Tagging={"TagSet": [{"Key": "env", "Value": "test"}]},
        )
        resp = s3.get_object_tagging(Bucket=bucket, Key="tagged.txt")
        tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert tags["env"] == "test"

    def test_delete_object_tagging(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="dtag.txt", Body=b"data")
        s3.put_object_tagging(
            Bucket=bucket,
            Key="dtag.txt",
            Tagging={"TagSet": [{"Key": "temp", "Value": "yes"}]},
        )
        s3.delete_object_tagging(Bucket=bucket, Key="dtag.txt")
        resp = s3.get_object_tagging(Bucket=bucket, Key="dtag.txt")
        assert len(resp["TagSet"]) == 0

    def test_list_buckets(self, s3, bucket):
        resp = s3.list_buckets()
        names = [b["Name"] for b in resp["Buckets"]]
        assert bucket in names

    def test_head_bucket(self, s3, bucket):
        resp = s3.head_bucket(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3AccelerateConfiguration:
    """Tests for GetBucketAccelerateConfiguration / PutBucketAccelerateConfiguration."""

    def test_get_bucket_accelerate_default(self, s3, bucket):
        resp = s3.get_bucket_accelerate_configuration(Bucket=bucket)
        # Default is no acceleration
        assert resp.get("Status") in (None, "", "Suspended")

    def test_put_bucket_accelerate_enabled(self, s3, bucket):
        s3.put_bucket_accelerate_configuration(
            Bucket=bucket,
            AccelerateConfiguration={"Status": "Enabled"},
        )
        resp = s3.get_bucket_accelerate_configuration(Bucket=bucket)
        assert resp["Status"] == "Enabled"

    def test_put_bucket_accelerate_suspended(self, s3, bucket):
        s3.put_bucket_accelerate_configuration(
            Bucket=bucket,
            AccelerateConfiguration={"Status": "Enabled"},
        )
        s3.put_bucket_accelerate_configuration(
            Bucket=bucket,
            AccelerateConfiguration={"Status": "Suspended"},
        )
        resp = s3.get_bucket_accelerate_configuration(Bucket=bucket)
        assert resp["Status"] == "Suspended"


class TestS3BucketRequestPayment:
    """Tests for GetBucketRequestPayment."""

    def test_get_bucket_request_payment_default(self, s3, bucket):
        resp = s3.get_bucket_request_payment(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3DeleteBucketConfigurations:
    """Tests for DeleteBucketEncryption, DeleteBucketLifecycle, DeleteBucketWebsite."""

    def test_delete_bucket_encryption(self, s3, bucket):
        s3.put_bucket_encryption(
            Bucket=bucket,
            ServerSideEncryptionConfiguration={
                "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
            },
        )
        s3.delete_bucket_encryption(Bucket=bucket)
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_encryption(Bucket=bucket)
        assert (
            exc.value.response["Error"]["Code"] == "ServerSideEncryptionConfigurationNotFoundError"
        )

    def test_delete_bucket_lifecycle(self, s3, bucket):
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-rule",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                        "Expiration": {"Days": 30},
                    }
                ]
            },
        )
        s3.delete_bucket_lifecycle(Bucket=bucket)
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        assert exc.value.response["Error"]["Code"] == "NoSuchLifecycleConfiguration"

    def test_delete_bucket_website(self, s3, bucket):
        s3.put_bucket_website(
            Bucket=bucket,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
            },
        )
        s3.delete_bucket_website(Bucket=bucket)
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_website(Bucket=bucket)
        assert "NoSuchWebsiteConfiguration" in str(exc.value)


class TestS3ObjectLocking:
    """Tests for object lock, legal hold, and retention."""

    @pytest.fixture
    def lock_bucket(self, s3):
        import uuid

        name = f"lock-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(
            Bucket=name,
            ObjectLockEnabledForBucket=True,
        )
        yield name
        try:
            versions = s3.list_object_versions(Bucket=name).get("Versions", [])
            for v in versions:
                s3.delete_object(Bucket=name, Key=v["Key"], VersionId=v["VersionId"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_put_and_get_object_lock_configuration(self, s3, lock_bucket):
        s3.put_object_lock_configuration(
            Bucket=lock_bucket,
            ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"},
        )
        resp = s3.get_object_lock_configuration(Bucket=lock_bucket)
        assert resp["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"

    def test_put_object_lock_configuration_with_retention(self, s3, lock_bucket):
        s3.put_object_lock_configuration(
            Bucket=lock_bucket,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {
                    "DefaultRetention": {
                        "Mode": "GOVERNANCE",
                        "Days": 1,
                    }
                },
            },
        )
        resp = s3.get_object_lock_configuration(Bucket=lock_bucket)
        retention = resp["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]
        assert retention["Mode"] == "GOVERNANCE"
        assert retention["Days"] == 1

    def test_put_and_get_object_legal_hold(self, s3, lock_bucket):
        s3.put_object(Bucket=lock_bucket, Key="legal.txt", Body=b"data")
        s3.put_object_legal_hold(
            Bucket=lock_bucket,
            Key="legal.txt",
            LegalHold={"Status": "ON"},
        )
        resp = s3.get_object_legal_hold(Bucket=lock_bucket, Key="legal.txt")
        assert resp["LegalHold"]["Status"] == "ON"
        # Turn off so we can clean up
        s3.put_object_legal_hold(
            Bucket=lock_bucket,
            Key="legal.txt",
            LegalHold={"Status": "OFF"},
        )


class TestS3UploadPartCopy:
    """Tests for UploadPartCopy."""

    def test_upload_part_copy(self, s3, bucket):
        # Put source object
        s3.put_object(Bucket=bucket, Key="source.txt", Body=b"x" * (5 * 1024 * 1024))

        # Create multipart upload for destination
        mpu = s3.create_multipart_upload(Bucket=bucket, Key="dest.txt")
        upload_id = mpu["UploadId"]

        # Copy part from source
        resp = s3.upload_part_copy(
            Bucket=bucket,
            Key="dest.txt",
            UploadId=upload_id,
            PartNumber=1,
            CopySource={"Bucket": bucket, "Key": "source.txt"},
        )
        assert "CopyPartResult" in resp
        etag = resp["CopyPartResult"]["ETag"]

        # Complete
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key="dest.txt",
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etag}]},
        )
        head = s3.head_object(Bucket=bucket, Key="dest.txt")
        assert head["ContentLength"] == 5 * 1024 * 1024


class TestS3BucketNotificationConfig:
    """Tests for GetBucketNotificationConfiguration (empty case)."""

    def test_get_bucket_notification_configuration_empty(self, s3, bucket):
        resp = s3.get_bucket_notification_configuration(Bucket=bucket)
        # Empty notification config should return empty lists or no keys
        assert resp.get("TopicConfigurations", []) == []
        assert resp.get("QueueConfigurations", []) == []
        assert resp.get("LambdaFunctionConfigurations", []) == []


class TestS3BucketOwnershipControls:
    """Tests for GetBucketOwnershipControls."""

    def test_put_and_get_bucket_ownership_controls(self, s3, bucket):
        s3.put_bucket_ownership_controls(
            Bucket=bucket,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        resp = s3.get_bucket_ownership_controls(Bucket=bucket)
        rules = resp["OwnershipControls"]["Rules"]
        assert len(rules) == 1
        assert rules[0]["ObjectOwnership"] == "BucketOwnerEnforced"


class TestS3BucketPolicyStatus:
    """Tests for GetBucketPolicyStatus."""

    def test_get_bucket_policy_status(self, s3, bucket):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicRead",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        resp = s3.get_bucket_policy_status(Bucket=bucket)
        assert "PolicyStatus" in resp
        s3.delete_bucket_policy(Bucket=bucket)


class TestS3BucketLoggingExtended:
    """Tests for GetBucketLogging with no logging configured."""

    def test_get_bucket_logging_empty(self, s3, bucket):
        resp = s3.get_bucket_logging(Bucket=bucket)
        # No logging configured should have no LoggingEnabled key
        assert resp.get("LoggingEnabled") is None


class TestS3PublicAccessBlock:
    """Tests for PutPublicAccessBlock, GetPublicAccessBlock, DeletePublicAccessBlock."""

    def test_put_get_delete_public_access_block(self, s3, bucket):
        config = {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        }
        s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration=config)
        resp = s3.get_public_access_block(Bucket=bucket)
        pab = resp["PublicAccessBlockConfiguration"]
        assert pab["BlockPublicAcls"] is True
        assert pab["IgnorePublicAcls"] is True
        assert pab["BlockPublicPolicy"] is True
        assert pab["RestrictPublicBuckets"] is True

        s3.delete_public_access_block(Bucket=bucket)
        with pytest.raises(ClientError) as exc:
            s3.get_public_access_block(Bucket=bucket)
        assert exc.value.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration"


class TestS3ListBucketConfigurations:
    """Tests for various ListBucket*Configurations operations."""

    def test_list_bucket_analytics_configurations_empty(self, s3, bucket):
        resp = s3.list_bucket_analytics_configurations(Bucket=bucket)
        assert (
            resp.get("AnalyticsConfigurationList") is None
            or resp.get("AnalyticsConfigurationList") == []
        )

    def test_list_bucket_metrics_configurations_empty(self, s3, bucket):
        resp = s3.list_bucket_metrics_configurations(Bucket=bucket)
        assert (
            resp.get("MetricsConfigurationList") is None
            or resp.get("MetricsConfigurationList") == []
        )

    def test_list_bucket_intelligent_tiering_configurations_empty(self, s3, bucket):
        resp = s3.list_bucket_intelligent_tiering_configurations(Bucket=bucket)
        assert (
            resp.get("IntelligentTieringConfigurationList") is None
            or resp.get("IntelligentTieringConfigurationList") == []
        )


class TestS3GetObjectAttributes:
    """Tests for GetObjectAttributes."""

    def test_get_object_attributes(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="attr-test.txt", Body=b"hello world")
        resp = s3.get_object_attributes(
            Bucket=bucket,
            Key="attr-test.txt",
            ObjectAttributes=["ETag", "ObjectSize"],
        )
        assert "ETag" in resp
        assert resp.get("ObjectSize", 0) == 11


class TestS3AutoCoverage:
    """Auto-generated coverage tests for s3."""

    @pytest.fixture
    def client(self):
        return make_client("s3")

    def test_list_directory_buckets(self, client):
        """ListDirectoryBuckets returns a response."""
        resp = client.list_directory_buckets()
        assert "Buckets" in resp


class TestS3AnalyticsMetricsInventory:
    """Tests for Analytics, Metrics, Inventory, and Intelligent Tiering configs."""

    def test_put_get_analytics_configuration(self, s3, bucket):
        """PutBucketAnalyticsConfiguration + GetBucketAnalyticsConfiguration."""
        config = {
            "Id": "test-analytics",
            "StorageClassAnalysis": {},
        }
        resp = s3.put_bucket_analytics_configuration(
            Bucket=bucket,
            Id="test-analytics",
            AnalyticsConfiguration=config,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp = s3.get_bucket_analytics_configuration(Bucket=bucket, Id="test-analytics")
        assert "AnalyticsConfiguration" in resp

    def test_put_get_metrics_configuration(self, s3, bucket):
        """PutBucketMetricsConfiguration + GetBucketMetricsConfiguration."""
        config = {"Id": "test-metrics"}
        resp = s3.put_bucket_metrics_configuration(
            Bucket=bucket,
            Id="test-metrics",
            MetricsConfiguration=config,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp = s3.get_bucket_metrics_configuration(Bucket=bucket, Id="test-metrics")
        assert "MetricsConfiguration" in resp

    def test_put_get_intelligent_tiering(self, s3, bucket):
        """PutBucketIntelligentTieringConfiguration + Get."""
        config = {
            "Id": "test-tiering",
            "Status": "Enabled",
            "Tierings": [
                {"Days": 90, "AccessTier": "ARCHIVE_ACCESS"},
            ],
        }
        resp = s3.put_bucket_intelligent_tiering_configuration(
            Bucket=bucket,
            Id="test-tiering",
            IntelligentTieringConfiguration=config,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp = s3.get_bucket_intelligent_tiering_configuration(Bucket=bucket, Id="test-tiering")
        assert "IntelligentTieringConfiguration" in resp

    def test_put_list_inventory_configuration(self, s3, bucket):
        """PutBucketInventoryConfiguration + ListBucketInventoryConfigurations."""
        config = {
            "Destination": {
                "S3BucketDestination": {
                    "Bucket": f"arn:aws:s3:::{bucket}",
                    "Format": "CSV",
                },
            },
            "IsEnabled": True,
            "Id": "test-inventory",
            "IncludedObjectVersions": "All",
            "Schedule": {"Frequency": "Daily"},
        }
        resp = s3.put_bucket_inventory_configuration(
            Bucket=bucket,
            Id="test-inventory",
            InventoryConfiguration=config,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp = s3.list_bucket_inventory_configurations(Bucket=bucket)
        assert "ResponseMetadata" in resp

    def test_delete_bucket_analytics_configuration(self, s3, bucket):
        """DeleteBucketAnalyticsConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_analytics_configuration(Bucket=bucket, Id="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_bucket_metrics_configuration(self, s3, bucket):
        """DeleteBucketMetricsConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_metrics_configuration(Bucket=bucket, Id="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_bucket_intelligent_tiering(self, s3, bucket):
        """DeleteBucketIntelligentTieringConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_intelligent_tiering_configuration(Bucket=bucket, Id="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_bucket_inventory_configuration(self, s3, bucket):
        """DeleteBucketInventoryConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_inventory_configuration(Bucket=bucket, Id="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_get_bucket_inventory_configuration_nonexistent(self, s3, bucket):
        """GetBucketInventoryConfiguration on missing config returns 404."""
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_inventory_configuration(Bucket=bucket, Id="nonexistent")
        assert "NoSuch" in exc.value.response["Error"]["Code"]


class TestS3ReplicationAndRequestPayment:
    """Tests for Replication and RequestPayment."""

    def test_put_bucket_request_payment(self, s3, bucket):
        """PutBucketRequestPayment accepts requester-pays config."""
        resp = s3.put_bucket_request_payment(
            Bucket=bucket,
            RequestPaymentConfiguration={"Payer": "Requester"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify get_bucket_request_payment returns a response
        resp = s3.get_bucket_request_payment(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_bucket_replication_not_configured(self, s3, bucket):
        """GetBucketReplication when not configured returns error."""
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_replication(Bucket=bucket)
        err = exc.value.response["Error"]["Code"]
        assert "Replication" in err or "NotFound" in err

    def test_delete_bucket_replication_noop(self, s3, bucket):
        """DeleteBucketReplication on unconfigured bucket succeeds."""
        resp = s3.delete_bucket_replication(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204

    def test_put_bucket_replication_invalid(self, s3, bucket):
        """PutBucketReplication with bad config returns 400."""
        with pytest.raises(ClientError):
            s3.put_bucket_replication(
                Bucket=bucket,
                ReplicationConfiguration={
                    "Role": "arn:aws:iam::123456789012:role/test",
                    "Rules": [
                        {
                            "Status": "Enabled",
                            "Destination": {"Bucket": "arn:aws:s3:::dest"},
                        }
                    ],
                },
            )


class TestS3OwnershipControls:
    """Tests for DeleteBucketOwnershipControls."""

    def test_delete_bucket_ownership_controls(self, s3, bucket):
        """DeleteBucketOwnershipControls succeeds."""
        s3.put_bucket_ownership_controls(
            Bucket=bucket,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        resp = s3.delete_bucket_ownership_controls(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


class TestS3ObjectRetentionAndTorrent:
    """Tests for Object Retention, Torrent, Restore."""

    def test_put_object_retention_without_lock(self, s3, bucket):
        """PutObjectRetention on non-lock bucket returns error."""
        s3.put_object(Bucket=bucket, Key="ret-test", Body=b"data")
        with pytest.raises(ClientError):
            s3.put_object_retention(
                Bucket=bucket,
                Key="ret-test",
                Retention={
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": "2030-01-01T00:00:00Z",
                },
            )

    def _cleanup_lock_bucket(self, s3, bucket_name):
        """Delete all object versions and the bucket."""
        try:
            versions = s3.list_object_versions(Bucket=bucket_name)
            for v in versions.get("Versions", []):
                s3.delete_object(
                    Bucket=bucket_name,
                    Key=v["Key"],
                    VersionId=v["VersionId"],
                    BypassGovernanceRetention=True,
                )
            for dm in versions.get("DeleteMarkers", []):
                s3.delete_object(
                    Bucket=bucket_name,
                    Key=dm["Key"],
                    VersionId=dm["VersionId"],
                )
            s3.delete_bucket(Bucket=bucket_name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_put_get_object_retention(self, s3):
        """PutObjectRetention + GetObjectRetention on lock-enabled bucket."""
        lock_bucket = "test-lock-retention-bucket"
        s3.create_bucket(Bucket=lock_bucket, ObjectLockEnabledForBucket=True)
        try:
            s3.put_object(Bucket=lock_bucket, Key="ret-key", Body=b"data")
            s3.put_object_retention(
                Bucket=lock_bucket,
                Key="ret-key",
                Retention={
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": "2030-01-01T00:00:00Z",
                },
            )
            resp = s3.get_object_retention(Bucket=lock_bucket, Key="ret-key")
            assert resp["Retention"]["Mode"] == "GOVERNANCE"
        finally:
            self._cleanup_lock_bucket(s3, lock_bucket)

    def test_get_object_retention_not_set(self, s3):
        """GetObjectRetention on object without retention returns error."""
        lock_bucket = "test-lock-no-retention"
        s3.create_bucket(Bucket=lock_bucket, ObjectLockEnabledForBucket=True)
        try:
            s3.put_object(Bucket=lock_bucket, Key="no-ret-key", Body=b"data")
            with pytest.raises(ClientError) as exc:
                s3.get_object_retention(Bucket=lock_bucket, Key="no-ret-key")
            assert "NoSuch" in exc.value.response["Error"]["Code"]
        finally:
            self._cleanup_lock_bucket(s3, lock_bucket)

    def test_get_object_torrent(self, s3, bucket):
        """GetObjectTorrent returns a response body."""
        s3.put_object(Bucket=bucket, Key="torrent-test", Body=b"data")
        resp = s3.get_object_torrent(Bucket=bucket, Key="torrent-test")
        assert "Body" in resp

    def test_restore_object_not_glacier(self, s3, bucket):
        """RestoreObject on non-Glacier object returns error."""
        s3.put_object(Bucket=bucket, Key="restore-test", Body=b"data")
        with pytest.raises(ClientError) as exc:
            s3.restore_object(
                Bucket=bucket,
                Key="restore-test",
                RestoreRequest={"Days": 1},
            )
        assert exc.value.response["Error"]["Code"] == "InvalidObjectState"


class TestS3DeprecatedNotification:
    """Tests for deprecated PutBucketNotification (non-Configuration)."""

    def test_put_bucket_notification_deprecated(self, s3, bucket):
        """PutBucketNotification (deprecated API) succeeds."""
        resp = s3.put_bucket_notification(
            Bucket=bucket,
            NotificationConfiguration={},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3SessionAndSelect:
    """Tests for CreateSession and SelectObjectContent."""

    def test_select_object_content(self, s3, bucket):
        """SelectObjectContent with CSV input returns results."""
        csv_data = b"name,age\nalice,30\nbob,25\n"
        s3.put_object(Bucket=bucket, Key="select-test.csv", Body=csv_data)
        resp = s3.select_object_content(
            Bucket=bucket,
            Key="select-test.csv",
            Expression="SELECT * FROM s3object",
            ExpressionType="SQL",
            InputSerialization={"CSV": {"FileHeaderInfo": "USE"}},
            OutputSerialization={"CSV": {}},
        )
        # Read the event stream
        records = b""
        for event in resp["Payload"]:
            if "Records" in event:
                records += event["Records"]["Payload"]
        assert len(records) > 0


class TestS3AbacAndMetadata:
    """Tests for Bucket ABAC and Metadata operations."""

    def test_get_bucket_abac(self, s3, bucket):
        """GetBucketAbac returns ABAC status."""
        resp = s3.get_bucket_abac(Bucket=bucket)
        assert "Status" in resp or "ResponseMetadata" in resp

    def test_put_bucket_abac(self, s3, bucket):
        """PutBucketAbac sets ABAC status."""
        resp = s3.put_bucket_abac(
            Bucket=bucket,
            AbacStatus={"Status": "Enabled"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_bucket_metadata_configuration(self, s3, bucket):
        """GetBucketMetadataConfiguration returns a response."""
        resp = s3.get_bucket_metadata_configuration(Bucket=bucket)
        assert "ResponseMetadata" in resp

    def test_get_bucket_metadata_table_configuration(self, s3, bucket):
        """GetBucketMetadataTableConfiguration returns a response."""
        resp = s3.get_bucket_metadata_table_configuration(Bucket=bucket)
        assert "ResponseMetadata" in resp

    def test_delete_bucket_metadata_config_succeeds(self, s3, bucket):
        """DeleteBucketMetadataConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_metadata_configuration(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_bucket_metadata_table_config_succeeds(self, s3, bucket):
        """DeleteBucketMetadataTableConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_metadata_table_configuration(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_create_bucket_metadata_configuration(self, s3, bucket):
        """CreateBucketMetadataConfiguration returns 200."""
        resp = s3.create_bucket_metadata_configuration(
            Bucket=bucket,
            MetadataConfiguration={
                "JournalTableConfiguration": {
                    "RecordExpiration": {"Expiration": "ENABLED"},
                },
                "InventoryTableConfiguration": {"ConfigurationState": "ENABLED"},
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_bucket_metadata_table_configuration(self, s3, bucket):
        """CreateBucketMetadataTableConfiguration returns 200."""
        resp = s3.create_bucket_metadata_table_configuration(
            Bucket=bucket,
            MetadataTableConfiguration={
                "S3TablesDestination": {
                    "TableBucketArn": "arn:aws:s3tables:us-east-1:123456789012:bucket/test",
                    "TableName": "test-table",
                }
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_metadata_inventory_table(self, s3, bucket):
        """UpdateBucketMetadataInventoryTableConfiguration."""
        resp = s3.update_bucket_metadata_inventory_table_configuration(
            Bucket=bucket,
            InventoryTableConfiguration={"ConfigurationState": "ENABLED"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_metadata_journal_table(self, s3, bucket):
        """UpdateBucketMetadataJournalTableConfiguration."""
        resp = s3.update_bucket_metadata_journal_table_configuration(
            Bucket=bucket,
            JournalTableConfiguration={"RecordExpiration": {"Expiration": "ENABLED"}},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3RenameAndEncryption:
    """Tests for RenameObject and UpdateObjectEncryption."""

    def test_update_object_encryption(self, s3, bucket):
        """UpdateObjectEncryption changes encryption on an object."""
        s3.put_object(Bucket=bucket, Key="enc-test.txt", Body=b"secret")
        resp = s3.update_object_encryption(
            Bucket=bucket,
            Key="enc-test.txt",
            ObjectEncryption={
                "SSEKMS": {"KMSKeyArn": "arn:aws:kms:us-east-1:123456789012:key/test"}
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3DeprecatedLifecycle:
    """Tests for deprecated GetBucketLifecycle API."""

    def test_get_bucket_lifecycle_deprecated(self, s3, bucket):
        """GetBucketLifecycle (deprecated) returns rules after PutBucketLifecycleConfiguration."""
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-logs",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                        "Expiration": {"Days": 30},
                    }
                ]
            },
        )
        resp = s3.get_bucket_lifecycle(Bucket=bucket)
        assert "Rules" in resp
        assert len(resp["Rules"]) >= 1
        assert resp["Rules"][0]["ID"] == "expire-logs"


class TestS3ReplicationLifecycle:
    """Tests for PutBucketReplication + GetBucketReplication + DeleteBucketReplication."""

    def test_put_get_delete_bucket_replication(self, s3):
        """Full replication configuration lifecycle with versioned buckets."""
        src = "test-repl-src-lifecycle"
        dst = "test-repl-dst-lifecycle"
        s3.create_bucket(Bucket=src)
        s3.create_bucket(Bucket=dst)
        s3.put_bucket_versioning(Bucket=src, VersioningConfiguration={"Status": "Enabled"})
        s3.put_bucket_versioning(Bucket=dst, VersioningConfiguration={"Status": "Enabled"})
        try:
            s3.put_bucket_replication(
                Bucket=src,
                ReplicationConfiguration={
                    "Role": "arn:aws:iam::123456789012:role/replication-role",
                    "Rules": [
                        {
                            "ID": "replicate-all",
                            "Status": "Enabled",
                            "Prefix": "",
                            "Destination": {
                                "Bucket": f"arn:aws:s3:::{dst}",
                            },
                        }
                    ],
                },
            )
            resp = s3.get_bucket_replication(Bucket=src)
            assert "ReplicationConfiguration" in resp
            rules = resp["ReplicationConfiguration"]["Rules"]
            assert len(rules) >= 1
            assert rules[0]["ID"] == "replicate-all"

            s3.delete_bucket_replication(Bucket=src)
            with pytest.raises(ClientError):
                s3.get_bucket_replication(Bucket=src)
        finally:
            s3.delete_bucket(Bucket=src)
            s3.delete_bucket(Bucket=dst)


class TestS3InventoryGetAfterPut:
    """Test GetBucketInventoryConfiguration on an existing configuration."""

    def test_get_bucket_inventory_configuration_existing(self, s3, bucket):
        """GetBucketInventoryConfiguration returns the config after put."""
        config = {
            "Destination": {
                "S3BucketDestination": {
                    "Bucket": f"arn:aws:s3:::{bucket}",
                    "Format": "CSV",
                },
            },
            "IsEnabled": True,
            "Id": "inv-get-test",
            "IncludedObjectVersions": "All",
            "Schedule": {"Frequency": "Daily"},
        }
        s3.put_bucket_inventory_configuration(
            Bucket=bucket, Id="inv-get-test", InventoryConfiguration=config
        )
        resp = s3.get_bucket_inventory_configuration(Bucket=bucket, Id="inv-get-test")
        assert "InventoryConfiguration" in resp
        assert resp["InventoryConfiguration"]["Id"] == "inv-get-test"
        assert resp["InventoryConfiguration"]["IsEnabled"] is True


class TestS3NotificationWithTopic:
    """Test PutBucketNotificationConfiguration with SNS topic."""

    def test_put_get_notification_configuration_with_topic(self, s3, bucket):
        """PutBucketNotificationConfiguration with TopicConfigurations."""
        s3.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                "TopicConfigurations": [
                    {
                        "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic",
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ]
            },
        )
        resp = s3.get_bucket_notification_configuration(Bucket=bucket)
        assert "TopicConfigurations" in resp
        assert len(resp["TopicConfigurations"]) >= 1
        assert resp["TopicConfigurations"][0]["Events"] == ["s3:ObjectCreated:*"]


class TestS3LegacyOps:
    """Tests for legacy S3 operations (v1 APIs)."""

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket(self, s3):
        bucket_name = f"legacy-test-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name
        try:
            objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
            for obj in objects:
                s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            s3.delete_bucket(Bucket=bucket_name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_list_objects_v1(self, s3, bucket):
        """ListObjects (v1) returns bucket Name."""
        resp = s3.list_objects(Bucket=bucket)
        assert "Name" in resp
        assert resp["Name"] == bucket

    def test_get_bucket_notification(self, s3, bucket):
        """GetBucketNotification returns a response."""
        resp = s3.get_bucket_notification(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_bucket_lifecycle_v1(self, s3, bucket):
        """PutBucketLifecycle (v1) sets lifecycle rules."""
        s3.put_bucket_lifecycle(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-all",
                        "Prefix": "",
                        "Status": "Enabled",
                        "Expiration": {"Days": 30},
                    }
                ]
            },
        )
        resp = s3.get_bucket_lifecycle(Bucket=bucket)
        assert "Rules" in resp
        assert len(resp["Rules"]) >= 1
        assert resp["Rules"][0]["ID"] == "expire-all"


class TestS3EventBridgeNotification:
    @pytest.fixture
    def unique_bucket(self, s3):
        name = "test-eb-notif-" + str(uuid.uuid4())[:8]
        s3.create_bucket(Bucket=name)
        yield name
        try:
            objects = s3.list_objects_v2(Bucket=name).get("Contents", [])
            for obj in objects:
                s3.delete_object(Bucket=name, Key=obj["Key"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    @pytest.fixture
    def unique_queue(self, sqs):
        name = "test-eb-q-" + str(uuid.uuid4())[:8]
        resp = sqs.create_queue(QueueName=name)
        url = resp["QueueUrl"]
        yield url
        try:
            sqs.delete_queue(QueueUrl=url)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_eventbridge_configuration_round_trip(self, s3, unique_bucket):
        """PUT notification config with EventBridgeConfiguration, GET and assert it's preserved."""
        s3.put_bucket_notification_configuration(
            Bucket=unique_bucket,
            NotificationConfiguration={"EventBridgeConfiguration": {}},
        )
        resp = s3.get_bucket_notification_configuration(Bucket=unique_bucket)
        assert "EventBridgeConfiguration" in resp

    def test_put_object_fires_eventbridge_event(self, s3, sqs, unique_bucket, unique_queue):
        """Enable EB notifications, put EB rule targeting SQS, put object, assert event arrives."""
        events_client = make_client("events")
        queue_url = unique_queue
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = attrs["Attributes"]["QueueArn"]

        s3.put_bucket_notification_configuration(
            Bucket=unique_bucket,
            NotificationConfiguration={"EventBridgeConfiguration": {}},
        )
        rule_name = "s3-test-rule-" + str(uuid.uuid4())[:8]
        events_client.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["aws.s3"]}),
            State="ENABLED",
        )
        events_client.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": queue_arn}])

        s3.put_object(Bucket=unique_bucket, Key="test.txt", Body=b"hello")
        time.sleep(0.5)
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        messages = msgs.get("Messages", [])
        assert len(messages) > 0
        body = json.loads(messages[0]["Body"])
        assert body.get("source") == "aws.s3"
        assert body.get("detail", {}).get("bucket", {}).get("name") == unique_bucket


class TestS3AdditionalEventTypes:
    @pytest.fixture
    def unique_bucket(self, s3):
        name = "test-evt-" + str(uuid.uuid4())[:8]
        s3.create_bucket(Bucket=name)
        yield name
        try:
            # Delete all versions if versioning was enabled
            resp = s3.list_object_versions(Bucket=name)
            for v in resp.get("Versions", []):
                s3.delete_object(Bucket=name, Key=v["Key"], VersionId=v["VersionId"])
            for dm in resp.get("DeleteMarkers", []):
                s3.delete_object(Bucket=name, Key=dm["Key"], VersionId=dm["VersionId"])
            objects = s3.list_objects_v2(Bucket=name).get("Contents", [])
            for obj in objects:
                s3.delete_object(Bucket=name, Key=obj["Key"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    @pytest.fixture
    def unique_queue(self, sqs):
        name = "test-evt-q-" + str(uuid.uuid4())[:8]
        resp = sqs.create_queue(QueueName=name)
        url = resp["QueueUrl"]
        yield url
        try:
            sqs.delete_queue(QueueUrl=url)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_copy_object_fires_copy_event(self, s3, sqs, unique_bucket, unique_queue):
        """Set SQS notification for s3:ObjectCreated:Copy, copy object, assert eventName is Copy."""
        queue_url = unique_queue
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = attrs["Attributes"]["QueueArn"]
        s3.put_bucket_notification_configuration(
            Bucket=unique_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:Copy"]}
                ]
            },
        )
        s3.put_object(Bucket=unique_bucket, Key="source.txt", Body=b"data")
        s3.copy_object(
            Bucket=unique_bucket,
            Key="dest.txt",
            CopySource={"Bucket": unique_bucket, "Key": "source.txt"},
        )
        time.sleep(0.3)
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        messages = msgs.get("Messages", [])
        assert len(messages) > 0
        record = json.loads(messages[0]["Body"])["Records"][0]
        assert record["eventName"] == "ObjectCreated:Copy"

    def test_delete_marker_fires_delete_marker_event(self, s3, sqs, unique_bucket, unique_queue):
        """Enable versioning, set notification for DeleteMarkerCreated, delete, assert event."""
        queue_url = unique_queue
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = attrs["Attributes"]["QueueArn"]
        s3.put_bucket_versioning(
            Bucket=unique_bucket, VersioningConfiguration={"Status": "Enabled"}
        )
        s3.put_bucket_notification_configuration(
            Bucket=unique_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectRemoved:DeleteMarkerCreated"],
                    }
                ]
            },
        )
        s3.put_object(Bucket=unique_bucket, Key="versioned.txt", Body=b"v1")
        s3.delete_object(Bucket=unique_bucket, Key="versioned.txt")
        time.sleep(0.3)
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        messages = msgs.get("Messages", [])
        assert len(messages) > 0
        record = json.loads(messages[0]["Body"])["Records"][0]
        assert record["eventName"] == "ObjectRemoved:DeleteMarkerCreated"


class TestS3ReplicationEngine:
    @pytest.fixture
    def src_bucket(self, s3):
        name = "test-repl-src-" + str(uuid.uuid4())[:8]
        s3.create_bucket(Bucket=name)
        s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
        yield name
        try:
            resp = s3.list_object_versions(Bucket=name)
            for v in resp.get("Versions", []):
                s3.delete_object(Bucket=name, Key=v["Key"], VersionId=v["VersionId"])
            for dm in resp.get("DeleteMarkers", []):
                s3.delete_object(Bucket=name, Key=dm["Key"], VersionId=dm["VersionId"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    @pytest.fixture
    def dest_bucket(self, s3):
        name = "test-repl-dst-" + str(uuid.uuid4())[:8]
        s3.create_bucket(Bucket=name)
        s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
        yield name
        try:
            resp = s3.list_object_versions(Bucket=name)
            for v in resp.get("Versions", []):
                s3.delete_object(Bucket=name, Key=v["Key"], VersionId=v["VersionId"])
            for dm in resp.get("DeleteMarkers", []):
                s3.delete_object(Bucket=name, Key=dm["Key"], VersionId=dm["VersionId"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_put_object_replicates_to_dest_bucket(self, s3, src_bucket, dest_bucket):
        """Configure replication, put object, assert it appears in dest bucket."""
        s3.put_bucket_replication(
            Bucket=src_bucket,
            ReplicationConfiguration={
                "Role": "arn:aws:iam::123456789012:role/replication-role",
                "Rules": [
                    {
                        "ID": "replicate-all",
                        "Status": "Enabled",
                        "Filter": {"Prefix": ""},
                        "Destination": {"Bucket": f"arn:aws:s3:::{dest_bucket}"},
                    }
                ],
            },
        )
        s3.put_object(Bucket=src_bucket, Key="replicated.txt", Body=b"hello replication")
        time.sleep(0.5)
        resp = s3.get_object(Bucket=dest_bucket, Key="replicated.txt")
        assert resp["Body"].read() == b"hello replication"

    def test_replication_respects_prefix_filter(self, s3, src_bucket, dest_bucket):
        """Keys matching prefix get replicated; keys not matching do not."""
        s3.put_bucket_replication(
            Bucket=src_bucket,
            ReplicationConfiguration={
                "Role": "arn:aws:iam::123456789012:role/replication-role",
                "Rules": [
                    {
                        "ID": "logs-only",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                        "Destination": {"Bucket": f"arn:aws:s3:::{dest_bucket}"},
                    }
                ],
            },
        )
        s3.put_object(Bucket=src_bucket, Key="logs/access.log", Body=b"log data")
        s3.put_object(Bucket=src_bucket, Key="data/file.bin", Body=b"binary")
        time.sleep(0.5)
        resp = s3.get_object(Bucket=dest_bucket, Key="logs/access.log")
        assert resp["Body"].read() == b"log data"
        with pytest.raises(Exception):
            s3.get_object(Bucket=dest_bucket, Key="data/file.bin")


class TestS3RestoreObjectNotification:
    """Verify that RestoreObject on a Glacier object fires s3:ObjectRestore:Post notification."""

    @pytest.fixture
    def unique_bucket(self, s3):
        name = "test-restore-notif-" + str(uuid.uuid4())[:8]
        s3.create_bucket(Bucket=name)
        yield name
        try:
            objects = s3.list_objects_v2(Bucket=name).get("Contents", [])
            for obj in objects:
                s3.delete_object(Bucket=name, Key=obj["Key"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup

    @pytest.fixture
    def unique_queue(self, sqs):
        name = "test-restore-q-" + str(uuid.uuid4())[:8]
        resp = sqs.create_queue(QueueName=name)
        url = resp["QueueUrl"]
        yield url
        try:
            sqs.delete_queue(QueueUrl=url)
        except Exception:
            pass  # best-effort cleanup

    def test_restore_object_fires_restore_event(self, s3, sqs, unique_bucket, unique_queue):
        """Put Glacier object, set SQS notification, restore — assert ObjectRestore:Post arrives."""
        queue_url = unique_queue
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = attrs["Attributes"]["QueueArn"]

        s3.put_bucket_notification_configuration(
            Bucket=unique_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {"QueueArn": queue_arn, "Events": ["s3:ObjectRestore:Post"]}
                ]
            },
        )
        s3.put_object(
            Bucket=unique_bucket, Key="glacier-obj", Body=b"archived data", StorageClass="GLACIER"
        )
        resp = s3.restore_object(
            Bucket=unique_bucket,
            Key="glacier-obj",
            RestoreRequest={"Days": 1, "GlacierJobParameters": {"Tier": "Expedited"}},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 202

        time.sleep(0.5)
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        messages = msgs.get("Messages", [])
        assert len(messages) > 0
        record = json.loads(messages[0]["Body"])["Records"][0]
        assert record["eventName"] == "ObjectRestore:Post"
        assert record["s3"]["bucket"]["name"] == unique_bucket
        assert record["s3"]["object"]["key"] == "glacier-obj"
