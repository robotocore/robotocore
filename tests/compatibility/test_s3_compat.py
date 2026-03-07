"""S3 compatibility tests — verify robotocore matches LocalStack behavior."""

import json
import os
import time
from urllib.request import Request as URLRequest
from urllib.request import urlopen

import boto3
import pytest

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
    bucket_name = "test-compat-bucket"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass


class TestS3BasicOperations:
    def test_create_bucket(self, s3):
        bucket_name = "test-create-bucket"
        response = s3.create_bucket(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        s3.delete_bucket(Bucket=bucket_name)

    def test_put_and_get_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="hello.txt", Body=b"hello world")
        response = s3.get_object(Bucket=bucket, Key="hello.txt")
        assert response["Body"].read() == b"hello world"

    def test_list_objects(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="a.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="b.txt", Body=b"b")
        response = s3.list_objects_v2(Bucket=bucket)
        keys = [obj["Key"] for obj in response["Contents"]]
        assert sorted(keys) == ["a.txt", "b.txt"]

    def test_delete_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="delete-me.txt", Body=b"bye")
        s3.delete_object(Bucket=bucket, Key="delete-me.txt")
        response = s3.list_objects_v2(Bucket=bucket)
        assert response.get("Contents") is None or len(response["Contents"]) == 0

    def test_head_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="meta.txt", Body=b"metadata test")
        response = s3.head_object(Bucket=bucket, Key="meta.txt")
        assert response["ContentLength"] == 13

    def test_copy_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="original.txt", Body=b"original")
        s3.copy_object(
            Bucket=bucket,
            Key="copy.txt",
            CopySource={"Bucket": bucket, "Key": "original.txt"},
        )
        response = s3.get_object(Bucket=bucket, Key="copy.txt")
        assert response["Body"].read() == b"original"

    def test_list_buckets(self, s3, bucket):
        response = s3.list_buckets()
        names = [b["Name"] for b in response["Buckets"]]
        assert bucket in names

    def test_put_object_with_content_type(self, s3, bucket):
        s3.put_object(
            Bucket=bucket,
            Key="page.html",
            Body=b"<h1>hi</h1>",
            ContentType="text/html",
        )
        response = s3.head_object(Bucket=bucket, Key="page.html")
        assert response["ContentType"] == "text/html"

    def test_list_objects_with_prefix(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="dir/a.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="dir/b.txt", Body=b"b")
        s3.put_object(Bucket=bucket, Key="other.txt", Body=b"c")
        response = s3.list_objects_v2(Bucket=bucket, Prefix="dir/")
        keys = [obj["Key"] for obj in response["Contents"]]
        assert sorted(keys) == ["dir/a.txt", "dir/b.txt"]

    def test_delete_objects_batch(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="del1.txt", Body=b"1")
        s3.put_object(Bucket=bucket, Key="del2.txt", Body=b"2")
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": "del1.txt"}, {"Key": "del2.txt"}]},
        )
        assert len(response.get("Deleted", [])) == 2


class TestS3Multipart:
    def test_multipart_upload(self, s3, bucket):
        # Create multipart upload
        response = s3.create_multipart_upload(Bucket=bucket, Key="large.bin")
        upload_id = response["UploadId"]

        # Upload parts
        part1 = s3.upload_part(
            Bucket=bucket,
            Key="large.bin",
            UploadId=upload_id,
            PartNumber=1,
            Body=b"a" * (5 * 1024 * 1024),
        )
        part2 = s3.upload_part(
            Bucket=bucket,
            Key="large.bin",
            UploadId=upload_id,
            PartNumber=2,
            Body=b"b" * 1024,
        )

        # Complete
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key="large.bin",
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": part1["ETag"]},
                    {"PartNumber": 2, "ETag": part2["ETag"]},
                ]
            },
        )

        # Verify
        response = s3.head_object(Bucket=bucket, Key="large.bin")
        assert response["ContentLength"] == 5 * 1024 * 1024 + 1024

    def test_abort_multipart_upload(self, s3, bucket):
        response = s3.create_multipart_upload(Bucket=bucket, Key="abort.bin")
        upload_id = response["UploadId"]
        s3.abort_multipart_upload(
            Bucket=bucket,
            Key="abort.bin",
            UploadId=upload_id,
        )

    def test_list_parts(self, s3, bucket):
        response = s3.create_multipart_upload(Bucket=bucket, Key="parts.bin")
        upload_id = response["UploadId"]

        s3.upload_part(
            Bucket=bucket,
            Key="parts.bin",
            UploadId=upload_id,
            PartNumber=1,
            Body=b"a" * (5 * 1024 * 1024),
        )
        s3.upload_part(
            Bucket=bucket,
            Key="parts.bin",
            UploadId=upload_id,
            PartNumber=2,
            Body=b"b" * 1024,
        )

        parts_resp = s3.list_parts(Bucket=bucket, Key="parts.bin", UploadId=upload_id)
        parts = parts_resp["Parts"]
        assert len(parts) == 2
        assert parts[0]["PartNumber"] == 1
        assert parts[1]["PartNumber"] == 2

        s3.abort_multipart_upload(Bucket=bucket, Key="parts.bin", UploadId=upload_id)

    def test_list_multipart_uploads(self, s3, bucket):
        resp1 = s3.create_multipart_upload(Bucket=bucket, Key="multi1.bin")
        resp2 = s3.create_multipart_upload(Bucket=bucket, Key="multi2.bin")

        listing = s3.list_multipart_uploads(Bucket=bucket)
        upload_keys = [u["Key"] for u in listing.get("Uploads", [])]
        assert "multi1.bin" in upload_keys
        assert "multi2.bin" in upload_keys

        s3.abort_multipart_upload(
            Bucket=bucket, Key="multi1.bin", UploadId=resp1["UploadId"]
        )
        s3.abort_multipart_upload(
            Bucket=bucket, Key="multi2.bin", UploadId=resp2["UploadId"]
        )


class TestS3Versioning:
    def test_put_and_get_bucket_versioning(self, s3, bucket):
        s3.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )
        response = s3.get_bucket_versioning(Bucket=bucket)
        assert response["Status"] == "Enabled"

    def test_versioned_objects(self, s3, bucket):
        s3.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

        s3.put_object(Bucket=bucket, Key="versioned.txt", Body=b"v1")
        s3.put_object(Bucket=bucket, Key="versioned.txt", Body=b"v2")

        versions = s3.list_object_versions(Bucket=bucket, Prefix="versioned.txt")
        version_list = versions.get("Versions", [])
        assert len(version_list) == 2

        # Latest version should be v2
        latest = s3.get_object(Bucket=bucket, Key="versioned.txt")
        assert latest["Body"].read() == b"v2"

    def test_get_specific_version(self, s3, bucket):
        s3.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

        resp1 = s3.put_object(Bucket=bucket, Key="ver.txt", Body=b"first")
        v1_id = resp1["VersionId"]
        s3.put_object(Bucket=bucket, Key="ver.txt", Body=b"second")

        obj = s3.get_object(Bucket=bucket, Key="ver.txt", VersionId=v1_id)
        assert obj["Body"].read() == b"first"

    def test_delete_versioned_object_creates_marker(self, s3, bucket):
        s3.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )
        s3.put_object(Bucket=bucket, Key="delver.txt", Body=b"will be deleted")
        s3.delete_object(Bucket=bucket, Key="delver.txt")

        versions = s3.list_object_versions(Bucket=bucket, Prefix="delver.txt")
        markers = versions.get("DeleteMarkers", [])
        assert len(markers) >= 1

    def test_suspend_versioning(self, s3, bucket):
        s3.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )
        s3.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Suspended"},
        )
        response = s3.get_bucket_versioning(Bucket=bucket)
        assert response["Status"] == "Suspended"


class TestS3ObjectMetadata:
    def test_head_object_content_type(self, s3, bucket):
        s3.put_object(
            Bucket=bucket,
            Key="typed.json",
            Body=b'{"a":1}',
            ContentType="application/json",
        )
        response = s3.head_object(Bucket=bucket, Key="typed.json")
        assert response["ContentType"] == "application/json"
        assert response["ContentLength"] == 7

    def test_head_object_custom_metadata(self, s3, bucket):
        s3.put_object(
            Bucket=bucket,
            Key="meta.txt",
            Body=b"data",
            Metadata={"author": "test-user", "version": "42"},
        )
        response = s3.head_object(Bucket=bucket, Key="meta.txt")
        assert response["Metadata"]["author"] == "test-user"
        assert response["Metadata"]["version"] == "42"

    def test_copy_object_preserves_body(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="src.txt", Body=b"copy me please")
        s3.copy_object(
            Bucket=bucket,
            Key="dst.txt",
            CopySource={"Bucket": bucket, "Key": "src.txt"},
        )
        obj = s3.get_object(Bucket=bucket, Key="dst.txt")
        assert obj["Body"].read() == b"copy me please"

    def test_copy_object_across_buckets(self, s3, bucket):
        other_bucket = "test-copy-target-bucket"
        s3.create_bucket(Bucket=other_bucket)
        try:
            s3.put_object(Bucket=bucket, Key="cross.txt", Body=b"cross-bucket")
            s3.copy_object(
                Bucket=other_bucket,
                Key="cross-copy.txt",
                CopySource={"Bucket": bucket, "Key": "cross.txt"},
            )
            obj = s3.get_object(Bucket=other_bucket, Key="cross-copy.txt")
            assert obj["Body"].read() == b"cross-bucket"
        finally:
            try:
                s3.delete_object(Bucket=other_bucket, Key="cross-copy.txt")
                s3.delete_bucket(Bucket=other_bucket)
            except Exception:
                pass

    def test_copy_object_with_new_metadata(self, s3, bucket):
        s3.put_object(
            Bucket=bucket,
            Key="orig.txt",
            Body=b"hello",
            Metadata={"old": "val"},
        )
        s3.copy_object(
            Bucket=bucket,
            Key="replaced.txt",
            CopySource={"Bucket": bucket, "Key": "orig.txt"},
            Metadata={"new": "val2"},
            MetadataDirective="REPLACE",
        )
        response = s3.head_object(Bucket=bucket, Key="replaced.txt")
        assert response["Metadata"].get("new") == "val2"
        assert "old" not in response["Metadata"]


class TestS3BucketCors:
    def test_put_get_delete_bucket_cors(self, s3, bucket):
        cors_config = {
            "CORSRules": [
                {
                    "AllowedHeaders": ["*"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedOrigins": ["https://example.com"],
                    "MaxAgeSeconds": 3600,
                }
            ]
        }
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors_config)

        response = s3.get_bucket_cors(Bucket=bucket)
        rules = response["CORSRules"]
        assert len(rules) == 1
        assert "GET" in rules[0]["AllowedMethods"]
        assert "PUT" in rules[0]["AllowedMethods"]
        assert rules[0]["AllowedOrigins"] == ["https://example.com"]

        s3.delete_bucket_cors(Bucket=bucket)
        with pytest.raises(s3.exceptions.ClientError) as exc_info:
            s3.get_bucket_cors(Bucket=bucket)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchCORSConfiguration"

    def test_cors_multiple_rules(self, s3, bucket):
        cors_config = {
            "CORSRules": [
                {
                    "AllowedHeaders": ["Authorization"],
                    "AllowedMethods": ["GET"],
                    "AllowedOrigins": ["https://one.example.com"],
                },
                {
                    "AllowedHeaders": ["*"],
                    "AllowedMethods": ["PUT", "POST"],
                    "AllowedOrigins": ["https://two.example.com"],
                    "ExposeHeaders": ["x-amz-request-id"],
                },
            ]
        }
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors_config)

        response = s3.get_bucket_cors(Bucket=bucket)
        assert len(response["CORSRules"]) == 2


class TestS3BucketTagging:
    def test_put_get_delete_bucket_tagging(self, s3, bucket):
        tag_set = {
            "TagSet": [
                {"Key": "env", "Value": "test"},
                {"Key": "project", "Value": "robotocore"},
            ]
        }
        s3.put_bucket_tagging(Bucket=bucket, Tagging=tag_set)

        response = s3.get_bucket_tagging(Bucket=bucket)
        tags = {t["Key"]: t["Value"] for t in response["TagSet"]}
        assert tags["env"] == "test"
        assert tags["project"] == "robotocore"

        s3.delete_bucket_tagging(Bucket=bucket)
        with pytest.raises(s3.exceptions.ClientError) as exc_info:
            s3.get_bucket_tagging(Bucket=bucket)
        assert "NoSuchTagSet" in exc_info.value.response["Error"]["Code"]


class TestS3ObjectTagging:
    def test_put_get_delete_object_tagging(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="tagged.txt", Body=b"tagged")

        tag_set = {
            "TagSet": [
                {"Key": "status", "Value": "active"},
                {"Key": "tier", "Value": "free"},
            ]
        }
        s3.put_object_tagging(Bucket=bucket, Key="tagged.txt", Tagging=tag_set)

        response = s3.get_object_tagging(Bucket=bucket, Key="tagged.txt")
        tags = {t["Key"]: t["Value"] for t in response["TagSet"]}
        assert tags["status"] == "active"
        assert tags["tier"] == "free"

        s3.delete_object_tagging(Bucket=bucket, Key="tagged.txt")
        response = s3.get_object_tagging(Bucket=bucket, Key="tagged.txt")
        assert len(response["TagSet"]) == 0


class TestS3BucketLifecycle:
    def test_put_and_get_lifecycle_configuration(self, s3, bucket):
        lifecycle = {
            "Rules": [
                {
                    "ID": "expire-old",
                    "Filter": {"Prefix": "logs/"},
                    "Status": "Enabled",
                    "Expiration": {"Days": 30},
                }
            ]
        }
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration=lifecycle,
        )

        response = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = response["Rules"]
        assert len(rules) == 1
        assert rules[0]["ID"] == "expire-old"
        assert rules[0]["Expiration"]["Days"] == 30
        assert rules[0]["Status"] == "Enabled"

    def test_lifecycle_multiple_rules(self, s3, bucket):
        lifecycle = {
            "Rules": [
                {
                    "ID": "expire-logs",
                    "Filter": {"Prefix": "logs/"},
                    "Status": "Enabled",
                    "Expiration": {"Days": 7},
                },
                {
                    "ID": "archive-data",
                    "Filter": {"Prefix": "data/"},
                    "Status": "Enabled",
                    "Transitions": [
                        {"Days": 90, "StorageClass": "GLACIER"},
                    ],
                },
            ]
        }
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration=lifecycle,
        )

        response = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = response["Rules"]
        assert len(rules) == 2
        rule_ids = {r["ID"] for r in rules}
        assert rule_ids == {"expire-logs", "archive-data"}


class TestS3BucketPolicy:
    def test_put_get_delete_bucket_policy(self, s3, bucket):
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

        response = s3.get_bucket_policy(Bucket=bucket)
        returned_policy = json.loads(response["Policy"])
        assert returned_policy["Statement"][0]["Sid"] == "PublicRead"
        assert returned_policy["Statement"][0]["Effect"] == "Allow"

        s3.delete_bucket_policy(Bucket=bucket)
        with pytest.raises(s3.exceptions.ClientError) as exc_info:
            s3.get_bucket_policy(Bucket=bucket)
        err_code = exc_info.value.response["Error"]["Code"]
        assert "NoSuchBucketPolicy" in err_code or "404" in str(
            exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
        )


class TestS3EventNotifications:
    def test_put_object_triggers_sqs_notification(self, s3, sqs):
        bucket_name = "notif-test-s3"
        s3.create_bucket(Bucket=bucket_name)
        q_url = sqs.create_queue(QueueName="s3-event-test")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": q_arn,
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ],
            },
        )

        s3.put_object(Bucket=bucket_name, Key="notify.txt", Body=b"hello")

        time.sleep(0.5)
        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        record = body["Records"][0]
        assert record["eventName"] == "Put"
        assert record["s3"]["bucket"]["name"] == bucket_name
        assert record["s3"]["object"]["key"] == "notify.txt"

        s3.delete_object(Bucket=bucket_name, Key="notify.txt")
        s3.delete_bucket(Bucket=bucket_name)
        sqs.delete_queue(QueueUrl=q_url)

    def test_delete_object_triggers_notification(self, s3, sqs):
        bucket_name = "notif-delete-s3"
        s3.create_bucket(Bucket=bucket_name)
        q_url = sqs.create_queue(QueueName="s3-delete-event")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": q_arn,
                        "Events": ["s3:ObjectRemoved:*"],
                    }
                ],
            },
        )

        s3.put_object(Bucket=bucket_name, Key="will-delete.txt", Body=b"bye")
        s3.delete_object(Bucket=bucket_name, Key="will-delete.txt")

        time.sleep(0.5)
        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["Records"][0]["eventName"] == "Delete"

        s3.delete_bucket(Bucket=bucket_name)
        sqs.delete_queue(QueueUrl=q_url)

    def test_prefix_filter(self, s3, sqs):
        bucket_name = "notif-prefix-s3"
        s3.create_bucket(Bucket=bucket_name)
        q_url = sqs.create_queue(QueueName="s3-prefix-event")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": q_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {"FilterRules": [{"Name": "prefix", "Value": "images/"}]}
                        },
                    }
                ],
            },
        )

        # This should NOT trigger (wrong prefix)
        s3.put_object(Bucket=bucket_name, Key="docs/readme.txt", Body=b"no")
        # This SHOULD trigger
        s3.put_object(Bucket=bucket_name, Key="images/photo.jpg", Body=b"yes")

        time.sleep(0.5)
        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2, MaxNumberOfMessages=10)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["Records"][0]["s3"]["object"]["key"] == "images/photo.jpg"

        s3.delete_object(Bucket=bucket_name, Key="docs/readme.txt")
        s3.delete_object(Bucket=bucket_name, Key="images/photo.jpg")
        s3.delete_bucket(Bucket=bucket_name)
        sqs.delete_queue(QueueUrl=q_url)


class TestS3PresignedUrls:
    def test_presigned_get_url(self, s3, bucket):
        """Test generating and using a presigned GET URL."""
        s3.put_object(Bucket=bucket, Key="presigned-get.txt", Body=b"hello presigned")

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": "presigned-get.txt"},
            ExpiresIn=3600,
        )
        # The URL should point at our local endpoint
        assert ENDPOINT_URL.split("://")[1] in url

        req = URLRequest(url, method="GET")
        resp = urlopen(req)
        assert resp.status == 200
        assert resp.read() == b"hello presigned"

    def test_presigned_put_url(self, s3, bucket):
        """Test generating and using a presigned PUT URL."""
        url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": "presigned-upload.txt"},
            ExpiresIn=3600,
        )

        req = URLRequest(url, data=b"uploaded via presigned", method="PUT")
        req.add_header("Content-Type", "application/octet-stream")
        resp = urlopen(req)
        assert resp.status == 200

        obj = s3.get_object(Bucket=bucket, Key="presigned-upload.txt")
        assert obj["Body"].read() == b"uploaded via presigned"

    def test_presigned_head_url(self, s3, bucket):
        """Test generating and using a presigned HEAD URL."""
        s3.put_object(Bucket=bucket, Key="presigned-head.txt", Body=b"check me")

        url = s3.generate_presigned_url(
            "head_object",
            Params={"Bucket": bucket, "Key": "presigned-head.txt"},
            ExpiresIn=3600,
        )

        req = URLRequest(url, method="HEAD")
        resp = urlopen(req)
        assert resp.status == 200
        assert resp.headers.get("Content-Length") == "8"

    def test_presigned_get_url_with_response_headers(self, s3, bucket):
        """Test presigned GET URL with response content-disposition override."""
        s3.put_object(Bucket=bucket, Key="download.txt", Body=b"download content")

        url = s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": "download.txt",
                "ResponseContentDisposition": "attachment; filename=out.txt",
            },
            ExpiresIn=3600,
        )

        req = URLRequest(url, method="GET")
        resp = urlopen(req)
        assert resp.status == 200
        assert resp.read() == b"download content"

    def test_presigned_delete_url(self, s3, bucket):
        """Test generating and using a presigned DELETE URL."""
        s3.put_object(Bucket=bucket, Key="presigned-delete.txt", Body=b"delete me")

        url = s3.generate_presigned_url(
            "delete_object",
            Params={"Bucket": bucket, "Key": "presigned-delete.txt"},
            ExpiresIn=3600,
        )

        req = URLRequest(url, method="DELETE")
        resp = urlopen(req)
        assert resp.status == 204

        # Verify object is gone
        response = s3.list_objects_v2(Bucket=bucket, Prefix="presigned-delete.txt")
        assert response.get("Contents") is None or len(response["Contents"]) == 0
