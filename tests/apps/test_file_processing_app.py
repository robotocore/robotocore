"""
File Processing Application Tests

Simulates a file processing service that uploads documents to S3,
stores metadata in DynamoDB, and supports versioned file updates.
"""

import hashlib
import json
import uuid

import pytest


@pytest.fixture
def upload_bucket(s3, unique_name):
    bucket = f"user-uploads-{unique_name}"
    s3.create_bucket(Bucket=bucket)
    yield bucket
    # Cleanup: delete all objects (including versions) then bucket
    try:
        versions = s3.list_object_versions(Bucket=bucket).get("Versions", [])
        for v in versions:
            s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
        delete_markers = s3.list_object_versions(Bucket=bucket).get("DeleteMarkers", [])
        for dm in delete_markers:
            s3.delete_object(Bucket=bucket, Key=dm["Key"], VersionId=dm["VersionId"])
    except Exception:
        pass
    try:
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except Exception:
        pass
    s3.delete_bucket(Bucket=bucket)


@pytest.fixture
def metadata_table(dynamodb, unique_name):
    table_name = f"file-metadata-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "file_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "file_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


class TestFileProcessingApp:
    def test_upload_and_list_files(self, s3, upload_bucket):
        """Upload 3 files to the user-uploads bucket and verify they appear in listing."""
        files = {
            "reports/quarterly-2024-q1.pdf": b"Q1 financial report",
            "reports/quarterly-2024-q2.pdf": b"Q2 financial report",
            "invoices/inv-20240315.pdf": b"Invoice #20240315",
        }
        for key, body in files.items():
            s3.put_object(Bucket=upload_bucket, Key=key, Body=body)

        response = s3.list_objects_v2(Bucket=upload_bucket)
        assert response["KeyCount"] == 3
        listed_keys = {obj["Key"] for obj in response["Contents"]}
        assert listed_keys == set(files.keys())

    def test_file_metadata_storage(self, s3, dynamodb, upload_bucket, metadata_table):
        """Upload a file to S3, store metadata in DynamoDB, read back and verify."""
        file_id = str(uuid.uuid4())
        file_body = b"Employee handbook v3.2"
        file_key = f"docs/{file_id}/handbook.pdf"

        s3.put_object(Bucket=upload_bucket, Key=file_key, Body=file_body)

        dynamodb.put_item(
            TableName=metadata_table,
            Item={
                "file_id": {"S": file_id},
                "bucket": {"S": upload_bucket},
                "key": {"S": file_key},
                "size_bytes": {"N": str(len(file_body))},
                "content_type": {"S": "application/pdf"},
                "uploaded_by": {"S": "user-jane-doe"},
            },
        )

        result = dynamodb.get_item(
            TableName=metadata_table,
            Key={"file_id": {"S": file_id}},
        )
        item = result["Item"]
        assert item["bucket"]["S"] == upload_bucket
        assert item["key"]["S"] == file_key
        assert item["size_bytes"]["N"] == str(len(file_body))
        assert item["uploaded_by"]["S"] == "user-jane-doe"

    def test_presigned_url_download(self, s3, upload_bucket):
        """Upload object, generate presigned GET URL, verify URL structure."""
        s3.put_object(
            Bucket=upload_bucket,
            Key="shared/report.csv",
            Body=b"id,name\n1,Alice\n2,Bob",
        )

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": upload_bucket, "Key": "shared/report.csv"},
            ExpiresIn=3600,
        )

        assert upload_bucket in url
        assert "shared/report.csv" in url
        assert "Signature" in url or "X-Amz-Signature" in url

    def test_multipart_upload(self, s3, upload_bucket):
        """Upload a 10MB file via multipart, download, verify content hash."""
        key = "large-files/dataset.bin"
        # S3 requires minimum 5MB per part (except last), so use 5MB chunks
        chunk_a = b"A" * (5 * 1024 * 1024)
        chunk_b = b"B" * (5 * 1024 * 1024)
        expected_hash = hashlib.md5(chunk_a + chunk_b).hexdigest()

        mpu = s3.create_multipart_upload(Bucket=upload_bucket, Key=key)
        upload_id = mpu["UploadId"]

        part1 = s3.upload_part(
            Bucket=upload_bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=chunk_a,
        )
        part2 = s3.upload_part(
            Bucket=upload_bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=2,
            Body=chunk_b,
        )

        s3.complete_multipart_upload(
            Bucket=upload_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": part1["ETag"]},
                    {"PartNumber": 2, "ETag": part2["ETag"]},
                ]
            },
        )

        downloaded = s3.get_object(Bucket=upload_bucket, Key=key)
        body = downloaded["Body"].read()
        assert len(body) == 10 * 1024 * 1024
        assert hashlib.md5(body).hexdigest() == expected_hash

    def test_versioned_file_updates(self, s3, upload_bucket):
        """Enable versioning, upload same key 3 times, list versions."""
        s3.put_bucket_versioning(
            Bucket=upload_bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

        key = "config/app-settings.json"
        versions_content = [
            json.dumps({"debug": True, "version": 1}),
            json.dumps({"debug": False, "version": 2}),
            json.dumps({"debug": False, "version": 3, "feature_flags": ["new-ui"]}),
        ]

        for content in versions_content:
            s3.put_object(Bucket=upload_bucket, Key=key, Body=content.encode())

        response = s3.list_object_versions(Bucket=upload_bucket, Prefix=key)
        versions = response.get("Versions", [])
        assert len(versions) == 3

        # Latest version should have the feature_flags
        latest = s3.get_object(Bucket=upload_bucket, Key=key)
        body = json.loads(latest["Body"].read())
        assert body["version"] == 3
        assert "feature_flags" in body
