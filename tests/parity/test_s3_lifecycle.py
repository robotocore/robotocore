"""S3 lifecycle parity tests.

Tests S3 CRUD operations, presigned URLs, versioning, and multipart upload.
These cover core S3 operations that any AWS-compatible emulator must support.
"""

import uuid

import requests


def _bucket_name():
    return f"parity-s3-{uuid.uuid4().hex[:12]}"


class TestS3CrudOperations:
    """Basic S3 object CRUD: create bucket, put/get/list/delete objects."""

    def test_put_get_delete_object(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        key = "test-key.txt"
        body = b"hello world"

        try:
            s3.create_bucket(Bucket=bucket)

            # Put object
            s3.put_object(Bucket=bucket, Key=key, Body=body)

            # Get object
            resp = s3.get_object(Bucket=bucket, Key=key)
            assert resp["Body"].read() == body
            assert resp["ContentLength"] == len(body)

            # List objects
            listing = s3.list_objects_v2(Bucket=bucket)
            assert listing["KeyCount"] == 1
            assert listing["Contents"][0]["Key"] == key

            # Delete object
            s3.delete_object(Bucket=bucket, Key=key)

            # Verify deleted
            listing = s3.list_objects_v2(Bucket=bucket)
            assert listing["KeyCount"] == 0
        finally:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_multiple_objects_and_prefix_listing(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        keys = ["docs/a.txt", "docs/b.txt", "images/c.png", "root.txt"]

        try:
            s3.create_bucket(Bucket=bucket)

            for key in keys:
                s3.put_object(Bucket=bucket, Key=key, Body=b"content")

            # List all
            listing = s3.list_objects_v2(Bucket=bucket)
            assert listing["KeyCount"] == 4

            # List with prefix
            listing = s3.list_objects_v2(Bucket=bucket, Prefix="docs/")
            assert listing["KeyCount"] == 2
            listed_keys = [obj["Key"] for obj in listing["Contents"]]
            assert sorted(listed_keys) == ["docs/a.txt", "docs/b.txt"]

            # List with delimiter (common prefixes)
            listing = s3.list_objects_v2(Bucket=bucket, Delimiter="/")
            assert len(listing.get("CommonPrefixes", [])) == 2
            prefixes = sorted(p["Prefix"] for p in listing["CommonPrefixes"])
            assert prefixes == ["docs/", "images/"]
        finally:
            for key in keys:
                try:
                    s3.delete_object(Bucket=bucket, Key=key)
                except Exception:
                    pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_head_object_metadata(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        key = "meta-test.txt"

        try:
            s3.create_bucket(Bucket=bucket)
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=b"data",
                ContentType="text/plain",
                Metadata={"custom-key": "custom-value"},
            )

            head = s3.head_object(Bucket=bucket, Key=key)
            assert head["ContentType"] == "text/plain"
            assert head["Metadata"]["custom-key"] == "custom-value"
            assert head["ContentLength"] == 4
        finally:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup


class TestS3PresignedUrls:
    """Presigned URL upload and download."""

    def test_presigned_get(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        key = "presigned-get.txt"
        body = b"presigned content"

        try:
            s3.create_bucket(Bucket=bucket)
            s3.put_object(Bucket=bucket, Key=key, Body=body)

            url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=300,
            )

            resp = requests.get(url)
            assert resp.status_code == 200
            assert resp.content == body
        finally:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_presigned_put(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        key = "presigned-put.txt"
        body = b"uploaded via presigned"

        try:
            s3.create_bucket(Bucket=bucket)

            url = s3.generate_presigned_url(
                "put_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=300,
            )

            resp = requests.put(url, data=body)
            assert resp.status_code == 200

            get_resp = s3.get_object(Bucket=bucket, Key=key)
            assert get_resp["Body"].read() == body
        finally:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup


class TestS3Versioning:
    """S3 versioning: enable, put versions, list versions, delete."""

    def test_versioning_lifecycle(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        key = "versioned.txt"

        try:
            s3.create_bucket(Bucket=bucket)

            s3.put_bucket_versioning(
                Bucket=bucket,
                VersioningConfiguration={"Status": "Enabled"},
            )

            versioning = s3.get_bucket_versioning(Bucket=bucket)
            assert versioning["Status"] == "Enabled"

            v1 = s3.put_object(Bucket=bucket, Key=key, Body=b"version 1")
            v1_id = v1["VersionId"]

            v2 = s3.put_object(Bucket=bucket, Key=key, Body=b"version 2")
            v2_id = v2["VersionId"]

            assert v1_id != v2_id

            versions = s3.list_object_versions(Bucket=bucket, Prefix=key)
            version_ids = [v["VersionId"] for v in versions["Versions"]]
            assert v1_id in version_ids
            assert v2_id in version_ids

            resp = s3.get_object(Bucket=bucket, Key=key, VersionId=v1_id)
            assert resp["Body"].read() == b"version 1"

            resp = s3.get_object(Bucket=bucket, Key=key, VersionId=v2_id)
            assert resp["Body"].read() == b"version 2"

            s3.delete_object(Bucket=bucket, Key=key, VersionId=v1_id)

            versions = s3.list_object_versions(Bucket=bucket, Prefix=key)
            remaining = [v["VersionId"] for v in versions["Versions"]]
            assert v1_id not in remaining
            assert v2_id in remaining
        finally:
            try:
                versions = s3.list_object_versions(Bucket=bucket)
                for v in versions.get("Versions", []):
                    s3.delete_object(
                        Bucket=bucket,
                        Key=v["Key"],
                        VersionId=v["VersionId"],
                    )
                for dm in versions.get("DeleteMarkers", []):
                    s3.delete_object(
                        Bucket=bucket,
                        Key=dm["Key"],
                        VersionId=dm["VersionId"],
                    )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup


class TestS3MultipartUpload:
    """S3 multipart upload: initiate, upload parts, complete."""

    def test_multipart_upload(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        key = "multipart.bin"

        part_size = 5 * 1024 * 1024  # 5MB
        part1_data = b"A" * part_size
        part2_data = b"B" * 1024

        try:
            s3.create_bucket(Bucket=bucket)

            mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = mpu["UploadId"]
            assert upload_id

            part1 = s3.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=part1_data,
            )
            part2 = s3.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=2,
                Body=part2_data,
            )

            s3.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": 1, "ETag": part1["ETag"]},
                        {"PartNumber": 2, "ETag": part2["ETag"]},
                    ]
                },
            )

            resp = s3.get_object(Bucket=bucket, Key=key)
            content = resp["Body"].read()
            assert len(content) == part_size + 1024
            assert content[:10] == b"A" * 10
            assert content[-10:] == b"B" * 10
        finally:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_abort_multipart_upload(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        key = "abort-multipart.bin"

        try:
            s3.create_bucket(Bucket=bucket)

            mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = mpu["UploadId"]

            s3.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=b"X" * 1024,
            )

            uploads = s3.list_multipart_uploads(Bucket=bucket)
            upload_ids = [u["UploadId"] for u in uploads.get("Uploads", [])]
            assert upload_id in upload_ids

            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)

            uploads = s3.list_multipart_uploads(Bucket=bucket)
            upload_ids = [u["UploadId"] for u in uploads.get("Uploads", [])]
            assert upload_id not in upload_ids
        finally:
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup


class TestS3CopyObject:
    """S3 copy operations."""

    def test_copy_object(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        src_key = "original.txt"
        dst_key = "copy.txt"
        body = b"copy me"

        try:
            s3.create_bucket(Bucket=bucket)
            s3.put_object(Bucket=bucket, Key=src_key, Body=body)

            s3.copy_object(
                Bucket=bucket,
                Key=dst_key,
                CopySource={"Bucket": bucket, "Key": src_key},
            )

            resp = s3.get_object(Bucket=bucket, Key=dst_key)
            assert resp["Body"].read() == body

            resp = s3.get_object(Bucket=bucket, Key=src_key)
            assert resp["Body"].read() == body
        finally:
            for k in [src_key, dst_key]:
                try:
                    s3.delete_object(Bucket=bucket, Key=k)
                except Exception:
                    pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup


class TestS3Tagging:
    """S3 object tagging."""

    def test_object_tagging(self, aws_client):
        s3 = aws_client.s3
        bucket = _bucket_name()
        key = "tagged.txt"

        try:
            s3.create_bucket(Bucket=bucket)
            s3.put_object(Bucket=bucket, Key=key, Body=b"data")

            s3.put_object_tagging(
                Bucket=bucket,
                Key=key,
                Tagging={
                    "TagSet": [
                        {"Key": "env", "Value": "test"},
                        {"Key": "team", "Value": "platform"},
                    ]
                },
            )

            tags = s3.get_object_tagging(Bucket=bucket, Key=key)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagSet"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "platform"

            s3.delete_object_tagging(Bucket=bucket, Key=key)
            tags = s3.get_object_tagging(Bucket=bucket, Key=key)
            assert len(tags["TagSet"]) == 0
        finally:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup
