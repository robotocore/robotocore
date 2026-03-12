"""
File Processing Service

A production-grade document management service that stores files in S3 with
rich metadata in DynamoDB. Supports versioning, deduplication, lifecycle
management, search, and storage analytics.

This module is framework-agnostic: it receives boto3 clients via its
constructor and has no dependency on any web framework, emulator, or test
harness. You could wire it into Flask, FastAPI, or a CLI tool unchanged.

Architecture:
    Client -> FileProcessingService
                  |          |
                  v          v
                 S3      DynamoDB
              (files)   (metadata + GSIs)
"""

from __future__ import annotations

import hashlib
import mimetypes
import uuid
from datetime import UTC, datetime

from .models import (
    FileMetadata,
    FileStatus,
    FileVersion,
    SearchQuery,
    StorageStats,
    UploadResult,
)

# Default content-type when we cannot detect one
_DEFAULT_CONTENT_TYPE = "application/octet-stream"


class FileProcessingService:
    """Manages file uploads, metadata, versioning, and search.

    All AWS interaction goes through the boto3 clients injected at
    construction time. The service never creates its own clients, so
    switching between robotocore and real AWS is a matter of passing
    different clients.
    """

    def __init__(
        self,
        s3_client,
        dynamodb_client,
        bucket: str,
        table_name: str,
    ) -> None:
        self.s3 = s3_client
        self.ddb = dynamodb_client
        self.bucket = bucket
        self.table_name = table_name

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    def enable_versioning(self) -> None:
        """Turn on S3 bucket versioning."""
        self.s3.put_bucket_versioning(
            Bucket=self.bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def upload_file(
        self,
        key: str,
        body: bytes,
        *,
        content_type: str | None = None,
        tags: list[str] | None = None,
        uploaded_by: str = "system",
        prefix: str = "",
    ) -> UploadResult:
        """Upload a file to S3 and record metadata in DynamoDB.

        If *content_type* is not provided it will be guessed from the key
        extension.  A SHA-256 digest is computed client-side for integrity
        verification.
        """
        if content_type is None:
            content_type = self._detect_content_type(key)

        sha256 = hashlib.sha256(body).hexdigest()
        file_id = str(uuid.uuid4())
        full_key = f"{prefix}{key}" if prefix else key

        put_resp = self.s3.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=body,
            ContentType=content_type,
        )

        version_id = put_resp.get("VersionId")
        etag = put_resp.get("ETag", "").strip('"')
        now = datetime.now(UTC).isoformat()

        meta = FileMetadata(
            file_id=file_id,
            key=full_key,
            bucket=self.bucket,
            size=len(body),
            content_type=content_type,
            sha256=sha256,
            uploaded_at=now,
            status=FileStatus.UPLOADING,
            tags=tags or [],
            version_id=version_id,
            prefix=prefix,
            uploaded_by=uploaded_by,
        )
        self.ddb.put_item(TableName=self.table_name, Item=meta.to_dynamodb_item())

        return UploadResult(
            file_id=file_id,
            key=full_key,
            version_id=version_id,
            etag=etag,
            sha256=sha256,
            size=len(body),
        )

    def upload_files(
        self,
        files: dict[str, bytes],
        *,
        prefix: str = "",
        tags: list[str] | None = None,
        uploaded_by: str = "system",
    ) -> list[UploadResult]:
        """Bulk-upload multiple files under a common prefix."""
        results: list[UploadResult] = []
        for key, body in files.items():
            result = self.upload_file(
                key,
                body,
                prefix=prefix,
                tags=tags,
                uploaded_by=uploaded_by,
            )
            results.append(result)
        return results

    def upload_with_deduplication(
        self,
        key: str,
        body: bytes,
        *,
        tags: list[str] | None = None,
        uploaded_by: str = "system",
        prefix: str = "",
    ) -> UploadResult | FileMetadata:
        """Upload a file, but skip if an identical file (by SHA-256) already exists.

        Returns the existing *FileMetadata* when a duplicate is detected,
        or a fresh *UploadResult* otherwise.
        """
        sha256 = hashlib.sha256(body).hexdigest()

        # Scan for an existing item with matching hash.  In production
        # you'd use a GSI on sha256 -- here we do a simple scan to keep
        # the table schema minimal.
        resp = self.ddb.scan(
            TableName=self.table_name,
            FilterExpression="sha256 = :h",
            ExpressionAttributeValues={":h": {"S": sha256}},
        )
        if resp.get("Items"):
            return FileMetadata.from_dynamodb_item(resp["Items"][0])

        return self.upload_file(key, body, tags=tags, uploaded_by=uploaded_by, prefix=prefix)

    # ------------------------------------------------------------------
    # Download / read
    # ------------------------------------------------------------------

    def download_file(self, key: str, *, version_id: str | None = None) -> bytes:
        """Download file contents from S3."""
        params: dict = {"Bucket": self.bucket, "Key": key}
        if version_id:
            params["VersionId"] = version_id
        resp = self.s3.get_object(**params)
        return resp["Body"].read()

    def get_metadata(self, file_id: str) -> FileMetadata | None:
        """Retrieve metadata for a single file by ID."""
        resp = self.ddb.get_item(
            TableName=self.table_name,
            Key={"file_id": {"S": file_id}},
        )
        item = resp.get("Item")
        if not item:
            return None
        return FileMetadata.from_dynamodb_item(item)

    # ------------------------------------------------------------------
    # Versioning
    # ------------------------------------------------------------------

    def list_versions(self, key: str) -> list[FileVersion]:
        """List all S3 versions of an object, newest first."""
        resp = self.s3.list_object_versions(Bucket=self.bucket, Prefix=key)
        versions = resp.get("Versions", [])
        result: list[FileVersion] = []
        for v in versions:
            if v["Key"] != key:
                continue
            result.append(
                FileVersion(
                    version_id=v["VersionId"],
                    uploaded_at=v["LastModified"].isoformat()
                    if hasattr(v["LastModified"], "isoformat")
                    else str(v["LastModified"]),
                    size=v["Size"],
                    sha256="",  # S3 doesn't store SHA-256 by default
                    is_latest=v.get("IsLatest", False),
                )
            )
        # Sort newest-first
        result.sort(key=lambda fv: fv.uploaded_at, reverse=True)
        return result

    def get_version(self, key: str, version_id: str) -> bytes:
        """Retrieve the contents of a specific version."""
        return self.download_file(key, version_id=version_id)

    def delete_version(self, key: str, version_id: str) -> None:
        """Delete a specific version of an object."""
        self.s3.delete_object(Bucket=self.bucket, Key=key, VersionId=version_id)

    def restore_version(self, key: str, version_id: str) -> UploadResult:
        """Restore a previous version by copying it as the new current version."""
        body = self.download_file(key, version_id=version_id)
        return self.upload_file(key, body)

    # ------------------------------------------------------------------
    # Search and listing
    # ------------------------------------------------------------------

    def list_files(self, prefix: str = "") -> list[FileMetadata]:
        """List file metadata records, optionally filtered by prefix."""
        if prefix:
            resp = self.ddb.scan(
                TableName=self.table_name,
                FilterExpression="begins_with(#k, :p)",
                ExpressionAttributeNames={"#k": "key"},
                ExpressionAttributeValues={":p": {"S": prefix}},
            )
        else:
            resp = self.ddb.scan(TableName=self.table_name)
        return [FileMetadata.from_dynamodb_item(i) for i in resp.get("Items", [])]

    def search(self, query: SearchQuery) -> list[FileMetadata]:
        """Search metadata by multiple criteria (server-side scan + filter).

        In production you would push as much filtering as possible into
        DynamoDB GSI queries.  Here we build a FilterExpression from the
        supplied criteria so the tests exercise real DynamoDB evaluation.
        """
        filter_parts: list[str] = []
        expr_values: dict = {}
        expr_names: dict = {}

        if "content_type" in query:
            filter_parts.append("content_type = :ct")
            expr_values[":ct"] = {"S": query["content_type"]}

        if "status" in query:
            filter_parts.append("#st = :st")
            expr_values[":st"] = {"S": query["status"]}
            expr_names["#st"] = "status"

        if "prefix" in query:
            filter_parts.append("begins_with(#k, :pfx)")
            expr_values[":pfx"] = {"S": query["prefix"]}
            expr_names["#k"] = "key"

        if "date_from" in query:
            filter_parts.append("uploaded_at >= :df")
            expr_values[":df"] = {"S": query["date_from"]}

        if "date_to" in query:
            filter_parts.append("uploaded_at <= :dt")
            expr_values[":dt"] = {"S": query["date_to"]}

        if "tags" in query:
            for i, tag in enumerate(query["tags"]):
                filter_parts.append(f"contains(tags, :tag{i})")
                expr_values[f":tag{i}"] = {"S": tag}

        scan_kwargs: dict = {"TableName": self.table_name}
        if filter_parts:
            scan_kwargs["FilterExpression"] = " AND ".join(filter_parts)
            scan_kwargs["ExpressionAttributeValues"] = expr_values
        if expr_names:
            scan_kwargs["ExpressionAttributeNames"] = expr_names

        resp = self.ddb.scan(**scan_kwargs)
        items = [FileMetadata.from_dynamodb_item(i) for i in resp.get("Items", [])]

        limit = query.get("limit")
        if limit:
            items = items[:limit]
        return items

    # ------------------------------------------------------------------
    # Lifecycle management
    # ------------------------------------------------------------------

    def set_status(self, file_id: str, status: FileStatus) -> None:
        """Transition a file to a new lifecycle status."""
        self.ddb.update_item(
            TableName=self.table_name,
            Key={"file_id": {"S": file_id}},
            UpdateExpression="SET #st = :s",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":s": {"S": status.value}},
        )

    def archive_file(self, file_id: str) -> str | None:
        """Move a file to the archive/ prefix and update its status.

        Returns the new key, or None if the file was not found.
        """
        meta = self.get_metadata(file_id)
        if meta is None:
            return None

        old_key = meta.key
        new_key = f"archive/{old_key}"

        self.s3.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": old_key},
            Key=new_key,
        )
        self.s3.delete_object(Bucket=self.bucket, Key=old_key)

        self.ddb.update_item(
            TableName=self.table_name,
            Key={"file_id": {"S": file_id}},
            UpdateExpression="SET #k = :nk, #st = :s, prefix = :p",
            ExpressionAttributeNames={"#k": "key", "#st": "status"},
            ExpressionAttributeValues={
                ":nk": {"S": new_key},
                ":s": {"S": FileStatus.ARCHIVED.value},
                ":p": {"S": "archive/"},
            },
        )
        return new_key

    def copy_file(self, file_id: str, new_prefix: str) -> UploadResult | None:
        """Copy a file to a new prefix, creating a new metadata record."""
        meta = self.get_metadata(file_id)
        if meta is None:
            return None

        body = self.download_file(meta.key)
        # Strip the old prefix from the key to get the bare filename
        bare_key = meta.key
        if meta.prefix and bare_key.startswith(meta.prefix):
            bare_key = bare_key[len(meta.prefix) :]

        return self.upload_file(
            bare_key,
            body,
            content_type=meta.content_type,
            tags=meta.tags,
            uploaded_by=meta.uploaded_by,
            prefix=new_prefix,
        )

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    def delete_file(self, file_id: str) -> bool:
        """Delete a file from S3 and its metadata from DynamoDB.

        Returns True if the file existed and was deleted.
        """
        meta = self.get_metadata(file_id)
        if meta is None:
            return False

        self.s3.delete_object(Bucket=self.bucket, Key=meta.key)
        self.ddb.delete_item(
            TableName=self.table_name,
            Key={"file_id": {"S": file_id}},
        )
        return True

    def delete_files(self, file_ids: list[str]) -> int:
        """Delete multiple files.  Returns the count of files actually deleted."""
        deleted = 0
        for fid in file_ids:
            if self.delete_file(fid):
                deleted += 1
        return deleted

    # ------------------------------------------------------------------
    # Presigned URLs
    # ------------------------------------------------------------------

    def generate_download_url(self, key: str, *, expires_in: int = 3600) -> str:
        """Generate a presigned GET URL for direct browser download."""
        return self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_in,
        )

    def generate_upload_url(
        self, key: str, *, content_type: str = _DEFAULT_CONTENT_TYPE, expires_in: int = 3600
    ) -> str:
        """Generate a presigned PUT URL for direct browser upload."""
        return self.s3.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self.bucket,
                "Key": key,
                "ContentType": content_type,
            },
            ExpiresIn=expires_in,
        )

    # ------------------------------------------------------------------
    # Storage analytics
    # ------------------------------------------------------------------

    def get_storage_stats(self, prefix: str = "") -> StorageStats:
        """Calculate storage statistics for a given prefix."""
        files = self.list_files(prefix=prefix)
        total_bytes = sum(f.size for f in files)
        last_modified = None
        if files:
            last_modified = max(f.uploaded_at for f in files)
        return StorageStats(
            prefix=prefix,
            total_files=len(files),
            total_bytes=total_bytes,
            last_modified=last_modified,
        )

    def get_all_prefix_stats(self) -> list[StorageStats]:
        """Return storage stats grouped by unique prefix."""
        all_files = self.list_files()
        prefixes: set[str] = set()
        for f in all_files:
            prefixes.add(f.prefix or "")
        return [self.get_storage_stats(p) for p in sorted(prefixes)]

    # ------------------------------------------------------------------
    # Thumbnail tracking (simulated)
    # ------------------------------------------------------------------

    def request_thumbnail(self, file_id: str) -> bool:
        """Record that a thumbnail should be generated for *file_id*.

        In a real system this would publish an event to SQS/SNS.  Here we
        simply add a tag and transition the status to PROCESSING, then
        re-write the full metadata record with the new tag list.
        """
        meta = self.get_metadata(file_id)
        if meta is None:
            return False

        # Update in-memory model
        meta.status = FileStatus.PROCESSING
        if "thumbnail-requested" not in meta.tags:
            meta.tags.append("thumbnail-requested")

        # Re-put the full item (avoids complex UpdateExpression for SS types)
        self.ddb.put_item(
            TableName=self.table_name,
            Item=meta.to_dynamodb_item(),
        )
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_content_type(key: str) -> str:
        ctype, _ = mimetypes.guess_type(key)
        return ctype or _DEFAULT_CONTENT_TYPE
