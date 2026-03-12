"""
Data models for the file processing service.

These are plain dataclasses representing the domain objects. No AWS SDK
imports -- models are pure Python so they can be serialized, tested, and
evolved independently of the persistence layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TypedDict


class FileStatus(StrEnum):
    """Lifecycle status of a managed file."""

    UPLOADING = "uploading"
    PROCESSING = "processing"
    PROCESSED = "processed"
    ARCHIVED = "archived"
    DELETED = "deleted"


@dataclass
class FileMetadata:
    """Full metadata record for a file stored in the system."""

    file_id: str
    key: str
    bucket: str
    size: int
    content_type: str
    sha256: str
    uploaded_at: str  # ISO-8601
    status: FileStatus = FileStatus.UPLOADING
    tags: list[str] = field(default_factory=list)
    version_id: str | None = None
    prefix: str = ""
    uploaded_by: str = "system"

    def to_dynamodb_item(self) -> dict:
        """Serialize to a DynamoDB item (attribute-value format)."""
        item: dict = {
            "file_id": {"S": self.file_id},
            "key": {"S": self.key},
            "bucket": {"S": self.bucket},
            "size": {"N": str(self.size)},
            "content_type": {"S": self.content_type},
            "sha256": {"S": self.sha256},
            "uploaded_at": {"S": self.uploaded_at},
            "status": {"S": self.status.value},
            "tags": {"SS": self.tags} if self.tags else {"SS": ["_none_"]},
            "prefix": {"S": self.prefix},
            "uploaded_by": {"S": self.uploaded_by},
        }
        if self.version_id:
            item["version_id"] = {"S": self.version_id}
        return item

    @classmethod
    def from_dynamodb_item(cls, item: dict) -> FileMetadata:
        """Deserialize from a DynamoDB attribute-value map."""
        tags_raw = item.get("tags", {}).get("SS", [])
        tags = [t for t in tags_raw if t != "_none_"]
        return cls(
            file_id=item["file_id"]["S"],
            key=item["key"]["S"],
            bucket=item["bucket"]["S"],
            size=int(item["size"]["N"]),
            content_type=item["content_type"]["S"],
            sha256=item["sha256"]["S"],
            uploaded_at=item["uploaded_at"]["S"],
            status=FileStatus(item["status"]["S"]),
            tags=tags,
            version_id=item.get("version_id", {}).get("S"),
            prefix=item.get("prefix", {}).get("S", ""),
            uploaded_by=item.get("uploaded_by", {}).get("S", "system"),
        )


@dataclass
class FileVersion:
    """A single version of a file (maps to an S3 object version)."""

    version_id: str
    uploaded_at: str
    size: int
    sha256: str
    is_latest: bool = False


@dataclass
class UploadResult:
    """Returned by the upload operation."""

    file_id: str
    key: str
    version_id: str | None
    etag: str
    sha256: str
    size: int


@dataclass
class StorageStats:
    """Aggregated storage statistics for a prefix."""

    prefix: str
    total_files: int
    total_bytes: int
    last_modified: str | None = None  # ISO-8601 or None if empty


class SearchQuery(TypedDict, total=False):
    """Parameters for searching file metadata."""

    tags: list[str]
    content_type: str
    prefix: str
    date_from: str  # ISO-8601
    date_to: str  # ISO-8601
    status: str
    limit: int
