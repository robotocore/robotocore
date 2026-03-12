"""
Tests for file upload operations.

Covers: single uploads, bulk uploads, content-type detection, tagging,
deduplication, prefix organization, large files, and SHA-256 integrity.
"""

from __future__ import annotations

import hashlib

from .app import FileProcessingService
from .models import FileMetadata, FileStatus, UploadResult


class TestSingleUpload:
    """Upload a single file and verify both S3 and DynamoDB agree."""

    def test_upload_stores_object_in_s3(self, file_service: FileProcessingService):
        body = b"Hello, world!"
        result = file_service.upload_file("hello.txt", body)

        # Verify the object exists in S3 with correct content
        downloaded = file_service.download_file(result.key)
        assert downloaded == body

    def test_upload_records_metadata(self, file_service: FileProcessingService):
        body = b"metadata check"
        result = file_service.upload_file("meta.txt", body)

        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.key == "meta.txt"
        assert meta.size == len(body)
        assert meta.content_type == "text/plain"
        assert meta.sha256 == hashlib.sha256(body).hexdigest()
        assert meta.status == FileStatus.UPLOADING
        assert meta.bucket == file_service.bucket

    def test_upload_sha256_matches(self, file_service: FileProcessingService):
        body = b"integrity test payload \x00\xff"
        expected = hashlib.sha256(body).hexdigest()
        result = file_service.upload_file("checksum.bin", body)

        assert result.sha256 == expected
        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.sha256 == expected

    def test_upload_returns_etag(self, file_service: FileProcessingService):
        result = file_service.upload_file("etag.txt", b"etag body")
        assert result.etag  # non-empty string

    def test_upload_auto_detects_json_content_type(self, file_service: FileProcessingService):
        result = file_service.upload_file("data.json", b'{"a": 1}')
        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.content_type == "application/json"

    def test_upload_auto_detects_csv_content_type(self, file_service: FileProcessingService):
        result = file_service.upload_file("report.csv", b"a,b\n1,2")
        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.content_type == "text/csv"

    def test_upload_explicit_content_type_override(self, file_service: FileProcessingService):
        result = file_service.upload_file(
            "data.bin", b"\x00\x01\x02", content_type="application/x-custom"
        )
        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.content_type == "application/x-custom"

    def test_upload_unknown_extension_defaults_to_octet_stream(
        self, file_service: FileProcessingService
    ):
        result = file_service.upload_file("mystery.xyzzy", b"???")
        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.content_type == "application/octet-stream"


class TestUploadWithTags:
    """Verify tag storage and queryability."""

    def test_upload_with_tags_stores_them(self, file_service: FileProcessingService):
        result = file_service.upload_file(
            "tagged.txt", b"tagged content", tags=["finance", "quarterly"]
        )
        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert set(meta.tags) == {"finance", "quarterly"}

    def test_upload_without_tags_has_empty_list(self, file_service: FileProcessingService):
        result = file_service.upload_file("plain.txt", b"no tags")
        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.tags == []

    def test_tags_searchable_after_upload(self, file_service: FileProcessingService):
        file_service.upload_file("a.txt", b"a", tags=["important"])
        file_service.upload_file("b.txt", b"b", tags=["draft"])

        results = file_service.search({"tags": ["important"]})
        assert len(results) == 1
        assert results[0].key == "a.txt"


class TestDeduplication:
    """Upload the same content twice; verify dedup prevents a second write."""

    def test_duplicate_returns_existing_metadata(self, file_service: FileProcessingService):
        body = b"deduplicate me"
        first = file_service.upload_with_deduplication("dup1.txt", body)
        second = file_service.upload_with_deduplication("dup2.txt", body)

        # First call returns an UploadResult; second returns existing FileMetadata
        assert isinstance(first, UploadResult)
        assert isinstance(second, FileMetadata)
        assert second.sha256 == hashlib.sha256(body).hexdigest()

    def test_different_content_is_not_deduplicated(self, file_service: FileProcessingService):
        r1 = file_service.upload_with_deduplication("unique1.txt", b"aaa")
        r2 = file_service.upload_with_deduplication("unique2.txt", b"bbb")
        assert isinstance(r1, UploadResult)
        assert isinstance(r2, UploadResult)
        assert r1.file_id != r2.file_id


class TestPrefixOrganization:
    """Verify files are organized under prefixes correctly."""

    def test_upload_to_prefix(self, file_service: FileProcessingService):
        result = file_service.upload_file("invoice.pdf", b"invoice", prefix="documents/")
        assert result.key == "documents/invoice.pdf"

        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.prefix == "documents/"

    def test_list_files_by_prefix(self, file_service: FileProcessingService):
        file_service.upload_file("a.txt", b"a", prefix="docs/")
        file_service.upload_file("b.txt", b"b", prefix="images/")
        file_service.upload_file("c.txt", b"c", prefix="docs/")

        docs = file_service.list_files(prefix="docs/")
        assert len(docs) == 2
        keys = {f.key for f in docs}
        assert keys == {"docs/a.txt", "docs/c.txt"}


class TestBulkUpload:
    """Upload multiple files in one call."""

    def test_bulk_upload_returns_all_results(
        self, file_service: FileProcessingService, sample_files
    ):
        results = file_service.upload_files(sample_files, prefix="bulk/")
        assert len(results) == len(sample_files)
        for r in results:
            assert r.key.startswith("bulk/")
            assert r.sha256  # non-empty

    def test_bulk_upload_all_downloadable(self, file_service: FileProcessingService, sample_files):
        results = file_service.upload_files(sample_files, prefix="dl-test/")
        for r in results:
            body = file_service.download_file(r.key)
            assert len(body) > 0


class TestLargeFileUpload:
    """Simulate a larger file to ensure S3 handles it."""

    def test_upload_1mb_file(self, file_service: FileProcessingService):
        body = b"X" * (1024 * 1024)
        result = file_service.upload_file("large.bin", body)

        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.size == 1024 * 1024

        downloaded = file_service.download_file(result.key)
        assert len(downloaded) == 1024 * 1024
        assert hashlib.sha256(downloaded).hexdigest() == result.sha256
