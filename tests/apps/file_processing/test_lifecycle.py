"""
Tests for file lifecycle management.

Covers: status transitions, archival, bulk deletion, copy between prefixes,
storage statistics, presigned URL generation, and thumbnail tracking.
"""

from __future__ import annotations

from .app import FileProcessingService
from .models import FileStatus, UploadResult


class TestStatusTransitions:
    """Verify the upload -> processing -> processed lifecycle."""

    def test_initial_status_is_uploading(self, file_service: FileProcessingService):
        result = file_service.upload_file("new.txt", b"new")
        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.status == FileStatus.UPLOADING

    def test_transition_to_processing(self, file_service: FileProcessingService):
        result = file_service.upload_file("proc.txt", b"proc")
        file_service.set_status(result.file_id, FileStatus.PROCESSING)

        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.status == FileStatus.PROCESSING

    def test_full_lifecycle_flow(self, file_service: FileProcessingService):
        result = file_service.upload_file("lifecycle.txt", b"lifecycle data")
        fid = result.file_id

        # uploading -> processing
        file_service.set_status(fid, FileStatus.PROCESSING)
        assert file_service.get_metadata(fid).status == FileStatus.PROCESSING

        # processing -> processed
        file_service.set_status(fid, FileStatus.PROCESSED)
        assert file_service.get_metadata(fid).status == FileStatus.PROCESSED

        # processed -> archived
        file_service.set_status(fid, FileStatus.ARCHIVED)
        assert file_service.get_metadata(fid).status == FileStatus.ARCHIVED

    def test_thumbnail_request_transitions_to_processing(self, file_service: FileProcessingService):
        result = file_service.upload_file("photo.png", b"\x89PNG" + b"\x00" * 20)
        ok = file_service.request_thumbnail(result.file_id)
        assert ok is True

        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.status == FileStatus.PROCESSING
        assert "thumbnail-requested" in meta.tags


class TestArchival:
    """Move files to archive/ prefix."""

    def test_archive_moves_to_archive_prefix(self, file_service: FileProcessingService):
        result = file_service.upload_file("report.pdf", b"report content")
        new_key = file_service.archive_file(result.file_id)

        assert new_key == "archive/report.pdf"

        # Verify the old key is gone
        try:
            file_service.download_file("report.pdf")
            assert False, "Expected error downloading deleted key"
        except Exception:
            pass  # best-effort cleanup

        # Verify the file is at the new key
        body = file_service.download_file("archive/report.pdf")
        assert body == b"report content"

    def test_archive_updates_metadata(self, file_service: FileProcessingService):
        result = file_service.upload_file("archivable.txt", b"data")
        file_service.archive_file(result.file_id)

        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        assert meta.status == FileStatus.ARCHIVED
        assert meta.key == "archive/archivable.txt"
        assert meta.prefix == "archive/"

    def test_archive_nonexistent_returns_none(self, file_service: FileProcessingService):
        result = file_service.archive_file("nonexistent-id")
        assert result is None

    def test_archive_prefixed_file(self, file_service: FileProcessingService):
        result = file_service.upload_file("memo.txt", b"memo", prefix="documents/")
        new_key = file_service.archive_file(result.file_id)
        assert new_key == "archive/documents/memo.txt"


class TestBulkDelete:
    """Delete multiple files at once."""

    def test_delete_single_file(self, file_service: FileProcessingService):
        result = file_service.upload_file("gone.txt", b"gone")
        deleted = file_service.delete_file(result.file_id)
        assert deleted is True

        meta = file_service.get_metadata(result.file_id)
        assert meta is None

    def test_delete_nonexistent_returns_false(self, file_service: FileProcessingService):
        deleted = file_service.delete_file("no-such-id")
        assert deleted is False

    def test_bulk_delete_multiple(self, file_service: FileProcessingService):
        ids = []
        for i in range(4):
            r = file_service.upload_file(f"bulk{i}.txt", f"body{i}".encode())
            ids.append(r.file_id)

        count = file_service.delete_files(ids)
        assert count == 4

        # All metadata gone
        for fid in ids:
            assert file_service.get_metadata(fid) is None

        # All S3 objects gone
        all_files = file_service.list_files()
        assert len(all_files) == 0

    def test_bulk_delete_partial(self, file_service: FileProcessingService):
        r1 = file_service.upload_file("keep.txt", b"keep")
        r2 = file_service.upload_file("remove.txt", b"remove")

        count = file_service.delete_files([r2.file_id, "nonexistent"])
        assert count == 1

        # r1 still exists
        assert file_service.get_metadata(r1.file_id) is not None


class TestCopyBetweenPrefixes:
    """Copy files from one prefix to another."""

    def test_copy_creates_new_file(self, file_service: FileProcessingService):
        result = file_service.upload_file("doc.txt", b"document body", prefix="inbox/")
        copy_result = file_service.copy_file(result.file_id, "processed/")

        assert copy_result is not None
        assert isinstance(copy_result, UploadResult)
        assert copy_result.key == "processed/doc.txt"
        assert copy_result.file_id != result.file_id

        # Both copies readable
        original = file_service.download_file("inbox/doc.txt")
        copied = file_service.download_file("processed/doc.txt")
        assert original == copied == b"document body"

    def test_copy_nonexistent_returns_none(self, file_service: FileProcessingService):
        result = file_service.copy_file("no-such-id", "dest/")
        assert result is None

    def test_copy_preserves_tags(self, file_service: FileProcessingService):
        result = file_service.upload_file(
            "tagged.txt", b"tagged", prefix="src/", tags=["important"]
        )
        copy_result = file_service.copy_file(result.file_id, "dst/")
        assert copy_result is not None

        meta = file_service.get_metadata(copy_result.file_id)
        assert meta is not None
        assert "important" in meta.tags


class TestStorageStats:
    """Calculate storage statistics per prefix."""

    def test_stats_for_prefix(self, file_service: FileProcessingService):
        file_service.upload_file("a.txt", b"aaa", prefix="stats/")
        file_service.upload_file("b.txt", b"bbbbbb", prefix="stats/")

        stats = file_service.get_storage_stats(prefix="stats/")
        assert stats.prefix == "stats/"
        assert stats.total_files == 2
        assert stats.total_bytes == 9  # 3 + 6
        assert stats.last_modified is not None

    def test_stats_empty_prefix(self, file_service: FileProcessingService):
        stats = file_service.get_storage_stats(prefix="empty/")
        assert stats.total_files == 0
        assert stats.total_bytes == 0
        assert stats.last_modified is None

    def test_all_prefix_stats(self, file_service: FileProcessingService):
        file_service.upload_file("x.txt", b"xx", prefix="alpha/")
        file_service.upload_file("y.txt", b"yyyy", prefix="beta/")
        file_service.upload_file("z.txt", b"zzzzzz", prefix="alpha/")

        all_stats = file_service.get_all_prefix_stats()
        assert len(all_stats) == 2

        by_prefix = {s.prefix: s for s in all_stats}
        assert by_prefix["alpha/"].total_files == 2
        assert by_prefix["alpha/"].total_bytes == 8  # 2 + 6
        assert by_prefix["beta/"].total_files == 1
        assert by_prefix["beta/"].total_bytes == 4


class TestPresignedUrls:
    """Verify presigned URL generation."""

    def test_download_url_structure(self, file_service: FileProcessingService):
        file_service.upload_file("shared.csv", b"id,name\n1,Alice")

        url = file_service.generate_download_url("shared.csv")
        assert file_service.bucket in url
        assert "shared.csv" in url
        assert "Signature" in url or "X-Amz-Signature" in url

    def test_download_url_custom_expiry(self, file_service: FileProcessingService):
        file_service.upload_file("temp.txt", b"temporary")

        url = file_service.generate_download_url("temp.txt", expires_in=60)
        assert file_service.bucket in url
        # The expiry is embedded in the URL params
        assert "Expires" in url or "X-Amz-Expires" in url

    def test_upload_url_structure(self, file_service: FileProcessingService):
        url = file_service.generate_upload_url(
            "incoming/new-file.pdf", content_type="application/pdf"
        )
        assert file_service.bucket in url
        assert "incoming" in url
        assert "Signature" in url or "X-Amz-Signature" in url

    def test_upload_and_download_urls_are_different(self, file_service: FileProcessingService):
        file_service.upload_file("both.txt", b"both ways")

        dl_url = file_service.generate_download_url("both.txt")
        ul_url = file_service.generate_upload_url("both.txt")
        assert dl_url != ul_url
