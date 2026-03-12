"""
Tests for file search and listing.

Covers: prefix listing, tag search, content-type search, date-range
filtering, status filtering, compound queries, and result limiting.
"""

from __future__ import annotations

from datetime import UTC, datetime

from .app import FileProcessingService
from .models import FileStatus


class TestListByPrefix:
    """List files organized under different prefixes."""

    def test_list_all_files(self, file_service: FileProcessingService):
        file_service.upload_file("a.txt", b"a")
        file_service.upload_file("b.txt", b"b")
        file_service.upload_file("c.txt", b"c")

        all_files = file_service.list_files()
        assert len(all_files) == 3

    def test_list_by_prefix_filters_correctly(self, file_service: FileProcessingService):
        file_service.upload_file("r1.pdf", b"r1", prefix="reports/")
        file_service.upload_file("r2.pdf", b"r2", prefix="reports/")
        file_service.upload_file("i1.png", b"i1", prefix="images/")

        reports = file_service.list_files(prefix="reports/")
        assert len(reports) == 2
        for f in reports:
            assert f.key.startswith("reports/")

        images = file_service.list_files(prefix="images/")
        assert len(images) == 1
        assert images[0].key == "images/i1.png"

    def test_list_empty_prefix(self, file_service: FileProcessingService):
        result = file_service.list_files(prefix="nonexistent/")
        assert result == []


class TestSearchByTag:
    """Search for files by tag."""

    def test_find_by_single_tag(self, file_service: FileProcessingService):
        file_service.upload_file("a.txt", b"a", tags=["finance"])
        file_service.upload_file("b.txt", b"b", tags=["engineering"])
        file_service.upload_file("c.txt", b"c", tags=["finance", "quarterly"])

        results = file_service.search({"tags": ["finance"]})
        assert len(results) == 2
        keys = {r.key for r in results}
        assert "a.txt" in keys
        assert "c.txt" in keys

    def test_find_by_multiple_tags_is_and(self, file_service: FileProcessingService):
        file_service.upload_file("x.txt", b"x", tags=["finance", "quarterly"])
        file_service.upload_file("y.txt", b"y", tags=["finance"])

        # Both tags required (AND)
        results = file_service.search({"tags": ["finance", "quarterly"]})
        assert len(results) == 1
        assert results[0].key == "x.txt"

    def test_no_matching_tags_returns_empty(self, file_service: FileProcessingService):
        file_service.upload_file("a.txt", b"a", tags=["alpha"])
        results = file_service.search({"tags": ["nonexistent"]})
        assert results == []


class TestSearchByContentType:
    """Search by content type."""

    def test_find_json_files(self, file_service: FileProcessingService):
        file_service.upload_file("config.json", b'{"k": "v"}')
        file_service.upload_file("data.csv", b"a,b\n1,2")
        file_service.upload_file("other.json", b"[]")

        results = file_service.search({"content_type": "application/json"})
        assert len(results) == 2
        for r in results:
            assert r.content_type == "application/json"

    def test_find_text_files(self, file_service: FileProcessingService):
        file_service.upload_file("notes.txt", b"notes")
        file_service.upload_file("pic.png", b"\x89PNG" + b"\x00" * 10)

        results = file_service.search({"content_type": "text/plain"})
        assert len(results) == 1
        assert results[0].key == "notes.txt"


class TestSearchByStatus:
    """Search by lifecycle status."""

    def test_find_processing_files(self, file_service: FileProcessingService):
        r1 = file_service.upload_file("a.txt", b"a")
        file_service.upload_file("b.txt", b"b")

        file_service.set_status(r1.file_id, FileStatus.PROCESSING)

        results = file_service.search({"status": "processing"})
        assert len(results) == 1
        assert results[0].file_id == r1.file_id

    def test_find_archived_files(self, file_service: FileProcessingService):
        r1 = file_service.upload_file("old.txt", b"old")
        file_service.set_status(r1.file_id, FileStatus.ARCHIVED)

        results = file_service.search({"status": "archived"})
        assert len(results) == 1
        assert results[0].status == FileStatus.ARCHIVED


class TestSearchByDateRange:
    """Filter by upload date."""

    def test_date_from_filter(self, file_service: FileProcessingService):
        file_service.upload_file("recent.txt", b"recent")

        # All our files were just uploaded, so using a date far in the past
        # should return everything
        results = file_service.search({"date_from": "2020-01-01T00:00:00"})
        assert len(results) >= 1

    def test_date_to_filter_excludes_future(self, file_service: FileProcessingService):
        file_service.upload_file("past.txt", b"past")

        # Filter to only files before year 2020 -> should be empty
        results = file_service.search({"date_to": "2020-01-01T00:00:00"})
        assert len(results) == 0

    def test_date_range_both_bounds(self, file_service: FileProcessingService):
        file_service.upload_file("bounded.txt", b"bounded")

        datetime.now(UTC)
        results = file_service.search(
            {
                "date_from": "2020-01-01T00:00:00",
                "date_to": "2099-12-31T23:59:59",
            }
        )
        assert len(results) >= 1


class TestCompoundQueries:
    """Combine multiple search criteria."""

    def test_tag_and_content_type(self, file_service: FileProcessingService):
        file_service.upload_file("a.json", b'{"a":1}', tags=["important"])
        file_service.upload_file("b.json", b'{"b":2}', tags=["draft"])
        file_service.upload_file("c.txt", b"text", tags=["important"])

        results = file_service.search(
            {
                "tags": ["important"],
                "content_type": "application/json",
            }
        )
        assert len(results) == 1
        assert results[0].key == "a.json"

    def test_status_and_prefix(self, file_service: FileProcessingService):
        r1 = file_service.upload_file("a.txt", b"a", prefix="docs/")
        r2 = file_service.upload_file("b.txt", b"b", prefix="docs/")
        r3 = file_service.upload_file("c.txt", b"c", prefix="images/")

        file_service.set_status(r1.file_id, FileStatus.PROCESSED)
        file_service.set_status(r2.file_id, FileStatus.PROCESSED)
        file_service.set_status(r3.file_id, FileStatus.PROCESSED)

        results = file_service.search({"status": "processed", "prefix": "docs/"})
        assert len(results) == 2
        for r in results:
            assert r.key.startswith("docs/")

    def test_search_with_limit(self, file_service: FileProcessingService):
        for i in range(5):
            file_service.upload_file(f"file{i}.txt", f"content{i}".encode())

        results = file_service.search({"limit": 3})
        assert len(results) == 3

    def test_empty_search_returns_all(self, file_service: FileProcessingService):
        file_service.upload_file("one.txt", b"one")
        file_service.upload_file("two.txt", b"two")

        results = file_service.search({})
        assert len(results) == 2
