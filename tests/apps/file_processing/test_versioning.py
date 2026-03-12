"""
Tests for file versioning.

Covers: enabling versioning, uploading multiple versions of the same key,
listing versions, retrieving by version ID, deleting specific versions,
restoring previous versions, and version metadata tracking in DynamoDB.
"""

from __future__ import annotations

import json

from .app import FileProcessingService
from .models import UploadResult


class TestVersioningBasics:
    """Fundamental versioning operations."""

    def test_enable_versioning_and_upload_two_versions(self, file_service: FileProcessingService):
        file_service.enable_versioning()

        file_service.upload_file("config.json", b'{"v": 1}')
        file_service.upload_file("config.json", b'{"v": 2}')

        versions = file_service.list_versions("config.json")
        assert len(versions) >= 2

    def test_versions_have_distinct_ids(self, file_service: FileProcessingService):
        file_service.enable_versioning()

        file_service.upload_file("doc.txt", b"version 1")
        file_service.upload_file("doc.txt", b"version 2")

        versions = file_service.list_versions("doc.txt")
        version_ids = {v.version_id for v in versions}
        # At least two distinct version IDs
        assert len(version_ids) >= 2

    def test_latest_version_is_flagged(self, file_service: FileProcessingService):
        file_service.enable_versioning()

        file_service.upload_file("flag.txt", b"old")
        file_service.upload_file("flag.txt", b"new")

        versions = file_service.list_versions("flag.txt")
        latest = [v for v in versions if v.is_latest]
        assert len(latest) == 1


class TestVersionRetrieval:
    """Retrieve specific versions by ID."""

    def test_get_specific_version_content(self, file_service: FileProcessingService):
        file_service.enable_versioning()

        r1 = file_service.upload_file("data.txt", b"first")
        file_service.upload_file("data.txt", b"second")

        # Current (latest) content should be "second"
        current = file_service.download_file("data.txt")
        assert current == b"second"

        # Retrieve the first version explicitly
        if r1.version_id:
            old = file_service.get_version("data.txt", r1.version_id)
            assert old == b"first"

    def test_list_versions_ordering(self, file_service: FileProcessingService):
        file_service.enable_versioning()

        contents = [b"alpha", b"beta", b"gamma"]
        for c in contents:
            file_service.upload_file("ordered.txt", c)

        versions = file_service.list_versions("ordered.txt")
        assert len(versions) >= 3
        # list_versions returns newest first, so sizes should match gamma, beta, alpha
        sizes = [v.size for v in versions]
        assert sizes[0] == len(b"gamma")

    def test_three_versions_all_readable(self, file_service: FileProcessingService):
        file_service.enable_versioning()

        payloads = [
            json.dumps({"debug": True, "version": 1}).encode(),
            json.dumps({"debug": False, "version": 2}).encode(),
            json.dumps({"debug": False, "version": 3, "flags": ["new-ui"]}).encode(),
        ]
        results: list[UploadResult] = []
        for p in payloads:
            results.append(file_service.upload_file("settings.json", p))

        # Read each version back and verify
        for r, expected_payload in zip(results, payloads):
            if r.version_id:
                body = file_service.get_version("settings.json", r.version_id)
                assert body == expected_payload


class TestVersionDeletion:
    """Delete a specific version without affecting others."""

    def test_delete_version_removes_only_that_version(self, file_service: FileProcessingService):
        file_service.enable_versioning()

        r1 = file_service.upload_file("delme.txt", b"v1")
        r2 = file_service.upload_file("delme.txt", b"v2")
        r3 = file_service.upload_file("delme.txt", b"v3")

        before = file_service.list_versions("delme.txt")
        assert len(before) >= 3

        if r2.version_id:
            file_service.delete_version("delme.txt", r2.version_id)

        after = file_service.list_versions("delme.txt")
        after_ids = {v.version_id for v in after}
        if r2.version_id:
            assert r2.version_id not in after_ids
        # The remaining versions should still be there
        if r1.version_id:
            assert r1.version_id in after_ids
        if r3.version_id:
            assert r3.version_id in after_ids


class TestVersionRestore:
    """Restore a previous version as the new current version."""

    def test_restore_makes_old_content_current(self, file_service: FileProcessingService):
        file_service.enable_versioning()

        r1 = file_service.upload_file("restore.txt", b"original")
        file_service.upload_file("restore.txt", b"modified")

        # Current should be "modified"
        assert file_service.download_file("restore.txt") == b"modified"

        # Restore v1
        if r1.version_id:
            restored = file_service.restore_version("restore.txt", r1.version_id)
            assert isinstance(restored, UploadResult)

            # Now current should be "original" again
            assert file_service.download_file("restore.txt") == b"original"

    def test_restore_creates_new_version(self, file_service: FileProcessingService):
        file_service.enable_versioning()

        r1 = file_service.upload_file("rv.txt", b"one")
        file_service.upload_file("rv.txt", b"two")

        before_count = len(file_service.list_versions("rv.txt"))

        if r1.version_id:
            file_service.restore_version("rv.txt", r1.version_id)

        after_count = len(file_service.list_versions("rv.txt"))
        # Restore copies old content as a new version, so count increases
        assert after_count > before_count


class TestVersionMetadataTracking:
    """Verify that DynamoDB metadata tracks version IDs."""

    def test_metadata_has_version_id_when_versioning_enabled(
        self, file_service: FileProcessingService
    ):
        file_service.enable_versioning()
        result = file_service.upload_file("versioned.txt", b"data")

        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        # When versioning is enabled, S3 returns a VersionId
        assert meta.version_id is not None
        assert len(meta.version_id) > 0

    def test_metadata_version_id_is_none_without_versioning(
        self, file_service: FileProcessingService
    ):
        result = file_service.upload_file("unversioned.txt", b"data")
        meta = file_service.get_metadata(result.file_id)
        assert meta is not None
        # Without versioning, no VersionId
        assert meta.version_id is None
