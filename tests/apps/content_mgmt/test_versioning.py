"""Tests for content versioning."""


class TestVersionHistory:
    """Version snapshots are created on every edit."""

    def test_initial_version(self, cms, sample_article):
        versions = cms.list_versions(sample_article.content_id)
        assert len(versions) == 1
        assert versions[0].version == 1
        assert versions[0].title == sample_article.title

    def test_edit_creates_new_version(self, cms, sample_article):
        cms.update_content(
            sample_article.content_id,
            title="Edited Title",
            body="Edited body",
            author="editor",
        )
        versions = cms.list_versions(sample_article.content_id)
        assert len(versions) == 2
        assert versions[1].version == 2
        assert versions[1].title == "Edited Title"
        assert versions[1].body == "Edited body"
        assert versions[1].updated_by == "editor"

    def test_multiple_edits_track_all_versions(self, cms, sample_article):
        for i in range(2, 5):
            cms.update_content(
                sample_article.content_id,
                title=f"Title v{i}",
                author="editor",
            )
        versions = cms.list_versions(sample_article.content_id)
        assert len(versions) == 4
        assert [v.version for v in versions] == [1, 2, 3, 4]

    def test_get_specific_version(self, cms, sample_article):
        cms.update_content(
            sample_article.content_id,
            title="Version 2 Title",
            body="Version 2 Body",
            author="editor",
        )
        v1 = cms.get_version(sample_article.content_id, 1)
        assert v1 is not None
        assert v1.title == sample_article.title

        v2 = cms.get_version(sample_article.content_id, 2)
        assert v2 is not None
        assert v2.title == "Version 2 Title"


class TestRevert:
    """Reverting to a previous version."""

    def test_revert_to_previous_version(self, cms, sample_article):
        original_title = sample_article.title
        original_body = sample_article.body

        cms.update_content(
            sample_article.content_id,
            title="Changed Title",
            body="Changed Body",
            author="editor",
        )
        # Now at version 2, revert to version 1
        reverted = cms.revert_to_version(sample_article.content_id, 1, actor="admin")
        assert reverted.title == original_title
        assert reverted.body == original_body
        assert reverted.version == 3  # revert creates a NEW version

    def test_revert_nonexistent_version_raises(self, cms, sample_article):
        import pytest

        with pytest.raises(ValueError, match="Version 99 not found"):
            cms.revert_to_version(sample_article.content_id, 99, actor="admin")


class TestVersionDiff:
    """Comparing two versions of content."""

    def test_version_diff(self, cms, sample_article):
        cms.update_content(
            sample_article.content_id,
            title="New Title",
            body="New Body",
            author="editor",
        )
        v1 = cms.get_version(sample_article.content_id, 1)
        v2 = cms.get_version(sample_article.content_id, 2)

        assert v1.title != v2.title
        assert v1.body != v2.body
        assert v1.title == sample_article.title
        assert v2.title == "New Title"
