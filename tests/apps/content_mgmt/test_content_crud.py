"""Tests for content CRUD operations."""

import pytest

from .models import ARTICLE, BLOG_POST, DRAFT, PAGE


class TestCreateContent:
    """Creating content items of various types."""

    def test_create_article_all_fields(self, cms):
        item = cms.create_content(
            content_type="article",
            title="My First Article",
            body="Article body goes here.",
            author="alice",
            category="tech",
            tags=["python", "aws"],
        )
        assert item.content_id
        assert item.title == "My First Article"
        assert item.body == "Article body goes here."
        assert item.author == "alice"
        assert item.category == "tech"
        assert item.status == DRAFT
        assert item.version == 1
        assert item.slug == "my-first-article"
        assert set(item.tags) == {"python", "aws"}

    def test_create_article_read_back(self, cms):
        item = cms.create_content(
            title="Readable Article",
            body="Some body",
            author="bob",
            category="science",
        )
        fetched = cms.get_content(item.content_id)
        assert fetched is not None
        assert fetched.title == "Readable Article"
        assert fetched.body == "Some body"
        assert fetched.author == "bob"
        assert fetched.category == "science"
        assert fetched.content_type == ARTICLE
        assert fetched.version == 1

    def test_create_page(self, cms):
        item = cms.create_content(
            content_type="page",
            title="About Us",
            body="We are a company.",
            author="admin",
            category="corporate",
        )
        assert item.content_type == PAGE

    def test_create_blog_post(self, cms):
        item = cms.create_content(
            content_type="blog_post",
            title="Dev Log 1",
            body="Today we shipped...",
            author="dev",
            category="engineering",
        )
        assert item.content_type == BLOG_POST

    def test_create_invalid_type_raises(self, cms):
        with pytest.raises(ValueError, match="Invalid content_type"):
            cms.create_content(
                content_type="podcast",
                title="Nope",
                author="x",
            )

    def test_slug_generation(self, cms):
        item = cms.create_content(
            title="Hello World! This is a Test",
            author="a",
            category="misc",
        )
        assert item.slug == "hello-world-this-is-a-test"

    def test_slug_uniqueness(self, cms):
        a = cms.create_content(title="Duplicate Title", author="a", category="misc")
        b = cms.create_content(title="Duplicate Title", author="b", category="misc")
        assert a.slug != b.slug
        assert b.slug.startswith("duplicate-title")

    def test_content_with_tags_queryable(self, cms):
        cms.create_content(
            title="Tagged One",
            author="a",
            category="tech",
            tags=["serverless", "lambda"],
        )
        cms.create_content(
            title="Tagged Two",
            author="a",
            category="tech",
            tags=["serverless", "docker"],
        )
        results = cms.search_by_tags("serverless")
        assert len(results) >= 2
        titles = {r.title for r in results}
        assert "Tagged One" in titles
        assert "Tagged Two" in titles


class TestUpdateContent:
    """Updating content items."""

    def test_update_title_and_body(self, cms, sample_article):
        updated = cms.update_content(
            sample_article.content_id,
            title="Updated Title",
            body="Updated body text.",
            author="editor",
        )
        assert updated.title == "Updated Title"
        assert updated.body == "Updated body text."
        assert updated.version == 2

    def test_update_increments_version(self, cms, sample_article):
        cms.update_content(sample_article.content_id, title="V2", author="e")
        cms.update_content(sample_article.content_id, title="V3", author="e")
        item = cms.get_content(sample_article.content_id)
        assert item.version == 3

    def test_update_category(self, cms, sample_article):
        updated = cms.update_content(sample_article.content_id, category="science", author="e")
        assert updated.category == "science"

    def test_update_nonexistent_raises(self, cms):
        with pytest.raises(ValueError, match="Content not found"):
            cms.update_content("nonexistent-id", title="Nope", author="x")


class TestDeleteContent:
    """Deleting content items."""

    def test_delete_content(self, cms, sample_article):
        cms.delete_content(sample_article.content_id, actor="admin")
        assert cms.get_content(sample_article.content_id) is None

    def test_delete_removes_versions(self, cms, sample_article):
        cms.update_content(sample_article.content_id, title="V2", author="e")
        cms.delete_content(sample_article.content_id, actor="admin")
        versions = cms.list_versions(sample_article.content_id)
        assert len(versions) == 0
