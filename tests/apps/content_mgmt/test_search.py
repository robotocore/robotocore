"""Tests for content search and query operations."""

from .models import DRAFT, REVIEW


class TestSearchByCategory:
    """Querying the by-category GSI."""

    def test_search_by_category(self, cms):
        cms.create_content(title="Tech 1", author="a", category="tech")
        cms.create_content(title="Tech 2", author="a", category="tech")
        cms.create_content(title="Science 1", author="a", category="science")

        results = cms.search_by_category("tech")
        assert len(results) >= 2
        assert all(r.category == "tech" for r in results)

    def test_search_empty_category(self, cms):
        results = cms.search_by_category("nonexistent-category")
        assert len(results) == 0


class TestSearchByStatus:
    """Querying the by-status GSI."""

    def test_search_by_status(self, cms):
        cms.create_content(title="Draft 1", author="a", category="misc")
        cms.create_content(title="Draft 2", author="a", category="misc")
        c = cms.create_content(title="In Review", author="a", category="misc")
        cms.transition(c.content_id, REVIEW, actor="e")

        drafts = cms.search_by_status(DRAFT)
        assert len(drafts) >= 2

        reviews = cms.search_by_status(REVIEW)
        assert len(reviews) >= 1
        assert any(r.content_id == c.content_id for r in reviews)


class TestSearchByAuthor:
    """Querying the by-author GSI."""

    def test_search_by_author(self, cms):
        cms.create_content(title="Alice Post 1", author="alice", category="tech")
        cms.create_content(title="Alice Post 2", author="alice", category="science")
        cms.create_content(title="Bob Post", author="bob", category="tech")

        alice_items = cms.search_by_author("alice")
        assert len(alice_items) >= 2
        assert all(i.author == "alice" for i in alice_items)

        bob_items = cms.search_by_author("bob")
        assert len(bob_items) >= 1
        assert all(i.author == "bob" for i in bob_items)


class TestSearchByTags:
    """Scanning by tag value."""

    def test_search_by_tag(self, cms):
        cms.create_content(
            title="Python Guide", author="a", category="tech", tags=["python", "guide"]
        )
        cms.create_content(title="AWS Guide", author="a", category="tech", tags=["aws", "guide"])
        cms.create_content(title="Rust Intro", author="a", category="tech", tags=["rust"])

        guides = cms.search_by_tags("guide")
        assert len(guides) >= 2
        titles = {g.title for g in guides}
        assert "Python Guide" in titles
        assert "AWS Guide" in titles


class TestCompoundSearch:
    """Combining category + status filters."""

    def test_compound_category_and_status(self, cms):
        cms.create_content(title="Tech Draft", author="a", category="tech")
        b = cms.create_content(title="Tech Review", author="a", category="tech")
        cms.transition(b.content_id, REVIEW, actor="e")
        cms.create_content(title="Science Draft", author="a", category="science")

        results = cms.search_compound(category="tech", status=DRAFT)
        assert len(results) >= 1
        assert all(r.category == "tech" and r.status == DRAFT for r in results)


class TestPagination:
    """Limit parameter for paginated results."""

    def test_limit_results(self, cms):
        for i in range(5):
            cms.create_content(title=f"Paginated {i}", author="a", category="paginate-test")

        results = cms.search_by_category("paginate-test", limit=2)
        assert len(results) <= 2
