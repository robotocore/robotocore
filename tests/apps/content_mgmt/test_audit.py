"""Tests for content audit trail via CloudWatch Logs."""

import time


class TestAuditLogging:
    """Audit entries are written to CloudWatch Logs on every mutation."""

    def test_content_creation_logged(self, cms):
        item = cms.create_content(
            title="Audited Article",
            author="audit-user",
            category="tech",
        )
        # Small delay for log propagation
        time.sleep(0.2)
        trail = cms.get_audit_trail(content_id=item.content_id)
        actions = [e.action for e in trail]
        assert "content_created" in actions

    def test_content_update_logged(self, cms, sample_article):
        cms.update_content(
            sample_article.content_id,
            title="Audit Update Test",
            author="audit-editor",
        )
        time.sleep(0.2)
        trail = cms.get_audit_trail(content_id=sample_article.content_id)
        actions = [e.action for e in trail]
        assert "content_updated" in actions

    def test_content_publish_logged(self, cms, sample_article):
        cms.transition(sample_article.content_id, "REVIEW", actor="editor")
        cms.transition(sample_article.content_id, "PUBLISHED", actor="publisher")
        time.sleep(0.2)
        trail = cms.get_audit_trail(content_id=sample_article.content_id)
        actions = [e.action for e in trail]
        assert "status_published" in actions

    def test_audit_entries_include_actor(self, cms):
        item = cms.create_content(
            title="Actor Test",
            author="specific-actor",
            category="tech",
        )
        time.sleep(0.2)
        trail = cms.get_audit_trail(content_id=item.content_id)
        # The create entry should have the author as actor
        create_entries = [e for e in trail if e.action == "content_created"]
        assert len(create_entries) >= 1
        assert create_entries[0].actor == "specific-actor"

    def test_audit_entries_include_details(self, cms):
        item = cms.create_content(
            content_type="page",
            title="Details Test",
            author="a",
            category="tech",
        )
        time.sleep(0.2)
        trail = cms.get_audit_trail(content_id=item.content_id)
        create_entries = [e for e in trail if e.action == "content_created"]
        assert len(create_entries) >= 1
        assert "type=page" in create_entries[0].details

    def test_full_audit_trail(self, cms):
        """Get the unfiltered audit trail."""
        cms.create_content(title="Trail A", author="a", category="tech")
        cms.create_content(title="Trail B", author="b", category="science")
        time.sleep(0.2)
        trail = cms.get_audit_trail()  # no filter
        assert len(trail) >= 2
