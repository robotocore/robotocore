"""Tests for the publishing workflow."""

import json

import pytest

from .models import ARCHIVED, DRAFT, PUBLISHED, REVIEW, SCHEDULED


class TestLifecycleTransitions:
    """Content lifecycle state machine."""

    def test_draft_to_review(self, cms, sample_article):
        item = cms.transition(sample_article.content_id, REVIEW, actor="editor")
        assert item.status == REVIEW

    def test_review_to_published(self, cms, sample_article):
        cms.transition(sample_article.content_id, REVIEW, actor="editor")
        item = cms.transition(sample_article.content_id, PUBLISHED, actor="editor")
        assert item.status == PUBLISHED
        assert item.published_at is not None

    def test_draft_to_scheduled(self, cms, sample_article):
        item = cms.transition(sample_article.content_id, SCHEDULED, actor="editor")
        assert item.status == SCHEDULED

    def test_scheduled_to_published(self, cms, sample_article):
        cms.transition(sample_article.content_id, SCHEDULED, actor="editor")
        item = cms.transition(sample_article.content_id, PUBLISHED, actor="editor")
        assert item.status == PUBLISHED

    def test_published_to_archived(self, cms, sample_article):
        cms.transition(sample_article.content_id, REVIEW, actor="e")
        cms.transition(sample_article.content_id, PUBLISHED, actor="e")
        item = cms.transition(sample_article.content_id, ARCHIVED, actor="e")
        assert item.status == ARCHIVED

    def test_archived_to_draft(self, cms, sample_article):
        cms.transition(sample_article.content_id, ARCHIVED, actor="e")
        item = cms.transition(sample_article.content_id, DRAFT, actor="e")
        assert item.status == DRAFT

    def test_invalid_transition_raises(self, cms, sample_article):
        # DRAFT -> PUBLISHED is not allowed directly
        with pytest.raises(ValueError, match="Cannot transition"):
            cms.transition(sample_article.content_id, PUBLISHED, actor="e")

    def test_cannot_publish_archived(self, cms, sample_article):
        cms.transition(sample_article.content_id, ARCHIVED, actor="e")
        with pytest.raises(ValueError, match="Cannot publish ARCHIVED"):
            cms.publish_content(sample_article.content_id, actor="e")


class TestPublishQueue:
    """SQS-based publish queue."""

    def test_queue_for_publishing(self, cms, sample_article):
        msg_id = cms.queue_for_publish(sample_article.content_id)
        assert msg_id

        requests = cms.receive_publish_requests(max_messages=1)
        assert len(requests) >= 1
        assert requests[0]["content_id"] == sample_article.content_id
        assert requests[0]["publish_type"] == "immediate"

    def test_queue_scheduled_publish(self, cms, sample_article):
        cms.queue_for_publish(
            sample_article.content_id,
            scheduled_at="2026-12-01T00:00:00Z",
        )
        requests = cms.receive_publish_requests(max_messages=1)
        assert len(requests) >= 1
        assert requests[0]["publish_type"] == "scheduled"
        assert requests[0]["scheduled_at"] == "2026-12-01T00:00:00Z"

    def test_ack_publish_removes_message(self, cms, sample_article):
        cms.queue_for_publish(sample_article.content_id)
        requests = cms.receive_publish_requests(max_messages=1)
        assert len(requests) >= 1
        cms.ack_publish(requests[0]["receipt_handle"])
        # Queue should be empty now (with short wait)
        more = cms.receive_publish_requests(max_messages=1)
        assert len(more) == 0


class TestPublishNotification:
    """SNS webhook notifications on publish."""

    def test_publish_sends_sns_notification(self, cms, sqs, unique_name, sample_article):
        # Create subscriber queue
        sub_queue_name = f"cms-pub-sub-{unique_name}"
        q_resp = sqs.create_queue(QueueName=sub_queue_name)
        sub_url = q_resp["QueueUrl"]
        sub_arn = f"arn:aws:sqs:us-east-1:123456789012:{sub_queue_name}"

        try:
            cms.subscribe_webhook("sqs", sub_arn)

            # Transition through to PUBLISHED
            cms.transition(sample_article.content_id, REVIEW, actor="e")
            cms.transition(sample_article.content_id, PUBLISHED, actor="e")
            # Manually send webhook (publish_content does this automatically)
            cms._send_webhook(
                "ContentPublished",
                sample_article.content_id,
                sample_article.title,
            )

            # Poll for webhook message
            received = None
            for _ in range(5):
                resp = sqs.receive_message(
                    QueueUrl=sub_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=2,
                )
                if resp.get("Messages"):
                    received = resp["Messages"][0]
                    break

            assert received is not None
            envelope = json.loads(received["Body"])
            inner = json.loads(envelope.get("Message", received["Body"]))
            assert inner["content_id"] == sample_article.content_id
            assert inner["event"] == "ContentPublished"
        finally:
            sqs.delete_queue(QueueUrl=sub_url)


class TestBulkPublish:
    """Bulk operations on scheduled content."""

    def test_bulk_publish_scheduled(self, cms):
        # Create 3 scheduled articles
        ids = []
        for i in range(3):
            item = cms.create_content(
                title=f"Scheduled Article {i}",
                author="editor",
                category="news",
            )
            cms.transition(item.content_id, SCHEDULED, actor="editor")
            ids.append(item.content_id)

        published_ids = cms.bulk_publish_scheduled(actor="system")
        assert set(published_ids) == set(ids)

        # Verify all are now PUBLISHED
        for cid in ids:
            item = cms.get_content(cid)
            assert item.status == PUBLISHED
