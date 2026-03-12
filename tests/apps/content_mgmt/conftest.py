"""
Fixtures for content management system tests.

Provides a fully-initialised ``ContentManagementSystem`` instance with all
AWS resources created, plus convenience fixtures for pre-populated content.
"""

import pytest

from .app import ContentManagementSystem


@pytest.fixture
def cms(s3, dynamodb, sqs, sns, logs, events, unique_name):
    """A ContentManagementSystem with all resources created."""
    system = ContentManagementSystem(
        s3=s3,
        dynamodb=dynamodb,
        sqs=sqs,
        sns=sns,
        logs=logs,
        events=events,
        unique_name=unique_name,
    )
    system.setup()
    yield system
    system.teardown()


@pytest.fixture
def sample_article(cms):
    """A pre-created article in DRAFT status."""
    return cms.create_content(
        content_type="article",
        title="Sample Test Article",
        body="This is the body of a sample article for testing.",
        author="test-author",
        category="tech",
        tags=["python", "testing"],
    )


@pytest.fixture
def sample_media(cms):
    """A pre-uploaded JPEG image."""
    return cms.upload_media(
        data=b"fake-jpeg-bytes-for-testing",
        filename="hero.jpg",
        content_type="image/jpeg",
        alt_text="A hero image",
        tags=["hero", "banner"],
    )
