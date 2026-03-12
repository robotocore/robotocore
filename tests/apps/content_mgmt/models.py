"""
Data models for the Content Management System.

Plain dataclasses -- no AWS or framework dependencies.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _uuid() -> str:
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# Content lifecycle states
# ---------------------------------------------------------------------------
DRAFT = "DRAFT"
REVIEW = "REVIEW"
SCHEDULED = "SCHEDULED"
PUBLISHED = "PUBLISHED"
ARCHIVED = "ARCHIVED"

VALID_STATES = {DRAFT, REVIEW, SCHEDULED, PUBLISHED, ARCHIVED}
VALID_TRANSITIONS: dict[str, set[str]] = {
    DRAFT: {REVIEW, SCHEDULED, ARCHIVED},
    REVIEW: {DRAFT, SCHEDULED, PUBLISHED, ARCHIVED},
    SCHEDULED: {DRAFT, PUBLISHED, ARCHIVED},
    PUBLISHED: {ARCHIVED},
    ARCHIVED: {DRAFT},
}

# ---------------------------------------------------------------------------
# Content types
# ---------------------------------------------------------------------------
ARTICLE = "article"
PAGE = "page"
BLOG_POST = "blog_post"
MEDIA_ASSET = "media_asset"
SNIPPET = "snippet"

VALID_CONTENT_TYPES = {ARTICLE, PAGE, BLOG_POST, MEDIA_ASSET, SNIPPET}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class ContentItem:
    content_id: str = field(default_factory=_uuid)
    content_type: str = ARTICLE
    title: str = ""
    slug: str = ""
    body: str = ""
    author: str = ""
    category: str = ""
    tags: list[str] = field(default_factory=list)
    status: str = DRAFT
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    published_at: str | None = None
    version: int = 1
    parent_id: str | None = None
    related_ids: list[str] = field(default_factory=list)


@dataclass
class MediaAsset:
    asset_id: str = field(default_factory=_uuid)
    bucket: str = ""
    key: str = ""
    content_type: str = "image/jpeg"
    size: int = 0
    alt_text: str = ""
    tags: list[str] = field(default_factory=list)


@dataclass
class PublishRequest:
    content_id: str = ""
    scheduled_at: str | None = None
    publish_type: str = "immediate"


@dataclass
class ContentVersion:
    content_id: str = ""
    version: int = 1
    title: str = ""
    body: str = ""
    updated_at: str = field(default_factory=_now)
    updated_by: str = ""


@dataclass
class AuditEntry:
    content_id: str = ""
    action: str = ""
    actor: str = ""
    timestamp: str = field(default_factory=_now)
    details: str = ""


@dataclass
class ContentStats:
    category: str = ""
    total: int = 0
    published: int = 0
    draft: int = 0
    archived: int = 0
