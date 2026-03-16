"""
Content Management System -- a headless CMS built on AWS primitives.

Architecture
------------
- **S3**             : media asset storage (versioning enabled)
- **DynamoDB**       : content metadata with GSIs for category, status, author, publish-date
- **SQS**           : publish queue -- workers drain it to perform actual publication
- **SNS**           : webhook fan-out when content is published / updated
- **CloudWatch Logs**: immutable audit trail of every content mutation
- **EventBridge**   : scheduled-publishing rules

The class is self-contained: give it boto3 clients and a ``unique_name`` and it
will create all the AWS resources it needs.  Call ``teardown()`` to clean up.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from .models import (
    ARCHIVED,
    DRAFT,
    PUBLISHED,
    REVIEW,
    SCHEDULED,
    VALID_CONTENT_TYPES,
    VALID_TRANSITIONS,
    AuditEntry,
    ContentItem,
    ContentStats,
    ContentVersion,
    MediaAsset,
    PublishRequest,
)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _slugify(text: str) -> str:
    """Convert a title into a URL-safe slug."""
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


class ContentManagementSystem:
    """Full-featured headless CMS backed by six AWS services."""

    # --------------------------------------------------------------------- #
    # Construction / teardown
    # --------------------------------------------------------------------- #
    def __init__(
        self,
        s3,
        dynamodb,
        sqs,
        sns,
        logs,
        events,
        unique_name: str,
        region: str = "us-east-1",
        account_id: str = "123456789012",
    ) -> None:
        self.s3 = s3
        self.dynamodb = dynamodb
        self.sqs = sqs
        self.sns = sns
        self.logs = logs
        self.events = events
        self.unique_name = unique_name
        self.region = region
        self.account_id = account_id

        # Resource names (deterministic from unique_name)
        self.media_bucket = f"cms-media-{unique_name}"
        self.content_table = f"cms-content-{unique_name}"
        self.versions_table = f"cms-versions-{unique_name}"
        self.media_table = f"cms-media-meta-{unique_name}"
        self.publish_queue_name = f"cms-publish-{unique_name}"
        self.webhook_topic_name = f"cms-webhooks-{unique_name}"
        self.audit_log_group = f"/cms/audit/{unique_name}"
        self.audit_stream = "content-changes"

        # Resolved ARNs / URLs (populated by setup())
        self.publish_queue_url: str = ""
        self.webhook_topic_arn: str = ""

        # EventBridge rules we create (tracked for cleanup)
        self._eb_rules: list[str] = []

    # --------------------------------------------------------------------- #
    # Resource lifecycle
    # --------------------------------------------------------------------- #
    def setup(self) -> None:
        """Create all AWS resources."""
        self._create_s3_bucket()
        self._create_content_table()
        self._create_versions_table()
        self._create_media_table()
        self._create_publish_queue()
        self._create_webhook_topic()
        self._create_audit_log()

    def teardown(self) -> None:
        """Delete every resource we created.  Best-effort -- swallows errors."""
        self._delete_eb_rules()
        self._delete_audit_log()
        self._delete_webhook_topic()
        self._delete_publish_queue()
        self._delete_media_table()
        self._delete_versions_table()
        self._delete_content_table()
        self._delete_s3_bucket()

    # --------------------------------------------------------------------- #
    # Content CRUD
    # --------------------------------------------------------------------- #
    def create_content(
        self,
        *,
        content_type: str = "article",
        title: str,
        body: str = "",
        author: str = "system",
        category: str = "general",
        tags: list[str] | None = None,
        parent_id: str | None = None,
        related_ids: list[str] | None = None,
    ) -> ContentItem:
        """Create a new content item in DRAFT status."""
        if content_type not in VALID_CONTENT_TYPES:
            raise ValueError(f"Invalid content_type: {content_type}")

        content_id = uuid.uuid4().hex[:12]
        slug = _slugify(title)
        slug = self._ensure_unique_slug(slug, content_id)
        now = _now()
        tags = tags or []
        related_ids = related_ids or []

        item = ContentItem(
            content_id=content_id,
            content_type=content_type,
            title=title,
            slug=slug,
            body=body,
            author=author,
            category=category,
            tags=tags,
            status=DRAFT,
            created_at=now,
            updated_at=now,
            version=1,
            parent_id=parent_id,
            related_ids=related_ids,
        )
        self._put_content_item(item)
        self._save_version(item, updated_by=author)
        self._audit("content_created", content_id, author, f"type={content_type}")
        return item

    def get_content(self, content_id: str) -> ContentItem | None:
        """Read a single content item by ID."""
        resp = self.dynamodb.get_item(
            TableName=self.content_table,
            Key={"content_id": {"S": content_id}},
        )
        raw = resp.get("Item")
        if not raw:
            return None
        return self._item_from_dynamo(raw)

    def update_content(
        self,
        content_id: str,
        *,
        title: str | None = None,
        body: str | None = None,
        category: str | None = None,
        tags: list[str] | None = None,
        author: str = "system",
    ) -> ContentItem:
        """Update mutable fields; increments version and records a snapshot."""
        existing = self.get_content(content_id)
        if existing is None:
            raise ValueError(f"Content not found: {content_id}")

        now = _now()
        updates: list[str] = ["updated_at = :u", "version = :v"]
        names: dict[str, str] = {}
        values: dict[str, Any] = {
            ":u": {"S": now},
            ":v": {"N": str(existing.version + 1)},
        }
        changed_fields: list[str] = []

        if title is not None and title != existing.title:
            updates.append("title = :t")
            values[":t"] = {"S": title}
            new_slug = _slugify(title)
            new_slug = self._ensure_unique_slug(new_slug, content_id)
            updates.append("slug = :sl")
            values[":sl"] = {"S": new_slug}
            existing.title = title
            existing.slug = new_slug
            changed_fields.append("title")

        if body is not None and body != existing.body:
            updates.append("body = :b")
            values[":b"] = {"S": body}
            existing.body = body
            changed_fields.append("body")

        if category is not None and category != existing.category:
            updates.append("category = :c")
            values[":c"] = {"S": category}
            existing.category = category
            changed_fields.append("category")

        if tags is not None:
            updates.append("tags = :tg")
            values[":tg"] = {"SS": tags} if tags else {"SS": ["__empty__"]}
            existing.tags = tags
            changed_fields.append("tags")

        kwargs: dict[str, Any] = {
            "TableName": self.content_table,
            "Key": {"content_id": {"S": content_id}},
            "UpdateExpression": "SET " + ", ".join(updates),
            "ExpressionAttributeValues": values,
        }
        if names:
            kwargs["ExpressionAttributeNames"] = names
        self.dynamodb.update_item(**kwargs)
        existing.version += 1
        existing.updated_at = now

        self._save_version(existing, updated_by=author)
        self._audit(
            "content_updated",
            content_id,
            author,
            f"fields={','.join(changed_fields)}",
        )
        return existing

    def delete_content(self, content_id: str, *, actor: str = "system") -> None:
        """Hard-delete a content item, its versions, and any associated media metadata."""
        self.dynamodb.delete_item(
            TableName=self.content_table,
            Key={"content_id": {"S": content_id}},
        )
        # Delete version history
        versions = self.list_versions(content_id)
        for v in versions:
            self.dynamodb.delete_item(
                TableName=self.versions_table,
                Key={
                    "content_id": {"S": content_id},
                    "version": {"N": str(v.version)},
                },
            )
        self._audit("content_deleted", content_id, actor, "")

    # --------------------------------------------------------------------- #
    # Content lifecycle transitions
    # --------------------------------------------------------------------- #
    def transition(self, content_id: str, new_status: str, *, actor: str = "system") -> ContentItem:
        """Move content to a new lifecycle state."""
        item = self.get_content(content_id)
        if item is None:
            raise ValueError(f"Content not found: {content_id}")

        allowed = VALID_TRANSITIONS.get(item.status, set())
        if new_status not in allowed:
            raise ValueError(
                f"Cannot transition from {item.status} to {new_status}. Allowed: {allowed}"
            )

        now = _now()
        updates = ["#s = :st", "updated_at = :u"]
        names = {"#s": "status"}
        values: dict[str, Any] = {":st": {"S": new_status}, ":u": {"S": now}}

        if new_status == PUBLISHED:
            updates.append("published_at = :pa")
            values[":pa"] = {"S": now}

        self.dynamodb.update_item(
            TableName=self.content_table,
            Key={"content_id": {"S": content_id}},
            UpdateExpression="SET " + ", ".join(updates),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

        item.status = new_status
        item.updated_at = now
        if new_status == PUBLISHED:
            item.published_at = now

        self._audit(
            f"status_{new_status.lower()}",
            content_id,
            actor,
            f"from={item.status}",
        )
        return item

    # --------------------------------------------------------------------- #
    # Media management
    # --------------------------------------------------------------------- #
    def upload_media(
        self,
        *,
        data: bytes,
        filename: str,
        content_type: str = "image/jpeg",
        alt_text: str = "",
        tags: list[str] | None = None,
    ) -> MediaAsset:
        """Upload a media file to S3 and record metadata in DynamoDB."""
        asset_id = uuid.uuid4().hex[:12]
        key = f"media/{asset_id}/{filename}"
        tags = tags or []

        self.s3.put_object(
            Bucket=self.media_bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )

        asset = MediaAsset(
            asset_id=asset_id,
            bucket=self.media_bucket,
            key=key,
            content_type=content_type,
            size=len(data),
            alt_text=alt_text,
            tags=tags,
        )
        self._put_media_metadata(asset)
        self._audit("media_uploaded", asset_id, "system", f"key={key}")
        return asset

    def get_media(self, asset_id: str) -> MediaAsset | None:
        """Read media metadata by asset ID."""
        resp = self.dynamodb.get_item(
            TableName=self.media_table,
            Key={"asset_id": {"S": asset_id}},
        )
        raw = resp.get("Item")
        if not raw:
            return None
        return self._media_from_dynamo(raw)

    def get_media_data(self, asset: MediaAsset) -> bytes:
        """Download the actual file bytes from S3."""
        resp = self.s3.get_object(Bucket=asset.bucket, Key=asset.key)
        return resp["Body"].read()

    def list_media(self) -> list[MediaAsset]:
        """List all media assets."""
        resp = self.dynamodb.scan(TableName=self.media_table)
        return [self._media_from_dynamo(item) for item in resp.get("Items", [])]

    def delete_media(self, asset_id: str, *, actor: str = "system") -> None:
        """Delete media from both S3 and DynamoDB."""
        asset = self.get_media(asset_id)
        if asset is None:
            return
        try:
            self.s3.delete_object(Bucket=self.media_bucket, Key=asset.key)
        except Exception:
            pass  # best-effort cleanup
        self.dynamodb.delete_item(
            TableName=self.media_table,
            Key={"asset_id": {"S": asset_id}},
        )
        self._audit("media_deleted", asset_id, actor, f"key={asset.key}")

    def media_versions(self, asset: MediaAsset) -> list[dict]:
        """List S3 versions for a media asset."""
        resp = self.s3.list_object_versions(Bucket=asset.bucket, Prefix=asset.key)
        return resp.get("Versions", [])

    def reupload_media(self, asset: MediaAsset, data: bytes) -> MediaAsset:
        """Upload a new version of an existing media asset."""
        self.s3.put_object(
            Bucket=asset.bucket,
            Key=asset.key,
            Body=data,
            ContentType=asset.content_type,
        )
        asset.size = len(data)
        self._put_media_metadata(asset)
        self._audit("media_reuploaded", asset.asset_id, "system", f"key={asset.key}")
        return asset

    # --------------------------------------------------------------------- #
    # Publishing workflow
    # --------------------------------------------------------------------- #
    def queue_for_publish(self, content_id: str, *, scheduled_at: str | None = None) -> str:
        """Send a publish request onto the SQS queue.  Returns message ID."""
        req = PublishRequest(
            content_id=content_id,
            scheduled_at=scheduled_at,
            publish_type="scheduled" if scheduled_at else "immediate",
        )
        resp = self.sqs.send_message(
            QueueUrl=self.publish_queue_url,
            MessageBody=json.dumps(
                {
                    "content_id": req.content_id,
                    "scheduled_at": req.scheduled_at,
                    "publish_type": req.publish_type,
                }
            ),
            MessageAttributes={
                "publish_type": {
                    "DataType": "String",
                    "StringValue": req.publish_type,
                },
            },
        )
        self._audit("publish_queued", content_id, "system", f"type={req.publish_type}")
        return resp["MessageId"]

    def receive_publish_requests(self, max_messages: int = 10) -> list[dict]:
        """Drain up to *max_messages* from the publish queue."""
        resp = self.sqs.receive_message(
            QueueUrl=self.publish_queue_url,
            MaxNumberOfMessages=min(max_messages, 10),
            WaitTimeSeconds=3,
            MessageAttributeNames=["All"],
        )
        messages = resp.get("Messages", [])
        result = []
        for msg in messages:
            body = json.loads(msg["Body"])
            result.append(
                {
                    "content_id": body["content_id"],
                    "scheduled_at": body.get("scheduled_at"),
                    "publish_type": body.get("publish_type", "immediate"),
                    "receipt_handle": msg["ReceiptHandle"],
                }
            )
        return result

    def ack_publish(self, receipt_handle: str) -> None:
        """Delete a processed publish message from the queue."""
        self.sqs.delete_message(
            QueueUrl=self.publish_queue_url,
            ReceiptHandle=receipt_handle,
        )

    def publish_content(self, content_id: str, *, actor: str = "system") -> ContentItem:
        """Full publish: transition state + send SNS notification."""
        item = self.get_content(content_id)
        if item is None:
            raise ValueError(f"Content not found: {content_id}")
        if item.status == ARCHIVED:
            raise ValueError("Cannot publish ARCHIVED content")

        # Transition to PUBLISHED (may go through REVIEW first)
        if item.status == DRAFT:
            self.transition(content_id, REVIEW, actor=actor)
        if item.status in (REVIEW, SCHEDULED):
            item = self.transition(content_id, PUBLISHED, actor=actor)
        elif item.status == PUBLISHED:
            pass  # already published
        else:
            item = self.transition(content_id, PUBLISHED, actor=actor)

        self._send_webhook("ContentPublished", content_id, item.title)
        return item

    def bulk_publish_scheduled(self, *, actor: str = "system") -> list[str]:
        """Publish every item currently in SCHEDULED status.  Returns IDs."""
        items = self.search_by_status(SCHEDULED)
        published_ids = []
        for item in items:
            self.transition(item.content_id, PUBLISHED, actor=actor)
            self._send_webhook("ContentPublished", item.content_id, item.title)
            published_ids.append(item.content_id)
        return published_ids

    def archive_old_content(self, before_date: str, *, actor: str = "system") -> list[str]:
        """Archive all PUBLISHED content updated before *before_date*."""
        items = self.search_by_status(PUBLISHED)
        archived = []
        for item in items:
            if item.updated_at < before_date:
                self.transition(item.content_id, ARCHIVED, actor=actor)
                archived.append(item.content_id)
        return archived

    # --------------------------------------------------------------------- #
    # Webhook notifications (SNS)
    # --------------------------------------------------------------------- #
    def subscribe_webhook(self, protocol: str, endpoint: str) -> str:
        """Subscribe an endpoint to the webhook topic.  Returns subscription ARN."""
        resp = self.sns.subscribe(
            TopicArn=self.webhook_topic_arn,
            Protocol=protocol,
            Endpoint=endpoint,
        )
        return resp["SubscriptionArn"]

    def _send_webhook(self, event_type: str, content_id: str, title: str) -> None:
        """Publish a webhook event to SNS."""
        self.sns.publish(
            TopicArn=self.webhook_topic_arn,
            Subject=event_type,
            Message=json.dumps(
                {
                    "event": event_type,
                    "content_id": content_id,
                    "title": title,
                    "timestamp": _now(),
                }
            ),
        )

    # --------------------------------------------------------------------- #
    # Search / query
    # --------------------------------------------------------------------- #
    def search_by_category(self, category: str, *, limit: int = 50) -> list[ContentItem]:
        """Query the by-category GSI."""
        resp = self.dynamodb.query(
            TableName=self.content_table,
            IndexName="by-category",
            KeyConditionExpression="category = :c",
            ExpressionAttributeValues={":c": {"S": category}},
            Limit=limit,
        )
        return [self._item_from_dynamo(i) for i in resp.get("Items", [])]

    def search_by_status(self, status: str, *, limit: int = 50) -> list[ContentItem]:
        """Query the by-status GSI."""
        resp = self.dynamodb.query(
            TableName=self.content_table,
            IndexName="by-status",
            KeyConditionExpression="#s = :st",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":st": {"S": status}},
            Limit=limit,
        )
        return [self._item_from_dynamo(i) for i in resp.get("Items", [])]

    def search_by_author(self, author: str, *, limit: int = 50) -> list[ContentItem]:
        """Query the by-author GSI."""
        resp = self.dynamodb.query(
            TableName=self.content_table,
            IndexName="by-author",
            KeyConditionExpression="author = :a",
            ExpressionAttributeValues={":a": {"S": author}},
            Limit=limit,
        )
        return [self._item_from_dynamo(i) for i in resp.get("Items", [])]

    def search_by_publish_date(self, start: str, end: str, *, limit: int = 50) -> list[ContentItem]:
        """Query the by-publish-date GSI for a date range."""
        resp = self.dynamodb.query(
            TableName=self.content_table,
            IndexName="by-publish-date",
            KeyConditionExpression=("content_type = :ct AND published_at BETWEEN :s AND :e"),
            ExpressionAttributeValues={
                ":ct": {"S": "article"},
                ":s": {"S": start},
                ":e": {"S": end},
            },
            Limit=limit,
        )
        return [self._item_from_dynamo(i) for i in resp.get("Items", [])]

    def search_by_tags(self, tag: str) -> list[ContentItem]:
        """Scan + filter for items containing a specific tag."""
        resp = self.dynamodb.scan(
            TableName=self.content_table,
            FilterExpression="contains(tags, :tag)",
            ExpressionAttributeValues={":tag": {"S": tag}},
        )
        return [self._item_from_dynamo(i) for i in resp.get("Items", [])]

    def search_compound(self, *, category: str, status: str, limit: int = 50) -> list[ContentItem]:
        """Query by-category GSI then client-side filter by status."""
        items = self.search_by_category(category, limit=limit)
        return [i for i in items if i.status == status]

    # --------------------------------------------------------------------- #
    # Versioning
    # --------------------------------------------------------------------- #
    def list_versions(self, content_id: str) -> list[ContentVersion]:
        """Return all saved versions of a content item, oldest first."""
        resp = self.dynamodb.query(
            TableName=self.versions_table,
            KeyConditionExpression="content_id = :cid",
            ExpressionAttributeValues={":cid": {"S": content_id}},
            ScanIndexForward=True,
        )
        return [self._version_from_dynamo(i) for i in resp.get("Items", [])]

    def get_version(self, content_id: str, version: int) -> ContentVersion | None:
        """Read a specific version snapshot."""
        resp = self.dynamodb.get_item(
            TableName=self.versions_table,
            Key={
                "content_id": {"S": content_id},
                "version": {"N": str(version)},
            },
        )
        raw = resp.get("Item")
        if not raw:
            return None
        return self._version_from_dynamo(raw)

    def revert_to_version(
        self, content_id: str, version: int, *, actor: str = "system"
    ) -> ContentItem:
        """Restore content to a previous version (creates a new version)."""
        old = self.get_version(content_id, version)
        if old is None:
            raise ValueError(f"Version {version} not found for {content_id}")
        return self.update_content(content_id, title=old.title, body=old.body, author=actor)

    # --------------------------------------------------------------------- #
    # Content relationships
    # --------------------------------------------------------------------- #
    def set_parent(self, child_id: str, parent_id: str) -> None:
        """Set the parent of a content item (for page hierarchies)."""
        self.dynamodb.update_item(
            TableName=self.content_table,
            Key={"content_id": {"S": child_id}},
            UpdateExpression="SET parent_id = :p",
            ExpressionAttributeValues={":p": {"S": parent_id}},
        )

    def get_children(self, parent_id: str) -> list[ContentItem]:
        """Find all content items whose parent is *parent_id*."""
        resp = self.dynamodb.scan(
            TableName=self.content_table,
            FilterExpression="parent_id = :p",
            ExpressionAttributeValues={":p": {"S": parent_id}},
        )
        return [self._item_from_dynamo(i) for i in resp.get("Items", [])]

    def add_related(self, content_id: str, related_id: str) -> None:
        """Add a related-content link."""
        self.dynamodb.update_item(
            TableName=self.content_table,
            Key={"content_id": {"S": content_id}},
            UpdateExpression=(
                "SET related_ids = list_append(if_not_exists(related_ids, :empty), :r)"
            ),
            ExpressionAttributeValues={
                ":r": {"L": [{"S": related_id}]},
                ":empty": {"L": []},
            },
        )

    # --------------------------------------------------------------------- #
    # Slug management
    # --------------------------------------------------------------------- #
    def _ensure_unique_slug(self, slug: str, content_id: str) -> str:
        """Append a numeric suffix if the slug already exists for a different item."""
        base_slug = slug
        counter = 0
        while True:
            candidate = f"{base_slug}-{counter}" if counter else base_slug
            resp = self.dynamodb.scan(
                TableName=self.content_table,
                FilterExpression="slug = :s AND content_id <> :cid",
                ExpressionAttributeValues={
                    ":s": {"S": candidate},
                    ":cid": {"S": content_id},
                },
            )
            if not resp.get("Items"):
                return candidate
            counter += 1

    # --------------------------------------------------------------------- #
    # Audit trail (CloudWatch Logs)
    # --------------------------------------------------------------------- #
    def _audit(self, action: str, content_id: str, actor: str, details: str) -> None:
        """Append an entry to the audit log."""
        entry = AuditEntry(
            content_id=content_id,
            action=action,
            actor=actor,
            timestamp=_now(),
            details=details,
        )
        ts = int(time.time() * 1000)
        try:
            self.logs.put_log_events(
                logGroupName=self.audit_log_group,
                logStreamName=self.audit_stream,
                logEvents=[
                    {
                        "timestamp": ts,
                        "message": json.dumps(
                            {
                                "content_id": entry.content_id,
                                "action": entry.action,
                                "actor": entry.actor,
                                "timestamp": entry.timestamp,
                                "details": entry.details,
                            }
                        ),
                    }
                ],
            )
        except Exception:
            pass  # best-effort audit

    def get_audit_trail(self, content_id: str | None = None) -> list[AuditEntry]:
        """Read the audit log, optionally filtering by content_id."""
        try:
            if content_id:
                resp = self.logs.filter_log_events(
                    logGroupName=self.audit_log_group,
                    filterPattern=content_id,
                )
            else:
                resp = self.logs.get_log_events(
                    logGroupName=self.audit_log_group,
                    logStreamName=self.audit_stream,
                    startFromHead=True,
                )
            events = resp.get("events", [])
            entries = []
            for evt in events:
                try:
                    data = json.loads(evt["message"])
                    entries.append(
                        AuditEntry(
                            content_id=data.get("content_id", ""),
                            action=data.get("action", ""),
                            actor=data.get("actor", ""),
                            timestamp=data.get("timestamp", ""),
                            details=data.get("details", ""),
                        )
                    )
                except (json.JSONDecodeError, KeyError):
                    pass  # intentionally ignored
            return entries
        except Exception:
            return []

    # --------------------------------------------------------------------- #
    # Scheduled publishing (EventBridge)
    # --------------------------------------------------------------------- #
    def create_schedule_rule(self, content_id: str, cron_expression: str) -> str:
        """Create an EventBridge rule for scheduled publishing."""
        rule_name = f"cms-sched-{content_id}-{self.unique_name}"
        self.events.put_rule(
            Name=rule_name,
            ScheduleExpression=cron_expression,
            State="ENABLED",
        )
        queue_arn = f"arn:aws:sqs:{self.region}:{self.account_id}:{self.publish_queue_name}"
        self.events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "publish-queue", "Arn": queue_arn}],
        )
        self._eb_rules.append(rule_name)
        return rule_name

    def describe_schedule_rule(self, rule_name: str) -> dict:
        """Describe an EventBridge schedule rule."""
        return self.events.describe_rule(Name=rule_name)

    # --------------------------------------------------------------------- #
    # Statistics
    # --------------------------------------------------------------------- #
    def content_stats_by_category(self) -> list[ContentStats]:
        """Scan the table and compute per-category statistics."""
        resp = self.dynamodb.scan(TableName=self.content_table)
        items = resp.get("Items", [])
        buckets: dict[str, ContentStats] = {}
        for raw in items:
            cat = raw.get("category", {}).get("S", "uncategorized")
            status = raw.get("status", {}).get("S", DRAFT)
            if cat not in buckets:
                buckets[cat] = ContentStats(category=cat)
            stats = buckets[cat]
            stats.total += 1
            if status == PUBLISHED:
                stats.published += 1
            elif status == DRAFT:
                stats.draft += 1
            elif status == ARCHIVED:
                stats.archived += 1
        return list(buckets.values())

    # --------------------------------------------------------------------- #
    # Private helpers -- DynamoDB marshalling
    # --------------------------------------------------------------------- #
    def _put_content_item(self, item: ContentItem) -> None:
        ddb_item: dict[str, Any] = {
            "content_id": {"S": item.content_id},
            "content_type": {"S": item.content_type},
            "title": {"S": item.title},
            "slug": {"S": item.slug},
            "body": {"S": item.body},
            "author": {"S": item.author},
            "category": {"S": item.category},
            "status": {"S": item.status},
            "created_at": {"S": item.created_at},
            "updated_at": {"S": item.updated_at},
            "version": {"N": str(item.version)},
        }
        if item.tags:
            ddb_item["tags"] = {"SS": item.tags}
        if item.published_at:
            ddb_item["published_at"] = {"S": item.published_at}
        if item.parent_id:
            ddb_item["parent_id"] = {"S": item.parent_id}
        if item.related_ids:
            ddb_item["related_ids"] = {"L": [{"S": rid} for rid in item.related_ids]}
        self.dynamodb.put_item(TableName=self.content_table, Item=ddb_item)

    def _item_from_dynamo(self, raw: dict) -> ContentItem:
        tags_raw = raw.get("tags", {})
        tags = list(tags_raw.get("SS", []))
        related_raw = raw.get("related_ids", {}).get("L", [])
        related = [r["S"] for r in related_raw]
        return ContentItem(
            content_id=raw["content_id"]["S"],
            content_type=raw.get("content_type", {}).get("S", "article"),
            title=raw.get("title", {}).get("S", ""),
            slug=raw.get("slug", {}).get("S", ""),
            body=raw.get("body", {}).get("S", ""),
            author=raw.get("author", {}).get("S", ""),
            category=raw.get("category", {}).get("S", ""),
            tags=tags,
            status=raw.get("status", {}).get("S", DRAFT),
            created_at=raw.get("created_at", {}).get("S", ""),
            updated_at=raw.get("updated_at", {}).get("S", ""),
            published_at=raw.get("published_at", {}).get("S"),
            version=int(raw.get("version", {}).get("N", "1")),
            parent_id=raw.get("parent_id", {}).get("S"),
            related_ids=related,
        )

    def _put_media_metadata(self, asset: MediaAsset) -> None:
        ddb_item: dict[str, Any] = {
            "asset_id": {"S": asset.asset_id},
            "bucket": {"S": asset.bucket},
            "key_path": {"S": asset.key},
            "content_type": {"S": asset.content_type},
            "size": {"N": str(asset.size)},
            "alt_text": {"S": asset.alt_text},
        }
        if asset.tags:
            ddb_item["tags"] = {"SS": asset.tags}
        self.dynamodb.put_item(TableName=self.media_table, Item=ddb_item)

    def _media_from_dynamo(self, raw: dict) -> MediaAsset:
        tags_raw = raw.get("tags", {})
        tags = list(tags_raw.get("SS", []))
        return MediaAsset(
            asset_id=raw["asset_id"]["S"],
            bucket=raw.get("bucket", {}).get("S", ""),
            key=raw.get("key_path", {}).get("S", ""),
            content_type=raw.get("content_type", {}).get("S", ""),
            size=int(raw.get("size", {}).get("N", "0")),
            alt_text=raw.get("alt_text", {}).get("S", ""),
            tags=tags,
        )

    def _save_version(self, item: ContentItem, *, updated_by: str) -> None:
        self.dynamodb.put_item(
            TableName=self.versions_table,
            Item={
                "content_id": {"S": item.content_id},
                "version": {"N": str(item.version)},
                "title": {"S": item.title},
                "body": {"S": item.body},
                "updated_at": {"S": item.updated_at},
                "updated_by": {"S": updated_by},
            },
        )

    def _version_from_dynamo(self, raw: dict) -> ContentVersion:
        return ContentVersion(
            content_id=raw["content_id"]["S"],
            version=int(raw["version"]["N"]),
            title=raw.get("title", {}).get("S", ""),
            body=raw.get("body", {}).get("S", ""),
            updated_at=raw.get("updated_at", {}).get("S", ""),
            updated_by=raw.get("updated_by", {}).get("S", ""),
        )

    # --------------------------------------------------------------------- #
    # Private helpers -- resource creation / deletion
    # --------------------------------------------------------------------- #
    def _create_s3_bucket(self) -> None:
        self.s3.create_bucket(Bucket=self.media_bucket)
        self.s3.put_bucket_versioning(
            Bucket=self.media_bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

    def _create_content_table(self) -> None:
        self.dynamodb.create_table(
            TableName=self.content_table,
            KeySchema=[{"AttributeName": "content_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "content_id", "AttributeType": "S"},
                {"AttributeName": "category", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
                {"AttributeName": "author", "AttributeType": "S"},
                {"AttributeName": "updated_at", "AttributeType": "S"},
                {"AttributeName": "content_type", "AttributeType": "S"},
                {"AttributeName": "published_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "by-category",
                    "KeySchema": [
                        {"AttributeName": "category", "KeyType": "HASH"},
                        {"AttributeName": "updated_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "by-status",
                    "KeySchema": [
                        {"AttributeName": "status", "KeyType": "HASH"},
                        {"AttributeName": "updated_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "by-author",
                    "KeySchema": [
                        {"AttributeName": "author", "KeyType": "HASH"},
                        {"AttributeName": "updated_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "by-publish-date",
                    "KeySchema": [
                        {"AttributeName": "content_type", "KeyType": "HASH"},
                        {"AttributeName": "published_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )

    def _create_versions_table(self) -> None:
        self.dynamodb.create_table(
            TableName=self.versions_table,
            KeySchema=[
                {"AttributeName": "content_id", "KeyType": "HASH"},
                {"AttributeName": "version", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "content_id", "AttributeType": "S"},
                {"AttributeName": "version", "AttributeType": "N"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

    def _create_media_table(self) -> None:
        self.dynamodb.create_table(
            TableName=self.media_table,
            KeySchema=[{"AttributeName": "asset_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "asset_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

    def _create_publish_queue(self) -> None:
        resp = self.sqs.create_queue(QueueName=self.publish_queue_name)
        self.publish_queue_url = resp["QueueUrl"]

    def _create_webhook_topic(self) -> None:
        resp = self.sns.create_topic(Name=self.webhook_topic_name)
        self.webhook_topic_arn = resp["TopicArn"]

    def _create_audit_log(self) -> None:
        self.logs.create_log_group(logGroupName=self.audit_log_group)
        self.logs.create_log_stream(
            logGroupName=self.audit_log_group,
            logStreamName=self.audit_stream,
        )

    def _delete_s3_bucket(self) -> None:
        try:
            resp = self.s3.list_object_versions(Bucket=self.media_bucket)
            for v in resp.get("Versions", []):
                self.s3.delete_object(
                    Bucket=self.media_bucket, Key=v["Key"], VersionId=v["VersionId"]
                )
            for dm in resp.get("DeleteMarkers", []):
                self.s3.delete_object(
                    Bucket=self.media_bucket, Key=dm["Key"], VersionId=dm["VersionId"]
                )
        except Exception:
            pass  # best-effort cleanup
        try:
            objs = self.s3.list_objects_v2(Bucket=self.media_bucket).get("Contents", [])
            for obj in objs:
                self.s3.delete_object(Bucket=self.media_bucket, Key=obj["Key"])
        except Exception:
            pass  # best-effort cleanup
        try:
            self.s3.delete_bucket(Bucket=self.media_bucket)
        except Exception:
            pass  # best-effort cleanup

    def _delete_content_table(self) -> None:
        try:
            self.dynamodb.delete_table(TableName=self.content_table)
        except Exception:
            pass  # best-effort cleanup

    def _delete_versions_table(self) -> None:
        try:
            self.dynamodb.delete_table(TableName=self.versions_table)
        except Exception:
            pass  # best-effort cleanup

    def _delete_media_table(self) -> None:
        try:
            self.dynamodb.delete_table(TableName=self.media_table)
        except Exception:
            pass  # best-effort cleanup

    def _delete_publish_queue(self) -> None:
        try:
            self.sqs.delete_queue(QueueUrl=self.publish_queue_url)
        except Exception:
            pass  # best-effort cleanup

    def _delete_webhook_topic(self) -> None:
        try:
            self.sns.delete_topic(TopicArn=self.webhook_topic_arn)
        except Exception:
            pass  # best-effort cleanup

    def _delete_audit_log(self) -> None:
        try:
            self.logs.delete_log_stream(
                logGroupName=self.audit_log_group,
                logStreamName=self.audit_stream,
            )
        except Exception:
            pass  # best-effort cleanup
        try:
            self.logs.delete_log_group(logGroupName=self.audit_log_group)
        except Exception:
            pass  # best-effort cleanup

    def _delete_eb_rules(self) -> None:
        for rule_name in self._eb_rules:
            try:
                self.events.remove_targets(Rule=rule_name, Ids=["publish-queue"])
            except Exception:
                pass  # best-effort cleanup
            try:
                self.events.delete_rule(Name=rule_name)
            except Exception:
                pass  # best-effort cleanup
        self._eb_rules.clear()
