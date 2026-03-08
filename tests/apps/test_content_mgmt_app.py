"""
Content Management System Application Tests

Simulates a headless CMS where:
- Media assets stored in S3 (with versioning for edit history)
- Content metadata in DynamoDB (GSI by category, GSI by status)
- Publish queue in SQS (content awaiting publication)
- Webhook notifications via SNS when content is published
- Content audit trail in CloudWatch Logs
- Scheduled publishing via EventBridge rules
"""

import json
import time
import uuid

import pytest


@pytest.fixture
def media_bucket(s3, unique_name):
    bucket = f"cms-media-{unique_name}"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    yield bucket
    # Cleanup: delete all versions + delete markers + bucket
    try:
        resp = s3.list_object_versions(Bucket=bucket)
        for v in resp.get("Versions", []):
            s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
        for dm in resp.get("DeleteMarkers", []):
            s3.delete_object(Bucket=bucket, Key=dm["Key"], VersionId=dm["VersionId"])
    except Exception:
        pass
    try:
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except Exception:
        pass
    s3.delete_bucket(Bucket=bucket)


@pytest.fixture
def content_table(dynamodb, unique_name):
    table_name = f"cms-content-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "content_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "content_id", "AttributeType": "S"},
            {"AttributeName": "category", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
            {"AttributeName": "updated_at", "AttributeType": "S"},
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
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def publish_queue(sqs, unique_name):
    queue_name = f"cms-publish-{unique_name}"
    resp = sqs.create_queue(QueueName=queue_name)
    queue_url = resp["QueueUrl"]
    yield queue_url
    sqs.delete_queue(QueueUrl=queue_url)


@pytest.fixture
def webhook_topic(sns, unique_name):
    topic_name = f"cms-webhook-{unique_name}"
    resp = sns.create_topic(Name=topic_name)
    topic_arn = resp["TopicArn"]
    yield topic_arn
    sns.delete_topic(TopicArn=topic_arn)


@pytest.fixture
def content_audit_log(logs, unique_name):
    group_name = f"/cms/audit/{unique_name}"
    stream_name = "content-changes"
    logs.create_log_group(logGroupName=group_name)
    logs.create_log_stream(logGroupName=group_name, logStreamName=stream_name)
    yield group_name, stream_name
    logs.delete_log_stream(logGroupName=group_name, logStreamName=stream_name)
    logs.delete_log_group(logGroupName=group_name)


@pytest.fixture
def event_bus():
    yield "default"


class TestMediaManagement:
    """S3 with versioning for media asset management."""

    def test_upload_media_asset(self, s3, media_bucket):
        """Upload a media asset and verify it can be retrieved."""
        content_id = str(uuid.uuid4())
        key = f"media/{content_id}/hero.jpg"
        body = b"image-v1"

        s3.put_object(Bucket=media_bucket, Key=key, Body=body)

        resp = s3.get_object(Bucket=media_bucket, Key=key)
        assert resp["Body"].read() == b"image-v1"

    def test_media_versioning(self, s3, media_bucket):
        """Upload same key 3 times, verify 3 versions exist and latest is correct."""
        key = "media/article-001/banner.png"
        contents = [b"banner-v1", b"banner-v2", b"banner-v3"]

        for body in contents:
            s3.put_object(Bucket=media_bucket, Key=key, Body=body)

        resp = s3.list_object_versions(Bucket=media_bucket, Prefix=key)
        versions = resp.get("Versions", [])
        assert len(versions) == 3

        latest = s3.get_object(Bucket=media_bucket, Key=key)
        assert latest["Body"].read() == b"banner-v3"

    def test_restore_previous_version(self, s3, media_bucket):
        """Upload v1 then v2, retrieve v1 by VersionId."""
        key = "media/article-002/photo.jpg"

        resp_v1 = s3.put_object(Bucket=media_bucket, Key=key, Body=b"original-photo")
        v1_version_id = resp_v1.get("VersionId")

        s3.put_object(Bucket=media_bucket, Key=key, Body=b"edited-photo")

        # If put_object didn't return VersionId, get it from list_object_versions
        if not v1_version_id:
            resp = s3.list_object_versions(Bucket=media_bucket, Prefix=key)
            versions = sorted(
                resp.get("Versions", []),
                key=lambda v: v["LastModified"],
            )
            v1_version_id = versions[0]["VersionId"]

        restored = s3.get_object(Bucket=media_bucket, Key=key, VersionId=v1_version_id)
        assert restored["Body"].read() == b"original-photo"

    def test_delete_and_recover_media(self, s3, media_bucket):
        """Delete creates a marker; removing the marker restores the object."""
        key = "media/article-003/thumbnail.png"
        s3.put_object(Bucket=media_bucket, Key=key, Body=b"thumbnail-data")

        # Delete object (creates delete marker)
        s3.delete_object(Bucket=media_bucket, Key=key)

        # Verify object is "gone" via normal get
        with pytest.raises(Exception):
            s3.get_object(Bucket=media_bucket, Key=key)

        # Find the delete marker
        resp = s3.list_object_versions(Bucket=media_bucket, Prefix=key)
        delete_markers = resp.get("DeleteMarkers", [])
        assert len(delete_markers) >= 1
        dm_version_id = delete_markers[0]["VersionId"]

        # Remove the delete marker
        s3.delete_object(Bucket=media_bucket, Key=key, VersionId=dm_version_id)

        # Object is restored
        restored = s3.get_object(Bucket=media_bucket, Key=key)
        assert restored["Body"].read() == b"thumbnail-data"


class TestContentMetadata:
    """DynamoDB for content metadata with GSIs."""

    def test_create_content(self, dynamodb, content_table):
        """Create a content item and verify all fields."""
        content_id = f"article-{uuid.uuid4().hex[:8]}"
        item = {
            "content_id": {"S": content_id},
            "title": {"S": "Introduction to AWS"},
            "category": {"S": "tech"},
            "status": {"S": "draft"},
            "author": {"S": "jane-doe"},
            "created_at": {"S": "2026-01-15T10:00:00Z"},
            "updated_at": {"S": "2026-01-15T10:00:00Z"},
            "body_s3_key": {"S": f"content/{content_id}/body.md"},
        }
        dynamodb.put_item(TableName=content_table, Item=item)

        resp = dynamodb.get_item(
            TableName=content_table,
            Key={"content_id": {"S": content_id}},
        )
        result = resp["Item"]
        assert result["title"]["S"] == "Introduction to AWS"
        assert result["category"]["S"] == "tech"
        assert result["status"]["S"] == "draft"
        assert result["author"]["S"] == "jane-doe"
        assert result["body_s3_key"]["S"] == f"content/{content_id}/body.md"

    def test_query_by_category(self, dynamodb, content_table):
        """Insert articles in multiple categories, query GSI for one category."""
        articles = [
            ("tech", "t1", "2026-01-01T01:00:00Z"),
            ("tech", "t2", "2026-01-01T02:00:00Z"),
            ("tech", "t3", "2026-01-01T03:00:00Z"),
            ("business", "b1", "2026-01-01T04:00:00Z"),
            ("business", "b2", "2026-01-01T05:00:00Z"),
            ("health", "h1", "2026-01-01T06:00:00Z"),
        ]
        for category, cid, ts in articles:
            dynamodb.put_item(
                TableName=content_table,
                Item={
                    "content_id": {"S": cid},
                    "category": {"S": category},
                    "status": {"S": "draft"},
                    "updated_at": {"S": ts},
                    "title": {"S": f"Article {cid}"},
                },
            )

        resp = dynamodb.query(
            TableName=content_table,
            IndexName="by-category",
            KeyConditionExpression="category = :cat",
            ExpressionAttributeValues={":cat": {"S": "tech"}},
        )
        assert resp["Count"] == 3

    def test_editorial_workflow(self, dynamodb, content_table):
        """Query GSI by status to find articles in review."""
        items = [
            ("draft-1", "draft", "2026-02-01T01:00:00Z"),
            ("draft-2", "draft", "2026-02-01T02:00:00Z"),
            ("review-1", "review", "2026-02-01T03:00:00Z"),
            ("review-2", "review", "2026-02-01T04:00:00Z"),
            ("pub-1", "published", "2026-02-01T05:00:00Z"),
        ]
        for cid, status, ts in items:
            dynamodb.put_item(
                TableName=content_table,
                Item={
                    "content_id": {"S": cid},
                    "category": {"S": "general"},
                    "status": {"S": status},
                    "updated_at": {"S": ts},
                    "title": {"S": f"Article {cid}"},
                },
            )

        resp = dynamodb.query(
            TableName=content_table,
            IndexName="by-status",
            KeyConditionExpression="#s = :status",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":status": {"S": "review"}},
        )
        assert resp["Count"] == 2
        returned_ids = {item["content_id"]["S"] for item in resp["Items"]}
        assert returned_ids == {"review-1", "review-2"}

    def test_update_content(self, dynamodb, content_table):
        """Create draft, update title/body/status, verify changes."""
        content_id = f"upd-{uuid.uuid4().hex[:8]}"
        dynamodb.put_item(
            TableName=content_table,
            Item={
                "content_id": {"S": content_id},
                "title": {"S": "Original Title"},
                "category": {"S": "tech"},
                "status": {"S": "draft"},
                "updated_at": {"S": "2026-03-01T10:00:00Z"},
                "body_s3_key": {"S": "content/original.md"},
            },
        )

        dynamodb.update_item(
            TableName=content_table,
            Key={"content_id": {"S": content_id}},
            UpdateExpression=("SET title = :t, body_s3_key = :b, updated_at = :u, #s = :st"),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":t": {"S": "Updated Title"},
                ":b": {"S": "content/revised.md"},
                ":u": {"S": "2026-03-01T12:00:00Z"},
                ":st": {"S": "review"},
            },
        )

        resp = dynamodb.get_item(
            TableName=content_table,
            Key={"content_id": {"S": content_id}},
        )
        item = resp["Item"]
        assert item["title"]["S"] == "Updated Title"
        assert item["body_s3_key"]["S"] == "content/revised.md"
        assert item["status"]["S"] == "review"
        assert item["updated_at"]["S"] == "2026-03-01T12:00:00Z"

    def test_content_tags(self, dynamodb, content_table):
        """Store tags as StringSet and verify retrieval."""
        content_id = f"tag-{uuid.uuid4().hex[:8]}"
        dynamodb.put_item(
            TableName=content_table,
            Item={
                "content_id": {"S": content_id},
                "title": {"S": "Tagged Article"},
                "category": {"S": "tech"},
                "status": {"S": "draft"},
                "updated_at": {"S": "2026-03-01T10:00:00Z"},
                "tags": {"SS": ["python", "aws", "serverless"]},
            },
        )

        resp = dynamodb.get_item(
            TableName=content_table,
            Key={"content_id": {"S": content_id}},
        )
        tags = set(resp["Item"]["tags"]["SS"])
        assert "python" in tags
        assert "aws" in tags
        assert "serverless" in tags


class TestPublishWorkflow:
    """SQS + SNS for the publication pipeline."""

    def test_queue_for_publication(self, sqs, publish_queue):
        """Send a publish request to SQS and verify the message body."""
        message = json.dumps(
            {
                "content_id": "article-100",
                "scheduled_time": "2026-03-08T18:00:00Z",
                "author": "editor-bob",
            }
        )
        sqs.send_message(QueueUrl=publish_queue, MessageBody=message)

        resp = sqs.receive_message(
            QueueUrl=publish_queue,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        messages = resp.get("Messages", [])
        assert len(messages) == 1
        body = json.loads(messages[0]["Body"])
        assert body["content_id"] == "article-100"
        assert body["author"] == "editor-bob"

    def test_publish_notification(self, sns, sqs, webhook_topic, unique_name):
        """Subscribe SQS to SNS topic, publish, verify delivery."""
        # Create a subscriber queue
        sub_queue_name = f"cms-sub-{unique_name}"
        q_resp = sqs.create_queue(QueueName=sub_queue_name)
        sub_queue_url = q_resp["QueueUrl"]
        queue_arn = f"arn:aws:sqs:us-east-1:123456789012:{sub_queue_name}"

        try:
            sub_resp = sns.subscribe(
                TopicArn=webhook_topic,
                Protocol="sqs",
                Endpoint=queue_arn,
            )
            assert "SubscriptionArn" in sub_resp

            sns.publish(
                TopicArn=webhook_topic,
                Message=json.dumps(
                    {
                        "content_id": "article-200",
                        "title": "Breaking News",
                        "url": "https://cms.example.com/articles/200",
                    }
                ),
                Subject="Content Published",
            )

            # Poll for the message
            received = None
            for _ in range(5):
                resp = sqs.receive_message(
                    QueueUrl=sub_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=2,
                )
                if resp.get("Messages"):
                    received = resp["Messages"][0]
                    break

            assert received is not None
            # SNS wraps the message in an envelope
            envelope = json.loads(received["Body"])
            inner = json.loads(envelope.get("Message", received["Body"]))
            assert inner["content_id"] == "article-200"
            assert inner["title"] == "Breaking News"
        finally:
            sqs.delete_queue(QueueUrl=sub_queue_url)

    def test_batch_publish_queue(self, sqs, publish_queue):
        """Send a batch of 5 articles to the publish queue."""
        entries = [
            {
                "Id": f"msg-{i}",
                "MessageBody": json.dumps(
                    {
                        "content_id": f"batch-article-{i}",
                        "action": "publish",
                    }
                ),
            }
            for i in range(5)
        ]
        resp = sqs.send_message_batch(QueueUrl=publish_queue, Entries=entries)
        assert len(resp.get("Successful", [])) == 5

        # Receive all messages (may need multiple calls)
        all_messages = []
        for _ in range(5):
            recv = sqs.receive_message(
                QueueUrl=publish_queue,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2,
            )
            all_messages.extend(recv.get("Messages", []))
            if len(all_messages) >= 5:
                break

        assert len(all_messages) == 5
        received_ids = {json.loads(m["Body"])["content_id"] for m in all_messages}
        expected_ids = {f"batch-article-{i}" for i in range(5)}
        assert received_ids == expected_ids

    def test_publish_with_attributes(self, sqs, publish_queue):
        """Send message with MessageAttributes, verify they are returned."""
        sqs.send_message(
            QueueUrl=publish_queue,
            MessageBody=json.dumps({"content_id": "featured-1"}),
            MessageAttributes={
                "content_type": {
                    "DataType": "String",
                    "StringValue": "article",
                },
                "priority": {
                    "DataType": "String",
                    "StringValue": "featured",
                },
            },
        )

        resp = sqs.receive_message(
            QueueUrl=publish_queue,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"],
        )
        messages = resp.get("Messages", [])
        assert len(messages) == 1
        attrs = messages[0].get("MessageAttributes", {})
        assert attrs["content_type"]["StringValue"] == "article"
        assert attrs["priority"]["StringValue"] == "featured"


class TestContentAudit:
    """CloudWatch Logs for content audit trail."""

    def test_log_content_changes(self, logs, content_audit_log):
        """Put 5 audit events and verify they are all returned in order."""
        group_name, stream_name = content_audit_log
        base_ts = int(time.time() * 1000)

        events = [
            {"action": "created", "content_id": "art-1"},
            {"action": "edited", "content_id": "art-1"},
            {"action": "reviewed", "content_id": "art-1"},
            {"action": "approved", "content_id": "art-1"},
            {"action": "published", "content_id": "art-1"},
        ]
        log_events = [
            {
                "timestamp": base_ts + i * 1000,
                "message": json.dumps(evt),
            }
            for i, evt in enumerate(events)
        ]

        logs.put_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            logEvents=log_events,
        )

        resp = logs.get_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            startFromHead=True,
        )
        returned = resp.get("events", [])
        assert len(returned) >= 5
        actions = [json.loads(e["message"])["action"] for e in returned[:5]]
        assert actions == ["created", "edited", "reviewed", "approved", "published"]

    def test_filter_audit_by_action(self, logs, content_audit_log):
        """Put mixed events, filter for 'published' only."""
        group_name, stream_name = content_audit_log
        base_ts = int(time.time() * 1000)

        mixed_events = [
            {"action": "created", "content_id": "art-10"},
            {"action": "published", "content_id": "art-10"},
            {"action": "created", "content_id": "art-11"},
            {"action": "edited", "content_id": "art-11"},
            {"action": "published", "content_id": "art-11"},
        ]
        log_events = [
            {
                "timestamp": base_ts + i * 1000,
                "message": json.dumps(evt),
            }
            for i, evt in enumerate(mixed_events)
        ]

        logs.put_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            logEvents=log_events,
        )

        resp = logs.filter_log_events(
            logGroupName=group_name,
            filterPattern="published",
        )
        matched = resp.get("events", [])
        assert len(matched) >= 2
        for evt in matched:
            assert "published" in evt["message"]


class TestContentLifecycle:
    """EventBridge rules and end-to-end content lifecycle."""

    def test_create_eventbridge_rule(self, events, event_bus):
        """Create a scheduled rule and verify it is ENABLED."""
        rule_name = f"cms-schedule-{uuid.uuid4().hex[:8]}"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(1 hour)",
            State="ENABLED",
            EventBusName=event_bus,
        )

        try:
            resp = events.describe_rule(Name=rule_name, EventBusName=event_bus)
            assert resp["State"] == "ENABLED"
            assert resp["ScheduleExpression"] == "rate(1 hour)"
        finally:
            events.delete_rule(Name=rule_name, EventBusName=event_bus)

    def test_eventbridge_with_target(self, events, sqs, event_bus, unique_name):
        """Create a rule with an SQS target, verify target is listed."""
        rule_name = f"cms-rule-{unique_name}"
        queue_name = f"cms-eb-target-{unique_name}"

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        queue_arn = f"arn:aws:sqs:us-east-1:123456789012:{queue_name}"

        try:
            events.put_rule(
                Name=rule_name,
                EventPattern=json.dumps(
                    {
                        "source": ["cms.content"],
                        "detail-type": ["ContentPublished"],
                    }
                ),
                State="ENABLED",
                EventBusName=event_bus,
            )

            events.put_targets(
                Rule=rule_name,
                EventBusName=event_bus,
                Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
            )

            resp = events.list_targets_by_rule(Rule=rule_name, EventBusName=event_bus)
            targets = resp.get("Targets", [])
            assert len(targets) == 1
            assert targets[0]["Arn"] == queue_arn
            assert targets[0]["Id"] == "sqs-target"
        finally:
            events.remove_targets(Rule=rule_name, Ids=["sqs-target"], EventBusName=event_bus)
            events.delete_rule(Name=rule_name, EventBusName=event_bus)
            sqs.delete_queue(QueueUrl=queue_url)

    def test_full_content_lifecycle(
        self,
        s3,
        dynamodb,
        sqs,
        sns,
        logs,
        media_bucket,
        content_table,
        publish_queue,
        webhook_topic,
        content_audit_log,
        unique_name,
    ):
        """End-to-end: draft -> media upload -> review -> queue -> publish -> notify -> audit."""
        content_id = f"lifecycle-{uuid.uuid4().hex[:8]}"
        group_name, stream_name = content_audit_log
        base_ts = int(time.time() * 1000)

        # 1. Create content metadata as draft
        dynamodb.put_item(
            TableName=content_table,
            Item={
                "content_id": {"S": content_id},
                "title": {"S": "Lifecycle Test Article"},
                "category": {"S": "tech"},
                "status": {"S": "draft"},
                "author": {"S": "alice"},
                "created_at": {"S": "2026-03-08T10:00:00Z"},
                "updated_at": {"S": "2026-03-08T10:00:00Z"},
                "body_s3_key": {"S": f"content/{content_id}/body.md"},
            },
        )

        # 2. Upload media to S3
        media_key = f"media/{content_id}/hero.jpg"
        s3.put_object(Bucket=media_bucket, Key=media_key, Body=b"hero-image-data")

        # 3. Update status to review
        dynamodb.update_item(
            TableName=content_table,
            Key={"content_id": {"S": content_id}},
            UpdateExpression="SET #s = :st, updated_at = :u",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":st": {"S": "review"},
                ":u": {"S": "2026-03-08T11:00:00Z"},
            },
        )

        # 4. Queue for publication
        sqs.send_message(
            QueueUrl=publish_queue,
            MessageBody=json.dumps(
                {
                    "content_id": content_id,
                    "action": "publish",
                }
            ),
        )

        # 5. Receive from queue (simulating publish worker)
        recv = sqs.receive_message(
            QueueUrl=publish_queue,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )
        assert len(recv.get("Messages", [])) >= 1
        pub_msg = json.loads(recv["Messages"][0]["Body"])
        assert pub_msg["content_id"] == content_id

        # 6. Update status to published
        dynamodb.update_item(
            TableName=content_table,
            Key={"content_id": {"S": content_id}},
            UpdateExpression="SET #s = :st, updated_at = :u",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":st": {"S": "published"},
                ":u": {"S": "2026-03-08T12:00:00Z"},
            },
        )

        # 7. Publish webhook via SNS
        # Create subscriber queue for verification
        sub_queue_name = f"cms-lifecycle-sub-{unique_name}"
        sq = sqs.create_queue(QueueName=sub_queue_name)
        sub_queue_url = sq["QueueUrl"]
        sub_queue_arn = f"arn:aws:sqs:us-east-1:123456789012:{sub_queue_name}"

        try:
            sns.subscribe(
                TopicArn=webhook_topic,
                Protocol="sqs",
                Endpoint=sub_queue_arn,
            )

            sns.publish(
                TopicArn=webhook_topic,
                Message=json.dumps(
                    {
                        "content_id": content_id,
                        "title": "Lifecycle Test Article",
                        "url": f"https://cms.example.com/articles/{content_id}",
                    }
                ),
                Subject="Content Published",
            )

            # 8. Receive webhook notification
            webhook_msg = None
            for _ in range(5):
                wr = sqs.receive_message(
                    QueueUrl=sub_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=2,
                )
                if wr.get("Messages"):
                    webhook_msg = wr["Messages"][0]
                    break
            assert webhook_msg is not None
        finally:
            sqs.delete_queue(QueueUrl=sub_queue_url)

        # 9. Log audit events
        audit_events = [
            {"action": "created", "content_id": content_id},
            {"action": "media_uploaded", "content_id": content_id},
            {"action": "status_changed", "content_id": content_id, "to": "review"},
            {"action": "published", "content_id": content_id},
        ]
        logs.put_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            logEvents=[
                {
                    "timestamp": base_ts + i * 1000,
                    "message": json.dumps(evt),
                }
                for i, evt in enumerate(audit_events)
            ],
        )

        # Verify final state
        # S3: media exists
        media_resp = s3.get_object(Bucket=media_bucket, Key=media_key)
        assert media_resp["Body"].read() == b"hero-image-data"

        # DynamoDB: status is published
        db_resp = dynamodb.get_item(
            TableName=content_table,
            Key={"content_id": {"S": content_id}},
        )
        assert db_resp["Item"]["status"]["S"] == "published"

        # CloudWatch Logs: audit trail
        log_resp = logs.get_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            startFromHead=True,
        )
        log_actions = [json.loads(e["message"])["action"] for e in log_resp.get("events", [])]
        assert "created" in log_actions
        assert "published" in log_actions
