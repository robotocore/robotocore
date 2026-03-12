"""Tests for build notifications via SNS -> SQS."""

from .app import CICDPipeline


class TestNotifications:
    """SNS/SQS-based build notification system."""

    def test_build_success_notification(self, pipeline, unique_name):
        topic_arn = pipeline.create_notification_topic(f"notify-{unique_name}")
        queue_resp = pipeline.sqs.create_queue(QueueName=f"sub-{unique_name}")
        queue_url = queue_resp["QueueUrl"]
        try:
            pipeline.subscribe_queue(topic_arn, queue_url)

            build = pipeline.queue_build(repo="org/notif-app", branch="main", commit_sha="nnn111")
            pipeline.transition_build(build.build_id, CICDPipeline.SUCCESS)
            updated = pipeline.get_build(build.build_id)

            pipeline.notify_build_event(topic_arn, updated, event_type="build_succeeded")

            messages = pipeline.receive_notifications(queue_url, wait_seconds=10)
            assert len(messages) >= 1
            msg = messages[0]
            assert msg["build_id"] == build.build_id
            assert msg["status"] == CICDPipeline.SUCCESS
            assert msg["repo"] == "org/notif-app"
            assert msg["event_type"] == "build_succeeded"
        finally:
            pipeline.sqs.delete_queue(QueueUrl=queue_url)
            pipeline.sns.delete_topic(TopicArn=topic_arn)

    def test_build_failure_notification_with_details(self, pipeline, unique_name):
        topic_arn = pipeline.create_notification_topic(f"fail-notify-{unique_name}")
        queue_resp = pipeline.sqs.create_queue(QueueName=f"fail-sub-{unique_name}")
        queue_url = queue_resp["QueueUrl"]
        try:
            pipeline.subscribe_queue(topic_arn, queue_url)

            build = pipeline.queue_build(repo="org/fail-app", branch="main", commit_sha="fff111")
            pipeline.transition_build(build.build_id, CICDPipeline.BUILDING)
            failed = pipeline.fail_build(build.build_id, "Segfault in tests")

            pipeline.notify_build_event(topic_arn, failed, event_type="build_failed")

            messages = pipeline.receive_notifications(queue_url, wait_seconds=10)
            assert len(messages) >= 1
            msg = messages[0]
            assert msg["status"] == CICDPipeline.FAILED
            assert msg["build_id"] == build.build_id
            assert "FAILED" in msg["message"]
        finally:
            pipeline.sqs.delete_queue(QueueUrl=queue_url)
            pipeline.sns.delete_topic(TopicArn=topic_arn)

    def test_notification_includes_build_metadata(self, pipeline, unique_name):
        topic_arn = pipeline.create_notification_topic(f"meta-notify-{unique_name}")
        queue_resp = pipeline.sqs.create_queue(QueueName=f"meta-sub-{unique_name}")
        queue_url = queue_resp["QueueUrl"]
        try:
            pipeline.subscribe_queue(topic_arn, queue_url)

            build = pipeline.queue_build(
                repo="org/meta-notif-app", branch="develop", commit_sha="mmm111"
            )
            pipeline.notify_build_event(topic_arn, build, event_type="build_queued")

            messages = pipeline.receive_notifications(queue_url, wait_seconds=10)
            assert len(messages) >= 1
            msg = messages[0]
            assert msg["repo"] == "org/meta-notif-app"
            assert msg["timestamp"] is not None
            assert "build_id" in msg
            assert "event_type" in msg
            assert msg["event_type"] == "build_queued"
        finally:
            pipeline.sqs.delete_queue(QueueUrl=queue_url)
            pipeline.sns.delete_topic(TopicArn=topic_arn)
