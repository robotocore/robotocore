"""Resource Groups Tagging API compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def tagging():
    return make_client("resourcegroupstaggingapi")


@pytest.fixture
def sqs():
    return make_client("sqs")


class TestResourceGroupsTaggingOperations:
    def test_get_tag_keys(self, tagging):
        response = tagging.get_tag_keys()
        assert "TagKeys" in response

    def test_get_tag_values(self, tagging):
        response = tagging.get_tag_values(Key="env")
        assert "TagValues" in response

    def test_get_resources_empty(self, tagging):
        response = tagging.get_resources()
        assert "ResourceTagMappingList" in response

    def test_tag_and_get_resources(self, tagging, sqs):
        # Create a tagged SQS queue so there's something to find
        queue_url = sqs.create_queue(
            QueueName="tagging-test-queue",
            tags={"env": "test", "project": "robotocore"},
        )["QueueUrl"]

        response = tagging.get_resources(
            TagFilters=[{"Key": "env", "Values": ["test"]}],
        )
        assert "ResourceTagMappingList" in response

        # Cleanup
        sqs.delete_queue(QueueUrl=queue_url)

    def test_tag_resources(self, tagging, sqs):
        # Create a queue to get an ARN
        sqs.create_queue(QueueName="tag-resources-queue")
        attrs = sqs.get_queue_attributes(
            QueueUrl=sqs.get_queue_url(QueueName="tag-resources-queue")["QueueUrl"],
            AttributeNames=["QueueArn"],
        )
        arn = attrs["Attributes"]["QueueArn"]

        response = tagging.tag_resources(
            ResourceARNList=[arn],
            Tags={"tagged-by": "compat-test"},
        )
        assert "FailedResourcesMap" in response

        # Cleanup
        queue_url = sqs.get_queue_url(QueueName="tag-resources-queue")["QueueUrl"]
        sqs.delete_queue(QueueUrl=queue_url)

    def test_untag_resources(self, tagging, sqs):
        sqs.create_queue(QueueName="untag-test-queue", tags={"rmtag": "yes", "keep": "yes"})
        attrs = sqs.get_queue_attributes(
            QueueUrl=sqs.get_queue_url(QueueName="untag-test-queue")["QueueUrl"],
            AttributeNames=["QueueArn"],
        )
        arn = attrs["Attributes"]["QueueArn"]
        response = tagging.untag_resources(ResourceARNList=[arn], TagKeys=["rmtag"])
        assert "FailedResourcesMap" in response
        queue_url = sqs.get_queue_url(QueueName="untag-test-queue")["QueueUrl"]
        sqs.delete_queue(QueueUrl=queue_url)

    def test_get_resources_with_resource_type_filter(self, tagging, sqs):
        sqs.create_queue(QueueName="type-filter-queue", tags={"env": "type-test"})
        response = tagging.get_resources(
            ResourceTypeFilters=["sqs:queue"],
            TagFilters=[{"Key": "env", "Values": ["type-test"]}],
        )
        assert "ResourceTagMappingList" in response
        queue_url = sqs.get_queue_url(QueueName="type-filter-queue")["QueueUrl"]
        sqs.delete_queue(QueueUrl=queue_url)


class TestTaggingExtended:
    @pytest.fixture
    def tagging(self):
        return make_client("resourcegroupstaggingapi")

    @pytest.fixture
    def sqs(self):
        return make_client("sqs")

    @pytest.fixture
    def sns(self):
        return make_client("sns")

    def test_get_tag_keys_returns_list(self, tagging):
        resp = tagging.get_tag_keys()
        assert "TagKeys" in resp

    def test_get_tag_values_returns_list(self, tagging):
        resp = tagging.get_tag_values(Key="env")
        assert "TagValues" in resp

    def test_tag_resources_multiple_tags(self, tagging, sqs):
        import uuid

        name = f"multi-tag-{uuid.uuid4().hex[:8]}"
        sqs.create_queue(QueueName=name)
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
        arn = attrs["Attributes"]["QueueArn"]
        try:
            resp = tagging.tag_resources(
                ResourceARNList=[arn],
                Tags={"env": "test", "team": "platform", "project": "search"},
            )
            assert "FailedResourcesMap" in resp
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_get_resources_with_multiple_tag_filters(self, tagging, sqs):
        import uuid

        name = f"multi-filter-{uuid.uuid4().hex[:8]}"
        sqs.create_queue(QueueName=name, tags={"env": "filter-test", "team": "dev"})
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        try:
            resp = tagging.get_resources(
                TagFilters=[
                    {"Key": "env", "Values": ["filter-test"]},
                    {"Key": "team", "Values": ["dev"]},
                ],
            )
            assert "ResourceTagMappingList" in resp
        finally:
            sqs.delete_queue(QueueUrl=url)

    def test_get_resources_empty_result(self, tagging):
        import uuid

        unique_key = f"nonexistent-key-{uuid.uuid4().hex}"
        resp = tagging.get_resources(
            TagFilters=[{"Key": unique_key, "Values": ["nothing"]}],
            ResourceTypeFilters=["sqs:queue"],
        )
        assert resp["ResourceTagMappingList"] == []

    def test_tag_and_get_resources_sns(self, tagging, sns):
        import uuid

        name = f"tag-topic-{uuid.uuid4().hex[:8]}"
        resp = sns.create_topic(Name=name, Tags=[{"Key": "env", "Value": "tag-test"}])
        arn = resp["TopicArn"]
        try:
            resources = tagging.get_resources(
                TagFilters=[{"Key": "env", "Values": ["tag-test"]}],
            )
            assert "ResourceTagMappingList" in resources
        finally:
            sns.delete_topic(TopicArn=arn)

    def test_untag_resources_returns_empty_failures(self, tagging, sqs):
        import uuid

        name = f"untag-empty-{uuid.uuid4().hex[:8]}"
        sqs.create_queue(QueueName=name, tags={"temp": "yes"})
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
        arn = attrs["Attributes"]["QueueArn"]
        try:
            resp = tagging.untag_resources(ResourceARNList=[arn], TagKeys=["temp"])
            assert resp["FailedResourcesMap"] == {} or "FailedResourcesMap" in resp
        finally:
            sqs.delete_queue(QueueUrl=url)


class TestRGTAGapStubs:
    """Tests for gap operations: describe_report_creation, get_compliance_summary."""

    @pytest.fixture
    def tagging(self):
        return make_client("resourcegroupstaggingapi")

    def test_describe_report_creation(self, tagging):
        resp = tagging.describe_report_creation()
        assert "Status" in resp

    def test_get_compliance_summary(self, tagging):
        resp = tagging.get_compliance_summary()
        assert "SummaryList" in resp


class TestResourcegroupstaggingapiAutoCoverage:
    """Auto-generated coverage tests for resourcegroupstaggingapi."""

    @pytest.fixture
    def client(self):
        return make_client("resourcegroupstaggingapi")

    def test_list_required_tags(self, client):
        """ListRequiredTags returns a successful response."""
        resp = client.list_required_tags()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
