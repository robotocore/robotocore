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
