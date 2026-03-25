"""Bedrock Agent compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def bedrock_agent():
    return make_client("bedrock-agent")


class TestBedrockAgentOperations:
    def test_list_agents(self, bedrock_agent):
        response = bedrock_agent.list_agents()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "agentSummaries" in response
        assert isinstance(response["agentSummaries"], list)

    def test_list_knowledge_bases(self, bedrock_agent):
        response = bedrock_agent.list_knowledge_bases()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "knowledgeBaseSummaries" in response
        assert isinstance(response["knowledgeBaseSummaries"], list)

    def test_create_agent(self, bedrock_agent):
        """CreateAgent returns an agent object with agentId."""
        response = bedrock_agent.create_agent(
            agentName="compat-test-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "agent" in response
        assert "agentId" in response["agent"]
        assert response["agent"]["agentName"] == "compat-test-agent"
        # Cleanup
        bedrock_agent.delete_agent(agentId=response["agent"]["agentId"])

    def test_get_agent(self, bedrock_agent):
        """GetAgent raises ResourceNotFoundException for a nonexistent agent."""
        with pytest.raises(ClientError) as exc:
            bedrock_agent.get_agent(agentId="nonexistent-agent-id-99999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_agent_real(self, bedrock_agent):
        """GetAgent returns the agent for a valid agentId."""
        create_resp = bedrock_agent.create_agent(
            agentName="compat-get-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = create_resp["agent"]["agentId"]
        try:
            get_resp = bedrock_agent.get_agent(agentId=agent_id)
            assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agent" in get_resp
            assert get_resp["agent"]["agentId"] == agent_id
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_delete_agent(self, bedrock_agent):
        """DeleteAgent returns agentId and agentStatus after deletion."""
        create_resp = bedrock_agent.create_agent(
            agentName="compat-delete-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = create_resp["agent"]["agentId"]
        del_resp = bedrock_agent.delete_agent(agentId=agent_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert del_resp["agentId"] == agent_id
        assert "agentStatus" in del_resp

    def test_delete_agent_nonexistent(self, bedrock_agent):
        """DeleteAgent raises ResourceNotFoundException for a nonexistent agent."""
        with pytest.raises(ClientError) as exc:
            bedrock_agent.delete_agent(agentId="nonexistent-agent-id-99999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_knowledge_base(self, bedrock_agent):
        """CreateKnowledgeBase returns a knowledgeBase object with knowledgeBaseId."""
        response = bedrock_agent.create_knowledge_base(
            name="compat-test-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration={
                "type": "VECTOR",
                "vectorKnowledgeBaseConfiguration": {
                    "embeddingModelArn": (
                        "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
                    )
                },
            },
            storageConfiguration={
                "type": "OPENSEARCH_SERVERLESS",
                "opensearchServerlessConfiguration": {
                    "collectionArn": "arn:aws:aoss:us-east-1:123456789012:collection/test",
                    "vectorIndexName": "test-index",
                    "fieldMapping": {
                        "vectorField": "embedding",
                        "textField": "text",
                        "metadataField": "metadata",
                    },
                },
            },
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "knowledgeBase" in response
        assert "knowledgeBaseId" in response["knowledgeBase"]
        assert "knowledgeBaseArn" in response["knowledgeBase"]
        # Cleanup
        bedrock_agent.delete_knowledge_base(
            knowledgeBaseId=response["knowledgeBase"]["knowledgeBaseId"]
        )

    def test_get_knowledge_base_nonexistent(self, bedrock_agent):
        """GetKnowledgeBase raises ResourceNotFoundException for a nonexistent KB."""
        with pytest.raises(ClientError) as exc:
            bedrock_agent.get_knowledge_base(knowledgeBaseId="nonexistent-kb-99999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_knowledge_base_real(self, bedrock_agent):
        """GetKnowledgeBase returns the knowledge base for a valid knowledgeBaseId."""
        create_resp = bedrock_agent.create_knowledge_base(
            name="compat-get-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration={
                "type": "VECTOR",
                "vectorKnowledgeBaseConfiguration": {
                    "embeddingModelArn": (
                        "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
                    )
                },
            },
            storageConfiguration={
                "type": "OPENSEARCH_SERVERLESS",
                "opensearchServerlessConfiguration": {
                    "collectionArn": "arn:aws:aoss:us-east-1:123456789012:collection/test",
                    "vectorIndexName": "test-index",
                    "fieldMapping": {
                        "vectorField": "embedding",
                        "textField": "text",
                        "metadataField": "metadata",
                    },
                },
            },
        )
        kb_id = create_resp["knowledgeBase"]["knowledgeBaseId"]
        try:
            get_resp = bedrock_agent.get_knowledge_base(knowledgeBaseId=kb_id)
            assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "knowledgeBase" in get_resp
            assert get_resp["knowledgeBase"]["knowledgeBaseId"] == kb_id
        finally:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)

    def test_delete_knowledge_base_nonexistent(self, bedrock_agent):
        """DeleteKnowledgeBase raises ResourceNotFoundException for a nonexistent KB."""
        with pytest.raises(ClientError) as exc:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId="nonexistent-kb-99999")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_tag_resource_and_list_tags(self, bedrock_agent):
        """TagResource and ListTagsForResource work together on an agent."""
        create_resp = bedrock_agent.create_agent(
            agentName="compat-tag-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = create_resp["agent"]["agentId"]
        agent_arn = create_resp["agent"]["agentArn"]
        try:
            tag_resp = bedrock_agent.tag_resource(
                resourceArn=agent_arn,
                tags={"env": "test", "team": "platform"},
            )
            assert tag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            list_resp = bedrock_agent.list_tags_for_resource(resourceArn=agent_arn)
            assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "tags" in list_resp
            assert list_resp["tags"]["env"] == "test"
            assert list_resp["tags"]["team"] == "platform"
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_untag_resource(self, bedrock_agent):
        """UntagResource removes specific tags from an agent."""
        create_resp = bedrock_agent.create_agent(
            agentName="compat-untag-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = create_resp["agent"]["agentId"]
        agent_arn = create_resp["agent"]["agentArn"]
        try:
            bedrock_agent.tag_resource(
                resourceArn=agent_arn,
                tags={"env": "test", "team": "platform"},
            )
            untag_resp = bedrock_agent.untag_resource(
                resourceArn=agent_arn,
                tagKeys=["env"],
            )
            assert untag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            list_resp = bedrock_agent.list_tags_for_resource(resourceArn=agent_arn)
            assert "env" not in list_resp["tags"]
            assert list_resp["tags"]["team"] == "platform"
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)
