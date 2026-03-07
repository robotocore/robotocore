"""Bedrock Agent compatibility tests."""

import pytest

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
