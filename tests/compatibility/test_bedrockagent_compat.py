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

    def test_update_agent(self, bedrock_agent):
        """UpdateAgent returns the updated agent."""
        create_resp = bedrock_agent.create_agent(
            agentName="compat-update-agent",
            agentResourceRoleArn="arn:aws:iam::123456789012:role/test-agent-role",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = create_resp["agent"]["agentId"]
        try:
            update_resp = bedrock_agent.update_agent(
                agentId=agent_id,
                agentName="compat-updated-agent",
                agentResourceRoleArn="arn:aws:iam::123456789012:role/test-agent-role",
                foundationModel="amazon.titan-text-lite-v1",
            )
            assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agent" in update_resp
            assert update_resp["agent"]["agentName"] == "compat-updated-agent"
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_prepare_agent(self, bedrock_agent):
        """PrepareAgent returns agentId and agentStatus."""
        create_resp = bedrock_agent.create_agent(
            agentName="compat-prepare-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = create_resp["agent"]["agentId"]
        try:
            prepare_resp = bedrock_agent.prepare_agent(agentId=agent_id)
            assert prepare_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert prepare_resp["agentId"] == agent_id
            assert prepare_resp["agentStatus"] == "PREPARED"
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_list_agent_versions(self, bedrock_agent):
        """ListAgentVersions returns version summaries for an agent."""
        create_resp = bedrock_agent.create_agent(
            agentName="compat-versions-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = create_resp["agent"]["agentId"]
        try:
            versions_resp = bedrock_agent.list_agent_versions(agentId=agent_id)
            assert versions_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agentVersionSummaries" in versions_resp
            assert isinstance(versions_resp["agentVersionSummaries"], list)
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_agent_alias_crud(self, bedrock_agent):
        """CreateAgentAlias, GetAgentAlias, ListAgentAliases, UpdateAgentAlias, DeleteAgentAlias."""
        create_resp = bedrock_agent.create_agent(
            agentName="compat-alias-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = create_resp["agent"]["agentId"]
        try:
            alias_resp = bedrock_agent.create_agent_alias(
                agentId=agent_id,
                agentAliasName="compat-alias",
            )
            assert alias_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agentAlias" in alias_resp
            alias_id = alias_resp["agentAlias"]["agentAliasId"]

            get_resp = bedrock_agent.get_agent_alias(agentId=agent_id, agentAliasId=alias_id)
            assert get_resp["agentAlias"]["agentAliasId"] == alias_id

            list_resp = bedrock_agent.list_agent_aliases(agentId=agent_id)
            assert "agentAliasSummaries" in list_resp
            assert any(a["agentAliasId"] == alias_id for a in list_resp["agentAliasSummaries"])

            update_resp = bedrock_agent.update_agent_alias(
                agentId=agent_id,
                agentAliasId=alias_id,
                agentAliasName="compat-alias-updated",
            )
            assert update_resp["agentAlias"]["agentAliasName"] == "compat-alias-updated"

            del_resp = bedrock_agent.delete_agent_alias(agentId=agent_id, agentAliasId=alias_id)
            assert del_resp["agentAliasId"] == alias_id
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_agent_action_group_crud(self, bedrock_agent):
        """CreateAgentActionGroup, GetAgentActionGroup, ListAgentActionGroups, Delete."""
        create_resp = bedrock_agent.create_agent(
            agentName="compat-ag-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = create_resp["agent"]["agentId"]
        agent_version = create_resp["agent"]["agentVersion"]
        try:
            ag_resp = bedrock_agent.create_agent_action_group(
                agentId=agent_id,
                agentVersion=agent_version,
                actionGroupName="compat-action-group",
                actionGroupState="ENABLED",
            )
            assert ag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            ag_id = ag_resp["agentActionGroup"]["actionGroupId"]

            get_resp = bedrock_agent.get_agent_action_group(
                agentId=agent_id, agentVersion=agent_version, actionGroupId=ag_id
            )
            assert get_resp["agentActionGroup"]["actionGroupId"] == ag_id

            list_resp = bedrock_agent.list_agent_action_groups(
                agentId=agent_id, agentVersion=agent_version
            )
            assert "actionGroupSummaries" in list_resp

            bedrock_agent.delete_agent_action_group(
                agentId=agent_id, agentVersion=agent_version, actionGroupId=ag_id
            )
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_update_knowledge_base(self, bedrock_agent):
        """UpdateKnowledgeBase returns the updated knowledge base."""
        kb_config = {
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"  # noqa: E501
            },
        }
        storage_config = {
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
        }
        create_resp = bedrock_agent.create_knowledge_base(
            name="compat-update-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration=kb_config,
            storageConfiguration=storage_config,
        )
        kb_id = create_resp["knowledgeBase"]["knowledgeBaseId"]
        try:
            update_resp = bedrock_agent.update_knowledge_base(
                knowledgeBaseId=kb_id,
                name="compat-updated-kb",
                roleArn="arn:aws:iam::123456789012:role/test-kb-role",
                knowledgeBaseConfiguration=kb_config,
                storageConfiguration=storage_config,
            )
            assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert update_resp["knowledgeBase"]["name"] == "compat-updated-kb"
        finally:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)

    def test_data_source_crud(self, bedrock_agent):
        """CreateDataSource, GetDataSource, ListDataSources, UpdateDataSource, DeleteDataSource."""
        kb_config = {
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"  # noqa: E501
            },
        }
        storage_config = {
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
        }
        create_kb = bedrock_agent.create_knowledge_base(
            name="compat-ds-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration=kb_config,
            storageConfiguration=storage_config,
        )
        kb_id = create_kb["knowledgeBase"]["knowledgeBaseId"]
        try:
            ds_resp = bedrock_agent.create_data_source(
                knowledgeBaseId=kb_id,
                name="compat-data-source",
                dataSourceConfiguration={
                    "type": "S3",
                    "s3Configuration": {
                        "bucketArn": "arn:aws:s3:::my-test-bucket",
                    },
                },
            )
            assert ds_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            ds_id = ds_resp["dataSource"]["dataSourceId"]

            get_resp = bedrock_agent.get_data_source(knowledgeBaseId=kb_id, dataSourceId=ds_id)
            assert get_resp["dataSource"]["dataSourceId"] == ds_id

            list_resp = bedrock_agent.list_data_sources(knowledgeBaseId=kb_id)
            assert "dataSourceSummaries" in list_resp

            update_resp = bedrock_agent.update_data_source(
                knowledgeBaseId=kb_id,
                dataSourceId=ds_id,
                name="compat-updated-ds",
                dataSourceConfiguration={
                    "type": "S3",
                    "s3Configuration": {"bucketArn": "arn:aws:s3:::my-test-bucket"},
                },
            )
            assert update_resp["dataSource"]["name"] == "compat-updated-ds"

            del_resp = bedrock_agent.delete_data_source(knowledgeBaseId=kb_id, dataSourceId=ds_id)
            assert del_resp["dataSourceId"] == ds_id
        finally:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)

    def test_ingestion_job_crud(self, bedrock_agent):
        """StartIngestionJob, GetIngestionJob, ListIngestionJobs."""
        kb_config = {
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"  # noqa: E501
            },
        }
        storage_config = {
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
        }
        create_kb = bedrock_agent.create_knowledge_base(
            name="compat-ij-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration=kb_config,
            storageConfiguration=storage_config,
        )
        kb_id = create_kb["knowledgeBase"]["knowledgeBaseId"]
        try:
            ds_resp = bedrock_agent.create_data_source(
                knowledgeBaseId=kb_id,
                name="compat-ij-ds",
                dataSourceConfiguration={
                    "type": "S3",
                    "s3Configuration": {"bucketArn": "arn:aws:s3:::my-test-bucket"},
                },
            )
            ds_id = ds_resp["dataSource"]["dataSourceId"]

            job_resp = bedrock_agent.start_ingestion_job(knowledgeBaseId=kb_id, dataSourceId=ds_id)
            assert job_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            job_id = job_resp["ingestionJob"]["ingestionJobId"]
            assert job_resp["ingestionJob"]["status"] == "COMPLETE"

            get_resp = bedrock_agent.get_ingestion_job(
                knowledgeBaseId=kb_id, dataSourceId=ds_id, ingestionJobId=job_id
            )
            assert get_resp["ingestionJob"]["ingestionJobId"] == job_id

            list_resp = bedrock_agent.list_ingestion_jobs(knowledgeBaseId=kb_id, dataSourceId=ds_id)
            assert "ingestionJobSummaries" in list_resp
        finally:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)

    def test_flow_crud(self, bedrock_agent):
        """CreateFlow, GetFlow, ListFlows, UpdateFlow, PrepareFlow, DeleteFlow."""
        create_resp = bedrock_agent.create_flow(
            name="compat-test-flow",
            executionRoleArn="arn:aws:iam::123456789012:role/test-flow-role",
        )
        assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "id" in create_resp
        flow_id = create_resp["id"]
        try:
            get_resp = bedrock_agent.get_flow(flowIdentifier=flow_id)
            assert get_resp["id"] == flow_id
            assert get_resp["name"] == "compat-test-flow"

            list_resp = bedrock_agent.list_flows()
            assert "flowSummaries" in list_resp
            assert any(f["id"] == flow_id for f in list_resp["flowSummaries"])

            update_resp = bedrock_agent.update_flow(
                flowIdentifier=flow_id,
                name="compat-updated-flow",
                executionRoleArn="arn:aws:iam::123456789012:role/test-flow-role",
            )
            assert update_resp["name"] == "compat-updated-flow"

            prepare_resp = bedrock_agent.prepare_flow(flowIdentifier=flow_id)
            assert prepare_resp["status"] == "PREPARED"
        finally:
            bedrock_agent.delete_flow(flowIdentifier=flow_id)

    def test_flow_alias_crud(self, bedrock_agent):
        """CreateFlowAlias, GetFlowAlias, ListFlowAliases, UpdateFlowAlias, DeleteFlowAlias."""
        flow_resp = bedrock_agent.create_flow(
            name="compat-alias-flow",
            executionRoleArn="arn:aws:iam::123456789012:role/test-flow-role",
        )
        flow_id = flow_resp["id"]
        ver_resp = bedrock_agent.create_flow_version(flowIdentifier=flow_id)
        flow_version = ver_resp["version"]
        try:
            alias_resp = bedrock_agent.create_flow_alias(
                flowIdentifier=flow_id,
                name="compat-flow-alias",
                routingConfiguration=[{"flowVersion": flow_version}],
            )
            assert alias_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            alias_id = alias_resp["id"]

            get_resp = bedrock_agent.get_flow_alias(
                flowIdentifier=flow_id, aliasIdentifier=alias_id
            )
            assert get_resp["id"] == alias_id

            list_resp = bedrock_agent.list_flow_aliases(flowIdentifier=flow_id)
            assert "flowAliasSummaries" in list_resp

            update_resp = bedrock_agent.update_flow_alias(
                flowIdentifier=flow_id,
                aliasIdentifier=alias_id,
                name="compat-flow-alias-updated",
                routingConfiguration=[{"flowVersion": flow_version}],
            )
            assert update_resp["name"] == "compat-flow-alias-updated"

            del_resp = bedrock_agent.delete_flow_alias(
                flowIdentifier=flow_id, aliasIdentifier=alias_id
            )
            assert del_resp["id"] == alias_id
        finally:
            bedrock_agent.delete_flow(flowIdentifier=flow_id)

    def test_flow_version_crud(self, bedrock_agent):
        """CreateFlowVersion, GetFlowVersion, ListFlowVersions, DeleteFlowVersion."""
        flow_resp = bedrock_agent.create_flow(
            name="compat-version-flow",
            executionRoleArn="arn:aws:iam::123456789012:role/test-flow-role",
        )
        flow_id = flow_resp["id"]
        try:
            ver_resp = bedrock_agent.create_flow_version(flowIdentifier=flow_id)
            assert ver_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            flow_version = ver_resp["version"]

            get_resp = bedrock_agent.get_flow_version(
                flowIdentifier=flow_id, flowVersion=flow_version
            )
            assert get_resp["version"] == flow_version

            list_resp = bedrock_agent.list_flow_versions(flowIdentifier=flow_id)
            assert "flowVersionSummaries" in list_resp

            del_resp = bedrock_agent.delete_flow_version(
                flowIdentifier=flow_id, flowVersion=flow_version
            )
            assert del_resp["version"] == flow_version
        finally:
            bedrock_agent.delete_flow(flowIdentifier=flow_id)

    def test_prompt_crud(self, bedrock_agent):
        """CreatePrompt, GetPrompt, ListPrompts, UpdatePrompt, CreatePromptVersion, DeletePrompt."""
        create_resp = bedrock_agent.create_prompt(name="compat-test-prompt")
        assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "id" in create_resp
        prompt_id = create_resp["id"]
        try:
            get_resp = bedrock_agent.get_prompt(promptIdentifier=prompt_id)
            assert get_resp["id"] == prompt_id

            list_resp = bedrock_agent.list_prompts()
            assert "promptSummaries" in list_resp
            assert any(p["id"] == prompt_id for p in list_resp["promptSummaries"])

            update_resp = bedrock_agent.update_prompt(
                promptIdentifier=prompt_id, name="compat-updated-prompt"
            )
            assert update_resp["name"] == "compat-updated-prompt"

            ver_resp = bedrock_agent.create_prompt_version(promptIdentifier=prompt_id)
            assert ver_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            bedrock_agent.delete_prompt(promptIdentifier=prompt_id)

    def test_associate_agent_knowledge_base(self, bedrock_agent):
        """AssociateAgentKnowledgeBase links a KB to an agent."""
        agent_resp = bedrock_agent.create_agent(
            agentName="compat-assoc-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = agent_resp["agent"]["agentId"]
        agent_version = agent_resp["agent"]["agentVersion"]
        kb_config = {
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": (
                    "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
                )
            },
        }
        storage_config = {
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
        }
        kb_resp = bedrock_agent.create_knowledge_base(
            name="compat-assoc-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration=kb_config,
            storageConfiguration=storage_config,
        )
        kb_id = kb_resp["knowledgeBase"]["knowledgeBaseId"]
        try:
            resp = bedrock_agent.associate_agent_knowledge_base(
                agentId=agent_id,
                agentVersion=agent_version,
                knowledgeBaseId=kb_id,
                description="compat-test",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agentKnowledgeBase" in resp
            assert resp["agentKnowledgeBase"]["knowledgeBaseId"] == kb_id
            assert resp["agentKnowledgeBase"]["agentId"] == agent_id
        finally:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_get_agent_knowledge_base_and_list(self, bedrock_agent):
        """GetAgentKnowledgeBase and ListAgentKnowledgeBases work after association."""
        agent_resp = bedrock_agent.create_agent(
            agentName="compat-getkb-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = agent_resp["agent"]["agentId"]
        agent_version = agent_resp["agent"]["agentVersion"]
        kb_config = {
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": (
                    "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
                )
            },
        }
        storage_config = {
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
        }
        kb_resp = bedrock_agent.create_knowledge_base(
            name="compat-getkb-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration=kb_config,
            storageConfiguration=storage_config,
        )
        kb_id = kb_resp["knowledgeBase"]["knowledgeBaseId"]
        try:
            bedrock_agent.associate_agent_knowledge_base(
                agentId=agent_id,
                agentVersion=agent_version,
                knowledgeBaseId=kb_id,
                description="compat-test",
            )

            get_resp = bedrock_agent.get_agent_knowledge_base(
                agentId=agent_id, agentVersion=agent_version, knowledgeBaseId=kb_id
            )
            assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert get_resp["agentKnowledgeBase"]["knowledgeBaseId"] == kb_id

            list_resp = bedrock_agent.list_agent_knowledge_bases(
                agentId=agent_id, agentVersion=agent_version
            )
            assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agentKnowledgeBaseSummaries" in list_resp
            kb_ids = [
                s["knowledgeBaseId"] for s in list_resp["agentKnowledgeBaseSummaries"]
            ]
            assert kb_id in kb_ids
        finally:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_update_agent_knowledge_base(self, bedrock_agent):
        """UpdateAgentKnowledgeBase updates the description on a linked KB."""
        agent_resp = bedrock_agent.create_agent(
            agentName="compat-updatekb-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = agent_resp["agent"]["agentId"]
        agent_version = agent_resp["agent"]["agentVersion"]
        kb_config = {
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": (
                    "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
                )
            },
        }
        storage_config = {
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
        }
        kb_resp = bedrock_agent.create_knowledge_base(
            name="compat-updatekb-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration=kb_config,
            storageConfiguration=storage_config,
        )
        kb_id = kb_resp["knowledgeBase"]["knowledgeBaseId"]
        try:
            bedrock_agent.associate_agent_knowledge_base(
                agentId=agent_id,
                agentVersion=agent_version,
                knowledgeBaseId=kb_id,
                description="original-desc",
            )

            update_resp = bedrock_agent.update_agent_knowledge_base(
                agentId=agent_id,
                agentVersion=agent_version,
                knowledgeBaseId=kb_id,
                description="updated-desc",
            )
            assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agentKnowledgeBase" in update_resp
            assert update_resp["agentKnowledgeBase"]["description"] == "updated-desc"
        finally:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_disassociate_agent_knowledge_base(self, bedrock_agent):
        """DisassociateAgentKnowledgeBase removes the KB link from an agent."""
        agent_resp = bedrock_agent.create_agent(
            agentName="compat-disassoc-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = agent_resp["agent"]["agentId"]
        agent_version = agent_resp["agent"]["agentVersion"]
        kb_config = {
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": (
                    "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
                )
            },
        }
        storage_config = {
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
        }
        kb_resp = bedrock_agent.create_knowledge_base(
            name="compat-disassoc-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration=kb_config,
            storageConfiguration=storage_config,
        )
        kb_id = kb_resp["knowledgeBase"]["knowledgeBaseId"]
        try:
            bedrock_agent.associate_agent_knowledge_base(
                agentId=agent_id,
                agentVersion=agent_version,
                knowledgeBaseId=kb_id,
                description="to-be-disassociated",
            )
            disassoc_resp = bedrock_agent.disassociate_agent_knowledge_base(
                agentId=agent_id, agentVersion=agent_version, knowledgeBaseId=kb_id
            )
            assert disassoc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            list_resp = bedrock_agent.list_agent_knowledge_bases(
                agentId=agent_id, agentVersion=agent_version
            )
            kb_ids = [
                s["knowledgeBaseId"] for s in list_resp["agentKnowledgeBaseSummaries"]
            ]
            assert kb_id not in kb_ids
        finally:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_get_agent_version(self, bedrock_agent):
        """GetAgentVersion returns the version details for an agent."""
        agent_resp = bedrock_agent.create_agent(
            agentName="compat-getver-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = agent_resp["agent"]["agentId"]
        agent_version = agent_resp["agent"]["agentVersion"]
        try:
            resp = bedrock_agent.get_agent_version(
                agentId=agent_id, agentVersion=agent_version
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agentVersion" in resp
            assert resp["agentVersion"]["agentId"] == agent_id
            assert resp["agentVersion"]["version"] == agent_version
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_delete_agent_version(self, bedrock_agent):
        """DeleteAgentVersion removes a non-DRAFT agent version."""
        agent_resp = bedrock_agent.create_agent(
            agentName="compat-delver-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = agent_resp["agent"]["agentId"]
        try:
            # Prepare to create a numbered version
            bedrock_agent.prepare_agent(agentId=agent_id)
            # ListAgentVersions shows DRAFT; delete it with DRAFT version string
            del_resp = bedrock_agent.delete_agent_version(
                agentId=agent_id,
                agentVersion=agent_resp["agent"]["agentVersion"],
            )
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agentId" in del_resp
            assert del_resp["agentId"] == agent_id
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_update_agent_action_group(self, bedrock_agent):
        """UpdateAgentActionGroup updates name/state on an existing action group."""
        agent_resp = bedrock_agent.create_agent(
            agentName="compat-updateag-agent",
            foundationModel="amazon.titan-text-lite-v1",
        )
        agent_id = agent_resp["agent"]["agentId"]
        agent_version = agent_resp["agent"]["agentVersion"]
        try:
            ag_resp = bedrock_agent.create_agent_action_group(
                agentId=agent_id,
                agentVersion=agent_version,
                actionGroupName="compat-update-ag",
                actionGroupState="ENABLED",
            )
            ag_id = ag_resp["agentActionGroup"]["actionGroupId"]

            update_resp = bedrock_agent.update_agent_action_group(
                agentId=agent_id,
                agentVersion=agent_version,
                actionGroupId=ag_id,
                actionGroupName="compat-updated-ag",
                actionGroupState="DISABLED",
            )
            assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "agentActionGroup" in update_resp
            assert update_resp["agentActionGroup"]["actionGroupName"] == "compat-updated-ag"
            assert update_resp["agentActionGroup"]["actionGroupState"] == "DISABLED"
        finally:
            bedrock_agent.delete_agent(agentId=agent_id)

    def test_stop_ingestion_job(self, bedrock_agent):
        """StopIngestionJob stops a running ingestion job."""
        kb_config = {
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": (
                    "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
                )
            },
        }
        storage_config = {
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
        }
        kb_resp = bedrock_agent.create_knowledge_base(
            name="compat-stop-ij-kb",
            roleArn="arn:aws:iam::123456789012:role/test-kb-role",
            knowledgeBaseConfiguration=kb_config,
            storageConfiguration=storage_config,
        )
        kb_id = kb_resp["knowledgeBase"]["knowledgeBaseId"]
        try:
            ds_resp = bedrock_agent.create_data_source(
                knowledgeBaseId=kb_id,
                name="compat-stop-ij-ds",
                dataSourceConfiguration={
                    "type": "S3",
                    "s3Configuration": {"bucketArn": "arn:aws:s3:::my-test-bucket"},
                },
            )
            ds_id = ds_resp["dataSource"]["dataSourceId"]
            job_resp = bedrock_agent.start_ingestion_job(
                knowledgeBaseId=kb_id, dataSourceId=ds_id
            )
            job_id = job_resp["ingestionJob"]["ingestionJobId"]

            stop_resp = bedrock_agent.stop_ingestion_job(
                knowledgeBaseId=kb_id, dataSourceId=ds_id, ingestionJobId=job_id
            )
            assert stop_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "ingestionJob" in stop_resp
            assert stop_resp["ingestionJob"]["ingestionJobId"] == job_id
        finally:
            bedrock_agent.delete_knowledge_base(knowledgeBaseId=kb_id)
