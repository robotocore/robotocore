"""Amazon Connect compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def connect():
    return make_client("connect")


def _create_instance(client):
    """Helper to create a Connect instance."""
    resp = client.create_instance(
        IdentityManagementType="CONNECT_MANAGED",
        InboundCallsEnabled=True,
        OutboundCallsEnabled=True,
    )
    return resp["Id"], resp["Arn"]


def _list_all_instance_ids(client):
    """Paginate through all instances and return their IDs."""
    ids = []
    paginator = client.get_paginator("list_instances")
    for page in paginator.paginate():
        for inst in page.get("InstanceSummaryList", []):
            ids.append(inst["Id"])
    return ids


class TestConnectInstances:
    def test_create_instance(self, connect):
        instance_id, arn = _create_instance(connect)
        assert instance_id is not None
        assert "arn:aws:connect:" in arn
        assert instance_id in arn

    def test_create_instance_returns_id_and_arn(self, connect):
        resp = connect.create_instance(
            IdentityManagementType="CONNECT_MANAGED",
            InboundCallsEnabled=True,
            OutboundCallsEnabled=False,
        )
        assert "Id" in resp
        assert "Arn" in resp
        assert len(resp["Id"]) > 0

    def test_list_instances(self, connect):
        _create_instance(connect)
        resp = connect.list_instances()
        assert "InstanceSummaryList" in resp
        assert len(resp["InstanceSummaryList"]) >= 1

    def test_list_instances_has_summary_fields(self, connect):
        """Verify list_instances returns summaries with expected fields."""
        resp = connect.list_instances()
        assert len(resp["InstanceSummaryList"]) >= 1
        summary = resp["InstanceSummaryList"][0]
        assert "Id" in summary
        assert "Arn" in summary
        assert "IdentityManagementType" in summary

    def test_describe_instance(self, connect):
        instance_id, arn = _create_instance(connect)
        resp = connect.describe_instance(InstanceId=instance_id)
        assert "Instance" in resp
        instance = resp["Instance"]
        assert instance["Id"] == instance_id
        assert instance["Arn"] == arn

    def test_describe_instance_fields(self, connect):
        instance_id, _ = _create_instance(connect)
        resp = connect.describe_instance(InstanceId=instance_id)
        instance = resp["Instance"]
        assert instance["IdentityManagementType"] == "CONNECT_MANAGED"
        assert instance["InboundCallsEnabled"] is True
        assert instance["OutboundCallsEnabled"] is True

    def test_describe_instance_matches_create(self, connect):
        """Describe returns the same ID and ARN that create returned."""
        instance_id, arn = _create_instance(connect)
        resp = connect.describe_instance(InstanceId=instance_id)
        assert resp["Instance"]["Id"] == instance_id
        assert resp["Instance"]["Arn"] == arn

    def test_delete_instance(self, connect):
        instance_id, _ = _create_instance(connect)
        connect.delete_instance(InstanceId=instance_id)
        with pytest.raises(ClientError):
            connect.describe_instance(InstanceId=instance_id)

    def test_delete_instance_then_describe_fails(self, connect):
        instance_id, _ = _create_instance(connect)
        connect.delete_instance(InstanceId=instance_id)
        with pytest.raises(ClientError):
            connect.describe_instance(InstanceId=instance_id)

    def test_create_multiple_instances_unique_ids(self, connect):
        id1, _ = _create_instance(connect)
        id2, _ = _create_instance(connect)
        assert id1 != id2
        # Both should be describable
        resp1 = connect.describe_instance(InstanceId=id1)
        resp2 = connect.describe_instance(InstanceId=id2)
        assert resp1["Instance"]["Id"] == id1
        assert resp2["Instance"]["Id"] == id2

    def test_delete_one_of_multiple_instances(self, connect):
        id1, _ = _create_instance(connect)
        id2, _ = _create_instance(connect)
        connect.delete_instance(InstanceId=id1)
        # Verify deleted instance is gone
        with pytest.raises(ClientError):
            connect.describe_instance(InstanceId=id1)
        # Verify the other instance still exists
        resp = connect.describe_instance(InstanceId=id2)
        assert resp["Instance"]["Id"] == id2

    def test_list_tags_for_resource(self, connect):
        """ListTagsForResource returns tags on a Connect instance."""
        instance_id, arn = _create_instance(connect)
        try:
            resp = connect.list_tags_for_resource(resourceArn=arn)
            assert "tags" in resp
            assert isinstance(resp["tags"], dict)
        finally:
            try:
                connect.delete_instance(InstanceId=instance_id)
            except Exception:
                pass

    def test_list_analytics_data_associations(self, connect):
        """ListAnalyticsDataAssociations returns a response."""
        instance_id, _ = _create_instance(connect)
        try:
            resp = connect.list_analytics_data_associations(InstanceId=instance_id)
            assert "Results" in resp
            assert isinstance(resp["Results"], list)
        except ClientError as e:
            # Some implementations may not support this yet
            assert e.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidRequestException",
            )
        finally:
            try:
                connect.delete_instance(InstanceId=instance_id)
            except Exception:
                pass


class TestConnectListOps:
    """Tests for Connect list operations that take InstanceId."""

    @pytest.fixture
    def instance_id(self, connect):
        iid, _ = _create_instance(connect)
        yield iid

    def test_list_agent_statuses(self, connect, instance_id):
        resp = connect.list_agent_statuses(InstanceId=instance_id)
        assert "AgentStatusSummaryList" in resp
        assert isinstance(resp["AgentStatusSummaryList"], list)

    def test_list_approved_origins(self, connect, instance_id):
        resp = connect.list_approved_origins(InstanceId=instance_id)
        assert "Origins" in resp
        assert isinstance(resp["Origins"], list)

    def test_list_bots(self, connect, instance_id):
        resp = connect.list_bots(InstanceId=instance_id, LexVersion="V2")
        assert "LexBots" in resp
        assert isinstance(resp["LexBots"], list)

    def test_list_contact_evaluations(self, connect, instance_id):
        resp = connect.list_contact_evaluations(InstanceId=instance_id, ContactId="fake-contact-id")
        assert "EvaluationSummaryList" in resp
        assert isinstance(resp["EvaluationSummaryList"], list)

    def test_list_contact_flow_modules(self, connect, instance_id):
        resp = connect.list_contact_flow_modules(InstanceId=instance_id)
        assert "ContactFlowModulesSummaryList" in resp
        assert isinstance(resp["ContactFlowModulesSummaryList"], list)

    def test_list_contact_flow_versions(self, connect, instance_id):
        resp = connect.list_contact_flow_versions(
            InstanceId=instance_id, ContactFlowId="fake-flow-id"
        )
        assert "ContactFlowVersionSummaryList" in resp
        assert isinstance(resp["ContactFlowVersionSummaryList"], list)

    def test_list_contact_flows(self, connect, instance_id):
        resp = connect.list_contact_flows(InstanceId=instance_id)
        assert "ContactFlowSummaryList" in resp
        assert isinstance(resp["ContactFlowSummaryList"], list)

    def test_list_contact_references(self, connect, instance_id):
        resp = connect.list_contact_references(
            InstanceId=instance_id,
            ContactId="fake-contact-id",
            ReferenceTypes=["URL"],
        )
        assert "ReferenceSummaryList" in resp
        assert isinstance(resp["ReferenceSummaryList"], list)

    def test_list_default_vocabularies(self, connect, instance_id):
        resp = connect.list_default_vocabularies(InstanceId=instance_id)
        assert "DefaultVocabularyList" in resp
        assert isinstance(resp["DefaultVocabularyList"], list)

    def test_list_evaluation_form_versions(self, connect, instance_id):
        resp = connect.list_evaluation_form_versions(
            InstanceId=instance_id, EvaluationFormId="fake-form-id"
        )
        assert "EvaluationFormVersionSummaryList" in resp
        assert isinstance(resp["EvaluationFormVersionSummaryList"], list)

    def test_list_evaluation_forms(self, connect, instance_id):
        resp = connect.list_evaluation_forms(InstanceId=instance_id)
        assert "EvaluationFormSummaryList" in resp
        assert isinstance(resp["EvaluationFormSummaryList"], list)

    def test_list_flow_associations(self, connect, instance_id):
        resp = connect.list_flow_associations(InstanceId=instance_id)
        assert "FlowAssociationSummaryList" in resp
        assert isinstance(resp["FlowAssociationSummaryList"], list)

    def test_list_hours_of_operation_overrides(self, connect, instance_id):
        resp = connect.list_hours_of_operation_overrides(
            InstanceId=instance_id, HoursOfOperationId="fake-hoo-id"
        )
        assert "HoursOfOperationOverrideList" in resp

    def test_list_hours_of_operations(self, connect, instance_id):
        resp = connect.list_hours_of_operations(InstanceId=instance_id)
        assert "HoursOfOperationSummaryList" in resp
        assert isinstance(resp["HoursOfOperationSummaryList"], list)

    def test_list_instance_attributes(self, connect, instance_id):
        resp = connect.list_instance_attributes(InstanceId=instance_id)
        assert "Attributes" in resp
        assert isinstance(resp["Attributes"], list)

    def test_list_instance_storage_configs(self, connect, instance_id):
        resp = connect.list_instance_storage_configs(
            InstanceId=instance_id, ResourceType="CHAT_TRANSCRIPTS"
        )
        assert "StorageConfigs" in resp
        assert isinstance(resp["StorageConfigs"], list)

    def test_list_instances(self, connect):
        resp = connect.list_instances()
        assert "InstanceSummaryList" in resp
        assert len(resp["InstanceSummaryList"]) >= 1

    def test_list_lambda_functions(self, connect, instance_id):
        resp = connect.list_lambda_functions(InstanceId=instance_id)
        assert "LambdaFunctions" in resp
        assert isinstance(resp["LambdaFunctions"], list)

    def test_list_phone_numbers(self, connect, instance_id):
        resp = connect.list_phone_numbers(InstanceId=instance_id)
        assert "PhoneNumberSummaryList" in resp
        assert isinstance(resp["PhoneNumberSummaryList"], list)

    def test_list_phone_numbers_v2(self, connect):
        resp = connect.list_phone_numbers_v2()
        assert "ListPhoneNumbersSummaryList" in resp
        assert isinstance(resp["ListPhoneNumbersSummaryList"], list)

    def test_list_prompts(self, connect, instance_id):
        resp = connect.list_prompts(InstanceId=instance_id)
        assert "PromptSummaryList" in resp
        assert isinstance(resp["PromptSummaryList"], list)

    def test_list_queue_quick_connects(self, connect, instance_id):
        resp = connect.list_queue_quick_connects(InstanceId=instance_id, QueueId="fake-queue-id")
        assert "QuickConnectSummaryList" in resp
        assert isinstance(resp["QuickConnectSummaryList"], list)

    def test_list_queues(self, connect, instance_id):
        resp = connect.list_queues(InstanceId=instance_id)
        assert "QueueSummaryList" in resp
        assert isinstance(resp["QueueSummaryList"], list)

    def test_list_quick_connects(self, connect, instance_id):
        resp = connect.list_quick_connects(InstanceId=instance_id)
        assert "QuickConnectSummaryList" in resp
        assert isinstance(resp["QuickConnectSummaryList"], list)

    def test_list_routing_profiles(self, connect, instance_id):
        resp = connect.list_routing_profiles(InstanceId=instance_id)
        assert "RoutingProfileSummaryList" in resp
        assert isinstance(resp["RoutingProfileSummaryList"], list)

    def test_list_security_keys(self, connect, instance_id):
        resp = connect.list_security_keys(InstanceId=instance_id)
        assert "SecurityKeys" in resp
        assert isinstance(resp["SecurityKeys"], list)

    def test_list_security_profile_applications(self, connect, instance_id):
        resp = connect.list_security_profile_applications(
            InstanceId=instance_id, SecurityProfileId="fake-sp-id"
        )
        assert "Applications" in resp
        assert isinstance(resp["Applications"], list)

    def test_list_security_profile_permissions(self, connect, instance_id):
        resp = connect.list_security_profile_permissions(
            InstanceId=instance_id, SecurityProfileId="fake-sp-id"
        )
        assert "Permissions" in resp

    def test_list_security_profiles(self, connect, instance_id):
        resp = connect.list_security_profiles(InstanceId=instance_id)
        assert "SecurityProfileSummaryList" in resp
        assert isinstance(resp["SecurityProfileSummaryList"], list)

    def test_list_use_cases(self, connect, instance_id):
        """ListUseCases returns use case list for an integration association."""
        resp = connect.list_use_cases(
            InstanceId=instance_id,
            IntegrationAssociationId="fake-ia-id",
        )
        assert "UseCaseSummaryList" in resp
        assert isinstance(resp["UseCaseSummaryList"], list)
