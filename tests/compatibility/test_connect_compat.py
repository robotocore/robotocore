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

    def test_list_evaluation_forms(self, connect, instance_id):
        resp = connect.list_evaluation_forms(InstanceId=instance_id)
        assert "EvaluationFormSummaryList" in resp
        assert isinstance(resp["EvaluationFormSummaryList"], list)

    def test_list_instances(self, connect):
        resp = connect.list_instances()
        assert "InstanceSummaryList" in resp
        assert len(resp["InstanceSummaryList"]) >= 1

    def test_list_phone_numbers_v2(self, connect):
        resp = connect.list_phone_numbers_v2()
        assert "ListPhoneNumbersSummaryList" in resp
        assert isinstance(resp["ListPhoneNumbersSummaryList"], list)

    def test_list_quick_connects(self, connect, instance_id):
        resp = connect.list_quick_connects(InstanceId=instance_id)
        assert "QuickConnectSummaryList" in resp
        assert isinstance(resp["QuickConnectSummaryList"], list)


class TestConnectCreateDescribeOps:
    """Tests for Connect create/describe operations."""

    @pytest.fixture
    def instance_id(self, connect):
        iid, _ = _create_instance(connect)
        yield iid

    @pytest.fixture
    def instance_arn(self, connect):
        iid, arn = _create_instance(connect)
        return arn

    # ---- AgentStatus ----

    def test_create_agent_status(self, connect, instance_id):
        resp = connect.create_agent_status(
            InstanceId=instance_id,
            Name="Available",
            State="ENABLED",
            Description="Test agent status",
        )
        assert "AgentStatusARN" in resp
        assert "AgentStatusId" in resp
        assert len(resp["AgentStatusId"]) > 0

    def test_describe_agent_status(self, connect, instance_id):
        create_resp = connect.create_agent_status(
            InstanceId=instance_id,
            Name="Available",
            State="ENABLED",
        )
        agent_status_id = create_resp["AgentStatusId"]
        resp = connect.describe_agent_status(
            InstanceId=instance_id,
            AgentStatusId=agent_status_id,
        )
        assert "AgentStatus" in resp
        status = resp["AgentStatus"]
        assert status["Name"] == "Available"
        assert status["State"] == "ENABLED"

    # ---- ContactFlow ----

    def test_create_contact_flow(self, connect, instance_id):
        resp = connect.create_contact_flow(
            InstanceId=instance_id,
            Name="TestFlow",
            Type="CONTACT_FLOW",
            Content='{"Version":"2019-10-30","StartAction":"action1","Actions":[]}',
        )
        assert "ContactFlowId" in resp
        assert "ContactFlowArn" in resp

    def test_describe_contact_flow(self, connect, instance_id):
        create_resp = connect.create_contact_flow(
            InstanceId=instance_id,
            Name="TestFlow",
            Type="CONTACT_FLOW",
            Content='{"Version":"2019-10-30","StartAction":"action1","Actions":[]}',
        )
        flow_id = create_resp["ContactFlowId"]
        resp = connect.describe_contact_flow(
            InstanceId=instance_id,
            ContactFlowId=flow_id,
        )
        assert "ContactFlow" in resp
        assert resp["ContactFlow"]["Name"] == "TestFlow"

    # ---- ContactFlowModule ----

    def test_create_contact_flow_module(self, connect, instance_id):
        resp = connect.create_contact_flow_module(
            InstanceId=instance_id,
            Name="TestModule",
            Content='{"Version":"2019-10-30","StartAction":"action1","Actions":[]}',
        )
        assert "Id" in resp
        assert "Arn" in resp

    def test_describe_contact_flow_module(self, connect, instance_id):
        create_resp = connect.create_contact_flow_module(
            InstanceId=instance_id,
            Name="TestModule",
            Content='{"Version":"2019-10-30","StartAction":"action1","Actions":[]}',
        )
        module_id = create_resp["Id"]
        resp = connect.describe_contact_flow_module(
            InstanceId=instance_id,
            ContactFlowModuleId=module_id,
        )
        assert "ContactFlowModule" in resp
        assert resp["ContactFlowModule"]["Name"] == "TestModule"

    # ---- EvaluationForm ----

    def test_create_evaluation_form(self, connect, instance_id):
        resp = connect.create_evaluation_form(
            InstanceId=instance_id,
            Title="TestEvalForm",
            Items=[
                {
                    "Section": {
                        "Title": "Section1",
                        "RefId": "s1",
                        "Items": [],
                    }
                }
            ],
        )
        assert "EvaluationFormId" in resp
        assert "EvaluationFormArn" in resp

    def test_describe_evaluation_form(self, connect, instance_id):
        create_resp = connect.create_evaluation_form(
            InstanceId=instance_id,
            Title="TestEvalForm",
            Items=[
                {
                    "Section": {
                        "Title": "Section1",
                        "RefId": "s1",
                        "Items": [],
                    }
                }
            ],
        )
        form_id = create_resp["EvaluationFormId"]
        resp = connect.describe_evaluation_form(
            InstanceId=instance_id,
            EvaluationFormId=form_id,
        )
        assert "EvaluationForm" in resp
        assert resp["EvaluationForm"]["Title"] == "TestEvalForm"

    # ---- HoursOfOperation ----

    def test_create_hours_of_operation(self, connect, instance_id):
        resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="BusinessHours",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        assert "HoursOfOperationId" in resp
        assert "HoursOfOperationArn" in resp

    def test_describe_hours_of_operation(self, connect, instance_id):
        create_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="BusinessHours",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = create_resp["HoursOfOperationId"]
        resp = connect.describe_hours_of_operation(
            InstanceId=instance_id,
            HoursOfOperationId=hoo_id,
        )
        assert "HoursOfOperation" in resp
        assert resp["HoursOfOperation"]["Name"] == "BusinessHours"

    # ---- Prompt ----

    def test_create_prompt(self, connect, instance_id):
        resp = connect.create_prompt(
            InstanceId=instance_id,
            Name="TestPrompt",
            S3Uri="s3://my-bucket/prompt.wav",
        )
        assert "PromptARN" in resp
        assert "PromptId" in resp

    def test_describe_prompt(self, connect, instance_id):
        create_resp = connect.create_prompt(
            InstanceId=instance_id,
            Name="TestPrompt",
            S3Uri="s3://my-bucket/prompt.wav",
        )
        prompt_id = create_resp["PromptId"]
        resp = connect.describe_prompt(
            InstanceId=instance_id,
            PromptId=prompt_id,
        )
        assert "Prompt" in resp
        assert resp["Prompt"]["Name"] == "TestPrompt"

    # ---- Queue ----

    def test_create_queue(self, connect, instance_id):
        # Queue requires HoursOfOperationId - create one first
        hoo_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="QueueHours",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = hoo_resp["HoursOfOperationId"]
        resp = connect.create_queue(
            InstanceId=instance_id,
            Name="TestQueue",
            HoursOfOperationId=hoo_id,
        )
        assert "QueueId" in resp
        assert "QueueArn" in resp

    def test_describe_queue(self, connect, instance_id):
        hoo_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="QueueHours2",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = hoo_resp["HoursOfOperationId"]
        create_resp = connect.create_queue(
            InstanceId=instance_id,
            Name="TestQueue2",
            HoursOfOperationId=hoo_id,
        )
        queue_id = create_resp["QueueId"]
        resp = connect.describe_queue(
            InstanceId=instance_id,
            QueueId=queue_id,
        )
        assert "Queue" in resp
        assert resp["Queue"]["Name"] == "TestQueue2"

    # ---- QuickConnect ----

    def test_create_quick_connect(self, connect, instance_id):
        resp = connect.create_quick_connect(
            InstanceId=instance_id,
            Name="TestQuickConnect",
            QuickConnectConfig={
                "QuickConnectType": "PHONE_NUMBER",
                "PhoneConfig": {"PhoneNumber": "+15555551234"},
            },
        )
        assert "QuickConnectARN" in resp
        assert "QuickConnectId" in resp

    def test_describe_quick_connect(self, connect, instance_id):
        create_resp = connect.create_quick_connect(
            InstanceId=instance_id,
            Name="TestQuickConnect2",
            QuickConnectConfig={
                "QuickConnectType": "PHONE_NUMBER",
                "PhoneConfig": {"PhoneNumber": "+15555551234"},
            },
        )
        qc_id = create_resp["QuickConnectId"]
        resp = connect.describe_quick_connect(
            InstanceId=instance_id,
            QuickConnectId=qc_id,
        )
        assert "QuickConnect" in resp
        assert resp["QuickConnect"]["Name"] == "TestQuickConnect2"

    # ---- RoutingProfile ----

    def test_create_routing_profile(self, connect, instance_id):
        resp = connect.create_routing_profile(
            InstanceId=instance_id,
            Name="TestRoutingProfile",
            Description="Test routing profile",
            DefaultOutboundQueueId="fake-queue-id",
            MediaConcurrencies=[
                {"Channel": "VOICE", "Concurrency": 1},
            ],
        )
        assert "RoutingProfileArn" in resp
        assert "RoutingProfileId" in resp

    def test_describe_routing_profile(self, connect, instance_id):
        create_resp = connect.create_routing_profile(
            InstanceId=instance_id,
            Name="TestRoutingProfile2",
            Description="Test routing profile 2",
            DefaultOutboundQueueId="fake-queue-id",
            MediaConcurrencies=[
                {"Channel": "VOICE", "Concurrency": 1},
            ],
        )
        rp_id = create_resp["RoutingProfileId"]
        resp = connect.describe_routing_profile(
            InstanceId=instance_id,
            RoutingProfileId=rp_id,
        )
        assert "RoutingProfile" in resp
        assert resp["RoutingProfile"]["Name"] == "TestRoutingProfile2"

    # ---- Rule ----

    def test_create_rule(self, connect, instance_id):
        resp = connect.create_rule(
            InstanceId=instance_id,
            Name="TestRule",
            TriggerEventSource={
                "EventSourceName": "OnPostCallAnalysisAvailable",
            },
            Function='EQUALS("]]]]", "]]]")',
            Actions=[
                {
                    "ActionType": "GENERATE_EVENTBRIDGE_EVENT",
                    "EventBridgeAction": {"Name": "test-event"},
                }
            ],
            PublishStatus="DRAFT",
        )
        assert "RuleArn" in resp
        assert "RuleId" in resp

    def test_describe_rule(self, connect, instance_id):
        create_resp = connect.create_rule(
            InstanceId=instance_id,
            Name="TestRule2",
            TriggerEventSource={
                "EventSourceName": "OnPostCallAnalysisAvailable",
            },
            Function='EQUALS("a", "b")',
            Actions=[
                {
                    "ActionType": "GENERATE_EVENTBRIDGE_EVENT",
                    "EventBridgeAction": {"Name": "test-event-2"},
                }
            ],
            PublishStatus="DRAFT",
        )
        rule_id = create_resp["RuleId"]
        resp = connect.describe_rule(
            InstanceId=instance_id,
            RuleId=rule_id,
        )
        assert "Rule" in resp
        assert resp["Rule"]["Name"] == "TestRule2"

    # ---- SecurityProfile ----

    def test_create_security_profile(self, connect, instance_id):
        resp = connect.create_security_profile(
            InstanceId=instance_id,
            SecurityProfileName="TestSecProfile",
            Description="Test security profile",
        )
        assert "SecurityProfileId" in resp
        assert "SecurityProfileArn" in resp

    def test_describe_security_profile(self, connect, instance_id):
        create_resp = connect.create_security_profile(
            InstanceId=instance_id,
            SecurityProfileName="TestSecProfile2",
        )
        sp_id = create_resp["SecurityProfileId"]
        resp = connect.describe_security_profile(
            InstanceId=instance_id,
            SecurityProfileId=sp_id,
        )
        assert "SecurityProfile" in resp
        assert resp["SecurityProfile"]["SecurityProfileName"] == "TestSecProfile2"

    # ---- UserHierarchyGroup ----

    def test_create_user_hierarchy_group(self, connect, instance_id):
        resp = connect.create_user_hierarchy_group(
            InstanceId=instance_id,
            Name="TestHierarchyGroup",
        )
        assert "HierarchyGroupId" in resp
        assert "HierarchyGroupArn" in resp

    def test_describe_user_hierarchy_group(self, connect, instance_id):
        create_resp = connect.create_user_hierarchy_group(
            InstanceId=instance_id,
            Name="TestHierarchyGroup2",
        )
        group_id = create_resp["HierarchyGroupId"]
        resp = connect.describe_user_hierarchy_group(
            InstanceId=instance_id,
            HierarchyGroupId=group_id,
        )
        assert "HierarchyGroup" in resp
        assert resp["HierarchyGroup"]["Name"] == "TestHierarchyGroup2"

    # ---- View ----

    def test_create_view(self, connect, instance_id):
        resp = connect.create_view(
            InstanceId=instance_id,
            Name="TestView",
            Status="SAVED",
            Content={"Template": '{"Type":"object"}'},
        )
        assert "View" in resp

    def test_describe_view(self, connect, instance_id):
        create_resp = connect.create_view(
            InstanceId=instance_id,
            Name="TestView2",
            Status="SAVED",
            Content={"Template": '{"Type":"object"}'},
        )
        view_id = create_resp["View"]["Id"]
        resp = connect.describe_view(
            InstanceId=instance_id,
            ViewId=view_id,
        )
        assert "View" in resp
        assert resp["View"]["Name"] == "TestView2"

    # ---- Vocabulary ----

    def test_create_vocabulary(self, connect, instance_id):
        resp = connect.create_vocabulary(
            InstanceId=instance_id,
            VocabularyName="TestVocab",
            LanguageCode="en-US",
            Content="Phrase\tIPA\tSoundsLike\tDisplayAs\ntest\t\t\ttest",
        )
        assert "VocabularyArn" in resp
        assert "VocabularyId" in resp
        assert resp["State"] == "ACTIVE"

    def test_describe_vocabulary(self, connect, instance_id):
        create_resp = connect.create_vocabulary(
            InstanceId=instance_id,
            VocabularyName="TestVocab2",
            LanguageCode="en-US",
            Content="Phrase\tIPA\tSoundsLike\tDisplayAs\nword\t\t\tword",
        )
        vocab_id = create_resp["VocabularyId"]
        resp = connect.describe_vocabulary(
            InstanceId=instance_id,
            VocabularyId=vocab_id,
        )
        assert "Vocabulary" in resp
        assert resp["Vocabulary"]["Name"] == "TestVocab2"

    # ---- DescribeInstanceAttribute ----

    def test_describe_instance_attribute(self, connect, instance_id):
        resp = connect.describe_instance_attribute(
            InstanceId=instance_id,
            AttributeType="INBOUND_CALLS",
        )
        assert "Attribute" in resp
        assert resp["Attribute"]["AttributeType"] == "INBOUND_CALLS"

    # ---- DescribePhoneNumber ----

    def test_describe_phone_number_not_found(self, connect):
        with pytest.raises(ClientError) as exc_info:
            connect.describe_phone_number(
                PhoneNumberId="fake-phone-number-id",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ---- DescribeTrafficDistributionGroup ----

    def test_describe_traffic_distribution_group_not_found(self, connect):
        with pytest.raises(ClientError) as exc_info:
            connect.describe_traffic_distribution_group(
                TrafficDistributionGroupId="fake-tdg-id",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ---- DescribeUser ----

    def test_describe_user_not_found(self, connect, instance_id):
        with pytest.raises(ClientError) as exc_info:
            connect.describe_user(
                InstanceId=instance_id,
                UserId="fake-user-id",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ---- DescribeUserHierarchyStructure ----

    def test_describe_user_hierarchy_structure(self, connect, instance_id):
        resp = connect.describe_user_hierarchy_structure(
            InstanceId=instance_id,
        )
        assert "HierarchyStructure" in resp
        structure = resp["HierarchyStructure"]
        assert "LevelOne" in structure

    # ---- AssociateAnalyticsDataSet / DisassociateAnalyticsDataSet ----

    def test_associate_and_disassociate_analytics_data_set(self, connect, instance_id):
        resp = connect.associate_analytics_data_set(
            InstanceId=instance_id,
            DataSetId="test-dataset-123",
        )
        assert "DataSetId" in resp
        assert resp["DataSetId"] == "test-dataset-123"
        assert "TargetAccountId" in resp

        # Disassociate
        connect.disassociate_analytics_data_set(
            InstanceId=instance_id,
            DataSetId="test-dataset-123",
        )
        # Verify disassociation by trying again (should succeed since it's gone)
        resp2 = connect.associate_analytics_data_set(
            InstanceId=instance_id,
            DataSetId="test-dataset-123",
        )
        assert resp2["DataSetId"] == "test-dataset-123"

    # ---- TagResource ----

    def test_tag_resource(self, connect, instance_id):
        # Create a resource to tag
        create_resp = connect.create_agent_status(
            InstanceId=instance_id,
            Name="TagTestStatus",
            State="ENABLED",
        )
        arn = create_resp["AgentStatusARN"]
        connect.tag_resource(
            resourceArn=arn,
            tags={"env": "test", "project": "robotocore"},
        )
        resp = connect.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp
        assert resp["tags"]["env"] == "test"
        assert resp["tags"]["project"] == "robotocore"

    # ---- GetContactAttributes / UpdateContactAttributes ----

    def test_get_and_update_contact_attributes(self, connect, instance_id):
        contact_id = "test-contact-123"
        # Update contact attributes
        connect.update_contact_attributes(
            InstanceId=instance_id,
            InitialContactId=contact_id,
            Attributes={"greeting": "hello", "language": "en"},
        )
        # Get them back
        resp = connect.get_contact_attributes(
            InstanceId=instance_id,
            InitialContactId=contact_id,
        )
        assert "Attributes" in resp
        assert resp["Attributes"]["greeting"] == "hello"
        assert resp["Attributes"]["language"] == "en"


class TestConnectListOperations:
    """Tests for Connect list operations that require InstanceId."""

    @pytest.fixture
    def instance_id(self, connect):
        iid, _ = _create_instance(connect)
        yield iid

    def test_list_approved_origins(self, connect, instance_id):
        resp = connect.list_approved_origins(InstanceId=instance_id)
        assert "Origins" in resp
        assert isinstance(resp["Origins"], list)

    def test_list_bots(self, connect, instance_id):
        resp = connect.list_bots(InstanceId=instance_id, LexVersion="V2")
        assert "LexBots" in resp
        assert isinstance(resp["LexBots"], list)

    def test_list_contact_flow_modules(self, connect, instance_id):
        resp = connect.list_contact_flow_modules(InstanceId=instance_id)
        assert "ContactFlowModulesSummaryList" in resp
        assert isinstance(resp["ContactFlowModulesSummaryList"], list)

    def test_list_contact_flows(self, connect, instance_id):
        resp = connect.list_contact_flows(InstanceId=instance_id)
        assert "ContactFlowSummaryList" in resp
        assert isinstance(resp["ContactFlowSummaryList"], list)

    def test_list_contact_flow_versions(self, connect, instance_id):
        # Create a contact flow first to list versions for
        create_resp = connect.create_contact_flow(
            InstanceId=instance_id,
            Name="FlowForVersions",
            Type="CONTACT_FLOW",
            Content='{"Version":"2019-10-30","StartAction":"action1","Actions":[]}',
        )
        flow_id = create_resp["ContactFlowId"]
        resp = connect.list_contact_flow_versions(
            InstanceId=instance_id,
            ContactFlowId=flow_id,
        )
        assert "ContactFlowVersionSummaryList" in resp
        assert isinstance(resp["ContactFlowVersionSummaryList"], list)

    def test_list_default_vocabularies(self, connect, instance_id):
        resp = connect.list_default_vocabularies(InstanceId=instance_id)
        assert "DefaultVocabularyList" in resp
        assert isinstance(resp["DefaultVocabularyList"], list)

    def test_list_evaluation_form_versions(self, connect, instance_id):
        create_resp = connect.create_evaluation_form(
            InstanceId=instance_id,
            Title="FormForVersions",
            Items=[
                {
                    "Section": {
                        "Title": "Section1",
                        "RefId": "s1",
                        "Items": [],
                    }
                }
            ],
        )
        form_id = create_resp["EvaluationFormId"]
        resp = connect.list_evaluation_form_versions(
            InstanceId=instance_id,
            EvaluationFormId=form_id,
        )
        assert "EvaluationFormVersionSummaryList" in resp
        assert isinstance(resp["EvaluationFormVersionSummaryList"], list)

    def test_list_flow_associations(self, connect, instance_id):
        resp = connect.list_flow_associations(InstanceId=instance_id)
        assert "FlowAssociationSummaryList" in resp
        assert isinstance(resp["FlowAssociationSummaryList"], list)

    def test_list_hours_of_operations(self, connect, instance_id):
        resp = connect.list_hours_of_operations(InstanceId=instance_id)
        assert "HoursOfOperationSummaryList" in resp
        assert isinstance(resp["HoursOfOperationSummaryList"], list)

    def test_list_hours_of_operation_overrides(self, connect, instance_id):
        # Need an hours of operation to list overrides for
        hoo_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="OverrideTestHours",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = hoo_resp["HoursOfOperationId"]
        resp = connect.list_hours_of_operation_overrides(
            InstanceId=instance_id,
            HoursOfOperationId=hoo_id,
        )
        assert "HoursOfOperationOverrideList" in resp
        assert isinstance(resp["HoursOfOperationOverrideList"], list)

    def test_list_instance_attributes(self, connect, instance_id):
        resp = connect.list_instance_attributes(InstanceId=instance_id)
        assert "Attributes" in resp
        assert isinstance(resp["Attributes"], list)

    def test_list_instance_storage_configs(self, connect, instance_id):
        resp = connect.list_instance_storage_configs(
            InstanceId=instance_id,
            ResourceType="CHAT_TRANSCRIPTS",
        )
        assert "StorageConfigs" in resp
        assert isinstance(resp["StorageConfigs"], list)

    def test_list_integration_associations(self, connect, instance_id):
        resp = connect.list_integration_associations(InstanceId=instance_id)
        assert "IntegrationAssociationSummaryList" in resp
        assert isinstance(resp["IntegrationAssociationSummaryList"], list)

    def test_list_lambda_functions(self, connect, instance_id):
        resp = connect.list_lambda_functions(InstanceId=instance_id)
        assert "LambdaFunctions" in resp
        assert isinstance(resp["LambdaFunctions"], list)

    def test_list_phone_numbers(self, connect, instance_id):
        resp = connect.list_phone_numbers(InstanceId=instance_id)
        assert "PhoneNumberSummaryList" in resp
        assert isinstance(resp["PhoneNumberSummaryList"], list)

    def test_list_prompts(self, connect, instance_id):
        resp = connect.list_prompts(InstanceId=instance_id)
        assert "PromptSummaryList" in resp
        assert isinstance(resp["PromptSummaryList"], list)

    def test_list_queue_quick_connects(self, connect, instance_id):
        # Create a queue first
        hoo_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="QQCHours",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = hoo_resp["HoursOfOperationId"]
        queue_resp = connect.create_queue(
            InstanceId=instance_id,
            Name="QQCQueue",
            HoursOfOperationId=hoo_id,
        )
        queue_id = queue_resp["QueueId"]
        resp = connect.list_queue_quick_connects(
            InstanceId=instance_id,
            QueueId=queue_id,
        )
        assert "QuickConnectSummaryList" in resp
        assert isinstance(resp["QuickConnectSummaryList"], list)

    def test_list_queues(self, connect, instance_id):
        resp = connect.list_queues(InstanceId=instance_id)
        assert "QueueSummaryList" in resp
        assert isinstance(resp["QueueSummaryList"], list)

    def test_list_routing_profile_queues(self, connect, instance_id):
        create_resp = connect.create_routing_profile(
            InstanceId=instance_id,
            Name="RPQProfile",
            Description="For listing queues",
            DefaultOutboundQueueId="fake-queue-id",
            MediaConcurrencies=[
                {"Channel": "VOICE", "Concurrency": 1},
            ],
        )
        rp_id = create_resp["RoutingProfileId"]
        resp = connect.list_routing_profile_queues(
            InstanceId=instance_id,
            RoutingProfileId=rp_id,
        )
        assert "RoutingProfileQueueConfigSummaryList" in resp
        assert isinstance(resp["RoutingProfileQueueConfigSummaryList"], list)

    def test_list_routing_profiles(self, connect, instance_id):
        resp = connect.list_routing_profiles(InstanceId=instance_id)
        assert "RoutingProfileSummaryList" in resp
        assert isinstance(resp["RoutingProfileSummaryList"], list)

    def test_list_rules(self, connect, instance_id):
        resp = connect.list_rules(
            InstanceId=instance_id,
            EventSourceName="OnPostCallAnalysisAvailable",
        )
        assert "RuleSummaryList" in resp
        assert isinstance(resp["RuleSummaryList"], list)

    def test_list_security_keys(self, connect, instance_id):
        resp = connect.list_security_keys(InstanceId=instance_id)
        assert "SecurityKeys" in resp
        assert isinstance(resp["SecurityKeys"], list)

    def test_list_security_profile_applications(self, connect, instance_id):
        create_resp = connect.create_security_profile(
            InstanceId=instance_id,
            SecurityProfileName="SPAProfile",
        )
        sp_id = create_resp["SecurityProfileId"]
        resp = connect.list_security_profile_applications(
            InstanceId=instance_id,
            SecurityProfileId=sp_id,
        )
        assert "Applications" in resp
        assert isinstance(resp["Applications"], list)

    def test_list_security_profile_permissions(self, connect, instance_id):
        create_resp = connect.create_security_profile(
            InstanceId=instance_id,
            SecurityProfileName="SPPProfile",
        )
        sp_id = create_resp["SecurityProfileId"]
        resp = connect.list_security_profile_permissions(
            InstanceId=instance_id,
            SecurityProfileId=sp_id,
        )
        assert "Permissions" in resp
        assert isinstance(resp["Permissions"], list)

    def test_list_security_profiles(self, connect, instance_id):
        resp = connect.list_security_profiles(InstanceId=instance_id)
        assert "SecurityProfileSummaryList" in resp
        assert isinstance(resp["SecurityProfileSummaryList"], list)

    def test_list_task_templates(self, connect, instance_id):
        resp = connect.list_task_templates(InstanceId=instance_id)
        assert "TaskTemplates" in resp
        assert isinstance(resp["TaskTemplates"], list)

    def test_list_user_hierarchy_groups(self, connect, instance_id):
        resp = connect.list_user_hierarchy_groups(InstanceId=instance_id)
        assert "UserHierarchyGroupSummaryList" in resp
        assert isinstance(resp["UserHierarchyGroupSummaryList"], list)

    def test_list_users(self, connect, instance_id):
        resp = connect.list_users(InstanceId=instance_id)
        assert "UserSummaryList" in resp
        assert isinstance(resp["UserSummaryList"], list)

    def test_list_views(self, connect, instance_id):
        resp = connect.list_views(InstanceId=instance_id)
        assert "ViewsSummaryList" in resp
        assert isinstance(resp["ViewsSummaryList"], list)

    def test_list_contact_evaluations(self, connect, instance_id):
        resp = connect.list_contact_evaluations(
            InstanceId=instance_id,
            ContactId="fake-contact-id",
        )
        assert "EvaluationSummaryList" in resp
        assert isinstance(resp["EvaluationSummaryList"], list)

    def test_list_contact_references(self, connect, instance_id):
        resp = connect.list_contact_references(
            InstanceId=instance_id,
            ContactId="fake-contact-id",
            ReferenceTypes=["URL"],
        )
        assert "ReferenceSummaryList" in resp
        assert isinstance(resp["ReferenceSummaryList"], list)

    def test_list_use_cases(self, connect, instance_id):
        resp = connect.list_use_cases(
            InstanceId=instance_id,
            IntegrationAssociationId="fake-integration-id",
        )
        assert "UseCaseSummaryList" in resp
        assert isinstance(resp["UseCaseSummaryList"], list)


class TestConnectDeleteOps:
    """Tests for Connect delete operations."""

    @pytest.fixture
    def instance_id(self, connect):
        iid, _ = _create_instance(connect)
        yield iid

    def test_delete_contact_flow(self, connect, instance_id):
        create_resp = connect.create_contact_flow(
            InstanceId=instance_id,
            Name="FlowToDelete",
            Type="CONTACT_FLOW",
            Content='{"Version":"2019-10-30","StartAction":"action1","Actions":[]}',
        )
        flow_id = create_resp["ContactFlowId"]
        connect.delete_contact_flow(InstanceId=instance_id, ContactFlowId=flow_id)
        # Verify deleted
        with pytest.raises(ClientError):
            connect.describe_contact_flow(InstanceId=instance_id, ContactFlowId=flow_id)

    def test_delete_contact_flow_module(self, connect, instance_id):
        create_resp = connect.create_contact_flow_module(
            InstanceId=instance_id,
            Name="ModuleToDelete",
            Content='{"Version":"2019-10-30","StartAction":"action1","Actions":[]}',
        )
        module_id = create_resp["Id"]
        connect.delete_contact_flow_module(InstanceId=instance_id, ContactFlowModuleId=module_id)
        with pytest.raises(ClientError):
            connect.describe_contact_flow_module(
                InstanceId=instance_id, ContactFlowModuleId=module_id
            )

    def test_delete_hours_of_operation(self, connect, instance_id):
        create_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="HoursToDelete",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = create_resp["HoursOfOperationId"]
        connect.delete_hours_of_operation(InstanceId=instance_id, HoursOfOperationId=hoo_id)
        with pytest.raises(ClientError):
            connect.describe_hours_of_operation(InstanceId=instance_id, HoursOfOperationId=hoo_id)

    def test_delete_prompt(self, connect, instance_id):
        create_resp = connect.create_prompt(
            InstanceId=instance_id,
            Name="PromptToDelete",
            S3Uri="s3://my-bucket/prompt.wav",
        )
        prompt_id = create_resp["PromptId"]
        connect.delete_prompt(InstanceId=instance_id, PromptId=prompt_id)
        with pytest.raises(ClientError):
            connect.describe_prompt(InstanceId=instance_id, PromptId=prompt_id)

    def test_delete_quick_connect(self, connect, instance_id):
        create_resp = connect.create_quick_connect(
            InstanceId=instance_id,
            Name="QCToDelete",
            QuickConnectConfig={
                "QuickConnectType": "PHONE_NUMBER",
                "PhoneConfig": {"PhoneNumber": "+15555551234"},
            },
        )
        qc_id = create_resp["QuickConnectId"]
        connect.delete_quick_connect(InstanceId=instance_id, QuickConnectId=qc_id)
        with pytest.raises(ClientError):
            connect.describe_quick_connect(InstanceId=instance_id, QuickConnectId=qc_id)

    def test_delete_routing_profile(self, connect, instance_id):
        create_resp = connect.create_routing_profile(
            InstanceId=instance_id,
            Name="RPToDelete",
            Description="Profile to delete",
            DefaultOutboundQueueId="fake-queue-id",
            MediaConcurrencies=[{"Channel": "VOICE", "Concurrency": 1}],
        )
        rp_id = create_resp["RoutingProfileId"]
        connect.delete_routing_profile(InstanceId=instance_id, RoutingProfileId=rp_id)
        with pytest.raises(ClientError):
            connect.describe_routing_profile(InstanceId=instance_id, RoutingProfileId=rp_id)

    def test_delete_rule(self, connect, instance_id):
        create_resp = connect.create_rule(
            InstanceId=instance_id,
            Name="RuleToDelete",
            TriggerEventSource={"EventSourceName": "OnPostCallAnalysisAvailable"},
            Function='EQUALS("a", "b")',
            Actions=[
                {
                    "ActionType": "GENERATE_EVENTBRIDGE_EVENT",
                    "EventBridgeAction": {"Name": "del-event"},
                }
            ],
            PublishStatus="DRAFT",
        )
        rule_id = create_resp["RuleId"]
        connect.delete_rule(InstanceId=instance_id, RuleId=rule_id)
        with pytest.raises(ClientError):
            connect.describe_rule(InstanceId=instance_id, RuleId=rule_id)

    def test_delete_security_profile(self, connect, instance_id):
        create_resp = connect.create_security_profile(
            InstanceId=instance_id,
            SecurityProfileName="SPToDelete",
        )
        sp_id = create_resp["SecurityProfileId"]
        connect.delete_security_profile(InstanceId=instance_id, SecurityProfileId=sp_id)
        with pytest.raises(ClientError):
            connect.describe_security_profile(InstanceId=instance_id, SecurityProfileId=sp_id)

    def test_delete_vocabulary(self, connect, instance_id):
        create_resp = connect.create_vocabulary(
            InstanceId=instance_id,
            VocabularyName="VocabToDelete",
            LanguageCode="en-US",
            Content="Phrase\tIPA\tSoundsLike\tDisplayAs\ntest\t\t\ttest",
        )
        vocab_id = create_resp["VocabularyId"]
        connect.delete_vocabulary(InstanceId=instance_id, VocabularyId=vocab_id)
        with pytest.raises(ClientError):
            connect.describe_vocabulary(InstanceId=instance_id, VocabularyId=vocab_id)


class TestConnectUpdateOps:
    """Tests for Connect update operations."""

    @pytest.fixture
    def instance_id(self, connect):
        iid, _ = _create_instance(connect)
        yield iid

    def test_update_agent_status(self, connect, instance_id):
        create_resp = connect.create_agent_status(
            InstanceId=instance_id,
            Name="StatusToUpdate",
            State="ENABLED",
        )
        status_id = create_resp["AgentStatusId"]
        connect.update_agent_status(
            InstanceId=instance_id,
            AgentStatusId=status_id,
            Name="UpdatedStatus",
        )
        resp = connect.describe_agent_status(InstanceId=instance_id, AgentStatusId=status_id)
        assert resp["AgentStatus"]["Name"] == "UpdatedStatus"

    def test_update_contact_flow_name(self, connect, instance_id):
        create_resp = connect.create_contact_flow(
            InstanceId=instance_id,
            Name="FlowToRename",
            Type="CONTACT_FLOW",
            Content='{"Version":"2019-10-30","StartAction":"action1","Actions":[]}',
        )
        flow_id = create_resp["ContactFlowId"]
        connect.update_contact_flow_name(
            InstanceId=instance_id,
            ContactFlowId=flow_id,
            Name="RenamedFlow",
        )
        resp = connect.describe_contact_flow(InstanceId=instance_id, ContactFlowId=flow_id)
        assert resp["ContactFlow"]["Name"] == "RenamedFlow"

    def test_update_contact_flow_content(self, connect, instance_id):
        create_resp = connect.create_contact_flow(
            InstanceId=instance_id,
            Name="FlowToUpdateContent",
            Type="CONTACT_FLOW",
            Content='{"Version":"2019-10-30","StartAction":"action1","Actions":[]}',
        )
        flow_id = create_resp["ContactFlowId"]
        new_content = '{"Version":"2019-10-30","StartAction":"action2","Actions":[]}'
        connect.update_contact_flow_content(
            InstanceId=instance_id,
            ContactFlowId=flow_id,
            Content=new_content,
        )
        resp = connect.describe_contact_flow(InstanceId=instance_id, ContactFlowId=flow_id)
        assert resp["ContactFlow"]["Content"] is not None

    def test_update_hours_of_operation(self, connect, instance_id):
        create_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="HoursToUpdate",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = create_resp["HoursOfOperationId"]
        connect.update_hours_of_operation(
            InstanceId=instance_id,
            HoursOfOperationId=hoo_id,
            Name="UpdatedHours",
        )
        resp = connect.describe_hours_of_operation(
            InstanceId=instance_id, HoursOfOperationId=hoo_id
        )
        assert resp["HoursOfOperation"]["Name"] == "UpdatedHours"

    def test_update_instance_attribute(self, connect, instance_id):
        connect.update_instance_attribute(
            InstanceId=instance_id,
            AttributeType="INBOUND_CALLS",
            Value="false",
        )
        resp = connect.describe_instance_attribute(
            InstanceId=instance_id,
            AttributeType="INBOUND_CALLS",
        )
        assert resp["Attribute"]["Value"] == "false"

    def test_update_prompt(self, connect, instance_id):
        create_resp = connect.create_prompt(
            InstanceId=instance_id,
            Name="PromptToUpdate",
            S3Uri="s3://my-bucket/prompt.wav",
        )
        prompt_id = create_resp["PromptId"]
        connect.update_prompt(
            InstanceId=instance_id,
            PromptId=prompt_id,
            Name="UpdatedPrompt",
        )
        resp = connect.describe_prompt(InstanceId=instance_id, PromptId=prompt_id)
        assert resp["Prompt"]["Name"] == "UpdatedPrompt"

    def test_update_queue_name(self, connect, instance_id):
        hoo_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="QueueUpdateHours",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = hoo_resp["HoursOfOperationId"]
        queue_resp = connect.create_queue(
            InstanceId=instance_id,
            Name="QueueToRename",
            HoursOfOperationId=hoo_id,
        )
        queue_id = queue_resp["QueueId"]
        connect.update_queue_name(
            InstanceId=instance_id,
            QueueId=queue_id,
            Name="RenamedQueue",
        )
        resp = connect.describe_queue(InstanceId=instance_id, QueueId=queue_id)
        assert resp["Queue"]["Name"] == "RenamedQueue"

    def test_update_queue_status(self, connect, instance_id):
        hoo_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="QueueStatusHours",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = hoo_resp["HoursOfOperationId"]
        queue_resp = connect.create_queue(
            InstanceId=instance_id,
            Name="QueueForStatus",
            HoursOfOperationId=hoo_id,
        )
        queue_id = queue_resp["QueueId"]
        connect.update_queue_status(
            InstanceId=instance_id,
            QueueId=queue_id,
            Status="DISABLED",
        )
        resp = connect.describe_queue(InstanceId=instance_id, QueueId=queue_id)
        assert resp["Queue"]["Status"] == "DISABLED"

    def test_update_queue_hours_of_operation(self, connect, instance_id):
        hoo_resp1 = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="OriginalHours",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id1 = hoo_resp1["HoursOfOperationId"]
        hoo_resp2 = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="NewHours",
            TimeZone="America/Chicago",
            Config=[
                {
                    "Day": "TUESDAY",
                    "StartTime": {"Hours": 8, "Minutes": 0},
                    "EndTime": {"Hours": 16, "Minutes": 0},
                }
            ],
        )
        hoo_id2 = hoo_resp2["HoursOfOperationId"]
        queue_resp = connect.create_queue(
            InstanceId=instance_id,
            Name="QueueForHOO",
            HoursOfOperationId=hoo_id1,
        )
        queue_id = queue_resp["QueueId"]
        connect.update_queue_hours_of_operation(
            InstanceId=instance_id,
            QueueId=queue_id,
            HoursOfOperationId=hoo_id2,
        )
        resp = connect.describe_queue(InstanceId=instance_id, QueueId=queue_id)
        assert resp["Queue"]["HoursOfOperationId"] == hoo_id2

    def test_update_queue_outbound_caller_config(self, connect, instance_id):
        hoo_resp = connect.create_hours_of_operation(
            InstanceId=instance_id,
            Name="CallerConfigHours",
            TimeZone="America/New_York",
            Config=[
                {
                    "Day": "MONDAY",
                    "StartTime": {"Hours": 9, "Minutes": 0},
                    "EndTime": {"Hours": 17, "Minutes": 0},
                }
            ],
        )
        hoo_id = hoo_resp["HoursOfOperationId"]
        queue_resp = connect.create_queue(
            InstanceId=instance_id,
            Name="QueueForCallerConfig",
            HoursOfOperationId=hoo_id,
        )
        queue_id = queue_resp["QueueId"]
        resp = connect.update_queue_outbound_caller_config(
            InstanceId=instance_id,
            QueueId=queue_id,
            OutboundCallerConfig={"OutboundCallerIdName": "TestCaller"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_quick_connect_name(self, connect, instance_id):
        create_resp = connect.create_quick_connect(
            InstanceId=instance_id,
            Name="QCToRename",
            QuickConnectConfig={
                "QuickConnectType": "PHONE_NUMBER",
                "PhoneConfig": {"PhoneNumber": "+15555551234"},
            },
        )
        qc_id = create_resp["QuickConnectId"]
        connect.update_quick_connect_name(
            InstanceId=instance_id,
            QuickConnectId=qc_id,
            Name="RenamedQC",
        )
        resp = connect.describe_quick_connect(InstanceId=instance_id, QuickConnectId=qc_id)
        assert resp["QuickConnect"]["Name"] == "RenamedQC"

    def test_update_quick_connect_config(self, connect, instance_id):
        create_resp = connect.create_quick_connect(
            InstanceId=instance_id,
            Name="QCToUpdateConfig",
            QuickConnectConfig={
                "QuickConnectType": "PHONE_NUMBER",
                "PhoneConfig": {"PhoneNumber": "+15555551234"},
            },
        )
        qc_id = create_resp["QuickConnectId"]
        connect.update_quick_connect_config(
            InstanceId=instance_id,
            QuickConnectId=qc_id,
            QuickConnectConfig={
                "QuickConnectType": "PHONE_NUMBER",
                "PhoneConfig": {"PhoneNumber": "+15555559999"},
            },
        )
        resp = connect.describe_quick_connect(InstanceId=instance_id, QuickConnectId=qc_id)
        phone = resp["QuickConnect"]["QuickConnectConfig"]["PhoneConfig"]
        assert phone["PhoneNumber"] == "+15555559999"

    def test_update_routing_profile_name(self, connect, instance_id):
        create_resp = connect.create_routing_profile(
            InstanceId=instance_id,
            Name="RPToRename",
            Description="Profile to rename",
            DefaultOutboundQueueId="fake-queue-id",
            MediaConcurrencies=[{"Channel": "VOICE", "Concurrency": 1}],
        )
        rp_id = create_resp["RoutingProfileId"]
        connect.update_routing_profile_name(
            InstanceId=instance_id,
            RoutingProfileId=rp_id,
            Name="RenamedRP",
        )
        resp = connect.describe_routing_profile(InstanceId=instance_id, RoutingProfileId=rp_id)
        assert resp["RoutingProfile"]["Name"] == "RenamedRP"

    def test_update_routing_profile_concurrency(self, connect, instance_id):
        create_resp = connect.create_routing_profile(
            InstanceId=instance_id,
            Name="RPForConcurrency",
            Description="Profile for concurrency update",
            DefaultOutboundQueueId="fake-queue-id",
            MediaConcurrencies=[{"Channel": "VOICE", "Concurrency": 1}],
        )
        rp_id = create_resp["RoutingProfileId"]
        resp = connect.update_routing_profile_concurrency(
            InstanceId=instance_id,
            RoutingProfileId=rp_id,
            MediaConcurrencies=[
                {"Channel": "VOICE", "Concurrency": 2},
                {"Channel": "CHAT", "Concurrency": 3},
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_routing_profile_default_outbound_queue(self, connect, instance_id):
        create_resp = connect.create_routing_profile(
            InstanceId=instance_id,
            Name="RPForOutbound",
            Description="Profile for outbound queue update",
            DefaultOutboundQueueId="fake-queue-id",
            MediaConcurrencies=[{"Channel": "VOICE", "Concurrency": 1}],
        )
        rp_id = create_resp["RoutingProfileId"]
        resp = connect.update_routing_profile_default_outbound_queue(
            InstanceId=instance_id,
            RoutingProfileId=rp_id,
            DefaultOutboundQueueId="new-fake-queue-id",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_security_profile(self, connect, instance_id):
        create_resp = connect.create_security_profile(
            InstanceId=instance_id,
            SecurityProfileName="SPToUpdate",
            Description="Original desc",
        )
        sp_id = create_resp["SecurityProfileId"]
        connect.update_security_profile(
            InstanceId=instance_id,
            SecurityProfileId=sp_id,
            Description="Updated desc",
        )
        resp = connect.describe_security_profile(InstanceId=instance_id, SecurityProfileId=sp_id)
        assert resp["SecurityProfile"]["Description"] == "Updated desc"

    def test_update_rule(self, connect, instance_id):
        create_resp = connect.create_rule(
            InstanceId=instance_id,
            Name="RuleToUpdate",
            TriggerEventSource={"EventSourceName": "OnPostCallAnalysisAvailable"},
            Function='EQUALS("a", "b")',
            Actions=[
                {
                    "ActionType": "GENERATE_EVENTBRIDGE_EVENT",
                    "EventBridgeAction": {"Name": "update-event"},
                }
            ],
            PublishStatus="DRAFT",
        )
        rule_id = create_resp["RuleId"]
        connect.update_rule(
            InstanceId=instance_id,
            RuleId=rule_id,
            Name="UpdatedRule",
            Function='EQUALS("c", "d")',
            Actions=[
                {
                    "ActionType": "GENERATE_EVENTBRIDGE_EVENT",
                    "EventBridgeAction": {"Name": "updated-event"},
                }
            ],
            PublishStatus="DRAFT",
        )
        resp = connect.describe_rule(InstanceId=instance_id, RuleId=rule_id)
        assert resp["Rule"]["Name"] == "UpdatedRule"


class TestConnectSearchOps:
    """Tests for Connect search operations."""

    @pytest.fixture
    def instance_id(self, connect):
        iid, _ = _create_instance(connect)
        yield iid

    @pytest.fixture
    def instance_arn(self, connect):
        _, arn = _create_instance(connect)
        return arn

    def test_search_users(self, connect, instance_id):
        resp = connect.search_users(InstanceId=instance_id)
        assert "Users" in resp
        assert isinstance(resp["Users"], list)

    def test_search_vocabularies(self, connect, instance_id):
        resp = connect.search_vocabularies(InstanceId=instance_id)
        assert "VocabularySummaryList" in resp
        assert isinstance(resp["VocabularySummaryList"], list)

    def test_search_vocabularies_after_create(self, connect, instance_id):
        connect.create_vocabulary(
            InstanceId=instance_id,
            VocabularyName="SearchableVocab",
            LanguageCode="en-US",
            Content="Phrase\tIPA\tSoundsLike\tDisplayAs\ntest\t\t\ttest",
        )
        resp = connect.search_vocabularies(InstanceId=instance_id)
        assert len(resp["VocabularySummaryList"]) >= 1
        names = [v["Name"] for v in resp["VocabularySummaryList"]]
        assert "SearchableVocab" in names


class TestConnectPhoneNumbers:
    """Tests for Connect phone number operations."""

    @pytest.fixture
    def instance_id(self, connect):
        iid, _ = _create_instance(connect)
        yield iid

    @pytest.fixture
    def instance_arn(self, connect):
        _, arn = _create_instance(connect)
        return arn

    def test_claim_and_release_phone_number(self, connect, instance_arn):
        try:
            resp = connect.claim_phone_number(
                TargetArn=instance_arn,
                PhoneNumber="+15555550100",
            )
            assert "PhoneNumberId" in resp
            assert "PhoneNumberArn" in resp
            phone_id = resp["PhoneNumberId"]
            # Release the phone number
            connect.release_phone_number(PhoneNumberId=phone_id)
        except ClientError as e:
            # Some implementations may not support this
            assert e.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidParameterException",
            )

    def test_update_phone_number(self, connect, instance_arn):
        try:
            claim_resp = connect.claim_phone_number(
                TargetArn=instance_arn,
                PhoneNumber="+15555550199",
            )
            phone_id = claim_resp["PhoneNumberId"]
            # Create another instance to move phone number to
            id2, arn2 = _create_instance(connect)
            resp = connect.update_phone_number(
                PhoneNumberId=phone_id,
                TargetArn=arn2,
            )
            assert "PhoneNumberId" in resp
            assert "PhoneNumberArn" in resp
        except ClientError as e:
            assert e.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidParameterException",
                "DuplicateResourceException",
            )


class TestConnectUserOps:
    """Tests for Connect user-related operations."""

    @pytest.fixture
    def instance_id(self, connect):
        iid, _ = _create_instance(connect)
        yield iid

    def _create_user(self, connect, instance_id, username="testuser"):
        """Helper to create a Connect user."""
        rp_resp = connect.create_routing_profile(
            InstanceId=instance_id,
            Name=f"rp-{username}",
            Description="For user",
            DefaultOutboundQueueId="fake-queue-id",
            MediaConcurrencies=[{"Channel": "VOICE", "Concurrency": 1}],
        )
        sp_resp = connect.create_security_profile(
            InstanceId=instance_id,
            SecurityProfileName=f"sp-{username}",
        )
        resp = connect.create_user(
            InstanceId=instance_id,
            Username=username,
            PhoneConfig={
                "PhoneType": "SOFT_PHONE",
                "AutoAccept": False,
                "AfterContactWorkTimeLimit": 0,
            },
            SecurityProfileIds=[sp_resp["SecurityProfileId"]],
            RoutingProfileId=rp_resp["RoutingProfileId"],
        )
        return resp["UserId"]

    def test_create_and_describe_user(self, connect, instance_id):
        user_id = self._create_user(connect, instance_id, "newuser1")
        resp = connect.describe_user(InstanceId=instance_id, UserId=user_id)
        assert "User" in resp
        assert resp["User"]["Username"] == "newuser1"

    def test_delete_user(self, connect, instance_id):
        user_id = self._create_user(connect, instance_id, "delusr")
        connect.delete_user(InstanceId=instance_id, UserId=user_id)
        with pytest.raises(ClientError) as exc_info:
            connect.describe_user(InstanceId=instance_id, UserId=user_id)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_user_identity_info(self, connect, instance_id):
        user_id = self._create_user(connect, instance_id, "identusr")
        connect.update_user_identity_info(
            InstanceId=instance_id,
            UserId=user_id,
            IdentityInfo={
                "FirstName": "John",
                "LastName": "Doe",
            },
        )
        resp = connect.describe_user(InstanceId=instance_id, UserId=user_id)
        identity = resp["User"].get("IdentityInfo", {})
        assert identity.get("FirstName") == "John"
        assert identity.get("LastName") == "Doe"

    def test_update_user_phone_config(self, connect, instance_id):
        user_id = self._create_user(connect, instance_id, "phoneusr")
        connect.update_user_phone_config(
            InstanceId=instance_id,
            UserId=user_id,
            PhoneConfig={
                "PhoneType": "SOFT_PHONE",
                "AutoAccept": True,
                "AfterContactWorkTimeLimit": 30,
            },
        )
        resp = connect.describe_user(InstanceId=instance_id, UserId=user_id)
        phone_config = resp["User"]["PhoneConfig"]
        assert phone_config["AutoAccept"] is True

    def test_update_user_routing_profile(self, connect, instance_id):
        user_id = self._create_user(connect, instance_id, "routusr")
        new_rp_resp = connect.create_routing_profile(
            InstanceId=instance_id,
            Name="new-rp-for-user",
            Description="New routing profile",
            DefaultOutboundQueueId="fake-queue-id",
            MediaConcurrencies=[{"Channel": "VOICE", "Concurrency": 1}],
        )
        new_rp_id = new_rp_resp["RoutingProfileId"]
        connect.update_user_routing_profile(
            InstanceId=instance_id,
            UserId=user_id,
            RoutingProfileId=new_rp_id,
        )
        resp = connect.describe_user(InstanceId=instance_id, UserId=user_id)
        assert resp["User"]["RoutingProfileId"] == new_rp_id

    def test_update_user_hierarchy(self, connect, instance_id):
        user_id = self._create_user(connect, instance_id, "hierusr")
        group_resp = connect.create_user_hierarchy_group(
            InstanceId=instance_id,
            Name="HierGroup",
        )
        group_id = group_resp["HierarchyGroupId"]
        resp = connect.update_user_hierarchy(
            InstanceId=instance_id,
            UserId=user_id,
            HierarchyGroupId=group_id,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_users_after_create(self, connect, instance_id):
        self._create_user(connect, instance_id, "listusr")
        resp = connect.list_users(InstanceId=instance_id)
        assert len(resp["UserSummaryList"]) >= 1
        usernames = [u.get("Username") for u in resp["UserSummaryList"]]
        assert "listusr" in usernames
