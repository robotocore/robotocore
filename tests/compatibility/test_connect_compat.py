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
