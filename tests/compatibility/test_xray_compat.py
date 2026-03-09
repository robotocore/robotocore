"""X-Ray compatibility tests."""

import time
import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def xray():
    return make_client("xray")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestXRaySamplingRuleOperations:
    def test_create_and_get_sampling_rules(self, xray):
        rule_name = _unique("rule")
        resp = xray.create_sampling_rule(
            SamplingRule={
                "RuleName": rule_name,
                "ResourceARN": "*",
                "Priority": 1000,
                "FixedRate": 0.05,
                "ReservoirSize": 1,
                "ServiceName": "*",
                "ServiceType": "*",
                "Host": "*",
                "HTTPMethod": "*",
                "URLPath": "*",
                "Version": 1,
            }
        )
        created = resp["SamplingRuleRecord"]
        assert created["SamplingRule"]["RuleName"] == rule_name

        # GetSamplingRules should include our rule
        rules_resp = xray.get_sampling_rules()
        rule_names = [r["SamplingRule"]["RuleName"] for r in rules_resp["SamplingRuleRecords"]]
        assert rule_name in rule_names

        # Cleanup
        xray.delete_sampling_rule(RuleName=rule_name)

    def test_delete_sampling_rule(self, xray):
        rule_name = _unique("rule")
        xray.create_sampling_rule(
            SamplingRule={
                "RuleName": rule_name,
                "ResourceARN": "*",
                "Priority": 1001,
                "FixedRate": 0.1,
                "ReservoirSize": 2,
                "ServiceName": "*",
                "ServiceType": "*",
                "Host": "*",
                "HTTPMethod": "*",
                "URLPath": "*",
                "Version": 1,
            }
        )
        del_resp = xray.delete_sampling_rule(RuleName=rule_name)
        assert del_resp["SamplingRuleRecord"]["SamplingRule"]["RuleName"] == rule_name

    def test_get_sampling_statistic_summaries(self, xray):
        resp = xray.get_sampling_statistic_summaries()
        assert "SamplingStatisticSummaries" in resp


class TestXRayGroupOperations:
    def test_create_and_get_group(self, xray):
        group_name = _unique("group")
        resp = xray.create_group(GroupName=group_name)
        group = resp["Group"]
        assert group["GroupName"] == group_name
        group_arn = group["GroupARN"]

        # GetGroup by name
        get_resp = xray.get_group(GroupName=group_name)
        assert get_resp["Group"]["GroupName"] == group_name

        # Cleanup
        xray.delete_group(GroupARN=group_arn)

    def test_get_groups(self, xray):
        group_name = _unique("group")
        resp = xray.create_group(GroupName=group_name)
        group_arn = resp["Group"]["GroupARN"]

        groups_resp = xray.get_groups()
        group_names = [g["GroupName"] for g in groups_resp["Groups"]]
        assert group_name in group_names

        # Cleanup
        xray.delete_group(GroupARN=group_arn)

    def test_delete_group(self, xray):
        group_name = _unique("group")
        resp = xray.create_group(GroupName=group_name)
        group_arn = resp["Group"]["GroupARN"]

        xray.delete_group(GroupARN=group_arn)

        # Verify deleted
        groups_resp = xray.get_groups()
        group_names = [g["GroupName"] for g in groups_resp["Groups"]]
        assert group_name not in group_names

    def test_tag_and_untag_group(self, xray):
        group_name = _unique("group")
        resp = xray.create_group(GroupName=group_name)
        group_arn = resp["Group"]["GroupARN"]

        # Tag
        xray.tag_resource(
            ResourceARN=group_arn,
            Tags=[{"Key": "env", "Value": "test"}],
        )

        # List tags
        tags_resp = xray.list_tags_for_resource(ResourceARN=group_arn)
        tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert tags["env"] == "test"

        # Untag
        xray.untag_resource(ResourceARN=group_arn, TagKeys=["env"])
        tags_resp = xray.list_tags_for_resource(ResourceARN=group_arn)
        tag_keys = [t["Key"] for t in tags_resp["Tags"]]
        assert "env" not in tag_keys

        # Cleanup
        xray.delete_group(GroupARN=group_arn)


class TestXRayEncryptionConfig:
    def test_get_encryption_config(self, xray):
        resp = xray.get_encryption_config()
        config = resp["EncryptionConfig"]
        assert "Type" in config
        assert config["Status"] in ("UPDATING", "ACTIVE")

    def test_put_encryption_config(self, xray):
        resp = xray.put_encryption_config(Type="NONE")
        config = resp["EncryptionConfig"]
        assert config["Type"] == "NONE"


def _make_trace_id():
    """Generate a valid X-Ray trace ID: 1-<hex_time>-<24_hex_chars>."""
    hex_time = format(int(time.time()), "08x")
    suffix = uuid.uuid4().hex[:24]
    return f"1-{hex_time}-{suffix}"


def _make_segment_doc(trace_id, name="test-segment"):
    """Create a minimal trace segment document JSON string."""
    import json

    seg_id = uuid.uuid4().hex[:16]
    now = time.time()
    return json.dumps(
        {
            "trace_id": trace_id,
            "id": seg_id,
            "name": name,
            "start_time": now - 1,
            "end_time": now,
        }
    )


class TestXRayTraceOperations:
    def test_put_trace_segments(self, xray):
        trace_id = _make_trace_id()
        resp = xray.put_trace_segments(TraceSegmentDocuments=[_make_segment_doc(trace_id)])
        assert "UnprocessedTraceSegments" in resp

    def test_batch_get_traces(self, xray):
        trace_id = _make_trace_id()
        xray.put_trace_segments(TraceSegmentDocuments=[_make_segment_doc(trace_id)])
        resp = xray.batch_get_traces(TraceIds=[trace_id])
        assert "Traces" in resp
        assert "UnprocessedTraceIds" in resp

    def test_get_trace_summaries(self, xray):
        now = time.time()
        resp = xray.get_trace_summaries(
            StartTime=now - 3600,
            EndTime=now,
        )
        assert "TraceSummaries" in resp

    def test_get_trace_graph(self, xray):
        trace_id = _make_trace_id()
        xray.put_trace_segments(TraceSegmentDocuments=[_make_segment_doc(trace_id)])
        resp = xray.get_trace_graph(TraceIds=[trace_id])
        assert "Services" in resp

    def test_get_service_graph(self, xray):
        now = time.time()
        resp = xray.get_service_graph(
            StartTime=now - 3600,
            EndTime=now,
        )
        assert "Services" in resp

    def test_put_telemetry_records(self, xray):
        now = time.time()
        resp = xray.put_telemetry_records(
            TelemetryRecords=[
                {
                    "Timestamp": now,
                    "SegmentsReceivedCount": 10,
                    "SegmentsSentCount": 10,
                    "SegmentsRejectedCount": 0,
                    "BackendConnectionErrors": {
                        "TimeoutCount": 0,
                        "ConnectionRefusedCount": 0,
                        "HTTPCode4XXCount": 0,
                        "HTTPCode5XXCount": 0,
                        "OtherCount": 0,
                    },
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_multiple_trace_segments(self, xray):
        """PutTraceSegments with multiple segments in one call."""
        trace_id = _make_trace_id()
        seg1 = _make_segment_doc(trace_id, name="seg-a")
        seg2 = _make_segment_doc(trace_id, name="seg-b")
        resp = xray.put_trace_segments(TraceSegmentDocuments=[seg1, seg2])
        assert "UnprocessedTraceSegments" in resp
        assert isinstance(resp["UnprocessedTraceSegments"], list)

    def test_get_trace_summaries_has_approximate_time(self, xray):
        """GetTraceSummaries response includes ApproximateTime."""
        now = time.time()
        resp = xray.get_trace_summaries(StartTime=now - 3600, EndTime=now)
        assert "TraceSummaries" in resp
        assert "ApproximateTime" in resp


class TestXRaySamplingRuleTagging:
    """Tests for tagging sampling rules."""

    def test_tag_and_list_tags_sampling_rule(self, xray):
        """Tag a sampling rule and verify via ListTagsForResource."""
        rule_name = _unique("trule")
        resp = xray.create_sampling_rule(
            SamplingRule={
                "RuleName": rule_name,
                "ResourceARN": "*",
                "Priority": 1002,
                "FixedRate": 0.05,
                "ReservoirSize": 1,
                "ServiceName": "*",
                "ServiceType": "*",
                "Host": "*",
                "HTTPMethod": "*",
                "URLPath": "*",
                "Version": 1,
            }
        )
        rule_arn = resp["SamplingRuleRecord"]["SamplingRule"]["RuleARN"]
        try:
            xray.tag_resource(
                ResourceARN=rule_arn,
                Tags=[{"Key": "team", "Value": "platform"}],
            )
            tags_resp = xray.list_tags_for_resource(ResourceARN=rule_arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["team"] == "platform"
        finally:
            xray.delete_sampling_rule(RuleName=rule_name)

    def test_untag_sampling_rule(self, xray):
        """Untag a sampling rule removes the tag."""
        rule_name = _unique("urule")
        resp = xray.create_sampling_rule(
            SamplingRule={
                "RuleName": rule_name,
                "ResourceARN": "*",
                "Priority": 1003,
                "FixedRate": 0.05,
                "ReservoirSize": 1,
                "ServiceName": "*",
                "ServiceType": "*",
                "Host": "*",
                "HTTPMethod": "*",
                "URLPath": "*",
                "Version": 1,
            }
        )
        rule_arn = resp["SamplingRuleRecord"]["SamplingRule"]["RuleARN"]
        try:
            xray.tag_resource(
                ResourceARN=rule_arn,
                Tags=[{"Key": "env", "Value": "dev"}, {"Key": "keep", "Value": "yes"}],
            )
            xray.untag_resource(ResourceARN=rule_arn, TagKeys=["env"])
            tags_resp = xray.list_tags_for_resource(ResourceARN=rule_arn)
            tag_keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "env" not in tag_keys
            assert "keep" in tag_keys
        finally:
            xray.delete_sampling_rule(RuleName=rule_name)


class TestXRayGroupWithFilter:
    """Tests for groups with filter expressions."""

    def test_create_group_with_filter(self, xray):
        """CreateGroup with FilterExpression stores it."""
        group_name = _unique("fgrp")
        resp = xray.create_group(
            GroupName=group_name,
            FilterExpression='service("test-svc")',
        )
        group = resp["Group"]
        assert group["GroupName"] == group_name
        assert "FilterExpression" in group
        group_arn = group["GroupARN"]

        # Verify via GetGroup
        get_resp = xray.get_group(GroupName=group_name)
        assert get_resp["Group"]["FilterExpression"] == 'service("test-svc")'

        xray.delete_group(GroupARN=group_arn)

    def test_get_group_by_arn(self, xray):
        """GetGroup can be called with GroupARN instead of GroupName."""
        group_name = _unique("garngrp")
        resp = xray.create_group(GroupName=group_name)
        group_arn = resp["Group"]["GroupARN"]

        get_resp = xray.get_group(GroupARN=group_arn)
        assert get_resp["Group"]["GroupName"] == group_name

        xray.delete_group(GroupARN=group_arn)


class TestXRayEncryptionConfigRoundTrip:
    """Tests for encryption config changes."""

    def test_put_then_get_encryption_config(self, xray):
        """PutEncryptionConfig NONE then GetEncryptionConfig matches."""
        xray.put_encryption_config(Type="NONE")
        resp = xray.get_encryption_config()
        assert resp["EncryptionConfig"]["Type"] == "NONE"


class TestXRaySamplingRuleDetails:
    """Tests for sampling rule structure and fields."""

    def test_sampling_rule_has_rule_arn(self, xray):
        """Created sampling rule has RuleARN field."""
        rule_name = _unique("arnrule")
        resp = xray.create_sampling_rule(
            SamplingRule={
                "RuleName": rule_name,
                "ResourceARN": "*",
                "Priority": 1004,
                "FixedRate": 0.02,
                "ReservoirSize": 3,
                "ServiceName": "*",
                "ServiceType": "*",
                "Host": "*",
                "HTTPMethod": "*",
                "URLPath": "*",
                "Version": 1,
            }
        )
        record = resp["SamplingRuleRecord"]
        assert "RuleARN" in record["SamplingRule"]
        assert record["SamplingRule"]["RuleName"] == rule_name
        assert record["SamplingRule"]["FixedRate"] == 0.02
        assert record["SamplingRule"]["ReservoirSize"] == 3

        xray.delete_sampling_rule(RuleName=rule_name)


class TestXRayResourcePolicyOperations:
    """Tests for PutResourcePolicy / ListResourcePolicies / DeleteResourcePolicy."""

    def test_put_and_list_resource_policy(self, xray):
        """PutResourcePolicy creates a policy, ListResourcePolicies returns it."""
        policy_name = _unique("pol")
        doc = '{"Version":"2012-10-17","Statement":[]}'
        put_resp = xray.put_resource_policy(PolicyName=policy_name, PolicyDocument=doc)
        policy = put_resp["ResourcePolicy"]
        assert policy["PolicyName"] == policy_name
        assert "PolicyRevisionId" in policy

        list_resp = xray.list_resource_policies()
        names = [p["PolicyName"] for p in list_resp["ResourcePolicies"]]
        assert policy_name in names

        # Cleanup
        xray.delete_resource_policy(PolicyName=policy_name)

    def test_delete_resource_policy_with_revision_id(self, xray):
        """DeleteResourcePolicy succeeds when given matching PolicyRevisionId."""
        policy_name = _unique("delpol")
        doc = '{"Version":"2012-10-17","Statement":[]}'
        put_resp = xray.put_resource_policy(PolicyName=policy_name, PolicyDocument=doc)
        rev_id = put_resp["ResourcePolicy"]["PolicyRevisionId"]

        del_resp = xray.delete_resource_policy(PolicyName=policy_name, PolicyRevisionId=rev_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify it's gone
        list_resp = xray.list_resource_policies()
        names = [p["PolicyName"] for p in list_resp["ResourcePolicies"]]
        assert policy_name not in names

    def test_delete_resource_policy_wrong_revision_raises(self, xray):
        """DeleteResourcePolicy with wrong revision ID raises error."""
        policy_name = _unique("badrev")
        doc = '{"Version":"2012-10-17","Statement":[]}'
        xray.put_resource_policy(PolicyName=policy_name, PolicyDocument=doc)

        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            xray.delete_resource_policy(PolicyName=policy_name, PolicyRevisionId="wrong-id")
        assert "InvalidPolicyRevisionId" in str(exc_info.value)

        # Cleanup
        xray.delete_resource_policy(PolicyName=policy_name)

    def test_put_resource_policy_update(self, xray):
        """PutResourcePolicy with same name updates the policy."""
        policy_name = _unique("uppol")
        doc1 = '{"Version":"2012-10-17","Statement":[]}'
        resp1 = xray.put_resource_policy(PolicyName=policy_name, PolicyDocument=doc1)
        rev1 = resp1["ResourcePolicy"]["PolicyRevisionId"]

        doc2 = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}'
        resp2 = xray.put_resource_policy(PolicyName=policy_name, PolicyDocument=doc2)
        rev2 = resp2["ResourcePolicy"]["PolicyRevisionId"]
        assert rev1 != rev2

        # Cleanup
        xray.delete_resource_policy(PolicyName=policy_name)


class TestXRayInsightSummaries:
    """Tests for GetInsightSummaries."""

    def test_get_insight_summaries_returns_empty_list(self, xray):
        """GetInsightSummaries returns empty list when no insights exist."""
        now = time.time()
        resp = xray.get_insight_summaries(StartTime=now - 3600, EndTime=now)
        assert "InsightSummaries" in resp
        assert isinstance(resp["InsightSummaries"], list)


class TestXRayGetSamplingTargets:
    """Tests for GetSamplingTargets."""

    def test_get_sampling_targets_returns_structure(self, xray):
        """GetSamplingTargets returns expected top-level keys."""
        resp = xray.get_sampling_targets(
            SamplingStatisticsDocuments=[
                {
                    "RuleName": "Default",
                    "ClientID": "abcdef0123456789abcdef01",
                    "Timestamp": time.time(),
                    "RequestCount": 100,
                    "SampledCount": 5,
                    "BorrowCount": 0,
                }
            ]
        )
        assert "SamplingTargetDocuments" in resp
        assert "LastRuleModification" in resp
        assert "UnprocessedStatistics" in resp
        assert isinstance(resp["SamplingTargetDocuments"], list)
        assert isinstance(resp["UnprocessedStatistics"], list)


class TestXRayTimeSeriesServiceStatistics:
    """Tests for GetTimeSeriesServiceStatistics."""

    def test_get_time_series_service_statistics_returns_empty(self, xray):
        """GetTimeSeriesServiceStatistics returns empty list with no data."""
        now = time.time()
        resp = xray.get_time_series_service_statistics(StartTime=now - 3600, EndTime=now)
        assert "TimeSeriesServiceStatistics" in resp
        assert isinstance(resp["TimeSeriesServiceStatistics"], list)


class TestXRayIndexingRules:
    """Tests for GetIndexingRules."""

    def test_get_indexing_rules(self, xray):
        """GetIndexingRules returns indexing rules list."""
        resp = xray.get_indexing_rules()
        assert "IndexingRules" in resp
        assert isinstance(resp["IndexingRules"], list)


class TestXRayTraceRetrieval:
    """Tests for trace retrieval operations."""

    def test_start_trace_retrieval(self, xray):
        """StartTraceRetrieval returns a retrieval token."""
        now = time.time()
        resp = xray.start_trace_retrieval(
            TraceIds=["1-12345678-abcdef012345678901234567"],
            StartTime=now - 3600,
            EndTime=now,
        )
        assert "RetrievalToken" in resp
        assert isinstance(resp["RetrievalToken"], str)
        assert len(resp["RetrievalToken"]) > 0

    def test_cancel_trace_retrieval(self, xray):
        """CancelTraceRetrieval succeeds for a started retrieval."""
        now = time.time()
        start_resp = xray.start_trace_retrieval(
            TraceIds=["1-12345678-abcdef012345678901234567"],
            StartTime=now - 3600,
            EndTime=now,
        )
        token = start_resp["RetrievalToken"]
        cancel_resp = xray.cancel_trace_retrieval(RetrievalToken=token)
        assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_retrieved_traces_graph(self, xray):
        """GetRetrievedTracesGraph returns services and retrieval status."""
        now = time.time()
        start_resp = xray.start_trace_retrieval(
            TraceIds=["1-12345678-abcdef012345678901234567"],
            StartTime=now - 3600,
            EndTime=now,
        )
        token = start_resp["RetrievalToken"]
        resp = xray.get_retrieved_traces_graph(RetrievalToken=token)
        assert "Services" in resp
        assert "RetrievalStatus" in resp
        assert isinstance(resp["Services"], list)

    def test_list_retrieved_traces(self, xray):
        """ListRetrievedTraces returns traces and retrieval status."""
        now = time.time()
        start_resp = xray.start_trace_retrieval(
            TraceIds=["1-12345678-abcdef012345678901234567"],
            StartTime=now - 3600,
            EndTime=now,
        )
        token = start_resp["RetrievalToken"]
        resp = xray.list_retrieved_traces(RetrievalToken=token)
        assert "Traces" in resp
        assert "RetrievalStatus" in resp
        assert "TraceFormat" in resp
        assert isinstance(resp["Traces"], list)


class TestXRayInsightOperations:
    """Tests for insight operations with nonexistent IDs."""

    def test_get_insight_nonexistent(self, xray):
        """GetInsight with nonexistent InsightId raises InvalidRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            xray.get_insight(InsightId="fake-insight-id-00000000")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_insight_events_nonexistent(self, xray):
        """GetInsightEvents with nonexistent InsightId raises InvalidRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            xray.get_insight_events(InsightId="fake-insight-id-00000000")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_insight_impact_graph_nonexistent(self, xray):
        """GetInsightImpactGraph with nonexistent InsightId raises InvalidRequestException."""
        from botocore.exceptions import ClientError

        now = time.time()
        with pytest.raises(ClientError) as exc:
            xray.get_insight_impact_graph(
                InsightId="fake-insight-id-00000000",
                StartTime=now - 3600,
                EndTime=now,
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"
