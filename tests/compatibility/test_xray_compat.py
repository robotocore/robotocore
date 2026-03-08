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
