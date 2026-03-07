"""Kinesis Analytics v2 compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def kav2():
    return make_client("kinesisanalyticsv2")


def _unique_name():
    return f"test-kav2-{uuid.uuid4().hex[:8]}"


ROLE_ARN = "arn:aws:iam::123456789012:role/test"


class TestKinesisAnalyticsV2Operations:
    def test_list_applications_empty(self, kav2):
        """ListApplications returns ApplicationSummaries."""
        response = kav2.list_applications()
        assert "ApplicationSummaries" in response

    def test_create_application(self, kav2):
        """CreateApplication returns ApplicationDetail with expected fields."""
        name = _unique_name()
        response = kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
        )
        detail = response["ApplicationDetail"]
        assert "ApplicationARN" in detail
        assert detail["RuntimeEnvironment"] == "FLINK-1_18"
        assert detail["ServiceExecutionRole"] == ROLE_ARN
        assert "ApplicationStatus" in detail
        assert "ApplicationVersionId" in detail
        assert "CreateTimestamp" in detail

    def test_create_application_arn_format(self, kav2):
        """CreateApplication returns a properly formatted ARN."""
        name = _unique_name()
        response = kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
        )
        arn = response["ApplicationDetail"]["ApplicationARN"]
        assert arn.startswith("arn:aws:kinesisanalytics:")
        assert name in arn

    def test_describe_application(self, kav2):
        """DescribeApplication returns the same detail as CreateApplication."""
        name = _unique_name()
        create_resp = kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
        )
        create_detail = create_resp["ApplicationDetail"]

        describe_resp = kav2.describe_application(ApplicationName=name)
        describe_detail = describe_resp["ApplicationDetail"]

        assert describe_detail["ApplicationARN"] == create_detail["ApplicationARN"]
        assert describe_detail["RuntimeEnvironment"] == "FLINK-1_18"
        assert describe_detail["ServiceExecutionRole"] == ROLE_ARN
        assert describe_detail["ApplicationVersionId"] == create_detail["ApplicationVersionId"]

    def test_describe_application_fields(self, kav2):
        """DescribeApplication returns maintenance config and timestamps."""
        name = _unique_name()
        kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
        )
        detail = kav2.describe_application(ApplicationName=name)["ApplicationDetail"]
        assert "CreateTimestamp" in detail
        assert "LastUpdateTimestamp" in detail
        assert "ApplicationMaintenanceConfigurationDescription" in detail

    def test_list_applications_includes_created(self, kav2):
        """ListApplications includes a newly created application."""
        name = _unique_name()
        kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
        )
        response = kav2.list_applications()
        names = [s["ApplicationName"] for s in response["ApplicationSummaries"]]
        assert name in names

    def test_list_applications_summary_fields(self, kav2):
        """ListApplications summary has expected fields."""
        name = _unique_name()
        kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
        )
        response = kav2.list_applications()
        matching = [
            s for s in response["ApplicationSummaries"] if s["ApplicationName"] == name
        ]
        assert len(matching) == 1
        summary = matching[0]
        assert "ApplicationARN" in summary
        assert "ApplicationStatus" in summary
        assert "RuntimeEnvironment" in summary

    def test_tag_resource(self, kav2):
        """TagResource adds tags to an application."""
        name = _unique_name()
        resp = kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
        )
        arn = resp["ApplicationDetail"]["ApplicationARN"]

        kav2.tag_resource(
            ResourceARN=arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        tags_resp = kav2.list_tags_for_resource(ResourceARN=arn)
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "platform"

    def test_list_tags_for_resource_empty(self, kav2):
        """ListTagsForResource on an untagged app returns empty list."""
        name = _unique_name()
        resp = kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
        )
        arn = resp["ApplicationDetail"]["ApplicationARN"]
        tags_resp = kav2.list_tags_for_resource(ResourceARN=arn)
        assert tags_resp["Tags"] == []

    def test_create_application_with_tags(self, kav2):
        """CreateApplication with Tags parameter applies tags."""
        name = _unique_name()
        resp = kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
            Tags=[{"Key": "created", "Value": "with-tags"}],
        )
        arn = resp["ApplicationDetail"]["ApplicationARN"]
        tags_resp = kav2.list_tags_for_resource(ResourceARN=arn)
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert tag_map["created"] == "with-tags"

    def test_tag_resource_overwrites_existing(self, kav2):
        """TagResource with same key overwrites the value."""
        name = _unique_name()
        resp = kav2.create_application(
            ApplicationName=name,
            RuntimeEnvironment="FLINK-1_18",
            ServiceExecutionRole=ROLE_ARN,
        )
        arn = resp["ApplicationDetail"]["ApplicationARN"]

        kav2.tag_resource(ResourceARN=arn, Tags=[{"Key": "ver", "Value": "1"}])
        kav2.tag_resource(ResourceARN=arn, Tags=[{"Key": "ver", "Value": "2"}])

        tags_resp = kav2.list_tags_for_resource(ResourceARN=arn)
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert tag_map["ver"] == "2"

    def test_create_application_different_runtimes(self, kav2):
        """CreateApplication works with different runtime environments."""
        for runtime in ["SQL-1_0", "FLINK-1_18"]:
            name = _unique_name()
            resp = kav2.create_application(
                ApplicationName=name,
                RuntimeEnvironment=runtime,
                ServiceExecutionRole=ROLE_ARN,
            )
            assert resp["ApplicationDetail"]["RuntimeEnvironment"] == runtime
