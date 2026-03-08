"""CloudTrail compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def cloudtrail():
    return make_client("cloudtrail")


@pytest.fixture
def s3():
    return make_client("s3")


@pytest.fixture
def trail_with_bucket(cloudtrail, s3):
    """Create an S3 bucket and a trail, yield (trail_name, bucket_name, trail_arn), then clean up."""  # noqa: E501
    bucket_name = _unique("ct-bucket")
    trail_name = _unique("ct-trail")
    s3.create_bucket(Bucket=bucket_name)
    resp = cloudtrail.create_trail(Name=trail_name, S3BucketName=bucket_name)
    trail_arn = resp["TrailARN"]
    yield trail_name, bucket_name, trail_arn
    # Cleanup
    try:
        cloudtrail.delete_trail(Name=trail_name)
    except Exception:
        pass
    try:
        s3.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass


class TestCloudTrailTrailOperations:
    def test_create_trail(self, cloudtrail, s3):
        bucket = _unique("ct-bucket")
        trail = _unique("ct-trail")
        s3.create_bucket(Bucket=bucket)
        try:
            resp = cloudtrail.create_trail(Name=trail, S3BucketName=bucket)
            assert resp["Name"] == trail
            assert resp["S3BucketName"] == bucket
            assert "TrailARN" in resp
        finally:
            cloudtrail.delete_trail(Name=trail)
            s3.delete_bucket(Bucket=bucket)

    def test_describe_trails(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        resp = cloudtrail.describe_trails()
        trail_names = [t["Name"] for t in resp["trailList"]]
        assert trail_name in trail_names

    def test_get_trail(self, cloudtrail, trail_with_bucket):
        trail_name, bucket_name, _ = trail_with_bucket
        resp = cloudtrail.get_trail(Name=trail_name)
        assert resp["Trail"]["Name"] == trail_name
        assert resp["Trail"]["S3BucketName"] == bucket_name

    def test_get_trail_status(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        resp = cloudtrail.get_trail_status(Name=trail_name)
        assert "IsLogging" in resp
        assert resp["IsLogging"] is False

    def test_list_trails(self, cloudtrail, trail_with_bucket):
        trail_name, _, trail_arn = trail_with_bucket
        resp = cloudtrail.list_trails()
        found = [t for t in resp["Trails"] if t["Name"] == trail_name]
        assert len(found) == 1
        assert found[0]["TrailARN"] == trail_arn

    def test_start_stop_logging(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        cloudtrail.start_logging(Name=trail_name)
        resp = cloudtrail.get_trail_status(Name=trail_name)
        assert resp["IsLogging"] is True

        cloudtrail.stop_logging(Name=trail_name)
        resp = cloudtrail.get_trail_status(Name=trail_name)
        assert resp["IsLogging"] is False

    def test_delete_trail(self, cloudtrail, s3):
        bucket = _unique("ct-bucket")
        trail = _unique("ct-trail")
        s3.create_bucket(Bucket=bucket)
        cloudtrail.create_trail(Name=trail, S3BucketName=bucket)
        cloudtrail.delete_trail(Name=trail)

        # Verify trail is gone
        resp = cloudtrail.describe_trails()
        trail_names = [t["Name"] for t in resp["trailList"]]
        assert trail not in trail_names
        s3.delete_bucket(Bucket=bucket)


class TestCloudTrailEventSelectors:
    def test_get_event_selectors_default(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        resp = cloudtrail.get_event_selectors(TrailName=trail_name)
        assert "EventSelectors" in resp

    def test_put_and_get_event_selectors(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        selectors = [
            {
                "ReadWriteType": "All",
                "IncludeManagementEvents": True,
            }
        ]
        cloudtrail.put_event_selectors(
            TrailName=trail_name,
            EventSelectors=selectors,
        )
        resp = cloudtrail.get_event_selectors(TrailName=trail_name)
        assert len(resp["EventSelectors"]) == 1
        assert resp["EventSelectors"][0]["ReadWriteType"] == "All"
        assert resp["EventSelectors"][0]["IncludeManagementEvents"] is True

    def test_put_event_selectors_read_only(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        selectors = [
            {
                "ReadWriteType": "ReadOnly",
                "IncludeManagementEvents": True,
            }
        ]
        cloudtrail.put_event_selectors(
            TrailName=trail_name,
            EventSelectors=selectors,
        )
        resp = cloudtrail.get_event_selectors(TrailName=trail_name)
        assert resp["EventSelectors"][0]["ReadWriteType"] == "ReadOnly"


class TestCloudTrailTags:
    def test_add_and_list_tags(self, cloudtrail, trail_with_bucket):
        _, _, trail_arn = trail_with_bucket
        cloudtrail.add_tags(
            ResourceId=trail_arn,
            TagsList=[{"Key": "env", "Value": "test"}, {"Key": "project", "Value": "roboto"}],
        )
        resp = cloudtrail.list_tags(ResourceIdList=[trail_arn])
        assert len(resp["ResourceTagList"]) == 1
        tags = {t["Key"]: t["Value"] for t in resp["ResourceTagList"][0]["TagsList"]}
        assert tags["env"] == "test"
        assert tags["project"] == "roboto"

    def test_remove_tags(self, cloudtrail, trail_with_bucket):
        _, _, trail_arn = trail_with_bucket
        cloudtrail.add_tags(
            ResourceId=trail_arn,
            TagsList=[{"Key": "env", "Value": "test"}, {"Key": "keep", "Value": "yes"}],
        )
        cloudtrail.remove_tags(
            ResourceId=trail_arn,
            TagsList=[{"Key": "env", "Value": "test"}],
        )
        resp = cloudtrail.list_tags(ResourceIdList=[trail_arn])
        tags = {t["Key"]: t["Value"] for t in resp["ResourceTagList"][0]["TagsList"]}
        assert "env" not in tags
        assert tags["keep"] == "yes"

    def test_list_tags_empty(self, cloudtrail, trail_with_bucket):
        _, _, trail_arn = trail_with_bucket
        resp = cloudtrail.list_tags(ResourceIdList=[trail_arn])
        assert len(resp["ResourceTagList"]) == 1
        assert resp["ResourceTagList"][0]["TagsList"] == []


class TestCloudTrailUpdateTrail:
    """Tests for UpdateTrail operation."""

    def test_update_trail_enable_log_file_validation(self, cloudtrail, trail_with_bucket):
        trail_name, bucket_name, _ = trail_with_bucket
        resp = cloudtrail.update_trail(
            Name=trail_name,
            S3BucketName=bucket_name,
            EnableLogFileValidation=True,
        )
        assert resp["Name"] == trail_name
        assert resp["LogFileValidationEnabled"] is True

    def test_update_trail_multi_region(self, cloudtrail, trail_with_bucket):
        trail_name, bucket_name, _ = trail_with_bucket
        resp = cloudtrail.update_trail(
            Name=trail_name,
            S3BucketName=bucket_name,
            IsMultiRegionTrail=True,
        )
        assert resp["Name"] == trail_name
        assert resp["IsMultiRegionTrail"] is True

    def test_update_trail_include_global_events(self, cloudtrail, trail_with_bucket):
        trail_name, bucket_name, _ = trail_with_bucket
        resp = cloudtrail.update_trail(
            Name=trail_name,
            S3BucketName=bucket_name,
            IncludeGlobalServiceEvents=False,
        )
        assert resp["Name"] == trail_name
        assert resp["IncludeGlobalServiceEvents"] is False

    def test_update_trail_verify_via_get(self, cloudtrail, trail_with_bucket):
        trail_name, bucket_name, _ = trail_with_bucket
        cloudtrail.update_trail(
            Name=trail_name,
            S3BucketName=bucket_name,
            EnableLogFileValidation=True,
            IsMultiRegionTrail=True,
        )
        resp = cloudtrail.get_trail(Name=trail_name)
        assert resp["Trail"]["LogFileValidationEnabled"] is True
        assert resp["Trail"]["IsMultiRegionTrail"] is True


class TestCloudTrailInsightSelectors:
    """Tests for GetInsightSelectors and PutInsightSelectors."""

    def test_get_insight_selectors_default(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        resp = cloudtrail.get_insight_selectors(TrailName=trail_name)
        assert "TrailARN" in resp
        # Default may have empty or no insight selectors
        selectors = resp.get("InsightSelectors", [])
        assert isinstance(selectors, list)

    def test_put_insight_selectors_api_call_rate(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        resp = cloudtrail.put_insight_selectors(
            TrailName=trail_name,
            InsightSelectors=[{"InsightType": "ApiCallRateInsight"}],
        )
        assert "TrailARN" in resp
        assert len(resp["InsightSelectors"]) == 1
        assert resp["InsightSelectors"][0]["InsightType"] == "ApiCallRateInsight"

    def test_put_insight_selectors_verify_via_get(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        cloudtrail.put_insight_selectors(
            TrailName=trail_name,
            InsightSelectors=[{"InsightType": "ApiCallRateInsight"}],
        )
        resp = cloudtrail.get_insight_selectors(TrailName=trail_name)
        found = [
            s for s in resp.get("InsightSelectors", []) if s["InsightType"] == "ApiCallRateInsight"
        ]
        assert len(found) == 1

    def test_put_insight_selectors_empty_clears(self, cloudtrail, trail_with_bucket):
        trail_name, _, _ = trail_with_bucket
        # Set then clear
        cloudtrail.put_insight_selectors(
            TrailName=trail_name,
            InsightSelectors=[{"InsightType": "ApiCallRateInsight"}],
        )
        resp = cloudtrail.put_insight_selectors(
            TrailName=trail_name,
            InsightSelectors=[],
        )
        assert resp.get("InsightSelectors", []) == []
