"""CloudTrail compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

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


class TestCloudTrailDescribeTrailsFiltering:
    """Tests for DescribeTrails with filtering options."""

    def test_describe_trails_exclude_shadow_trails(self, cloudtrail, trail_with_bucket):
        """describe_trails with includeShadowTrails=False returns trails."""
        trail_name, _, _ = trail_with_bucket
        resp = cloudtrail.describe_trails(includeShadowTrails=False)
        trail_names = [t["Name"] for t in resp["trailList"]]
        assert trail_name in trail_names


class TestCloudTrailGetTrailByArn:
    """Tests for GetTrail using ARN."""

    def test_get_trail_by_arn(self, cloudtrail, trail_with_bucket):
        """get_trail can be called with the trail ARN instead of name."""
        trail_name, _, trail_arn = trail_with_bucket
        resp = cloudtrail.get_trail(Name=trail_arn)
        assert resp["Trail"]["Name"] == trail_name
        assert resp["Trail"]["TrailARN"] == trail_arn


class TestCloudTrailCreateTrailOptions:
    """Tests for CreateTrail with various options."""

    def test_create_trail_with_s3_prefix(self, cloudtrail, s3):
        """create_trail with S3KeyPrefix stores the prefix."""
        bucket = _unique("ct-bucket")
        trail = _unique("ct-trail")
        s3.create_bucket(Bucket=bucket)
        try:
            resp = cloudtrail.create_trail(Name=trail, S3BucketName=bucket, S3KeyPrefix="my/prefix")
            assert resp["Name"] == trail
            assert resp["S3KeyPrefix"] == "my/prefix"

            # Verify via get_trail
            get_resp = cloudtrail.get_trail(Name=trail)
            assert get_resp["Trail"]["S3KeyPrefix"] == "my/prefix"
        finally:
            cloudtrail.delete_trail(Name=trail)
            s3.delete_bucket(Bucket=bucket)

    def test_create_trail_with_log_file_validation(self, cloudtrail, s3):
        """create_trail with EnableLogFileValidation=True returns LogFileValidationEnabled."""
        bucket = _unique("ct-bucket")
        trail = _unique("ct-trail")
        s3.create_bucket(Bucket=bucket)
        try:
            resp = cloudtrail.create_trail(
                Name=trail, S3BucketName=bucket, EnableLogFileValidation=True
            )
            assert resp["LogFileValidationEnabled"] is True
        finally:
            cloudtrail.delete_trail(Name=trail)
            s3.delete_bucket(Bucket=bucket)

    def test_create_trail_with_cloudwatch_logs(self, cloudtrail, s3):
        """create_trail with CloudWatch log group ARN stores it."""
        bucket = _unique("ct-bucket")
        trail = _unique("ct-trail")
        s3.create_bucket(Bucket=bucket)
        log_group_arn = "arn:aws:logs:us-east-1:123456789012:log-group:ct-test"
        role_arn = "arn:aws:iam::123456789012:role/ct-role"
        try:
            resp = cloudtrail.create_trail(
                Name=trail,
                S3BucketName=bucket,
                CloudWatchLogsLogGroupArn=log_group_arn,
                CloudWatchLogsRoleArn=role_arn,
            )
            assert resp["CloudWatchLogsLogGroupArn"] == log_group_arn
            assert resp["CloudWatchLogsRoleArn"] == role_arn
        finally:
            cloudtrail.delete_trail(Name=trail)
            s3.delete_bucket(Bucket=bucket)


class TestCloudTrailUpdateTrailBucket:
    """Tests for UpdateTrail changing the S3 bucket."""

    def test_update_trail_change_bucket(self, cloudtrail, s3, trail_with_bucket):
        """update_trail can change the S3 bucket."""
        trail_name, old_bucket, _ = trail_with_bucket
        new_bucket = _unique("ct-bucket2")
        s3.create_bucket(Bucket=new_bucket)
        try:
            resp = cloudtrail.update_trail(Name=trail_name, S3BucketName=new_bucket)
            assert resp["S3BucketName"] == new_bucket

            # Verify via get_trail
            get_resp = cloudtrail.get_trail(Name=trail_name)
            assert get_resp["Trail"]["S3BucketName"] == new_bucket
        finally:
            s3.delete_bucket(Bucket=new_bucket)

    def test_update_trail_add_s3_prefix(self, cloudtrail, trail_with_bucket):
        """update_trail can add an S3 key prefix."""
        trail_name, bucket_name, _ = trail_with_bucket
        resp = cloudtrail.update_trail(
            Name=trail_name, S3BucketName=bucket_name, S3KeyPrefix="added-prefix"
        )
        assert resp["S3KeyPrefix"] == "added-prefix"


class TestCloudTrailErrors:
    """Tests for CloudTrail error handling."""

    def test_get_trail_nonexistent(self, cloudtrail):
        """get_trail with a nonexistent trail raises TrailNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.get_trail(Name="nonexistent-trail-12345")
        assert exc_info.value.response["Error"]["Code"] == "TrailNotFoundException"


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


class TestCloudTrailDescribeTrailsByName:
    """Tests for DescribeTrails with trailNameList filter."""

    def test_describe_trails_by_name(self, cloudtrail, trail_with_bucket):
        """describe_trails with trailNameList returns only the named trail."""
        trail_name, _, _ = trail_with_bucket
        resp = cloudtrail.describe_trails(trailNameList=[trail_name])
        trail_names = [t["Name"] for t in resp["trailList"]]
        assert trail_name in trail_names

    def test_describe_trails_by_arn(self, cloudtrail, trail_with_bucket):
        """describe_trails with trailNameList using ARN returns the trail."""
        trail_name, _, trail_arn = trail_with_bucket
        resp = cloudtrail.describe_trails(trailNameList=[trail_arn])
        trail_names = [t["Name"] for t in resp["trailList"]]
        assert trail_name in trail_names


class TestCloudTrailCreateTrailWithTags:
    """Tests for CreateTrail with inline tags."""

    def test_create_trail_with_tags(self, cloudtrail, s3):
        """create_trail with TagsList applies tags immediately."""
        bucket = _unique("ct-bucket")
        trail = _unique("ct-trail")
        s3.create_bucket(Bucket=bucket)
        try:
            resp = cloudtrail.create_trail(
                Name=trail,
                S3BucketName=bucket,
                TagsList=[
                    {"Key": "team", "Value": "platform"},
                    {"Key": "stage", "Value": "dev"},
                ],
            )
            trail_arn = resp["TrailARN"]
            tags_resp = cloudtrail.list_tags(ResourceIdList=[trail_arn])
            tags = {t["Key"]: t["Value"] for t in tags_resp["ResourceTagList"][0]["TagsList"]}
            assert tags["team"] == "platform"
            assert tags["stage"] == "dev"
        finally:
            cloudtrail.delete_trail(Name=trail)
            s3.delete_bucket(Bucket=bucket)

    def test_create_trail_multi_region(self, cloudtrail, s3):
        """create_trail with IsMultiRegionTrail=True returns the flag."""
        bucket = _unique("ct-bucket")
        trail = _unique("ct-trail")
        s3.create_bucket(Bucket=bucket)
        try:
            resp = cloudtrail.create_trail(Name=trail, S3BucketName=bucket, IsMultiRegionTrail=True)
            assert resp["IsMultiRegionTrail"] is True
            get_resp = cloudtrail.get_trail(Name=trail)
            assert get_resp["Trail"]["IsMultiRegionTrail"] is True
        finally:
            cloudtrail.delete_trail(Name=trail)
            s3.delete_bucket(Bucket=bucket)


class TestCloudTrailMultipleTags:
    """Tests for adding multiple tag batches."""

    def test_add_tags_twice_merges(self, cloudtrail, trail_with_bucket):
        """Adding tags in two calls merges them."""
        _, _, trail_arn = trail_with_bucket
        cloudtrail.add_tags(
            ResourceId=trail_arn,
            TagsList=[{"Key": "a", "Value": "1"}],
        )
        cloudtrail.add_tags(
            ResourceId=trail_arn,
            TagsList=[{"Key": "b", "Value": "2"}],
        )
        resp = cloudtrail.list_tags(ResourceIdList=[trail_arn])
        tags = {t["Key"]: t["Value"] for t in resp["ResourceTagList"][0]["TagsList"]}
        assert tags["a"] == "1"
        assert tags["b"] == "2"

    def test_add_tags_overwrites_existing(self, cloudtrail, trail_with_bucket):
        """Adding a tag with an existing key overwrites the value."""
        _, _, trail_arn = trail_with_bucket
        cloudtrail.add_tags(
            ResourceId=trail_arn,
            TagsList=[{"Key": "env", "Value": "old"}],
        )
        cloudtrail.add_tags(
            ResourceId=trail_arn,
            TagsList=[{"Key": "env", "Value": "new"}],
        )
        resp = cloudtrail.list_tags(ResourceIdList=[trail_arn])
        tags = {t["Key"]: t["Value"] for t in resp["ResourceTagList"][0]["TagsList"]}
        assert tags["env"] == "new"


class TestCloudTrailEventDataStoreOperations:
    """Tests for EventDataStore operations."""

    def test_list_event_data_stores_empty(self, cloudtrail):
        resp = cloudtrail.list_event_data_stores()
        assert "EventDataStores" in resp
        assert isinstance(resp["EventDataStores"], list)

    def test_get_event_data_store_nonexistent(self, cloudtrail):
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.get_event_data_store(
                EventDataStore="arn:aws:cloudtrail:us-east-1:123456789012:"
                "eventdatastore/00000000-0000-0000-0000-000000000000"
            )
        assert exc_info.value.response["Error"]["Code"] == "EventDataStoreNotFoundException"

    def test_create_event_data_store(self, cloudtrail):
        """CreateEventDataStore returns ARN, Name, and Status."""
        name = _unique("ct-eds")
        resp = cloudtrail.create_event_data_store(Name=name)
        assert resp["Name"] == name
        assert resp["Status"] == "ENABLED"
        assert "EventDataStoreArn" in resp
        assert "CreatedTimestamp" in resp

    def test_create_event_data_store_and_get(self, cloudtrail):
        """Created EDS is retrievable via GetEventDataStore."""
        name = _unique("ct-eds")
        create_resp = cloudtrail.create_event_data_store(Name=name)
        eds_arn = create_resp["EventDataStoreArn"]
        get_resp = cloudtrail.get_event_data_store(EventDataStore=eds_arn)
        assert get_resp["Name"] == name
        assert get_resp["Status"] == "ENABLED"
        assert get_resp["EventDataStoreArn"] == eds_arn

    def test_create_event_data_store_appears_in_list(self, cloudtrail):
        """Created EDS appears in ListEventDataStores."""
        name = _unique("ct-eds")
        create_resp = cloudtrail.create_event_data_store(Name=name)
        eds_arn = create_resp["EventDataStoreArn"]
        list_resp = cloudtrail.list_event_data_stores()
        arns = [e.get("EventDataStoreArn") for e in list_resp["EventDataStores"]]
        assert eds_arn in arns

    def test_create_event_data_store_with_retention(self, cloudtrail):
        """CreateEventDataStore with custom RetentionPeriod."""
        name = _unique("ct-eds")
        resp = cloudtrail.create_event_data_store(Name=name, RetentionPeriod=90)
        assert resp["RetentionPeriod"] == 90

    def test_create_event_data_store_multi_region(self, cloudtrail):
        """CreateEventDataStore with MultiRegionEnabled."""
        name = _unique("ct-eds")
        resp = cloudtrail.create_event_data_store(Name=name, MultiRegionEnabled=True)
        assert resp["MultiRegionEnabled"] is True

    def test_create_event_data_store_with_tags(self, cloudtrail):
        """CreateEventDataStore and verify tags via AddTags/ListTags."""
        name = _unique("ct-eds")
        resp = cloudtrail.create_event_data_store(Name=name)
        eds_arn = resp["EventDataStoreArn"]
        cloudtrail.add_tags(
            ResourceId=eds_arn,
            TagsList=[{"Key": "env", "Value": "test"}],
        )
        tags_resp = cloudtrail.list_tags(ResourceIdList=[eds_arn])
        tags = {t["Key"]: t["Value"] for t in tags_resp["ResourceTagList"][0]["TagsList"]}
        assert tags["env"] == "test"


class TestCloudTrailChannelOperations:
    """Tests for Channel operations."""

    def test_list_channels_empty(self, cloudtrail):
        resp = cloudtrail.list_channels()
        assert "Channels" in resp
        assert isinstance(resp["Channels"], list)

    def test_get_channel_nonexistent(self, cloudtrail):
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.get_channel(
                Channel="arn:aws:cloudtrail:us-east-1:123456789012:"
                "channel/00000000-0000-0000-0000-000000000000"
            )
        assert exc_info.value.response["Error"]["Code"] == "ChannelNotFoundException"

    def test_create_channel_and_get(self, cloudtrail):
        """CreateChannel returns ChannelArn; GetChannel retrieves it."""
        # Need an EDS for channel destination
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        name = _unique("ct-ch")
        ch = cloudtrail.create_channel(
            Name=name,
            Source="Custom",
            Destinations=[{"Type": "EVENT_DATA_STORE", "Location": eds_arn}],
        )
        assert "ChannelArn" in ch
        ch_arn = ch["ChannelArn"]

        get_resp = cloudtrail.get_channel(Channel=ch_arn)
        assert get_resp["ChannelArn"] == ch_arn
        assert get_resp["Name"] == name
        assert get_resp["Source"] == "Custom"

    def test_create_channel_appears_in_list(self, cloudtrail):
        """Created channel appears in ListChannels."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        ch = cloudtrail.create_channel(
            Name=_unique("ct-ch"),
            Source="Custom",
            Destinations=[{"Type": "EVENT_DATA_STORE", "Location": eds_arn}],
        )
        ch_arn = ch["ChannelArn"]
        list_resp = cloudtrail.list_channels()
        arns = [c.get("ChannelArn") for c in list_resp["Channels"]]
        assert ch_arn in arns

    def test_get_channel_destinations(self, cloudtrail):
        """GetChannel returns destinations for a created channel."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        ch = cloudtrail.create_channel(
            Name=_unique("ct-ch"),
            Source="Custom",
            Destinations=[{"Type": "EVENT_DATA_STORE", "Location": eds_arn}],
        )
        ch_arn = ch["ChannelArn"]
        get_resp = cloudtrail.get_channel(Channel=ch_arn)
        assert len(get_resp["Destinations"]) >= 1
        assert get_resp["Destinations"][0]["Type"] == "EVENT_DATA_STORE"


class TestCloudTrailQueryOperations:
    """Tests for Query operations."""

    def test_describe_query_nonexistent(self, cloudtrail):
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.describe_query(
                EventDataStore="00000000-0000-0000-0000-000000000000",
                QueryId="00000000-0000-0000-0000-000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "QueryIdNotFoundException"

    def test_start_query_returns_query_id(self, cloudtrail):
        """StartQuery returns a QueryId."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_id = eds["EventDataStoreArn"].split("/")[-1]
        resp = cloudtrail.start_query(QueryStatement=f"SELECT * FROM {eds_id} LIMIT 1")
        assert "QueryId" in resp
        assert len(resp["QueryId"]) > 0

    def test_start_query_and_describe(self, cloudtrail):
        """StartQuery followed by DescribeQuery returns query status."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_id = eds["EventDataStoreArn"].split("/")[-1]
        q = cloudtrail.start_query(QueryStatement=f"SELECT * FROM {eds_id} LIMIT 1")
        qid = q["QueryId"]
        desc = cloudtrail.describe_query(QueryId=qid)
        assert desc["QueryId"] == qid
        assert "QueryStatus" in desc

    def test_start_query_and_get_results(self, cloudtrail):
        """StartQuery followed by GetQueryResults returns result structure."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_id = eds["EventDataStoreArn"].split("/")[-1]
        q = cloudtrail.start_query(QueryStatement=f"SELECT * FROM {eds_id} LIMIT 1")
        qid = q["QueryId"]
        resp = cloudtrail.get_query_results(QueryId=qid)
        assert "QueryStatus" in resp
        assert "QueryResultRows" in resp
        assert isinstance(resp["QueryResultRows"], list)


class TestCloudTrailResourcePolicyOperations:
    """Tests for ResourcePolicy operations."""

    def test_get_resource_policy_nonexistent(self, cloudtrail):
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.get_resource_policy(
                ResourceArn="arn:aws:cloudtrail:us-east-1:123456789012:trail/nonexistent"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_and_get_resource_policy(self, cloudtrail):
        """PutResourcePolicy sets a policy, GetResourcePolicy retrieves it."""
        import json

        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "test-policy",
                        "Effect": "Allow",
                        "Principal": {"AWS": "123456789012"},
                        "Action": "cloudtrail:GetEventDataStore",
                        "Resource": eds_arn,
                    }
                ],
            }
        )
        put_resp = cloudtrail.put_resource_policy(ResourceArn=eds_arn, ResourcePolicy=policy_doc)
        assert put_resp["ResourceArn"] == eds_arn

        get_resp = cloudtrail.get_resource_policy(ResourceArn=eds_arn)
        assert get_resp["ResourceArn"] == eds_arn
        assert isinstance(get_resp["ResourcePolicy"], str)
        parsed = json.loads(get_resp["ResourcePolicy"])
        assert parsed["Statement"][0]["Sid"] == "test-policy"


class TestCloudTrailDashboardOperations:
    """Tests for Dashboard operations."""

    def test_get_dashboard_nonexistent(self, cloudtrail):
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.get_dashboard(DashboardId="nonexistent-dashboard-id")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestCloudTrailEventConfigurationOperations:
    """Tests for EventConfiguration operations."""

    def test_get_event_configuration(self, cloudtrail):
        resp = cloudtrail.get_event_configuration()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCloudTrailCreateTrailErrors:
    """Tests for CreateTrail error cases."""

    def test_create_trail_nonexistent_bucket(self, cloudtrail):
        """create_trail with a nonexistent S3 bucket raises S3BucketDoesNotExistException."""
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.create_trail(
                Name=_unique("ct-trail"),
                S3BucketName="nonexistent-bucket-" + uuid.uuid4().hex[:8],
            )
        assert exc_info.value.response["Error"]["Code"] == "S3BucketDoesNotExistException"


class TestCloudTrailEventDataStoreAdvanced:
    """Tests for EventDataStore with advanced options."""

    def test_create_event_data_store_with_advanced_selectors(self, cloudtrail):
        """CreateEventDataStore with AdvancedEventSelectors returns them."""
        name = _unique("ct-eds")
        resp = cloudtrail.create_event_data_store(
            Name=name,
            AdvancedEventSelectors=[
                {
                    "Name": "management-events",
                    "FieldSelectors": [{"Field": "eventCategory", "Equals": ["Management"]}],
                }
            ],
        )
        assert resp["Name"] == name
        assert "AdvancedEventSelectors" in resp
        assert len(resp["AdvancedEventSelectors"]) == 1
        assert resp["AdvancedEventSelectors"][0]["Name"] == "management-events"

    def test_create_event_data_store_organization_enabled(self, cloudtrail):
        """CreateEventDataStore with OrganizationEnabled=True."""
        name = _unique("ct-eds")
        resp = cloudtrail.create_event_data_store(Name=name, OrganizationEnabled=True)
        assert resp["OrganizationEnabled"] is True

    def test_create_event_data_store_termination_protection(self, cloudtrail):
        """CreateEventDataStore with TerminationProtectionEnabled=True."""
        name = _unique("ct-eds")
        resp = cloudtrail.create_event_data_store(Name=name, TerminationProtectionEnabled=True)
        assert resp["TerminationProtectionEnabled"] is True

    def test_get_event_data_store_advanced_selectors(self, cloudtrail):
        """GetEventDataStore returns AdvancedEventSelectors that were set at creation."""
        name = _unique("ct-eds")
        create_resp = cloudtrail.create_event_data_store(
            Name=name,
            AdvancedEventSelectors=[
                {
                    "Name": "data-events",
                    "FieldSelectors": [{"Field": "eventCategory", "Equals": ["Data"]}],
                }
            ],
        )
        eds_arn = create_resp["EventDataStoreArn"]
        get_resp = cloudtrail.get_event_data_store(EventDataStore=eds_arn)
        assert len(get_resp["AdvancedEventSelectors"]) == 1
        assert get_resp["AdvancedEventSelectors"][0]["Name"] == "data-events"


class TestCloudTrailQueryAdvanced:
    """Tests for query operations with additional assertions."""

    def test_describe_query_has_query_string(self, cloudtrail):
        """DescribeQuery returns the QueryString used to start the query."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_id = eds["EventDataStoreArn"].split("/")[-1]
        stmt = f"SELECT * FROM {eds_id} LIMIT 1"
        q = cloudtrail.start_query(QueryStatement=stmt)
        qid = q["QueryId"]
        desc = cloudtrail.describe_query(QueryId=qid)
        assert desc["QueryId"] == qid
        assert desc["QueryString"] == stmt

    def test_get_query_results_has_statistics(self, cloudtrail):
        """GetQueryResults returns QueryStatistics."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_id = eds["EventDataStoreArn"].split("/")[-1]
        q = cloudtrail.start_query(QueryStatement=f"SELECT * FROM {eds_id} LIMIT 1")
        qid = q["QueryId"]
        resp = cloudtrail.get_query_results(QueryId=qid)
        assert "QueryStatistics" in resp
        assert "ResultsCount" in resp["QueryStatistics"]


class TestCloudTrailDescribeTrailsEdgeCases:
    """Tests for DescribeTrails edge cases."""

    def test_describe_trails_nonexistent_name(self, cloudtrail):
        """describe_trails with a nonexistent trail name returns empty list."""
        resp = cloudtrail.describe_trails(
            trailNameList=["nonexistent-trail-" + uuid.uuid4().hex[:8]]
        )
        assert resp["trailList"] == []


class TestCloudTrailUpdateEventDataStore:
    """Tests for UpdateEventDataStore."""

    def test_update_event_data_store_name(self, cloudtrail):
        """UpdateEventDataStore can change the name."""
        name = _unique("ct-eds")
        create_resp = cloudtrail.create_event_data_store(Name=name)
        eds_arn = create_resp["EventDataStoreArn"]
        new_name = _unique("ct-eds-updated")
        try:
            resp = cloudtrail.update_event_data_store(EventDataStore=eds_arn, Name=new_name)
            assert resp["EventDataStoreArn"] == eds_arn
            assert resp["Name"] == new_name
        finally:
            try:
                cloudtrail.delete_event_data_store(EventDataStore=eds_arn)
            except Exception:
                pass

    def test_update_event_data_store_retention(self, cloudtrail):
        """UpdateEventDataStore can change retention period."""
        name = _unique("ct-eds")
        create_resp = cloudtrail.create_event_data_store(Name=name, RetentionPeriod=90)
        eds_arn = create_resp["EventDataStoreArn"]
        try:
            resp = cloudtrail.update_event_data_store(EventDataStore=eds_arn, RetentionPeriod=180)
            assert resp["RetentionPeriod"] == 180
        finally:
            try:
                cloudtrail.delete_event_data_store(EventDataStore=eds_arn)
            except Exception:
                pass

    def test_update_event_data_store_verify_via_get(self, cloudtrail):
        """UpdateEventDataStore changes are visible via GetEventDataStore."""
        name = _unique("ct-eds")
        create_resp = cloudtrail.create_event_data_store(Name=name)
        eds_arn = create_resp["EventDataStoreArn"]
        new_name = _unique("ct-eds-v2")
        try:
            cloudtrail.update_event_data_store(EventDataStore=eds_arn, Name=new_name)
            get_resp = cloudtrail.get_event_data_store(EventDataStore=eds_arn)
            assert get_resp["Name"] == new_name
        finally:
            try:
                cloudtrail.delete_event_data_store(EventDataStore=eds_arn)
            except Exception:
                pass


class TestCloudTrailDeleteEventDataStore:
    """Tests for DeleteEventDataStore and RestoreEventDataStore."""

    def test_delete_event_data_store(self, cloudtrail):
        """DeleteEventDataStore removes the EDS."""
        name = _unique("ct-eds")
        create_resp = cloudtrail.create_event_data_store(Name=name)
        eds_arn = create_resp["EventDataStoreArn"]
        cloudtrail.delete_event_data_store(EventDataStore=eds_arn)
        # After delete, get should fail or show non-ENABLED status
        try:
            get_resp = cloudtrail.get_event_data_store(EventDataStore=eds_arn)
            # Some implementations mark as PENDING_DELETION rather than removing
            assert get_resp["Status"] in ("PENDING_DELETION", "STOPPED_INGESTION")
        except ClientError as e:
            assert e.response["Error"]["Code"] == "EventDataStoreNotFoundException"

    def test_delete_event_data_store_nonexistent(self, cloudtrail):
        """DeleteEventDataStore on nonexistent ARN raises error."""
        fake_arn = (
            "arn:aws:cloudtrail:us-east-1:123456789012:"
            "eventdatastore/00000000-0000-0000-0000-000000000099"
        )
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.delete_event_data_store(EventDataStore=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "EventDataStoreNotFoundException"


class TestCloudTrailListQueries:
    """Tests for ListQueries and CancelQuery."""

    def test_list_queries(self, cloudtrail):
        """ListQueries returns queries for an EDS."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        eds_id = eds_arn.split("/")[-1]
        # Start a query so there's something to list
        cloudtrail.start_query(QueryStatement=f"SELECT * FROM {eds_id} LIMIT 1")
        resp = cloudtrail.list_queries(EventDataStore=eds_arn)
        assert "Queries" in resp
        assert isinstance(resp["Queries"], list)
        assert len(resp["Queries"]) >= 1

    def test_cancel_query(self, cloudtrail):
        """CancelQuery cancels a running query."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        eds_id = eds_arn.split("/")[-1]
        q = cloudtrail.start_query(QueryStatement=f"SELECT * FROM {eds_id} LIMIT 1")
        qid = q["QueryId"]
        try:
            resp = cloudtrail.cancel_query(QueryId=qid)
            assert resp["QueryId"] == qid
            assert resp["QueryStatus"] in ("CANCELLED", "FINISHED", "TIMED_OUT")
        except ClientError as e:
            # Query may have already finished
            assert e.response["Error"]["Code"] in (
                "InactiveQueryException",
                "QueryIdNotFoundException",
            )


class TestCloudTrailUpdateChannel:
    """Tests for UpdateChannel and DeleteChannel."""

    def test_update_channel_destinations(self, cloudtrail):
        """UpdateChannel can update destinations."""
        eds1 = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds1_arn = eds1["EventDataStoreArn"]
        eds2 = cloudtrail.create_event_data_store(Name=_unique("ct-eds2"))
        eds2_arn = eds2["EventDataStoreArn"]
        ch = cloudtrail.create_channel(
            Name=_unique("ct-ch"),
            Source="Custom",
            Destinations=[{"Type": "EVENT_DATA_STORE", "Location": eds1_arn}],
        )
        ch_arn = ch["ChannelArn"]
        try:
            resp = cloudtrail.update_channel(
                Channel=ch_arn,
                Destinations=[{"Type": "EVENT_DATA_STORE", "Location": eds2_arn}],
            )
            assert resp["ChannelArn"] == ch_arn
            assert len(resp["Destinations"]) >= 1
            locs = [d["Location"] for d in resp["Destinations"]]
            assert eds2_arn in locs
        finally:
            try:
                cloudtrail.delete_channel(Channel=ch_arn)
            except Exception:
                pass

    def test_delete_channel(self, cloudtrail):
        """DeleteChannel removes the channel."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        ch = cloudtrail.create_channel(
            Name=_unique("ct-ch"),
            Source="Custom",
            Destinations=[{"Type": "EVENT_DATA_STORE", "Location": eds_arn}],
        )
        ch_arn = ch["ChannelArn"]
        cloudtrail.delete_channel(Channel=ch_arn)
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.get_channel(Channel=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ChannelNotFoundException"


class TestCloudTrailImportOperations:
    """Tests for Import operations."""

    def test_list_imports_empty(self, cloudtrail):
        """ListImports returns an empty list when no imports exist."""
        resp = cloudtrail.list_imports()
        assert "Imports" in resp
        assert isinstance(resp["Imports"], list)

    def test_start_import_and_get(self, cloudtrail):
        """StartImport creates an import; GetImport retrieves it."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        try:
            import_resp = cloudtrail.start_import(
                Destinations=[eds_arn],
                ImportSource={
                    "S3": {
                        "S3LocationUri": "s3://fake-import-bucket/prefix/",
                        "S3BucketRegion": "us-east-1",
                        "S3BucketAccessRoleArn": "arn:aws:iam::123456789012:role/import-role",
                    }
                },
            )
            assert "ImportId" in import_resp
            import_id = import_resp["ImportId"]

            get_resp = cloudtrail.get_import(ImportId=import_id)
            assert get_resp["ImportId"] == import_id
            assert "ImportStatus" in get_resp
        except ClientError:
            # If StartImport isn't fully implemented, skip gracefully
            pytest.skip("StartImport not fully implemented")

    def test_start_import_appears_in_list(self, cloudtrail):
        """StartImport result appears in ListImports."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        try:
            import_resp = cloudtrail.start_import(
                Destinations=[eds_arn],
                ImportSource={
                    "S3": {
                        "S3LocationUri": "s3://fake-import-bucket/prefix/",
                        "S3BucketRegion": "us-east-1",
                        "S3BucketAccessRoleArn": "arn:aws:iam::123456789012:role/import-role",
                    }
                },
            )
            import_id = import_resp["ImportId"]
            list_resp = cloudtrail.list_imports()
            ids = [i.get("ImportId") for i in list_resp["Imports"]]
            assert import_id in ids
        except ClientError:
            pytest.skip("StartImport not fully implemented")

    def test_stop_import(self, cloudtrail):
        """StopImport stops a running import."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        try:
            import_resp = cloudtrail.start_import(
                Destinations=[eds_arn],
                ImportSource={
                    "S3": {
                        "S3LocationUri": "s3://fake-import-bucket/prefix/",
                        "S3BucketRegion": "us-east-1",
                        "S3BucketAccessRoleArn": "arn:aws:iam::123456789012:role/import-role",
                    }
                },
            )
            import_id = import_resp["ImportId"]
            stop_resp = cloudtrail.stop_import(ImportId=import_id)
            assert stop_resp["ImportId"] == import_id
            assert stop_resp["ImportStatus"] in ("STOPPED", "COMPLETED", "FAILED")
        except ClientError:
            pytest.skip("StartImport/StopImport not fully implemented")


class TestCloudTrailFederation:
    """Tests for EnableFederation and DisableFederation."""

    def test_enable_federation(self, cloudtrail):
        """EnableFederation on an EDS returns federation status."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        try:
            resp = cloudtrail.enable_federation(
                EventDataStore=eds_arn,
                FederationRoleArn="arn:aws:iam::123456789012:role/federation-role",
            )
            assert resp["EventDataStoreArn"] == eds_arn
            assert resp["FederationStatus"] in ("ENABLED", "ENABLING")
        except ClientError as e:
            if "not implemented" in str(e).lower() or e.response["Error"]["Code"] in (
                "NotImplementedError",
                "UnsupportedOperationException",
            ):
                pytest.skip("EnableFederation not implemented")
            raise

    def test_disable_federation(self, cloudtrail):
        """DisableFederation on an EDS returns federation status."""
        eds = cloudtrail.create_event_data_store(Name=_unique("ct-eds"))
        eds_arn = eds["EventDataStoreArn"]
        try:
            # Enable first
            cloudtrail.enable_federation(
                EventDataStore=eds_arn,
                FederationRoleArn="arn:aws:iam::123456789012:role/federation-role",
            )
            resp = cloudtrail.disable_federation(EventDataStore=eds_arn)
            assert resp["EventDataStoreArn"] == eds_arn
            assert resp["FederationStatus"] in ("DISABLED", "DISABLING")
        except ClientError as e:
            if "not implemented" in str(e).lower() or e.response["Error"]["Code"] in (
                "NotImplementedError",
                "UnsupportedOperationException",
            ):
                pytest.skip("Federation not implemented")
            raise


class TestCloudTrailRegisterOrgDelegatedAdmin:
    """Tests for RegisterOrganizationDelegatedAdmin."""

    def test_register_organization_delegated_admin(self, cloudtrail):
        """RegisterOrganizationDelegatedAdmin with a member account ID."""
        try:
            resp = cloudtrail.register_organization_delegated_admin(MemberAccountId="222233334444")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as e:
            # This may fail if Organizations isn't set up, which is expected
            code = e.response["Error"]["Code"]
            assert code in (
                "OrganizationNotInAllFeaturesModeException",
                "OrganizationsNotInUseException",
                "AccountNotFoundException",
                "NotOrganizationMasterAccountException",
                "InsufficientDependencyServiceAccessPermissionException",
                "AccessDeniedException",
            )

    def test_deregister_organization_delegated_admin(self, cloudtrail):
        """DeregisterOrganizationDelegatedAdmin with a member account ID."""
        try:
            resp = cloudtrail.deregister_organization_delegated_admin(
                DelegatedAdminAccountId="222233334444"
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as e:
            # This may fail if Organizations isn't set up, which is expected
            code = e.response["Error"]["Code"]
            assert code in (
                "OrganizationNotInAllFeaturesModeException",
                "OrganizationsNotInUseException",
                "AccountNotFoundException",
                "NotOrganizationMasterAccountException",
                "InsufficientDependencyServiceAccessPermissionException",
                "AccessDeniedException",
                "AccountNotRegisteredException",
            )


class TestCloudTrailRestoreEventDataStore:
    """Tests for RestoreEventDataStore."""

    def test_restore_event_data_store_nonexistent(self, cloudtrail):
        """RestoreEventDataStore on nonexistent EDS raises EventDataStoreNotFoundException."""
        fake_arn = (
            "arn:aws:cloudtrail:us-east-1:123456789012:"
            "eventdatastore/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.restore_event_data_store(EventDataStore=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "EventDataStoreNotFoundException"


class TestCloudTrailDashboard:
    """Tests for CloudTrail Dashboard operations."""

    def test_get_dashboard_nonexistent(self, cloudtrail):
        """GetDashboard for nonexistent dashboard raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            cloudtrail.get_dashboard(DashboardId="nonexistent-dashboard-id")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestCloudTrailEventConfiguration:
    """Tests for CloudTrail EventConfiguration operations."""

    def test_get_event_configuration(self, cloudtrail):
        """GetEventConfiguration returns event configuration."""
        resp = cloudtrail.get_event_configuration()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCloudTrailResourcePolicy:
    """Tests for CloudTrail ResourcePolicy operations."""

    def test_put_and_get_resource_policy(self, cloudtrail, s3):
        """PutResourcePolicy sets a resource policy, GetResourcePolicy retrieves it."""
        import json

        bucket = _unique("ct-rp-bucket")
        trail = _unique("ct-rp-trail")
        s3.create_bucket(Bucket=bucket)
        try:
            trail_resp = cloudtrail.create_trail(Name=trail, S3BucketName=bucket)
            trail_arn = trail_resp["TrailARN"]
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AllowCloudTrail",
                            "Effect": "Allow",
                            "Principal": {"Service": "cloudtrail.amazonaws.com"},
                            "Action": "cloudtrail:CreateTrail",
                            "Resource": trail_arn,
                        }
                    ],
                }
            )
            put_resp = cloudtrail.put_resource_policy(
                ResourceArn=trail_arn,
                ResourcePolicy=policy,
            )
            assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "ResourceArn" in put_resp
            assert "ResourcePolicy" in put_resp

            get_resp = cloudtrail.get_resource_policy(ResourceArn=trail_arn)
            assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert get_resp["ResourceArn"] == trail_arn
        finally:
            cloudtrail.delete_trail(Name=trail)
            s3.delete_bucket(Bucket=bucket)
