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
