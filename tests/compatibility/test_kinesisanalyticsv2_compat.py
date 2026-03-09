"""Kinesis Analytics v2 compatibility tests."""

import uuid

import botocore.exceptions
import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def kav2():
    return make_client("kinesisanalyticsv2")


def _unique_name():
    return f"test-kav2-{uuid.uuid4().hex[:8]}"


def _create_app(kav2, name=None, runtime="FLINK-1_18"):
    """Helper to create an app and return (name, arn, version_id)."""
    if name is None:
        name = _unique_name()
    resp = kav2.create_application(
        ApplicationName=name,
        RuntimeEnvironment=runtime,
        ServiceExecutionRole=ROLE_ARN,
    )
    detail = resp["ApplicationDetail"]
    return name, detail["ApplicationARN"], detail["ApplicationVersionId"]


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
        matching = [s for s in response["ApplicationSummaries"] if s["ApplicationName"] == name]
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

    def test_delete_application(self, kav2):
        """DeleteApplication removes an application."""
        name, arn, _ = _create_app(kav2)
        # Get create timestamp for delete call
        detail = kav2.describe_application(ApplicationName=name)["ApplicationDetail"]
        create_ts = detail["CreateTimestamp"]
        kav2.delete_application(ApplicationName=name, CreateTimestamp=create_ts)
        # Verify it's gone
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.describe_application(ApplicationName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_application_not_found(self, kav2):
        """DeleteApplication on nonexistent app raises ResourceNotFoundException."""
        import datetime

        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.delete_application(
                ApplicationName="nonexistent-app-xyz",
                CreateTimestamp=datetime.datetime(2024, 1, 1),
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_application_not_found(self, kav2):
        """DescribeApplication on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.describe_application(ApplicationName="nonexistent-app-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_application_version(self, kav2):
        """DescribeApplicationVersion returns version details."""
        name, arn, version_id = _create_app(kav2)
        resp = kav2.describe_application_version(
            ApplicationName=name, ApplicationVersionId=version_id
        )
        detail = resp["ApplicationVersionDetail"]
        assert detail["ApplicationName"] == name
        assert detail["ApplicationVersionId"] == version_id

    def test_describe_application_version_not_found(self, kav2):
        """DescribeApplicationVersion on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.describe_application_version(
                ApplicationName="nonexistent-app-xyz", ApplicationVersionId=1
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_application_versions(self, kav2):
        """ListApplicationVersions returns version summaries."""
        name, arn, _ = _create_app(kav2)
        resp = kav2.list_application_versions(ApplicationName=name)
        assert "ApplicationVersionSummaries" in resp
        assert len(resp["ApplicationVersionSummaries"]) >= 1

    def test_list_application_versions_not_found(self, kav2):
        """ListApplicationVersions on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.list_application_versions(ApplicationName="nonexistent-app-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_application_snapshots(self, kav2):
        """ListApplicationSnapshots returns empty list for new app."""
        name, arn, _ = _create_app(kav2)
        resp = kav2.list_application_snapshots(ApplicationName=name)
        assert "SnapshotSummaries" in resp

    def test_list_application_snapshots_not_found(self, kav2):
        """ListApplicationSnapshots on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.list_application_snapshots(ApplicationName="nonexistent-app-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_application_operations(self, kav2):
        """ListApplicationOperations returns operations list."""
        name, arn, _ = _create_app(kav2)
        resp = kav2.list_application_operations(ApplicationName=name)
        assert "ApplicationOperationInfoList" in resp

    def test_list_application_operations_not_found(self, kav2):
        """ListApplicationOperations on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.list_application_operations(ApplicationName="nonexistent-app-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_untag_resource(self, kav2):
        """UntagResource removes tags from an application."""
        name, arn, _ = _create_app(kav2)
        kav2.tag_resource(
            ResourceARN=arn,
            Tags=[{"Key": "remove-me", "Value": "yes"}, {"Key": "keep-me", "Value": "yes"}],
        )
        kav2.untag_resource(ResourceARN=arn, TagKeys=["remove-me"])
        tags_resp = kav2.list_tags_for_resource(ResourceARN=arn)
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert "remove-me" not in tag_map
        assert tag_map["keep-me"] == "yes"

    def test_update_application(self, kav2):
        """UpdateApplication changes the service execution role."""
        name, arn, version_id = _create_app(kav2)
        new_role = "arn:aws:iam::123456789012:role/updated"
        resp = kav2.update_application(
            ApplicationName=name,
            CurrentApplicationVersionId=version_id,
            ServiceExecutionRoleUpdate=new_role,
        )
        detail = resp["ApplicationDetail"]
        assert detail["ServiceExecutionRole"] == new_role

    def test_update_application_not_found(self, kav2):
        """UpdateApplication on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.update_application(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_application_maintenance_configuration(self, kav2):
        """UpdateApplicationMaintenanceConfiguration changes maintenance window."""
        name, arn, _ = _create_app(kav2)
        resp = kav2.update_application_maintenance_configuration(
            ApplicationName=name,
            ApplicationMaintenanceConfigurationUpdate={
                "ApplicationMaintenanceWindowStartTimeUpdate": "04:00",
            },
        )
        assert "ApplicationMaintenanceConfigurationDescription" in resp

    def test_update_application_maintenance_configuration_not_found(self, kav2):
        """UpdateApplicationMaintenanceConfiguration on nonexistent app raises error."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.update_application_maintenance_configuration(
                ApplicationName="nonexistent-app-xyz",
                ApplicationMaintenanceConfigurationUpdate={
                    "ApplicationMaintenanceWindowStartTimeUpdate": "04:00",
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_application_presigned_url(self, kav2):
        """CreateApplicationPresignedUrl returns a URL."""
        name, arn, _ = _create_app(kav2)
        resp = kav2.create_application_presigned_url(
            ApplicationName=name,
            UrlType="FLINK_DASHBOARD_URL",
        )
        assert "AuthorizedUrl" in resp

    def test_create_application_snapshot(self, kav2):
        """CreateApplicationSnapshot creates a snapshot."""
        name, arn, _ = _create_app(kav2)
        snap_name = f"snap-{uuid.uuid4().hex[:8]}"
        kav2.create_application_snapshot(ApplicationName=name, SnapshotName=snap_name)
        # Verify it appears in list
        resp = kav2.list_application_snapshots(ApplicationName=name)
        snap_names = [s["SnapshotName"] for s in resp["SnapshotSummaries"]]
        assert snap_name in snap_names

    def test_describe_application_snapshot(self, kav2):
        """DescribeApplicationSnapshot returns snapshot details."""
        name, arn, _ = _create_app(kav2)
        snap_name = f"snap-{uuid.uuid4().hex[:8]}"
        kav2.create_application_snapshot(ApplicationName=name, SnapshotName=snap_name)
        resp = kav2.describe_application_snapshot(ApplicationName=name, SnapshotName=snap_name)
        assert "SnapshotDetails" in resp
        assert resp["SnapshotDetails"]["SnapshotName"] == snap_name

    def test_describe_application_snapshot_not_found(self, kav2):
        """DescribeApplicationSnapshot on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.describe_application_snapshot(
                ApplicationName="nonexistent-app-xyz",
                SnapshotName="nonexistent-snap",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_application_snapshot(self, kav2):
        """DeleteApplicationSnapshot removes a snapshot."""
        name, arn, _ = _create_app(kav2)
        snap_name = f"snap-{uuid.uuid4().hex[:8]}"
        kav2.create_application_snapshot(ApplicationName=name, SnapshotName=snap_name)
        # Get snapshot creation timestamp
        snap_resp = kav2.describe_application_snapshot(ApplicationName=name, SnapshotName=snap_name)
        snap_ts = snap_resp["SnapshotDetails"]["SnapshotCreationTimestamp"]
        kav2.delete_application_snapshot(
            ApplicationName=name,
            SnapshotName=snap_name,
            SnapshotCreationTimestamp=snap_ts,
        )
        # Verify gone
        resp = kav2.list_application_snapshots(ApplicationName=name)
        snap_names = [s["SnapshotName"] for s in resp["SnapshotSummaries"]]
        assert snap_name not in snap_names

    def test_delete_application_snapshot_not_found(self, kav2):
        """DeleteApplicationSnapshot on nonexistent app raises ResourceNotFoundException."""
        import datetime

        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.delete_application_snapshot(
                ApplicationName="nonexistent-app-xyz",
                SnapshotName="nonexistent-snap",
                SnapshotCreationTimestamp=datetime.datetime(2024, 1, 1),
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_application_operation_not_found(self, kav2):
        """DescribeApplicationOperation on nonexistent app raises error."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.describe_application_operation(
                ApplicationName="nonexistent-app-xyz",
                OperationId="fake-op-id",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_application_not_found(self, kav2):
        """StartApplication on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.start_application(ApplicationName="nonexistent-app-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_application_not_found(self, kav2):
        """StopApplication on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.stop_application(ApplicationName="nonexistent-app-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_rollback_application_not_found(self, kav2):
        """RollbackApplication on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.rollback_application(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_add_application_cloud_watch_logging_option(self, kav2):
        """AddApplicationCloudWatchLoggingOption adds a CW logging config."""
        name, arn, version_id = _create_app(kav2)
        log_stream_arn = "arn:aws:logs:us-east-1:123456789012:log-group:test:log-stream:test"
        resp = kav2.add_application_cloud_watch_logging_option(
            ApplicationName=name,
            CurrentApplicationVersionId=version_id,
            CloudWatchLoggingOption={"LogStreamARN": log_stream_arn},
        )
        assert "ApplicationARN" in resp

    def test_add_application_cloud_watch_logging_option_not_found(self, kav2):
        """AddApplicationCloudWatchLoggingOption on nonexistent app raises error."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.add_application_cloud_watch_logging_option(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
                CloudWatchLoggingOption={
                    "LogStreamARN": "arn:aws:logs:us-east-1:123456789012:log-group:x:log-stream:y"
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_application_cloud_watch_logging_option_not_found(self, kav2):
        """DeleteApplicationCloudWatchLoggingOption on nonexistent app raises error."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.delete_application_cloud_watch_logging_option(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
                CloudWatchLoggingOptionId="1.1",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_add_application_output(self, kav2):
        """AddApplicationOutput adds an output to an app."""
        name, arn, version_id = _create_app(kav2, runtime="SQL-1_0")
        resp = kav2.add_application_output(
            ApplicationName=name,
            CurrentApplicationVersionId=version_id,
            Output={
                "Name": "test-output",
                "DestinationSchema": {"RecordFormatType": "JSON"},
                "KinesisStreamsOutput": {
                    "ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/test"
                },
            },
        )
        assert "ApplicationARN" in resp

    def test_add_application_output_not_found(self, kav2):
        """AddApplicationOutput on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.add_application_output(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
                Output={
                    "Name": "test-output",
                    "DestinationSchema": {"RecordFormatType": "JSON"},
                    "KinesisStreamsOutput": {
                        "ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/test"
                    },
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_application_output_not_found(self, kav2):
        """DeleteApplicationOutput on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.delete_application_output(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
                OutputId="1.1",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_add_application_input_processing_configuration_not_found(self, kav2):
        """AddApplicationInputProcessingConfiguration on nonexistent app raises error."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.add_application_input_processing_configuration(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
                InputId="1.1",
                InputProcessingConfiguration={
                    "InputLambdaProcessor": {
                        "ResourceARN": "arn:aws:lambda:us-east-1:123456789012:function:test"
                    }
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_application_input_processing_configuration_not_found(self, kav2):
        """DeleteApplicationInputProcessingConfiguration on nonexistent app raises error."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.delete_application_input_processing_configuration(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
                InputId="1.1",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_application_reference_data_source_not_found(self, kav2):
        """DeleteApplicationReferenceDataSource on nonexistent app raises error."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.delete_application_reference_data_source(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
                ReferenceId="1.1",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_application_vpc_configuration_not_found(self, kav2):
        """DeleteApplicationVpcConfiguration on nonexistent app raises error."""
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kav2.delete_application_vpc_configuration(
                ApplicationName="nonexistent-app-xyz",
                CurrentApplicationVersionId=1,
                VpcConfigurationId="1.1",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_discover_input_schema(self, kav2):
        """DiscoverInputSchema returns an input schema."""
        resp = kav2.discover_input_schema(
            ServiceExecutionRole=ROLE_ARN,
            S3Configuration={
                "BucketARN": "arn:aws:s3:::test-bucket",
                "FileKey": "test.json",
            },
        )
        assert "InputSchema" in resp

    def test_add_application_input(self, kav2):
        """AddApplicationInput adds an input to a SQL app."""
        name, arn, version_id = _create_app(kav2, runtime="SQL-1_0")
        resp = kav2.add_application_input(
            ApplicationName=name,
            CurrentApplicationVersionId=version_id,
            Input={
                "NamePrefix": "SOURCE_SQL_STREAM",
                "KinesisStreamsInput": {
                    "ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/test"
                },
                "InputSchema": {
                    "RecordFormat": {
                        "RecordFormatType": "JSON",
                        "MappingParameters": {"JSONMappingParameters": {"RecordRowPath": "$"}},
                    },
                    "RecordColumns": [
                        {
                            "Name": "col1",
                            "SqlType": "VARCHAR(64)",
                            "Mapping": "$.col1",
                        }
                    ],
                },
            },
        )
        assert "ApplicationARN" in resp

    def test_add_application_reference_data_source(self, kav2):
        """AddApplicationReferenceDataSource adds a reference to a SQL app."""
        name, arn, version_id = _create_app(kav2, runtime="SQL-1_0")
        resp = kav2.add_application_reference_data_source(
            ApplicationName=name,
            CurrentApplicationVersionId=version_id,
            ReferenceDataSource={
                "TableName": "ref_table",
                "S3ReferenceDataSource": {
                    "BucketARN": "arn:aws:s3:::test-bucket",
                    "FileKey": "ref.csv",
                },
                "ReferenceSchema": {
                    "RecordFormat": {"RecordFormatType": "CSV"},
                    "RecordColumns": [
                        {"Name": "col1", "SqlType": "VARCHAR(64)", "Mapping": "col1"}
                    ],
                },
            },
        )
        assert "ApplicationARN" in resp

    def test_add_application_vpc_configuration(self, kav2):
        """AddApplicationVpcConfiguration adds a VPC config."""
        name, arn, version_id = _create_app(kav2)
        resp = kav2.add_application_vpc_configuration(
            ApplicationName=name,
            CurrentApplicationVersionId=version_id,
            VpcConfiguration={
                "SubnetIds": ["subnet-12345678"],
                "SecurityGroupIds": ["sg-12345678"],
            },
        )
        assert "ApplicationARN" in resp
