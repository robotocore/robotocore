"""DataSync compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def datasync():
    return make_client("datasync")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestDataSyncLocationOperations:
    def test_create_location_s3(self, datasync):
        response = datasync.create_location_s3(
            S3BucketArn="arn:aws:s3:::test-bucket",
            S3Config={"BucketAccessRoleArn": "arn:aws:iam::123456789012:role/datasync-role"},
        )
        arn = response["LocationArn"]
        assert arn.startswith("arn:aws:datasync:")
        assert ":location/loc-" in arn
        # Clean up
        datasync.delete_location(LocationArn=arn)

    def test_describe_location_s3(self, datasync):
        bucket_name = _unique("describe-bucket")
        create_resp = datasync.create_location_s3(
            S3BucketArn=f"arn:aws:s3:::{bucket_name}",
            S3Config={"BucketAccessRoleArn": "arn:aws:iam::123456789012:role/datasync-role"},
        )
        arn = create_resp["LocationArn"]

        desc = datasync.describe_location_s3(LocationArn=arn)
        assert desc["LocationArn"] == arn
        assert bucket_name in desc["LocationUri"]
        assert "S3Config" in desc
        # Clean up
        datasync.delete_location(LocationArn=arn)

    def test_create_location_s3_with_subdirectory(self, datasync):
        create_resp = datasync.create_location_s3(
            S3BucketArn="arn:aws:s3:::subdir-bucket",
            S3Config={"BucketAccessRoleArn": "arn:aws:iam::123456789012:role/datasync-role"},
            Subdirectory="/data/",
        )
        arn = create_resp["LocationArn"]

        desc = datasync.describe_location_s3(LocationArn=arn)
        assert "/data/" in desc["LocationUri"]
        # Clean up
        datasync.delete_location(LocationArn=arn)

    def test_list_locations(self, datasync):
        resp1 = datasync.create_location_s3(
            S3BucketArn=f"arn:aws:s3:::{_unique('list-bucket')}",
            S3Config={"BucketAccessRoleArn": "arn:aws:iam::123456789012:role/datasync-role"},
        )
        arn = resp1["LocationArn"]

        locations = datasync.list_locations()
        arns = [loc["LocationArn"] for loc in locations["Locations"]]
        assert arn in arns
        # Clean up
        datasync.delete_location(LocationArn=arn)

    def test_delete_location(self, datasync):
        create_resp = datasync.create_location_s3(
            S3BucketArn=f"arn:aws:s3:::{_unique('del-bucket')}",
            S3Config={"BucketAccessRoleArn": "arn:aws:iam::123456789012:role/datasync-role"},
        )
        arn = create_resp["LocationArn"]
        datasync.delete_location(LocationArn=arn)

        # Verify it's gone
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_s3(LocationArn=arn)
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_delete_location_removes_from_list(self, datasync):
        create_resp = datasync.create_location_s3(
            S3BucketArn=f"arn:aws:s3:::{_unique('dellist-bucket')}",
            S3Config={"BucketAccessRoleArn": "arn:aws:iam::123456789012:role/datasync-role"},
        )
        arn = create_resp["LocationArn"]
        datasync.delete_location(LocationArn=arn)

        locations = datasync.list_locations()
        arns = [loc["LocationArn"] for loc in locations["Locations"]]
        assert arn not in arns


class TestDataSyncTaskOperations:
    def _create_location(self, client):
        """Helper: create a location for task source/dest."""
        resp = client.create_location_s3(
            S3BucketArn=f"arn:aws:s3:::{_unique('task-bucket')}",
            S3Config={"BucketAccessRoleArn": "arn:aws:iam::123456789012:role/datasync-role"},
        )
        return resp["LocationArn"]

    def test_list_tasks_empty(self, datasync):
        response = datasync.list_tasks()
        assert "Tasks" in response
        assert isinstance(response["Tasks"], list)

    def test_create_task(self, datasync):
        src = self._create_location(datasync)
        dst = self._create_location(datasync)
        try:
            resp = datasync.create_task(
                SourceLocationArn=src,
                DestinationLocationArn=dst,
            )
            task_arn = resp["TaskArn"]
            assert task_arn.startswith("arn:aws:datasync:")
            assert ":task/task-" in task_arn
        finally:
            datasync.delete_location(LocationArn=src)
            datasync.delete_location(LocationArn=dst)

    def test_describe_task(self, datasync):
        src = self._create_location(datasync)
        dst = self._create_location(datasync)
        try:
            task_arn = datasync.create_task(
                SourceLocationArn=src,
                DestinationLocationArn=dst,
            )["TaskArn"]
            desc = datasync.describe_task(TaskArn=task_arn)
            assert desc["TaskArn"] == task_arn
            assert desc["SourceLocationArn"] == src
            assert desc["DestinationLocationArn"] == dst
            assert "Status" in desc
        finally:
            datasync.delete_task(TaskArn=task_arn)
            datasync.delete_location(LocationArn=src)
            datasync.delete_location(LocationArn=dst)

    def test_update_task(self, datasync):
        src = self._create_location(datasync)
        dst = self._create_location(datasync)
        try:
            task_arn = datasync.create_task(
                SourceLocationArn=src,
                DestinationLocationArn=dst,
            )["TaskArn"]
            datasync.update_task(
                TaskArn=task_arn,
                Name="updated-task-name",
            )
            desc = datasync.describe_task(TaskArn=task_arn)
            assert desc["Name"] == "updated-task-name"
        finally:
            datasync.delete_task(TaskArn=task_arn)
            datasync.delete_location(LocationArn=src)
            datasync.delete_location(LocationArn=dst)

    def test_delete_task(self, datasync):
        src = self._create_location(datasync)
        dst = self._create_location(datasync)
        try:
            task_arn = datasync.create_task(
                SourceLocationArn=src,
                DestinationLocationArn=dst,
            )["TaskArn"]
            datasync.delete_task(TaskArn=task_arn)
            with pytest.raises(ClientError) as exc:
                datasync.describe_task(TaskArn=task_arn)
            assert exc.value.response["Error"]["Code"] == "InvalidRequestException"
        finally:
            datasync.delete_location(LocationArn=src)
            datasync.delete_location(LocationArn=dst)


class TestDataSyncTaskExecutions:
    """Tests for task execution operations."""

    def _create_task(self, client):
        src = client.create_location_s3(
            S3BucketArn=f"arn:aws:s3:::{_unique('exec-src')}",
            S3Config={"BucketAccessRoleArn": ("arn:aws:iam::123456789012:role/datasync-role")},
        )["LocationArn"]
        dst = client.create_location_s3(
            S3BucketArn=f"arn:aws:s3:::{_unique('exec-dst')}",
            S3Config={"BucketAccessRoleArn": ("arn:aws:iam::123456789012:role/datasync-role")},
        )["LocationArn"]
        task_arn = client.create_task(SourceLocationArn=src, DestinationLocationArn=dst)["TaskArn"]
        return task_arn, src, dst

    def test_start_task_execution(self, datasync):
        """StartTaskExecution starts an execution for a task."""
        task_arn, src, dst = self._create_task(datasync)
        try:
            resp = datasync.start_task_execution(TaskArn=task_arn)
            assert "TaskExecutionArn" in resp
            assert ":task/task-" in resp["TaskExecutionArn"]
        finally:
            datasync.delete_task(TaskArn=task_arn)
            datasync.delete_location(LocationArn=src)
            datasync.delete_location(LocationArn=dst)

    def test_describe_task_execution(self, datasync):
        """DescribeTaskExecution returns execution details."""
        task_arn, src, dst = self._create_task(datasync)
        try:
            exec_resp = datasync.start_task_execution(TaskArn=task_arn)
            exec_arn = exec_resp["TaskExecutionArn"]
            desc = datasync.describe_task_execution(TaskExecutionArn=exec_arn)
            assert desc["TaskExecutionArn"] == exec_arn
            assert "Status" in desc
        finally:
            datasync.delete_task(TaskArn=task_arn)
            datasync.delete_location(LocationArn=src)
            datasync.delete_location(LocationArn=dst)

    def test_cancel_task_execution(self, datasync):
        """CancelTaskExecution cancels a running execution."""
        task_arn, src, dst = self._create_task(datasync)
        try:
            exec_resp = datasync.start_task_execution(TaskArn=task_arn)
            exec_arn = exec_resp["TaskExecutionArn"]
            cancel_resp = datasync.cancel_task_execution(TaskExecutionArn=exec_arn)
            assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            datasync.delete_task(TaskArn=task_arn)
            datasync.delete_location(LocationArn=src)
            datasync.delete_location(LocationArn=dst)

    def test_list_tasks_contains_created(self, datasync):
        """ListTasks includes a task we just created."""
        task_arn, src, dst = self._create_task(datasync)
        try:
            resp = datasync.list_tasks()
            arns = [t["TaskArn"] for t in resp["Tasks"]]
            assert task_arn in arns
        finally:
            datasync.delete_task(TaskArn=task_arn)
            datasync.delete_location(LocationArn=src)
            datasync.delete_location(LocationArn=dst)


class TestDataSyncLocationSmbOperations:
    """Tests for DataSync SMB location operations."""

    def test_create_and_describe_location_smb(self, datasync):
        """CreateLocationSmb creates a location, DescribeLocationSmb returns its details."""
        create_resp = datasync.create_location_smb(
            Subdirectory="/share",
            ServerHostname="smb.example.com",
            User="admin",
            Password="password123",
            AgentArns=["arn:aws:datasync:us-east-1:123456789012:agent/agent-fake123"],
        )
        arn = create_resp["LocationArn"]
        assert arn.startswith("arn:aws:datasync:")

        desc = datasync.describe_location_smb(LocationArn=arn)
        assert desc["LocationArn"] == arn
        assert "LocationUri" in desc
        assert "AgentArns" in desc
        assert isinstance(desc["AgentArns"], list)
        assert desc["User"] == "admin"

        datasync.delete_location(LocationArn=arn)


class TestDataSyncAgentOperations:
    """Tests for DataSync Agent operations."""

    def test_create_agent(self, datasync):
        """CreateAgent creates an agent and returns an ARN."""
        resp = datasync.create_agent(
            ActivationKey="AAAAA-BBBBB-CCCCC-DDDDD-EEEEE",
            AgentName=_unique("test-agent"),
        )
        assert "AgentArn" in resp
        assert resp["AgentArn"].startswith("arn:aws:datasync:")
        assert ":agent/agent-" in resp["AgentArn"]

    def test_create_agent_appears_in_list(self, datasync):
        """ListAgents includes a freshly created agent."""
        resp = datasync.create_agent(
            ActivationKey="AAAAA-BBBBB-CCCCC-DDDDD-FFFFF",
            AgentName=_unique("list-agent"),
        )
        agent_arn = resp["AgentArn"]
        list_resp = datasync.list_agents()
        assert "Agents" in list_resp
        arns = [a["AgentArn"] for a in list_resp["Agents"]]
        assert agent_arn in arns

    def test_list_agents(self, datasync):
        """ListAgents returns a list."""
        resp = datasync.list_agents()
        assert "Agents" in resp
        assert isinstance(resp["Agents"], list)

    def test_create_agent_and_describe(self, datasync):
        """DescribeAgent returns details for a created agent."""
        name = _unique("desc-agent")
        create_resp = datasync.create_agent(
            ActivationKey="AAAAA-BBBBB-CCCCC-DDDDD-GGGGG",
            AgentName=name,
        )
        agent_arn = create_resp["AgentArn"]
        desc = datasync.describe_agent(AgentArn=agent_arn)
        assert desc["AgentArn"] == agent_arn
        assert desc["Name"] == name
        assert "Status" in desc


class TestDataSyncLocationAzureBlobOperations:
    """Tests for DataSync Azure Blob location operations."""

    def test_create_location_azure_blob(self, datasync):
        """CreateLocationAzureBlob creates a location and returns an ARN."""
        resp = datasync.create_location_azure_blob(
            ContainerUrl="https://myaccount.blob.core.windows.net/mycontainer",
            AuthenticationType="SAS",
            SasConfiguration={
                "Token": "sv=2021-06-08&ss=b&srt=co&sp=rlx&se=2025-01-01T00:00:00Z&sig=fake",
            },
            AgentArns=["arn:aws:datasync:us-east-1:123456789012:agent/agent-fake123"],
        )
        arn = resp["LocationArn"]
        assert arn.startswith("arn:aws:datasync:")
        assert ":location/loc-" in arn
        # Clean up
        datasync.delete_location(LocationArn=arn)

    def test_create_and_describe_location_azure_blob(self, datasync):
        """DescribeLocationAzureBlob returns details for a created location."""
        resp = datasync.create_location_azure_blob(
            ContainerUrl="https://descaccount.blob.core.windows.net/desccontainer",
            AuthenticationType="SAS",
            SasConfiguration={
                "Token": "sv=2021-06-08&ss=b&srt=co&sp=rlx&se=2025-01-01T00:00:00Z&sig=fake2",
            },
            AgentArns=["arn:aws:datasync:us-east-1:123456789012:agent/agent-fake123"],
        )
        arn = resp["LocationArn"]
        desc = datasync.describe_location_azure_blob(LocationArn=arn)
        assert desc["LocationArn"] == arn
        assert "LocationUri" in desc
        assert "AuthenticationType" in desc
        # Clean up
        datasync.delete_location(LocationArn=arn)


class TestDataSyncLocationObjectStorageOperations:
    """Tests for DataSync Object Storage location operations."""

    def test_create_location_object_storage(self, datasync):
        """CreateLocationObjectStorage creates a location and returns an ARN."""
        resp = datasync.create_location_object_storage(
            ServerHostname="s3.example.com",
            BucketName=_unique("objstorage"),
            AgentArns=["arn:aws:datasync:us-east-1:123456789012:agent/agent-fake123"],
        )
        arn = resp["LocationArn"]
        assert arn.startswith("arn:aws:datasync:")
        assert ":location/loc-" in arn
        # Clean up
        datasync.delete_location(LocationArn=arn)

    def test_create_and_describe_location_object_storage(self, datasync):
        """DescribeLocationObjectStorage returns details for a created location."""
        bucket_name = _unique("objstorage-desc")
        resp = datasync.create_location_object_storage(
            ServerHostname="s3.example.com",
            BucketName=bucket_name,
            AgentArns=["arn:aws:datasync:us-east-1:123456789012:agent/agent-fake123"],
        )
        arn = resp["LocationArn"]
        desc = datasync.describe_location_object_storage(LocationArn=arn)
        assert desc["LocationArn"] == arn
        assert "LocationUri" in desc
        assert "AgentArns" in desc
        # Clean up
        datasync.delete_location(LocationArn=arn)

    def test_create_location_object_storage_appears_in_list(self, datasync):
        """ListLocations includes a created Object Storage location."""
        resp = datasync.create_location_object_storage(
            ServerHostname="s3.example.com",
            BucketName=_unique("objstorage-list"),
            AgentArns=["arn:aws:datasync:us-east-1:123456789012:agent/agent-fake123"],
        )
        arn = resp["LocationArn"]
        list_resp = datasync.list_locations()
        arns = [loc["LocationArn"] for loc in list_resp["Locations"]]
        assert arn in arns
        # Clean up
        datasync.delete_location(LocationArn=arn)


class TestDataSyncDescribeLocationVariants:
    """Tests for describe operations on various location types with fake ARNs."""

    def test_describe_agent_nonexistent(self, datasync):
        """DescribeAgent for nonexistent agent raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_agent(
                AgentArn="arn:aws:datasync:us-east-1:123456789012:agent/agent-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_azure_blob_nonexistent(self, datasync):
        """DescribeLocationAzureBlob for nonexistent location raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_azure_blob(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_efs_nonexistent(self, datasync):
        """DescribeLocationEfs for nonexistent location raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_efs(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_fsx_lustre_nonexistent(self, datasync):
        """DescribeLocationFsxLustre for nonexistent location raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_fsx_lustre(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_fsx_ontap_nonexistent(self, datasync):
        """DescribeLocationFsxOntap for nonexistent location raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_fsx_ontap(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_fsx_open_zfs_nonexistent(self, datasync):
        """DescribeLocationFsxOpenZfs for nonexistent location raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_fsx_open_zfs(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_fsx_windows_nonexistent(self, datasync):
        """DescribeLocationFsxWindows for nonexistent location raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_fsx_windows(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_hdfs_nonexistent(self, datasync):
        """DescribeLocationHdfs for nonexistent location raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_hdfs(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_nfs_nonexistent(self, datasync):
        """DescribeLocationNfs for nonexistent location raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_nfs(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_object_storage_nonexistent(self, datasync):
        """DescribeLocationObjectStorage for nonexistent location raises error."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_object_storage(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_describe_location_s3_nonexistent(self, datasync):
        """DescribeLocationS3 for nonexistent location raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            datasync.describe_location_s3(
                LocationArn="arn:aws:datasync:us-east-1:123456789012:location/loc-00000000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"
