"""EMR compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def emr():
    return make_client("emr")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def cluster_id(emr):
    resp = emr.run_job_flow(
        Name=_unique("test-cluster"),
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "SlaveInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cid = resp["JobFlowId"]
    yield cid
    try:
        emr.terminate_job_flows(JobFlowIds=[cid])
    except Exception:
        pass


class TestEMRClusterOperations:
    def test_run_job_flow(self, emr):
        name = _unique("test-cluster")
        resp = emr.run_job_flow(
            Name=name,
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )
        assert "JobFlowId" in resp
        cluster_id = resp["JobFlowId"]
        assert cluster_id.startswith("j-")
        emr.terminate_job_flows(JobFlowIds=[cluster_id])

    def test_list_clusters(self, emr, cluster_id):
        resp = emr.list_clusters()
        cluster_ids = [c["Id"] for c in resp["Clusters"]]
        assert cluster_id in cluster_ids

    def test_describe_cluster(self, emr, cluster_id):
        resp = emr.describe_cluster(ClusterId=cluster_id)
        cluster = resp["Cluster"]
        assert cluster["Id"] == cluster_id
        assert "Name" in cluster
        assert "Status" in cluster

    def test_list_steps(self, emr, cluster_id):
        resp = emr.list_steps(ClusterId=cluster_id)
        assert "Steps" in resp

    def test_list_instance_groups(self, emr, cluster_id):
        resp = emr.list_instance_groups(ClusterId=cluster_id)
        assert "InstanceGroups" in resp

    def test_terminate_job_flows(self, emr):
        resp = emr.run_job_flow(
            Name=_unique("term-cluster"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )
        cluster_id = resp["JobFlowId"]
        emr.terminate_job_flows(JobFlowIds=[cluster_id])
        desc = emr.describe_cluster(ClusterId=cluster_id)
        status = desc["Cluster"]["Status"]["State"]
        assert status in ("TERMINATING", "TERMINATED")


class TestEMRSecurityConfiguration:
    def test_create_security_configuration(self, emr):
        name = _unique("sec-config")
        config = json.dumps(
            {
                "EncryptionConfiguration": {
                    "EnableInTransitEncryption": False,
                    "EnableAtRestEncryption": False,
                }
            }
        )
        resp = emr.create_security_configuration(
            Name=name,
            SecurityConfiguration=config,
        )
        assert resp["Name"] == name
        assert "CreationDateTime" in resp
        emr.delete_security_configuration(Name=name)

    def test_describe_security_configuration(self, emr):
        name = _unique("sec-config")
        config = json.dumps(
            {
                "EncryptionConfiguration": {
                    "EnableInTransitEncryption": False,
                    "EnableAtRestEncryption": False,
                }
            }
        )
        emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        resp = emr.describe_security_configuration(Name=name)
        assert resp["Name"] == name
        assert "SecurityConfiguration" in resp
        emr.delete_security_configuration(Name=name)

    def test_delete_security_configuration(self, emr):
        name = _unique("sec-config")
        config = json.dumps(
            {
                "EncryptionConfiguration": {
                    "EnableInTransitEncryption": False,
                    "EnableAtRestEncryption": False,
                }
            }
        )
        emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        emr.delete_security_configuration(Name=name)
        with pytest.raises(ClientError):
            emr.describe_security_configuration(Name=name)


class TestEMRTags:
    def test_add_tags_to_cluster(self, emr, cluster_id):
        _cluster_arn = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]["ClusterArn"]
        emr.add_tags(
            ResourceId=cluster_id,
            Tags=[
                {"Key": "Environment", "Value": "test"},
                {"Key": "Project", "Value": "robotocore"},
            ],
        )
        desc = emr.describe_cluster(ClusterId=cluster_id)
        tags = {t["Key"]: t["Value"] for t in desc["Cluster"]["Tags"]}
        assert tags["Environment"] == "test"
        assert tags["Project"] == "robotocore"


class TestEmrAutoCoverage:
    """Auto-generated coverage tests for emr."""

    @pytest.fixture
    def client(self):
        return make_client("emr")

    def test_describe_job_flows(self, client):
        """DescribeJobFlows returns a response."""
        resp = client.describe_job_flows()
        assert "JobFlows" in resp

    def test_get_block_public_access_configuration(self, client):
        """GetBlockPublicAccessConfiguration returns a response."""
        resp = client.get_block_public_access_configuration()
        assert "BlockPublicAccessConfiguration" in resp

    def test_list_release_labels(self, client):
        """ListReleaseLabels returns a response."""
        resp = client.list_release_labels()
        assert "ReleaseLabels" in resp

    def test_modify_instance_groups(self, client):
        """ModifyInstanceGroups returns a response."""
        client.modify_instance_groups()
