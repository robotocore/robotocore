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

    def test_list_clusters(self, emr):
        resp = emr.list_clusters()
        assert "Clusters" in resp
        assert isinstance(resp["Clusters"], list)

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


class TestEMRStepOperations:
    """Tests for EMR step CRUD operations."""

    def test_add_job_flow_steps(self, emr, cluster_id):
        """AddJobFlowSteps adds steps and returns step IDs."""
        resp = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "test-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["echo", "hello"],
                    },
                }
            ],
        )
        assert "StepIds" in resp
        assert len(resp["StepIds"]) == 1

    def test_describe_step(self, emr, cluster_id):
        """DescribeStep returns step details after adding a step."""
        add_resp = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "describe-me",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["echo", "test"],
                    },
                }
            ],
        )
        step_id = add_resp["StepIds"][0]
        resp = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        assert "Step" in resp
        assert resp["Step"]["Id"] == step_id
        assert resp["Step"]["Name"] == "describe-me"

    def test_add_multiple_steps(self, emr, cluster_id):
        """AddJobFlowSteps with multiple steps returns all step IDs."""
        resp = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "step-1",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "1"]},
                },
                {
                    "Name": "step-2",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "2"]},
                },
            ],
        )
        assert len(resp["StepIds"]) == 2

    def test_list_steps_after_add(self, emr, cluster_id):
        """ListSteps shows steps after they are added."""
        emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "listed-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "hi"]},
                }
            ],
        )
        resp = emr.list_steps(ClusterId=cluster_id)
        step_names = [s["Name"] for s in resp["Steps"]]
        assert "listed-step" in step_names


class TestEMRInstanceOperations:
    """Tests for EMR instance and instance group operations."""

    def test_list_instances(self, emr, cluster_id):
        """ListInstances returns instances for a cluster."""
        resp = emr.list_instances(ClusterId=cluster_id)
        assert "Instances" in resp
        assert isinstance(resp["Instances"], list)

    def test_list_bootstrap_actions(self, emr, cluster_id):
        """ListBootstrapActions returns bootstrap action list."""
        resp = emr.list_bootstrap_actions(ClusterId=cluster_id)
        assert "BootstrapActions" in resp
        assert isinstance(resp["BootstrapActions"], list)

    def test_add_instance_groups(self, emr, cluster_id):
        """AddInstanceGroups adds a TASK instance group."""
        emr.add_instance_groups(
            InstanceGroups=[
                {
                    "Name": "task-group",
                    "InstanceRole": "TASK",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                }
            ],
            JobFlowId=cluster_id,
        )
        # Verify the group was added
        igs = emr.list_instance_groups(ClusterId=cluster_id)
        names = [g["Name"] for g in igs["InstanceGroups"]]
        assert "task-group" in names

    def test_list_instance_groups_after_add(self, emr, cluster_id):
        """ListInstanceGroups includes groups added via AddInstanceGroups."""
        emr.add_instance_groups(
            InstanceGroups=[
                {
                    "Name": "extra-task",
                    "InstanceRole": "TASK",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                }
            ],
            JobFlowId=cluster_id,
        )
        resp = emr.list_instance_groups(ClusterId=cluster_id)
        group_names = [g["Name"] for g in resp["InstanceGroups"]]
        assert "extra-task" in group_names


class TestEMRClusterSettings:
    """Tests for EMR cluster configuration operations."""

    def test_set_termination_protection(self, emr, cluster_id):
        """SetTerminationProtection enables and verifies protection."""
        emr.set_termination_protection(JobFlowIds=[cluster_id], TerminationProtected=True)
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["TerminationProtected"] is True
        # Reset so fixture cleanup works
        emr.set_termination_protection(JobFlowIds=[cluster_id], TerminationProtected=False)

    def test_set_visible_to_all_users(self, emr, cluster_id):
        """SetVisibleToAllUsers updates cluster visibility."""
        emr.set_visible_to_all_users(JobFlowIds=[cluster_id], VisibleToAllUsers=True)
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["VisibleToAllUsers"] is True

    def test_modify_cluster_step_concurrency(self, emr, cluster_id):
        """ModifyCluster updates step concurrency level."""
        resp = emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=2)
        assert "StepConcurrencyLevel" in resp
        assert resp["StepConcurrencyLevel"] == 2


class TestEMRTagRemoval:
    """Tests for EMR tag removal."""

    def test_remove_tags(self, emr, cluster_id):
        """RemoveTags removes previously added tags."""
        emr.add_tags(
            ResourceId=cluster_id,
            Tags=[
                {"Key": "ToRemove", "Value": "yes"},
                {"Key": "ToKeep", "Value": "yes"},
            ],
        )
        emr.remove_tags(ResourceId=cluster_id, TagKeys=["ToRemove"])
        desc = emr.describe_cluster(ClusterId=cluster_id)
        tags = {t["Key"]: t["Value"] for t in desc["Cluster"]["Tags"]}
        assert "ToRemove" not in tags
        assert tags.get("ToKeep") == "yes"


class TestEMRBlockPublicAccess:
    """Tests for EMR block public access configuration."""

    def test_put_block_public_access_configuration(self, emr):
        """PutBlockPublicAccessConfiguration sets the configuration."""
        emr.put_block_public_access_configuration(
            BlockPublicAccessConfiguration={
                "BlockPublicSecurityGroupRules": True,
                "PermittedPublicSecurityGroupRuleRanges": [],
            }
        )
        resp = emr.get_block_public_access_configuration()
        config = resp["BlockPublicAccessConfiguration"]
        assert config["BlockPublicSecurityGroupRules"] is True


class TestEMRAutoScaling:
    """Tests for EMR auto-scaling policy operations."""

    def test_put_auto_scaling_policy(self, emr, cluster_id):
        """PutAutoScalingPolicy attaches a scaling policy to an instance group."""
        igs = emr.list_instance_groups(ClusterId=cluster_id)
        assert len(igs["InstanceGroups"]) > 0
        ig_id = igs["InstanceGroups"][0]["Id"]
        resp = emr.put_auto_scaling_policy(
            ClusterId=cluster_id,
            InstanceGroupId=ig_id,
            AutoScalingPolicy={
                "Constraints": {"MinCapacity": 1, "MaxCapacity": 5},
                "Rules": [
                    {
                        "Name": "scale-out",
                        "Action": {
                            "SimpleScalingPolicyConfiguration": {
                                "ScalingAdjustment": 1,
                                "AdjustmentType": "CHANGE_IN_CAPACITY",
                            }
                        },
                        "Trigger": {
                            "CloudWatchAlarmDefinition": {
                                "ComparisonOperator": "GREATER_THAN",
                                "MetricName": "YARNMemoryAvailablePercentage",
                                "Period": 300,
                                "Statistic": "AVERAGE",
                                "Threshold": 15.0,
                                "Unit": "PERCENT",
                            }
                        },
                    }
                ],
            },
        )
        assert "AutoScalingPolicy" in resp or "ClusterId" in resp

    def test_remove_auto_scaling_policy(self, emr, cluster_id):
        """RemoveAutoScalingPolicy removes the scaling policy."""
        igs = emr.list_instance_groups(ClusterId=cluster_id)
        ig_id = igs["InstanceGroups"][0]["Id"]
        # Add policy first
        emr.put_auto_scaling_policy(
            ClusterId=cluster_id,
            InstanceGroupId=ig_id,
            AutoScalingPolicy={
                "Constraints": {"MinCapacity": 1, "MaxCapacity": 5},
                "Rules": [
                    {
                        "Name": "scale-out",
                        "Action": {
                            "SimpleScalingPolicyConfiguration": {
                                "ScalingAdjustment": 1,
                                "AdjustmentType": "CHANGE_IN_CAPACITY",
                            }
                        },
                        "Trigger": {
                            "CloudWatchAlarmDefinition": {
                                "ComparisonOperator": "GREATER_THAN",
                                "MetricName": "YARNMemoryAvailablePercentage",
                                "Period": 300,
                                "Statistic": "AVERAGE",
                                "Threshold": 15.0,
                                "Unit": "PERCENT",
                            }
                        },
                    }
                ],
            },
        )
        resp = emr.remove_auto_scaling_policy(ClusterId=cluster_id, InstanceGroupId=ig_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEMRListSupportedInstanceTypes:
    """Tests for EMR ListSupportedInstanceTypes."""

    def test_list_supported_instance_types(self, emr):
        """ListSupportedInstanceTypes returns instance type list."""
        resp = emr.list_supported_instance_types(ReleaseLabel="emr-6.10.0")
        assert "SupportedInstanceTypes" in resp
        assert isinstance(resp["SupportedInstanceTypes"], list)


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
