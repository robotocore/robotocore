"""EMR compatibility tests."""

import json
import uuid
from datetime import datetime

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


class TestEMRRunJobFlowVariants:
    """Tests for RunJobFlow with different configurations."""

    def test_run_job_flow_with_tags(self, emr):
        """RunJobFlow with Tags creates cluster with tags attached."""
        name = _unique("tagged-cluster")
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
            Tags=[
                {"Key": "Env", "Value": "test"},
                {"Key": "App", "Value": "robotocore"},
            ],
        )
        cid = resp["JobFlowId"]
        desc = emr.describe_cluster(ClusterId=cid)
        tags = {t["Key"]: t["Value"] for t in desc["Cluster"]["Tags"]}
        assert tags["Env"] == "test"
        assert tags["App"] == "robotocore"
        emr.terminate_job_flows(JobFlowIds=[cid])

    def test_run_job_flow_with_bootstrap_actions(self, emr):
        """RunJobFlow with BootstrapActions stores them on the cluster."""
        name = _unique("bootstrap-cluster")
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
            BootstrapActions=[
                {
                    "Name": "install-deps",
                    "ScriptBootstrapAction": {
                        "Path": "s3://my-bucket/bootstrap.sh",
                        "Args": ["--verbose"],
                    },
                }
            ],
        )
        cid = resp["JobFlowId"]
        ba = emr.list_bootstrap_actions(ClusterId=cid)
        assert len(ba["BootstrapActions"]) == 1
        assert ba["BootstrapActions"][0]["Name"] == "install-deps"
        emr.terminate_job_flows(JobFlowIds=[cid])

    def test_run_job_flow_with_configurations(self, emr):
        """RunJobFlow with Configurations stores them on the cluster."""
        name = _unique("config-cluster")
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
            Configurations=[
                {
                    "Classification": "spark-defaults",
                    "Properties": {"spark.executor.memory": "2g"},
                }
            ],
        )
        cid = resp["JobFlowId"]
        desc = emr.describe_cluster(ClusterId=cid)
        configs = desc["Cluster"].get("Configurations", [])
        assert len(configs) >= 1
        assert configs[0]["Classification"] == "spark-defaults"
        emr.terminate_job_flows(JobFlowIds=[cid])

    def test_run_job_flow_with_inline_steps(self, emr):
        """RunJobFlow with Steps creates cluster with steps already added."""
        name = _unique("steps-cluster")
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
            Steps=[
                {
                    "Name": "inline-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["echo", "inline"],
                    },
                }
            ],
        )
        cid = resp["JobFlowId"]
        steps = emr.list_steps(ClusterId=cid)
        step_names = [s["Name"] for s in steps["Steps"]]
        assert "inline-step" in step_names
        emr.terminate_job_flows(JobFlowIds=[cid])


class TestEMRDescribeClusterDetails:
    """Tests for detailed DescribeCluster field verification."""

    def test_describe_cluster_release_label(self, emr, cluster_id):
        """DescribeCluster returns the correct ReleaseLabel."""
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["ReleaseLabel"] == "emr-6.10.0"

    def test_describe_cluster_service_role(self, emr, cluster_id):
        """DescribeCluster returns the ServiceRole."""
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["ServiceRole"] == "EMR_DefaultRole"

    def test_describe_cluster_has_arn(self, emr, cluster_id):
        """DescribeCluster includes a valid ClusterArn."""
        desc = emr.describe_cluster(ClusterId=cluster_id)
        arn = desc["Cluster"]["ClusterArn"]
        assert arn.startswith("arn:aws:elasticmapreduce:")
        assert cluster_id in arn

    def test_describe_cluster_termination_protected_default(self, emr, cluster_id):
        """DescribeCluster shows TerminationProtected defaults to False."""
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["TerminationProtected"] is False

    def test_describe_cluster_auto_terminate_default(self, emr, cluster_id):
        """DescribeCluster shows AutoTerminate defaults to False for keep-alive clusters."""
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["AutoTerminate"] is False


class TestEMRListClustersFiltered:
    """Tests for ListClusters with filters."""

    def test_list_clusters_by_state(self, emr, cluster_id):
        """ListClusters with ClusterStates filter returns matching clusters."""
        resp = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        assert "Clusters" in resp
        cluster_ids = [c["Id"] for c in resp["Clusters"]]
        assert cluster_id in cluster_ids

    def test_list_clusters_by_created_after(self, emr, cluster_id):
        """ListClusters with CreatedAfter filter returns clusters."""
        resp = emr.list_clusters(CreatedAfter=datetime(2020, 1, 1))
        assert "Clusters" in resp
        assert len(resp["Clusters"]) >= 1


class TestEMRTagOverwrite:
    """Tests for EMR tag overwrite behavior."""

    def test_add_tags_overwrites_existing(self, emr, cluster_id):
        """AddTags with same key overwrites the value."""
        emr.add_tags(
            ResourceId=cluster_id,
            Tags=[{"Key": "Stage", "Value": "dev"}],
        )
        emr.add_tags(
            ResourceId=cluster_id,
            Tags=[{"Key": "Stage", "Value": "prod"}],
        )
        desc = emr.describe_cluster(ClusterId=cluster_id)
        tags = {t["Key"]: t["Value"] for t in desc["Cluster"]["Tags"]}
        assert tags["Stage"] == "prod"


class TestEMRSecurityConfigEncryption:
    """Tests for security configurations with encryption details."""

    def test_security_config_with_full_encryption(self, emr):
        """Security configuration stores full encryption settings."""
        name = _unique("full-encrypt")
        config = json.dumps(
            {
                "EncryptionConfiguration": {
                    "EnableInTransitEncryption": True,
                    "InTransitEncryptionConfiguration": {
                        "TLSCertificateConfiguration": {
                            "CertificateProviderType": "PEM",
                            "S3Object": "s3://bucket/certs.zip",
                        }
                    },
                    "EnableAtRestEncryption": True,
                    "AtRestEncryptionConfiguration": {
                        "S3EncryptionConfiguration": {"EncryptionMode": "SSE-S3"},
                        "LocalDiskEncryptionConfiguration": {
                            "EncryptionKeyProviderType": "AwsKms",
                            "AwsKmsKey": "arn:aws:kms:us-east-1:123456789012:key/abc",
                        },
                    },
                }
            }
        )
        emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        resp = emr.describe_security_configuration(Name=name)
        parsed = json.loads(resp["SecurityConfiguration"])
        enc = parsed["EncryptionConfiguration"]
        assert enc["EnableInTransitEncryption"] is True
        assert enc["EnableAtRestEncryption"] is True
        emr.delete_security_configuration(Name=name)


class TestEMRModifyInstanceGroupsWithParams:
    """Tests for ModifyInstanceGroups with actual parameters."""

    def test_modify_instance_groups_resize(self, emr, cluster_id):
        """ModifyInstanceGroups resizes an instance group."""
        igs = emr.list_instance_groups(ClusterId=cluster_id)
        assert len(igs["InstanceGroups"]) > 0
        ig_id = igs["InstanceGroups"][0]["Id"]
        resp = emr.modify_instance_groups(
            InstanceGroups=[{"InstanceGroupId": ig_id, "InstanceCount": 2}]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEMRStepDetails:
    """Tests for step configuration details."""

    def test_step_jar_and_args(self, emr, cluster_id):
        """Steps preserve custom Jar path and arguments."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "custom-jar-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "s3://my-bucket/my-jar.jar",
                        "Args": ["arg1", "arg2"],
                    },
                }
            ],
        )
        step_id = add["StepIds"][0]
        step = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        config = step["Step"]["Config"]
        assert config["Jar"] == "s3://my-bucket/my-jar.jar"
        assert config["Args"] == ["arg1", "arg2"]

    def test_step_has_status(self, emr, cluster_id):
        """Steps have a Status with a State field."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "status-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["echo", "hi"],
                    },
                }
            ],
        )
        step_id = add["StepIds"][0]
        step = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        assert "State" in step["Step"]["Status"]


class TestEmrAutoCoverage:
    """Auto-generated coverage tests for emr."""

    @pytest.fixture
    def client(self):
        return make_client("emr")

    def test_get_block_public_access_configuration(self, client):
        """GetBlockPublicAccessConfiguration returns a response."""
        resp = client.get_block_public_access_configuration()
        assert "BlockPublicAccessConfiguration" in resp

    def test_list_release_labels(self, client):
        """ListReleaseLabels returns a response."""
        resp = client.list_release_labels()
        assert "ReleaseLabels" in resp

    def test_modify_instance_groups_no_args(self, client):
        """ModifyInstanceGroups with no args succeeds."""
        resp = client.modify_instance_groups()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEMRSecurityConfigurationCRUD:
    """Tests for EMR security configuration CRUD operations."""

    def test_create_and_describe_security_configuration(self, emr):
        """CreateSecurityConfiguration + DescribeSecurityConfiguration roundtrip."""
        name = _unique("sec-config")
        config = json.dumps({"EncryptionConfiguration": {"EnableInTransitEncryption": False}})
        create_resp = emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        assert create_resp["Name"] == name
        assert "CreationDateTime" in create_resp

        desc_resp = emr.describe_security_configuration(Name=name)
        assert desc_resp["Name"] == name
        assert "SecurityConfiguration" in desc_resp

        emr.delete_security_configuration(Name=name)

    def test_delete_security_configuration(self, emr):
        """DeleteSecurityConfiguration removes the configuration."""
        name = _unique("sec-config")
        config = json.dumps({"EncryptionConfiguration": {"EnableInTransitEncryption": False}})
        emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        del_resp = emr.delete_security_configuration(Name=name)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_security_configuration_nonexistent(self, emr):
        """DescribeSecurityConfiguration for nonexistent name raises error."""
        with pytest.raises(ClientError) as exc:
            emr.describe_security_configuration(Name="nonexistent-config")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )


class TestEMRTerminate:
    """Tests for EMR terminate operations."""

    def test_terminate_job_flows(self, emr):
        """TerminateJobFlows terminates a running cluster."""
        resp = emr.run_job_flow(
            Name=_unique("terminate-test"),
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
        term_resp = emr.terminate_job_flows(JobFlowIds=[cid])
        assert term_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify cluster is terminated
        desc = emr.describe_cluster(ClusterId=cid)
        assert desc["Cluster"]["Status"]["State"] in (
            "TERMINATING",
            "TERMINATED",
            "TERMINATED_WITH_ERRORS",
        )
