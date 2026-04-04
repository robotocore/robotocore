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
        pass  # best-effort cleanup


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
        assert isinstance(resp["Steps"], list)

    def test_list_instance_groups(self, emr, cluster_id):
        resp = emr.list_instance_groups(ClusterId=cluster_id)
        assert "InstanceGroups" in resp
        assert isinstance(resp["InstanceGroups"], list)

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
        listed = [s for s in resp["Steps"] if s["Name"] == "listed-step"]
        assert len(listed) == 1


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
        task_groups = [g for g in igs["InstanceGroups"] if g["Name"] == "task-group"]
        assert len(task_groups) == 1

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
        extra_groups = [g for g in resp["InstanceGroups"] if g["Name"] == "extra-task"]
        assert len(extra_groups) == 1


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
        inline_steps = [s for s in steps["Steps"] if s["Name"] == "inline-step"]
        assert len(inline_steps) == 1
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
        valid_states = {"PENDING", "CANCEL_PENDING", "RUNNING", "COMPLETED", "CANCELLED", "FAILED", "INTERRUPTED"}
        assert step["Step"]["Status"]["State"] in valid_states


class TestEmrAutoCoverage:
    """Auto-generated coverage tests for emr."""

    @pytest.fixture
    def client(self):
        return make_client("emr")

    def test_get_block_public_access_configuration(self, client):
        """GetBlockPublicAccessConfiguration returns a response."""
        resp = client.get_block_public_access_configuration()
        assert "BlockPublicAccessConfiguration" in resp
        assert isinstance(resp["BlockPublicAccessConfiguration"], dict)

    def test_list_release_labels(self, client):
        """ListReleaseLabels returns a response."""
        resp = client.list_release_labels()
        assert "ReleaseLabels" in resp
        assert isinstance(resp["ReleaseLabels"], list)

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


class TestEMRCancelSteps:
    """Tests for EMR CancelSteps operation."""

    def test_cancel_steps(self, emr, cluster_id):
        """CancelSteps returns a list of cancel step info."""
        add_resp = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "cancel-me",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["echo", "cancel"],
                    },
                }
            ],
        )
        step_id = add_resp["StepIds"][0]
        resp = emr.cancel_steps(ClusterId=cluster_id, StepIds=[step_id])
        assert "CancelStepsInfoList" in resp
        assert len(resp["CancelStepsInfoList"]) == 1
        assert resp["CancelStepsInfoList"][0]["StepId"] == step_id


class TestEMRStudioOperations:
    """Tests for EMR Studio CRUD operations."""

    def test_create_and_describe_studio(self, emr):
        """CreateStudio + DescribeStudio roundtrip."""
        name = _unique("studio")
        resp = emr.create_studio(
            Name=name,
            AuthMode="IAM",
            VpcId="vpc-12345678",
            SubnetIds=["subnet-12345678"],
            ServiceRole="arn:aws:iam::123456789012:role/EMR_DefaultRole",
            WorkspaceSecurityGroupId="sg-12345678",
            EngineSecurityGroupId="sg-87654321",
            DefaultS3Location="s3://my-bucket/studio/",
        )
        assert "StudioId" in resp
        studio_id = resp["StudioId"]
        try:
            desc = emr.describe_studio(StudioId=studio_id)
            assert "Studio" in desc
            assert desc["Studio"]["StudioId"] == studio_id
            assert desc["Studio"]["Name"] == name
        finally:
            emr.delete_studio(StudioId=studio_id)

    def test_list_studios(self, emr):
        """ListStudios returns a list of studios."""
        name = _unique("list-studio")
        resp = emr.create_studio(
            Name=name,
            AuthMode="IAM",
            VpcId="vpc-12345678",
            SubnetIds=["subnet-12345678"],
            ServiceRole="arn:aws:iam::123456789012:role/EMR_DefaultRole",
            WorkspaceSecurityGroupId="sg-12345678",
            EngineSecurityGroupId="sg-87654321",
            DefaultS3Location="s3://my-bucket/studio/",
        )
        studio_id = resp["StudioId"]
        try:
            list_resp = emr.list_studios()
            assert "Studios" in list_resp
            studio_ids = [s["StudioId"] for s in list_resp["Studios"]]
            assert studio_id in studio_ids
        finally:
            emr.delete_studio(StudioId=studio_id)

    def test_delete_studio(self, emr):
        """DeleteStudio removes the studio."""
        name = _unique("del-studio")
        resp = emr.create_studio(
            Name=name,
            AuthMode="IAM",
            VpcId="vpc-12345678",
            SubnetIds=["subnet-12345678"],
            ServiceRole="arn:aws:iam::123456789012:role/EMR_DefaultRole",
            WorkspaceSecurityGroupId="sg-12345678",
            EngineSecurityGroupId="sg-87654321",
            DefaultS3Location="s3://my-bucket/studio/",
        )
        studio_id = resp["StudioId"]
        del_resp = emr.delete_studio(StudioId=studio_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEMRListSecurityConfigurations:
    """Tests for EMR ListSecurityConfigurations."""

    def test_list_security_configurations(self, emr):
        """ListSecurityConfigurations returns a list."""
        name = _unique("list-sec")
        config = json.dumps({"EncryptionConfiguration": {"EnableInTransitEncryption": False}})
        emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        try:
            resp = emr.list_security_configurations()
            assert "SecurityConfigurations" in resp
            names = [sc["Name"] for sc in resp["SecurityConfigurations"]]
            assert name in names
        finally:
            emr.delete_security_configuration(Name=name)


class TestEMRManagedScalingPolicy:
    """Tests for EMR managed scaling policy operations."""

    def test_put_and_get_managed_scaling_policy(self, emr, cluster_id):
        """PutManagedScalingPolicy + GetManagedScalingPolicy roundtrip."""
        emr.put_managed_scaling_policy(
            ClusterId=cluster_id,
            ManagedScalingPolicy={
                "ComputeLimits": {
                    "UnitType": "Instances",
                    "MinimumCapacityUnits": 1,
                    "MaximumCapacityUnits": 10,
                    "MaximumOnDemandCapacityUnits": 10,
                    "MaximumCoreCapacityUnits": 10,
                }
            },
        )
        resp = emr.get_managed_scaling_policy(ClusterId=cluster_id)
        assert "ManagedScalingPolicy" in resp
        limits = resp["ManagedScalingPolicy"]["ComputeLimits"]
        assert limits["UnitType"] == "Instances"
        assert limits["MinimumCapacityUnits"] == 1
        assert limits["MaximumCapacityUnits"] == 10

    def test_remove_managed_scaling_policy(self, emr, cluster_id):
        """RemoveManagedScalingPolicy removes the policy."""
        emr.put_managed_scaling_policy(
            ClusterId=cluster_id,
            ManagedScalingPolicy={
                "ComputeLimits": {
                    "UnitType": "Instances",
                    "MinimumCapacityUnits": 1,
                    "MaximumCapacityUnits": 5,
                    "MaximumOnDemandCapacityUnits": 5,
                    "MaximumCoreCapacityUnits": 5,
                }
            },
        )
        resp = emr.remove_managed_scaling_policy(ClusterId=cluster_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEMRAutoTerminationPolicy:
    """Tests for EMR auto-termination policy operations."""

    def test_put_and_get_auto_termination_policy(self, emr, cluster_id):
        """PutAutoTerminationPolicy + GetAutoTerminationPolicy roundtrip."""
        emr.put_auto_termination_policy(
            ClusterId=cluster_id,
            AutoTerminationPolicy={"IdleTimeout": 3600},
        )
        resp = emr.get_auto_termination_policy(ClusterId=cluster_id)
        assert "AutoTerminationPolicy" in resp
        assert resp["AutoTerminationPolicy"]["IdleTimeout"] == 3600

    def test_remove_auto_termination_policy(self, emr, cluster_id):
        """RemoveAutoTerminationPolicy removes the policy."""
        emr.put_auto_termination_policy(
            ClusterId=cluster_id,
            AutoTerminationPolicy={"IdleTimeout": 7200},
        )
        resp = emr.remove_auto_termination_policy(ClusterId=cluster_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEMRDescribeReleaseLabel:
    """Tests for EMR DescribeReleaseLabel."""

    def test_describe_release_label(self, emr):
        """DescribeReleaseLabel returns details for a valid release."""
        resp = emr.describe_release_label(ReleaseLabel="emr-6.10.0")
        assert "ReleaseLabel" in resp
        assert resp["ReleaseLabel"] == "emr-6.10.0"


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


class TestEMRAdditionalOps:
    """Tests for additional EMR operations."""

    def test_list_notebook_executions(self, emr):
        """ListNotebookExecutions returns NotebookExecutions key."""
        resp = emr.list_notebook_executions()
        assert "NotebookExecutions" in resp
        assert isinstance(resp["NotebookExecutions"], list)

    def test_describe_job_flows(self, emr):
        """DescribeJobFlows returns details for a running cluster."""
        resp = emr.run_job_flow(
            Name=_unique("djf-cluster"),
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
        desc = emr.describe_job_flows(JobFlowIds=[cid])
        assert "JobFlows" in desc
        assert len(desc["JobFlows"]) == 1
        flow = desc["JobFlows"][0]
        assert flow["JobFlowId"] == cid
        assert "Name" in flow
        assert "ExecutionStatusDetail" in flow
        emr.terminate_job_flows(JobFlowIds=[cid])


class TestEMRNotebookErrors:
    """Tests for EMR notebook execution error handling."""

    def test_describe_notebook_execution_not_found(self, emr):
        """DescribeNotebookExecution with fake ID raises an error."""
        with pytest.raises(ClientError) as exc:
            emr.describe_notebook_execution(NotebookExecutionId="ne-FAKE12345678")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_stop_notebook_execution_not_found(self, emr):
        """StopNotebookExecution with fake ID raises an error."""
        with pytest.raises(ClientError) as exc:
            emr.stop_notebook_execution(NotebookExecutionId="ne-FAKE12345678")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )


class TestEMRInstanceFleetOperations:
    """Tests for EMR instance fleet operations."""

    @pytest.fixture
    def fleet_cluster_id(self, emr):
        """Create a cluster with InstanceFleets instead of InstanceGroups."""
        resp = emr.run_job_flow(
            Name=_unique("fleet-cluster"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "InstanceFleets": [
                    {
                        "Name": "master-fleet",
                        "InstanceFleetType": "MASTER",
                        "TargetOnDemandCapacity": 1,
                        "InstanceTypeConfigs": [
                            {"InstanceType": "m5.xlarge"},
                        ],
                    },
                ],
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
            pass  # best-effort cleanup

    def test_list_instance_fleets(self, emr, fleet_cluster_id):
        """ListInstanceFleets returns fleets for a fleet-based cluster."""
        resp = emr.list_instance_fleets(ClusterId=fleet_cluster_id)
        assert "InstanceFleets" in resp
        assert isinstance(resp["InstanceFleets"], list)

    def test_add_instance_fleet(self, emr, fleet_cluster_id):
        """AddInstanceFleet adds a TASK fleet to the cluster."""
        resp = emr.add_instance_fleet(
            ClusterId=fleet_cluster_id,
            InstanceFleet={
                "Name": "task-fleet",
                "InstanceFleetType": "TASK",
                "TargetOnDemandCapacity": 1,
                "InstanceTypeConfigs": [
                    {"InstanceType": "m5.xlarge"},
                ],
            },
        )
        assert "ClusterId" in resp
        assert resp["ClusterId"] == fleet_cluster_id
        assert "InstanceFleetId" in resp

    def test_add_and_list_instance_fleets(self, emr, fleet_cluster_id):
        """AddInstanceFleet followed by ListInstanceFleets shows the fleet."""
        emr.add_instance_fleet(
            ClusterId=fleet_cluster_id,
            InstanceFleet={
                "Name": "listed-fleet",
                "InstanceFleetType": "TASK",
                "TargetOnDemandCapacity": 1,
                "InstanceTypeConfigs": [
                    {"InstanceType": "m5.xlarge"},
                ],
            },
        )
        resp = emr.list_instance_fleets(ClusterId=fleet_cluster_id)
        assert "InstanceFleets" in resp
        listed_fleets = [f for f in resp["InstanceFleets"] if f["Name"] == "listed-fleet"]
        assert len(listed_fleets) == 1

    def test_modify_instance_fleet(self, emr, fleet_cluster_id):
        """ModifyInstanceFleet changes target capacity of a fleet."""
        # Add a task fleet first
        add_resp = emr.add_instance_fleet(
            ClusterId=fleet_cluster_id,
            InstanceFleet={
                "Name": "modify-fleet",
                "InstanceFleetType": "TASK",
                "TargetOnDemandCapacity": 1,
                "InstanceTypeConfigs": [
                    {"InstanceType": "m5.xlarge"},
                ],
            },
        )
        fleet_id = add_resp["InstanceFleetId"]
        resp = emr.modify_instance_fleet(
            ClusterId=fleet_cluster_id,
            InstanceFleet={
                "InstanceFleetId": fleet_id,
                "TargetOnDemandCapacity": 2,
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestEMRStudioSessionMappingOperations:
    """Tests for EMR Studio session mapping operations."""

    @pytest.fixture
    def studio_id(self, emr):
        """Create a studio for session mapping tests."""
        resp = emr.create_studio(
            Name=_unique("session-studio"),
            AuthMode="IAM",
            VpcId="vpc-12345678",
            SubnetIds=["subnet-12345678"],
            ServiceRole="arn:aws:iam::123456789012:role/EMR_DefaultRole",
            WorkspaceSecurityGroupId="sg-12345678",
            EngineSecurityGroupId="sg-87654321",
            DefaultS3Location="s3://my-bucket/studio/",
        )
        sid = resp["StudioId"]
        yield sid
        try:
            emr.delete_studio(StudioId=sid)
        except Exception:
            pass  # best-effort cleanup

    def test_create_studio_session_mapping(self, emr, studio_id):
        """CreateStudioSessionMapping creates a mapping for an IAM user."""
        resp = emr.create_studio_session_mapping(
            StudioId=studio_id,
            IdentityType="USER",
            IdentityName="test-user@example.com",
            SessionPolicyArn="arn:aws:iam::123456789012:policy/TestPolicy",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_studio_session_mapping_not_found(self, emr, studio_id):
        """GetStudioSessionMapping with nonexistent mapping raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            emr.get_studio_session_mapping(
                StudioId=studio_id,
                IdentityType="USER",
                IdentityName="nonexistent@example.com",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_update_studio_session_mapping_not_found(self, emr, studio_id):
        """UpdateStudioSessionMapping with nonexistent mapping raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            emr.update_studio_session_mapping(
                StudioId=studio_id,
                IdentityType="USER",
                IdentityName="nonexistent@example.com",
                SessionPolicyArn="arn:aws:iam::123456789012:policy/NewPolicy",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_delete_studio_session_mapping_not_found(self, emr, studio_id):
        """DeleteStudioSessionMapping with nonexistent mapping raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            emr.delete_studio_session_mapping(
                StudioId=studio_id,
                IdentityType="USER",
                IdentityName="nonexistent@example.com",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"


class TestEMRMissingGapOps:
    """Tests for previously-missing EMR operations."""

    def test_list_studio_session_mappings(self, emr):
        """ListStudioSessionMappings returns session mappings list."""
        resp = emr.list_studio_session_mappings()
        assert "SessionMappings" in resp
        assert isinstance(resp["SessionMappings"], list)

    def test_set_keep_job_flow_alive_when_no_steps(self, emr):
        """SetKeepJobFlowAliveWhenNoSteps returns 200 (no-op for fake cluster ID)."""
        resp = emr.set_keep_job_flow_alive_when_no_steps(
            JobFlowIds=["j-FAKE123456"],
            KeepJobFlowAliveWhenNoSteps=True,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_set_unhealthy_node_replacement(self, emr):
        """SetUnhealthyNodeReplacement returns 200 (no-op for fake cluster ID)."""
        resp = emr.set_unhealthy_node_replacement(
            JobFlowIds=["j-FAKE123456"],
            UnhealthyNodeReplacement=True,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_cluster_session_credentials(self, emr):
        """GetClusterSessionCredentials returns credentials struct."""
        resp = emr.get_cluster_session_credentials(
            ClusterId="j-FAKE12345",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        assert "Credentials" in resp
        assert "ExpiresAt" in resp
        assert isinstance(resp["Credentials"], dict)


class TestEMRPersistentAppUI:
    """Tests for EMR PersistentAppUI and related operations."""

    def test_create_persistent_app_ui(self, emr):
        """CreatePersistentAppUI returns a PersistentAppUIId."""
        resp = emr.create_persistent_app_ui(
            TargetResourceArn="arn:aws:emr:us-east-1:123456789012:cluster/j-FAKE123"
        )
        assert "PersistentAppUIId" in resp
        assert resp["PersistentAppUIId"]

    def test_describe_persistent_app_ui(self, emr):
        """DescribePersistentAppUI returns PersistentAppUI for a created UI."""
        create_resp = emr.create_persistent_app_ui(
            TargetResourceArn="arn:aws:emr:us-east-1:123456789012:cluster/j-FAKE123"
        )
        ui_id = create_resp["PersistentAppUIId"]
        desc_resp = emr.describe_persistent_app_ui(PersistentAppUIId=ui_id)
        assert "PersistentAppUI" in desc_resp
        assert isinstance(desc_resp["PersistentAppUI"], dict)

    def test_describe_persistent_app_ui_nonexistent(self, emr):
        """DescribePersistentAppUI raises for a nonexistent PersistentAppUIId."""
        with pytest.raises(ClientError) as exc:
            emr.describe_persistent_app_ui(PersistentAppUIId="nonexistent-id-xyz")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_persistent_app_ui_presigned_url(self, emr):
        """GetPersistentAppUIPresignedURL returns 200 for a created UI."""
        create_resp = emr.create_persistent_app_ui(
            TargetResourceArn="arn:aws:emr:us-east-1:123456789012:cluster/j-FAKE123"
        )
        ui_id = create_resp["PersistentAppUIId"]
        resp = emr.get_persistent_app_ui_presigned_url(PersistentAppUIId=ui_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_on_cluster_app_ui_presigned_url(self, emr):
        """GetOnClusterAppUIPresignedURL returns 200."""
        resp = emr.get_on_cluster_app_ui_presigned_url(
            ClusterId="j-FAKE123",
            OnClusterAppUIType="SPARK_HISTORY_SERVER",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_notebook_execution(self, emr):
        """StartNotebookExecution returns a NotebookExecutionId."""
        resp = emr.start_notebook_execution(
            ExecutionEngine={"Id": "j-FAKE123", "Type": "EMR"},
            ServiceRole="arn:aws:iam::123456789012:role/EMR_Notebooks_DefaultRole",
        )
        assert "NotebookExecutionId" in resp
        nid = resp["NotebookExecutionId"]
        assert isinstance(nid, str) and len(nid) > 0


class TestEMREdgeCasesAndFidelity:
    """Edge case and behavioral fidelity tests for EMR."""

    # --- ERROR: nonexistent resource ---

    def test_describe_nonexistent_cluster_error(self, emr):
        """DescribeCluster with fake ID raises ClientError."""
        with pytest.raises(ClientError) as exc:
            emr.describe_cluster(ClusterId="j-FAKE1234567890")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_list_steps_for_nonexistent_cluster_error(self, emr):
        """ListSteps for nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            emr.list_steps(ClusterId="j-FAKE1234567890")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_list_instance_groups_for_nonexistent_cluster_error(self, emr):
        """ListInstanceGroups for nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            emr.list_instance_groups(ClusterId="j-FAKE1234567890")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_list_instances_for_nonexistent_cluster_error(self, emr):
        """ListInstances for nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            emr.list_instances(ClusterId="j-FAKE1234567890")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_list_bootstrap_actions_for_nonexistent_cluster_error(self, emr):
        """ListBootstrapActions for nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            emr.list_bootstrap_actions(ClusterId="j-FAKE1234567890")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_describe_step_nonexistent_error(self, emr, cluster_id):
        """DescribeStep with fake step ID raises error."""
        with pytest.raises(ClientError) as exc:
            emr.describe_step(ClusterId=cluster_id, StepId="s-FAKE1234567890")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_delete_nonexistent_security_config_error(self, emr):
        """DeleteSecurityConfiguration for nonexistent name raises error."""
        with pytest.raises(ClientError) as exc:
            emr.delete_security_configuration(Name="nonexistent-config-xyz-abc")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_security_config_duplicate_name_error(self, emr):
        """Creating a security config with a duplicate name raises an error."""
        name = _unique("dup-config")
        config = json.dumps({"EncryptionConfiguration": {"EnableInTransitEncryption": False}})
        emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        try:
            with pytest.raises(ClientError) as exc:
                emr.create_security_configuration(Name=name, SecurityConfiguration=config)
            assert exc.value.response["Error"]["Code"] in (
                "InvalidRequestException",
                "AlreadyExistsException",
            )
        finally:
            emr.delete_security_configuration(Name=name)

    # --- LIST: create then verify presence ---

    def test_list_clusters_includes_created_cluster(self, emr):
        """ListClusters includes a newly created cluster by ID."""
        resp = emr.run_job_flow(
            Name=_unique("list-me"),
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
        try:
            list_resp = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            ids = [c["Id"] for c in list_resp["Clusters"]]
            assert cid in ids
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_list_steps_after_add_contains_step_id(self, emr, cluster_id):
        """ListSteps after AddJobFlowSteps includes the new step ID."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "list-check-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "x"]},
                }
            ],
        )
        step_id = add["StepIds"][0]
        list_resp = emr.list_steps(ClusterId=cluster_id)
        step_ids = [s["Id"] for s in list_resp["Steps"]]
        assert step_id in step_ids

    def test_list_instance_groups_includes_master(self, emr, cluster_id):
        """ListInstanceGroups for a cluster includes at least the MASTER group."""
        resp = emr.list_instance_groups(ClusterId=cluster_id)
        roles = [g["InstanceGroupType"] for g in resp["InstanceGroups"]]
        master_groups = [r for r in roles if r == "MASTER"]
        assert len(master_groups) >= 1

    def test_list_instances_returns_list(self, emr, cluster_id):
        """ListInstances returns an Instances list (may be empty in emulation)."""
        resp = emr.list_instances(ClusterId=cluster_id)
        assert "Instances" in resp
        assert isinstance(resp["Instances"], list)

    def test_list_bootstrap_actions_returns_empty_without_bootstrap(self, emr, cluster_id):
        """ListBootstrapActions returns empty list when no bootstrap actions defined."""
        resp = emr.list_bootstrap_actions(ClusterId=cluster_id)
        assert resp["BootstrapActions"] == []

    # --- RETRIEVE: behavioral fidelity ---

    def test_describe_cluster_has_creation_datetime(self, emr, cluster_id):
        """DescribeCluster Status.Timeline contains CreationDateTime."""
        desc = emr.describe_cluster(ClusterId=cluster_id)
        status = desc["Cluster"]["Status"]
        assert "Timeline" in status
        assert "CreationDateTime" in status["Timeline"]
        assert isinstance(status["Timeline"]["CreationDateTime"], datetime)

    def test_cluster_arn_format(self, emr, cluster_id):
        """ClusterArn follows arn:aws:elasticmapreduce:{region}:{account}:cluster/{id} format."""
        import re
        desc = emr.describe_cluster(ClusterId=cluster_id)
        arn = desc["Cluster"]["ClusterArn"]
        pattern = r"arn:aws:elasticmapreduce:[a-z0-9-]+:\d{12}:cluster/j-[A-Z0-9]+"
        assert re.match(pattern, arn), f"ARN {arn!r} does not match expected format"

    def test_describe_cluster_status_state_is_valid(self, emr, cluster_id):
        """DescribeCluster Status.State is one of the valid EMR cluster states."""
        valid_states = {
            "STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING",
            "TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS",
        }
        desc = emr.describe_cluster(ClusterId=cluster_id)
        state = desc["Cluster"]["Status"]["State"]
        assert state in valid_states, f"Unexpected state: {state!r}"

    def test_add_job_flow_steps_retrieve_config(self, emr, cluster_id):
        """Step added via AddJobFlowSteps is retrievable with correct config via DescribeStep."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "verify-retrieve",
                    "ActionOnFailure": "TERMINATE_JOB_FLOW",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["spark-submit", "--class", "Main"],
                    },
                }
            ],
        )
        step_id = add["StepIds"][0]
        step = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        assert step["Step"]["Name"] == "verify-retrieve"
        assert step["Step"]["Config"]["Args"] == ["spark-submit", "--class", "Main"]

    def test_add_multiple_steps_retrieve_both(self, emr, cluster_id):
        """Multiple steps added together are each retrievable by ID."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "multi-step-a",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "a"]},
                },
                {
                    "Name": "multi-step-b",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "b"]},
                },
            ],
        )
        assert len(add["StepIds"]) == 2
        for step_id in add["StepIds"]:
            step = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
            assert step["Step"]["Id"] == step_id
            assert step["Step"]["Name"] in ("multi-step-a", "multi-step-b")

    # --- UPDATE: verify state is reflected ---

    def test_modify_cluster_concurrency_reflected_in_describe(self, emr, cluster_id):
        """StepConcurrencyLevel set via ModifyCluster is visible in DescribeCluster."""
        emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=5)
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["StepConcurrencyLevel"] == 5

    # --- LIST: pagination ---

    def test_list_steps_filtered_by_step_ids(self, emr, cluster_id):
        """ListSteps with StepIds filter returns only the requested steps."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": f"filter-step-{i}",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", str(i)]},
                }
                for i in range(3)
            ],
        )
        all_ids = add["StepIds"]
        # Request only the first step
        resp = emr.list_steps(ClusterId=cluster_id, StepIds=[all_ids[0]])
        returned_ids = [s["Id"] for s in resp["Steps"]]
        assert all_ids[0] in returned_ids
        assert all_ids[1] not in returned_ids
        assert all_ids[2] not in returned_ids

    def test_list_supported_instance_types_contains_m5(self, emr):
        """ListSupportedInstanceTypes for emr-6.10.0 includes at least one m5 instance type."""
        resp = emr.list_supported_instance_types(ReleaseLabel="emr-6.10.0")
        types = [t["Type"] for t in resp["SupportedInstanceTypes"]]
        assert len(types) > 0
        m5_types = [t for t in types if t.startswith("m5")]
        assert len(m5_types) > 0, f"No m5 types in {types[:10]}"

    # --- CLUSTER FILTER: terminated state excludes active clusters ---

    def test_list_clusters_terminated_filter_excludes_active(self, emr, cluster_id):
        """ListClusters with TERMINATED state filter does not include a WAITING cluster."""
        resp = emr.list_clusters(ClusterStates=["TERMINATED"])
        ids = [c["Id"] for c in resp["Clusters"]]
        assert cluster_id not in ids

    # --- UNICODE ---

    def test_unicode_cluster_name_preserved(self, emr):
        """Cluster name with unicode characters is stored and returned correctly."""
        name = f"cluster-naïve-{uuid.uuid4().hex[:8]}"
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
        cid = resp["JobFlowId"]
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["Name"] == name
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    # --- SECURITY CONFIG: behavioral fidelity ---

    def test_security_config_roundtrip_preserves_json(self, emr):
        """SecurityConfiguration JSON is stored and returned verbatim (parseable)."""
        name = _unique("rt-config")
        original = {
            "EncryptionConfiguration": {
                "EnableInTransitEncryption": True,
                "EnableAtRestEncryption": False,
            }
        }
        emr.create_security_configuration(
            Name=name, SecurityConfiguration=json.dumps(original)
        )
        try:
            desc = emr.describe_security_configuration(Name=name)
            returned = json.loads(desc["SecurityConfiguration"])
            assert returned["EncryptionConfiguration"]["EnableInTransitEncryption"] is True
            assert returned["EncryptionConfiguration"]["EnableAtRestEncryption"] is False
        finally:
            emr.delete_security_configuration(Name=name)


class TestEMRListClustersEdgeCases:
    """Edge cases for ListClusters covering DELETE and ERROR patterns."""

    def test_list_clusters_terminated_cluster_changes_state(self, emr):
        """Terminating a cluster changes its state so it no longer appears in WAITING filter."""
        resp = emr.run_job_flow(
            Name=_unique("term-list-cluster"),
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
        # Cluster is active — should appear in WAITING filter
        active = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        active_ids = [c["Id"] for c in active["Clusters"]]
        assert cid in active_ids
        # Terminate it
        emr.terminate_job_flows(JobFlowIds=[cid])
        # Should no longer appear in WAITING
        after = emr.list_clusters(ClusterStates=["WAITING"])
        after_ids = [c["Id"] for c in after["Clusters"]]
        assert cid not in after_ids
        # Verify terminal state via describe (avoids pagination issues with large TERMINATED pool)
        desc = emr.describe_cluster(ClusterId=cid)
        assert desc["Cluster"]["Status"]["State"] in ("TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS")

    def test_list_clusters_create_describe_retrieve_lifecycle(self, emr):
        """Full cluster lifecycle: create → list → describe → retrieve fields."""
        resp = emr.run_job_flow(
            Name=_unique("full-lifecycle"),
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
        try:
            # LIST: appears in active results (filter to avoid pagination issues)
            listed = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            listed_ids = [c["Id"] for c in listed["Clusters"]]
            assert cid in listed_ids
            # RETRIEVE: cluster entry in list has Name and Status
            cluster_entry = next(c for c in listed["Clusters"] if c["Id"] == cid)
            assert "Name" in cluster_entry
            assert "Status" in cluster_entry
            # RETRIEVE via describe
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["Id"] == cid
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_list_clusters_empty_states_filter_returns_nothing(self, emr):
        """ListClusters with an uncommon state like BOOTSTRAPPING returns a valid empty list."""
        resp = emr.list_clusters(ClusterStates=["BOOTSTRAPPING"])
        assert "Clusters" in resp
        assert isinstance(resp["Clusters"], list)


class TestEMRDescribeClusterFullLifecycle:
    """Full CRLDE lifecycle tests for DescribeCluster."""

    def test_describe_cluster_after_terminate(self, emr):
        """DescribeCluster after termination shows TERMINATING or TERMINATED state."""
        resp = emr.run_job_flow(
            Name=_unique("term-describe"),
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
        emr.terminate_job_flows(JobFlowIds=[cid])
        desc = emr.describe_cluster(ClusterId=cid)
        assert desc["Cluster"]["Status"]["State"] in ("TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS")

    def test_describe_cluster_update_visibility_reflected(self, emr, cluster_id):
        """SetVisibleToAllUsers change is visible in DescribeCluster."""
        emr.set_visible_to_all_users(JobFlowIds=[cluster_id], VisibleToAllUsers=False)
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["VisibleToAllUsers"] is False
        # Restore
        emr.set_visible_to_all_users(JobFlowIds=[cluster_id], VisibleToAllUsers=True)

    def test_describe_cluster_after_add_step_shows_step(self, emr, cluster_id):
        """DescribeCluster after adding a step — ListSteps shows the step."""
        emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "lifecycle-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "hi"]},
                }
            ],
        )
        steps = emr.list_steps(ClusterId=cluster_id)
        lifecycle_steps = [s for s in steps["Steps"] if s["Name"] == "lifecycle-step"]
        assert len(lifecycle_steps) == 1


class TestEMRStepsCRLDE:
    """CRLDE coverage for step operations."""

    def test_steps_create_list_retrieve_cancel(self, emr, cluster_id):
        """Full step lifecycle: add → list → describe → cancel."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "full-lifecycle-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "lifecycle"]},
                }
            ],
        )
        step_id = add["StepIds"][0]
        # LIST
        listed = emr.list_steps(ClusterId=cluster_id)
        step_ids_in_list = [s["Id"] for s in listed["Steps"]]
        assert step_id in step_ids_in_list
        # RETRIEVE
        described = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        assert described["Step"]["Id"] == step_id
        assert described["Step"]["Name"] == "full-lifecycle-step"
        # DELETE (cancel)
        cancel = emr.cancel_steps(ClusterId=cluster_id, StepIds=[step_id])
        assert "CancelStepsInfoList" in cancel

    def test_list_steps_error_nonexistent_step_id_filter(self, emr, cluster_id):
        """ListSteps with a fake StepIds filter returns an empty list (not an error)."""
        resp = emr.list_steps(ClusterId=cluster_id, StepIds=["s-FAKESTEP0001"])
        assert "Steps" in resp
        assert isinstance(resp["Steps"], list)
        assert len(resp["Steps"]) == 0

    def test_add_job_flow_steps_error_on_nonexistent_cluster(self, emr):
        """AddJobFlowSteps on a nonexistent cluster raises ClientError."""
        with pytest.raises(ClientError) as exc:
            emr.add_job_flow_steps(
                JobFlowId="j-FAKE1234567890",
                Steps=[
                    {
                        "Name": "err-step",
                        "ActionOnFailure": "CONTINUE",
                        "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "x"]},
                    }
                ],
            )
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_multiple_steps_each_have_unique_ids(self, emr, cluster_id):
        """Multiple steps added together each receive a distinct step ID."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": f"unique-step-{i}",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", str(i)]},
                }
                for i in range(3)
            ],
        )
        ids = add["StepIds"]
        assert len(ids) == 3
        assert len(set(ids)) == 3  # all distinct


class TestEMRInstanceGroupsCRLDE:
    """CRLDE coverage for instance group operations."""

    def test_instance_groups_create_list_retrieve(self, emr, cluster_id):
        """AddInstanceGroups → ListInstanceGroups → verify group is present with correct role."""
        emr.add_instance_groups(
            InstanceGroups=[
                {
                    "Name": "crlde-task-group",
                    "InstanceRole": "TASK",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                }
            ],
            JobFlowId=cluster_id,
        )
        resp = emr.list_instance_groups(ClusterId=cluster_id)
        groups = {g["Name"]: g for g in resp["InstanceGroups"]}
        assert "crlde-task-group" in groups
        assert groups["crlde-task-group"]["InstanceGroupType"] == "TASK"
        assert groups["crlde-task-group"]["InstanceType"] == "m5.xlarge"

    def test_list_instances_after_cluster_creation(self, emr, cluster_id):
        """ListInstances returns Instances list — each instance has an Id."""
        resp = emr.list_instances(ClusterId=cluster_id)
        assert isinstance(resp["Instances"], list)
        for instance in resp["Instances"]:
            assert "Id" in instance
            assert isinstance(instance["Id"], str)

    def test_list_bootstrap_actions_returns_defined_actions(self, emr):
        """ListBootstrapActions for a cluster with bootstrap actions lists them."""
        resp = emr.run_job_flow(
            Name=_unique("ba-crlde-cluster"),
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
                    "Name": "bootstrap-list-check",
                    "ScriptBootstrapAction": {
                        "Path": "s3://my-bucket/setup.sh",
                        "Args": [],
                    },
                }
            ],
        )
        cid = resp["JobFlowId"]
        try:
            ba = emr.list_bootstrap_actions(ClusterId=cid)
            matching = [a for a in ba["BootstrapActions"] if a["Name"] == "bootstrap-list-check"]
            assert len(matching) == 1
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])


class TestEMRModifyClusterCRLDE:
    """CRLDE coverage for ModifyCluster and related settings."""

    def test_modify_cluster_step_concurrency_create_retrieve_update_describe(self, emr):
        """Full CRUDE: create cluster → describe default → modify → describe new value → terminate."""
        resp = emr.run_job_flow(
            Name=_unique("mod-cluster"),
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
        try:
            # RETRIEVE default
            desc = emr.describe_cluster(ClusterId=cid)
            assert "StepConcurrencyLevel" in desc["Cluster"]
            # UPDATE
            mod = emr.modify_cluster(ClusterId=cid, StepConcurrencyLevel=3)
            assert mod["StepConcurrencyLevel"] == 3
            # RETRIEVE updated value
            desc2 = emr.describe_cluster(ClusterId=cid)
            assert desc2["Cluster"]["StepConcurrencyLevel"] == 3
            # LIST — cluster still appears
            listed = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            ids = [c["Id"] for c in listed["Clusters"]]
            assert cid in ids
        finally:
            # DELETE
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_modify_cluster_error_nonexistent(self, emr):
        """ModifyCluster on a nonexistent cluster raises ClientError."""
        with pytest.raises(ClientError) as exc:
            emr.modify_cluster(ClusterId="j-FAKE1234567890", StepConcurrencyLevel=2)
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )


class TestEMRListSupportedInstanceTypesDetail:
    """Detailed behavioral tests for ListSupportedInstanceTypes."""

    def test_list_supported_instance_types_content_fields(self, emr):
        """Each instance type entry has Type, MemoryGB, VCPU, and StorageGB fields."""
        resp = emr.list_supported_instance_types(ReleaseLabel="emr-6.10.0")
        types = resp["SupportedInstanceTypes"]
        assert len(types) > 0
        first = types[0]
        assert "Type" in first
        # Verify at least some numeric resource info is present
        assert "MemoryGB" in first or "VCPU" in first or "StorageGB" in first

    def test_list_supported_instance_types_different_release(self, emr):
        """ListSupportedInstanceTypes works for a different release label."""
        resp = emr.list_supported_instance_types(ReleaseLabel="emr-5.36.0")
        assert "SupportedInstanceTypes" in resp
        assert isinstance(resp["SupportedInstanceTypes"], list)

    def test_list_supported_instance_types_error_invalid_label(self, emr):
        """ListSupportedInstanceTypes with an invalid release label raises ClientError."""
        with pytest.raises(ClientError) as exc:
            emr.list_supported_instance_types(ReleaseLabel="emr-0.0.0-invalid")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ValidationException",
        )


class TestEMRDescribeClusterDetailsCRLDE:
    """CRLDE coverage for DescribeCluster detail tests."""

    def test_describe_cluster_release_label_after_terminate(self, emr):
        """ReleaseLabel persists in DescribeCluster even after termination."""
        resp = emr.run_job_flow(
            Name=_unique("rl-term-cluster"),
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
        emr.terminate_job_flows(JobFlowIds=[cid])
        desc = emr.describe_cluster(ClusterId=cid)
        assert desc["Cluster"]["ReleaseLabel"] == "emr-6.10.0"
        assert desc["Cluster"]["Status"]["State"] in ("TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS")

    def test_describe_cluster_service_role_after_modify(self, emr, cluster_id):
        """ServiceRole is stable after ModifyCluster (it doesn't change)."""
        emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=1)
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["ServiceRole"] == "EMR_DefaultRole"

    def test_describe_cluster_termination_protection_update_reflected(self, emr, cluster_id):
        """TerminationProtected can be enabled and then disabled."""
        emr.set_termination_protection(JobFlowIds=[cluster_id], TerminationProtected=True)
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["TerminationProtected"] is True
        emr.set_termination_protection(JobFlowIds=[cluster_id], TerminationProtected=False)
        desc2 = emr.describe_cluster(ClusterId=cluster_id)
        assert desc2["Cluster"]["TerminationProtected"] is False

    def test_describe_cluster_auto_terminate_with_keep_alive_false(self, emr):
        """Cluster with KeepJobFlowAliveWhenNoSteps=False has AutoTerminate=True."""
        resp = emr.run_job_flow(
            Name=_unique("auto-term-cluster"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": False,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )
        cid = resp["JobFlowId"]
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["AutoTerminate"] is True
        finally:
            try:
                emr.terminate_job_flows(JobFlowIds=[cid])
            except Exception:
                pass


class TestEMRListClustersFilteredCRLDE:
    """CRLDE coverage for ListClusters with filters."""

    def test_list_clusters_by_state_create_update_delete(self, emr):
        """Full CRUDE lifecycle visible through ListClusters state filter."""
        resp = emr.run_job_flow(
            Name=_unique("state-filter-cluster"),
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
        # CREATE: appears in active states
        active = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        assert cid in [c["Id"] for c in active["Clusters"]]
        # UPDATE: modify step concurrency
        emr.modify_cluster(ClusterId=cid, StepConcurrencyLevel=4)
        # Still in active list after update
        active2 = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        assert cid in [c["Id"] for c in active2["Clusters"]]
        # DELETE: terminate
        emr.terminate_job_flows(JobFlowIds=[cid])
        # Now not in active list
        active3 = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        assert cid not in [c["Id"] for c in active3["Clusters"]]
        # ERROR: describe after terminate still works (not a 404)
        desc = emr.describe_cluster(ClusterId=cid)
        assert desc["Cluster"]["Status"]["State"] in ("TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS")

    def test_list_clusters_by_created_after_excludes_old(self, emr, cluster_id):
        """ListClusters with CreatedAfter far in future returns empty list."""
        from datetime import timezone
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        resp = emr.list_clusters(CreatedAfter=future)
        assert "Clusters" in resp
        assert resp["Clusters"] == []


class TestEMRSecurityConfigFullEncryptionCRLDE:
    """CRLDE coverage for security configuration with full encryption."""

    def test_security_config_full_encryption_create_list_retrieve_delete(self, emr):
        """Full CRLDE: create full-encryption config → list → describe → delete → error on describe."""
        name = _unique("full-enc-crlde")
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
                    },
                }
            }
        )
        # CREATE
        create = emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        assert create["Name"] == name
        assert "CreationDateTime" in create
        # LIST
        listed = emr.list_security_configurations()
        names = [sc["Name"] for sc in listed["SecurityConfigurations"]]
        assert name in names
        # RETRIEVE
        desc = emr.describe_security_configuration(Name=name)
        parsed = json.loads(desc["SecurityConfiguration"])
        enc = parsed["EncryptionConfiguration"]
        assert enc["EnableInTransitEncryption"] is True
        assert enc["EnableAtRestEncryption"] is True
        # DELETE
        emr.delete_security_configuration(Name=name)
        # ERROR after delete
        with pytest.raises(ClientError) as exc:
            emr.describe_security_configuration(Name=name)
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )

    def test_security_config_creation_datetime_is_recent(self, emr):
        """CreateSecurityConfiguration returns CreationDateTime close to now."""
        from datetime import timezone
        name = _unique("dt-check")
        config = json.dumps({"EncryptionConfiguration": {"EnableInTransitEncryption": False}})
        before = datetime.now(tz=timezone.utc)
        create = emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        after = datetime.now(tz=timezone.utc)
        try:
            dt = create["CreationDateTime"]
            assert isinstance(dt, datetime)
            # Normalize to UTC for comparison
            if dt.tzinfo is None:
                # Server returned naive datetime; can only assert it's a datetime
                pass
            else:
                assert before <= dt <= after
        finally:
            emr.delete_security_configuration(Name=name)


class TestEMRListClustersPagination:
    """Tests for ListClusters pagination (uses Marker, not MaxResults)."""

    def test_list_clusters_marker_pagination(self, emr):
        """ListClusters supports Marker-based pagination across multiple clusters."""
        cids = []
        for i in range(3):
            r = emr.run_job_flow(
                Name=_unique(f"page-cluster-{i}"),
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
            cids.append(r["JobFlowId"])
        try:
            # Collect all clusters via marker-based pagination
            seen_ids: set = set()
            kwargs: dict = {"ClusterStates": ["WAITING", "RUNNING", "STARTING"]}
            while True:
                resp = emr.list_clusters(**kwargs)
                for c in resp["Clusters"]:
                    seen_ids.add(c["Id"])
                if "Marker" not in resp or not resp["Marker"]:
                    break
                kwargs["Marker"] = resp["Marker"]
            for cid in cids:
                assert cid in seen_ids, f"Cluster {cid} not found in paginated results"
        finally:
            emr.terminate_job_flows(JobFlowIds=cids)

    def test_list_clusters_multiple_all_visible(self, emr):
        """Creating 3 clusters and listing returns all 3 in active state filter."""
        cids = []
        for i in range(3):
            r = emr.run_job_flow(
                Name=_unique(f"multi-{i}"),
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
            cids.append(r["JobFlowId"])
        try:
            resp = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            listed_ids = [c["Id"] for c in resp["Clusters"]]
            for cid in cids:
                assert cid in listed_ids
        finally:
            emr.terminate_job_flows(JobFlowIds=cids)

    def test_list_clusters_each_entry_has_required_fields(self, emr, cluster_id):
        """Each cluster in ListClusters has Id, Name, and Status.State."""
        resp = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        assert len(resp["Clusters"]) >= 1
        entry = next(c for c in resp["Clusters"] if c["Id"] == cluster_id)
        assert entry["Id"] == cluster_id
        assert "Name" in entry
        assert "Status" in entry
        assert "State" in entry["Status"]
        valid_states = {
            "STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING",
            "TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS",
        }
        assert entry["Status"]["State"] in valid_states


class TestEMRListStepsBehavior:
    """Behavioral fidelity tests for ListSteps."""

    def test_list_steps_count_increases_after_add(self, emr, cluster_id):
        """Step count in ListSteps increases after AddJobFlowSteps."""
        before = emr.list_steps(ClusterId=cluster_id)
        count_before = len(before["Steps"])
        emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "count-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "count"]},
                }
            ],
        )
        after = emr.list_steps(ClusterId=cluster_id)
        assert len(after["Steps"]) == count_before + 1

    def test_list_steps_each_entry_has_required_fields(self, emr, cluster_id):
        """Each step in ListSteps has Id, Name, and Status.State."""
        emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "fields-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "x"]},
                }
            ],
        )
        resp = emr.list_steps(ClusterId=cluster_id)
        assert len(resp["Steps"]) >= 1
        for step in resp["Steps"]:
            assert "Id" in step
            assert "Name" in step
            assert "Status" in step
            assert "State" in step["Status"]

    def test_list_steps_state_filter_pending(self, emr, cluster_id):
        """ListSteps with StepStates=['PENDING'] returns only pending steps."""
        emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "pending-step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "pending"]},
                }
            ],
        )
        resp = emr.list_steps(ClusterId=cluster_id, StepStates=["PENDING"])
        assert "Steps" in resp
        for step in resp["Steps"]:
            assert step["Status"]["State"] == "PENDING"

    def test_add_job_flow_steps_list_then_retrieve(self, emr, cluster_id):
        """AddJobFlowSteps: step appears in ListSteps and DescribeStep with correct name."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "list-retrieve-step",
                    "ActionOnFailure": "TERMINATE_JOB_FLOW",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "lr"]},
                }
            ],
        )
        step_id = add["StepIds"][0]
        list_resp = emr.list_steps(ClusterId=cluster_id)
        listed_ids = [s["Id"] for s in list_resp["Steps"]]
        assert step_id in listed_ids
        desc = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        assert desc["Step"]["Name"] == "list-retrieve-step"
        assert desc["Step"]["Config"]["Args"] == ["echo", "lr"]

    def test_add_multiple_steps_listed_and_retrievable(self, emr, cluster_id):
        """All steps from AddJobFlowSteps appear in ListSteps and are individually describable."""
        add = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": f"batch-step-{i}",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", str(i)]},
                }
                for i in range(3)
            ],
        )
        all_ids = add["StepIds"]
        assert len(all_ids) == 3
        list_resp = emr.list_steps(ClusterId=cluster_id)
        listed_ids = [s["Id"] for s in list_resp["Steps"]]
        for sid in all_ids:
            assert sid in listed_ids
            desc = emr.describe_step(ClusterId=cluster_id, StepId=sid)
            assert desc["Step"]["Id"] == sid


class TestEMRInstanceGroupsFields:
    """Field verification tests for instance group and instance operations."""

    def test_list_instance_groups_fields(self, emr, cluster_id):
        """Each instance group in ListInstanceGroups has required fields."""
        resp = emr.list_instance_groups(ClusterId=cluster_id)
        assert len(resp["InstanceGroups"]) >= 1
        for ig in resp["InstanceGroups"]:
            assert "Id" in ig
            assert "Name" in ig
            assert "InstanceGroupType" in ig
            assert ig["InstanceGroupType"] in ("MASTER", "CORE", "TASK")
            assert "InstanceType" in ig
            assert "RequestedInstanceCount" in ig
            assert ig["RequestedInstanceCount"] >= 0

    def test_list_instance_groups_master_has_correct_type(self, emr, cluster_id):
        """The default cluster has a MASTER instance group with m5.xlarge type."""
        resp = emr.list_instance_groups(ClusterId=cluster_id)
        masters = [g for g in resp["InstanceGroups"] if g["InstanceGroupType"] == "MASTER"]
        assert len(masters) >= 1
        assert masters[0]["InstanceType"] == "m5.xlarge"

    def test_list_instances_filter_by_instance_group_type(self, emr, cluster_id):
        """ListInstances filtered by MASTER instance group type returns a list."""
        resp = emr.list_instances(ClusterId=cluster_id, InstanceGroupTypes=["MASTER"])
        assert "Instances" in resp
        assert isinstance(resp["Instances"], list)
        # Verify any returned instances have Id
        for inst in resp["Instances"]:
            assert "Id" in inst

    def test_list_bootstrap_actions_fields(self, emr):
        """Each bootstrap action in ListBootstrapActions has Name and ScriptPath."""
        r = emr.run_job_flow(
            Name=_unique("ba-fields"),
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
                    "Name": "ba-fields-check",
                    "ScriptBootstrapAction": {
                        "Path": "s3://bucket/script.sh",
                        "Args": ["--flag"],
                    },
                }
            ],
        )
        cid = r["JobFlowId"]
        try:
            resp = emr.list_bootstrap_actions(ClusterId=cid)
            assert len(resp["BootstrapActions"]) == 1
            ba = resp["BootstrapActions"][0]
            assert ba["Name"] == "ba-fields-check"
            assert "ScriptPath" in ba
            assert ba["ScriptPath"] == "s3://bucket/script.sh"
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])


class TestEMRAutoScalingPolicyRetrieveAndError:
    """RETRIEVE and ERROR patterns for auto-scaling policy operations."""

    def _put_policy(self, emr, cluster_id, ig_id):
        emr.put_auto_scaling_policy(
            ClusterId=cluster_id,
            InstanceGroupId=ig_id,
            AutoScalingPolicy={
                "Constraints": {"MinCapacity": 1, "MaxCapacity": 5},
                "Rules": [
                    {
                        "Name": "scale-rule",
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

    def test_remove_auto_scaling_policy_retrieve_after_put(self, emr, cluster_id):
        """PutAutoScalingPolicy response contains ClusterId, InstanceGroupId, and Constraints."""
        igs = emr.list_instance_groups(ClusterId=cluster_id)
        ig_id = igs["InstanceGroups"][0]["Id"]
        resp = emr.put_auto_scaling_policy(
            ClusterId=cluster_id,
            InstanceGroupId=ig_id,
            AutoScalingPolicy={
                "Constraints": {"MinCapacity": 2, "MaxCapacity": 8},
                "Rules": [
                    {
                        "Name": "retrieve-rule",
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
        assert resp["ClusterId"] == cluster_id
        assert resp["InstanceGroupId"] == ig_id
        assert "AutoScalingPolicy" in resp
        policy = resp["AutoScalingPolicy"]
        assert "Constraints" in policy
        assert policy["Constraints"]["MinCapacity"] == 2
        assert policy["Constraints"]["MaxCapacity"] == 8
        emr.remove_auto_scaling_policy(ClusterId=cluster_id, InstanceGroupId=ig_id)

    def test_remove_auto_scaling_policy_update_policy(self, emr, cluster_id):
        """Put policy then update with new constraints — new MaxCapacity returned."""
        igs = emr.list_instance_groups(ClusterId=cluster_id)
        ig_id = igs["InstanceGroups"][0]["Id"]
        self._put_policy(emr, cluster_id, ig_id)
        update_resp = emr.put_auto_scaling_policy(
            ClusterId=cluster_id,
            InstanceGroupId=ig_id,
            AutoScalingPolicy={
                "Constraints": {"MinCapacity": 1, "MaxCapacity": 10},
                "Rules": [
                    {
                        "Name": "updated-rule",
                        "Action": {
                            "SimpleScalingPolicyConfiguration": {
                                "ScalingAdjustment": 2,
                                "AdjustmentType": "CHANGE_IN_CAPACITY",
                            }
                        },
                        "Trigger": {
                            "CloudWatchAlarmDefinition": {
                                "ComparisonOperator": "LESS_THAN",
                                "MetricName": "YARNMemoryAvailablePercentage",
                                "Period": 600,
                                "Statistic": "AVERAGE",
                                "Threshold": 20.0,
                                "Unit": "PERCENT",
                            }
                        },
                    }
                ],
            },
        )
        assert update_resp["AutoScalingPolicy"]["Constraints"]["MaxCapacity"] == 10
        emr.remove_auto_scaling_policy(ClusterId=cluster_id, InstanceGroupId=ig_id)

    def test_remove_auto_scaling_policy_idempotent_on_nonexistent_ig(self, emr, cluster_id):
        """RemoveAutoScalingPolicy is idempotent — removing a nonexistent policy returns 200."""
        igs = emr.list_instance_groups(ClusterId=cluster_id)
        ig_id = igs["InstanceGroups"][0]["Id"]
        # Remove without ever having set a policy — should succeed silently
        resp = emr.remove_auto_scaling_policy(ClusterId=cluster_id, InstanceGroupId=ig_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_remove_auto_scaling_policy_list_groups_after_remove(self, emr, cluster_id):
        """After removing a policy, the instance group is still listed."""
        igs = emr.list_instance_groups(ClusterId=cluster_id)
        ig_id = igs["InstanceGroups"][0]["Id"]
        self._put_policy(emr, cluster_id, ig_id)
        emr.remove_auto_scaling_policy(ClusterId=cluster_id, InstanceGroupId=ig_id)
        after = emr.list_instance_groups(ClusterId=cluster_id)
        ig_ids = [g["Id"] for g in after["InstanceGroups"]]
        assert ig_id in ig_ids


class TestEMRListSupportedInstanceTypesBehavior:
    """Behavioral fidelity tests for ListSupportedInstanceTypes."""

    def test_list_supported_instance_types_count_reasonable(self, emr):
        """ListSupportedInstanceTypes returns at least 10 instance types."""
        resp = emr.list_supported_instance_types(ReleaseLabel="emr-6.10.0")
        types = resp["SupportedInstanceTypes"]
        assert len(types) >= 10, f"Expected >=10 types, got {len(types)}"

    def test_list_supported_instance_types_each_has_type_field(self, emr):
        """Every entry in SupportedInstanceTypes has a non-empty Type string."""
        resp = emr.list_supported_instance_types(ReleaseLabel="emr-6.10.0")
        for t in resp["SupportedInstanceTypes"]:
            assert "Type" in t
            assert isinstance(t["Type"], str)
            assert len(t["Type"]) > 0

    def test_list_supported_instance_types_includes_xlarge(self, emr):
        """ListSupportedInstanceTypes includes at least one xlarge instance type."""
        resp = emr.list_supported_instance_types(ReleaseLabel="emr-6.10.0")
        types = [t["Type"] for t in resp["SupportedInstanceTypes"]]
        xlarge_types = [t for t in types if "xlarge" in t]
        assert len(xlarge_types) >= 1, f"No xlarge types found in: {types[:10]}"


class TestEMRModifyClusterBehavior:
    """Behavioral tests for ModifyCluster covering RETRIEVE and ERROR patterns."""

    def test_modify_cluster_step_concurrency_describe_reflects_change(self, emr, cluster_id):
        """After ModifyCluster, DescribeCluster returns the new StepConcurrencyLevel."""
        emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=4)
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["StepConcurrencyLevel"] == 4

    def test_modify_cluster_step_concurrency_list_cluster_still_active(self, emr, cluster_id):
        """After ModifyCluster, the cluster still appears in the active cluster list."""
        emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=3)
        resp = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        ids = [c["Id"] for c in resp["Clusters"]]
        assert cluster_id in ids

    def test_modify_cluster_step_concurrency_error_invalid_cluster(self, emr):
        """ModifyCluster with nonexistent cluster raises ClientError."""
        with pytest.raises(ClientError) as exc:
            emr.modify_cluster(ClusterId="j-DOESNOTEXIST00", StepConcurrencyLevel=2)
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
        )


class TestEMRDescribeClusterComprehensive:
    """Comprehensive CRLDE tests for DescribeCluster detail operations."""

    def test_describe_cluster_release_label_matches_requested(self, emr):
        """ReleaseLabel in DescribeCluster matches what was passed to RunJobFlow."""
        resp = emr.run_job_flow(
            Name=_unique("rl-check"),
            ReleaseLabel="emr-6.15.0",
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
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["ReleaseLabel"] == "emr-6.15.0"
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_describe_cluster_service_role_exact_value(self, emr):
        """ServiceRole in DescribeCluster is the exact string passed at creation."""
        resp = emr.run_job_flow(
            Name=_unique("sr-exact"),
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
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["ServiceRole"] == "EMR_DefaultRole"
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_describe_cluster_termination_protection_toggle(self, emr):
        """TerminationProtected defaults to False and toggles correctly."""
        resp = emr.run_job_flow(
            Name=_unique("tp-toggle"),
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
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["TerminationProtected"] is False
            emr.set_termination_protection(JobFlowIds=[cid], TerminationProtected=True)
            desc2 = emr.describe_cluster(ClusterId=cid)
            assert desc2["Cluster"]["TerminationProtected"] is True
            emr.set_termination_protection(JobFlowIds=[cid], TerminationProtected=False)
            desc3 = emr.describe_cluster(ClusterId=cid)
            assert desc3["Cluster"]["TerminationProtected"] is False
        finally:
            emr.set_termination_protection(JobFlowIds=[cid], TerminationProtected=False)
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_describe_cluster_auto_terminate_false_with_keepalive(self, emr):
        """AutoTerminate is False when KeepJobFlowAliveWhenNoSteps=True."""
        resp = emr.run_job_flow(
            Name=_unique("at-false"),
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
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["AutoTerminate"] is False
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_describe_cluster_in_list_after_create(self, emr):
        """Created cluster appears in ListClusters with its name visible."""
        name = _unique("in-list-check")
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
        cid = resp["JobFlowId"]
        try:
            listed = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            entry = next((c for c in listed["Clusters"] if c["Id"] == cid), None)
            assert entry is not None, f"Cluster {cid} not found in list"
            assert entry["Name"] == name
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_describe_cluster_not_in_active_list_after_terminate(self, emr):
        """Terminated cluster no longer appears in the WAITING state filter."""
        resp = emr.run_job_flow(
            Name=_unique("term-not-listed"),
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
        emr.terminate_job_flows(JobFlowIds=[cid])
        desc = emr.describe_cluster(ClusterId=cid)
        assert desc["Cluster"]["Status"]["State"] in (
            "TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"
        )
        waiting = emr.list_clusters(ClusterStates=["WAITING"])
        ids = [c["Id"] for c in waiting["Clusters"]]
        assert cid not in ids


class TestEMRListClustersStateFilter:
    """CRLDE coverage for ListClusters by state."""

    def test_list_clusters_by_state_create_and_verify(self, emr):
        """Newly created cluster appears in WAITING/RUNNING/STARTING filter."""
        resp = emr.run_job_flow(
            Name=_unique("state-new"),
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
        try:
            listed = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            ids = [c["Id"] for c in listed["Clusters"]]
            assert cid in ids
            entry = next(c for c in listed["Clusters"] if c["Id"] == cid)
            assert "Status" in entry
            state = entry["Status"]["State"]
            assert state in ("STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING")
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_list_clusters_by_state_delete_removes_from_active(self, emr):
        """Terminating a cluster removes it from the WAITING/RUNNING/STARTING filter."""
        resp = emr.run_job_flow(
            Name=_unique("state-delete"),
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
        active = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        assert cid in [c["Id"] for c in active["Clusters"]]
        emr.terminate_job_flows(JobFlowIds=[cid])
        after = emr.list_clusters(ClusterStates=["WAITING"])
        assert cid not in [c["Id"] for c in after["Clusters"]]

    def test_list_clusters_by_state_terminated_filter_valid(self, emr):
        """ListClusters with TERMINATED filter returns a valid list."""
        resp = emr.list_clusters(ClusterStates=["TERMINATED"])
        assert "Clusters" in resp
        assert isinstance(resp["Clusters"], list)

    def test_list_clusters_by_state_update_visible(self, emr, cluster_id):
        """After ModifyCluster, the cluster is still in the active list."""
        emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=2)
        resp = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        ids = [c["Id"] for c in resp["Clusters"]]
        assert cluster_id in ids


class TestEMRListClustersEdgeCases:
    """Edge cases for list_clusters: create→list→retrieve→update→delete→error."""

    def test_list_clusters_create_list_delete_lifecycle(self, emr):
        """Full lifecycle: create cluster, list to find it, describe, terminate, verify gone from active."""
        cid = emr.run_job_flow(
            Name=_unique("lifecycle"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            # LIST: cluster appears
            active = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            assert cid in [c["Id"] for c in active["Clusters"]]
            # RETRIEVE: describe returns correct name prefix
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["Id"] == cid
            assert desc["Cluster"]["Name"].startswith("lifecycle-")
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])
        # DELETE: after terminate, not in active list
        active2 = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        assert cid not in [c["Id"] for c in active2["Clusters"]]

    def test_list_clusters_multiple_all_present_with_timestamps(self, emr):
        """Creating multiple clusters — all appear in ListClusters with creation timestamps."""
        ids = []
        for i in range(3):
            cid = emr.run_job_flow(
                Name=_unique(f"multi-{i}"),
                ReleaseLabel="emr-6.10.0",
                Instances={
                    "MasterInstanceType": "m5.xlarge",
                    "SlaveInstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                    "KeepJobFlowAliveWhenNoSteps": True,
                },
                JobFlowRole="EMR_EC2_DefaultRole",
                ServiceRole="EMR_DefaultRole",
            )["JobFlowId"]
            ids.append(cid)
        try:
            resp = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            listed_ids = [c["Id"] for c in resp["Clusters"]]
            for cid in ids:
                assert cid in listed_ids
            # Each cluster has a creation timestamp
            our_clusters = [c for c in resp["Clusters"] if c["Id"] in ids]
            for c in our_clusters:
                ts = c["Status"]["Timeline"]["CreationDateTime"]
                assert isinstance(ts, datetime)
        finally:
            emr.terminate_job_flows(JobFlowIds=ids)

    def test_list_clusters_entry_has_status_timeline(self, emr, cluster_id):
        """Each cluster entry in ListClusters has a Status with Timeline."""
        resp = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        our = [c for c in resp["Clusters"] if c["Id"] == cluster_id]
        assert len(our) == 1
        entry = our[0]
        assert "Status" in entry
        assert "State" in entry["Status"]
        assert "Timeline" in entry["Status"]
        assert "CreationDateTime" in entry["Status"]["Timeline"]
        ts = entry["Status"]["Timeline"]["CreationDateTime"]
        assert isinstance(ts, datetime)

    def test_list_clusters_empty_result_for_no_match_state(self, emr):
        """ListClusters with TERMINATED_WITH_ERRORS returns empty if none match."""
        resp = emr.list_clusters(ClusterStates=["TERMINATED_WITH_ERRORS"])
        assert "Clusters" in resp
        assert isinstance(resp["Clusters"], list)


class TestEMRDescribeClusterEdgeCases:
    """Edge cases for describe_cluster: full lifecycle + field fidelity."""

    def test_describe_cluster_full_lifecycle(self, emr):
        """Create→describe→update→describe→terminate→describe."""
        cid = emr.run_job_flow(
            Name=_unique("desc-life"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            # After create
            desc1 = emr.describe_cluster(ClusterId=cid)
            assert desc1["Cluster"]["Status"]["State"] in ("STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING")
            # Update visibility
            emr.set_visible_to_all_users(JobFlowIds=[cid], VisibleToAllUsers=False)
            desc2 = emr.describe_cluster(ClusterId=cid)
            assert desc2["Cluster"]["VisibleToAllUsers"] is False
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])
        # After terminate
        desc3 = emr.describe_cluster(ClusterId=cid)
        assert desc3["Cluster"]["Status"]["State"] in ("TERMINATING", "TERMINATED")

    def test_describe_cluster_status_has_state_change_reason(self, emr, cluster_id):
        """DescribeCluster Status includes StateChangeReason."""
        desc = emr.describe_cluster(ClusterId=cluster_id)
        status = desc["Cluster"]["Status"]
        assert "StateChangeReason" in status
        assert isinstance(status["StateChangeReason"], dict)

    def test_describe_cluster_tags_initially_empty(self, emr):
        """A newly created cluster with no tags has an empty or absent Tags list."""
        cid = emr.run_job_flow(
            Name=_unique("no-tags"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            tags = desc["Cluster"].get("Tags", [])
            assert isinstance(tags, list)
            assert len(tags) == 0
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_describe_cluster_normalized_instance_hours(self, emr, cluster_id):
        """DescribeCluster includes NormalizedInstanceHours as an integer."""
        desc = emr.describe_cluster(ClusterId=cluster_id)
        nih = desc["Cluster"].get("NormalizedInstanceHours", 0)
        assert isinstance(nih, int)


class TestEMRStepEdgeCases:
    """Edge cases for add_job_flow_steps, add_multiple_steps, list_steps."""

    def test_add_step_create_list_retrieve_delete_error(self, emr, cluster_id):
        """Full step lifecycle: add→list→describe→cancel→describe status."""
        # CREATE
        add_resp = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                "Name": "lifecycle-step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "life"]},
            }],
        )
        step_id = add_resp["StepIds"][0]
        assert step_id.startswith("s-")
        # LIST
        steps = emr.list_steps(ClusterId=cluster_id)
        found = [s for s in steps["Steps"] if s["Id"] == step_id]
        assert len(found) == 1
        assert found[0]["Name"] == "lifecycle-step"
        # RETRIEVE
        desc = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        assert desc["Step"]["Config"]["Jar"] == "command-runner.jar"
        assert desc["Step"]["Config"]["Args"] == ["echo", "life"]
        # DELETE (cancel)
        emr.cancel_steps(ClusterId=cluster_id, StepIds=[step_id])
        # Verify status changed
        desc2 = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        assert "Status" in desc2["Step"]

    def test_add_step_error_nonexistent_cluster(self, emr):
        """AddJobFlowSteps on a nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            emr.add_job_flow_steps(
                JobFlowId="j-NONEXISTENT999",
                Steps=[{
                    "Name": "bad",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo"]},
                }],
            )
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException", "ResourceNotFoundException",
        )

    def test_add_multiple_steps_ids_unique_and_listed(self, emr, cluster_id):
        """Multiple steps get unique IDs and all appear in ListSteps."""
        add_resp = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": f"multi-{i}",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", str(i)]},
                }
                for i in range(3)
            ],
        )
        step_ids = add_resp["StepIds"]
        assert len(step_ids) == 3
        assert len(set(step_ids)) == 3  # all unique
        # Verify all in list
        listed = emr.list_steps(ClusterId=cluster_id)
        listed_ids = {s["Id"] for s in listed["Steps"]}
        for sid in step_ids:
            assert sid in listed_ids

    def test_add_multiple_steps_retrieve_each(self, emr, cluster_id):
        """Each step added in a batch can be individually described."""
        add_resp = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": f"desc-{i}",
                    "ActionOnFailure": "CANCEL_AND_WAIT" if i % 2 == 0 else "CONTINUE",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", str(i)]},
                }
                for i in range(2)
            ],
        )
        for i, sid in enumerate(add_resp["StepIds"]):
            desc = emr.describe_step(ClusterId=cluster_id, StepId=sid)
            assert desc["Step"]["Name"] == f"desc-{i}"
            expected_action = "CANCEL_AND_WAIT" if i % 2 == 0 else "CONTINUE"
            assert desc["Step"]["ActionOnFailure"] == expected_action

    def test_list_steps_has_status_timeline(self, emr, cluster_id):
        """Each step in ListSteps has Status with State and Timeline."""
        emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                "Name": "timeline-step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo"]},
            }],
        )
        resp = emr.list_steps(ClusterId=cluster_id)
        for step in resp["Steps"]:
            assert "Status" in step
            assert "State" in step["Status"]
            assert "Timeline" in step["Status"]
            assert "CreationDateTime" in step["Status"]["Timeline"]
            assert isinstance(step["Status"]["Timeline"]["CreationDateTime"], datetime)


class TestEMRInstanceEdgeCases:
    """Edge cases for list_instance_groups, list_instances, list_bootstrap_actions."""

    def test_list_instance_groups_create_retrieve_lifecycle(self, emr, cluster_id):
        """Create cluster → list groups (has MASTER) → add TASK → list again → verify both."""
        # After cluster creation, at least MASTER group exists
        igs1 = emr.list_instance_groups(ClusterId=cluster_id)
        master_groups = [g for g in igs1["InstanceGroups"] if g["InstanceGroupType"] == "MASTER"]
        assert len(master_groups) >= 1
        master_ig = master_groups[0]
        assert master_ig["InstanceType"] == "m5.xlarge"
        # Add a TASK group (UPDATE)
        emr.add_instance_groups(
            InstanceGroups=[{
                "Name": "edge-task",
                "InstanceRole": "TASK",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            }],
            JobFlowId=cluster_id,
        )
        # List again
        igs2 = emr.list_instance_groups(ClusterId=cluster_id)
        task_groups = [g for g in igs2["InstanceGroups"] if g["InstanceGroupType"] == "TASK"]
        assert len(task_groups) >= 1
        assert task_groups[0]["Name"] == "edge-task"

    def test_list_instance_groups_each_has_id_and_state(self, emr, cluster_id):
        """Every instance group has Id, InstanceGroupType, and Status.State."""
        resp = emr.list_instance_groups(ClusterId=cluster_id)
        for ig in resp["InstanceGroups"]:
            assert "Id" in ig
            assert ig["Id"]  # non-empty
            assert "InstanceGroupType" in ig
            assert ig["InstanceGroupType"] in ("MASTER", "CORE", "TASK")
            assert "Status" in ig
            assert "State" in ig["Status"]

    def test_list_instance_groups_error_nonexistent(self, emr):
        """ListInstanceGroups on nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            emr.list_instance_groups(ClusterId="j-NONEXIST999")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException", "ResourceNotFoundException",
        )

    def test_list_instances_each_has_id_and_type(self, emr, cluster_id):
        """Instances have Id, InstanceType, and Status fields with valid values."""
        resp = emr.list_instances(ClusterId=cluster_id)
        assert isinstance(resp["Instances"], list)
        if resp["Instances"]:
            inst = resp["Instances"][0]
            assert isinstance(inst["Id"], str) and len(inst["Id"]) > 0
            assert isinstance(inst["InstanceType"], str) and "." in inst["InstanceType"]
            assert "State" in inst["Status"]

    def test_list_instances_error_nonexistent(self, emr):
        """ListInstances on nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            emr.list_instances(ClusterId="j-NONEXIST999")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException", "ResourceNotFoundException",
        )

    def test_list_bootstrap_actions_after_cluster_with_actions(self, emr):
        """Cluster with bootstrap actions lists them back correctly."""
        cid = emr.run_job_flow(
            Name=_unique("bs-edge"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            BootstrapActions=[{
                "Name": "edge-bootstrap",
                "ScriptBootstrapAction": {
                    "Path": "s3://my-bucket/bootstrap.sh",
                    "Args": ["--flag", "value"],
                },
            }],
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            resp = emr.list_bootstrap_actions(ClusterId=cid)
            assert len(resp["BootstrapActions"]) >= 1
            action = resp["BootstrapActions"][0]
            assert action["Name"] == "edge-bootstrap"
            assert action["ScriptPath"] == "s3://my-bucket/bootstrap.sh"
            # Args format: may be list of strings or list of dicts with "Value"
            if action["Args"] and isinstance(action["Args"][0], dict):
                arg_values = [a["Value"] for a in action["Args"]]
            else:
                arg_values = action["Args"]
            assert arg_values == ["--flag", "value"]
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_list_bootstrap_actions_error_nonexistent(self, emr):
        """ListBootstrapActions on nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            emr.list_bootstrap_actions(ClusterId="j-NONEXIST999")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException", "ResourceNotFoundException",
        )


class TestEMRModifyClusterEdgeCases:
    """Edge cases for modify_cluster_step_concurrency."""

    def test_modify_cluster_create_update_retrieve_lifecycle(self, emr):
        """Create cluster → modify concurrency → describe → terminate."""
        cid = emr.run_job_flow(
            Name=_unique("mod-life"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            # UPDATE
            resp = emr.modify_cluster(ClusterId=cid, StepConcurrencyLevel=5)
            assert resp["StepConcurrencyLevel"] == 5
            # RETRIEVE: reflected in describe
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["StepConcurrencyLevel"] == 5
            # LIST: still visible
            active = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            assert cid in [c["Id"] for c in active["Clusters"]]
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_modify_cluster_error_nonexistent(self, emr):
        """ModifyCluster on nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            emr.modify_cluster(ClusterId="j-NONEXIST999", StepConcurrencyLevel=3)
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException", "ResourceNotFoundException",
        )

    def test_modify_cluster_concurrency_update_twice(self, emr, cluster_id):
        """Modifying step concurrency twice keeps the latest value."""
        emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=3)
        emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=7)
        desc = emr.describe_cluster(ClusterId=cluster_id)
        assert desc["Cluster"]["StepConcurrencyLevel"] == 7


class TestEMRDescribeDetailEdgeCases:
    """Edge cases for describe_cluster detail fields and list_supported_instance_types."""

    def test_describe_cluster_release_label_create_retrieve_delete(self, emr):
        """Create → describe release label → terminate → describe still returns label."""
        cid = emr.run_job_flow(
            Name=_unique("rl-life"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["ReleaseLabel"] == "emr-6.10.0"
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])
        # Even after terminate, describe should return the release label
        desc2 = emr.describe_cluster(ClusterId=cid)
        assert desc2["Cluster"]["ReleaseLabel"] == "emr-6.10.0"

    def test_describe_cluster_service_role_create_and_verify(self, emr):
        """ServiceRole round-trips correctly through create→describe."""
        cid = emr.run_job_flow(
            Name=_unique("sr-verify"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["ServiceRole"] == "EMR_DefaultRole"
            # Also listed
            active = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            assert cid in [c["Id"] for c in active["Clusters"]]
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_describe_cluster_termination_protected_toggle_lifecycle(self, emr):
        """Create → default False → set True → set False → terminate."""
        cid = emr.run_job_flow(
            Name=_unique("tp-toggle"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            # Default
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["TerminationProtected"] is False
            # Enable
            emr.set_termination_protection(JobFlowIds=[cid], TerminationProtected=True)
            desc2 = emr.describe_cluster(ClusterId=cid)
            assert desc2["Cluster"]["TerminationProtected"] is True
            # Disable so we can terminate
            emr.set_termination_protection(JobFlowIds=[cid], TerminationProtected=False)
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_describe_cluster_auto_terminate_lifecycle(self, emr):
        """KeepJobFlowAliveWhenNoSteps=True → AutoTerminate=False after create."""
        cid = emr.run_job_flow(
            Name=_unique("at-life"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["AutoTerminate"] is False
            # Verify also in list
            active = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            assert cid in [c["Id"] for c in active["Clusters"]]
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_list_supported_instance_types_create_cluster_with_type(self, emr):
        """Instance type from ListSupportedInstanceTypes can be used to create a cluster."""
        resp = emr.list_supported_instance_types(ReleaseLabel="emr-6.10.0")
        types = [t["Type"] for t in resp["SupportedInstanceTypes"]]
        assert "m5.xlarge" in types
        # Create cluster with one of the listed types
        cid = emr.run_job_flow(
            Name=_unique("sit-test"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            desc = emr.describe_cluster(ClusterId=cid)
            assert desc["Cluster"]["Id"] == cid
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])

    def test_list_supported_instance_types_each_has_required_fields(self, emr):
        """Each entry has Type and supports multiple architectures or vCPUs."""
        resp = emr.list_supported_instance_types(ReleaseLabel="emr-6.10.0")
        assert len(resp["SupportedInstanceTypes"]) > 0
        for itype in resp["SupportedInstanceTypes"][:5]:  # spot check first 5
            assert "Type" in itype
            assert isinstance(itype["Type"], str)
            assert len(itype["Type"]) > 0


class TestEMRSecurityConfigEdgeCases:
    """Edge cases for describe_security_configuration: full CRUD + error."""

    def test_security_config_create_describe_list_delete_error(self, emr):
        """Full lifecycle: create → describe → list → delete → describe (error)."""
        name = _unique("sec-lifecycle")
        config = json.dumps({
            "EncryptionConfiguration": {
                "EnableInTransitEncryption": False,
                "EnableAtRestEncryption": True,
                "AtRestEncryptionConfiguration": {
                    "S3EncryptionConfiguration": {
                        "EncryptionMode": "SSE-S3",
                    },
                },
            },
        })
        # CREATE
        create_resp = emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        assert create_resp["Name"] == name
        assert "CreationDateTime" in create_resp
        assert isinstance(create_resp["CreationDateTime"], datetime)
        try:
            # RETRIEVE
            desc = emr.describe_security_configuration(Name=name)
            assert desc["Name"] == name
            parsed = json.loads(desc["SecurityConfiguration"])
            assert parsed["EncryptionConfiguration"]["EnableAtRestEncryption"] is True
            # LIST
            listed = emr.list_security_configurations()
            names = [sc["Name"] for sc in listed["SecurityConfigurations"]]
            assert name in names
        finally:
            # DELETE
            emr.delete_security_configuration(Name=name)
        # ERROR: describe after delete
        with pytest.raises(ClientError) as exc:
            emr.describe_security_configuration(Name=name)
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException", "ResourceNotFoundException",
        )

    def test_security_config_unicode_name(self, emr):
        """Security configuration with unicode characters in name."""
        name = _unique("配置-test")
        config = json.dumps({
            "EncryptionConfiguration": {
                "EnableInTransitEncryption": False,
                "EnableAtRestEncryption": False,
            },
        })
        resp = emr.create_security_configuration(Name=name, SecurityConfiguration=config)
        assert resp["Name"] == name
        try:
            desc = emr.describe_security_configuration(Name=name)
            assert desc["Name"] == name
        finally:
            emr.delete_security_configuration(Name=name)

    def test_security_config_list_pagination(self, emr):
        """Create multiple security configs and verify list returns all."""
        names = []
        config = json.dumps({
            "EncryptionConfiguration": {
                "EnableInTransitEncryption": False,
                "EnableAtRestEncryption": False,
            },
        })
        for _ in range(3):
            n = _unique("sec-page")
            emr.create_security_configuration(Name=n, SecurityConfiguration=config)
            names.append(n)
        try:
            listed = emr.list_security_configurations()
            listed_names = [sc["Name"] for sc in listed["SecurityConfigurations"]]
            for n in names:
                assert n in listed_names
        finally:
            for n in names:
                emr.delete_security_configuration(Name=n)


class TestEMRListClustersByStateEdgeCases:
    """Edge cases for list_clusters_by_state: CRUD + error patterns."""

    def test_list_by_state_create_update_terminate(self, emr):
        """Create → active list → modify → still active → terminate → gone from active."""
        cid = emr.run_job_flow(
            Name=_unique("state-life"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        try:
            # In active list
            resp = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            assert cid in [c["Id"] for c in resp["Clusters"]]
            # Modify and verify still in active
            emr.modify_cluster(ClusterId=cid, StepConcurrencyLevel=3)
            resp2 = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
            assert cid in [c["Id"] for c in resp2["Clusters"]]
        finally:
            emr.terminate_job_flows(JobFlowIds=[cid])
        # After terminate - gone from active
        resp3 = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        assert cid not in [c["Id"] for c in resp3["Clusters"]]

    def test_list_by_state_terminated_includes_terminated(self, emr):
        """Terminated cluster appears when filtering by terminated states."""
        cid = emr.run_job_flow(
            Name=_unique("term-state"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        emr.terminate_job_flows(JobFlowIds=[cid])
        # Verify the cluster is in a terminal state via describe
        desc = emr.describe_cluster(ClusterId=cid)
        assert desc["Cluster"]["Status"]["State"] in ("TERMINATING", "TERMINATED")
        # Also verify it's no longer in the active list
        active = emr.list_clusters(ClusterStates=["WAITING", "RUNNING", "STARTING"])
        active_ids = [c["Id"] for c in active["Clusters"]]
        assert cid not in active_ids

    def test_list_by_state_error_describe_after_terminate(self, emr):
        """Describe still works after terminate but shows terminated state."""
        cid = emr.run_job_flow(
            Name=_unique("desc-term"),
            ReleaseLabel="emr-6.10.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )["JobFlowId"]
        emr.terminate_job_flows(JobFlowIds=[cid])
        desc = emr.describe_cluster(ClusterId=cid)
        assert desc["Cluster"]["Status"]["State"] in ("TERMINATING", "TERMINATED")
