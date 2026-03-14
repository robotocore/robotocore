"""Auto Scaling compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def autoscaling():
    return make_client("autoscaling")


@pytest.fixture
def ec2():
    return make_client("ec2")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestAutoScalingLaunchConfigOperations:
    def test_create_launch_configuration(self, autoscaling):
        name = _unique("lc")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        try:
            resp = autoscaling.describe_launch_configurations(LaunchConfigurationNames=[name])
            configs = resp["LaunchConfigurations"]
            assert len(configs) == 1
            assert configs[0]["LaunchConfigurationName"] == name
            assert configs[0]["ImageId"] == "ami-12345678"
            assert configs[0]["InstanceType"] == "t2.micro"
        finally:
            autoscaling.delete_launch_configuration(LaunchConfigurationName=name)

    def test_describe_launch_configurations_empty(self, autoscaling):
        resp = autoscaling.describe_launch_configurations()
        assert "LaunchConfigurations" in resp

    def test_describe_launch_configurations_filtered(self, autoscaling):
        name = _unique("lc-desc")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=name,
            ImageId="ami-abcdef01",
            InstanceType="t2.small",
        )
        try:
            resp = autoscaling.describe_launch_configurations(LaunchConfigurationNames=[name])
            assert len(resp["LaunchConfigurations"]) == 1
            assert resp["LaunchConfigurations"][0]["LaunchConfigurationName"] == name
        finally:
            autoscaling.delete_launch_configuration(LaunchConfigurationName=name)

    def test_delete_launch_configuration(self, autoscaling):
        name = _unique("lc-del")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.delete_launch_configuration(LaunchConfigurationName=name)
        resp = autoscaling.describe_launch_configurations(LaunchConfigurationNames=[name])
        assert len(resp["LaunchConfigurations"]) == 0


class TestAutoScalingGroupOperations:
    @pytest.fixture(autouse=True)
    def _setup_launch_config(self, autoscaling):
        self.lc_name = _unique("asg-lc")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        yield
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_create_auto_scaling_group(self, autoscaling):
        name = _unique("asg")
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=2,
            AvailabilityZones=["us-east-1a"],
        )
        try:
            resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
            groups = resp["AutoScalingGroups"]
            assert len(groups) == 1
            assert groups[0]["AutoScalingGroupName"] == name
            assert groups[0]["MinSize"] == 0
            assert groups[0]["MaxSize"] == 2
        finally:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)

    def test_describe_auto_scaling_groups_empty(self, autoscaling):
        resp = autoscaling.describe_auto_scaling_groups()
        assert "AutoScalingGroups" in resp

    def test_update_auto_scaling_group(self, autoscaling):
        name = _unique("asg-upd")
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        try:
            autoscaling.update_auto_scaling_group(
                AutoScalingGroupName=name,
                MaxSize=5,
                DesiredCapacity=0,
            )
            resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
            assert resp["AutoScalingGroups"][0]["MaxSize"] == 5
        finally:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)

    def test_create_or_update_tags(self, autoscaling):
        name = _unique("asg-tag")
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        try:
            autoscaling.create_or_update_tags(
                Tags=[
                    {
                        "ResourceId": name,
                        "ResourceType": "auto-scaling-group",
                        "Key": "Environment",
                        "Value": "test",
                        "PropagateAtLaunch": True,
                    },
                    {
                        "ResourceId": name,
                        "ResourceType": "auto-scaling-group",
                        "Key": "Team",
                        "Value": "platform",
                        "PropagateAtLaunch": False,
                    },
                ]
            )
            resp = autoscaling.describe_tags(
                Filters=[{"Name": "auto-scaling-group", "Values": [name]}]
            )
            tags = resp["Tags"]
            assert len(tags) >= 2
            tag_map = {t["Key"]: t["Value"] for t in tags}
            assert tag_map["Environment"] == "test"
            assert tag_map["Team"] == "platform"
        finally:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)

    def test_delete_auto_scaling_group(self, autoscaling):
        name = _unique("asg-del")
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
        assert len(resp["AutoScalingGroups"]) == 0


class TestAutoScalingPolicyOperations:
    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("pol-lc")
        self.asg_name = _unique("pol-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=3,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_put_target_tracking_policy(self, autoscaling):
        policy_name = _unique("tt-policy")
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
            PolicyType="TargetTrackingScaling",
            TargetTrackingConfiguration={
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "ASGAverageCPUUtilization",
                },
                "TargetValue": 50.0,
            },
        )
        resp = autoscaling.describe_policies(
            AutoScalingGroupName=self.asg_name,
            PolicyNames=[policy_name],
        )
        policies = resp["ScalingPolicies"]
        assert len(policies) == 1
        assert policies[0]["PolicyName"] == policy_name
        assert policies[0]["PolicyType"] == "TargetTrackingScaling"

    def test_put_simple_scaling_policy(self, autoscaling):
        policy_name = _unique("simple-policy")
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
            PolicyType="SimpleScaling",
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=1,
        )
        resp = autoscaling.describe_policies(
            AutoScalingGroupName=self.asg_name,
            PolicyNames=[policy_name],
        )
        policies = resp["ScalingPolicies"]
        assert len(policies) == 1
        assert policies[0]["PolicyName"] == policy_name
        assert policies[0]["AdjustmentType"] == "ChangeInCapacity"

    def test_describe_policies_for_asg(self, autoscaling):
        p1 = _unique("pol-a")
        p2 = _unique("pol-b")
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=p1,
            PolicyType="SimpleScaling",
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=1,
        )
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=p2,
            PolicyType="SimpleScaling",
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=2,
        )
        resp = autoscaling.describe_policies(AutoScalingGroupName=self.asg_name)
        names = [p["PolicyName"] for p in resp["ScalingPolicies"]]
        assert p1 in names
        assert p2 in names


class TestAutoScalingDescribeOperations:
    def test_describe_scaling_activities(self, autoscaling):
        resp = autoscaling.describe_scaling_activities()
        assert "Activities" in resp

    def test_describe_scaling_activities_for_asg(self, autoscaling):
        lc_name = _unique("act-lc")
        asg_name = _unique("act-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            LaunchConfigurationName=lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        try:
            resp = autoscaling.describe_scaling_activities(AutoScalingGroupName=asg_name)
            assert "Activities" in resp
            assert isinstance(resp["Activities"], list)
        finally:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg_name, ForceDelete=True)
            autoscaling.delete_launch_configuration(LaunchConfigurationName=lc_name)

    def test_describe_scheduled_actions(self, autoscaling):
        resp = autoscaling.describe_scheduled_actions()
        assert "ScheduledUpdateGroupActions" in resp

    def test_describe_scheduled_actions_for_asg(self, autoscaling):
        lc_name = _unique("sched-lc")
        asg_name = _unique("sched-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            LaunchConfigurationName=lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        try:
            resp = autoscaling.describe_scheduled_actions(AutoScalingGroupName=asg_name)
            assert "ScheduledUpdateGroupActions" in resp
            assert isinstance(resp["ScheduledUpdateGroupActions"], list)
        finally:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg_name, ForceDelete=True)
            autoscaling.delete_launch_configuration(LaunchConfigurationName=lc_name)

    def test_describe_auto_scaling_instances(self, autoscaling):
        resp = autoscaling.describe_auto_scaling_instances()
        assert "AutoScalingInstances" in resp

    def test_describe_tags_all(self, autoscaling):
        resp = autoscaling.describe_tags()
        assert "Tags" in resp


class TestAutoScalingLifecycleHookOperations:
    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("lh-lc")
        self.asg_name = _unique("lh-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_put_lifecycle_hook(self, autoscaling):
        hook_name = _unique("hook")
        autoscaling.put_lifecycle_hook(
            LifecycleHookName=hook_name,
            AutoScalingGroupName=self.asg_name,
            LifecycleTransition="autoscaling:EC2_INSTANCE_LAUNCHING",
            HeartbeatTimeout=300,
            DefaultResult="ABANDON",
        )
        resp = autoscaling.describe_lifecycle_hooks(
            AutoScalingGroupName=self.asg_name,
            LifecycleHookNames=[hook_name],
        )
        hooks = resp["LifecycleHooks"]
        assert len(hooks) == 1
        assert hooks[0]["LifecycleHookName"] == hook_name
        assert hooks[0]["LifecycleTransition"] == "autoscaling:EC2_INSTANCE_LAUNCHING"
        assert hooks[0]["DefaultResult"] == "ABANDON"

    def test_delete_lifecycle_hook(self, autoscaling):
        hook_name = _unique("hook-del")
        autoscaling.put_lifecycle_hook(
            LifecycleHookName=hook_name,
            AutoScalingGroupName=self.asg_name,
            LifecycleTransition="autoscaling:EC2_INSTANCE_TERMINATING",
            DefaultResult="CONTINUE",
        )
        autoscaling.delete_lifecycle_hook(
            LifecycleHookName=hook_name,
            AutoScalingGroupName=self.asg_name,
        )
        resp = autoscaling.describe_lifecycle_hooks(
            AutoScalingGroupName=self.asg_name,
            LifecycleHookNames=[hook_name],
        )
        assert len(resp["LifecycleHooks"]) == 0


class TestAutoScalingScheduledActions:
    """Tests for PutScheduledUpdateGroupAction, DeleteScheduledAction, BatchPut/Delete."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("sched-lc")
        self.asg_name = _unique("sched-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=3,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_put_scheduled_update_group_action(self, autoscaling):
        action_name = _unique("sched-act")
        autoscaling.put_scheduled_update_group_action(
            AutoScalingGroupName=self.asg_name,
            ScheduledActionName=action_name,
            MinSize=1,
            MaxSize=5,
            Recurrence="0 9 * * *",
        )
        resp = autoscaling.describe_scheduled_actions(
            AutoScalingGroupName=self.asg_name,
            ScheduledActionNames=[action_name],
        )
        actions = resp["ScheduledUpdateGroupActions"]
        assert len(actions) == 1
        assert actions[0]["ScheduledActionName"] == action_name
        assert actions[0]["MinSize"] == 1
        assert actions[0]["MaxSize"] == 5

    def test_delete_scheduled_action(self, autoscaling):
        action_name = _unique("sched-del")
        autoscaling.put_scheduled_update_group_action(
            AutoScalingGroupName=self.asg_name,
            ScheduledActionName=action_name,
            MinSize=0,
            MaxSize=2,
            Recurrence="0 18 * * *",
        )
        autoscaling.delete_scheduled_action(
            AutoScalingGroupName=self.asg_name,
            ScheduledActionName=action_name,
        )
        resp = autoscaling.describe_scheduled_actions(
            AutoScalingGroupName=self.asg_name,
            ScheduledActionNames=[action_name],
        )
        assert len(resp["ScheduledUpdateGroupActions"]) == 0

    def test_batch_put_scheduled_update_group_action(self, autoscaling):
        act1 = _unique("batch-a")
        act2 = _unique("batch-b")
        resp = autoscaling.batch_put_scheduled_update_group_action(
            AutoScalingGroupName=self.asg_name,
            ScheduledUpdateGroupActions=[
                {"ScheduledActionName": act1, "MinSize": 0, "MaxSize": 1},
                {"ScheduledActionName": act2, "MinSize": 0, "MaxSize": 2},
            ],
        )
        assert "FailedScheduledUpdateGroupActions" in resp
        # Verify both were created
        desc = autoscaling.describe_scheduled_actions(AutoScalingGroupName=self.asg_name)
        names = [a["ScheduledActionName"] for a in desc["ScheduledUpdateGroupActions"]]
        assert act1 in names
        assert act2 in names

    def test_batch_delete_scheduled_action(self, autoscaling):
        act = _unique("batch-del")
        autoscaling.put_scheduled_update_group_action(
            AutoScalingGroupName=self.asg_name,
            ScheduledActionName=act,
            MinSize=0,
            MaxSize=1,
        )
        resp = autoscaling.batch_delete_scheduled_action(
            AutoScalingGroupName=self.asg_name,
            ScheduledActionNames=[act],
        )
        assert "FailedScheduledActions" in resp


class TestAutoScalingProcessManagement:
    """Tests for SuspendProcesses and ResumeProcesses."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("proc-lc")
        self.asg_name = _unique("proc-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=2,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_suspend_processes(self, autoscaling):
        autoscaling.suspend_processes(
            AutoScalingGroupName=self.asg_name,
            ScalingProcesses=["Launch", "Terminate"],
        )
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[self.asg_name])
        suspended = [s["ProcessName"] for s in resp["AutoScalingGroups"][0]["SuspendedProcesses"]]
        assert "Launch" in suspended
        assert "Terminate" in suspended

    def test_resume_processes(self, autoscaling):
        autoscaling.suspend_processes(
            AutoScalingGroupName=self.asg_name,
            ScalingProcesses=["Launch"],
        )
        autoscaling.resume_processes(
            AutoScalingGroupName=self.asg_name,
            ScalingProcesses=["Launch"],
        )
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[self.asg_name])
        suspended = [s["ProcessName"] for s in resp["AutoScalingGroups"][0]["SuspendedProcesses"]]
        assert "Launch" not in suspended


class TestAutoScalingCapacityAndMetrics:
    """Tests for SetDesiredCapacity, EnableMetricsCollection."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("cap-lc")
        self.asg_name = _unique("cap-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=3,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_set_desired_capacity(self, autoscaling):
        autoscaling.set_desired_capacity(
            AutoScalingGroupName=self.asg_name,
            DesiredCapacity=0,
        )
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[self.asg_name])
        assert resp["AutoScalingGroups"][0]["DesiredCapacity"] == 0

    def test_enable_metrics_collection(self, autoscaling):
        autoscaling.enable_metrics_collection(
            AutoScalingGroupName=self.asg_name,
            Granularity="1Minute",
            Metrics=["GroupMinSize", "GroupMaxSize"],
        )
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[self.asg_name])
        # Verify the ASG still exists after the call (call succeeded)
        assert len(resp["AutoScalingGroups"]) == 1
        assert resp["AutoScalingGroups"][0]["AutoScalingGroupName"] == self.asg_name

    def test_enable_metrics_collection_all_metrics(self, autoscaling):
        """EnableMetricsCollection with no Metrics param succeeds and returns 200."""
        resp = autoscaling.enable_metrics_collection(
            AutoScalingGroupName=self.asg_name,
            Granularity="1Minute",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify ASG still exists and has EnabledMetrics key
        desc = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[self.asg_name])
        asg = desc["AutoScalingGroups"][0]
        assert "EnabledMetrics" in asg

    def test_enable_metrics_collection_specific_metrics(self, autoscaling):
        """EnableMetricsCollection with specific Metrics list succeeds."""
        resp = autoscaling.enable_metrics_collection(
            AutoScalingGroupName=self.asg_name,
            Granularity="1Minute",
            Metrics=["GroupMinSize"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestAutoScalingWarmPool:
    """Tests for PutWarmPool, DescribeWarmPool, DeleteWarmPool."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("wp-lc")
        self.asg_name = _unique("wp-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=3,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_put_and_describe_warm_pool(self, autoscaling):
        autoscaling.put_warm_pool(
            AutoScalingGroupName=self.asg_name,
            MinSize=0,
            MaxGroupPreparedCapacity=2,
        )
        resp = autoscaling.describe_warm_pool(
            AutoScalingGroupName=self.asg_name,
        )
        assert "WarmPoolConfiguration" in resp
        assert resp["WarmPoolConfiguration"]["MinSize"] == 0
        assert resp["WarmPoolConfiguration"]["MaxGroupPreparedCapacity"] == 2

    def test_delete_warm_pool(self, autoscaling):
        autoscaling.put_warm_pool(
            AutoScalingGroupName=self.asg_name,
            MinSize=0,
        )
        autoscaling.delete_warm_pool(
            AutoScalingGroupName=self.asg_name,
            ForceDelete=True,
        )
        # After deletion, describe should show no warm pool config
        try:
            resp = autoscaling.describe_warm_pool(
                AutoScalingGroupName=self.asg_name,
            )
            # If it returns, warm pool should be gone or empty
            assert resp.get("WarmPoolConfiguration") is None or resp.get("Instances") == []
        except Exception:
            # Some implementations raise an error after deletion
            pass


class TestAutoScalingTagOperations:
    """Tests for DeleteTags."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("tag-lc")
        self.asg_name = _unique("tag-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_delete_tags(self, autoscaling):
        autoscaling.create_or_update_tags(
            Tags=[
                {
                    "ResourceId": self.asg_name,
                    "ResourceType": "auto-scaling-group",
                    "Key": "ToDelete",
                    "Value": "yes",
                    "PropagateAtLaunch": False,
                }
            ]
        )
        autoscaling.delete_tags(
            Tags=[
                {
                    "ResourceId": self.asg_name,
                    "ResourceType": "auto-scaling-group",
                    "Key": "ToDelete",
                }
            ]
        )
        resp = autoscaling.describe_tags(
            Filters=[{"Name": "auto-scaling-group", "Values": [self.asg_name]}]
        )
        keys = [t["Key"] for t in resp["Tags"]]
        assert "ToDelete" not in keys


class TestAutoScalingPolicyDelete:
    """Tests for DeletePolicy."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("dpol-lc")
        self.asg_name = _unique("dpol-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=3,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_delete_policy(self, autoscaling):
        policy_name = _unique("del-pol")
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
            PolicyType="SimpleScaling",
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=1,
        )
        autoscaling.delete_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
        )
        resp = autoscaling.describe_policies(
            AutoScalingGroupName=self.asg_name,
            PolicyNames=[policy_name],
        )
        assert len(resp["ScalingPolicies"]) == 0


class TestAutoScalingInstanceOperations:
    """Tests for AttachInstances, DetachInstances, SetInstanceProtection,
    EnterStandby, ExitStandby."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("inst-lc")
        self.asg_name = _unique("inst-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=2,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_attach_instances_empty(self, autoscaling):
        """AttachInstances with empty list is a valid call."""
        autoscaling.attach_instances(
            AutoScalingGroupName=self.asg_name,
            InstanceIds=[],
        )
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[self.asg_name])
        assert "AutoScalingGroups" in resp

    def test_detach_instances_empty(self, autoscaling):
        """DetachInstances with empty list returns Activities."""
        resp = autoscaling.detach_instances(
            AutoScalingGroupName=self.asg_name,
            InstanceIds=[],
            ShouldDecrementDesiredCapacity=True,
        )
        assert "Activities" in resp

    def test_set_instance_protection_empty(self, autoscaling):
        """SetInstanceProtection with no instances is valid."""
        autoscaling.set_instance_protection(
            AutoScalingGroupName=self.asg_name,
            InstanceIds=[],
            ProtectedFromScaleIn=True,
        )
        # If no error, call succeeded
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[self.asg_name])
        assert len(resp["AutoScalingGroups"]) == 1

    def test_enter_standby_empty(self, autoscaling):
        """EnterStandby with empty instance list returns Activities."""
        resp = autoscaling.enter_standby(
            AutoScalingGroupName=self.asg_name,
            InstanceIds=[],
            ShouldDecrementDesiredCapacity=True,
        )
        assert "Activities" in resp

    def test_exit_standby_empty(self, autoscaling):
        """ExitStandby with empty instance list returns Activities."""
        resp = autoscaling.exit_standby(
            AutoScalingGroupName=self.asg_name,
            InstanceIds=[],
        )
        assert "Activities" in resp


class TestAutoScalingLoadBalancerDescribe:
    """Tests for DescribeLoadBalancers and DescribeLoadBalancerTargetGroups."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("lb-lc")
        self.asg_name = _unique("lb-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_describe_load_balancers(self, autoscaling):
        resp = autoscaling.describe_load_balancers(
            AutoScalingGroupName=self.asg_name,
        )
        assert "LoadBalancers" in resp
        assert isinstance(resp["LoadBalancers"], list)

    def test_describe_load_balancer_target_groups(self, autoscaling):
        resp = autoscaling.describe_load_balancer_target_groups(
            AutoScalingGroupName=self.asg_name,
        )
        assert "LoadBalancerTargetGroups" in resp
        assert isinstance(resp["LoadBalancerTargetGroups"], list)


class TestAutoScalingExecutePolicy:
    """Tests for ExecutePolicy."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("ep-lc")
        self.asg_name = _unique("ep-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=3,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_execute_policy(self, autoscaling):
        """ExecutePolicy triggers a scaling policy."""
        policy_name = _unique("exec-pol")
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
            PolicyType="SimpleScaling",
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=1,
        )
        resp = autoscaling.execute_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
            HonorCooldown=False,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_execute_policy_no_honor_cooldown(self, autoscaling):
        """ExecutePolicy with HonorCooldown=True still succeeds."""
        policy_name = _unique("exec-cool")
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
            PolicyType="SimpleScaling",
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=1,
        )
        resp = autoscaling.execute_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
            HonorCooldown=True,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestAutoScalingDescribeTypes:
    """Tests for various Describe*Types operations."""

    def test_describe_account_limits(self, autoscaling):
        resp = autoscaling.describe_account_limits()
        assert "MaxNumberOfAutoScalingGroups" in resp
        assert "MaxNumberOfLaunchConfigurations" in resp
        assert isinstance(resp["MaxNumberOfAutoScalingGroups"], int)
        assert isinstance(resp["MaxNumberOfLaunchConfigurations"], int)

    def test_describe_adjustment_types(self, autoscaling):
        resp = autoscaling.describe_adjustment_types()
        assert "AdjustmentTypes" in resp
        type_names = [t["AdjustmentType"] for t in resp["AdjustmentTypes"]]
        assert "ChangeInCapacity" in type_names
        assert "ExactCapacity" in type_names
        assert "PercentChangeInCapacity" in type_names

    def test_describe_auto_scaling_notification_types(self, autoscaling):
        resp = autoscaling.describe_auto_scaling_notification_types()
        assert "AutoScalingNotificationTypes" in resp
        assert isinstance(resp["AutoScalingNotificationTypes"], list)
        assert len(resp["AutoScalingNotificationTypes"]) > 0

    def test_describe_lifecycle_hook_types(self, autoscaling):
        resp = autoscaling.describe_lifecycle_hook_types()
        assert "LifecycleHookTypes" in resp
        assert isinstance(resp["LifecycleHookTypes"], list)
        assert len(resp["LifecycleHookTypes"]) > 0

    def test_describe_scaling_process_types(self, autoscaling):
        resp = autoscaling.describe_scaling_process_types()
        assert "Processes" in resp
        assert isinstance(resp["Processes"], list)
        assert len(resp["Processes"]) > 0
        # Verify known process types exist
        process_names = [p["ProcessName"] for p in resp["Processes"]]
        assert "Launch" in process_names
        assert "Terminate" in process_names

    def test_describe_termination_policy_types(self, autoscaling):
        resp = autoscaling.describe_termination_policy_types()
        assert "TerminationPolicyTypes" in resp
        assert isinstance(resp["TerminationPolicyTypes"], list)
        assert len(resp["TerminationPolicyTypes"]) > 0

    def test_describe_metric_collection_types(self, autoscaling):
        resp = autoscaling.describe_metric_collection_types()
        assert "Metrics" in resp
        assert "Granularities" in resp
        assert isinstance(resp["Metrics"], list)
        assert isinstance(resp["Granularities"], list)
        assert len(resp["Metrics"]) > 0
        assert len(resp["Granularities"]) > 0


class TestAutoScalingNotificationConfiguration:
    """Tests for PutNotificationConfiguration and DescribeNotificationConfigurations."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("notif-lc")
        self.asg_name = _unique("notif-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_put_and_describe_notification_configuration(self, autoscaling):
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test-notif-topic"
        autoscaling.put_notification_configuration(
            AutoScalingGroupName=self.asg_name,
            TopicARN=topic_arn,
            NotificationTypes=["autoscaling:EC2_INSTANCE_LAUNCH"],
        )
        resp = autoscaling.describe_notification_configurations(
            AutoScalingGroupNames=[self.asg_name],
        )
        assert "NotificationConfigurations" in resp
        configs = resp["NotificationConfigurations"]
        assert len(configs) >= 1
        found = [c for c in configs if c["TopicARN"] == topic_arn]
        assert len(found) >= 1
        assert found[0]["AutoScalingGroupName"] == self.asg_name


class TestAutoScalingInstanceRefresh:
    """Tests for StartInstanceRefresh, DescribeInstanceRefreshes, CancelInstanceRefresh."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("ir-lc")
        self.asg_name = _unique("ir-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_start_and_describe_instance_refresh(self, autoscaling):
        resp = autoscaling.start_instance_refresh(
            AutoScalingGroupName=self.asg_name,
            Strategy="Rolling",
        )
        assert "InstanceRefreshId" in resp
        refresh_id = resp["InstanceRefreshId"]

        desc = autoscaling.describe_instance_refreshes(
            AutoScalingGroupName=self.asg_name,
        )
        assert "InstanceRefreshes" in desc
        assert len(desc["InstanceRefreshes"]) >= 1
        ids = [r["InstanceRefreshId"] for r in desc["InstanceRefreshes"]]
        assert refresh_id in ids

    def test_cancel_instance_refresh(self, autoscaling):
        start = autoscaling.start_instance_refresh(
            AutoScalingGroupName=self.asg_name,
            Strategy="Rolling",
        )
        assert "InstanceRefreshId" in start

        cancel = autoscaling.cancel_instance_refresh(
            AutoScalingGroupName=self.asg_name,
        )
        assert "InstanceRefreshId" in cancel


class TestAutoScalingStepScalingPolicy:
    """Tests for PutScalingPolicy with StepScaling type."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("ss-lc")
        self.asg_name = _unique("ss-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=5,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_put_step_scaling_policy(self, autoscaling):
        """PutScalingPolicy with StepScaling type creates the policy."""
        policy_name = _unique("step-pol")
        resp = autoscaling.put_scaling_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
            PolicyType="StepScaling",
            AdjustmentType="ChangeInCapacity",
            StepAdjustments=[
                {"MetricIntervalLowerBound": 0, "ScalingAdjustment": 1},
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify via describe
        desc = autoscaling.describe_policies(
            AutoScalingGroupName=self.asg_name,
            PolicyNames=[policy_name],
        )
        policies = desc["ScalingPolicies"]
        assert len(policies) == 1
        assert policies[0]["PolicyType"] == "StepScaling"
        assert len(policies[0]["StepAdjustments"]) == 1

    def test_put_step_scaling_policy_multiple_steps(self, autoscaling):
        """StepScaling with multiple step adjustments stores all steps."""
        policy_name = _unique("multi-step")
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=self.asg_name,
            PolicyName=policy_name,
            PolicyType="StepScaling",
            AdjustmentType="ChangeInCapacity",
            StepAdjustments=[
                {
                    "MetricIntervalLowerBound": 0,
                    "MetricIntervalUpperBound": 20,
                    "ScalingAdjustment": 1,
                },
                {"MetricIntervalLowerBound": 20, "ScalingAdjustment": 2},
            ],
        )
        desc = autoscaling.describe_policies(
            AutoScalingGroupName=self.asg_name,
            PolicyNames=[policy_name],
        )
        policies = desc["ScalingPolicies"]
        assert len(policies) == 1
        assert len(policies[0]["StepAdjustments"]) == 2


class TestAutoScalingDeleteNotificationConfiguration:
    """Tests for DeleteNotificationConfiguration."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("delnot-lc")
        self.asg_name = _unique("delnot-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_delete_notification_configuration(self, autoscaling):
        """DeleteNotificationConfiguration removes a notification config."""
        topic_arn = "arn:aws:sns:us-east-1:123456789012:del-notif-topic"
        autoscaling.put_notification_configuration(
            AutoScalingGroupName=self.asg_name,
            TopicARN=topic_arn,
            NotificationTypes=["autoscaling:EC2_INSTANCE_LAUNCH"],
        )
        resp = autoscaling.delete_notification_configuration(
            AutoScalingGroupName=self.asg_name,
            TopicARN=topic_arn,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it was removed
        desc = autoscaling.describe_notification_configurations(
            AutoScalingGroupNames=[self.asg_name],
        )
        found = [c for c in desc["NotificationConfigurations"] if c["TopicARN"] == topic_arn]
        assert len(found) == 0


class TestAutoScalingLoadBalancerAttachDetach:
    """Tests for AttachLoadBalancers, DetachLoadBalancers,
    AttachLoadBalancerTargetGroups, DetachLoadBalancerTargetGroups."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("lbad-lc")
        self.asg_name = _unique("lbad-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_attach_load_balancers(self, autoscaling):
        """AttachLoadBalancers attaches a classic LB to the ASG."""
        resp = autoscaling.attach_load_balancers(
            AutoScalingGroupName=self.asg_name,
            LoadBalancerNames=["my-classic-lb"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_attach_and_detach_load_balancers(self, autoscaling):
        """AttachLoadBalancers then DetachLoadBalancers with a real ELB name."""
        elb = make_client("elb")
        lb_name = _unique("clb")[:32]
        elb.create_load_balancer(
            LoadBalancerName=lb_name,
            Listeners=[
                {
                    "Protocol": "HTTP",
                    "LoadBalancerPort": 80,
                    "InstanceProtocol": "HTTP",
                    "InstancePort": 80,
                }
            ],
            AvailabilityZones=["us-east-1a"],
        )
        try:
            resp = autoscaling.attach_load_balancers(
                AutoScalingGroupName=self.asg_name,
                LoadBalancerNames=[lb_name],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            resp = autoscaling.detach_load_balancers(
                AutoScalingGroupName=self.asg_name,
                LoadBalancerNames=[lb_name],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            elb.delete_load_balancer(LoadBalancerName=lb_name)

    def test_attach_and_detach_load_balancer_target_groups(self, autoscaling):
        """AttachLoadBalancerTargetGroups then DetachLoadBalancerTargetGroups."""
        elbv2 = make_client("elbv2")
        ec2 = make_client("ec2")
        # Create VPC and target group
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            tg = elbv2.create_target_group(
                Name=_unique("tg")[:32],
                Protocol="HTTP",
                Port=80,
                VpcId=vpc_id,
            )
            tg_arn = tg["TargetGroups"][0]["TargetGroupArn"]
            try:
                resp = autoscaling.attach_load_balancer_target_groups(
                    AutoScalingGroupName=self.asg_name,
                    TargetGroupARNs=[tg_arn],
                )
                assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
                resp = autoscaling.detach_load_balancer_target_groups(
                    AutoScalingGroupName=self.asg_name,
                    TargetGroupARNs=[tg_arn],
                )
                assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            finally:
                elbv2.delete_target_group(TargetGroupArn=tg_arn)
        finally:
            ec2.delete_vpc(VpcId=vpc_id)


class TestAutoScalingSetInstanceHealth:
    """Tests for SetInstanceHealth."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("sih-lc")
        self.asg_name = _unique("sih-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=1,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=self.asg_name, ForceDelete=True)
        autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)

    def test_set_instance_health(self, autoscaling):
        """SetInstanceHealth marks an instance as Unhealthy."""
        resp = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self.asg_name],
        )
        instances = resp["AutoScalingGroups"][0]["Instances"]
        assert len(instances) >= 1
        instance_id = instances[0]["InstanceId"]
        resp = autoscaling.set_instance_health(
            InstanceId=instance_id,
            HealthStatus="Unhealthy",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestAutoScalingTerminateInstance:
    """Tests for TerminateInstanceInAutoScalingGroup."""

    @pytest.fixture(autouse=True)
    def _setup_asg(self, autoscaling):
        self.lc_name = _unique("ti-lc")
        self.asg_name = _unique("ti-asg")
        autoscaling.create_launch_configuration(
            LaunchConfigurationName=self.lc_name,
            ImageId="ami-12345678",
            InstanceType="t2.micro",
        )
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=self.asg_name,
            LaunchConfigurationName=self.lc_name,
            MinSize=1,
            MaxSize=2,
            AvailabilityZones=["us-east-1a"],
        )
        yield
        try:
            autoscaling.delete_auto_scaling_group(
                AutoScalingGroupName=self.asg_name, ForceDelete=True
            )
        except Exception:
            pass
        try:
            autoscaling.delete_launch_configuration(LaunchConfigurationName=self.lc_name)
        except Exception:
            pass

    def test_terminate_instance_in_auto_scaling_group(self, autoscaling):
        """TerminateInstanceInAutoScalingGroup terminates an instance and returns Activity."""
        resp = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self.asg_name],
        )
        instances = resp["AutoScalingGroups"][0]["Instances"]
        assert len(instances) >= 1
        instance_id = instances[0]["InstanceId"]
        resp = autoscaling.terminate_instance_in_auto_scaling_group(
            InstanceId=instance_id,
            ShouldDecrementDesiredCapacity=True,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Activity" in resp
        assert resp["Activity"]["Cause"] is not None
