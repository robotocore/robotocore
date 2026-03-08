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
