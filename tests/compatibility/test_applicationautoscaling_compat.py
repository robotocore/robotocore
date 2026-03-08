"""Application Auto Scaling compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def appas():
    return make_client("application-autoscaling")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestScalableTargetOperations:
    def test_register_scalable_target(self, appas):
        table = _unique("table")
        resource_id = f"table/{table}"
        appas.register_scalable_target(
            ServiceNamespace="dynamodb",
            ResourceId=resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
            MinCapacity=1,
            MaxCapacity=100,
        )
        try:
            resp = appas.describe_scalable_targets(ServiceNamespace="dynamodb")
            targets = resp["ScalableTargets"]
            matching = [t for t in targets if t["ResourceId"] == resource_id]
            assert len(matching) == 1
            assert matching[0]["ScalableDimension"] == "dynamodb:table:ReadCapacityUnits"
            assert matching[0]["MinCapacity"] == 1
            assert matching[0]["MaxCapacity"] == 100
        finally:
            appas.deregister_scalable_target(
                ServiceNamespace="dynamodb",
                ResourceId=resource_id,
                ScalableDimension="dynamodb:table:ReadCapacityUnits",
            )

    def test_describe_scalable_targets_empty(self, appas):
        resp = appas.describe_scalable_targets(ServiceNamespace="dynamodb")
        assert "ScalableTargets" in resp
        assert isinstance(resp["ScalableTargets"], list)

    def test_register_scalable_target_updates_existing(self, appas):
        table = _unique("table")
        resource_id = f"table/{table}"
        appas.register_scalable_target(
            ServiceNamespace="dynamodb",
            ResourceId=resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
            MinCapacity=1,
            MaxCapacity=50,
        )
        try:
            # Update the same target with new capacity
            appas.register_scalable_target(
                ServiceNamespace="dynamodb",
                ResourceId=resource_id,
                ScalableDimension="dynamodb:table:ReadCapacityUnits",
                MinCapacity=5,
                MaxCapacity=200,
            )
            resp = appas.describe_scalable_targets(ServiceNamespace="dynamodb")
            matching = [t for t in resp["ScalableTargets"] if t["ResourceId"] == resource_id]
            assert len(matching) == 1
            assert matching[0]["MinCapacity"] == 5
            assert matching[0]["MaxCapacity"] == 200
        finally:
            appas.deregister_scalable_target(
                ServiceNamespace="dynamodb",
                ResourceId=resource_id,
                ScalableDimension="dynamodb:table:ReadCapacityUnits",
            )

    def test_deregister_scalable_target(self, appas):
        table = _unique("table")
        resource_id = f"table/{table}"
        appas.register_scalable_target(
            ServiceNamespace="dynamodb",
            ResourceId=resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
            MinCapacity=1,
            MaxCapacity=100,
        )
        appas.deregister_scalable_target(
            ServiceNamespace="dynamodb",
            ResourceId=resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
        )
        resp = appas.describe_scalable_targets(ServiceNamespace="dynamodb")
        matching = [t for t in resp["ScalableTargets"] if t["ResourceId"] == resource_id]
        assert len(matching) == 0


class TestScalingPolicyOperations:
    @pytest.fixture(autouse=True)
    def _setup_target(self, appas):
        self.table = _unique("table")
        self.resource_id = f"table/{self.table}"
        appas.register_scalable_target(
            ServiceNamespace="dynamodb",
            ResourceId=self.resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
            MinCapacity=1,
            MaxCapacity=100,
        )
        yield
        appas.deregister_scalable_target(
            ServiceNamespace="dynamodb",
            ResourceId=self.resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
        )

    def test_put_scaling_policy(self, appas):
        policy_name = _unique("policy")
        resp = appas.put_scaling_policy(
            PolicyName=policy_name,
            ServiceNamespace="dynamodb",
            ResourceId=self.resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": 70.0,
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "DynamoDBReadCapacityUtilization",
                },
            },
        )
        assert "PolicyARN" in resp
        try:
            policies_resp = appas.describe_scaling_policies(ServiceNamespace="dynamodb")
            policies = policies_resp["ScalingPolicies"]
            matching = [p for p in policies if p["PolicyName"] == policy_name]
            assert len(matching) == 1
            assert matching[0]["PolicyType"] == "TargetTrackingScaling"
            assert matching[0]["ResourceId"] == self.resource_id
        finally:
            appas.delete_scaling_policy(
                PolicyName=policy_name,
                ServiceNamespace="dynamodb",
                ResourceId=self.resource_id,
                ScalableDimension="dynamodb:table:ReadCapacityUnits",
            )

    def test_describe_scaling_policies_empty(self, appas):
        resp = appas.describe_scaling_policies(ServiceNamespace="dynamodb")
        assert "ScalingPolicies" in resp
        assert isinstance(resp["ScalingPolicies"], list)

    def test_describe_scaling_policies_filtered_by_resource(self, appas):
        policy_name = _unique("policy")
        appas.put_scaling_policy(
            PolicyName=policy_name,
            ServiceNamespace="dynamodb",
            ResourceId=self.resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": 70.0,
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "DynamoDBReadCapacityUtilization",
                },
            },
        )
        try:
            resp = appas.describe_scaling_policies(
                ServiceNamespace="dynamodb",
                ResourceId=self.resource_id,
                ScalableDimension="dynamodb:table:ReadCapacityUnits",
            )
            matching = [p for p in resp["ScalingPolicies"] if p["PolicyName"] == policy_name]
            assert len(matching) == 1
        finally:
            appas.delete_scaling_policy(
                PolicyName=policy_name,
                ServiceNamespace="dynamodb",
                ResourceId=self.resource_id,
                ScalableDimension="dynamodb:table:ReadCapacityUnits",
            )

    def test_delete_scaling_policy(self, appas):
        policy_name = _unique("policy")
        appas.put_scaling_policy(
            PolicyName=policy_name,
            ServiceNamespace="dynamodb",
            ResourceId=self.resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": 70.0,
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "DynamoDBReadCapacityUtilization",
                },
            },
        )
        appas.delete_scaling_policy(
            PolicyName=policy_name,
            ServiceNamespace="dynamodb",
            ResourceId=self.resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
        )
        resp = appas.describe_scaling_policies(
            ServiceNamespace="dynamodb",
            ResourceId=self.resource_id,
            ScalableDimension="dynamodb:table:ReadCapacityUnits",
        )
        matching = [p for p in resp["ScalingPolicies"] if p["PolicyName"] == policy_name]
        assert len(matching) == 0
