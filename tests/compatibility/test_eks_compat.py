"""EKS compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def eks():
    return make_client("eks")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestEKSClusterOperations:
    """Tests for EKS cluster create, describe, list, tags, and delete."""

    def test_create_and_describe_cluster(self, eks):
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cluster = resp["cluster"]
            assert cluster["name"] == name
            assert cluster["status"] == "ACTIVE"
            assert "arn" in cluster
            assert cluster["roleArn"] == "arn:aws:iam::123456789012:role/eks-role"

            # Describe should return same info
            desc = eks.describe_cluster(name=name)["cluster"]
            assert desc["name"] == name
            assert desc["status"] == "ACTIVE"
            assert desc["arn"] == cluster["arn"]
            assert desc["roleArn"] == cluster["roleArn"]
        finally:
            eks.delete_cluster(name=name)

    def test_create_cluster_with_tags(self, eks):
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
            tags={"env": "test", "project": "robotocore"},
        )
        try:
            cluster = resp["cluster"]
            assert cluster["tags"]["env"] == "test"
            assert cluster["tags"]["project"] == "robotocore"
        finally:
            eks.delete_cluster(name=name)

    def test_create_cluster_returns_expected_fields(self, eks):
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cluster = resp["cluster"]
            assert "endpoint" in cluster
            assert "certificateAuthority" in cluster
            assert "kubernetesNetworkConfig" in cluster
            assert "version" in cluster
        finally:
            eks.delete_cluster(name=name)

    def test_list_clusters_includes_created(self, eks):
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            clusters = eks.list_clusters()["clusters"]
            assert name in clusters
        finally:
            eks.delete_cluster(name=name)

    def test_delete_cluster(self, eks):
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        resp = eks.delete_cluster(name=name)
        assert resp["cluster"]["name"] == name

        # Should not appear in list
        clusters = eks.list_clusters()["clusters"]
        assert name not in clusters

    def test_describe_nonexistent_cluster_raises(self, eks):
        with pytest.raises(ClientError) as exc_info:
            eks.describe_cluster(name="nonexistent-cluster")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_nonexistent_cluster_raises(self, eks):
        with pytest.raises(ClientError) as exc_info:
            eks.delete_cluster(name="nonexistent-cluster")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_duplicate_cluster_raises(self, eks):
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.create_cluster(
                    name=name,
                    roleArn="arn:aws:iam::123456789012:role/eks-role",
                    resourcesVpcConfig={
                        "subnetIds": ["subnet-12345"],
                        "securityGroupIds": ["sg-12345"],
                    },
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceInUseException"
        finally:
            eks.delete_cluster(name=name)

    def test_describe_after_delete_raises(self, eks):
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        eks.delete_cluster(name=name)

        with pytest.raises(ClientError) as exc_info:
            eks.describe_cluster(name=name)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestEKSListOperations:
    """Tests for EKS list operations."""

    def test_list_clusters_empty(self, eks):
        # List clusters; result should be a list (may or may not be empty
        # depending on other test state, but the call should succeed)
        resp = eks.list_clusters()
        assert isinstance(resp["clusters"], list)

    def test_list_clusters_multiple(self, eks):
        names = [_unique("cluster") for _ in range(3)]
        for n in names:
            eks.create_cluster(
                name=n,
                roleArn="arn:aws:iam::123456789012:role/eks-role",
                resourcesVpcConfig={
                    "subnetIds": ["subnet-12345"],
                    "securityGroupIds": ["sg-12345"],
                },
            )
        try:
            clusters = eks.list_clusters()["clusters"]
            for n in names:
                assert n in clusters
        finally:
            for n in names:
                eks.delete_cluster(name=n)


class TestEKSFargateProfileOperations:
    """Tests for EKS Fargate profile create, describe, and delete."""

    def test_create_and_describe_fargate_profile(self, eks):
        cluster_name = _unique("cluster")
        profile_name = _unique("fargate")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.create_fargate_profile(
                fargateProfileName=profile_name,
                clusterName=cluster_name,
                podExecutionRoleArn="arn:aws:iam::123456789012:role/fargate-role",
                selectors=[{"namespace": "default"}],
            )
            fp = resp["fargateProfile"]
            assert fp["fargateProfileName"] == profile_name
            assert fp["clusterName"] == cluster_name
            assert "fargateProfileArn" in fp

            desc = eks.describe_fargate_profile(
                fargateProfileName=profile_name,
                clusterName=cluster_name,
            )
            dfp = desc["fargateProfile"]
            assert dfp["fargateProfileName"] == profile_name
            assert dfp["clusterName"] == cluster_name
            assert dfp["fargateProfileArn"] == fp["fargateProfileArn"]
        finally:
            eks.delete_fargate_profile(
                fargateProfileName=profile_name,
                clusterName=cluster_name,
            )
            eks.delete_cluster(name=cluster_name)

    def test_delete_fargate_profile(self, eks):
        cluster_name = _unique("cluster")
        profile_name = _unique("fargate")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            eks.create_fargate_profile(
                fargateProfileName=profile_name,
                clusterName=cluster_name,
                podExecutionRoleArn="arn:aws:iam::123456789012:role/fargate-role",
                selectors=[{"namespace": "default"}],
            )
            resp = eks.delete_fargate_profile(
                fargateProfileName=profile_name,
                clusterName=cluster_name,
            )
            assert resp["fargateProfile"]["fargateProfileName"] == profile_name
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_describe_nonexistent_fargate_profile(self, eks):
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.describe_fargate_profile(
                    fargateProfileName="nonexistent",
                    clusterName=cluster_name,
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_list_fargate_profiles_empty(self, eks):
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.list_fargate_profiles(clusterName=cluster_name)
            assert isinstance(resp["fargateProfileNames"], list)
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_list_fargate_profiles_includes_created(self, eks):
        cluster_name = _unique("cluster")
        profile_name = _unique("fargate")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            eks.create_fargate_profile(
                fargateProfileName=profile_name,
                clusterName=cluster_name,
                podExecutionRoleArn="arn:aws:iam::123456789012:role/fargate-role",
                selectors=[{"namespace": "default"}],
            )
            resp = eks.list_fargate_profiles(clusterName=cluster_name)
            assert profile_name in resp["fargateProfileNames"]
        finally:
            try:
                eks.delete_fargate_profile(
                    fargateProfileName=profile_name, clusterName=cluster_name
                )
            except Exception:
                pass  # best-effort cleanup
            eks.delete_cluster(name=cluster_name)


class TestEKSNodegroupOperations:
    """Tests for EKS managed node group CRUD operations."""

    @pytest.fixture
    def cluster_name(self, eks):
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        yield name
        eks.delete_cluster(name=name)

    def test_create_nodegroup(self, eks, cluster_name):
        ng_name = _unique("ng")
        resp = eks.create_nodegroup(
            clusterName=cluster_name,
            nodegroupName=ng_name,
            nodeRole="arn:aws:iam::123456789012:role/node-role",
            subnets=["subnet-12345"],
        )
        ng = resp["nodegroup"]
        assert ng["nodegroupName"] == ng_name
        assert ng["clusterName"] == cluster_name
        assert "nodegroupArn" in ng
        eks.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)

    def test_describe_nodegroup(self, eks, cluster_name):
        ng_name = _unique("ng")
        eks.create_nodegroup(
            clusterName=cluster_name,
            nodegroupName=ng_name,
            nodeRole="arn:aws:iam::123456789012:role/node-role",
            subnets=["subnet-12345"],
        )
        try:
            resp = eks.describe_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)
            ng = resp["nodegroup"]
            assert ng["nodegroupName"] == ng_name
            assert ng["clusterName"] == cluster_name
            assert ng["nodeRole"] == "arn:aws:iam::123456789012:role/node-role"
        finally:
            eks.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)

    def test_list_nodegroups(self, eks, cluster_name):
        ng_name = _unique("ng")
        eks.create_nodegroup(
            clusterName=cluster_name,
            nodegroupName=ng_name,
            nodeRole="arn:aws:iam::123456789012:role/node-role",
            subnets=["subnet-12345"],
        )
        try:
            resp = eks.list_nodegroups(clusterName=cluster_name)
            assert ng_name in resp["nodegroups"]
        finally:
            eks.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)

    def test_list_nodegroups_empty(self, eks, cluster_name):
        resp = eks.list_nodegroups(clusterName=cluster_name)
        assert isinstance(resp["nodegroups"], list)

    def test_delete_nodegroup(self, eks, cluster_name):
        ng_name = _unique("ng")
        eks.create_nodegroup(
            clusterName=cluster_name,
            nodegroupName=ng_name,
            nodeRole="arn:aws:iam::123456789012:role/node-role",
            subnets=["subnet-12345"],
        )
        resp = eks.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)
        assert resp["nodegroup"]["nodegroupName"] == ng_name

    def test_describe_nonexistent_nodegroup(self, eks, cluster_name):
        with pytest.raises(ClientError) as exc_info:
            eks.describe_nodegroup(clusterName=cluster_name, nodegroupName="nonexistent")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_nodegroup_with_scaling(self, eks, cluster_name):
        ng_name = _unique("ng")
        resp = eks.create_nodegroup(
            clusterName=cluster_name,
            nodegroupName=ng_name,
            nodeRole="arn:aws:iam::123456789012:role/node-role",
            subnets=["subnet-12345"],
            scalingConfig={"minSize": 1, "maxSize": 3, "desiredSize": 2},
        )
        ng = resp["nodegroup"]
        assert ng["scalingConfig"]["minSize"] == 1
        assert ng["scalingConfig"]["maxSize"] == 3
        assert ng["scalingConfig"]["desiredSize"] == 2
        eks.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)


class TestEKSTagOperations:
    """Tests for EKS tag/untag/list-tags operations."""

    def test_list_tags_for_resource(self, eks):
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
            tags={"env": "test"},
        )
        arn = resp["cluster"]["arn"]
        try:
            tags_resp = eks.list_tags_for_resource(resourceArn=arn)
            assert "tags" in tags_resp
            assert tags_resp["tags"].get("env") == "test"
        finally:
            eks.delete_cluster(name=name)

    def test_tag_resource(self, eks):
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        arn = resp["cluster"]["arn"]
        try:
            eks.tag_resource(resourceArn=arn, tags={"team": "platform", "cost": "dev"})
            tags_resp = eks.list_tags_for_resource(resourceArn=arn)
            assert tags_resp["tags"]["team"] == "platform"
            assert tags_resp["tags"]["cost"] == "dev"
        finally:
            eks.delete_cluster(name=name)

    def test_untag_resource(self, eks):
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
            tags={"keep": "yes", "remove": "me"},
        )
        arn = resp["cluster"]["arn"]
        try:
            eks.untag_resource(resourceArn=arn, tagKeys=["remove"])
            tags_resp = eks.list_tags_for_resource(resourceArn=arn)
            assert "remove" not in tags_resp["tags"]
            assert tags_resp["tags"].get("keep") == "yes"
        finally:
            eks.delete_cluster(name=name)


class TestEKSUpdateOperations:
    """Tests for EKS update cluster/nodegroup config operations."""

    def test_update_cluster_config_logging(self, eks):
        """UpdateClusterConfig enables logging and succeeds."""
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.update_cluster_config(
                name=name,
                logging={"clusterLogging": [{"types": ["api", "audit"], "enabled": True}]},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            eks.delete_cluster(name=name)

    def test_update_nodegroup_config_scaling(self, eks):
        """UpdateNodegroupConfig updates scaling configuration."""
        cluster_name = _unique("cluster")
        ng_name = _unique("ng")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            eks.create_nodegroup(
                clusterName=cluster_name,
                nodegroupName=ng_name,
                nodeRole="arn:aws:iam::123456789012:role/node-role",
                subnets=["subnet-12345"],
                scalingConfig={"minSize": 1, "maxSize": 3, "desiredSize": 2},
            )
            resp = eks.update_nodegroup_config(
                clusterName=cluster_name,
                nodegroupName=ng_name,
                scalingConfig={"minSize": 1, "maxSize": 5, "desiredSize": 3},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                eks.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)
            except Exception:
                pass  # best-effort cleanup
            eks.delete_cluster(name=cluster_name)

    def test_update_nodegroup_config_labels(self, eks):
        """UpdateNodegroupConfig adds labels to a nodegroup."""
        cluster_name = _unique("cluster")
        ng_name = _unique("ng")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            eks.create_nodegroup(
                clusterName=cluster_name,
                nodegroupName=ng_name,
                nodeRole="arn:aws:iam::123456789012:role/node-role",
                subnets=["subnet-12345"],
            )
            resp = eks.update_nodegroup_config(
                clusterName=cluster_name,
                nodegroupName=ng_name,
                labels={"addOrUpdateLabels": {"env": "test"}},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                eks.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)
            except Exception:
                pass  # best-effort cleanup
            eks.delete_cluster(name=cluster_name)

    def test_update_cluster_config_nonexistent(self, eks):
        """UpdateClusterConfig on nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc_info:
            eks.update_cluster_config(
                name="nonexistent-cluster",
                logging={"clusterLogging": [{"types": ["api"], "enabled": True}]},
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_nodegroup_config_nonexistent(self, eks):
        """UpdateNodegroupConfig on nonexistent nodegroup raises error."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.update_nodegroup_config(
                    clusterName=cluster_name,
                    nodegroupName="nonexistent",
                    scalingConfig={"minSize": 1, "maxSize": 5, "desiredSize": 3},
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            eks.delete_cluster(name=cluster_name)


class TestEKSNodegroupWithTags:
    """Tests for EKS nodegroup tag operations."""

    def test_create_nodegroup_with_tags(self, eks):
        """CreateNodegroup with tags preserves them."""
        cluster_name = _unique("cluster")
        ng_name = _unique("ng")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.create_nodegroup(
                clusterName=cluster_name,
                nodegroupName=ng_name,
                nodeRole="arn:aws:iam::123456789012:role/node-role",
                subnets=["subnet-12345"],
                tags={"env": "test", "team": "platform"},
            )
            ng = resp["nodegroup"]
            assert ng["tags"]["env"] == "test"
            assert ng["tags"]["team"] == "platform"
        finally:
            try:
                eks.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)
            except Exception:
                pass  # best-effort cleanup
            eks.delete_cluster(name=cluster_name)

    def test_nodegroup_tags_in_describe(self, eks):
        """Tags set at creation appear in describe response."""
        cluster_name = _unique("cluster")
        ng_name = _unique("ng")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            eks.create_nodegroup(
                clusterName=cluster_name,
                nodegroupName=ng_name,
                nodeRole="arn:aws:iam::123456789012:role/node-role",
                subnets=["subnet-12345"],
                tags={"env": "staging"},
            )
            desc = eks.describe_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)
            assert desc["nodegroup"]["tags"].get("env") == "staging"
        finally:
            try:
                eks.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)
            except Exception:
                pass  # best-effort cleanup
            eks.delete_cluster(name=cluster_name)


class TestEKSClusterVersionAndConfig:
    """Tests for EKS cluster version and configuration details."""

    def test_cluster_has_kubernetes_version(self, eks):
        """Cluster response includes a Kubernetes version."""
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cluster = resp["cluster"]
            assert "version" in cluster
            assert cluster["version"]  # not empty
        finally:
            eks.delete_cluster(name=name)

    def test_cluster_has_platform_version(self, eks):
        """Cluster response includes platform version."""
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cluster = resp["cluster"]
            assert "platformVersion" in cluster
        finally:
            eks.delete_cluster(name=name)

    def test_cluster_logging_default(self, eks):
        """Cluster logging config is present in describe response."""
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            desc = eks.describe_cluster(name=name)
            cluster = desc["cluster"]
            assert "logging" in cluster
        finally:
            eks.delete_cluster(name=name)

    def test_cluster_resources_vpc_config(self, eks):
        """Cluster response includes resourcesVpcConfig."""
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cluster = resp["cluster"]
            assert "resourcesVpcConfig" in cluster
        finally:
            eks.delete_cluster(name=name)

    def test_cluster_created_at(self, eks):
        """Cluster response includes createdAt timestamp."""
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cluster = resp["cluster"]
            assert "createdAt" in cluster
        finally:
            eks.delete_cluster(name=name)


class TestEKSFargateProfileAdvanced:
    """Advanced Fargate profile tests."""

    def test_fargate_profile_with_tags(self, eks):
        """CreateFargateProfile with tags preserves them."""
        cluster_name = _unique("cluster")
        profile_name = _unique("fargate")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.create_fargate_profile(
                fargateProfileName=profile_name,
                clusterName=cluster_name,
                podExecutionRoleArn="arn:aws:iam::123456789012:role/fargate-role",
                selectors=[{"namespace": "default"}],
                tags={"env": "test"},
            )
            fp = resp["fargateProfile"]
            assert fp["tags"].get("env") == "test"
        finally:
            try:
                eks.delete_fargate_profile(
                    fargateProfileName=profile_name, clusterName=cluster_name
                )
            except Exception:
                pass  # best-effort cleanup
            eks.delete_cluster(name=cluster_name)

    def test_fargate_profile_with_multiple_selectors(self, eks):
        """CreateFargateProfile with multiple namespace selectors."""
        cluster_name = _unique("cluster")
        profile_name = _unique("fargate")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.create_fargate_profile(
                fargateProfileName=profile_name,
                clusterName=cluster_name,
                podExecutionRoleArn="arn:aws:iam::123456789012:role/fargate-role",
                selectors=[
                    {"namespace": "default"},
                    {"namespace": "kube-system"},
                ],
            )
            fp = resp["fargateProfile"]
            assert len(fp["selectors"]) == 2
            namespaces = [s["namespace"] for s in fp["selectors"]]
            assert "default" in namespaces
            assert "kube-system" in namespaces
        finally:
            try:
                eks.delete_fargate_profile(
                    fargateProfileName=profile_name, clusterName=cluster_name
                )
            except Exception:
                pass  # best-effort cleanup
            eks.delete_cluster(name=cluster_name)

    def test_fargate_profile_has_status(self, eks):
        """FargateProfile response includes status."""
        cluster_name = _unique("cluster")
        profile_name = _unique("fargate")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.create_fargate_profile(
                fargateProfileName=profile_name,
                clusterName=cluster_name,
                podExecutionRoleArn="arn:aws:iam::123456789012:role/fargate-role",
                selectors=[{"namespace": "default"}],
            )
            fp = resp["fargateProfile"]
            assert "status" in fp
        finally:
            try:
                eks.delete_fargate_profile(
                    fargateProfileName=profile_name, clusterName=cluster_name
                )
            except Exception:
                pass  # best-effort cleanup
            eks.delete_cluster(name=cluster_name)

    def test_describe_nonexistent_fargate_on_nonexistent_cluster(self, eks):
        """DescribeFargateProfile on nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc_info:
            eks.describe_fargate_profile(
                fargateProfileName="fake-profile",
                clusterName="nonexistent-cluster",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestEKSAddonOperations:
    """Tests for EKS addon operations."""

    def test_describe_addon_versions(self, eks):
        """DescribeAddonVersions returns addon info without requiring a cluster."""
        resp = eks.describe_addon_versions()
        assert "addons" in resp
        assert isinstance(resp["addons"], list)

    def test_describe_addon_versions_with_kubernetes_version(self, eks):
        """DescribeAddonVersions can filter by Kubernetes version."""
        resp = eks.describe_addon_versions(kubernetesVersion="1.29")
        assert "addons" in resp
        assert isinstance(resp["addons"], list)

    def test_describe_addon_nonexistent_cluster(self, eks):
        """DescribeAddon on nonexistent cluster raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            eks.describe_addon(clusterName="nonexistent-cluster", addonName="vpc-cni")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_addons_on_cluster(self, eks):
        """ListAddons on a real cluster returns empty list."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.list_addons(clusterName=cluster_name)
            assert "addons" in resp
            assert isinstance(resp["addons"], list)
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_list_addons_nonexistent_cluster(self, eks):
        """ListAddons on nonexistent cluster raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            eks.list_addons(clusterName="nonexistent-cluster")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestEKSPodIdentityOperations:
    """Tests for EKS Pod Identity association operations."""

    def test_list_pod_identity_associations_on_cluster(self, eks):
        """ListPodIdentityAssociations on a real cluster returns empty list."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.list_pod_identity_associations(clusterName=cluster_name)
            assert "associations" in resp
            assert isinstance(resp["associations"], list)
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_list_pod_identity_associations_nonexistent_cluster(self, eks):
        """ListPodIdentityAssociations on nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc_info:
            eks.list_pod_identity_associations(clusterName="nonexistent-cluster")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_pod_identity_association_nonexistent(self, eks):
        """DescribePodIdentityAssociation with fake ID raises error."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.describe_pod_identity_association(
                    clusterName=cluster_name,
                    associationId="nonexistent-id",
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            eks.delete_cluster(name=cluster_name)


class TestEKSAccessEntryOperations:
    """Tests for EKS access entry operations."""

    def test_list_access_entries_on_cluster(self, eks):
        """ListAccessEntries on a real cluster returns a list."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.list_access_entries(clusterName=cluster_name)
            assert "accessEntries" in resp
            assert isinstance(resp["accessEntries"], list)
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_list_access_entries_nonexistent_cluster(self, eks):
        """ListAccessEntries on nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc_info:
            eks.list_access_entries(clusterName="nonexistent-cluster")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_access_entry(self, eks):
        """CreateAccessEntry creates an access entry on a cluster."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            principal_arn = "arn:aws:iam::123456789012:role/test-access-role"
            resp = eks.create_access_entry(
                clusterName=cluster_name,
                principalArn=principal_arn,
            )
            entry = resp["accessEntry"]
            assert entry["clusterName"] == cluster_name
            assert entry["principalArn"] == principal_arn
            assert "accessEntryArn" in entry

            # Verify it appears in list
            list_resp = eks.list_access_entries(clusterName=cluster_name)
            assert principal_arn in list_resp["accessEntries"]
        finally:
            eks.delete_cluster(name=cluster_name)


class TestEKSAddonCRUD:
    """Tests for EKS addon create and delete operations."""

    def test_create_and_delete_addon(self, eks):
        """CreateAddon installs an addon, DeleteAddon removes it."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            addon_name = "vpc-cni"
            resp = eks.create_addon(
                clusterName=cluster_name,
                addonName=addon_name,
            )
            addon = resp["addon"]
            assert addon["addonName"] == addon_name
            assert addon["clusterName"] == cluster_name
            assert "addonArn" in addon

            # Verify in list
            list_resp = eks.list_addons(clusterName=cluster_name)
            assert addon_name in list_resp["addons"]

            # Delete
            del_resp = eks.delete_addon(clusterName=cluster_name, addonName=addon_name)
            assert del_resp["addon"]["addonName"] == addon_name
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_delete_addon_nonexistent(self, eks):
        """DeleteAddon on nonexistent addon raises ResourceNotFoundException."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.delete_addon(clusterName=cluster_name, addonName="nonexistent-addon")
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            eks.delete_cluster(name=cluster_name)


class TestEKSPodIdentityCRUD:
    """Tests for EKS Pod Identity association create and delete."""

    def test_create_and_delete_pod_identity_association(self, eks):
        """CreatePodIdentityAssociation creates, delete removes."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.create_pod_identity_association(
                clusterName=cluster_name,
                namespace="default",
                serviceAccount="my-service-account",
                roleArn="arn:aws:iam::123456789012:role/pod-identity-role",
            )
            assoc = resp["association"]
            assert assoc["clusterName"] == cluster_name
            assert assoc["namespace"] == "default"
            assert assoc["serviceAccount"] == "my-service-account"
            assert "associationId" in assoc
            assoc_id = assoc["associationId"]

            # Verify in list
            list_resp = eks.list_pod_identity_associations(clusterName=cluster_name)
            assoc_ids = [a["associationId"] for a in list_resp["associations"]]
            assert assoc_id in assoc_ids

            # Delete
            del_resp = eks.delete_pod_identity_association(
                clusterName=cluster_name, associationId=assoc_id
            )
            assert del_resp["association"]["associationId"] == assoc_id
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_delete_pod_identity_association_nonexistent(self, eks):
        """DeletePodIdentityAssociation with fake ID raises error."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.delete_pod_identity_association(
                    clusterName=cluster_name,
                    associationId="nonexistent-id",
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            eks.delete_cluster(name=cluster_name)


class TestEKSAdditionalOperations:
    """Tests for EKS operations not covered by other test classes."""

    def test_describe_addon_configuration(self, eks):
        """DescribeAddonConfiguration returns configuration schema."""
        resp = eks.describe_addon_configuration(
            addonName="vpc-cni",
            addonVersion="v1.12.0-eksbuild.1",
        )
        assert "addonName" in resp
        assert resp["addonName"] == "vpc-cni"

    def test_describe_cluster_versions(self, eks):
        """DescribeClusterVersions returns version list."""
        resp = eks.describe_cluster_versions()
        assert "clusterVersions" in resp
        assert isinstance(resp["clusterVersions"], list)

    def test_list_access_policies(self, eks):
        """ListAccessPolicies returns policy list."""
        resp = eks.list_access_policies()
        assert "accessPolicies" in resp
        assert isinstance(resp["accessPolicies"], list)

    def test_list_insights_on_cluster(self, eks):
        """ListInsights returns insights for a cluster."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.list_insights(clusterName=cluster_name)
            assert "insights" in resp
            assert isinstance(resp["insights"], list)
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_list_identity_provider_configs(self, eks):
        """ListIdentityProviderConfigs returns config list for a cluster."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.list_identity_provider_configs(clusterName=cluster_name)
            assert "identityProviderConfigs" in resp
            assert isinstance(resp["identityProviderConfigs"], list)
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_list_capabilities_empty(self, eks):
        """ListCapabilities returns capability list."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.list_capabilities(clusterName=cluster_name)
            assert "capabilities" in resp
            assert isinstance(resp["capabilities"], list)
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_list_eks_anywhere_subscriptions(self, eks):
        """ListEksAnywhereSubscriptions returns subscription list."""
        resp = eks.list_eks_anywhere_subscriptions()
        assert "subscriptions" in resp
        assert isinstance(resp["subscriptions"], list)

    def test_update_cluster_version(self, eks):
        """UpdateClusterVersion returns an update response."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.update_cluster_version(
                name=cluster_name,
                version="1.28",
            )
            assert "update" in resp
            assert "id" in resp["update"]
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_update_addon_on_cluster(self, eks):
        """UpdateAddon returns update response for existing addon."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            eks.create_addon(
                clusterName=cluster_name,
                addonName="vpc-cni",
            )
            resp = eks.update_addon(
                clusterName=cluster_name,
                addonName="vpc-cni",
            )
            assert "update" in resp
            assert "id" in resp["update"]
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_describe_insight_nonexistent(self, eks):
        """DescribeInsight with fake ID raises error."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.describe_insight(
                    clusterName=cluster_name,
                    id="fake-insight-id",
                )
            assert exc_info.value.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "NotFoundException",
            )
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_deregister_cluster_nonexistent(self, eks):
        """DeregisterCluster with nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc_info:
            eks.deregister_cluster(name="nonexistent-cluster-xyz")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_identity_provider_config_nonexistent(self, eks):
        """DescribeIdentityProviderConfig with nonexistent config raises error."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.describe_identity_provider_config(
                    clusterName=cluster_name,
                    identityProviderConfig={
                        "type": "oidc",
                        "name": "nonexistent-provider",
                    },
                )
            assert exc_info.value.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidParameterException",
            )
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_associate_identity_provider_config(self, eks):
        """AssociateIdentityProviderConfig associates an OIDC provider."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.associate_identity_provider_config(
                clusterName=cluster_name,
                oidc={
                    "identityProviderConfigName": "my-oidc",
                    "issuerUrl": "https://example.com",
                    "clientId": "my-client-id",
                },
            )
            assert "update" in resp
            assert "id" in resp["update"]
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_disassociate_identity_provider_config(self, eks):
        """DisassociateIdentityProviderConfig on a cluster."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            eks.associate_identity_provider_config(
                clusterName=cluster_name,
                oidc={
                    "identityProviderConfigName": "my-oidc",
                    "issuerUrl": "https://example.com",
                    "clientId": "my-client-id",
                },
            )
            resp = eks.disassociate_identity_provider_config(
                clusterName=cluster_name,
                identityProviderConfig={
                    "type": "oidc",
                    "name": "my-oidc",
                },
            )
            assert "update" in resp
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_associate_encryption_config(self, eks):
        """AssociateEncryptionConfig returns update response."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.associate_encryption_config(
                clusterName=cluster_name,
                encryptionConfig=[
                    {
                        "resources": ["secrets"],
                        "provider": {"keyArn": "arn:aws:kms:us-east-1:123456789012:key/fake-key"},
                    }
                ],
            )
            assert "update" in resp
            assert "id" in resp["update"]
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_create_and_delete_eks_anywhere_subscription(self, eks):
        """CreateEksAnywhereSubscription and DeleteEksAnywhereSubscription lifecycle."""
        resp = eks.create_eks_anywhere_subscription(
            name=_unique("sub"),
            term={"duration": 1, "unit": "MONTHS"},
        )
        sub = resp["subscription"]
        assert "id" in sub
        assert sub["status"] in ("CREATING", "ACTIVE")
        sub_id = sub["id"]

        desc = eks.describe_eks_anywhere_subscription(id=sub_id)
        assert desc["subscription"]["id"] == sub_id

        del_resp = eks.delete_eks_anywhere_subscription(id=sub_id)
        assert del_resp["subscription"]["id"] == sub_id

    def test_describe_capability_nonexistent(self, eks):
        """DescribeCapability with nonexistent name raises error."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.describe_capability(
                    clusterName=cluster_name,
                    capabilityName="nonexistent-cap",
                )
            assert exc_info.value.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "NotFoundException",
            )
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_update_eks_anywhere_subscription(self, eks):
        """UpdateEksAnywhereSubscription on existing subscription."""
        resp = eks.create_eks_anywhere_subscription(
            name=_unique("sub"),
            term={"duration": 1, "unit": "MONTHS"},
        )
        sub_id = resp["subscription"]["id"]
        try:
            upd = eks.update_eks_anywhere_subscription(
                id=sub_id,
                autoRenew=True,
            )
            assert "subscription" in upd
        finally:
            eks.delete_eks_anywhere_subscription(id=sub_id)


class TestEKSDescribePodIdentityAssociation:
    """Tests for DescribePodIdentityAssociation operation."""

    def test_describe_pod_identity_association(self, eks):
        """DescribePodIdentityAssociation returns association details."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            create_resp = eks.create_pod_identity_association(
                clusterName=cluster_name,
                namespace="kube-system",
                serviceAccount="test-sa",
                roleArn="arn:aws:iam::123456789012:role/pod-role",
            )
            assoc_id = create_resp["association"]["associationId"]

            desc_resp = eks.describe_pod_identity_association(
                clusterName=cluster_name,
                associationId=assoc_id,
            )
            assoc = desc_resp["association"]
            assert assoc["associationId"] == assoc_id
            assert assoc["clusterName"] == cluster_name
            assert assoc["namespace"] == "kube-system"
            assert assoc["serviceAccount"] == "test-sa"
            assert assoc["roleArn"] == "arn:aws:iam::123456789012:role/pod-role"

            eks.delete_pod_identity_association(clusterName=cluster_name, associationId=assoc_id)
        finally:
            eks.delete_cluster(name=cluster_name)


class TestEKSClusterCRUDExplicit:
    """Explicit tests for CreateCluster, DescribeCluster, DeleteCluster."""

    def test_create_cluster(self, eks):
        """CreateCluster returns cluster with expected fields."""
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cluster = resp["cluster"]
            assert cluster["name"] == name
            assert "arn" in cluster
            assert cluster["status"] == "ACTIVE"
        finally:
            eks.delete_cluster(name=name)

    def test_describe_cluster(self, eks):
        """DescribeCluster returns full cluster details."""
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = eks.describe_cluster(name=name)
            cluster = resp["cluster"]
            assert cluster["name"] == name
            assert cluster["status"] == "ACTIVE"
            assert "arn" in cluster
            assert "endpoint" in cluster
            assert "resourcesVpcConfig" in cluster
        finally:
            eks.delete_cluster(name=name)

    def test_delete_cluster(self, eks):
        """DeleteCluster removes cluster and returns it."""
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        resp = eks.delete_cluster(name=name)
        assert resp["cluster"]["name"] == name

        with pytest.raises(ClientError) as exc_info:
            eks.describe_cluster(name=name)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_deregister_cluster_nonexistent(self, eks):
        """DeregisterCluster with nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc_info:
            eks.deregister_cluster(name="nonexistent-cluster-deregister")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestEKSCapabilityOperations:
    """Tests for EKS capability create, describe, update, list, and delete."""

    def test_create_and_describe_capability(self, eks):
        """CreateCapability and DescribeCapability lifecycle."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cap_name = _unique("cap")
            resp = eks.create_capability(
                clusterName=cluster_name,
                capabilityName=cap_name,
                type="ACK",
                roleArn="arn:aws:iam::123456789012:role/cap-role",
                deletePropagationPolicy="RETAIN",
            )
            cap = resp["capability"]
            assert cap["capabilityName"] == cap_name
            assert cap["clusterName"] == cluster_name
            assert cap["type"] == "ACK"
            assert cap["roleArn"] == "arn:aws:iam::123456789012:role/cap-role"
            assert cap["status"] == "ACTIVE"
            assert "arn" in cap
            assert cap["deletePropagationPolicy"] == "RETAIN"

            # Describe should return same info
            desc = eks.describe_capability(
                clusterName=cluster_name,
                capabilityName=cap_name,
            )
            desc_cap = desc["capability"]
            assert desc_cap["capabilityName"] == cap_name
            assert desc_cap["arn"] == cap["arn"]
            assert desc_cap["type"] == "ACK"

            eks.delete_capability(clusterName=cluster_name, capabilityName=cap_name)
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_delete_capability(self, eks):
        """DeleteCapability removes a capability from the cluster."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cap_name = _unique("cap")
            eks.create_capability(
                clusterName=cluster_name,
                capabilityName=cap_name,
                type="ACK",
                roleArn="arn:aws:iam::123456789012:role/cap-role",
                deletePropagationPolicy="RETAIN",
            )
            resp = eks.delete_capability(
                clusterName=cluster_name,
                capabilityName=cap_name,
            )
            assert "capability" in resp
            assert resp["capability"]["capabilityName"] == cap_name

            # After deletion, list should not include it
            caps = eks.list_capabilities(clusterName=cluster_name)
            assert cap_name not in [c["capabilityName"] for c in caps.get("capabilities", [])]
        finally:
            eks.delete_cluster(name=cluster_name)

    def test_update_capability(self, eks):
        """UpdateCapability changes capability configuration."""
        cluster_name = _unique("cluster")
        eks.create_cluster(
            name=cluster_name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cap_name = _unique("cap")
            eks.create_capability(
                clusterName=cluster_name,
                capabilityName=cap_name,
                type="ACK",
                roleArn="arn:aws:iam::123456789012:role/cap-role",
                deletePropagationPolicy="RETAIN",
            )
            resp = eks.update_capability(
                clusterName=cluster_name,
                capabilityName=cap_name,
                roleArn="arn:aws:iam::123456789012:role/new-cap-role",
            )
            assert "update" in resp
            update = resp["update"]
            assert "id" in update
            assert update["type"] == "CapabilityUpdate"
            assert update["status"] == "Successful"

            eks.delete_capability(clusterName=cluster_name, capabilityName=cap_name)
        finally:
            eks.delete_cluster(name=cluster_name)
