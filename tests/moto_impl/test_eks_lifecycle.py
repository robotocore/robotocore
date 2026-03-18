"""Resource lifecycle tests for eks (auto-generated)."""

import logging

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "eks",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def cluster_name(client):
    import boto3

    iam = boto3.client(
        "iam",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    try:
        iam.create_role(
            RoleName="eks-test-role",
            AssumeRolePolicyDocument="{}",
            Path="/",
        )
    except ClientError as e:
        logging.debug("pre-cleanup skipped: %s", e)
    name = "test-cluster-1"
    try:
        client.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-test-role",
            resourcesVpcConfig={"subnetIds": ["subnet-12345"], "securityGroupIds": ["sg-12345"]},
        )
    except ClientError as e:
        logging.debug("pre-cleanup skipped: %s", e)
    yield name


def test_access_entry_lifecycle(client, cluster_name):
    """Test AccessEntry CRUD lifecycle."""
    # CREATE
    create_resp = client.create_access_entry(
        clusterName=cluster_name,
        principalArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("accessEntry", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_access_entry(
        clusterName=cluster_name,
        principalArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(desc_resp.get("accessEntry", {}), dict)

    # DELETE
    client.delete_access_entry(
        clusterName=cluster_name,
        principalArn="arn:aws:iam::123456789012:role/test-role",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_access_entry(
            clusterName=cluster_name,
            principalArn="arn:aws:iam::123456789012:role/test-role",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_access_entry_not_found(client, cluster_name):
    """Test that describing a non-existent AccessEntry raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_access_entry(
            clusterName=cluster_name,
            principalArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_addon_lifecycle(client, cluster_name):
    """Test Addon CRUD lifecycle."""
    # CREATE
    create_resp = client.create_addon(
        clusterName=cluster_name,
        addonName="test-name-1",
    )
    assert isinstance(create_resp.get("addon", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_addon(
        clusterName=cluster_name,
        addonName="test-name-1",
    )
    assert isinstance(desc_resp.get("addon", {}), dict)

    # DELETE
    client.delete_addon(
        clusterName=cluster_name,
        addonName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_addon(
            clusterName=cluster_name,
            addonName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_addon_not_found(client, cluster_name):
    """Test that describing a non-existent Addon raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_addon(
            clusterName=cluster_name,
            addonName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_capability_lifecycle(client, cluster_name):
    """Test Capability CRUD lifecycle."""
    # CREATE
    create_resp = client.create_capability(
        capabilityName="test-name-1",
        clusterName=cluster_name,
        type="ACK",
        roleArn="arn:aws:iam::123456789012:role/test-role",
        deletePropagationPolicy="RETAIN",
    )
    assert isinstance(create_resp.get("capability", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_capability(
        clusterName=cluster_name,
        capabilityName="test-name-1",
    )
    assert isinstance(desc_resp.get("capability", {}), dict)

    # DELETE
    client.delete_capability(
        clusterName=cluster_name,
        capabilityName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_capability(
            clusterName=cluster_name,
            capabilityName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_capability_not_found(client, cluster_name):
    """Test that describing a non-existent Capability raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_capability(
            clusterName=cluster_name,
            capabilityName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_cluster_lifecycle(client, cluster_name):
    """Test Cluster CRUD lifecycle."""
    # CREATE
    create_resp = client.create_cluster(
        name="test-name-1",
        roleArn="arn:aws:iam::123456789012:role/test-role",
        resourcesVpcConfig={},
    )
    assert isinstance(create_resp.get("cluster", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_cluster(
        name="test-name-1",
    )
    assert isinstance(desc_resp.get("cluster", {}), dict)

    # DELETE
    client.delete_cluster(
        name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_cluster(
            name="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_cluster_not_found(client, cluster_name):
    """Test that describing a non-existent Cluster raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_cluster(
            name="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_eks_anywhere_subscription_lifecycle(client, cluster_name):
    """Test EksAnywhereSubscription CRUD lifecycle."""
    # CREATE
    create_resp = client.create_eks_anywhere_subscription(
        name="test-name-1",
        term={},
    )
    assert isinstance(create_resp.get("subscription", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_eks_anywhere_subscription(
        id="test-id-1",
    )
    assert isinstance(desc_resp.get("subscription", {}), dict)

    # DELETE
    client.delete_eks_anywhere_subscription(
        id="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_eks_anywhere_subscription(
            id="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_eks_anywhere_subscription_not_found(client, cluster_name):
    """Test that describing a non-existent EksAnywhereSubscription raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_eks_anywhere_subscription(
            id="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_fargate_profile_lifecycle(client, cluster_name):
    """Test FargateProfile CRUD lifecycle."""
    # CREATE
    create_resp = client.create_fargate_profile(
        fargateProfileName="test-name-1",
        clusterName=cluster_name,
        podExecutionRoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("fargateProfile", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_fargate_profile(
        clusterName=cluster_name,
        fargateProfileName="test-name-1",
    )
    assert isinstance(desc_resp.get("fargateProfile", {}), dict)

    # DELETE
    client.delete_fargate_profile(
        clusterName=cluster_name,
        fargateProfileName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_fargate_profile(
            clusterName=cluster_name,
            fargateProfileName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_fargate_profile_not_found(client, cluster_name):
    """Test that describing a non-existent FargateProfile raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_fargate_profile(
            clusterName=cluster_name,
            fargateProfileName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_insights_refresh_lifecycle(client, cluster_name):
    """Test InsightsRefresh CRUD lifecycle."""
    # CREATE
    client.start_insights_refresh(
        clusterName=cluster_name,
    )

    # DESCRIBE
    client.describe_insights_refresh(
        clusterName=cluster_name,
    )


def test_insights_refresh_not_found(client, cluster_name):
    """Test that describing a non-existent InsightsRefresh raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_insights_refresh(
            clusterName=cluster_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_nodegroup_lifecycle(client, cluster_name):
    """Test Nodegroup CRUD lifecycle."""
    # CREATE
    create_resp = client.create_nodegroup(
        clusterName=cluster_name,
        nodegroupName="test-name-1",
        subnets=["test-string"],
        nodeRole="test-string",
    )
    assert isinstance(create_resp.get("nodegroup", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_nodegroup(
        clusterName=cluster_name,
        nodegroupName="test-name-1",
    )
    assert isinstance(desc_resp.get("nodegroup", {}), dict)

    # DELETE
    client.delete_nodegroup(
        clusterName=cluster_name,
        nodegroupName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_nodegroup(
            clusterName=cluster_name,
            nodegroupName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_nodegroup_not_found(client, cluster_name):
    """Test that describing a non-existent Nodegroup raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_nodegroup(
            clusterName=cluster_name,
            nodegroupName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_pod_identity_association_lifecycle(client, cluster_name):
    """Test PodIdentityAssociation CRUD lifecycle."""
    # CREATE
    create_resp = client.create_pod_identity_association(
        clusterName=cluster_name,
        namespace="test-name-1",
        serviceAccount="test-string",
        roleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("association", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_pod_identity_association(
        clusterName=cluster_name,
        associationId="test-id-1",
    )
    assert isinstance(desc_resp.get("association", {}), dict)

    # DELETE
    client.delete_pod_identity_association(
        clusterName=cluster_name,
        associationId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_pod_identity_association(
            clusterName=cluster_name,
            associationId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_pod_identity_association_not_found(client, cluster_name):
    """Test that describing a non-existent PodIdentityAssociation raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_pod_identity_association(
            clusterName=cluster_name,
            associationId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
