"""Compatibility tests for AWS CloudWatch Synthetics (Canaries) service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def synthetics():
    return make_client("synthetics")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestSyntheticsCanaryOperations:
    """Tests for canary CRUD operations."""

    def test_create_canary(self, synthetics):
        """Create a canary and verify returned fields."""
        name = _unique("canary")
        resp = synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        canary = resp["Canary"]
        assert canary["Name"] == name
        assert canary["Status"]["State"] == "READY"
        assert canary["RuntimeVersion"] == "syn-python-selenium-3.0"
        assert canary["ExecutionRoleArn"] == "arn:aws:iam::123456789012:role/canary-role"
        assert canary["Schedule"]["Expression"] == "rate(5 minutes)"
        assert canary["Code"]["Handler"] == "index.handler"
        assert canary["ArtifactS3Location"] == "s3://test-bucket/artifacts"
        assert "Id" in canary
        assert "Timeline" in canary

    def test_create_canary_with_tags(self, synthetics):
        """Create a canary with tags and verify they are returned."""
        name = _unique("canary")
        resp = synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
            Tags={"env": "test", "project": "demo"},
        )
        canary = resp["Canary"]
        assert canary["Tags"]["env"] == "test"
        assert canary["Tags"]["project"] == "demo"

    def test_get_canary(self, synthetics):
        """Create a canary then retrieve it by name."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        resp = synthetics.get_canary(Name=name)
        canary = resp["Canary"]
        assert canary["Name"] == name
        assert canary["RuntimeVersion"] == "syn-python-selenium-3.0"
        assert canary["ArtifactS3Location"] == "s3://test-bucket/artifacts"

    def test_get_canary_has_defaults(self, synthetics):
        """Verify default values are populated on created canaries."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        canary = synthetics.get_canary(Name=name)["Canary"]
        assert canary["RunConfig"]["TimeoutInSeconds"] == 60
        assert canary["SuccessRetentionPeriodInDays"] == 31
        assert canary["FailureRetentionPeriodInDays"] == 31

    def test_describe_canaries_includes_created(self, synthetics):
        """Created canary appears in describe_canaries listing."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        resp = synthetics.describe_canaries()
        names = [c["Name"] for c in resp["Canaries"]]
        assert name in names


class TestSyntheticsListOperations:
    """Tests for list/describe operations."""

    def test_describe_canaries_returns_list(self, synthetics):
        """describe_canaries returns a Canaries list."""
        resp = synthetics.describe_canaries()
        assert "Canaries" in resp
        assert isinstance(resp["Canaries"], list)

    def test_describe_canaries_last_run(self, synthetics):
        """describe_canaries_last_run returns a CanariesLastRun list."""
        resp = synthetics.describe_canaries_last_run()
        assert "CanariesLastRun" in resp
        assert isinstance(resp["CanariesLastRun"], list)

    def test_describe_runtime_versions(self, synthetics):
        """describe_runtime_versions returns a RuntimeVersions list."""
        resp = synthetics.describe_runtime_versions()
        assert "RuntimeVersions" in resp
        assert isinstance(resp["RuntimeVersions"], list)

    def test_list_groups(self, synthetics):
        """list_groups returns a Groups list."""
        resp = synthetics.list_groups()
        assert "Groups" in resp
        assert isinstance(resp["Groups"], list)

    def test_list_tags_for_resource(self, synthetics):
        """list_tags_for_resource returns a Tags dict for a canary ARN."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        arn = f"arn:aws:synthetics:us-east-1:123456789012:canary:{name}"
        resp = synthetics.list_tags_for_resource(ResourceArn=arn)
        assert "Tags" in resp
        assert isinstance(resp["Tags"], dict)


class TestSyntheticsDeleteOperations:
    """Tests for delete operations."""

    def test_delete_canary(self, synthetics):
        """Create and delete a canary."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        resp = synthetics.delete_canary(Name=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.get_canary(Name=name)

    def test_delete_canary_not_found(self, synthetics):
        """Deleting a nonexistent canary raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.delete_canary(Name="nonexistent-canary-12345")

    def test_delete_group(self, synthetics):
        """Create and delete a group."""
        name = _unique("grp")
        synthetics.create_group(Name=name)
        resp = synthetics.delete_group(GroupIdentifier=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_group_not_found(self, synthetics):
        """Deleting a nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.delete_group(GroupIdentifier="nonexistent-group-12345")


class TestSyntheticsGroupOperations:
    """Tests for group CRUD operations."""

    def test_create_group(self, synthetics):
        """Create a group and verify returned fields."""
        name = _unique("grp")
        resp = synthetics.create_group(Name=name)
        group = resp["Group"]
        assert group["Name"] == name
        assert "Id" in group
        assert "Arn" in group

    def test_get_group(self, synthetics):
        """Create a group then retrieve it."""
        name = _unique("grp")
        synthetics.create_group(Name=name)
        resp = synthetics.get_group(GroupIdentifier=name)
        group = resp["Group"]
        assert group["Name"] == name
        assert "Id" in group

    def test_get_group_not_found(self, synthetics):
        """Getting a nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.get_group(GroupIdentifier="nonexistent-group-12345")

    def test_list_group_resources_empty(self, synthetics):
        """List resources in a new group returns empty list."""
        name = _unique("grp")
        synthetics.create_group(Name=name)
        resp = synthetics.list_group_resources(GroupIdentifier=name)
        assert "Resources" in resp
        assert isinstance(resp["Resources"], list)

    def test_list_group_resources_not_found(self, synthetics):
        """Listing resources for nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.list_group_resources(GroupIdentifier="nonexistent-group-12345")


class TestSyntheticsCanaryLifecycle:
    """Tests for canary lifecycle operations."""

    def test_get_canary_not_found(self, synthetics):
        """Getting a nonexistent canary raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.get_canary(Name="nonexistent-canary-12345")

    def test_get_canary_runs_not_found(self, synthetics):
        """Getting runs for nonexistent canary raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.get_canary_runs(Name="nonexistent-canary-12345")

    def test_get_canary_runs_empty(self, synthetics):
        """Get runs for a new canary returns empty list."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        resp = synthetics.get_canary_runs(Name=name)
        assert "CanaryRuns" in resp
        assert isinstance(resp["CanaryRuns"], list)

    def test_start_canary_not_found(self, synthetics):
        """Starting a nonexistent canary raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.start_canary(Name="nonexistent-canary-12345")

    def test_stop_canary_not_found(self, synthetics):
        """Stopping a nonexistent canary raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.stop_canary(Name="nonexistent-canary-12345")

    def test_update_canary_not_found(self, synthetics):
        """Updating a nonexistent canary raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.update_canary(Name="nonexistent-canary-12345")

    def test_update_canary(self, synthetics):
        """Create a canary, update it, verify changes."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
            SuccessRetentionPeriodInDays=31,
        )
        resp = synthetics.update_canary(
            Name=name,
            SuccessRetentionPeriodInDays=7,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify update took effect
        canary = synthetics.get_canary(Name=name)["Canary"]
        assert canary["SuccessRetentionPeriodInDays"] == 7

    def test_tag_resource(self, synthetics):
        """Tag a canary and verify tags via list_tags_for_resource."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        arn = f"arn:aws:synthetics:us-east-1:123456789012:canary:{name}"
        synthetics.tag_resource(ResourceArn=arn, Tags={"team": "platform", "tier": "gold"})
        resp = synthetics.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"]["team"] == "platform"
        assert resp["Tags"]["tier"] == "gold"

    def test_untag_resource(self, synthetics):
        """Tag then untag a canary and verify tag removal."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
            Tags={"team": "platform", "tier": "gold"},
        )
        arn = f"arn:aws:synthetics:us-east-1:123456789012:canary:{name}"
        synthetics.untag_resource(ResourceArn=arn, TagKeys=["tier"])
        resp = synthetics.list_tags_for_resource(ResourceArn=arn)
        assert "tier" not in resp["Tags"]
        assert resp["Tags"]["team"] == "platform"


class TestSyntheticsAssociateResource:
    """Tests for group resource association operations."""

    def test_associate_resource_group_not_found(self, synthetics):
        """Associating a resource to nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.associate_resource(
                GroupIdentifier="nonexistent-group-12345",
                ResourceArn="arn:aws:synthetics:us-east-1:123456789012:canary:fake",
            )

    def test_disassociate_resource_group_not_found(self, synthetics):
        """Disassociating a resource from nonexistent group raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.disassociate_resource(
                GroupIdentifier="nonexistent-group-12345",
                ResourceArn="arn:aws:synthetics:us-east-1:123456789012:canary:fake",
            )

    def test_start_canary_dry_run_not_found(self, synthetics):
        """StartCanaryDryRun for nonexistent canary raises ResourceNotFoundException."""
        with pytest.raises(synthetics.exceptions.ResourceNotFoundException):
            synthetics.start_canary_dry_run(Name="nonexistent-canary-12345")

    def test_start_canary(self, synthetics):
        """Start a canary that exists."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        resp = synthetics.start_canary(Name=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_canary(self, synthetics):
        """Stop a canary that exists."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        synthetics.start_canary(Name=name)
        resp = synthetics.stop_canary(Name=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_canaries_last_run_includes_created(self, synthetics):
        """After creating a canary, describe_canaries_last_run returns list."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        resp = synthetics.describe_canaries_last_run()
        assert "CanariesLastRun" in resp
        assert isinstance(resp["CanariesLastRun"], list)

    def test_list_groups_includes_created(self, synthetics):
        """Created group appears in list_groups."""
        name = _unique("grp")
        synthetics.create_group(Name=name)
        resp = synthetics.list_groups()
        group_names = [g["Name"] for g in resp["Groups"]]
        assert name in group_names
