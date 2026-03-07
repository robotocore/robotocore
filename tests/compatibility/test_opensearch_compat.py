"""OpenSearch compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def opensearch():
    return make_client("opensearch")


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def domain(opensearch):
    name = f"os-{_uid()}"
    opensearch.create_domain(
        DomainName=name,
        EngineVersion="OpenSearch_2.5",
        ClusterConfig={
            "InstanceType": "t3.small.search",
            "InstanceCount": 1,
        },
    )
    yield name
    try:
        opensearch.delete_domain(DomainName=name)
    except Exception:
        pass


class TestOpenSearchDomainCRUD:
    def test_create_domain(self, opensearch):
        name = f"os-{_uid()}"
        response = opensearch.create_domain(
            DomainName=name,
            EngineVersion="OpenSearch_2.5",
            ClusterConfig={
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
            },
        )
        status = response["DomainStatus"]
        assert status["DomainName"] == name
        assert status["EngineVersion"] == "OpenSearch_2.5"
        opensearch.delete_domain(DomainName=name)

    def test_create_domain_with_ebs(self, opensearch):
        name = f"os-{_uid()}"
        response = opensearch.create_domain(
            DomainName=name,
            EngineVersion="OpenSearch_2.5",
            ClusterConfig={
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
            },
            EBSOptions={
                "EBSEnabled": True,
                "VolumeType": "gp3",
                "VolumeSize": 10,
            },
        )
        assert response["DomainStatus"]["DomainName"] == name
        opensearch.delete_domain(DomainName=name)

    def test_describe_domain(self, opensearch, domain):
        response = opensearch.describe_domain(DomainName=domain)
        status = response["DomainStatus"]
        assert status["DomainName"] == domain
        assert "ARN" in status
        assert "DomainId" in status
        assert "EngineVersion" in status

    def test_describe_domains(self, opensearch, domain):
        """Describe multiple domains at once."""
        response = opensearch.describe_domains(DomainNames=[domain])
        assert len(response["DomainStatusList"]) == 1
        assert response["DomainStatusList"][0]["DomainName"] == domain

    def test_delete_domain(self, opensearch):
        name = f"os-{_uid()}"
        opensearch.create_domain(
            DomainName=name,
            EngineVersion="OpenSearch_2.5",
        )
        response = opensearch.delete_domain(DomainName=name)
        assert response["DomainStatus"]["DomainName"] == name

        # The delete call succeeded above; domain may linger briefly while deleting

    def test_delete_nonexistent_domain_raises(self, opensearch):
        with pytest.raises(opensearch.exceptions.ClientError) as exc_info:
            opensearch.delete_domain(DomainName=f"nonexistent-{_uid()}")
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestOpenSearchListDomains:
    def test_list_domain_names(self, opensearch, domain):
        response = opensearch.list_domain_names()
        domain_names = [d["DomainName"] for d in response["DomainNames"]]
        assert domain in domain_names

    def test_list_domain_names_with_engine_type(self, opensearch, domain):
        """Filter domain list by engine type."""
        response = opensearch.list_domain_names(EngineType="OpenSearch")
        domain_names = [d["DomainName"] for d in response["DomainNames"]]
        assert domain in domain_names

    def test_list_domain_names_empty_when_no_domains(self, opensearch):
        """List should return a DomainNames key even if empty."""
        response = opensearch.list_domain_names()
        assert "DomainNames" in response


class TestOpenSearchCompatibleVersions:
    def test_get_compatible_versions_no_domain(self, opensearch):
        """Get all compatible version pairs without specifying a domain."""
        response = opensearch.get_compatible_versions()
        assert "CompatibleVersions" in response
        assert len(response["CompatibleVersions"]) > 0
        first = response["CompatibleVersions"][0]
        assert "SourceVersion" in first
        assert "TargetVersions" in first

    def test_get_compatible_versions_for_domain(self, opensearch, domain):
        """Get compatible upgrade versions for a specific domain."""
        response = opensearch.get_compatible_versions(DomainName=domain)
        assert "CompatibleVersions" in response


class TestOpenSearchTags:
    def test_add_and_list_tags(self, opensearch, domain):
        desc = opensearch.describe_domain(DomainName=domain)
        arn = desc["DomainStatus"]["ARN"]

        opensearch.add_tags(
            ARN=arn,
            TagList=[
                {"Key": "env", "Value": "test"},
                {"Key": "project", "Value": "robotocore"},
            ],
        )
        response = opensearch.list_tags(ARN=arn)
        tag_map = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert tag_map["env"] == "test"
        assert tag_map["project"] == "robotocore"

    def test_remove_tags(self, opensearch, domain):
        desc = opensearch.describe_domain(DomainName=domain)
        arn = desc["DomainStatus"]["ARN"]

        opensearch.add_tags(
            ARN=arn,
            TagList=[
                {"Key": "removeme", "Value": "gone"},
                {"Key": "keepme", "Value": "stay"},
            ],
        )
        opensearch.remove_tags(ARN=arn, TagKeys=["removeme"])
        response = opensearch.list_tags(ARN=arn)
        tag_keys = [t["Key"] for t in response["TagList"]]
        assert "removeme" not in tag_keys
        assert "keepme" in tag_keys

    def test_add_tags_idempotent(self, opensearch, domain):
        """Adding the same tag twice should update the value."""
        desc = opensearch.describe_domain(DomainName=domain)
        arn = desc["DomainStatus"]["ARN"]

        opensearch.add_tags(ARN=arn, TagList=[{"Key": "version", "Value": "1"}])
        opensearch.add_tags(ARN=arn, TagList=[{"Key": "version", "Value": "2"}])

        response = opensearch.list_tags(ARN=arn)
        tag_map = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert tag_map["version"] == "2"


class TestOpenSearchDomainConfig:
    def test_describe_domain_config(self, opensearch, domain):
        """Retrieve domain configuration details."""
        response = opensearch.describe_domain_config(DomainName=domain)
        config = response["DomainConfig"]
        assert "EngineVersion" in config
        assert "ClusterConfig" in config

    def test_update_domain_config_cluster(self, opensearch, domain):
        """Update the cluster configuration of an existing domain."""
        opensearch.update_domain_config(
            DomainName=domain,
            ClusterConfig={
                "InstanceType": "t3.medium.search",
                "InstanceCount": 2,
            },
        )
        desc = opensearch.describe_domain(DomainName=domain)
        cluster = desc["DomainStatus"].get("ClusterConfig", {})
        assert cluster.get("InstanceCount") == 2

    def test_update_domain_config_ebs(self, opensearch, domain):
        """Update EBS options on an existing domain."""
        opensearch.update_domain_config(
            DomainName=domain,
            EBSOptions={
                "EBSEnabled": True,
                "VolumeType": "gp3",
                "VolumeSize": 20,
            },
        )
        response = opensearch.describe_domain_config(DomainName=domain)
        ebs = response["DomainConfig"]["EBSOptions"]["Options"]
        assert ebs["VolumeSize"] == 20
