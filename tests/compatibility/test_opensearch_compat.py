"""OpenSearch compatibility tests."""

import json
import uuid

import pytest

from tests.compatibility.conftest import make_client


def _unique_domain():
    # OpenSearch domain names: 3-28 lowercase chars, start with letter
    return f"os-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def opensearch():
    return make_client("opensearch")


class TestOpenSearchOperations:
    def test_create_domain(self, opensearch):
        response = opensearch.create_domain(
            DomainName="test-domain",
            EngineVersion="OpenSearch_2.5",
            ClusterConfig={
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
            },
        )
        status = response["DomainStatus"]
        assert status["DomainName"] == "test-domain"
        assert status["EngineVersion"] == "OpenSearch_2.5"

        # Cleanup
        opensearch.delete_domain(DomainName="test-domain")

    def test_describe_domain(self, opensearch):
        opensearch.create_domain(
            DomainName="describe-domain",
            EngineVersion="OpenSearch_2.5",
        )
        response = opensearch.describe_domain(DomainName="describe-domain")
        assert response["DomainStatus"]["DomainName"] == "describe-domain"

        # Cleanup
        opensearch.delete_domain(DomainName="describe-domain")

    def test_list_domain_names(self, opensearch):
        opensearch.create_domain(
            DomainName="list-domain",
            EngineVersion="OpenSearch_2.5",
        )
        response = opensearch.list_domain_names()
        domain_names = [d["DomainName"] for d in response["DomainNames"]]
        assert "list-domain" in domain_names

        # Cleanup
        opensearch.delete_domain(DomainName="list-domain")

    def test_delete_domain(self, opensearch):
        opensearch.create_domain(
            DomainName="delete-domain",
            EngineVersion="OpenSearch_2.5",
        )
        response = opensearch.delete_domain(DomainName="delete-domain")
        assert response["DomainStatus"]["DomainName"] == "delete-domain"

    def test_add_and_list_tags(self, opensearch):
        create_response = opensearch.create_domain(
            DomainName="tags-domain",
            EngineVersion="OpenSearch_2.5",
        )
        arn = create_response["DomainStatus"]["ARN"]

        opensearch.add_tags(
            ARN=arn,
            TagList=[{"Key": "env", "Value": "test"}],
        )
        response = opensearch.list_tags(ARN=arn)
        tag_keys = [t["Key"] for t in response["TagList"]]
        assert "env" in tag_keys

        # Cleanup
        opensearch.delete_domain(DomainName="tags-domain")

    def test_remove_tags(self, opensearch):
        domain_name = _unique_domain()
        create_response = opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
        )
        arn = create_response["DomainStatus"]["ARN"]

        opensearch.add_tags(
            ARN=arn,
            TagList=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        opensearch.remove_tags(ARN=arn, TagKeys=["team"])
        response = opensearch.list_tags(ARN=arn)
        tag_keys = [t["Key"] for t in response["TagList"]]
        assert "env" in tag_keys
        assert "team" not in tag_keys

        opensearch.delete_domain(DomainName=domain_name)

    def test_describe_domains_multiple(self, opensearch):
        d1 = _unique_domain()
        d2 = _unique_domain()
        opensearch.create_domain(DomainName=d1, EngineVersion="OpenSearch_2.5")
        opensearch.create_domain(DomainName=d2, EngineVersion="OpenSearch_2.5")

        response = opensearch.describe_domains(DomainNames=[d1, d2])
        names = [d["DomainName"] for d in response["DomainStatusList"]]
        assert d1 in names
        assert d2 in names

        opensearch.delete_domain(DomainName=d1)
        opensearch.delete_domain(DomainName=d2)

    def test_describe_domain_config(self, opensearch):
        domain_name = _unique_domain()
        opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
            ClusterConfig={
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
            },
        )

        response = opensearch.describe_domain_config(DomainName=domain_name)
        config = response["DomainConfig"]
        assert "EngineVersion" in config
        assert "ClusterConfig" in config

        opensearch.delete_domain(DomainName=domain_name)

    def test_update_domain_config(self, opensearch):
        domain_name = _unique_domain()
        opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
            ClusterConfig={
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
            },
        )

        response = opensearch.update_domain_config(
            DomainName=domain_name,
            ClusterConfig={
                "InstanceType": "t3.medium.search",
                "InstanceCount": 2,
            },
        )
        assert "DomainConfig" in response

        opensearch.delete_domain(DomainName=domain_name)

    def test_get_compatible_versions(self, opensearch):
        domain_name = _unique_domain()
        opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
        )

        response = opensearch.get_compatible_versions(DomainName=domain_name)
        assert "CompatibleVersions" in response

        opensearch.delete_domain(DomainName=domain_name)

    def test_create_domain_with_ebs_options(self, opensearch):
        domain_name = _unique_domain()
        response = opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
            EBSOptions={
                "EBSEnabled": True,
                "VolumeType": "gp2",
                "VolumeSize": 10,
            },
        )
        status = response["DomainStatus"]
        assert status["DomainName"] == domain_name

        opensearch.delete_domain(DomainName=domain_name)

    def test_create_domain_with_access_policies(self, opensearch):
        domain_name = _unique_domain()
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "es:*",
                        "Resource": f"arn:aws:es:us-east-1:123456789012:domain/{domain_name}/*",
                    }
                ],
            }
        )
        response = opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
            AccessPolicies=policy,
        )
        assert response["DomainStatus"]["DomainName"] == domain_name

        opensearch.delete_domain(DomainName=domain_name)

    def test_update_domain_config_access_policies(self, opensearch):
        domain_name = _unique_domain()
        opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
        )

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "es:ESHttpGet",
                        "Resource": f"arn:aws:es:us-east-1:123456789012:domain/{domain_name}/*",
                    }
                ],
            }
        )
        response = opensearch.update_domain_config(
            DomainName=domain_name,
            AccessPolicies=policy,
        )
        assert "DomainConfig" in response

        opensearch.delete_domain(DomainName=domain_name)

    def test_create_domain_with_encryption_at_rest(self, opensearch):
        domain_name = _unique_domain()
        response = opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
            EncryptionAtRestOptions={"Enabled": True},
        )
        assert response["DomainStatus"]["DomainName"] == domain_name

        opensearch.delete_domain(DomainName=domain_name)

    def test_create_domain_with_node_to_node_encryption(self, opensearch):
        domain_name = _unique_domain()
        response = opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
            NodeToNodeEncryptionOptions={"Enabled": True},
        )
        assert response["DomainStatus"]["DomainName"] == domain_name

        opensearch.delete_domain(DomainName=domain_name)

    def test_list_domain_names_with_engine_type(self, opensearch):
        domain_name = _unique_domain()
        opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
        )

        response = opensearch.list_domain_names(EngineType="OpenSearch")
        names = [d["DomainName"] for d in response["DomainNames"]]
        assert domain_name in names

        opensearch.delete_domain(DomainName=domain_name)

    def test_describe_domain_has_arn(self, opensearch):
        domain_name = _unique_domain()
        opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
        )

        response = opensearch.describe_domain(DomainName=domain_name)
        status = response["DomainStatus"]
        assert "ARN" in status
        assert domain_name in status["ARN"]
        assert "DomainId" in status

        opensearch.delete_domain(DomainName=domain_name)


class TestOpenSearchExtended:
    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_create_domain_with_cluster_config(self, opensearch):
        name = _unique_domain()
        resp = opensearch.create_domain(
            DomainName=name,
            EngineVersion="OpenSearch_2.5",
            ClusterConfig={
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
            },
        )
        try:
            assert resp["DomainStatus"]["DomainName"] == name
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_domain_processing_status(self, opensearch):
        name = _unique_domain()
        opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            resp = opensearch.describe_domain(DomainName=name)
            assert "Processing" in resp["DomainStatus"]
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_domain_engine_version(self, opensearch):
        name = _unique_domain()
        opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            resp = opensearch.describe_domain(DomainName=name)
            assert resp["DomainStatus"]["EngineVersion"] == "OpenSearch_2.5"
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_describe_domain_config_engine_version(self, opensearch):
        name = _unique_domain()
        opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            resp = opensearch.describe_domain_config(DomainName=name)
            assert "DomainConfig" in resp
            assert "EngineVersion" in resp["DomainConfig"]
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_list_versions(self, opensearch):
        resp = opensearch.list_versions()
        assert "Versions" in resp
        assert len(resp["Versions"]) > 0

    def test_list_domain_names_empty(self, opensearch):
        resp = opensearch.list_domain_names()
        assert "DomainNames" in resp

    def test_add_multiple_tags(self, opensearch):
        name = _unique_domain()
        resp = opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        arn = resp["DomainStatus"]["ARN"]
        try:
            opensearch.add_tags(
                ARN=arn,
                TagList=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                    {"Key": "project", "Value": "search"},
                ],
            )
            tags = opensearch.list_tags(ARN=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "platform"
            assert tag_map["project"] == "search"
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_remove_specific_tags(self, opensearch):
        name = _unique_domain()
        resp = opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        arn = resp["DomainStatus"]["ARN"]
        try:
            opensearch.add_tags(
                ARN=arn,
                TagList=[
                    {"Key": "keep", "Value": "yes"},
                    {"Key": "remove", "Value": "yes"},
                ],
            )
            opensearch.remove_tags(ARN=arn, TagKeys=["remove"])
            tags = opensearch.list_tags(ARN=arn)
            keys = [t["Key"] for t in tags["TagList"]]
            assert "keep" in keys
            assert "remove" not in keys
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_update_domain_cluster_config(self, opensearch):
        name = _unique_domain()
        opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            resp = opensearch.update_domain_config(
                DomainName=name,
                ClusterConfig={"InstanceType": "t3.medium.search", "InstanceCount": 2},
            )
            assert "DomainConfig" in resp
        finally:
            opensearch.delete_domain(DomainName=name)


class TestOpenSearchGapStubs:
    """Tests for gap operations: list_domain_names, list_versions, list_vpc_endpoints."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_list_domain_names_empty(self, opensearch):
        resp = opensearch.list_domain_names()
        assert "DomainNames" in resp

    def test_list_versions(self, opensearch):
        resp = opensearch.list_versions()
        assert "Versions" in resp
        assert len(resp["Versions"]) > 0

    def test_list_vpc_endpoints(self, opensearch):
        resp = opensearch.list_vpc_endpoints()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
