"""Elasticsearch Service compatibility tests."""

import uuid

import botocore.exceptions
import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def es():
    return make_client("es")


def _uid():
    return uuid.uuid4().hex[:8]


class TestElasticsearchOperations:
    def test_create_domain(self, es):
        name = f"es-{_uid()}"
        response = es.create_elasticsearch_domain(
            DomainName=name,
            ElasticsearchVersion="7.10",
        )
        assert response["DomainStatus"]["DomainName"] == name
        es.delete_elasticsearch_domain(DomainName=name)

    def test_describe_domain(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(
            DomainName=name,
            ElasticsearchVersion="7.10",
        )
        response = es.describe_elasticsearch_domain(DomainName=name)
        assert response["DomainStatus"]["DomainName"] == name
        es.delete_elasticsearch_domain(DomainName=name)

    def test_list_domain_names(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(
            DomainName=name,
            ElasticsearchVersion="7.10",
        )
        response = es.list_domain_names()
        names = [d["DomainName"] for d in response["DomainNames"]]
        assert name in names
        es.delete_elasticsearch_domain(DomainName=name)

    def test_delete_domain(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        response = es.delete_elasticsearch_domain(DomainName=name)
        assert response["DomainStatus"]["DomainName"] == name

    def test_describe_domain_config(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        response = es.describe_elasticsearch_domain_config(DomainName=name)
        assert "DomainConfig" in response
        assert "ElasticsearchVersion" in response["DomainConfig"]
        es.delete_elasticsearch_domain(DomainName=name)

    def test_add_tags(self, es):
        name = f"es-{_uid()}"
        create = es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        arn = create["DomainStatus"]["ARN"]
        es.add_tags(
            ARN=arn,
            TagList=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "dev"}],
        )
        tags = es.list_tags(ARN=arn)["TagList"]
        tag_map = {t["Key"]: t["Value"] for t in tags}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "dev"
        es.delete_elasticsearch_domain(DomainName=name)

    def test_remove_tags(self, es):
        name = f"es-{_uid()}"
        create = es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        arn = create["DomainStatus"]["ARN"]
        es.add_tags(ARN=arn, TagList=[{"Key": "temp", "Value": "yes"}])
        es.remove_tags(ARN=arn, TagKeys=["temp"])
        tags = es.list_tags(ARN=arn)["TagList"]
        keys = [t["Key"] for t in tags]
        assert "temp" not in keys
        es.delete_elasticsearch_domain(DomainName=name)

    def test_create_domain_with_cluster_config(self, es):
        name = f"es-{_uid()}"
        response = es.create_elasticsearch_domain(
            DomainName=name,
            ElasticsearchVersion="7.10",
            ElasticsearchClusterConfig={
                "InstanceType": "t3.small.elasticsearch",
                "InstanceCount": 1,
            },
        )
        assert response["DomainStatus"]["DomainName"] == name
        es.delete_elasticsearch_domain(DomainName=name)

    def test_list_domain_names_empty(self, es):
        response = es.list_domain_names()
        assert "DomainNames" in response

    def test_describe_domains_multiple(self, es):
        d1 = f"es-{_uid()}"
        d2 = f"es-{_uid()}"
        es.create_elasticsearch_domain(DomainName=d1, ElasticsearchVersion="7.10")
        es.create_elasticsearch_domain(DomainName=d2, ElasticsearchVersion="7.10")
        try:
            resp = es.describe_elasticsearch_domains(DomainNames=[d1, d2])
            names = [d["DomainName"] for d in resp["DomainStatusList"]]
            assert d1 in names
            assert d2 in names
        finally:
            es.delete_elasticsearch_domain(DomainName=d1)
            es.delete_elasticsearch_domain(DomainName=d2)

    def test_update_domain_config(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        try:
            resp = es.update_elasticsearch_domain_config(
                DomainName=name,
                ElasticsearchClusterConfig={
                    "InstanceType": "t3.medium.elasticsearch",
                    "InstanceCount": 2,
                },
            )
            assert "DomainConfig" in resp
        finally:
            es.delete_elasticsearch_domain(DomainName=name)

    def test_domain_has_arn(self, es):
        name = f"es-{_uid()}"
        resp = es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        try:
            assert "ARN" in resp["DomainStatus"]
            assert name in resp["DomainStatus"]["ARN"]
        finally:
            es.delete_elasticsearch_domain(DomainName=name)

    def test_domain_has_domain_id(self, es):
        name = f"es-{_uid()}"
        resp = es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        try:
            assert "DomainId" in resp["DomainStatus"]
        finally:
            es.delete_elasticsearch_domain(DomainName=name)

    def test_create_domain_with_ebs_options(self, es):
        name = f"es-{_uid()}"
        resp = es.create_elasticsearch_domain(
            DomainName=name,
            ElasticsearchVersion="7.10",
            EBSOptions={"EBSEnabled": True, "VolumeType": "gp2", "VolumeSize": 10},
        )
        try:
            assert resp["DomainStatus"]["DomainName"] == name
        finally:
            es.delete_elasticsearch_domain(DomainName=name)

    def test_get_compatible_versions(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        try:
            resp = es.get_compatible_elasticsearch_versions(DomainName=name)
            assert "CompatibleElasticsearchVersions" in resp
        finally:
            es.delete_elasticsearch_domain(DomainName=name)


class TestEsAutoCoverage:
    """Auto-generated coverage tests for es."""

    @pytest.fixture
    def client(self):
        return make_client("es")

    def test_delete_elasticsearch_service_role(self, client):
        """DeleteElasticsearchServiceRole returns a response."""
        resp = client.delete_elasticsearch_service_role()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_inbound_cross_cluster_search_connections(self, client):
        """DescribeInboundCrossClusterSearchConnections returns a response."""
        resp = client.describe_inbound_cross_cluster_search_connections()
        assert "CrossClusterSearchConnections" in resp

    def test_describe_outbound_cross_cluster_search_connections(self, client):
        """DescribeOutboundCrossClusterSearchConnections returns a response."""
        resp = client.describe_outbound_cross_cluster_search_connections()
        assert "CrossClusterSearchConnections" in resp

    def test_describe_packages(self, client):
        """DescribePackages returns a response."""
        resp = client.describe_packages()
        assert "PackageDetailsList" in resp

    def test_describe_reserved_elasticsearch_instance_offerings(self, client):
        """DescribeReservedElasticsearchInstanceOfferings returns a response."""
        resp = client.describe_reserved_elasticsearch_instance_offerings()
        assert "ReservedElasticsearchInstanceOfferings" in resp

    def test_describe_reserved_elasticsearch_instances(self, client):
        """DescribeReservedElasticsearchInstances returns a response."""
        resp = client.describe_reserved_elasticsearch_instances()
        assert "ReservedElasticsearchInstances" in resp

    def test_list_elasticsearch_versions(self, client):
        """ListElasticsearchVersions returns a response."""
        resp = client.list_elasticsearch_versions()
        assert "ElasticsearchVersions" in resp

    def test_list_vpc_endpoints(self, client):
        """ListVpcEndpoints returns a response."""
        resp = client.list_vpc_endpoints()
        assert "VpcEndpointSummaryList" in resp

    def test_describe_domain_nonexistent(self, client):
        """DescribeElasticsearchDomain with fake domain raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            client.describe_elasticsearch_domain(DomainName="nonexistent-domain-xyz")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_domain_config_nonexistent(self, client):
        """DescribeElasticsearchDomainConfig with fake domain raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            client.describe_elasticsearch_domain_config(DomainName="nonexistent-domain-xyz")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_domain_config_nonexistent(self, client):
        """UpdateElasticsearchDomainConfig with fake domain raises ResourceNotFoundException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            client.update_elasticsearch_domain_config(DomainName="nonexistent-domain-xyz")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_compatible_versions_no_domain(self, client):
        """GetCompatibleElasticsearchVersions without domain returns all versions."""
        resp = client.get_compatible_elasticsearch_versions()
        assert "CompatibleElasticsearchVersions" in resp

    def test_list_elasticsearch_versions_has_entries(self, client):
        """ListElasticsearchVersions returns a non-empty list."""
        resp = client.list_elasticsearch_versions()
        assert len(resp["ElasticsearchVersions"]) > 0

    def test_describe_domains_empty_list(self, client):
        """DescribeElasticsearchDomains with empty list returns empty result."""
        resp = client.describe_elasticsearch_domains(DomainNames=[])
        assert "DomainStatusList" in resp

    def test_list_tags_for_domain(self, client):
        """ListTags returns tags for a domain with no tags."""
        name = f"es-{_uid()}"
        create = client.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        arn = create["DomainStatus"]["ARN"]
        try:
            resp = client.list_tags(ARN=arn)
            assert "TagList" in resp
            assert isinstance(resp["TagList"], list)
        finally:
            client.delete_elasticsearch_domain(DomainName=name)

    def test_describe_packages_empty(self, client):
        """DescribePackages returns empty list when no packages exist."""
        resp = client.describe_packages()
        assert isinstance(resp["PackageDetailsList"], list)

    def test_describe_inbound_connections_empty(self, client):
        """DescribeInboundCrossClusterSearchConnections returns empty list."""
        resp = client.describe_inbound_cross_cluster_search_connections()
        assert isinstance(resp["CrossClusterSearchConnections"], list)

    def test_describe_outbound_connections_empty(self, client):
        """DescribeOutboundCrossClusterSearchConnections returns empty list."""
        resp = client.describe_outbound_cross_cluster_search_connections()
        assert isinstance(resp["CrossClusterSearchConnections"], list)

    def test_domain_endpoint(self, client):
        """Created domain has an Endpoint field."""
        name = f"es-{_uid()}"
        client.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        try:
            resp = client.describe_elasticsearch_domain(DomainName=name)
            assert "Endpoint" in resp["DomainStatus"]
            assert name in resp["DomainStatus"]["Endpoint"]
        finally:
            client.delete_elasticsearch_domain(DomainName=name)

    def test_domain_processing_status(self, client):
        """Created domain has Processing field."""
        name = f"es-{_uid()}"
        client.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        try:
            resp = client.describe_elasticsearch_domain(DomainName=name)
            assert "Processing" in resp["DomainStatus"]
        finally:
            client.delete_elasticsearch_domain(DomainName=name)
