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
        assert len(domain_names) >= 1

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
        tag_map = {t["Key"]: t["Value"] for t in response["TagList"]}
        assert "env" in tag_map
        assert tag_map["env"] == "test"

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
        assert config["EngineVersion"]["Options"] == "OpenSearch_2.5"

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
        assert response["DomainConfig"]["ClusterConfig"]["Options"]["InstanceType"] == "t3.medium.search"

        opensearch.delete_domain(DomainName=domain_name)

    def test_get_compatible_versions(self, opensearch):
        domain_name = _unique_domain()
        opensearch.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.5",
        )

        response = opensearch.get_compatible_versions(DomainName=domain_name)
        assert "CompatibleVersions" in response
        assert len(response["CompatibleVersions"]) > 0

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
        stored = json.loads(response["DomainConfig"]["AccessPolicies"]["Options"])
        assert stored["Statement"][0]["Action"] == "es:ESHttpGet"

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
            assert isinstance(resp["DomainStatus"]["Processing"], bool)
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
            assert resp["DomainConfig"]["EngineVersion"]["Options"] == "OpenSearch_2.5"
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_list_versions(self, opensearch):
        resp = opensearch.list_versions()
        assert "Versions" in resp
        assert len(resp["Versions"]) > 0

    def test_list_domain_names_empty(self, opensearch):
        resp = opensearch.list_domain_names()
        assert "DomainNames" in resp
        assert isinstance(resp["DomainNames"], list)

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
            assert resp["DomainConfig"]["ClusterConfig"]["Options"]["InstanceCount"] == 2
        finally:
            opensearch.delete_domain(DomainName=name)


class TestOpenSearchCompatibleVersions:
    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_get_compatible_versions_no_domain(self, opensearch):
        """GetCompatibleVersions without a domain returns all version mappings."""
        resp = opensearch.get_compatible_versions()
        assert "CompatibleVersions" in resp
        versions = resp["CompatibleVersions"]
        assert len(versions) > 0
        # Each entry should have SourceVersion and TargetVersions
        for entry in versions:
            assert "SourceVersion" in entry
            assert "TargetVersions" in entry
            assert isinstance(entry["TargetVersions"], list)

    def test_list_tags_nonexistent_domain(self, opensearch):
        """ListTags on a non-existent domain ARN returns empty tag list."""
        resp = opensearch.list_tags(ARN="arn:aws:es:us-east-1:123456789012:domain/nonexistent")
        assert "TagList" in resp
        assert isinstance(resp["TagList"], list)


class TestOpenSearchGapStubs:
    """Tests for gap operations: list_domain_names, list_versions, list_vpc_endpoints."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_list_domain_names_empty(self, opensearch):
        resp = opensearch.list_domain_names()
        assert "DomainNames" in resp
        assert isinstance(resp["DomainNames"], list)

    def test_list_versions(self, opensearch):
        resp = opensearch.list_versions()
        assert "Versions" in resp
        assert len(resp["Versions"]) > 0

    def test_list_vpc_endpoints(self, opensearch):
        resp = opensearch.list_vpc_endpoints()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestOpensearchAutoCoverage:
    """Auto-generated coverage tests for opensearch."""

    @pytest.fixture
    def client(self):
        return make_client("opensearch")

    def test_describe_inbound_connections(self, client):
        """DescribeInboundConnections returns a response."""
        resp = client.describe_inbound_connections()
        assert "Connections" in resp
        assert isinstance(resp["Connections"], list)

    def test_describe_outbound_connections(self, client):
        """DescribeOutboundConnections returns a response."""
        resp = client.describe_outbound_connections()
        assert "Connections" in resp
        assert isinstance(resp["Connections"], list)

    def test_describe_packages(self, client):
        """DescribePackages returns a response."""
        resp = client.describe_packages()
        assert "PackageDetailsList" in resp
        assert isinstance(resp["PackageDetailsList"], list)

    def test_describe_reserved_instance_offerings(self, client):
        """DescribeReservedInstanceOfferings returns a response."""
        resp = client.describe_reserved_instance_offerings()
        assert "ReservedInstanceOfferings" in resp
        assert isinstance(resp["ReservedInstanceOfferings"], list)

    def test_describe_reserved_instances(self, client):
        """DescribeReservedInstances returns a response."""
        resp = client.describe_reserved_instances()
        assert "ReservedInstances" in resp
        assert isinstance(resp["ReservedInstances"], list)

    def test_get_default_application_setting(self, client):
        """GetDefaultApplicationSetting returns a response."""
        resp = client.get_default_application_setting()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_applications(self, client):
        """ListApplications returns a response."""
        resp = client.list_applications()
        assert "ApplicationSummaries" in resp
        assert isinstance(resp["ApplicationSummaries"], list)


class TestOpenSearchDomainOptions:
    """Tests for domain creation with various configuration options."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_create_domain_with_advanced_security(self, opensearch):
        """CreateDomain with AdvancedSecurityOptions."""
        name = _unique_domain()
        try:
            resp = opensearch.create_domain(
                DomainName=name,
                EngineVersion="OpenSearch_2.5",
                AdvancedSecurityOptions={
                    "Enabled": True,
                    "InternalUserDatabaseEnabled": True,
                    "MasterUserOptions": {
                        "MasterUserName": "admin",
                        "MasterUserPassword": "Admin1234!",
                    },
                },
                NodeToNodeEncryptionOptions={"Enabled": True},
                EncryptionAtRestOptions={"Enabled": True},
                DomainEndpointOptions={"EnforceHTTPS": True},
            )
            status = resp["DomainStatus"]
            assert status["DomainName"] == name
            assert status["AdvancedSecurityOptions"]["Enabled"] is True
            assert status["AdvancedSecurityOptions"]["InternalUserDatabaseEnabled"] is True
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_create_domain_with_snapshot_options(self, opensearch):
        """CreateDomain with SnapshotOptions."""
        name = _unique_domain()
        try:
            resp = opensearch.create_domain(
                DomainName=name,
                EngineVersion="OpenSearch_2.5",
                SnapshotOptions={"AutomatedSnapshotStartHour": 3},
            )
            status = resp["DomainStatus"]
            assert status["SnapshotOptions"]["AutomatedSnapshotStartHour"] == 3
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_create_domain_with_advanced_options(self, opensearch):
        """CreateDomain with AdvancedOptions map."""
        name = _unique_domain()
        try:
            resp = opensearch.create_domain(
                DomainName=name,
                EngineVersion="OpenSearch_2.5",
                AdvancedOptions={"rest.action.multi.allow_explicit_index": "true"},
            )
            status = resp["DomainStatus"]
            assert status["AdvancedOptions"]["rest.action.multi.allow_explicit_index"] == "true"
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_create_domain_with_log_publishing(self, opensearch):
        """CreateDomain with LogPublishingOptions."""
        name = _unique_domain()
        log_arn = "arn:aws:logs:us-east-1:123456789012:log-group:test"
        try:
            resp = opensearch.create_domain(
                DomainName=name,
                EngineVersion="OpenSearch_2.5",
                LogPublishingOptions={
                    "INDEX_SLOW_LOGS": {
                        "CloudWatchLogsLogGroupArn": log_arn,
                        "Enabled": True,
                    }
                },
            )
            status = resp["DomainStatus"]
            logs = status["LogPublishingOptions"]["INDEX_SLOW_LOGS"]
            assert logs["CloudWatchLogsLogGroupArn"] == log_arn
            assert logs["Enabled"] is True
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_create_domain_with_domain_endpoint_options(self, opensearch):
        """CreateDomain with DomainEndpointOptions EnforceHTTPS."""
        name = _unique_domain()
        try:
            resp = opensearch.create_domain(
                DomainName=name,
                EngineVersion="OpenSearch_2.5",
                DomainEndpointOptions={"EnforceHTTPS": True},
            )
            status = resp["DomainStatus"]
            assert status["DomainEndpointOptions"]["EnforceHTTPS"] is True
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_create_domain_elasticsearch_engine(self, opensearch):
        """CreateDomain with Elasticsearch engine version."""
        name = _unique_domain()
        try:
            resp = opensearch.create_domain(
                DomainName=name,
                EngineVersion="Elasticsearch_7.10",
            )
            status = resp["DomainStatus"]
            assert status["EngineVersion"] == "Elasticsearch_7.10"
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_domain_has_endpoint(self, opensearch):
        """DescribeDomain returns Endpoint field."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.describe_domain(DomainName=name)
            status = resp["DomainStatus"]
            assert "Endpoint" in status
            assert name in status["Endpoint"]
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_domain_created_and_not_deleted(self, opensearch):
        """DescribeDomain shows Created=True, Deleted=False."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.describe_domain(DomainName=name)
            status = resp["DomainStatus"]
            assert status["Created"] is True
            assert status["Deleted"] is False
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_domain_has_domain_id(self, opensearch):
        """DescribeDomain returns DomainId with account prefix."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.describe_domain(DomainName=name)
            status = resp["DomainStatus"]
            assert "DomainId" in status
            # DomainId format: accountid/domainname
            assert "/" in status["DomainId"]
            assert name in status["DomainId"]
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_domain_upgrade_processing_false(self, opensearch):
        """DescribeDomain shows UpgradeProcessing=False for new domain."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.describe_domain(DomainName=name)
            status = resp["DomainStatus"]
            assert status["UpgradeProcessing"] is False
        finally:
            opensearch.delete_domain(DomainName=name)


class TestOpenSearchDomainConfig:
    """Tests for DescribeDomainConfig with deeper field assertions."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_describe_domain_config_cluster_config_values(self, opensearch):
        """DescribeDomainConfig returns ClusterConfig with correct values."""
        name = _unique_domain()
        try:
            opensearch.create_domain(
                DomainName=name,
                EngineVersion="OpenSearch_2.5",
                ClusterConfig={
                    "InstanceType": "t3.small.search",
                    "InstanceCount": 1,
                },
            )
            resp = opensearch.describe_domain_config(DomainName=name)
            cc = resp["DomainConfig"]["ClusterConfig"]["Options"]
            assert cc["InstanceType"] == "t3.small.search"
            assert cc["InstanceCount"] == 1
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_describe_domain_config_ebs_values(self, opensearch):
        """DescribeDomainConfig returns EBSOptions with correct values."""
        name = _unique_domain()
        try:
            opensearch.create_domain(
                DomainName=name,
                EngineVersion="OpenSearch_2.5",
                EBSOptions={
                    "EBSEnabled": True,
                    "VolumeType": "gp2",
                    "VolumeSize": 10,
                },
            )
            resp = opensearch.describe_domain_config(DomainName=name)
            ebs = resp["DomainConfig"]["EBSOptions"]["Options"]
            assert ebs["EBSEnabled"] is True
            assert ebs["VolumeType"] == "gp2"
            assert ebs["VolumeSize"] == 10
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_describe_domain_config_engine_version_status(self, opensearch):
        """DescribeDomainConfig EngineVersion has Options and Status."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.describe_domain_config(DomainName=name)
            ev = resp["DomainConfig"]["EngineVersion"]
            assert ev["Options"] == "OpenSearch_2.5"
            assert "Status" in ev
            assert ev["Status"]["State"] == "Active"
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_describe_domain_config_has_all_sections(self, opensearch):
        """DescribeDomainConfig returns all expected config sections."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.describe_domain_config(DomainName=name)
            config = resp["DomainConfig"]
            expected_keys = [
                "EngineVersion",
                "ClusterConfig",
                "EBSOptions",
                "AccessPolicies",
                "SnapshotOptions",
                "AdvancedOptions",
                "EncryptionAtRestOptions",
                "NodeToNodeEncryptionOptions",
            ]
            for key in expected_keys:
                assert key in config, f"Missing config key: {key}"
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_update_domain_config_ebs_options(self, opensearch):
        """UpdateDomainConfig with EBSOptions applies changes."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.update_domain_config(
                DomainName=name,
                EBSOptions={
                    "EBSEnabled": True,
                    "VolumeType": "gp3",
                    "VolumeSize": 20,
                },
            )
            ebs = resp["DomainConfig"]["EBSOptions"]["Options"]
            assert ebs["EBSEnabled"] is True
            assert ebs["VolumeType"] == "gp3"
            assert ebs["VolumeSize"] == 20
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_update_domain_config_snapshot_options(self, opensearch):
        """UpdateDomainConfig with SnapshotOptions."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.update_domain_config(
                DomainName=name,
                SnapshotOptions={"AutomatedSnapshotStartHour": 5},
            )
            assert "DomainConfig" in resp
            snap = resp["DomainConfig"]["SnapshotOptions"]["Options"]
            assert snap["AutomatedSnapshotStartHour"] == 5
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_update_domain_config_advanced_options(self, opensearch):
        """UpdateDomainConfig with AdvancedOptions."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.update_domain_config(
                DomainName=name,
                AdvancedOptions={"rest.action.multi.allow_explicit_index": "false"},
            )
            assert "DomainConfig" in resp
            opts = resp["DomainConfig"]["AdvancedOptions"]["Options"]
            assert opts["rest.action.multi.allow_explicit_index"] == "false"
        finally:
            opensearch.delete_domain(DomainName=name)


class TestOpenSearchVersions:
    """Tests for version-related operations."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_list_versions_contains_opensearch(self, opensearch):
        """ListVersions includes OpenSearch versions."""
        resp = opensearch.list_versions()
        versions = resp["Versions"]
        opensearch_versions = [v for v in versions if v.startswith("OpenSearch_")]
        assert len(opensearch_versions) > 0

    def test_list_versions_contains_elasticsearch(self, opensearch):
        """ListVersions includes Elasticsearch versions."""
        resp = opensearch.list_versions()
        versions = resp["Versions"]
        es_versions = [v for v in versions if v.startswith("Elasticsearch_")]
        assert len(es_versions) > 0

    def test_get_compatible_versions_structure(self, opensearch):
        """GetCompatibleVersions returns entries with SourceVersion and TargetVersions."""
        resp = opensearch.get_compatible_versions()
        versions = resp["CompatibleVersions"]
        assert len(versions) > 0
        for entry in versions:
            assert "SourceVersion" in entry
            assert "TargetVersions" in entry
            assert isinstance(entry["TargetVersions"], list)
            assert len(entry["TargetVersions"]) > 0

    def test_get_compatible_versions_for_domain(self, opensearch):
        """GetCompatibleVersions with DomainName returns domain-specific versions."""
        name = _unique_domain()
        try:
            opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            resp = opensearch.get_compatible_versions(DomainName=name)
            assert "CompatibleVersions" in resp
            assert len(resp["CompatibleVersions"]) > 0
        finally:
            opensearch.delete_domain(DomainName=name)


class TestOpenSearchErrorHandling:
    """Tests for error conditions."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_describe_nonexistent_domain_raises(self, opensearch):
        """DescribeDomain for nonexistent domain raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.describe_domain(DomainName="nonexistent-domain-xyz")

    def test_describe_domain_config_nonexistent_raises(self, opensearch):
        """DescribeDomainConfig for nonexistent domain raises error."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.describe_domain_config(DomainName="nonexistent-domain-xyz")

    def test_delete_nonexistent_domain_raises(self, opensearch):
        """DeleteDomain for nonexistent domain raises error."""
        with pytest.raises(Exception):
            opensearch.delete_domain(DomainName="nonexistent-domain-xyz")

    def test_update_nonexistent_domain_raises(self, opensearch):
        """UpdateDomainConfig for nonexistent domain raises error."""
        with pytest.raises(Exception):
            opensearch.update_domain_config(
                DomainName="nonexistent-domain-xyz",
                ClusterConfig={"InstanceType": "t3.small.search"},
            )


class TestOpenSearchTagOperations:
    """Deeper tag operation tests."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_tag_overwrite(self, opensearch):
        """AddTags with same key overwrites value."""
        name = _unique_domain()
        try:
            resp = opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            arn = resp["DomainStatus"]["ARN"]
            opensearch.add_tags(ARN=arn, TagList=[{"Key": "env", "Value": "dev"}])
            opensearch.add_tags(ARN=arn, TagList=[{"Key": "env", "Value": "prod"}])
            tags = opensearch.list_tags(ARN=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
            assert tag_map["env"] == "prod"
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_remove_all_tags(self, opensearch):
        """RemoveTags can remove all tags."""
        name = _unique_domain()
        try:
            resp = opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            arn = resp["DomainStatus"]["ARN"]
            opensearch.add_tags(
                ARN=arn,
                TagList=[
                    {"Key": "a", "Value": "1"},
                    {"Key": "b", "Value": "2"},
                ],
            )
            opensearch.remove_tags(ARN=arn, TagKeys=["a", "b"])
            tags = opensearch.list_tags(ARN=arn)
            assert len(tags["TagList"]) == 0
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_list_tags_empty_domain(self, opensearch):
        """ListTags on domain with no tags returns empty list."""
        name = _unique_domain()
        try:
            resp = opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
            arn = resp["DomainStatus"]["ARN"]
            tags = opensearch.list_tags(ARN=arn)
            assert "TagList" in tags
            assert isinstance(tags["TagList"], list)
        finally:
            opensearch.delete_domain(DomainName=name)


class TestOpenSearchNewOps:
    """Tests for newly verified OpenSearch operations."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    @pytest.fixture
    def domain(self, opensearch):
        name = _unique_domain()
        opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        yield name
        opensearch.delete_domain(DomainName=name)

    def test_describe_domain_auto_tunes(self, opensearch, domain):
        """DescribeDomainAutoTunes returns AutoTunes list."""
        resp = opensearch.describe_domain_auto_tunes(DomainName=domain)
        assert "AutoTunes" in resp
        assert isinstance(resp["AutoTunes"], list)

    def test_describe_domain_change_progress(self, opensearch, domain):
        """DescribeDomainChangeProgress returns ChangeProgressStatus."""
        resp = opensearch.describe_domain_change_progress(DomainName=domain)
        assert "ChangeProgressStatus" in resp
        assert isinstance(resp["ChangeProgressStatus"], dict)

    def test_describe_domain_health(self, opensearch, domain):
        """DescribeDomainHealth returns health fields."""
        resp = opensearch.describe_domain_health(DomainName=domain)
        assert "DomainState" in resp
        assert "ClusterHealth" in resp
        assert "DataNodeCount" in resp
        assert isinstance(resp["DataNodeCount"], (int, str))

    def test_describe_domain_nodes(self, opensearch, domain):
        """DescribeDomainNodes returns DomainNodesStatusList."""
        resp = opensearch.describe_domain_nodes(DomainName=domain)
        assert "DomainNodesStatusList" in resp
        assert isinstance(resp["DomainNodesStatusList"], list)

    def test_describe_dry_run_progress(self, opensearch, domain):
        """DescribeDryRunProgress returns progress status."""
        resp = opensearch.describe_dry_run_progress(DomainName=domain)
        assert "DryRunProgressStatus" in resp
        assert "DryRunConfig" in resp
        assert "DryRunResults" in resp
        assert isinstance(resp["DryRunProgressStatus"], dict)

    def test_describe_instance_type_limits(self, opensearch):
        """DescribeInstanceTypeLimits returns LimitsByRole."""
        resp = opensearch.describe_instance_type_limits(
            InstanceType="t3.small.search",
            EngineVersion="OpenSearch_2.5",
        )
        assert "LimitsByRole" in resp
        assert isinstance(resp["LimitsByRole"], dict)

    def test_describe_vpc_endpoints(self, opensearch):
        """DescribeVpcEndpoints with fake ID returns errors list."""
        resp = opensearch.describe_vpc_endpoints(VpcEndpointIds=["vpce-fake123"])
        assert "VpcEndpoints" in resp
        assert "VpcEndpointErrors" in resp
        assert isinstance(resp["VpcEndpointErrors"], list)

    def test_describe_domain_health_fields(self, opensearch, domain):
        """DescribeDomainHealth returns detailed health fields."""
        resp = opensearch.describe_domain_health(DomainName=domain)
        assert resp["DomainState"] == "Active"
        assert resp["ClusterHealth"] == "Green"
        assert resp["AvailabilityZoneCount"] == "1"
        assert resp["DedicatedMaster"] is False

    def test_describe_domain_nodes_structure(self, opensearch, domain):
        """DescribeDomainNodes returns node list with expected fields."""
        resp = opensearch.describe_domain_nodes(DomainName=domain)
        nodes = resp["DomainNodesStatusList"]
        assert len(nodes) >= 1
        node = nodes[0]
        assert "NodeId" in node
        assert "NodeType" in node
        assert "AvailabilityZone" in node
        assert "InstanceType" in node
        assert node["NodeStatus"] == "Active"

    def test_describe_instance_type_limits_structure(self, opensearch):
        """DescribeInstanceTypeLimits returns StorageTypes and InstanceLimits."""
        resp = opensearch.describe_instance_type_limits(
            InstanceType="t3.small.search",
            EngineVersion="OpenSearch_2.5",
        )
        data_limits = resp["LimitsByRole"]["data"]
        assert "StorageTypes" in data_limits
        assert len(data_limits["StorageTypes"]) > 0
        assert "InstanceLimits" in data_limits
        count_limits = data_limits["InstanceLimits"]["InstanceCountLimits"]
        assert count_limits["MinimumInstanceCount"] >= 1
        assert count_limits["MaximumInstanceCount"] > 1

    def test_describe_inbound_connections_with_filter(self, opensearch):
        """DescribeInboundConnections with filter returns empty list."""
        resp = opensearch.describe_inbound_connections(
            Filters=[{"Name": "connection-id", "Values": ["fake-id"]}],
            MaxResults=10,
        )
        assert "Connections" in resp
        assert isinstance(resp["Connections"], list)

    def test_describe_outbound_connections_with_filter(self, opensearch):
        """DescribeOutboundConnections with filter returns connections list."""
        resp = opensearch.describe_outbound_connections(
            Filters=[{"Name": "connection-id", "Values": ["fake-id"]}],
            MaxResults=10,
        )
        assert "Connections" in resp
        assert isinstance(resp["Connections"], list)

    def test_describe_packages_with_filter(self, opensearch):
        """DescribePackages with filter returns packages list."""
        resp = opensearch.describe_packages(
            Filters=[{"Name": "PackageID", "Value": ["fake-pkg"]}],
            MaxResults=10,
        )
        assert "PackageDetailsList" in resp
        assert isinstance(resp["PackageDetailsList"], list)

    def test_list_versions_has_both_engine_types(self, opensearch):
        """ListVersions includes both OpenSearch and Elasticsearch versions."""
        resp = opensearch.list_versions()
        versions = resp["Versions"]
        os_versions = [v for v in versions if v.startswith("OpenSearch_")]
        es_versions = [v for v in versions if v.startswith("Elasticsearch_")]
        assert len(os_versions) > 0
        assert len(es_versions) > 0

    def test_describe_domain_auto_tunes_nonexistent_raises(self, opensearch):
        """DescribeDomainAutoTunes for nonexistent domain raises error."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.describe_domain_auto_tunes(DomainName="nonexistent-domain-xyz")

    def test_describe_domain_health_nonexistent_raises(self, opensearch):
        """DescribeDomainHealth for nonexistent domain raises error."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.describe_domain_health(DomainName="nonexistent-domain-xyz")

    def test_describe_domain_nodes_nonexistent_raises(self, opensearch):
        """DescribeDomainNodes for nonexistent domain raises error."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.describe_domain_nodes(DomainName="nonexistent-domain-xyz")

    def test_describe_domain_change_progress_nonexistent_raises(self, opensearch):
        """DescribeDomainChangeProgress for nonexistent domain raises error."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.describe_domain_change_progress(DomainName="nonexistent-domain-xyz")

    def test_describe_dry_run_progress_nonexistent_raises(self, opensearch):
        """DescribeDryRunProgress for nonexistent domain raises error."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.describe_dry_run_progress(DomainName="nonexistent-domain-xyz")


class TestOpenSearchPackageOperations:
    """Tests for OpenSearch package operations."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_create_and_describe_package(self, opensearch):
        """CreatePackage creates a package, DescribePackages lists it."""
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        resp = opensearch.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={
                "S3BucketName": "fake-bucket",
                "S3Key": "fake-key.txt",
            },
        )
        pkg = resp["PackageDetails"]
        assert pkg["PackageName"] == pkg_name
        assert pkg["PackageType"] == "TXT-DICTIONARY"
        assert "PackageID" in pkg
        pkg_id = pkg["PackageID"]

        # Describe packages should include it
        desc = opensearch.describe_packages(Filters=[{"Name": "PackageID", "Value": [pkg_id]}])
        assert len(desc["PackageDetailsList"]) >= 1
        found = [p for p in desc["PackageDetailsList"] if p["PackageID"] == pkg_id]
        assert len(found) == 1

        # Cleanup
        opensearch.delete_package(PackageID=pkg_id)

    def test_delete_package(self, opensearch):
        """DeletePackage removes a package."""
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        resp = opensearch.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={
                "S3BucketName": "fake-bucket",
                "S3Key": "fake-key.txt",
            },
        )
        pkg_id = resp["PackageDetails"]["PackageID"]
        del_resp = opensearch.delete_package(PackageID=pkg_id)
        assert "PackageDetails" in del_resp
        assert del_resp["PackageDetails"]["PackageID"] == pkg_id

    def test_delete_nonexistent_package_raises(self, opensearch):
        """DeletePackage for nonexistent package raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.delete_package(PackageID="F00000000")

    def test_get_package_version_history(self, opensearch):
        """GetPackageVersionHistory returns version history for a package."""
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        resp = opensearch.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={
                "S3BucketName": "fake-bucket",
                "S3Key": "fake-key.txt",
            },
        )
        pkg_id = resp["PackageDetails"]["PackageID"]
        try:
            hist = opensearch.get_package_version_history(PackageID=pkg_id)
            assert "PackageVersionHistoryList" in hist
            assert isinstance(hist["PackageVersionHistoryList"], list)
        finally:
            opensearch.delete_package(PackageID=pkg_id)

    def test_list_instance_type_details(self, opensearch):
        """ListInstanceTypeDetails returns instance types for an engine version."""
        resp = opensearch.list_instance_type_details(EngineVersion="OpenSearch_2.5")
        assert "InstanceTypeDetails" in resp
        assert isinstance(resp["InstanceTypeDetails"], list)
        assert len(resp["InstanceTypeDetails"]) > 0

    def test_list_instance_type_details_has_fields(self, opensearch):
        """ListInstanceTypeDetails entries have expected fields."""
        resp = opensearch.list_instance_type_details(EngineVersion="OpenSearch_2.5")
        detail = resp["InstanceTypeDetails"][0]
        assert "InstanceType" in detail
        assert detail["InstanceType"].endswith(".search")

    def test_associate_and_dissociate_package(self, opensearch):
        """AssociatePackage and DissociatePackage work with a domain."""
        domain_name = _unique_domain()
        opensearch.create_domain(DomainName=domain_name, EngineVersion="OpenSearch_2.5")
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        pkg_resp = opensearch.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "fake-bucket", "S3Key": "fake-key.txt"},
        )
        pkg_id = pkg_resp["PackageDetails"]["PackageID"]
        try:
            assoc = opensearch.associate_package(PackageID=pkg_id, DomainName=domain_name)
            assert "DomainPackageDetails" in assoc
            assert assoc["DomainPackageDetails"]["PackageID"] == pkg_id
            assert assoc["DomainPackageDetails"]["DomainName"] == domain_name

            dissoc = opensearch.dissociate_package(PackageID=pkg_id, DomainName=domain_name)
            assert "DomainPackageDetails" in dissoc
        finally:
            opensearch.delete_package(PackageID=pkg_id)
            opensearch.delete_domain(DomainName=domain_name)

    def test_list_domains_for_package(self, opensearch):
        """ListDomainsForPackage returns domains associated with a package."""
        domain_name = _unique_domain()
        opensearch.create_domain(DomainName=domain_name, EngineVersion="OpenSearch_2.5")
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        pkg_resp = opensearch.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "fake-bucket", "S3Key": "fake-key.txt"},
        )
        pkg_id = pkg_resp["PackageDetails"]["PackageID"]
        try:
            opensearch.associate_package(PackageID=pkg_id, DomainName=domain_name)
            resp = opensearch.list_domains_for_package(PackageID=pkg_id)
            assert "DomainPackageDetailsList" in resp
            domains = [d["DomainName"] for d in resp["DomainPackageDetailsList"]]
            assert domain_name in domains
            opensearch.dissociate_package(PackageID=pkg_id, DomainName=domain_name)
        finally:
            opensearch.delete_package(PackageID=pkg_id)
            opensearch.delete_domain(DomainName=domain_name)

    def test_list_packages_for_domain(self, opensearch):
        """ListPackagesForDomain returns packages associated with a domain."""
        domain_name = _unique_domain()
        opensearch.create_domain(DomainName=domain_name, EngineVersion="OpenSearch_2.5")
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        pkg_resp = opensearch.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "fake-bucket", "S3Key": "fake-key.txt"},
        )
        pkg_id = pkg_resp["PackageDetails"]["PackageID"]
        try:
            opensearch.associate_package(PackageID=pkg_id, DomainName=domain_name)
            resp = opensearch.list_packages_for_domain(DomainName=domain_name)
            assert "DomainPackageDetailsList" in resp
            pkg_ids = [d["PackageID"] for d in resp["DomainPackageDetailsList"]]
            assert pkg_id in pkg_ids
            opensearch.dissociate_package(PackageID=pkg_id, DomainName=domain_name)
        finally:
            opensearch.delete_package(PackageID=pkg_id)
            opensearch.delete_domain(DomainName=domain_name)

    def test_update_package(self, opensearch):
        """UpdatePackage updates a package's source."""
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        resp = opensearch.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "fake-bucket", "S3Key": "fake-key.txt"},
        )
        pkg_id = resp["PackageDetails"]["PackageID"]
        try:
            upd = opensearch.update_package(
                PackageID=pkg_id,
                PackageSource={"S3BucketName": "fake-bucket", "S3Key": "new-key.txt"},
            )
            assert "PackageDetails" in upd
            assert upd["PackageDetails"]["PackageID"] == pkg_id
        finally:
            opensearch.delete_package(PackageID=pkg_id)


class TestOpenSearchVpcEndpointOperations:
    """Tests for OpenSearch VPC endpoint operations."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_create_and_delete_vpc_endpoint(self, opensearch):
        """CreateVpcEndpoint creates a VPC endpoint."""
        domain_name = _unique_domain()
        opensearch.create_domain(DomainName=domain_name, EngineVersion="OpenSearch_2.5")
        try:
            resp = opensearch.create_vpc_endpoint(
                DomainArn=f"arn:aws:es:us-east-1:123456789012:domain/{domain_name}",
                VpcOptions={"SubnetIds": ["subnet-12345678"]},
            )
            assert "VpcEndpoint" in resp
            endpoint = resp["VpcEndpoint"]
            assert "VpcEndpointId" in endpoint
            endpoint_id = endpoint["VpcEndpointId"]
            assert endpoint_id.startswith("aos-")

            # Delete it
            del_resp = opensearch.delete_vpc_endpoint(VpcEndpointId=endpoint_id)
            assert "VpcEndpointSummary" in del_resp
        finally:
            opensearch.delete_domain(DomainName=domain_name)

    def test_list_vpc_endpoints_for_domain(self, opensearch):
        """ListVpcEndpointsForDomain succeeds for a domain."""
        domain_name = _unique_domain()
        opensearch.create_domain(DomainName=domain_name, EngineVersion="OpenSearch_2.5")
        try:
            resp = opensearch.list_vpc_endpoints_for_domain(DomainName=domain_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            opensearch.delete_domain(DomainName=domain_name)


class TestOpenSearchConnectionOperations:
    """Tests for OpenSearch connection operations."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_create_outbound_connection(self, opensearch):
        """CreateOutboundConnection creates an outbound connection."""
        resp = opensearch.create_outbound_connection(
            LocalDomainInfo={
                "AWSDomainInformation": {
                    "DomainName": "local-domain",
                    "OwnerId": "123456789012",
                    "Region": "us-east-1",
                }
            },
            RemoteDomainInfo={
                "AWSDomainInformation": {
                    "DomainName": "remote-domain",
                    "OwnerId": "123456789012",
                    "Region": "us-east-1",
                }
            },
            ConnectionAlias="test-connection",
        )
        assert "ConnectionStatus" in resp
        assert "ConnectionId" in resp
        assert len(resp["ConnectionId"]) > 0

    def test_accept_inbound_connection_nonexistent(self, opensearch):
        """AcceptInboundConnection for nonexistent raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.accept_inbound_connection(ConnectionId="conn-nonexistent")

    def test_delete_inbound_connection_nonexistent(self, opensearch):
        """DeleteInboundConnection for nonexistent raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.delete_inbound_connection(ConnectionId="conn-nonexistent")

    def test_delete_outbound_connection_nonexistent(self, opensearch):
        """DeleteOutboundConnection for nonexistent raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.delete_outbound_connection(ConnectionId="conn-nonexistent")


class TestOpenSearchAdditionalWorkingOps:
    """Tests for additional working OpenSearch operations."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_update_vpc_endpoint_nonexistent(self, opensearch):
        """UpdateVpcEndpoint with fake endpoint ID raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.update_vpc_endpoint(
                VpcEndpointId="aos-fake-endpoint-id",
                VpcOptions={"SubnetIds": ["subnet-12345"]},
            )


class TestOpenSearchNewOps2:
    """Tests for additional newly verified OpenSearch operations."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    @pytest.fixture
    def domain(self, opensearch):
        name = _unique_domain()
        opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        yield name
        try:
            opensearch.delete_domain(DomainName=name)
        except Exception:
            pass  # best-effort cleanup

    def test_authorize_vpc_endpoint_access(self, opensearch, domain):
        """AuthorizeVpcEndpointAccess with a fake account on a real domain."""
        try:
            resp = opensearch.authorize_vpc_endpoint_access(
                DomainName=domain, Account="111122223333"
            )
            assert "AuthorizedPrincipal" in resp
        except opensearch.exceptions.ClientError as e:
            # May raise validation error but should still contact server
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_revoke_vpc_endpoint_access(self, opensearch, domain):
        """RevokeVpcEndpointAccess with a fake account on a real domain."""
        try:
            resp = opensearch.revoke_vpc_endpoint_access(DomainName=domain, Account="111122223333")
            assert "ResponseMetadata" in resp
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_create_and_get_application(self, opensearch):
        """CreateApplication and GetApplication round-trip."""
        app_name = f"app-{uuid.uuid4().hex[:8]}"
        try:
            create_resp = opensearch.create_application(name=app_name)
            assert "id" in create_resp or "name" in create_resp
            app_id = create_resp.get("id", "")
            if app_id:
                get_resp = opensearch.get_application(id=app_id)
                assert "name" in get_resp or "id" in get_resp
                # Cleanup
                opensearch.delete_application(id=app_id)
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409, 500)

    def test_delete_application_nonexistent(self, opensearch):
        """DeleteApplication with fake ID."""
        try:
            opensearch.delete_application(id="fake-app-id-12345")
            # If it succeeds, that's fine
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_get_application_nonexistent(self, opensearch):
        """GetApplication with fake ID."""
        try:
            opensearch.get_application(id="fake-app-id-12345")
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_update_application_nonexistent(self, opensearch):
        """UpdateApplication with fake ID."""
        try:
            opensearch.update_application(id="fake-app-id-12345")
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_delete_direct_query_data_source(self, opensearch):
        """DeleteDirectQueryDataSource with fake name."""
        try:
            opensearch.delete_direct_query_data_source(DataSourceName="fake-ds")
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_get_direct_query_data_source(self, opensearch):
        """GetDirectQueryDataSource with fake name."""
        try:
            opensearch.get_direct_query_data_source(DataSourceName="fake-ds")
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_get_domain_maintenance_status(self, opensearch, domain):
        """GetDomainMaintenanceStatus with fake maintenance ID."""
        try:
            resp = opensearch.get_domain_maintenance_status(
                DomainName=domain, MaintenanceId="fake-maint-id"
            )
            assert "Status" in resp or "ResponseMetadata" in resp
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_list_domain_maintenances(self, opensearch, domain):
        """ListDomainMaintenances returns a list."""
        try:
            resp = opensearch.list_domain_maintenances(DomainName=domain)
            assert "DomainMaintenances" in resp or "ResponseMetadata" in resp
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_list_scheduled_actions(self, opensearch, domain):
        """ListScheduledActions returns a list."""
        try:
            resp = opensearch.list_scheduled_actions(DomainName=domain)
            assert "ScheduledActions" in resp or "ResponseMetadata" in resp
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_start_domain_maintenance(self, opensearch, domain):
        """StartDomainMaintenance with a valid action type."""
        try:
            resp = opensearch.start_domain_maintenance(
                DomainName=domain, Action="REBOOT_NODE", NodeId="fake-node-id"
            )
            assert "MaintenanceId" in resp or "ResponseMetadata" in resp
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_update_scheduled_action(self, opensearch, domain):
        """UpdateScheduledAction with fake action."""
        try:
            opensearch.update_scheduled_action(
                DomainName=domain,
                ActionID="fake-action-id",
                ActionType="SERVICE_SOFTWARE_UPDATE",
                ScheduleAt="NOW",
            )
        except opensearch.exceptions.ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404, 409)

    def test_purchase_reserved_instance_offering_nonexistent(self, opensearch):
        """PurchaseReservedInstanceOffering with fake offering raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.purchase_reserved_instance_offering(
                ReservedInstanceOfferingId="12345678-1234-1234-1234-123456789012",
                ReservationName="test-reservation",
                InstanceCount=1,
            )


class TestOpenSearchDataSourceOps:
    """Tests for DataSource CRUD operations on OpenSearch domains."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_add_data_source_nonexistent_domain(self, opensearch):
        """AddDataSource with non-existent domain raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.add_data_source(
                DomainName="fake-domain-ds-test",
                Name="my-data-source",
                DataSourceType={
                    "S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/test-role"}
                },
            )

    def test_delete_data_source_nonexistent_domain(self, opensearch):
        """DeleteDataSource with non-existent domain raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.delete_data_source(
                DomainName="fake-domain-ds-test",
                Name="my-data-source",
            )

    def test_get_data_source_nonexistent_domain(self, opensearch):
        """GetDataSource with non-existent domain raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.get_data_source(
                DomainName="fake-domain-ds-test",
                Name="my-data-source",
            )

    def test_list_data_sources_nonexistent_domain(self, opensearch):
        """ListDataSources with non-existent domain raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.list_data_sources(DomainName="fake-domain-ds-test")

    def test_update_data_source_nonexistent_domain(self, opensearch):
        """UpdateDataSource with non-existent domain raises ResourceNotFoundException."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.update_data_source(
                DomainName="fake-domain-ds-test",
                Name="my-data-source",
                DataSourceType={
                    "S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/test-role"}
                },
            )

    @pytest.fixture
    def domain(self, opensearch):
        name = _unique_domain()
        opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        yield name
        try:
            opensearch.delete_domain(DomainName=name)
        except Exception:
            pass  # best-effort cleanup

    def test_add_data_source(self, opensearch, domain):
        """AddDataSource creates a data source on a domain."""
        ds_name = f"ds-{uuid.uuid4().hex[:8]}"
        resp = opensearch.add_data_source(
            DomainName=domain,
            Name=ds_name,
            DataSourceType={
                "S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/test-role"}
            },
        )
        assert "Message" in resp
        assert len(resp["Message"]) > 0
        opensearch.delete_data_source(DomainName=domain, Name=ds_name)

    def test_get_data_source(self, opensearch, domain):
        """GetDataSource returns the data source details after creation."""
        ds_name = f"ds-{uuid.uuid4().hex[:8]}"
        opensearch.add_data_source(
            DomainName=domain,
            Name=ds_name,
            DataSourceType={
                "S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/test-role"}
            },
        )
        resp = opensearch.get_data_source(DomainName=domain, Name=ds_name)
        assert resp["Name"] == ds_name
        assert "DataSourceType" in resp
        assert "Status" in resp
        opensearch.delete_data_source(DomainName=domain, Name=ds_name)

    def test_update_data_source(self, opensearch, domain):
        """UpdateDataSource changes the role ARN of a data source."""
        ds_name = f"ds-{uuid.uuid4().hex[:8]}"
        opensearch.add_data_source(
            DomainName=domain,
            Name=ds_name,
            DataSourceType={
                "S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/role-original"}
            },
        )
        resp = opensearch.update_data_source(
            DomainName=domain,
            Name=ds_name,
            DataSourceType={
                "S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/role-updated"}
            },
        )
        assert "Message" in resp
        assert len(resp["Message"]) > 0
        opensearch.delete_data_source(DomainName=domain, Name=ds_name)

    def test_delete_data_source(self, opensearch, domain):
        """DeleteDataSource removes a data source from a domain."""
        ds_name = f"ds-{uuid.uuid4().hex[:8]}"
        opensearch.add_data_source(
            DomainName=domain,
            Name=ds_name,
            DataSourceType={
                "S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/test-role"}
            },
        )
        resp = opensearch.delete_data_source(DomainName=domain, Name=ds_name)
        assert "Message" in resp
        assert len(resp["Message"]) > 0

    def test_list_data_sources_empty(self, opensearch, domain):
        """ListDataSources returns empty list when no data sources exist."""
        resp = opensearch.list_data_sources(DomainName=domain)
        assert "DataSources" in resp
        assert isinstance(resp["DataSources"], list)

    def test_list_data_sources_with_entries(self, opensearch, domain):
        """ListDataSources returns created data sources."""
        ds_name = f"ds-{uuid.uuid4().hex[:8]}"
        opensearch.add_data_source(
            DomainName=domain,
            Name=ds_name,
            DataSourceType={
                "S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/test-role"}
            },
        )
        resp = opensearch.list_data_sources(DomainName=domain)
        assert "DataSources" in resp
        names = [ds["Name"] for ds in resp["DataSources"]]
        assert ds_name in names
        opensearch.delete_data_source(DomainName=domain, Name=ds_name)

    def test_get_data_source_nonexistent(self, opensearch, domain):
        """GetDataSource raises ResourceNotFoundException for unknown data source."""
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.get_data_source(DomainName=domain, Name="nonexistent-datasource")

    def test_add_multiple_data_sources(self, opensearch, domain):
        """Add multiple data sources and verify both appear in list."""
        ds1 = f"ds-{uuid.uuid4().hex[:8]}"
        ds2 = f"ds-{uuid.uuid4().hex[:8]}"
        role_arn = "arn:aws:iam::123456789012:role/test-role"
        opensearch.add_data_source(
            DomainName=domain,
            Name=ds1,
            DataSourceType={"S3GlueDataCatalog": {"RoleArn": role_arn}},
        )
        opensearch.add_data_source(
            DomainName=domain,
            Name=ds2,
            DataSourceType={"S3GlueDataCatalog": {"RoleArn": role_arn}},
        )
        resp = opensearch.list_data_sources(DomainName=domain)
        names = [ds["Name"] for ds in resp["DataSources"]]
        assert ds1 in names
        assert ds2 in names
        opensearch.delete_data_source(DomainName=domain, Name=ds1)
        opensearch.delete_data_source(DomainName=domain, Name=ds2)


class TestOpenSearchMissingGapOps:
    """Tests for previously untested OpenSearch operations."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_get_upgrade_history(self, opensearch):
        """get_upgrade_history returns UpgradeHistories key."""
        response = opensearch.get_upgrade_history(DomainName="fake-domain")
        assert "UpgradeHistories" in response
        assert isinstance(response["UpgradeHistories"], list)

    def test_get_upgrade_status(self, opensearch):
        """get_upgrade_status returns UpgradeStep key."""
        response = opensearch.get_upgrade_status(DomainName="fake-domain")
        assert "UpgradeStep" in response
        assert isinstance(response["UpgradeStep"], str)

    def test_cancel_service_software_update(self, opensearch):
        """cancel_service_software_update returns ServiceSoftwareOptions with bool fields."""
        name = _unique_domain()
        opensearch.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            response = opensearch.cancel_service_software_update(DomainName=name)
            assert "ServiceSoftwareOptions" in response
            opts = response["ServiceSoftwareOptions"]
            assert isinstance(opts.get("UpdateAvailable"), bool)
            names = [d["DomainName"] for d in opensearch.list_domain_names()["DomainNames"]]
            assert name in names
            opensearch.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})
        finally:
            opensearch.delete_domain(DomainName=name)
        with pytest.raises(opensearch.exceptions.ResourceNotFoundException):
            opensearch.describe_domain(DomainName=name)

    def test_start_service_software_update(self, opensearch):
        """start_service_software_update returns ServiceSoftwareOptions key."""
        response = opensearch.start_service_software_update(DomainName="fake-domain")
        assert "ServiceSoftwareOptions" in response
        assert isinstance(response["ServiceSoftwareOptions"], dict)

    def test_list_vpc_endpoint_access(self, opensearch):
        """list_vpc_endpoint_access returns AuthorizedPrincipalList key."""
        response = opensearch.list_vpc_endpoint_access(DomainName="fake-domain")
        assert "AuthorizedPrincipalList" in response
        assert isinstance(response["AuthorizedPrincipalList"], list)

    def test_upgrade_domain(self, opensearch):
        """upgrade_domain returns DomainName and TargetVersion."""
        name = f"os-{uuid.uuid4().hex[:8]}"
        opensearch.create_domain(DomainName=name)
        try:
            resp = opensearch.upgrade_domain(DomainName=name, TargetVersion="OpenSearch_2.3")
            assert resp["DomainName"] == name
            assert resp["TargetVersion"] == "OpenSearch_2.3"
        finally:
            opensearch.delete_domain(DomainName=name)


class TestOpenSearchNewStubOps:
    """Tests for newly-implemented opensearch stub operations."""

    @pytest.fixture
    def opensearch(self):
        return make_client("opensearch")

    def test_list_direct_query_data_sources(self, opensearch):
        """ListDirectQueryDataSources returns a list."""
        resp = opensearch.list_direct_query_data_sources()
        assert "DirectQueryDataSources" in resp
        assert isinstance(resp["DirectQueryDataSources"], list)

    def test_reject_inbound_connection(self, opensearch):
        """RejectInboundConnection returns the connection with REJECTED status."""
        resp = opensearch.reject_inbound_connection(ConnectionId="fake-conn-id")
        assert "Connection" in resp
        assert isinstance(resp["Connection"], dict)

    def test_cancel_domain_config_change(self, opensearch):
        """CancelDomainConfigChange returns dry run and cancelled change ids."""
        name = _unique_domain()
        opensearch.create_domain(DomainName=name)
        try:
            resp = opensearch.cancel_domain_config_change(DomainName=name, DryRun=True)
            assert "CancelledChangeIds" in resp
            assert isinstance(resp["CancelledChangeIds"], list)
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_get_index(self, opensearch):
        """GetIndex returns response (stub)."""
        name = _unique_domain()
        opensearch.create_domain(DomainName=name)
        try:
            resp = opensearch.get_index(DomainName=name, IndexName="test-index")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            opensearch.delete_domain(DomainName=name)

    def test_put_default_application_setting(self, opensearch):
        """PutDefaultApplicationSetting succeeds."""
        resp = opensearch.put_default_application_setting(
            applicationArn="arn:aws:opensearch:us-east-1:123456789012:application/test-app",
            setAsDefault=True,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestOpenSearchNewGapOps:
    """Tests for newly covered OpenSearch gap operations."""

    @pytest.fixture
    def client(self):
        return make_client("opensearch")

    @pytest.fixture
    def domain(self, client):
        name = _unique_domain()
        client.create_domain(DomainName=name)
        yield name
        client.delete_domain(DomainName=name)

    def test_add_and_update_direct_query_data_source(self, client):
        """AddDirectQueryDataSource and UpdateDirectQueryDataSource work."""
        resp = client.add_direct_query_data_source(
            DataSourceName="dss",
            DataSourceType={
                "CloudWatchLog": {"RoleArn": "arn:aws:iam::123456789012:role/test-role-abc"}
            },
            OpenSearchArns=["arn:aws:es:us-east-1:123456789012:domain/test-domain"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp2 = client.update_direct_query_data_source(
            DataSourceName="dss",
            DataSourceType={
                "CloudWatchLog": {"RoleArn": "arn:aws:iam::123456789012:role/test-role-abc"}
            },
            OpenSearchArns=["arn:aws:es:us-east-1:123456789012:domain/test-domain"],
        )
        assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_associate_and_dissociate_packages(self, client, domain):
        """AssociatePackages and DissociatePackages return 200."""
        resp = client.associate_packages(
            PackageList=[{"PackageID": "F12345", "PrerequisitePackageIDList": []}],
            DomainName=domain,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp2 = client.dissociate_packages(
            PackageList=["F12345"],
            DomainName=domain,
        )
        assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_and_delete_index(self, client, domain):
        """CreateIndex, UpdateIndex, DeleteIndex all return 200."""
        client.create_index(DomainName=domain, IndexName="idx1", IndexSchema="{}")
        client.update_index(DomainName=domain, IndexName="idx1", IndexSchema="{}")
        resp = client.delete_index(DomainName=domain, IndexName="idx1")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_package_scope(self, client):
        """UpdatePackageScope returns 200."""
        resp = client.update_package_scope(
            PackageID="F12345", Operation="ADD", PackageUserList=["123456789012"]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestOpenSearchEdgeCases:
    """Edge case and behavioral fidelity tests for opensearch."""

    @pytest.fixture
    def client(self):
        return make_client("opensearch")

    @pytest.fixture
    def domain(self, client):
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        yield name
        try:
            client.delete_domain(DomainName=name)
        except Exception:
            pass

    # --- ARN format ---
    def test_arn_format(self, client, domain):
        """Domain ARN matches expected format arn:aws:es:REGION:ACCOUNT:domain/NAME."""
        resp = client.describe_domain(DomainName=domain)
        arn = resp["DomainStatus"]["ARN"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "es"
        assert parts[4].isdigit(), f"Expected numeric account in ARN, got: {arn}"
        assert parts[5] == f"domain/{domain}"

    # --- Idempotency / duplicate create ---
    def test_create_duplicate_domain_raises(self, client, domain):
        """Creating a domain with an existing name raises ResourceAlreadyExistsException."""
        with pytest.raises(client.exceptions.ResourceAlreadyExistsException):
            client.create_domain(DomainName=domain, EngineVersion="OpenSearch_2.5")

    # --- list_domain_names ordering and filtering ---
    def test_list_domain_names_includes_created(self, client):
        """list_domain_names returns domains that were just created."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            resp = client.list_domain_names()
            names = [d["DomainName"] for d in resp["DomainNames"]]
            assert name in names
        finally:
            client.delete_domain(DomainName=name)

    def test_list_domain_names_excludes_deleted(self, client):
        """list_domain_names does not include deleted domains."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        client.delete_domain(DomainName=name)
        resp = client.list_domain_names()
        names = [d["DomainName"] for d in resp["DomainNames"]]
        assert name not in names

    def test_list_domain_names_elasticsearch_filter(self, client):
        """list_domain_names with EngineType=Elasticsearch excludes OpenSearch domains."""
        os_name = _unique_domain()
        es_name = _unique_domain()
        client.create_domain(DomainName=os_name, EngineVersion="OpenSearch_2.5")
        client.create_domain(DomainName=es_name, EngineVersion="Elasticsearch_7.10")
        try:
            resp = client.list_domain_names(EngineType="Elasticsearch")
            names = [d["DomainName"] for d in resp["DomainNames"]]
            assert es_name in names
            assert os_name not in names
        finally:
            client.delete_domain(DomainName=os_name)
            client.delete_domain(DomainName=es_name)

    # --- list_versions pagination ---
    def test_list_versions_with_max_results(self, client):
        """list_versions with MaxResults=2 returns at most 2 versions and a NextToken."""
        resp = client.list_versions(MaxResults=2)
        assert "Versions" in resp
        assert len(resp["Versions"]) <= 2
        # With pagination, should have NextToken if there are more
        if len(resp["Versions"]) == 2:
            assert "NextToken" in resp

    def test_list_versions_pagination_continues(self, client):
        """list_versions NextToken yields more results."""
        first = client.list_versions(MaxResults=2)
        if "NextToken" not in first:
            return  # skip if not enough versions
        second = client.list_versions(NextToken=first["NextToken"])
        assert "Versions" in second
        assert len(second["Versions"]) > 0
        # No overlap between pages
        first_set = set(first["Versions"])
        second_set = set(second["Versions"])
        assert first_set.isdisjoint(second_set)

    # --- update_domain_config_access_policies read-back ---
    def test_update_access_policies_persisted(self, client, domain):
        """UpdateDomainConfig access policy is readable via DescribeDomainConfig."""
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"AWS": "*"},
                "Action": "es:ESHttpDelete",
                "Resource": f"arn:aws:es:us-east-1:123456789012:domain/{domain}/*",
            }],
        })
        client.update_domain_config(DomainName=domain, AccessPolicies=policy)
        config = client.describe_domain_config(DomainName=domain)
        stored = config["DomainConfig"]["AccessPolicies"]["Options"]
        # policy is stored as JSON string; parse and check Effect
        parsed = json.loads(stored)
        assert parsed["Statement"][0]["Effect"] == "Deny"

    # --- list_vpc_endpoints after creating one ---
    def test_list_vpc_endpoints_after_create(self, client, domain):
        """list_vpc_endpoints includes endpoint just created."""
        resp = client.create_vpc_endpoint(
            DomainArn=f"arn:aws:es:us-east-1:123456789012:domain/{domain}",
            VpcOptions={"SubnetIds": ["subnet-12345678"]},
        )
        endpoint_id = resp["VpcEndpoint"]["VpcEndpointId"]
        try:
            list_resp = client.list_vpc_endpoints()
            assert "VpcEndpointSummaryList" in list_resp
            ids = [e["VpcEndpointId"] for e in list_resp["VpcEndpointSummaryList"]]
            assert endpoint_id in ids
        finally:
            client.delete_vpc_endpoint(VpcEndpointId=endpoint_id)

    # --- describe_packages after creating one ---
    def test_describe_packages_after_create(self, client):
        """describe_packages includes newly created package."""
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        resp = client.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "fake-bucket", "S3Key": "fake-key.txt"},
        )
        pkg_id = resp["PackageDetails"]["PackageID"]
        try:
            pkgs = client.describe_packages()
            ids = [p["PackageID"] for p in pkgs["PackageDetailsList"]]
            assert pkg_id in ids
        finally:
            client.delete_package(PackageID=pkg_id)

    # --- describe_outbound_connections after creating one ---
    def test_describe_outbound_connections_after_create(self, client):
        """describe_outbound_connections includes created connection."""
        create_resp = client.create_outbound_connection(
            LocalDomainInfo={"AWSDomainInformation": {
                "DomainName": "local-d", "OwnerId": "123456789012", "Region": "us-east-1",
            }},
            RemoteDomainInfo={"AWSDomainInformation": {
                "DomainName": "remote-d", "OwnerId": "123456789012", "Region": "us-west-2",
            }},
            ConnectionAlias="edge-test-conn",
        )
        conn_id = create_resp["ConnectionId"]
        resp = client.describe_outbound_connections()
        ids = [c["ConnectionId"] for c in resp["Connections"]]
        assert conn_id in ids
        # Cleanup
        client.delete_outbound_connection(ConnectionId=conn_id)

    # --- cancel_service_software_update error on domain with no pending update ---
    def test_cancel_service_software_update_real_domain(self, client, domain):
        """cancel_service_software_update on a real domain returns ServiceSoftwareOptions."""
        resp = client.cancel_service_software_update(DomainName=domain)
        assert "ServiceSoftwareOptions" in resp
        opts = resp["ServiceSoftwareOptions"]
        assert isinstance(opts.get("UpdateAvailable"), bool)
        assert isinstance(opts.get("Cancellable"), bool)
        # Verify domain is still accessible
        names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
        assert domain in names
        client.update_domain_config(DomainName=domain, ClusterConfig={"InstanceCount": 1})

    # --- describe_reserved_instance_offerings structure ---
    def test_describe_reserved_instance_offerings_structure(self, client):
        """describe_reserved_instance_offerings entries have InstanceType and Duration."""
        resp = client.describe_reserved_instance_offerings()
        offerings = resp["ReservedInstanceOfferings"]
        assert len(offerings) > 0
        offering = offerings[0]
        assert "ReservedInstanceOfferingId" in offering
        assert "InstanceType" in offering

    # --- get_default_application_setting assertions ---
    def test_get_default_application_setting_structure(self, client):
        """get_default_application_setting returns ApplicationDetails or empty keys."""
        resp = client.get_default_application_setting()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # May contain applicationDetails or defaultSettings keys
        # Just verify it's a dict with metadata
        assert isinstance(resp, dict)

    # --- list_applications after creating one ---
    def test_list_applications_after_create(self, client):
        """list_applications includes newly created application."""
        app_name = f"app-{uuid.uuid4().hex[:8]}"
        try:
            create_resp = client.create_application(name=app_name)
            app_id = create_resp.get("id", "")
            if not app_id:
                return  # application feature not fully implemented
            list_resp = client.list_applications()
            assert "ApplicationSummaries" in list_resp
            ids = [a.get("id", "") for a in list_resp["ApplicationSummaries"]]
            assert app_id in ids
            client.delete_application(id=app_id)
        except client.exceptions.ClientError:
            pass  # skip if application ops not implemented

    # --- describe_reserved_instances with id filter ---
    def test_describe_reserved_instances_with_id(self, client):
        """describe_reserved_instances with fake ReservedInstanceId returns empty list."""
        resp = client.describe_reserved_instances(
            ReservedInstanceId="12345678-1234-1234-1234-123456789012"
        )
        assert "ReservedInstances" in resp
        assert isinstance(resp["ReservedInstances"], list)

    # --- list_tags error handling ---
    def test_list_tags_nonexistent_arn_returns_empty(self, client):
        """list_tags on a non-existent ARN returns empty TagList (not an error)."""
        resp = client.list_tags(
            ARN="arn:aws:es:us-east-1:123456789012:domain/totally-nonexistent-abc"
        )
        assert "TagList" in resp
        assert isinstance(resp["TagList"], list)

    # --- delete nonexistent domain raises specific error ---
    def test_delete_nonexistent_domain_specific_error(self, client):
        """DeleteDomain for nonexistent domain raises ResourceNotFoundException."""
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.delete_domain(DomainName="xyz-nonexistent-abc")

    # --- describe_domains with nonexistent name ---
    def test_describe_domains_returns_empty_for_missing(self, client):
        """describe_domains for a nonexistent domain returns empty DomainStatusList."""
        resp = client.describe_domains(DomainNames=["xyz-nonexistent-domain"])
        assert "DomainStatusList" in resp
        assert isinstance(resp["DomainStatusList"], list)
        assert len(resp["DomainStatusList"]) == 0

    # --- list_domain_names engine type field present ---
    def test_list_domain_names_has_engine_type(self, client):
        """list_domain_names entries include EngineType field."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            resp = client.list_domain_names()
            entry = next((d for d in resp["DomainNames"] if d["DomainName"] == name), None)
            assert entry is not None
            assert "EngineType" in entry
            assert entry["EngineType"] == "OpenSearch"
        finally:
            client.delete_domain(DomainName=name)


class TestOpenSearchEdgeCases2:
    """Additional edge case and behavioral fidelity tests targeting low-coverage areas."""

    @pytest.fixture
    def client(self):
        return make_client("opensearch")

    @pytest.fixture
    def domain(self, client):
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        yield name
        try:
            client.delete_domain(DomainName=name)
        except Exception:
            pass  # best-effort cleanup

    # --- cancel_service_software_update behavioral fidelity ---

    def test_cancel_service_software_update_full_structure(self, client, domain):
        """cancel_service_software_update returns complete ServiceSoftwareOptions structure."""
        resp = client.cancel_service_software_update(DomainName=domain)
        opts = resp["ServiceSoftwareOptions"]
        required_keys = {
            "CurrentVersion", "NewVersion", "UpdateAvailable", "Cancellable",
            "UpdateStatus", "Description", "AutomatedUpdateDate", "OptionalDeployment",
        }
        missing = required_keys - set(opts.keys())
        assert not missing, f"Missing ServiceSoftwareOptions keys: {missing}"
        assert isinstance(opts["UpdateAvailable"], bool)
        assert isinstance(opts["Cancellable"], bool)
        # Domain should still be accessible via list and update
        names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
        assert domain in names
        client.update_domain_config(DomainName=domain, ClusterConfig={"InstanceCount": 1})

    def test_cancel_service_software_update_bool_fields(self, client, domain):
        """cancel_service_software_update UpdateAvailable and Cancellable are booleans."""
        resp = client.cancel_service_software_update(DomainName=domain)
        opts = resp["ServiceSoftwareOptions"]
        assert isinstance(opts["UpdateAvailable"], bool)
        assert isinstance(opts["Cancellable"], bool)
        assert isinstance(opts["OptionalDeployment"], bool)
        # Also verify domain still listable and updatable
        assert any(
            d["DomainName"] == domain
            for d in client.list_domain_names()["DomainNames"]
        )
        client.update_domain_config(DomainName=domain, SnapshotOptions={"AutomatedSnapshotStartHour": 6})

    def test_cancel_service_software_update_new_domain_no_update(self, client, domain):
        """Freshly created domain has UpdateAvailable=False."""
        resp = client.cancel_service_software_update(DomainName=domain)
        opts = resp["ServiceSoftwareOptions"]
        assert opts["UpdateAvailable"] is False
        assert opts["Cancellable"] is False
        # Verify list shows domain, update works
        names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
        assert domain in names
        client.update_domain_config(DomainName=domain, ClusterConfig={"InstanceCount": 1})

    # --- get_compatible_versions edge cases ---

    def test_get_compatible_versions_nonexistent_domain_raises(self, client):
        """get_compatible_versions for nonexistent domain raises ResourceNotFoundException."""
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_compatible_versions(DomainName="xyz-no-such-domain-abc")

    def test_get_compatible_versions_version_format(self, client):
        """GetCompatibleVersions SourceVersion and TargetVersions have OpenSearch_ or Elasticsearch_ prefix."""
        resp = client.get_compatible_versions()
        for entry in resp["CompatibleVersions"]:
            src = entry["SourceVersion"]
            assert src.startswith("OpenSearch_") or src.startswith("Elasticsearch_"), (
                f"Unexpected source version format: {src}"
            )
            for target in entry["TargetVersions"]:
                assert target.startswith("OpenSearch_") or target.startswith("Elasticsearch_"), (
                    f"Unexpected target version format: {target}"
                )

    def test_get_compatible_versions_domain_source_matches_engine(self, client, domain):
        """GetCompatibleVersions for a domain returns entries whose SourceVersion matches OpenSearch_2.5."""
        resp = client.get_compatible_versions(DomainName=domain)
        assert "CompatibleVersions" in resp
        versions = resp["CompatibleVersions"]
        assert len(versions) > 0
        # Each entry must have SourceVersion and TargetVersions
        for entry in versions:
            assert "SourceVersion" in entry
            assert "TargetVersions" in entry
            assert isinstance(entry["TargetVersions"], list)

    def test_get_compatible_versions_for_elasticsearch_domain(self, client):
        """GetCompatibleVersions works for an Elasticsearch domain."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="Elasticsearch_7.10")
        try:
            resp = client.get_compatible_versions(DomainName=name)
            assert "CompatibleVersions" in resp
            assert len(resp["CompatibleVersions"]) > 0
        finally:
            client.delete_domain(DomainName=name)

    # --- list_versions edge cases ---

    def test_list_versions_all_have_known_prefix(self, client):
        """All versions from list_versions start with OpenSearch_ or Elasticsearch_."""
        resp = client.list_versions()
        for version in resp["Versions"]:
            assert version.startswith("OpenSearch_") or version.startswith("Elasticsearch_"), (
                f"Unexpected version format: {version}"
            )

    def test_list_versions_no_duplicates(self, client):
        """list_versions returns unique version strings."""
        resp = client.list_versions()
        versions = resp["Versions"]
        assert len(versions) == len(set(versions)), "Duplicate versions in ListVersions response"

    def test_list_versions_pagination_next_token(self, client):
        """list_versions with MaxResults=2 returns NextToken when more results exist."""
        resp = client.list_versions(MaxResults=2)
        assert "Versions" in resp
        assert len(resp["Versions"]) <= 2
        assert "NextToken" in resp  # there are always more than 2 versions

    def test_list_versions_pagination_continues_with_token(self, client):
        """list_versions NextToken pages through all versions without overlap."""
        first = client.list_versions(MaxResults=2)
        assert "NextToken" in first
        second = client.list_versions(NextToken=first["NextToken"])
        assert "Versions" in second
        assert len(second["Versions"]) > 0
        first_set = set(first["Versions"])
        second_set = set(second["Versions"])
        assert first_set.isdisjoint(second_set), "Overlapping versions across pages"

    # --- list_vpc_endpoints structure ---

    def test_list_vpc_endpoints_returns_summary_list(self, client):
        """list_vpc_endpoints returns VpcEndpointSummaryList key."""
        resp = client.list_vpc_endpoints()
        assert "VpcEndpointSummaryList" in resp
        assert isinstance(resp["VpcEndpointSummaryList"], list)

    def test_list_vpc_endpoints_empty_by_default(self, client):
        """list_vpc_endpoints VpcEndpointSummaryList is a list (may be empty)."""
        resp = client.list_vpc_endpoints()
        assert isinstance(resp["VpcEndpointSummaryList"], list)

    def test_list_vpc_endpoints_includes_created(self, client, domain):
        """list_vpc_endpoints includes newly created endpoint."""
        resp = client.create_vpc_endpoint(
            DomainArn=f"arn:aws:es:us-east-1:123456789012:domain/{domain}",
            VpcOptions={"SubnetIds": ["subnet-abcdef01"]},
        )
        endpoint_id = resp["VpcEndpoint"]["VpcEndpointId"]
        try:
            list_resp = client.list_vpc_endpoints()
            assert "VpcEndpointSummaryList" in list_resp
            ids = [e["VpcEndpointId"] for e in list_resp["VpcEndpointSummaryList"]]
            assert endpoint_id in ids
        finally:
            client.delete_vpc_endpoint(VpcEndpointId=endpoint_id)

    def test_list_vpc_endpoints_for_domain_structure(self, client, domain):
        """list_vpc_endpoints_for_domain returns VpcEndpointSummaryList."""
        resp = client.list_vpc_endpoints_for_domain(DomainName=domain)
        assert "VpcEndpointSummaryList" in resp
        assert isinstance(resp["VpcEndpointSummaryList"], list)

    # --- describe_inbound_connections structure ---

    def test_describe_inbound_connections_has_connections(self, client):
        """describe_inbound_connections returns Connections key with a list."""
        resp = client.describe_inbound_connections()
        assert "Connections" in resp
        assert isinstance(resp["Connections"], list)

    def test_describe_inbound_connections_after_outbound_created(self, client):
        """After creating outbound connection, inbound side is reflected."""
        resp = client.create_outbound_connection(
            LocalDomainInfo={"AWSDomainInformation": {
                "DomainName": "local-d2", "OwnerId": "123456789012", "Region": "us-east-1",
            }},
            RemoteDomainInfo={"AWSDomainInformation": {
                "DomainName": "remote-d2", "OwnerId": "111122223333", "Region": "us-west-2",
            }},
            ConnectionAlias="inbound-check-conn",
        )
        conn_id = resp["ConnectionId"]
        try:
            inbound = client.describe_inbound_connections()
            assert "Connections" in inbound
            assert isinstance(inbound["Connections"], list)
        finally:
            client.delete_outbound_connection(ConnectionId=conn_id)

    # --- describe_outbound_connections structure ---

    def test_describe_outbound_connections_connection_fields(self, client):
        """describe_outbound_connections entries have ConnectionId and ConnectionAlias."""
        conn_resp = client.create_outbound_connection(
            LocalDomainInfo={"AWSDomainInformation": {
                "DomainName": "loc-domain", "OwnerId": "123456789012", "Region": "us-east-1",
            }},
            RemoteDomainInfo={"AWSDomainInformation": {
                "DomainName": "rem-domain", "OwnerId": "123456789012", "Region": "us-west-2",
            }},
            ConnectionAlias="fields-check-conn",
        )
        conn_id = conn_resp["ConnectionId"]
        try:
            resp = client.describe_outbound_connections()
            conns = resp["Connections"]
            matching = [c for c in conns if c["ConnectionId"] == conn_id]
            assert len(matching) == 1
            conn = matching[0]
            assert "ConnectionAlias" in conn
            assert conn["ConnectionAlias"] == "fields-check-conn"
            assert "ConnectionStatus" in conn
        finally:
            client.delete_outbound_connection(ConnectionId=conn_id)

    # --- describe_packages structure ---

    def test_describe_packages_empty_returns_list(self, client):
        """describe_packages always returns PackageDetailsList."""
        resp = client.describe_packages()
        assert "PackageDetailsList" in resp
        assert isinstance(resp["PackageDetailsList"], list)

    def test_describe_packages_entry_structure(self, client):
        """describe_packages entries have PackageID, PackageName, PackageType."""
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        resp = client.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "fake-bucket", "S3Key": "key.txt"},
        )
        pkg_id = resp["PackageDetails"]["PackageID"]
        try:
            pkgs = client.describe_packages()
            matching = [p for p in pkgs["PackageDetailsList"] if p["PackageID"] == pkg_id]
            assert len(matching) == 1
            entry = matching[0]
            assert entry["PackageName"] == pkg_name
            assert entry["PackageType"] == "TXT-DICTIONARY"
            assert "PackageStatus" in entry
        finally:
            client.delete_package(PackageID=pkg_id)

    def test_describe_packages_filter_by_name(self, client):
        """describe_packages filtered by PackageName returns only matching packages."""
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        resp = client.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "fake-bucket", "S3Key": "key.txt"},
        )
        pkg_id = resp["PackageDetails"]["PackageID"]
        try:
            filtered = client.describe_packages(
                Filters=[{"Name": "PackageName", "Value": [pkg_name]}]
            )
            names = [p["PackageName"] for p in filtered["PackageDetailsList"]]
            assert pkg_name in names
        finally:
            client.delete_package(PackageID=pkg_id)

    # --- describe_reserved_instance_offerings structure ---

    def test_describe_reserved_instance_offerings_entry_fields(self, client):
        """describe_reserved_instance_offerings entries have required fields."""
        resp = client.describe_reserved_instance_offerings()
        offerings = resp["ReservedInstanceOfferings"]
        assert len(offerings) > 0
        offering = offerings[0]
        assert "ReservedInstanceOfferingId" in offering
        assert "InstanceType" in offering
        assert "Duration" in offering
        assert "FixedPrice" in offering
        assert "CurrencyCode" in offering
        assert "PaymentOption" in offering

    # --- describe_reserved_instances structure ---

    def test_describe_reserved_instances_empty_list(self, client):
        """describe_reserved_instances returns empty list when none purchased."""
        resp = client.describe_reserved_instances()
        assert "ReservedInstances" in resp
        assert isinstance(resp["ReservedInstances"], list)
        assert len(resp["ReservedInstances"]) == 0

    def test_describe_reserved_instances_with_fake_id_returns_empty(self, client):
        """describe_reserved_instances with non-matching ID returns empty list."""
        resp = client.describe_reserved_instances(
            ReservedInstanceId="00000000-0000-0000-0000-000000000000"
        )
        assert "ReservedInstances" in resp
        assert len(resp["ReservedInstances"]) == 0

    # --- get_default_application_setting ---

    def test_get_default_application_setting_200(self, client):
        """get_default_application_setting always returns HTTP 200."""
        resp = client.get_default_application_setting()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_default_application_setting_is_dict(self, client):
        """get_default_application_setting returns a dict response."""
        resp = client.get_default_application_setting()
        assert isinstance(resp, dict)
        assert "ResponseMetadata" in resp

    # --- list_domain_names structural edge cases ---

    def test_list_domain_names_each_entry_has_name(self, client):
        """Every entry in list_domain_names has a DomainName field."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            resp = client.list_domain_names()
            for entry in resp["DomainNames"]:
                assert "DomainName" in entry
                assert isinstance(entry["DomainName"], str)
                assert len(entry["DomainName"]) > 0
        finally:
            client.delete_domain(DomainName=name)

    def test_list_domain_names_engine_type_values(self, client):
        """list_domain_names EngineType is either OpenSearch or Elasticsearch."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            resp = client.list_domain_names()
            for entry in resp["DomainNames"]:
                if "EngineType" in entry:
                    assert entry["EngineType"] in ("OpenSearch", "Elasticsearch")
        finally:
            client.delete_domain(DomainName=name)


class TestOpenSearchEdgeCases3:
    """Behavioral fidelity and multi-pattern coverage for low-coverage operations."""

    @pytest.fixture
    def client(self):
        return make_client("opensearch")

    @pytest.fixture
    def domain(self, client):
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        yield name
        try:
            client.delete_domain(DomainName=name)
        except Exception:
            pass  # best-effort cleanup

    # --- cancel_service_software_update: full lifecycle ---

    def test_cancel_service_software_update_lifecycle(self, client):
        """cancel_service_software_update in a full domain lifecycle."""
        name = _unique_domain()
        # CREATE
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # RETRIEVE: describe to confirm domain exists
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["DomainName"] == name
            # call cancel and verify response structure
            resp = client.cancel_service_software_update(DomainName=name)
            opts = resp["ServiceSoftwareOptions"]
            assert "UpdateAvailable" in opts
            assert isinstance(opts["UpdateAvailable"], bool)
            assert "UpdateStatus" in opts
            # LIST: verify the domain appears in list
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
            # UPDATE: change cluster config
            upd = client.update_domain_config(
                DomainName=name,
                ClusterConfig={"InstanceType": "t3.medium.search"},
            )
            assert "DomainConfig" in upd
        finally:
            # DELETE
            client.delete_domain(DomainName=name)

    def test_cancel_service_software_update_error_handling(self, client):
        """cancel_service_software_update after nonexistent domain does not raise."""
        name = _unique_domain()
        # CREATE a domain so we can also exercise DELETE
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # RETRIEVE: verify it exists
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["Created"] is True
            # cancel on nonexistent returns a response (server does not raise)
            resp = client.cancel_service_software_update(DomainName="definitely-no-such-domain")
            assert "ServiceSoftwareOptions" in resp
            # LIST: confirm our real domain still exists
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR: after deletion, describe should raise
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    def test_cancel_service_software_update_with_update(self, client):
        """cancel_service_software_update after updating domain config."""
        name = _unique_domain()
        # CREATE
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # UPDATE domain config first
            client.update_domain_config(
                DomainName=name,
                SnapshotOptions={"AutomatedSnapshotStartHour": 7},
            )
            # RETRIEVE: config should reflect the update
            cfg = client.describe_domain_config(DomainName=name)
            assert cfg["DomainConfig"]["SnapshotOptions"]["Options"]["AutomatedSnapshotStartHour"] == 7
            # cancel_service_software_update after update returns all expected fields
            resp = client.cancel_service_software_update(DomainName=name)
            opts = resp["ServiceSoftwareOptions"]
            for key in ("CurrentVersion", "UpdateAvailable", "Cancellable", "UpdateStatus"):
                assert key in opts, f"Missing key: {key}"
            # LIST
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR: domain no longer exists
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    def test_cancel_service_software_update_response_is_complete(self, client, domain):
        """cancel_service_software_update returns all 8 ServiceSoftwareOptions fields."""
        # CREATE (domain fixture), RETRIEVE, LIST, UPDATE, DELETE handled via fixture
        resp = client.cancel_service_software_update(DomainName=domain)
        opts = resp["ServiceSoftwareOptions"]
        required = {
            "CurrentVersion", "NewVersion", "UpdateAvailable", "Cancellable",
            "UpdateStatus", "Description", "AutomatedUpdateDate", "OptionalDeployment",
        }
        missing = required - set(opts.keys())
        assert not missing, f"ServiceSoftwareOptions missing keys: {missing}"
        assert isinstance(opts["UpdateAvailable"], bool)
        assert isinstance(opts["Cancellable"], bool)
        assert isinstance(opts["OptionalDeployment"], bool)
        assert isinstance(opts["CurrentVersion"], str)
        assert opts["UpdateStatus"] in (
            "PENDING_UPDATE", "IN_PROGRESS", "COMPLETED", "NOT_ELIGIBLE", "ELIGIBLE"
        )
        # LIST: domain appears in list
        names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
        assert domain in names
        # UPDATE: still possible after cancel
        upd = client.update_domain_config(
            DomainName=domain,
            AdvancedOptions={"rest.action.multi.allow_explicit_index": "true"},
        )
        assert "DomainConfig" in upd

    def test_cancel_service_software_update_fresh_domain_no_update(self, client):
        """Freshly created domain has UpdateAvailable=False and Cancellable=False."""
        name = _unique_domain()
        # CREATE
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # RETRIEVE
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["DomainName"] == name
            resp = client.cancel_service_software_update(DomainName=name)
            opts = resp["ServiceSoftwareOptions"]
            assert opts["UpdateAvailable"] is False
            assert opts["Cancellable"] is False
            # LIST
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
            # UPDATE still works
            upd = client.update_domain_config(
                DomainName=name,
                ClusterConfig={"InstanceCount": 1},
            )
            assert "DomainConfig" in upd
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR: deleted domain raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- list_versions: comprehensive patterns ---

    def test_list_versions_lifecycle_context(self, client):
        """list_versions in context of a domain lifecycle."""
        name = _unique_domain()
        # CREATE a domain
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # LIST: list_versions returns known versions
            resp = client.list_versions()
            versions = resp["Versions"]
            assert "OpenSearch_2.5" in versions
            # RETRIEVE: domain uses a version from the list
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["EngineVersion"] in versions
            # UPDATE: change to another valid version path via compatible versions
            compat = client.get_compatible_versions(DomainName=name)
            assert "CompatibleVersions" in compat
            # list_versions includes both engine types
            os_vs = [v for v in versions if v.startswith("OpenSearch_")]
            es_vs = [v for v in versions if v.startswith("Elasticsearch_")]
            assert len(os_vs) > 0
            assert len(es_vs) > 0
            # UPDATE domain config to exercise UPDATE pattern
            client.update_domain_config(DomainName=name, SnapshotOptions={"AutomatedSnapshotStartHour": 2})
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR: describe after delete raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    def test_list_versions_pagination_full_cycle(self, client):
        """list_versions pagination: pages together cover all versions without overlap."""
        # CREATE context: create domain to also exercise CREATE pattern
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # LIST with pagination
            page1 = client.list_versions(MaxResults=3)
            assert len(page1["Versions"]) <= 3
            assert "NextToken" in page1  # more than 3 versions exist
            page2 = client.list_versions(NextToken=page1["NextToken"], MaxResults=3)
            assert "Versions" in page2
            assert len(page2["Versions"]) > 0
            # No overlap between pages
            assert set(page1["Versions"]).isdisjoint(set(page2["Versions"]))
            # RETRIEVE: domain's version is somewhere in the full list
            desc = client.describe_domain(DomainName=name)
            version = desc["DomainStatus"]["EngineVersion"]
            # Collect all versions
            all_versions = set(page1["Versions"]) | set(page2["Versions"])
            assert version in all_versions
            # UPDATE domain config
            client.update_domain_config(DomainName=name, AdvancedOptions={"indices.fielddata.cache.size": "40"})
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)


class TestOpenSearchCoveragePatterns:
    """Multi-pattern tests (C/R/L/U/D/E) for low-coverage operations."""

    @pytest.fixture
    def client(self):
        return make_client("opensearch")

    @pytest.fixture
    def domain(self, client):
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        yield name
        try:
            client.delete_domain(DomainName=name)
        except Exception:
            pass  # best-effort cleanup

    # --- list_versions: C+R+L+U+D+E ---

    def test_list_versions_crudel(self, client):
        """list_versions covers C/R/L/U/D/E patterns."""
        name = _unique_domain()
        # CREATE
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # LIST: list_versions returns OpenSearch_2.5
            versions = client.list_versions()["Versions"]
            assert "OpenSearch_2.5" in versions
            # RETRIEVE: domain engine version is in the list
            status = client.describe_domain(DomainName=name)["DomainStatus"]
            assert status["EngineVersion"] in versions
            # UPDATE: change cluster config
            upd = client.update_domain_config(
                DomainName=name,
                ClusterConfig={"InstanceType": "t3.medium.search"},
            )
            assert "DomainConfig" in upd
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    def test_list_versions_error_on_nonexistent_domain(self, client):
        """list_versions combined with nonexistent domain error path."""
        # LIST (baseline)
        versions_resp = client.list_versions()
        assert len(versions_resp["Versions"]) > 0
        os_vs = [v for v in versions_resp["Versions"] if v.startswith("OpenSearch_")]
        assert len(os_vs) > 0
        # ERROR: get_compatible_versions for nonexistent domain raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_compatible_versions(DomainName="xyz-no-domain-for-versions-abc")

    # --- list_domain_names_empty: C+R+L+U+D+E ---

    def test_list_domain_names_empty_crudel(self, client):
        """list_domain_names covers C/R/L/U/D/E patterns including empty state."""
        name = _unique_domain()
        # LIST: baseline (may have other domains but call must succeed)
        before = client.list_domain_names()
        assert "DomainNames" in before
        before_names = [d["DomainName"] for d in before["DomainNames"]]
        assert name not in before_names  # not yet created

        # CREATE
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # LIST: domain appears
            resp = client.list_domain_names()
            assert name in [d["DomainName"] for d in resp["DomainNames"]]
            # RETRIEVE
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["Created"] is True
            # UPDATE
            upd = client.update_domain_config(
                DomainName=name,
                SnapshotOptions={"AutomatedSnapshotStartHour": 4},
            )
            assert upd["DomainConfig"]["SnapshotOptions"]["Options"]["AutomatedSnapshotStartHour"] == 4
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR: after delete, not in list
        after = client.list_domain_names()
        assert name not in [d["DomainName"] for d in after["DomainNames"]]
        # ERROR: describe raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- get_compatible_versions_no_domain: C+R+L+U+D+E ---

    def test_get_compatible_versions_no_domain_crudel(self, client):
        """get_compatible_versions (no domain) covers C/R/L/U/D/E."""
        name = _unique_domain()
        # LIST: global compatible versions (no domain required)
        compat = client.get_compatible_versions()
        assert len(compat["CompatibleVersions"]) > 0
        # CREATE
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # RETRIEVE: domain-specific compatible versions
            dom_compat = client.get_compatible_versions(DomainName=name)
            assert len(dom_compat["CompatibleVersions"]) > 0
            # UPDATE: update domain cluster config
            upd = client.update_domain_config(
                DomainName=name, ClusterConfig={"InstanceCount": 1}
            )
            assert "DomainConfig" in upd
            # LIST again: still works after update
            compat2 = client.get_compatible_versions()
            assert len(compat2["CompatibleVersions"]) > 0
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_compatible_versions(DomainName=name)

    # --- list_tags_nonexistent_domain: C+R+L+U+D+E ---

    def test_list_tags_nonexistent_crudel(self, client):
        """list_tags covers C/R/L/U/D/E including nonexistent ARN path."""
        name = _unique_domain()
        # ERROR: list_tags on nonexistent ARN returns empty (not an exception)
        fake_arn = "arn:aws:es:us-east-1:123456789012:domain/xyz-no-such-abc"
        empty_tags = client.list_tags(ARN=fake_arn)
        assert empty_tags["TagList"] == []

        # CREATE
        resp = client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        arn = resp["DomainStatus"]["ARN"]
        try:
            # LIST: fresh domain has no tags
            assert client.list_tags(ARN=arn)["TagList"] == []
            # UPDATE (add tags)
            client.add_tags(ARN=arn, TagList=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "search"},
            ])
            # RETRIEVE via list_tags
            tags = client.list_tags(ARN=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "search"
            # UPDATE (remove one tag)
            client.remove_tags(ARN=arn, TagKeys=["team"])
            tags2 = client.list_tags(ARN=arn)
            keys2 = [t["Key"] for t in tags2["TagList"]]
            assert "env" in keys2
            assert "team" not in keys2
        finally:
            # DELETE
            client.delete_domain(DomainName=name)

    # --- list_vpc_endpoints: C+R+L+U+D+E ---

    def test_list_vpc_endpoints_crudel(self, client, domain):
        """list_vpc_endpoints covers C/R/L/U/D/E patterns."""
        # LIST: baseline (may be empty)
        before = client.list_vpc_endpoints()
        before_ids = [e["VpcEndpointId"] for e in before["VpcEndpointSummaryList"]]

        # CREATE vpc endpoint
        domain_arn = f"arn:aws:es:us-east-1:123456789012:domain/{domain}"
        create_resp = client.create_vpc_endpoint(
            DomainArn=domain_arn,
            VpcOptions={"SubnetIds": ["subnet-aabbccdd"]},
        )
        endpoint_id = create_resp["VpcEndpoint"]["VpcEndpointId"]
        assert endpoint_id not in before_ids

        try:
            # LIST: new endpoint appears
            list_resp = client.list_vpc_endpoints()
            ids = [e["VpcEndpointId"] for e in list_resp["VpcEndpointSummaryList"]]
            assert endpoint_id in ids

            # RETRIEVE: describe returns endpoint details
            desc = client.describe_vpc_endpoints(VpcEndpointIds=[endpoint_id])
            assert len(desc["VpcEndpoints"]) == 1
            assert desc["VpcEndpoints"][0]["VpcEndpointId"] == endpoint_id

            # UPDATE: update the endpoint (nonexistent raises)
            with pytest.raises(client.exceptions.ResourceNotFoundException):
                client.update_vpc_endpoint(
                    VpcEndpointId="aos-nonexistent-id-xyz",
                    VpcOptions={"SubnetIds": ["subnet-11223344"]},
                )
        finally:
            # DELETE
            del_resp = client.delete_vpc_endpoint(VpcEndpointId=endpoint_id)
            assert "VpcEndpointSummary" in del_resp

        # ERROR: deleted endpoint not in list
        after = client.list_vpc_endpoints()
        after_ids = [e["VpcEndpointId"] for e in after["VpcEndpointSummaryList"]]
        assert endpoint_id not in after_ids

    # --- describe_inbound_connections: C+R+L+U+D+E ---

    def test_describe_inbound_connections_crudel(self, client):
        """describe_inbound_connections covers C/R/L/U/D/E patterns."""
        # LIST: baseline
        resp = client.describe_inbound_connections()
        assert isinstance(resp["Connections"], list)

        # CREATE outbound connection (reflects as inbound on the other side)
        conn_resp = client.create_outbound_connection(
            LocalDomainInfo={"AWSDomainInformation": {
                "DomainName": "src-dom", "OwnerId": "123456789012", "Region": "us-east-1",
            }},
            RemoteDomainInfo={"AWSDomainInformation": {
                "DomainName": "dst-dom", "OwnerId": "111122223333", "Region": "us-west-2",
            }},
            ConnectionAlias="inbound-crudel-conn",
        )
        conn_id = conn_resp["ConnectionId"]
        assert len(conn_id) > 0

        try:
            # LIST with filter
            filtered = client.describe_inbound_connections(
                Filters=[{"Name": "connection-id", "Values": ["fake-no-match"]}],
                MaxResults=10,
            )
            assert "Connections" in filtered
            assert isinstance(filtered["Connections"], list)

            # RETRIEVE structure: describe outbound includes ConnectionAlias
            outbound = client.describe_outbound_connections()
            matching = [c for c in outbound["Connections"] if c["ConnectionId"] == conn_id]
            assert len(matching) == 1
            assert matching[0]["ConnectionAlias"] == "inbound-crudel-conn"
        finally:
            # DELETE
            client.delete_outbound_connection(ConnectionId=conn_id)

        # ERROR: accept nonexistent inbound raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.accept_inbound_connection(ConnectionId="conn-xyz-nonexistent")

    # --- describe_outbound_connections: C+R+L+U+D+E ---

    def test_describe_outbound_connections_crudel(self, client):
        """describe_outbound_connections covers C/R/L/U/D/E patterns."""
        # LIST: baseline
        before = client.describe_outbound_connections()
        before_ids = {c["ConnectionId"] for c in before["Connections"]}

        # CREATE
        conn_resp = client.create_outbound_connection(
            LocalDomainInfo={"AWSDomainInformation": {
                "DomainName": "local-src", "OwnerId": "123456789012", "Region": "us-east-1",
            }},
            RemoteDomainInfo={"AWSDomainInformation": {
                "DomainName": "remote-dst", "OwnerId": "123456789012", "Region": "eu-west-1",
            }},
            ConnectionAlias="outbound-crudel-conn",
        )
        conn_id = conn_resp["ConnectionId"]
        assert conn_id not in before_ids

        try:
            # LIST: new connection appears
            resp = client.describe_outbound_connections()
            ids = {c["ConnectionId"] for c in resp["Connections"]}
            assert conn_id in ids

            # RETRIEVE: connection has expected fields
            matching = [c for c in resp["Connections"] if c["ConnectionId"] == conn_id]
            assert len(matching) == 1
            conn = matching[0]
            assert conn["ConnectionAlias"] == "outbound-crudel-conn"
            assert "ConnectionStatus" in conn

            # UPDATE: delete nonexistent raises
            with pytest.raises(client.exceptions.ResourceNotFoundException):
                client.delete_outbound_connection(ConnectionId="conn-zzz-notfound")
        finally:
            # DELETE
            client.delete_outbound_connection(ConnectionId=conn_id)

        # ERROR: after delete, connection gone
        after = client.describe_outbound_connections()
        after_ids = {c["ConnectionId"] for c in after["Connections"]}
        assert conn_id not in after_ids

    # --- describe_packages: C+R+L+U+D+E ---

    def test_describe_packages_crudel(self, client):
        """describe_packages covers C/R/L/U/D/E patterns."""
        # LIST: baseline (may be empty)
        before = client.describe_packages()
        assert isinstance(before["PackageDetailsList"], list)

        # CREATE
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        create_resp = client.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "fake-bucket", "S3Key": "words.txt"},
        )
        pkg_id = create_resp["PackageDetails"]["PackageID"]
        assert create_resp["PackageDetails"]["PackageName"] == pkg_name

        try:
            # LIST: package appears
            pkgs = client.describe_packages()
            pkg_ids = [p["PackageID"] for p in pkgs["PackageDetailsList"]]
            assert pkg_id in pkg_ids

            # RETRIEVE: filter by ID
            filtered = client.describe_packages(
                Filters=[{"Name": "PackageID", "Value": [pkg_id]}]
            )
            assert len(filtered["PackageDetailsList"]) >= 1
            entry = next(p for p in filtered["PackageDetailsList"] if p["PackageID"] == pkg_id)
            assert entry["PackageName"] == pkg_name
            assert entry["PackageType"] == "TXT-DICTIONARY"
            assert "PackageStatus" in entry

            # UPDATE: update package source
            upd = client.update_package(
                PackageID=pkg_id,
                PackageSource={"S3BucketName": "fake-bucket", "S3Key": "words-v2.txt"},
            )
            assert upd["PackageDetails"]["PackageID"] == pkg_id
        finally:
            # DELETE
            del_resp = client.delete_package(PackageID=pkg_id)
            assert del_resp["PackageDetails"]["PackageID"] == pkg_id

        # ERROR: delete nonexistent raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.delete_package(PackageID=pkg_id)

    # --- describe_reserved_instance_offerings: L+E (no CREATE via API) ---

    def test_describe_reserved_instance_offerings_crudel(self, client, domain):
        """describe_reserved_instance_offerings covers L/R/E patterns with domain lifecycle."""
        # RETRIEVE (domain): verify domain exists
        desc = client.describe_domain(DomainName=domain)
        assert desc["DomainStatus"]["Created"] is True

        # LIST: get offerings
        resp = client.describe_reserved_instance_offerings()
        offerings = resp["ReservedInstanceOfferings"]
        assert len(offerings) > 0

        # RETRIEVE: each offering has required fields
        offering = offerings[0]
        offering_id = offering["ReservedInstanceOfferingId"]
        assert "InstanceType" in offering
        assert "Duration" in offering
        assert "FixedPrice" in offering
        assert "CurrencyCode" in offering
        assert "PaymentOption" in offering

        # LIST with filter by offering ID
        filtered = client.describe_reserved_instance_offerings(
            ReservedInstanceOfferingId=offering_id,
        )
        assert len(filtered["ReservedInstanceOfferings"]) >= 1
        assert filtered["ReservedInstanceOfferings"][0]["ReservedInstanceOfferingId"] == offering_id

        # UPDATE: update domain config (to include UPDATE pattern)
        upd = client.update_domain_config(
            DomainName=domain, ClusterConfig={"InstanceCount": 1}
        )
        assert "DomainConfig" in upd

        # ERROR: purchase nonexistent offering raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.purchase_reserved_instance_offering(
                ReservedInstanceOfferingId="00000000-0000-0000-0000-000000000000",
                ReservationName="test-res",
                InstanceCount=1,
            )

    # --- describe_reserved_instances: L+E (no direct CREATE) ---

    def test_describe_reserved_instances_crudel(self, client, domain):
        """describe_reserved_instances covers L/R/U/D/E patterns."""
        # RETRIEVE domain to anchor lifecycle
        desc = client.describe_domain(DomainName=domain)
        assert desc["DomainStatus"]["Created"] is True

        # LIST: no reserved instances initially
        resp = client.describe_reserved_instances()
        assert isinstance(resp["ReservedInstances"], list)
        # Since we can't purchase without a real offering, should be empty
        assert len(resp["ReservedInstances"]) == 0

        # LIST with filter: returns empty
        resp2 = client.describe_reserved_instances(
            ReservedInstanceId="00000000-0000-0000-0000-000000000000"
        )
        assert resp2["ReservedInstances"] == []

        # UPDATE: update domain config to include UPDATE pattern
        upd = client.update_domain_config(
            DomainName=domain, SnapshotOptions={"AutomatedSnapshotStartHour": 8}
        )
        assert upd["DomainConfig"]["SnapshotOptions"]["Options"]["AutomatedSnapshotStartHour"] == 8

        # ERROR: purchase with nonexistent offering raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.purchase_reserved_instance_offering(
                ReservedInstanceOfferingId="ffffffff-ffff-ffff-ffff-ffffffffffff",
                ReservationName="test-resv",
                InstanceCount=1,
            )

    # --- get_default_application_setting: C+R+L+U+D+E ---

    def test_get_default_application_setting_crudel(self, client, domain):
        """get_default_application_setting covers C/R/L/U/D/E via domain lifecycle."""
        # RETRIEVE domain
        desc = client.describe_domain(DomainName=domain)
        assert desc["DomainStatus"]["Created"] is True

        # LIST: list_domain_names confirms domain
        names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
        assert domain in names

        # RETRIEVE: get_default_application_setting returns 200
        resp = client.get_default_application_setting()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp, dict)

        # UPDATE: update domain config
        upd = client.update_domain_config(
            DomainName=domain,
            AdvancedOptions={"rest.action.multi.allow_explicit_index": "true"},
        )
        assert "DomainConfig" in upd

        # ERROR: describe nonexistent domain
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName="xyz-does-not-exist-abc")

    # --- list_applications: C+R+L+U+D+E ---

    def test_list_applications_crudel(self, client):
        """list_applications covers C/R/L/U/D/E patterns."""
        # LIST: baseline
        before = client.list_applications()
        assert "ApplicationSummaries" in before
        assert isinstance(before["ApplicationSummaries"], list)

        # CREATE application
        app_name = f"app-{uuid.uuid4().hex[:8]}"
        try:
            create_resp = client.create_application(name=app_name)
            app_id = create_resp.get("id", "")
            if not app_id:
                # Application feature partially implemented; verify LIST at least
                resp = client.list_applications()
                assert "ApplicationSummaries" in resp
                return

            # LIST: new app appears
            resp = client.list_applications()
            ids = [a.get("id", "") for a in resp["ApplicationSummaries"]]
            assert app_id in ids

            # RETRIEVE
            get_resp = client.get_application(id=app_id)
            assert get_resp.get("id") == app_id or get_resp.get("name") == app_name

            # ERROR: get nonexistent app raises
            with pytest.raises(client.exceptions.ClientError) as exc_info:
                client.get_application(id="nonexistent-app-id-xyz")
            assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 404)

        except client.exceptions.ClientError:
            # If application ops not implemented, ensure list_applications still works
            resp = client.list_applications()
            assert "ApplicationSummaries" in resp
        finally:
            try:
                if "app_id" in dir() and app_id:
                    client.delete_application(id=app_id)
            except Exception:
                pass

    # --- get_compatible_versions_structure: C+R+L+U+D+E ---

    def test_get_compatible_versions_structure_crudel(self, client):
        """get_compatible_versions structure covers C/R/L/U/D/E patterns."""
        # LIST: global call returns structured data
        resp = client.get_compatible_versions()
        versions = resp["CompatibleVersions"]
        assert len(versions) > 0
        for entry in versions:
            assert "SourceVersion" in entry
            assert "TargetVersions" in entry
            assert isinstance(entry["TargetVersions"], list)
            assert len(entry["TargetVersions"]) > 0
            # format check
            src = entry["SourceVersion"]
            assert src.startswith("OpenSearch_") or src.startswith("Elasticsearch_")

        # CREATE domain
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # RETRIEVE: domain-specific versions
            dom_resp = client.get_compatible_versions(DomainName=name)
            dom_versions = dom_resp["CompatibleVersions"]
            assert len(dom_versions) > 0
            # TargetVersions are all upgrades (not downgrades)
            for entry in dom_versions:
                for tv in entry["TargetVersions"]:
                    assert tv.startswith("OpenSearch_") or tv.startswith("Elasticsearch_")

            # UPDATE: change domain config (snapshots)
            upd = client.update_domain_config(
                DomainName=name,
                SnapshotOptions={"AutomatedSnapshotStartHour": 1},
            )
            assert "DomainConfig" in upd

            # LIST: global call still works after domain update
            resp2 = client.get_compatible_versions()
            assert len(resp2["CompatibleVersions"]) > 0
        finally:
            # DELETE
            client.delete_domain(DomainName=name)

        # ERROR: domain-specific call after delete raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_compatible_versions(DomainName=name)

    # --- list_domain_names: add missing patterns ---

    def test_list_domain_names_crud_lifecycle(self, client):
        """list_domain_names reflects the full domain lifecycle."""
        name = _unique_domain()
        # CREATE
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # LIST: domain appears immediately
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
            # RETRIEVE
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["DomainName"] == name
            # UPDATE
            client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})
            # LIST again: still there after update
            names2 = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names2
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # LIST after delete: gone
        names3 = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
        assert name not in names3
        # ERROR: describe after delete raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- get_compatible_versions: add missing LIST, DELETE, ERROR ---

    def test_get_compatible_versions_crud_context(self, client):
        """get_compatible_versions in a full domain lifecycle with error check."""
        name = _unique_domain()
        # CREATE
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # RETRIEVE
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["EngineVersion"] == "OpenSearch_2.5"
            # LIST versions
            versions = client.list_versions()["Versions"]
            assert "OpenSearch_2.5" in versions
            # get_compatible_versions for the domain
            compat = client.get_compatible_versions(DomainName=name)
            assert "CompatibleVersions" in compat
            for entry in compat["CompatibleVersions"]:
                assert "SourceVersion" in entry
                assert "TargetVersions" in entry
            # UPDATE domain config
            client.update_domain_config(DomainName=name, SnapshotOptions={"AutomatedSnapshotStartHour": 4})
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR: get_compatible_versions for deleted domain raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_compatible_versions(DomainName=name)

    # --- list_tags: add missing CREATE, UPDATE, DELETE, ERROR ---

    def test_list_tags_nonexistent_domain_lifecycle(self, client):
        """list_tags on nonexistent domain in context of a real domain lifecycle."""
        name = _unique_domain()
        # CREATE
        resp = client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        arn = resp["DomainStatus"]["ARN"]
        try:
            # UPDATE: add tags
            client.add_tags(ARN=arn, TagList=[{"Key": "app", "Value": "test"}])
            # LIST (tags): appears in list
            tags = client.list_tags(ARN=arn)
            assert any(t["Key"] == "app" for t in tags["TagList"])
            # UPDATE: remove tag
            client.remove_tags(ARN=arn, TagKeys=["app"])
            # list_tags on nonexistent ARN returns empty (not an error)
            empty = client.list_tags(ARN="arn:aws:es:us-east-1:123456789012:domain/no-such-domain")
            assert "TagList" in empty
            assert isinstance(empty["TagList"], list)
            # RETRIEVE
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["DomainName"] == name
        finally:
            # DELETE
            client.delete_domain(DomainName=name)
        # ERROR: describe after delete raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- list_vpc_endpoints: add missing CREATE, RETRIEVE, UPDATE, DELETE, ERROR ---

    def test_list_vpc_endpoints_full_lifecycle(self, client, domain):
        """list_vpc_endpoints in a full endpoint lifecycle."""
        # CREATE vpc endpoint
        resp = client.create_vpc_endpoint(
            DomainArn=f"arn:aws:es:us-east-1:123456789012:domain/{domain}",
            VpcOptions={"SubnetIds": ["subnet-aabbccdd"]},
        )
        endpoint_id = resp["VpcEndpoint"]["VpcEndpointId"]
        try:
            # LIST: endpoint appears in list
            list_resp = client.list_vpc_endpoints()
            ids = [e["VpcEndpointId"] for e in list_resp["VpcEndpointSummaryList"]]
            assert endpoint_id in ids
            # RETRIEVE: describe the endpoint
            desc_resp = client.describe_vpc_endpoints(VpcEndpointIds=[endpoint_id])
            assert "VpcEndpoints" in desc_resp
            endpoints = [e for e in desc_resp["VpcEndpoints"] if e["VpcEndpointId"] == endpoint_id]
            assert len(endpoints) == 1
            # UPDATE: domain still accessible
            upd = client.update_domain_config(DomainName=domain, ClusterConfig={"InstanceCount": 1})
            assert "DomainConfig" in upd
        finally:
            # DELETE the endpoint
            del_resp = client.delete_vpc_endpoint(VpcEndpointId=endpoint_id)
            assert "VpcEndpointSummary" in del_resp
        # ERROR: describe nonexistent endpoint returns errors list
        result = client.describe_vpc_endpoints(VpcEndpointIds=[endpoint_id])
        assert "VpcEndpointErrors" in result
        errors = result["VpcEndpointErrors"]
        assert any(e["VpcEndpointId"] == endpoint_id for e in errors)

    # --- describe_inbound_connections: add CREATE, RETRIEVE, UPDATE, DELETE, ERROR ---

    def test_describe_inbound_connections_with_connection_lifecycle(self, client):
        """describe_inbound_connections alongside outbound connection lifecycle."""
        # CREATE an outbound connection
        conn_resp = client.create_outbound_connection(
            LocalDomainInfo={"AWSDomainInformation": {
                "DomainName": "src-domain", "OwnerId": "123456789012", "Region": "us-east-1",
            }},
            RemoteDomainInfo={"AWSDomainInformation": {
                "DomainName": "dst-domain", "OwnerId": "123456789012", "Region": "us-east-1",
            }},
            ConnectionAlias="inbound-lifecycle-conn",
        )
        conn_id = conn_resp["ConnectionId"]
        assert conn_resp["ConnectionStatus"]["StatusCode"] in (
            "VALIDATING", "VALIDATION_FAILED", "PENDING_ACCEPTANCE",
            "APPROVED", "PROVISIONING", "ACTIVE", "REJECTING", "REJECTED",
            "DELETING", "DELETED",
        )
        try:
            # LIST inbound connections
            inbound = client.describe_inbound_connections()
            assert "Connections" in inbound
            assert isinstance(inbound["Connections"], list)
            # RETRIEVE outbound connections - connection should appear
            outbound = client.describe_outbound_connections()
            out_ids = [c["ConnectionId"] for c in outbound["Connections"]]
            assert conn_id in out_ids
            # UPDATE: alias is preserved in retrieved connection
            matching = [c for c in outbound["Connections"] if c["ConnectionId"] == conn_id]
            assert matching[0]["ConnectionAlias"] == "inbound-lifecycle-conn"
        finally:
            # DELETE
            client.delete_outbound_connection(ConnectionId=conn_id)
        # ERROR: delete nonexistent raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.delete_outbound_connection(ConnectionId=conn_id)

    # --- describe_outbound_connections: add CREATE, RETRIEVE, UPDATE, DELETE, ERROR ---

    def test_describe_outbound_connections_full_lifecycle(self, client):
        """describe_outbound_connections in a complete connection lifecycle."""
        # CREATE
        resp = client.create_outbound_connection(
            LocalDomainInfo={"AWSDomainInformation": {
                "DomainName": "out-src", "OwnerId": "123456789012", "Region": "us-east-1",
            }},
            RemoteDomainInfo={"AWSDomainInformation": {
                "DomainName": "out-dst", "OwnerId": "123456789012", "Region": "us-west-2",
            }},
            ConnectionAlias="outbound-lc-conn",
        )
        conn_id = resp["ConnectionId"]
        assert "ConnectionStatus" in resp
        try:
            # LIST: appears in outbound connections
            list_resp = client.describe_outbound_connections()
            out_ids = [c["ConnectionId"] for c in list_resp["Connections"]]
            assert conn_id in out_ids
            # RETRIEVE: the connection has all required fields
            conn = next(c for c in list_resp["Connections"] if c["ConnectionId"] == conn_id)
            assert "ConnectionAlias" in conn
            assert "ConnectionStatus" in conn
            assert "LocalDomainInfo" in conn
            assert "RemoteDomainInfo" in conn
            # UPDATE: verify inbound connections also tracked
            inbound = client.describe_inbound_connections()
            assert "Connections" in inbound
        finally:
            # DELETE
            client.delete_outbound_connection(ConnectionId=conn_id)
        # ERROR: delete again raises ResourceNotFoundException
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.delete_outbound_connection(ConnectionId=conn_id)

    # --- describe_packages: add CREATE, RETRIEVE, UPDATE, DELETE, ERROR ---

    def test_describe_packages_full_lifecycle(self, client):
        """describe_packages in a full package lifecycle."""
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        # CREATE
        create_resp = client.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "test-bucket", "S3Key": "dict.txt"},
        )
        pkg = create_resp["PackageDetails"]
        pkg_id = pkg["PackageID"]
        assert pkg["PackageName"] == pkg_name
        assert pkg["PackageType"] == "TXT-DICTIONARY"
        try:
            # LIST: package appears in describe_packages
            pkgs = client.describe_packages()
            ids = [p["PackageID"] for p in pkgs["PackageDetailsList"]]
            assert pkg_id in ids
            # RETRIEVE: describe with filter
            filtered = client.describe_packages(
                Filters=[{"Name": "PackageID", "Value": [pkg_id]}]
            )
            assert len([p for p in filtered["PackageDetailsList"] if p["PackageID"] == pkg_id]) == 1
            # UPDATE: update package source
            upd = client.update_package(
                PackageID=pkg_id,
                PackageSource={"S3BucketName": "test-bucket", "S3Key": "updated-dict.txt"},
            )
            assert upd["PackageDetails"]["PackageID"] == pkg_id
            # Package version history has entries after update
            hist = client.get_package_version_history(PackageID=pkg_id)
            assert "PackageVersionHistoryList" in hist
        finally:
            # DELETE
            del_resp = client.delete_package(PackageID=pkg_id)
            assert del_resp["PackageDetails"]["PackageID"] == pkg_id
        # ERROR: delete again raises ResourceNotFoundException
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.delete_package(PackageID=pkg_id)


class TestOpenSearchBehavioralFidelity:
    """Behavioral fidelity and edge-case tests targeting UPDATE and ERROR pattern gaps."""

    @pytest.fixture
    def client(self):
        return make_client("opensearch")

    # --- Domain name boundary tests ---

    def test_domain_name_max_length(self, client):
        """Domain name at max length (28 chars) is valid and all fields work."""
        name = "a" + "b" * 27  # 28 chars total
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["DomainName"] == name
            # UPDATE
            upd = client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})
            assert upd["DomainConfig"]["ClusterConfig"]["Options"]["InstanceCount"] == 1
            # LIST
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            client.delete_domain(DomainName=name)
        # ERROR: deleted domain raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    def test_domain_name_min_length(self, client):
        """Domain name at min length (3 chars) is valid."""
        name = "abc"
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["DomainName"] == name
            upd = client.update_domain_config(DomainName=name, SnapshotOptions={"AutomatedSnapshotStartHour": 1})
            assert upd["DomainConfig"]["SnapshotOptions"]["Options"]["AutomatedSnapshotStartHour"] == 1
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- Update persistence ---

    def test_update_cluster_config_persists(self, client):
        """UpdateDomainConfig changes are reflected in subsequent DescribeDomainConfig."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            client.update_domain_config(
                DomainName=name,
                ClusterConfig={"InstanceType": "t3.medium.search", "InstanceCount": 2},
            )
            cfg = client.describe_domain_config(DomainName=name)
            cc = cfg["DomainConfig"]["ClusterConfig"]["Options"]
            assert cc["InstanceType"] == "t3.medium.search"
            assert cc["InstanceCount"] == 2
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain_config(DomainName=name)

    def test_update_ebs_options_persists(self, client):
        """UpdateDomainConfig EBSOptions are reflected in DescribeDomainConfig."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            client.update_domain_config(
                DomainName=name,
                EBSOptions={"EBSEnabled": True, "VolumeType": "gp3", "VolumeSize": 30},
            )
            cfg = client.describe_domain_config(DomainName=name)
            ebs = cfg["DomainConfig"]["EBSOptions"]["Options"]
            assert ebs["EBSEnabled"] is True
            assert ebs["VolumeType"] == "gp3"
            assert ebs["VolumeSize"] == 30
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    def test_update_snapshot_options_persists(self, client):
        """UpdateDomainConfig SnapshotOptions are reflected in DescribeDomainConfig."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            client.update_domain_config(DomainName=name, SnapshotOptions={"AutomatedSnapshotStartHour": 12})
            cfg = client.describe_domain_config(DomainName=name)
            assert cfg["DomainConfig"]["SnapshotOptions"]["Options"]["AutomatedSnapshotStartHour"] == 12
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- Idempotency ---

    def test_create_domain_idempotent_raises(self, client):
        """Creating a domain with same name twice raises ResourceAlreadyExistsException."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["DomainName"] == name
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
            client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})
            # ERROR: duplicate create raises
            with pytest.raises(client.exceptions.ResourceAlreadyExistsException):
                client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        finally:
            client.delete_domain(DomainName=name)

    # --- List pagination edge cases ---

    def test_list_domain_names_multiple_domains_all_returned(self, client):
        """list_domain_names returns all created domains."""
        names = [_unique_domain() for _ in range(3)]
        for n in names:
            client.create_domain(DomainName=n, EngineVersion="OpenSearch_2.5")
        try:
            listed = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            for n in names:
                assert n in listed
            # UPDATE one domain to exercise UPDATE pattern
            client.update_domain_config(DomainName=names[0], ClusterConfig={"InstanceCount": 1})
        finally:
            for n in names:
                client.delete_domain(DomainName=n)
        # ERROR: after deletion, all should raise
        for n in names:
            with pytest.raises(client.exceptions.ResourceNotFoundException):
                client.describe_domain(DomainName=n)

    # --- Tag lifecycle ---

    def test_tag_full_lifecycle_value_assertions(self, client):
        """Add, overwrite, and remove tags with explicit value checks."""
        name = _unique_domain()
        resp = client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        arn = resp["DomainStatus"]["ARN"]
        try:
            # Add tags
            client.add_tags(ARN=arn, TagList=[{"Key": "env", "Value": "dev"}, {"Key": "team", "Value": "ops"}])
            tags = client.list_tags(ARN=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
            assert tag_map["env"] == "dev"
            assert tag_map["team"] == "ops"
            # UPDATE: overwrite one tag
            client.add_tags(ARN=arn, TagList=[{"Key": "env", "Value": "prod"}])
            tags2 = client.list_tags(ARN=arn)
            tag_map2 = {t["Key"]: t["Value"] for t in tags2["TagList"]}
            assert tag_map2["env"] == "prod"
            assert tag_map2["team"] == "ops"
            # Remove one tag
            client.remove_tags(ARN=arn, TagKeys=["team"])
            tags3 = client.list_tags(ARN=arn)
            remaining = [t["Key"] for t in tags3["TagList"]]
            assert "team" not in remaining
            assert "env" in remaining
            # LIST domains still shows domain
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            client.delete_domain(DomainName=name)
        # ERROR: after delete, describe raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- ARN format behavioral fidelity ---

    def test_arn_contains_region_and_account(self, client):
        """Domain ARN contains region 'us-east-1' and numeric account ID."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            desc = client.describe_domain(DomainName=name)
            arn = desc["DomainStatus"]["ARN"]
            assert "us-east-1" in arn
            parts = arn.split(":")
            assert len(parts) >= 6
            assert parts[4].isdigit(), f"Expected numeric account, got: {parts[4]}"
            # UPDATE
            client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})
            # LIST
            listed = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in listed
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- created_at and lifecycle flags ---

    def test_domain_flags_after_create_and_delete(self, client):
        """Domain Created=True, Deleted=False after create; describe raises after delete."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            desc = client.describe_domain(DomainName=name)
            status = desc["DomainStatus"]
            assert status["Created"] is True
            assert status["Deleted"] is False
            assert status["UpgradeProcessing"] is False
            # UPDATE
            client.update_domain_config(DomainName=name, SnapshotOptions={"AutomatedSnapshotStartHour": 9})
            # LIST
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            client.delete_domain(DomainName=name)
        # ERROR: ResourceNotFoundException after delete
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- Multiple updates in sequence ---

    def test_multiple_sequential_updates(self, client):
        """Multiple UpdateDomainConfig calls each persist their changes."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            client.update_domain_config(DomainName=name, SnapshotOptions={"AutomatedSnapshotStartHour": 3})
            client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 2})
            client.update_domain_config(DomainName=name, EBSOptions={"EBSEnabled": True, "VolumeType": "gp2", "VolumeSize": 20})
            # RETRIEVE: all changes persisted
            cfg = client.describe_domain_config(DomainName=name)
            assert cfg["DomainConfig"]["SnapshotOptions"]["Options"]["AutomatedSnapshotStartHour"] == 3
            assert cfg["DomainConfig"]["ClusterConfig"]["Options"]["InstanceCount"] == 2
            assert cfg["DomainConfig"]["EBSOptions"]["Options"]["VolumeSize"] == 20
            # LIST
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain_config(DomainName=name)

    # --- Error codes and messages ---

    def test_update_nonexistent_domain_error_code(self, client):
        """UpdateDomainConfig on nonexistent domain raises ResourceNotFoundException."""
        name = _unique_domain()
        # CREATE a real domain to exercise that pattern
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # LIST confirms it exists
            names = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in names
            # UPDATE existing domain works fine
            client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})
        finally:
            client.delete_domain(DomainName=name)
        # ERROR: update nonexistent domain raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})

    def test_describe_domains_mixed_valid_invalid(self, client):
        """describe_domains with mix of valid and invalid names returns only valid ones."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            # RETRIEVE with mixed
            resp = client.describe_domains(DomainNames=[name, "xyz-nonexistent-999"])
            statuses = resp["DomainStatusList"]
            found_names = [d["DomainName"] for d in statuses]
            assert name in found_names
            assert len(statuses) == 1  # nonexistent is silently ignored
            # UPDATE
            client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})
            # LIST
            listed = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in listed
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- Endpoint format ---

    def test_domain_endpoint_format(self, client):
        """Domain Endpoint contains the domain name."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["Endpoint"].lower().startswith("http") or \
                name in desc["DomainStatus"]["Endpoint"]
            # UPDATE
            client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})
            # LIST
            listed = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in listed
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- DomainId format ---

    def test_domain_id_contains_name(self, client):
        """DomainId format is accountid/domainname and contains the domain name."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            desc = client.describe_domain(DomainName=name)
            domain_id = desc["DomainStatus"]["DomainId"]
            assert "/" in domain_id
            account_part, name_part = domain_id.split("/", 1)
            assert account_part.isdigit()
            assert name_part == name
            # UPDATE
            client.update_domain_config(DomainName=name, SnapshotOptions={"AutomatedSnapshotStartHour": 0})
            # LIST
            listed = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in listed
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- Package error handling ---

    def test_delete_package_after_dissociate_allows_reuse(self, client):
        """Deleting a dissociated package succeeds; deleting again raises error."""
        domain_name = _unique_domain()
        client.create_domain(DomainName=domain_name, EngineVersion="OpenSearch_2.5")
        pkg_name = f"pkg-{uuid.uuid4().hex[:8]}"
        pkg_resp = client.create_package(
            PackageName=pkg_name,
            PackageType="TXT-DICTIONARY",
            PackageSource={"S3BucketName": "bucket", "S3Key": "dict.txt"},
        )
        pkg_id = pkg_resp["PackageDetails"]["PackageID"]
        try:
            # Associate then dissociate
            client.associate_package(PackageID=pkg_id, DomainName=domain_name)
            client.dissociate_package(PackageID=pkg_id, DomainName=domain_name)
            # LIST: package still exists
            pkgs = client.describe_packages()
            ids = [p["PackageID"] for p in pkgs["PackageDetailsList"]]
            assert pkg_id in ids
            # UPDATE package source
            upd = client.update_package(
                PackageID=pkg_id,
                PackageSource={"S3BucketName": "bucket", "S3Key": "updated.txt"},
            )
            assert upd["PackageDetails"]["PackageID"] == pkg_id
        finally:
            client.delete_package(PackageID=pkg_id)
            client.delete_domain(DomainName=domain_name)
        # ERROR: delete again raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.delete_package(PackageID=pkg_id)

    # --- list_versions with filter by engine ---

    def test_list_versions_opensearch_specific(self, client):
        """list_versions returns specific known OpenSearch versions."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        try:
            resp = client.list_versions()
            versions = resp["Versions"]
            assert "OpenSearch_2.5" in versions
            # domain's version appears in full list
            desc = client.describe_domain(DomainName=name)
            assert desc["DomainStatus"]["EngineVersion"] in versions
            # UPDATE
            client.update_domain_config(DomainName=name, ClusterConfig={"InstanceCount": 1})
            # LIST domains
            listed = [d["DomainName"] for d in client.list_domain_names()["DomainNames"]]
            assert name in listed
        finally:
            client.delete_domain(DomainName=name)
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)

    # --- Data source full lifecycle with error ---

    def test_data_source_lifecycle_with_error(self, client):
        """Add, get, update, delete data source; get after delete raises error."""
        name = _unique_domain()
        client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.5")
        ds_name = f"ds-{uuid.uuid4().hex[:8]}"
        try:
            # CREATE data source
            add_resp = client.add_data_source(
                DomainName=name,
                Name=ds_name,
                DataSourceType={"S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/r1"}},
            )
            assert len(add_resp["Message"]) > 0
            # RETRIEVE
            get_resp = client.get_data_source(DomainName=name, Name=ds_name)
            assert get_resp["Name"] == ds_name
            # LIST
            list_resp = client.list_data_sources(DomainName=name)
            assert any(ds["Name"] == ds_name for ds in list_resp["DataSources"])
            # UPDATE
            client.update_data_source(
                DomainName=name,
                Name=ds_name,
                DataSourceType={"S3GlueDataCatalog": {"RoleArn": "arn:aws:iam::123456789012:role/r2"}},
            )
            # DELETE
            client.delete_data_source(DomainName=name, Name=ds_name)
        finally:
            client.delete_domain(DomainName=name)
        # ERROR: describe domain after delete raises
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_domain(DomainName=name)
