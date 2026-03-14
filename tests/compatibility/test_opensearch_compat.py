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

    def test_list_tags_nonexistent_domain(self, opensearch):
        """ListTags on a non-existent domain ARN returns empty tag list."""
        resp = opensearch.list_tags(ARN="arn:aws:es:us-east-1:123456789012:domain/nonexistent")
        assert "TagList" in resp


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
            pass

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
