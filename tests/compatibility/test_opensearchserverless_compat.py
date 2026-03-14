"""OpenSearch Serverless compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _uid():
    """Short unique suffix for resource names."""
    return uuid.uuid4().hex[:8]


@pytest.fixture
def opensearchserverless():
    return make_client("opensearchserverless")


class TestOpenSearchServerlessOperations:
    def test_list_collections(self, opensearchserverless):
        """ListCollections returns a list of collection summaries."""
        response = opensearchserverless.list_collections()
        assert "collectionSummaries" in response
        assert isinstance(response["collectionSummaries"], list)

    def test_list_collections_with_filter(self, opensearchserverless):
        """ListCollections accepts a filter parameter."""
        response = opensearchserverless.list_collections(collectionFilters={"status": "ACTIVE"})
        assert "collectionSummaries" in response

    def test_list_collections_status_code(self, opensearchserverless):
        """ListCollections returns HTTP 200."""
        response = opensearchserverless.list_collections()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestOpensearchserverlessAutoCoverage:
    """Auto-generated coverage tests for opensearchserverless."""

    @pytest.fixture
    def client(self):
        return make_client("opensearchserverless")

    def test_batch_get_collection(self, client):
        """BatchGetCollection returns a response."""
        resp = client.batch_get_collection()
        assert "collectionDetails" in resp

    def test_create_and_delete_collection(self, client):
        """CreateCollection creates a collection, DeleteCollection removes it."""
        suffix = _uid()
        coll_name = f"test-crdel-{suffix}"
        pol_name = f"enc-crdel-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/{coll_name}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        client.create_security_policy(name=pol_name, type="encryption", policy=policy)
        try:
            resp = client.create_collection(name=coll_name, type="SEARCH")
            detail = resp["createCollectionDetail"]
            assert detail["name"] == coll_name
            assert detail["status"] in ("CREATING", "ACTIVE")
            assert "id" in detail
            assert "arn" in detail

            del_resp = client.delete_collection(id=detail["id"])
            del_detail = del_resp["deleteCollectionDetail"]
            assert del_detail["id"] == detail["id"]
            assert del_detail["name"] == coll_name
            assert del_detail["status"] in ("DELETING", "ACTIVE")
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="encryption")
            except ClientError:
                pass

    def test_delete_nonexistent_collection(self, client):
        """DeleteCollection with a non-existent ID raises an error."""
        with pytest.raises(ClientError) as exc:
            client.delete_collection(id="does-not-exist-12345678")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
        )

    def test_create_security_policy(self, client):
        """CreateSecurityPolicy creates an encryption policy."""
        suffix = _uid()
        pol_name = f"enc-csp-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/test-csp-{suffix}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        try:
            resp = client.create_security_policy(name=pol_name, type="encryption", policy=policy)
            detail = resp["securityPolicyDetail"]
            assert detail["name"] == pol_name
            assert detail["type"] == "encryption"
            assert "policyVersion" in detail
            assert "createdDate" in detail
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="encryption")
            except ClientError:
                pass

    def test_get_security_policy(self, client):
        """GetSecurityPolicy retrieves a security policy by name and type."""
        suffix = _uid()
        pol_name = f"enc-gsp-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/test-gsp-{suffix}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        client.create_security_policy(name=pol_name, type="encryption", policy=policy)
        try:
            resp = client.get_security_policy(name=pol_name, type="encryption")
            detail = resp["securityPolicyDetail"]
            assert detail["name"] == pol_name
            assert detail["type"] == "encryption"
            assert "policy" in detail
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="encryption")
            except ClientError:
                pass

    def test_list_security_policies(self, client):
        """ListSecurityPolicies returns policy summaries."""
        resp = client.list_security_policies(type="encryption")
        assert "securityPolicySummaries" in resp
        assert isinstance(resp["securityPolicySummaries"], list)

    def test_tag_and_untag_resource(self, client):
        """TagResource and UntagResource manage tags on a collection."""
        suffix = _uid()
        coll_name = f"test-tag-{suffix}"
        pol_name = f"enc-tag-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/{coll_name}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        client.create_security_policy(name=pol_name, type="encryption", policy=policy)
        try:
            resp = client.create_collection(name=coll_name, type="SEARCH")
            arn = resp["createCollectionDetail"]["arn"]
            coll_id = resp["createCollectionDetail"]["id"]
            try:
                # Tag
                client.tag_resource(resourceArn=arn, tags=[{"key": "env", "value": "test"}])
                tags_resp = client.list_tags_for_resource(resourceArn=arn)
                assert any(t["key"] == "env" for t in tags_resp["tags"])

                # Untag
                client.untag_resource(resourceArn=arn, tagKeys=["env"])
                tags_resp2 = client.list_tags_for_resource(resourceArn=arn)
                assert not any(t["key"] == "env" for t in tags_resp2.get("tags", []))
            finally:
                client.delete_collection(id=coll_id)
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="encryption")
            except ClientError:
                pass

    def test_update_security_policy(self, client):
        """UpdateSecurityPolicy modifies a policy's description."""
        suffix = _uid()
        pol_name = f"enc-usp-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/test-usp-{suffix}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        create_resp = client.create_security_policy(name=pol_name, type="encryption", policy=policy)
        version = create_resp["securityPolicyDetail"]["policyVersion"]
        try:
            resp = client.update_security_policy(
                name=pol_name,
                type="encryption",
                policyVersion=version,
                description="Updated description",
                policy=policy,
            )
            detail = resp["securityPolicyDetail"]
            assert detail["name"] == pol_name
            assert detail["description"] == "Updated description"
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="encryption")
            except ClientError:
                pass

    def test_batch_get_collection_with_ids(self, client):
        """BatchGetCollection with specific IDs returns matching collections."""
        suffix = _uid()
        coll_name = f"test-bgc-{suffix}"
        pol_name = f"enc-bgc-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/{coll_name}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        client.create_security_policy(name=pol_name, type="encryption", policy=policy)
        try:
            resp = client.create_collection(name=coll_name, type="SEARCH")
            coll_id = resp["createCollectionDetail"]["id"]
            try:
                batch_resp = client.batch_get_collection(ids=[coll_id])
                assert "collectionDetails" in batch_resp
                found_ids = [c["id"] for c in batch_resp["collectionDetails"]]
                assert coll_id in found_ids
            finally:
                client.delete_collection(id=coll_id)
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="encryption")
            except ClientError:
                pass

    def test_create_vpc_endpoint(self, client):
        """CreateVpcEndpoint creates a VPC endpoint and returns its details."""
        suffix = _uid()
        resp = client.create_vpc_endpoint(
            name=f"test-vpce-{suffix}",
            vpcId=f"vpc-{suffix}",
            subnetIds=[f"subnet-{suffix}"],
        )
        detail = resp["createVpcEndpointDetail"]
        assert "id" in detail
        assert detail["name"] == f"test-vpce-{suffix}"
        assert detail["status"] in ("ACTIVE", "CREATING")

    def test_create_network_security_policy(self, client):
        """CreateSecurityPolicy with type=network creates a network policy."""
        suffix = _uid()
        pol_name = f"net-cnsp-{suffix}"
        policy = json.dumps(
            [
                {
                    "Rules": [
                        {
                            "ResourceType": "collection",
                            "Resource": [f"collection/test-cnsp-{suffix}"],
                        }
                    ],
                    "AllowFromPublic": True,
                }
            ]
        )
        try:
            resp = client.create_security_policy(name=pol_name, type="network", policy=policy)
            detail = resp["securityPolicyDetail"]
            assert detail["name"] == pol_name
            assert detail["type"] == "network"
            assert "policyVersion" in detail
            assert "createdDate" in detail
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="network")
            except ClientError:
                pass

    def test_list_security_policies_network(self, client):
        """ListSecurityPolicies with type=network returns policy summaries."""
        resp = client.list_security_policies(type="network")
        assert "securityPolicySummaries" in resp
        assert isinstance(resp["securityPolicySummaries"], list)

    def test_create_collection_timeseries_type(self, client):
        """CreateCollection with type=TIMESERIES creates a timeseries collection."""
        suffix = _uid()
        coll_name = f"test-ts-{suffix}"
        pol_name = f"enc-ts-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/{coll_name}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        client.create_security_policy(name=pol_name, type="encryption", policy=policy)
        try:
            resp = client.create_collection(name=coll_name, type="TIMESERIES")
            detail = resp["createCollectionDetail"]
            assert detail["name"] == coll_name
            assert detail["type"] == "TIMESERIES"
            assert "id" in detail
            client.delete_collection(id=detail["id"])
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="encryption")
            except ClientError:
                pass

    def test_batch_get_collection_empty(self, client):
        """BatchGetCollection with no args returns empty details."""
        resp = client.batch_get_collection()
        assert "collectionDetails" in resp
        assert isinstance(resp["collectionDetails"], list)

    def test_list_tags_for_resource(self, client):
        """ListTagsForResource returns tags for a collection."""
        suffix = _uid()
        coll_name = f"test-ltags-{suffix}"
        pol_name = f"enc-ltags-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/{coll_name}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        client.create_security_policy(name=pol_name, type="encryption", policy=policy)
        try:
            resp = client.create_collection(
                name=coll_name,
                type="SEARCH",
                tags=[{"key": "team", "value": "platform"}],
            )
            arn = resp["createCollectionDetail"]["arn"]
            coll_id = resp["createCollectionDetail"]["id"]
            try:
                tags_resp = client.list_tags_for_resource(resourceArn=arn)
                assert "tags" in tags_resp
                assert any(t["key"] == "team" for t in tags_resp["tags"])
            finally:
                client.delete_collection(id=coll_id)
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="encryption")
            except ClientError:
                pass


class TestOpenSearchServerlessAccessPolicies:
    """Tests for access policy operations."""

    @pytest.fixture
    def client(self):
        return make_client("opensearchserverless")

    def test_create_access_policy(self, client):
        """CreateAccessPolicy creates a data access policy."""
        suffix = _uid()
        name = f"ap-{suffix}"
        policy = json.dumps(
            [
                {
                    "Rules": [
                        {
                            "ResourceType": "collection",
                            "Resource": [f"collection/test-{suffix}"],
                            "Permission": ["aoss:CreateCollectionItems"],
                        }
                    ],
                    "Principal": ["arn:aws:iam::123456789012:root"],
                }
            ]
        )
        try:
            resp = client.create_access_policy(name=name, type="data", policy=policy)
            detail = resp["accessPolicyDetail"]
            assert detail["name"] == name
            assert detail["type"] == "data"
            assert "policyVersion" in detail
        finally:
            try:
                client.delete_access_policy(name=name, type="data")
            except ClientError:
                pass

    def test_get_access_policy(self, client):
        """GetAccessPolicy retrieves a policy by name and type."""
        suffix = _uid()
        name = f"ap-get-{suffix}"
        policy = json.dumps(
            [
                {
                    "Rules": [
                        {
                            "ResourceType": "collection",
                            "Resource": [f"collection/test-{suffix}"],
                            "Permission": ["aoss:*"],
                        }
                    ],
                    "Principal": ["arn:aws:iam::123456789012:root"],
                }
            ]
        )
        client.create_access_policy(name=name, type="data", policy=policy)
        try:
            resp = client.get_access_policy(name=name, type="data")
            detail = resp["accessPolicyDetail"]
            assert detail["name"] == name
            assert "policy" in detail
        finally:
            client.delete_access_policy(name=name, type="data")

    def test_list_access_policies(self, client):
        """ListAccessPolicies returns summaries for data type."""
        resp = client.list_access_policies(type="data")
        assert "accessPolicySummaries" in resp
        assert isinstance(resp["accessPolicySummaries"], list)

    def test_update_access_policy(self, client):
        """UpdateAccessPolicy modifies a data access policy's description."""
        suffix = _uid()
        name = f"ap-upd-{suffix}"
        policy = json.dumps(
            [
                {
                    "Rules": [
                        {
                            "ResourceType": "collection",
                            "Resource": [f"collection/test-{suffix}"],
                            "Permission": ["aoss:*"],
                        }
                    ],
                    "Principal": ["arn:aws:iam::123456789012:root"],
                }
            ]
        )
        create_resp = client.create_access_policy(name=name, type="data", policy=policy)
        version = create_resp["accessPolicyDetail"]["policyVersion"]
        try:
            resp = client.update_access_policy(
                name=name,
                type="data",
                policyVersion=version,
                description="Updated access policy",
            )
            detail = resp["accessPolicyDetail"]
            assert detail["name"] == name
            assert detail["description"] == "Updated access policy"
        finally:
            client.delete_access_policy(name=name, type="data")

    def test_delete_access_policy_nonexistent(self, client):
        """DeleteAccessPolicy for nonexistent policy raises error."""
        with pytest.raises(ClientError) as exc:
            client.delete_access_policy(name="nonexistent", type="data")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_access_policy_nonexistent(self, client):
        """GetAccessPolicy for nonexistent policy raises error."""
        with pytest.raises(ClientError) as exc:
            client.get_access_policy(name="nonexistent", type="data")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestOpenSearchServerlessLifecyclePolicies:
    """Tests for lifecycle policy operations."""

    @pytest.fixture
    def client(self):
        return make_client("opensearchserverless")

    def test_create_lifecycle_policy(self, client):
        """CreateLifecyclePolicy creates a retention policy."""
        suffix = _uid()
        name = f"lp-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/test-{suffix}"],
                        "MinIndexRetention": "15d",
                    }
                ]
            }
        )
        try:
            resp = client.create_lifecycle_policy(name=name, type="retention", policy=policy)
            detail = resp["lifecyclePolicyDetail"]
            assert detail["name"] == name
            assert detail["type"] == "retention"
        finally:
            try:
                client.delete_lifecycle_policy(name=name, type="retention")
            except ClientError:
                pass

    def test_list_lifecycle_policies(self, client):
        """ListLifecyclePolicies returns summaries."""
        resp = client.list_lifecycle_policies(type="retention")
        assert "lifecyclePolicySummaries" in resp
        assert isinstance(resp["lifecyclePolicySummaries"], list)

    def test_batch_get_lifecycle_policy(self, client):
        """BatchGetLifecyclePolicy retrieves lifecycle policies by identifiers."""
        suffix = _uid()
        name = f"lp-bg-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/test-{suffix}"],
                        "MinIndexRetention": "15d",
                    }
                ]
            }
        )
        client.create_lifecycle_policy(name=name, type="retention", policy=policy)
        try:
            resp = client.batch_get_lifecycle_policy(
                identifiers=[{"type": "retention", "name": name}]
            )
            assert "lifecyclePolicyDetails" in resp
            assert len(resp["lifecyclePolicyDetails"]) >= 1
            found = resp["lifecyclePolicyDetails"][0]
            assert found["name"] == name
            assert found["type"] == "retention"
        finally:
            client.delete_lifecycle_policy(name=name, type="retention")

    def test_batch_get_lifecycle_policy_nonexistent(self, client):
        """BatchGetLifecyclePolicy returns error details for nonexistent policy."""
        resp = client.batch_get_lifecycle_policy(
            identifiers=[{"type": "retention", "name": "nonexistent-lp"}]
        )
        assert "lifecyclePolicyErrorDetails" in resp
        assert len(resp["lifecyclePolicyErrorDetails"]) >= 1

    def test_batch_get_effective_lifecycle_policy(self, client):
        """BatchGetEffectiveLifecyclePolicy returns effective policies or errors."""
        resp = client.batch_get_effective_lifecycle_policy(
            resourceIdentifiers=[{"type": "retention", "resource": "collection/test"}]
        )
        assert "effectiveLifecyclePolicyDetails" in resp
        assert "effectiveLifecyclePolicyErrorDetails" in resp

    def test_delete_lifecycle_policy_nonexistent(self, client):
        """DeleteLifecyclePolicy for nonexistent policy raises error."""
        with pytest.raises(ClientError) as exc:
            client.delete_lifecycle_policy(name="nonexistent", type="retention")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestOpenSearchServerlessSecurityConfigs:
    """Tests for security config operations."""

    @pytest.fixture
    def client(self):
        return make_client("opensearchserverless")

    def test_create_security_config(self, client):
        """CreateSecurityConfig creates a SAML config."""
        suffix = _uid()
        name = f"sc-{suffix}"
        resp = client.create_security_config(
            name=name,
            type="saml",
            samlOptions={"metadata": "<xml>saml</xml>"},
            description="test config",
        )
        detail = resp["securityConfigDetail"]
        assert "id" in detail
        assert detail["type"] == "saml"
        client.delete_security_config(id=detail["id"])

    def test_get_security_config(self, client):
        """GetSecurityConfig retrieves a created SAML config by ID."""
        suffix = _uid()
        name = f"sc-get-{suffix}"
        resp = client.create_security_config(
            name=name,
            type="saml",
            samlOptions={"metadata": "<xml>saml</xml>"},
            description="test config for get",
        )
        sc_id = resp["securityConfigDetail"]["id"]
        try:
            got = client.get_security_config(id=sc_id)
            detail = got["securityConfigDetail"]
            assert detail["id"] == sc_id
            assert detail["type"] == "saml"
            assert "configVersion" in detail
        finally:
            client.delete_security_config(id=sc_id)

    def test_update_security_config(self, client):
        """UpdateSecurityConfig modifies a SAML config's description."""
        suffix = _uid()
        name = f"sc-upd-{suffix}"
        resp = client.create_security_config(
            name=name,
            type="saml",
            samlOptions={"metadata": "<xml>saml</xml>"},
            description="original",
        )
        detail = resp["securityConfigDetail"]
        sc_id = detail["id"]
        version = detail["configVersion"]
        try:
            upd = client.update_security_config(
                id=sc_id,
                configVersion=version,
                description="updated description",
            )
            upd_detail = upd["securityConfigDetail"]
            assert upd_detail["id"] == sc_id
            assert upd_detail["description"] == "updated description"
        finally:
            client.delete_security_config(id=sc_id)

    def test_get_security_config_nonexistent(self, client):
        """GetSecurityConfig for nonexistent ID raises error."""
        with pytest.raises(ClientError) as exc:
            client.get_security_config(id="nonexistent-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_security_configs(self, client):
        """ListSecurityConfigs returns summaries."""
        resp = client.list_security_configs(type="saml")
        assert "securityConfigSummaries" in resp
        assert isinstance(resp["securityConfigSummaries"], list)

    def test_delete_security_config_nonexistent(self, client):
        """DeleteSecurityConfig for nonexistent ID raises error."""
        with pytest.raises(ClientError) as exc:
            client.delete_security_config(id="nonexistent-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestOpenSearchServerlessVpcEndpoints:
    """Tests for VPC endpoint operations."""

    @pytest.fixture
    def client(self):
        return make_client("opensearchserverless")

    def test_list_vpc_endpoints(self, client):
        """ListVpcEndpoints returns endpoint summaries."""
        resp = client.list_vpc_endpoints()
        assert "vpcEndpointSummaries" in resp
        assert isinstance(resp["vpcEndpointSummaries"], list)

    def test_batch_get_vpc_endpoint(self, client):
        """BatchGetVpcEndpoint retrieves VPC endpoints by IDs."""
        suffix = _uid()
        create_resp = client.create_vpc_endpoint(
            name=f"test-bgvpce-{suffix}",
            vpcId=f"vpc-{suffix}",
            subnetIds=[f"subnet-{suffix}"],
        )
        vpce_id = create_resp["createVpcEndpointDetail"]["id"]
        try:
            resp = client.batch_get_vpc_endpoint(ids=[vpce_id])
            assert "vpcEndpointDetails" in resp
            found_ids = [ep["id"] for ep in resp["vpcEndpointDetails"]]
            assert vpce_id in found_ids
        finally:
            try:
                client.delete_vpc_endpoint(id=vpce_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_batch_get_vpc_endpoint_nonexistent(self, client):
        """BatchGetVpcEndpoint returns error details for nonexistent endpoints."""
        resp = client.batch_get_vpc_endpoint(ids=["vpce-nonexistent123"])
        assert "vpcEndpointErrorDetails" in resp
        assert len(resp["vpcEndpointErrorDetails"]) >= 1

    def test_delete_vpc_endpoint_nonexistent(self, client):
        """DeleteVpcEndpoint for nonexistent ID raises error."""
        with pytest.raises(ClientError) as exc:
            client.delete_vpc_endpoint(id="vpce-nonexistent12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestOpenSearchServerlessAccountSettings:
    """Tests for account settings and policies stats."""

    @pytest.fixture
    def client(self):
        return make_client("opensearchserverless")

    def test_get_account_settings(self, client):
        """GetAccountSettings returns capacity limits."""
        resp = client.get_account_settings()
        assert "accountSettingsDetail" in resp

    def test_get_policies_stats(self, client):
        """GetPoliciesStats returns policy counts."""
        resp = client.get_policies_stats()
        assert "TotalPolicyCount" in resp

    def test_delete_security_policy(self, client):
        """DeleteSecurityPolicy removes an encryption policy."""
        suffix = _uid()
        pol_name = f"enc-del-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/test-del-{suffix}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        client.create_security_policy(name=pol_name, type="encryption", policy=policy)
        resp = client.delete_security_policy(name=pol_name, type="encryption")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_collection(self, client):
        """UpdateCollection modifies a collection's description."""
        suffix = _uid()
        coll_name = f"test-upd-{suffix}"
        pol_name = f"enc-upd-{suffix}"
        policy = json.dumps(
            {
                "Rules": [
                    {
                        "ResourceType": "collection",
                        "Resource": [f"collection/{coll_name}"],
                    }
                ],
                "AWSOwnedKey": True,
            }
        )
        client.create_security_policy(name=pol_name, type="encryption", policy=policy)
        try:
            resp = client.create_collection(name=coll_name, type="SEARCH")
            coll_id = resp["createCollectionDetail"]["id"]
            try:
                upd = client.update_collection(id=coll_id, description="updated desc")
                assert "updateCollectionDetail" in upd
            finally:
                client.delete_collection(id=coll_id)
        finally:
            try:
                client.delete_security_policy(name=pol_name, type="encryption")
            except ClientError:
                pass


class TestOpenSearchServerlessUpdates:
    """Tests for update operations on OpenSearch Serverless."""

    @pytest.fixture
    def client(self):
        return make_client("opensearchserverless")

    def test_update_account_settings(self, client):
        """UpdateAccountSettings modifies capacity limits."""
        resp = client.update_account_settings(
            capacityLimits={"maxIndexingCapacityInOCU": 10, "maxSearchCapacityInOCU": 10}
        )
        assert "accountSettingsDetail" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_vpc_endpoint(self, client):
        """UpdateVpcEndpoint with a created VPC endpoint updates it."""
        suffix = _uid()
        create_resp = client.create_vpc_endpoint(
            name=f"test-uvpce-{suffix}",
            vpcId=f"vpc-{suffix}",
            subnetIds=[f"subnet-{suffix}"],
        )
        vpce_id = create_resp["createVpcEndpointDetail"]["id"]
        try:
            resp = client.update_vpc_endpoint(
                id=vpce_id,
                addSubnetIds=[f"subnet-new-{suffix}"],
            )
            assert "UpdateVpcEndpointDetail" in resp
        finally:
            try:
                client.delete_vpc_endpoint(id=vpce_id)
            except ClientError:
                pass
