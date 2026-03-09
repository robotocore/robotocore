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
