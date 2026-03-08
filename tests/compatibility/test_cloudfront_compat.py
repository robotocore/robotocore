"""CloudFront compatibility tests."""

import uuid

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from tests.compatibility.conftest import make_client


@pytest.fixture
def cf():
    return make_client("cloudfront")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _dist_config(comment="test"):
    """Minimal distribution config using a custom origin (avoids S3 bucket validation)."""
    return {
        "CallerReference": str(uuid.uuid4()),
        "Origins": {
            "Quantity": 1,
            "Items": [
                {
                    "Id": "origin1",
                    "DomainName": "example.com",
                    "CustomOriginConfig": {
                        "HTTPPort": 80,
                        "HTTPSPort": 443,
                        "OriginProtocolPolicy": "http-only",
                    },
                }
            ],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "origin1",
            "ViewerProtocolPolicy": "allow-all",
            "ForwardedValues": {"QueryString": False, "Cookies": {"Forward": "none"}},
        },
        "Comment": comment,
        "Enabled": True,
    }


def _generate_public_key_pem():
    """Generate an RSA public key in PEM format."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )


class TestCloudFrontDistributionOperations:
    def test_create_distribution(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("create-test"))
        dist = resp["Distribution"]
        assert "Id" in dist
        assert "ARN" in dist
        assert dist["DistributionConfig"]["Comment"] == "create-test"

    def test_get_distribution(self, cf):
        create_resp = cf.create_distribution(DistributionConfig=_dist_config("get-test"))
        dist_id = create_resp["Distribution"]["Id"]

        get_resp = cf.get_distribution(Id=dist_id)
        assert get_resp["Distribution"]["Id"] == dist_id
        assert "ETag" in get_resp

    def test_list_distributions(self, cf):
        cf.create_distribution(DistributionConfig=_dist_config("list-test"))

        resp = cf.list_distributions()
        dist_list = resp["DistributionList"]
        assert "Items" in dist_list
        assert len(dist_list["Items"]) >= 1

    def test_update_distribution(self, cf):
        create_resp = cf.create_distribution(DistributionConfig=_dist_config("update-test"))
        dist_id = create_resp["Distribution"]["Id"]

        get_resp = cf.get_distribution(Id=dist_id)
        etag = get_resp["ETag"]
        config = get_resp["Distribution"]["DistributionConfig"]
        config["Comment"] = "updated-comment"

        update_resp = cf.update_distribution(DistributionConfig=config, Id=dist_id, IfMatch=etag)
        assert update_resp["Distribution"]["DistributionConfig"]["Comment"] == "updated-comment"

    def test_list_tags_for_resource(self, cf):
        create_resp = cf.create_distribution(DistributionConfig=_dist_config("tags-test"))
        arn = create_resp["Distribution"]["ARN"]

        resp = cf.list_tags_for_resource(Resource=arn)
        assert "Tags" in resp
        assert "Items" in resp["Tags"]

    def test_tag_resource(self, cf):
        create_resp = cf.create_distribution(DistributionConfig=_dist_config("tag-test"))
        arn = create_resp["Distribution"]["ARN"]

        cf.tag_resource(
            Resource=arn,
            Tags={"Items": [{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "infra"}]},
        )

        resp = cf.list_tags_for_resource(Resource=arn)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]["Items"]}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "infra"

    def test_untag_resource(self, cf):
        create_resp = cf.create_distribution(DistributionConfig=_dist_config("untag-test"))
        arn = create_resp["Distribution"]["ARN"]

        cf.tag_resource(
            Resource=arn,
            Tags={"Items": [{"Key": "remove-me", "Value": "yes"}, {"Key": "keep", "Value": "yes"}]},
        )
        cf.untag_resource(Resource=arn, TagKeys={"Items": ["remove-me"]})

        resp = cf.list_tags_for_resource(Resource=arn)
        keys = [t["Key"] for t in resp["Tags"]["Items"]]
        assert "remove-me" not in keys
        assert "keep" in keys

    def test_delete_distribution(self, cf):
        config = _dist_config("delete-test")
        config["Enabled"] = True
        create_resp = cf.create_distribution(DistributionConfig=config)
        dist_id = create_resp["Distribution"]["Id"]

        # Disable before deleting
        get_resp = cf.get_distribution(Id=dist_id)
        etag = get_resp["ETag"]
        upd_config = get_resp["Distribution"]["DistributionConfig"]
        upd_config["Enabled"] = False
        update_resp = cf.update_distribution(
            DistributionConfig=upd_config, Id=dist_id, IfMatch=etag
        )
        new_etag = update_resp["ETag"]

        # Now delete
        cf.delete_distribution(Id=dist_id, IfMatch=new_etag)

        # Verify it no longer appears in list (or raises on get)
        listed = cf.list_distributions()
        listed_ids = [d["Id"] for d in listed["DistributionList"].get("Items", [])]
        assert dist_id not in listed_ids


class TestCloudFrontOriginAccessControl:
    def _create_oac(self, cf):
        name = _unique("oac")
        resp = cf.create_origin_access_control(
            OriginAccessControlConfig={
                "Name": name,
                "Description": "test oac",
                "SigningProtocol": "sigv4",
                "SigningBehavior": "always",
                "OriginAccessControlOriginType": "s3",
            }
        )
        return resp["OriginAccessControl"]["Id"], name

    def test_create_origin_access_control(self, cf):
        oac_id, name = self._create_oac(cf)
        assert oac_id is not None

    def test_get_origin_access_control(self, cf):
        oac_id, name = self._create_oac(cf)
        resp = cf.get_origin_access_control(Id=oac_id)
        oac = resp["OriginAccessControl"]
        assert oac["Id"] == oac_id
        config = oac["OriginAccessControlConfig"]
        assert config["Name"] == name
        assert config["SigningProtocol"] == "sigv4"

    def test_list_origin_access_controls(self, cf):
        oac_id, _ = self._create_oac(cf)
        resp = cf.list_origin_access_controls()
        oac_list = resp["OriginAccessControlList"]
        assert "Items" in oac_list
        ids = [item["Id"] for item in oac_list["Items"]]
        assert oac_id in ids

    def test_delete_origin_access_control(self, cf):
        oac_id, _ = self._create_oac(cf)
        get_resp = cf.get_origin_access_control(Id=oac_id)
        etag = get_resp.get("ETag", "")
        cf.delete_origin_access_control(Id=oac_id, IfMatch=etag)

        # Verify deleted — should not appear in list
        resp = cf.list_origin_access_controls()
        items = resp["OriginAccessControlList"].get("Items", [])
        ids = [item["Id"] for item in items] if items else []
        assert oac_id not in ids


class TestCloudFrontPublicKeys:
    def _create_public_key(self, cf):
        name = _unique("pk")
        pub_pem = _generate_public_key_pem()
        resp = cf.create_public_key(
            PublicKeyConfig={
                "CallerReference": str(uuid.uuid4()),
                "Name": name,
                "EncodedKey": pub_pem,
            }
        )
        pk_id = resp["PublicKey"]["Id"]
        return pk_id, name

    def test_create_public_key(self, cf):
        pk_id, name = self._create_public_key(cf)
        assert pk_id is not None

    def test_get_public_key(self, cf):
        pk_id, name = self._create_public_key(cf)
        resp = cf.get_public_key(Id=pk_id)
        pk = resp["PublicKey"]
        assert pk["Id"] == pk_id
        assert pk["PublicKeyConfig"]["Name"] == name

    def test_list_public_keys(self, cf):
        pk_id, _ = self._create_public_key(cf)
        resp = cf.list_public_keys()
        pk_list = resp["PublicKeyList"]
        assert pk_list["Quantity"] >= 1
        ids = [item["Id"] for item in pk_list["Items"]]
        assert pk_id in ids

    def test_delete_public_key(self, cf):
        pk_id, _ = self._create_public_key(cf)
        get_resp = cf.get_public_key(Id=pk_id)
        etag = get_resp.get("ETag", "")
        cf.delete_public_key(Id=pk_id, IfMatch=etag)

        # Verify deleted
        resp = cf.list_public_keys()
        items = resp["PublicKeyList"].get("Items", [])
        ids = [item["Id"] for item in items] if items else []
        assert pk_id not in ids


class TestCloudFrontInvalidations:
    def _create_dist(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("invalidation-test"))
        return resp["Distribution"]["Id"]

    def test_create_invalidation(self, cf):
        dist_id = self._create_dist(cf)
        resp = cf.create_invalidation(
            DistributionId=dist_id,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/index.html"]},
                "CallerReference": str(uuid.uuid4()),
            },
        )
        inv = resp["Invalidation"]
        assert "Id" in inv
        assert inv["InvalidationBatch"]["Paths"]["Items"] == ["/index.html"]

    def test_get_invalidation(self, cf):
        dist_id = self._create_dist(cf)
        create_resp = cf.create_invalidation(
            DistributionId=dist_id,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/assets/*"]},
                "CallerReference": str(uuid.uuid4()),
            },
        )
        inv_id = create_resp["Invalidation"]["Id"]

        get_resp = cf.get_invalidation(DistributionId=dist_id, Id=inv_id)
        inv = get_resp["Invalidation"]
        assert inv["Id"] == inv_id
        assert inv["InvalidationBatch"]["Paths"]["Items"] == ["/assets/*"]

    def test_list_invalidations(self, cf):
        dist_id = self._create_dist(cf)
        cf.create_invalidation(
            DistributionId=dist_id,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/page.html"]},
                "CallerReference": str(uuid.uuid4()),
            },
        )

        resp = cf.list_invalidations(DistributionId=dist_id)
        inv_list = resp["InvalidationList"]
        assert "Items" in inv_list
        assert inv_list["Quantity"] >= 1


class TestCloudFrontDistributionConfig:
    def test_get_distribution_config(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("config-test"))
        dist_id = resp["Distribution"]["Id"]

        config_resp = cf.get_distribution_config(Id=dist_id)
        assert "DistributionConfig" in config_resp
        assert config_resp["DistributionConfig"]["Comment"] == "config-test"
        assert "ETag" in config_resp


class TestCloudFrontOriginAccessControlUpdate:
    def test_update_origin_access_control(self, cf):
        name = _unique("oac")
        resp = cf.create_origin_access_control(
            OriginAccessControlConfig={
                "Name": name,
                "Description": "original",
                "SigningProtocol": "sigv4",
                "SigningBehavior": "always",
                "OriginAccessControlOriginType": "s3",
            }
        )
        oac_id = resp["OriginAccessControl"]["Id"]
        get_resp = cf.get_origin_access_control(Id=oac_id)
        etag = get_resp["ETag"]

        update_resp = cf.update_origin_access_control(
            Id=oac_id,
            IfMatch=etag,
            OriginAccessControlConfig={
                "Name": name,
                "Description": "updated",
                "SigningProtocol": "sigv4",
                "SigningBehavior": "always",
                "OriginAccessControlOriginType": "s3",
            },
        )
        updated = update_resp["OriginAccessControl"]["OriginAccessControlConfig"]
        assert updated["Description"] == "updated"


class TestCloudfrontAutoCoverage:
    """Auto-generated coverage tests for cloudfront."""

    @pytest.fixture
    def client(self):
        return make_client("cloudfront")

    def test_list_key_groups(self, client):
        """ListKeyGroups returns a response."""
        resp = client.list_key_groups()
        assert "KeyGroupList" in resp
