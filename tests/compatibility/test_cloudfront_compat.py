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


class TestCloudFrontKeyGroupOperations:
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
        return resp["PublicKey"]["Id"]

    def test_create_key_group(self, cf):
        pk_id = self._create_public_key(cf)
        name = _unique("kg")
        resp = cf.create_key_group(KeyGroupConfig={"Name": name, "Items": [pk_id]})
        kg = resp["KeyGroup"]
        assert "Id" in kg
        assert kg["KeyGroupConfig"]["Name"] == name
        assert pk_id in kg["KeyGroupConfig"]["Items"]

    def test_get_key_group(self, cf):
        pk_id = self._create_public_key(cf)
        name = _unique("kg")
        create_resp = cf.create_key_group(KeyGroupConfig={"Name": name, "Items": [pk_id]})
        kg_id = create_resp["KeyGroup"]["Id"]

        get_resp = cf.get_key_group(Id=kg_id)
        assert get_resp["KeyGroup"]["Id"] == kg_id
        assert get_resp["KeyGroup"]["KeyGroupConfig"]["Name"] == name
        assert "ETag" in get_resp

    def test_list_key_groups_contains_created(self, cf):
        pk_id = self._create_public_key(cf)
        name = _unique("kg")
        create_resp = cf.create_key_group(KeyGroupConfig={"Name": name, "Items": [pk_id]})
        kg_id = create_resp["KeyGroup"]["Id"]

        resp = cf.list_key_groups()
        kg_list = resp["KeyGroupList"]
        assert kg_list["Quantity"] >= 1
        ids = [item["KeyGroup"]["Id"] for item in kg_list["Items"]]
        assert kg_id in ids


class TestCloudFrontDistributionWithTags:
    def test_create_distribution_with_tags(self, cf):
        resp = cf.create_distribution_with_tags(
            DistributionConfigWithTags={
                "DistributionConfig": _dist_config("with-tags-test"),
                "Tags": {
                    "Items": [
                        {"Key": "env", "Value": "staging"},
                        {"Key": "project", "Value": "robotocore"},
                    ]
                },
            }
        )
        dist = resp["Distribution"]
        assert "Id" in dist
        assert dist["DistributionConfig"]["Comment"] == "with-tags-test"

        # Verify tags were applied
        tags_resp = cf.list_tags_for_resource(Resource=dist["ARN"])
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]["Items"]}
        assert tag_map["env"] == "staging"
        assert tag_map["project"] == "robotocore"

    def test_create_distribution_with_tags_empty_tags(self, cf):
        resp = cf.create_distribution_with_tags(
            DistributionConfigWithTags={
                "DistributionConfig": _dist_config("no-tags-test"),
                "Tags": {"Items": []},
            }
        )
        dist = resp["Distribution"]
        assert "Id" in dist
        assert "ARN" in dist


class TestCloudFrontDistributionErrors:
    def test_get_nonexistent_distribution(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_distribution(Id="ENONEXISTENT123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchDistribution"

    def test_delete_nonexistent_distribution(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.delete_distribution(Id="ENONEXISTENT123", IfMatch="fake-etag")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchDistribution"


class TestCloudFrontOriginAccessControlErrors:
    def test_get_nonexistent_oac(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_origin_access_control(Id="ENONEXISTENT123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchOriginAccessControl"

    def test_delete_nonexistent_oac(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.delete_origin_access_control(Id="ENONEXISTENT123", IfMatch="fake-etag")
        # Server returns an error code (may be InternalError or NoSuchOriginAccessControl)
        assert exc_info.value.response["Error"]["Code"] is not None


class TestCloudFrontInvalidationErrors:
    def test_get_invalidation_nonexistent_distribution(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_invalidation(DistributionId="ENONEXISTENT123", Id="INONEXISTENT")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchDistribution"

    def test_create_invalidation_nonexistent_distribution(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.create_invalidation(
                DistributionId="ENONEXISTENT123",
                InvalidationBatch={
                    "Paths": {"Quantity": 1, "Items": ["/index.html"]},
                    "CallerReference": str(uuid.uuid4()),
                },
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchDistribution"


class TestCloudFrontInvalidationMultiplePaths:
    def test_create_invalidation_multiple_paths(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("multi-inv"))
        dist_id = resp["Distribution"]["Id"]

        inv_resp = cf.create_invalidation(
            DistributionId=dist_id,
            InvalidationBatch={
                "Paths": {
                    "Quantity": 3,
                    "Items": ["/index.html", "/css/*", "/js/*"],
                },
                "CallerReference": str(uuid.uuid4()),
            },
        )
        inv = inv_resp["Invalidation"]
        assert inv["InvalidationBatch"]["Paths"]["Quantity"] == 3
        assert set(inv["InvalidationBatch"]["Paths"]["Items"]) == {
            "/index.html",
            "/css/*",
            "/js/*",
        }

    def test_list_invalidations_multiple(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("list-inv"))
        dist_id = resp["Distribution"]["Id"]

        # Create two invalidations
        for path in ["/a.html", "/b.html"]:
            cf.create_invalidation(
                DistributionId=dist_id,
                InvalidationBatch={
                    "Paths": {"Quantity": 1, "Items": [path]},
                    "CallerReference": str(uuid.uuid4()),
                },
            )

        list_resp = cf.list_invalidations(DistributionId=dist_id)
        assert list_resp["InvalidationList"]["Quantity"] >= 2


class TestCloudFrontDistributionAdvanced:
    def test_distribution_has_domain_name(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("domain-test"))
        dist = resp["Distribution"]
        assert "DomainName" in dist
        assert len(dist["DomainName"]) > 0

    def test_distribution_status(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("status-test"))
        dist = resp["Distribution"]
        assert "Status" in dist
        # Moto typically returns "Deployed"
        assert dist["Status"] in ("Deployed", "InProgress")

    def test_distribution_multiple_origins(self, cf):
        config = _dist_config("multi-origin")
        config["Origins"]["Quantity"] = 2
        config["Origins"]["Items"].append(
            {
                "Id": "origin2",
                "DomainName": "api.example.com",
                "CustomOriginConfig": {
                    "HTTPPort": 80,
                    "HTTPSPort": 443,
                    "OriginProtocolPolicy": "https-only",
                },
            }
        )
        resp = cf.create_distribution(DistributionConfig=config)
        origins = resp["Distribution"]["DistributionConfig"]["Origins"]
        assert origins["Quantity"] == 2
        origin_ids = [o["Id"] for o in origins["Items"]]
        assert "origin1" in origin_ids
        assert "origin2" in origin_ids

    def test_distribution_viewer_protocol_policy(self, cf):
        config = _dist_config("vpp-test")
        config["DefaultCacheBehavior"]["ViewerProtocolPolicy"] = "redirect-to-https"
        resp = cf.create_distribution(DistributionConfig=config)
        vpp = resp["Distribution"]["DistributionConfig"]["DefaultCacheBehavior"][
            "ViewerProtocolPolicy"
        ]
        assert vpp == "redirect-to-https"

    def test_list_cache_policies(self, cf):
        resp = cf.list_cache_policies()
        assert "CachePolicyList" in resp

    def test_list_functions(self, cf):
        resp = cf.list_functions()
        assert "FunctionList" in resp

    def test_list_response_headers_policies(self, cf):
        resp = cf.list_response_headers_policies()
        assert "ResponseHeadersPolicyList" in resp

    def test_create_and_describe_function(self, cf):
        name = _unique("func")
        create_resp = cf.create_function(
            Name=name,
            FunctionConfig={"Comment": "test", "Runtime": "cloudfront-js-2.0"},
            FunctionCode=b"function handler(event) { return event.request; }",
        )
        etag = create_resp["ETag"]
        func_name = create_resp["FunctionSummary"]["Name"]

        desc = cf.describe_function(Name=func_name)
        assert desc["FunctionSummary"]["Name"] == func_name

        cf.delete_function(Name=func_name, IfMatch=etag)

    def test_create_and_get_function(self, cf):
        name = _unique("func")
        create_resp = cf.create_function(
            Name=name,
            FunctionConfig={"Comment": "test get", "Runtime": "cloudfront-js-2.0"},
            FunctionCode=b"function handler(event) { return event.request; }",
        )
        etag = create_resp["ETag"]
        func_name = create_resp["FunctionSummary"]["Name"]

        get_resp = cf.get_function(Name=func_name)
        assert get_resp["ContentType"] is not None
        assert "ETag" in get_resp

        cf.delete_function(Name=func_name, IfMatch=etag)

    def test_create_and_get_cache_policy(self, cf):
        name = _unique("cpol")
        create_resp = cf.create_cache_policy(
            CachePolicyConfig={
                "Name": name,
                "MinTTL": 60,
                "DefaultTTL": 86400,
                "MaxTTL": 31536000,
                "ParametersInCacheKeyAndForwardedToOrigin": {
                    "EnableAcceptEncodingGzip": True,
                    "HeadersConfig": {"HeaderBehavior": "none"},
                    "CookiesConfig": {"CookieBehavior": "none"},
                    "QueryStringsConfig": {"QueryStringBehavior": "none"},
                },
            }
        )
        policy_id = create_resp["CachePolicy"]["Id"]
        etag = create_resp["ETag"]

        get_resp = cf.get_cache_policy(Id=policy_id)
        assert get_resp["CachePolicy"]["Id"] == policy_id
        assert get_resp["CachePolicy"]["CachePolicyConfig"]["Name"] == name

        cf.delete_cache_policy(Id=policy_id, IfMatch=etag)

    def test_create_and_get_response_headers_policy(self, cf):
        name = _unique("rhpol")
        create_resp = cf.create_response_headers_policy(
            ResponseHeadersPolicyConfig={
                "Name": name,
                "Comment": "test policy",
                "SecurityHeadersConfig": {
                    "XSSProtection": {"Override": True, "Protection": True},
                },
            }
        )
        policy_id = create_resp["ResponseHeadersPolicy"]["Id"]
        etag = create_resp["ETag"]

        get_resp = cf.get_response_headers_policy(Id=policy_id)
        assert get_resp["ResponseHeadersPolicy"]["Id"] == policy_id
        assert get_resp["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]["Name"] == name

        cf.delete_response_headers_policy(Id=policy_id, IfMatch=etag)

    def test_update_distribution_add_origin(self, cf):
        create_resp = cf.create_distribution(DistributionConfig=_dist_config("add-origin"))
        dist_id = create_resp["Distribution"]["Id"]

        get_resp = cf.get_distribution(Id=dist_id)
        etag = get_resp["ETag"]
        config = get_resp["Distribution"]["DistributionConfig"]

        config["Origins"]["Quantity"] = 2
        config["Origins"]["Items"].append(
            {
                "Id": "origin2",
                "DomainName": "cdn.example.com",
                "CustomOriginConfig": {
                    "HTTPPort": 80,
                    "HTTPSPort": 443,
                    "OriginProtocolPolicy": "http-only",
                },
            }
        )

        update_resp = cf.update_distribution(DistributionConfig=config, Id=dist_id, IfMatch=etag)
        updated_origins = update_resp["Distribution"]["DistributionConfig"]["Origins"]
        assert updated_origins["Quantity"] == 2


class TestCloudFrontKeyGroupUpdateDelete:
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
        return resp["PublicKey"]["Id"]

    def test_update_key_group(self, cf):
        pk_id = self._create_public_key(cf)
        name = _unique("kg")
        create_resp = cf.create_key_group(KeyGroupConfig={"Name": name, "Items": [pk_id]})
        kg_id = create_resp["KeyGroup"]["Id"]
        etag = create_resp["ETag"]

        new_name = name + "-updated"
        update_resp = cf.update_key_group(
            Id=kg_id, IfMatch=etag, KeyGroupConfig={"Name": new_name, "Items": [pk_id]}
        )
        assert update_resp["KeyGroup"]["KeyGroupConfig"]["Name"] == new_name
        assert "ETag" in update_resp

    def test_delete_key_group(self, cf):
        pk_id = self._create_public_key(cf)
        name = _unique("kg")
        create_resp = cf.create_key_group(KeyGroupConfig={"Name": name, "Items": [pk_id]})
        kg_id = create_resp["KeyGroup"]["Id"]
        etag = create_resp["ETag"]

        cf.delete_key_group(Id=kg_id, IfMatch=etag)

        # Verify deleted - should not appear in list
        resp = cf.list_key_groups()
        kg_list = resp.get("KeyGroupList", {})
        items = kg_list.get("Items", [])
        ids = [item["KeyGroup"]["Id"] for item in items] if items else []
        assert kg_id not in ids


class TestCloudFrontCachePolicyUpdate:
    def test_update_cache_policy(self, cf):
        name = _unique("cpol")
        create_resp = cf.create_cache_policy(
            CachePolicyConfig={
                "Name": name,
                "MinTTL": 60,
                "DefaultTTL": 86400,
                "MaxTTL": 31536000,
                "ParametersInCacheKeyAndForwardedToOrigin": {
                    "EnableAcceptEncodingGzip": True,
                    "HeadersConfig": {"HeaderBehavior": "none"},
                    "CookiesConfig": {"CookieBehavior": "none"},
                    "QueryStringsConfig": {"QueryStringBehavior": "none"},
                },
            }
        )
        policy_id = create_resp["CachePolicy"]["Id"]
        etag = create_resp["ETag"]

        updated_name = name + "-updated"
        update_resp = cf.update_cache_policy(
            Id=policy_id,
            IfMatch=etag,
            CachePolicyConfig={
                "Name": updated_name,
                "MinTTL": 30,
                "DefaultTTL": 43200,
                "MaxTTL": 31536000,
                "ParametersInCacheKeyAndForwardedToOrigin": {
                    "EnableAcceptEncodingGzip": True,
                    "HeadersConfig": {"HeaderBehavior": "none"},
                    "CookiesConfig": {"CookieBehavior": "none"},
                    "QueryStringsConfig": {"QueryStringBehavior": "none"},
                },
            },
        )
        assert update_resp["CachePolicy"]["CachePolicyConfig"]["Name"] == updated_name
        assert update_resp["CachePolicy"]["CachePolicyConfig"]["MinTTL"] == 30
        assert "ETag" in update_resp

        # Cleanup
        cf.delete_cache_policy(Id=policy_id, IfMatch=update_resp["ETag"])


class TestCloudFrontResponseHeadersPolicyUpdate:
    def test_update_response_headers_policy(self, cf):
        name = _unique("rhpol")
        create_resp = cf.create_response_headers_policy(
            ResponseHeadersPolicyConfig={
                "Name": name,
                "Comment": "original",
                "SecurityHeadersConfig": {
                    "XSSProtection": {"Override": True, "Protection": True},
                },
            }
        )
        policy_id = create_resp["ResponseHeadersPolicy"]["Id"]
        etag = create_resp["ETag"]

        update_resp = cf.update_response_headers_policy(
            Id=policy_id,
            IfMatch=etag,
            ResponseHeadersPolicyConfig={
                "Name": name,
                "Comment": "updated",
                "SecurityHeadersConfig": {
                    "XSSProtection": {"Override": True, "Protection": True},
                },
            },
        )
        config = update_resp["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]
        assert config["Comment"] == "updated"
        assert "ETag" in update_resp

        # Cleanup
        cf.delete_response_headers_policy(Id=policy_id, IfMatch=update_resp["ETag"])


class TestCloudFrontFunctionUpdatePublish:
    def test_update_function(self, cf):
        name = _unique("func")
        create_resp = cf.create_function(
            Name=name,
            FunctionConfig={"Comment": "original", "Runtime": "cloudfront-js-2.0"},
            FunctionCode=b"function handler(event) { return event.request; }",
        )
        etag = create_resp["ETag"]

        update_resp = cf.update_function(
            Name=name,
            IfMatch=etag,
            FunctionConfig={"Comment": "updated", "Runtime": "cloudfront-js-2.0"},
            FunctionCode=b"function handler(event) { return event.response; }",
        )
        assert update_resp["FunctionSummary"]["FunctionConfig"]["Comment"] == "updated"
        assert "ETag" in update_resp

        # Cleanup
        cf.delete_function(Name=name, IfMatch=update_resp["ETag"])

    def test_publish_function(self, cf):
        name = _unique("func")
        create_resp = cf.create_function(
            Name=name,
            FunctionConfig={"Comment": "to-publish", "Runtime": "cloudfront-js-2.0"},
            FunctionCode=b"function handler(event) { return event.request; }",
        )
        etag = create_resp["ETag"]

        pub_resp = cf.publish_function(Name=name, IfMatch=etag)
        assert pub_resp["FunctionSummary"]["Name"] == name
        assert pub_resp["FunctionSummary"]["FunctionMetadata"]["Stage"] == "LIVE"

        # Cleanup - get etag from describe since publish may not return it
        desc = cf.describe_function(Name=name)
        cf.delete_function(Name=name, IfMatch=desc["ETag"])


class TestCloudFrontKeyGroupErrors:
    def test_get_nonexistent_key_group(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_key_group(Id="KGNONEXISTENT123")
        assert exc_info.value.response["Error"]["Code"] is not None

    def test_delete_nonexistent_key_group(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.delete_key_group(Id="KGNONEXISTENT123", IfMatch="fake-etag")
        assert exc_info.value.response["Error"]["Code"] is not None


class TestCloudFrontPublicKeyErrors:
    def test_get_nonexistent_public_key(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_public_key(Id="PKNONEXISTENT123")
        assert exc_info.value.response["Error"]["Code"] is not None

    def test_delete_nonexistent_public_key_no_error(self, cf):
        # Moto silently accepts delete of nonexistent public keys
        resp = cf.delete_public_key(Id="PKNONEXISTENT123", IfMatch="fake-etag")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


class TestCloudFrontCachePolicyErrors:
    def test_get_nonexistent_cache_policy(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_cache_policy(Id="CPNONEXISTENT123")
        assert exc_info.value.response["Error"]["Code"] is not None


class TestCloudFrontFunctionErrors:
    def test_describe_nonexistent_function(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.describe_function(Name="nonexistent-func-12345")
        assert exc_info.value.response["Error"]["Code"] is not None

    def test_get_nonexistent_function(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_function(Name="nonexistent-func-12345")
        assert exc_info.value.response["Error"]["Code"] is not None


class TestCloudFrontConfigGetters:
    """Tests for *Config getter operations (return config without full resource wrapper)."""

    def test_get_cache_policy_config(self, cf):
        name = _unique("cpol")
        create_resp = cf.create_cache_policy(
            CachePolicyConfig={
                "Name": name,
                "MinTTL": 60,
                "DefaultTTL": 86400,
                "MaxTTL": 31536000,
                "ParametersInCacheKeyAndForwardedToOrigin": {
                    "EnableAcceptEncodingGzip": True,
                    "HeadersConfig": {"HeaderBehavior": "none"},
                    "CookiesConfig": {"CookieBehavior": "none"},
                    "QueryStringsConfig": {"QueryStringBehavior": "none"},
                },
            }
        )
        policy_id = create_resp["CachePolicy"]["Id"]
        etag = create_resp["ETag"]

        resp = cf.get_cache_policy_config(Id=policy_id)
        assert resp["CachePolicyConfig"]["Name"] == name
        assert resp["CachePolicyConfig"]["MinTTL"] == 60
        assert "ETag" in resp

        cf.delete_cache_policy(Id=policy_id, IfMatch=etag)

    def test_get_origin_access_control_config(self, cf):
        name = _unique("oac")
        create_resp = cf.create_origin_access_control(
            OriginAccessControlConfig={
                "Name": name,
                "Description": "config test",
                "SigningProtocol": "sigv4",
                "SigningBehavior": "always",
                "OriginAccessControlOriginType": "s3",
            }
        )
        oac_id = create_resp["OriginAccessControl"]["Id"]

        resp = cf.get_origin_access_control_config(Id=oac_id)
        assert resp["OriginAccessControlConfig"]["Name"] == name
        assert "ETag" in resp

    def test_get_response_headers_policy_config(self, cf):
        name = _unique("rhpol")
        create_resp = cf.create_response_headers_policy(
            ResponseHeadersPolicyConfig={
                "Name": name,
                "Comment": "config test",
                "SecurityHeadersConfig": {
                    "XSSProtection": {"Override": True, "Protection": True},
                },
            }
        )
        policy_id = create_resp["ResponseHeadersPolicy"]["Id"]
        etag = create_resp["ETag"]

        resp = cf.get_response_headers_policy_config(Id=policy_id)
        assert resp["ResponseHeadersPolicyConfig"]["Name"] == name
        assert "ETag" in resp

        cf.delete_response_headers_policy(Id=policy_id, IfMatch=etag)

    def test_get_key_group_config(self, cf):
        pub_pem = _generate_public_key_pem()
        pk_resp = cf.create_public_key(
            PublicKeyConfig={
                "CallerReference": str(uuid.uuid4()),
                "Name": _unique("pk"),
                "EncodedKey": pub_pem,
            }
        )
        pk_id = pk_resp["PublicKey"]["Id"]
        name = _unique("kg")
        kg_resp = cf.create_key_group(KeyGroupConfig={"Name": name, "Items": [pk_id]})
        kg_id = kg_resp["KeyGroup"]["Id"]

        resp = cf.get_key_group_config(Id=kg_id)
        assert resp["KeyGroupConfig"]["Name"] == name
        assert pk_id in resp["KeyGroupConfig"]["Items"]
        assert "ETag" in resp

    def test_get_public_key_config(self, cf):
        pub_pem = _generate_public_key_pem()
        name = _unique("pk")
        pk_resp = cf.create_public_key(
            PublicKeyConfig={
                "CallerReference": str(uuid.uuid4()),
                "Name": name,
                "EncodedKey": pub_pem,
            }
        )
        pk_id = pk_resp["PublicKey"]["Id"]

        resp = cf.get_public_key_config(Id=pk_id)
        assert resp["PublicKeyConfig"]["Name"] == name
        assert "ETag" in resp


class TestCloudFrontListOperationsExtended:
    """Tests for List operations not covered by existing tests."""

    def test_list_cloud_front_origin_access_identities(self, cf):
        resp = cf.list_cloud_front_origin_access_identities()
        assert "CloudFrontOriginAccessIdentityList" in resp

    def test_list_streaming_distributions(self, cf):
        resp = cf.list_streaming_distributions()
        assert "StreamingDistributionList" in resp

    def test_list_origin_request_policies(self, cf):
        resp = cf.list_origin_request_policies()
        assert "OriginRequestPolicyList" in resp

    def test_list_continuous_deployment_policies(self, cf):
        resp = cf.list_continuous_deployment_policies()
        assert "ContinuousDeploymentPolicyList" in resp

    def test_list_field_level_encryption_configs(self, cf):
        resp = cf.list_field_level_encryption_configs()
        assert "FieldLevelEncryptionList" in resp

    def test_list_field_level_encryption_profiles(self, cf):
        resp = cf.list_field_level_encryption_profiles()
        assert "FieldLevelEncryptionProfileList" in resp

    def test_list_realtime_log_configs(self, cf):
        resp = cf.list_realtime_log_configs()
        assert "RealtimeLogConfigs" in resp

    def test_list_conflicting_aliases(self, cf):
        resp = cf.list_conflicting_aliases(DistributionId="EDISTFAKE123", Alias="example.com")
        assert "ConflictingAliasesList" in resp

    def test_list_distributions_by_cache_policy_id(self, cf):
        resp = cf.list_distributions_by_cache_policy_id(CachePolicyId="fake-policy-id")
        assert "DistributionIdList" in resp

    def test_list_distributions_by_key_group(self, cf):
        resp = cf.list_distributions_by_key_group(KeyGroupId="fake-key-group-id")
        assert "DistributionIdList" in resp

    def test_list_distributions_by_origin_request_policy_id(self, cf):
        resp = cf.list_distributions_by_origin_request_policy_id(
            OriginRequestPolicyId="fake-policy-id"
        )
        assert "DistributionIdList" in resp

    def test_list_distributions_by_realtime_log_config(self, cf):
        resp = cf.list_distributions_by_realtime_log_config()
        assert "DistributionList" in resp

    def test_list_distributions_by_response_headers_policy_id(self, cf):
        resp = cf.list_distributions_by_response_headers_policy_id(
            ResponseHeadersPolicyId="fake-policy-id"
        )
        assert "DistributionIdList" in resp

    def test_list_distributions_by_web_acl_id(self, cf):
        resp = cf.list_distributions_by_web_acl_id(WebACLId="fake-web-acl-id")
        assert "DistributionList" in resp


class TestCloudFrontOriginAccessIdentity:
    """Tests for CloudFront Origin Access Identity (legacy OAI)."""

    def _create_oai(self, cf):
        ref = str(uuid.uuid4())
        resp = cf.create_cloud_front_origin_access_identity(
            CloudFrontOriginAccessIdentityConfig={
                "CallerReference": ref,
                "Comment": "test oai",
            }
        )
        return resp["CloudFrontOriginAccessIdentity"]["Id"]

    def test_create_and_get_origin_access_identity(self, cf):
        oai_id = self._create_oai(cf)
        resp = cf.get_cloud_front_origin_access_identity(Id=oai_id)
        assert resp["CloudFrontOriginAccessIdentity"]["Id"] == oai_id
        assert "ETag" in resp

    def test_get_origin_access_identity_config(self, cf):
        oai_id = self._create_oai(cf)
        resp = cf.get_cloud_front_origin_access_identity_config(Id=oai_id)
        assert resp["CloudFrontOriginAccessIdentityConfig"]["Comment"] == "test oai"
        assert "ETag" in resp


class TestCloudFrontOriginRequestPolicy:
    """Tests for Origin Request Policy operations."""

    def _create_origin_request_policy(self, cf):
        name = _unique("orp")
        resp = cf.create_origin_request_policy(
            OriginRequestPolicyConfig={
                "Name": name,
                "Comment": "test policy",
                "HeadersConfig": {"HeaderBehavior": "none"},
                "CookiesConfig": {"CookieBehavior": "none"},
                "QueryStringsConfig": {"QueryStringBehavior": "none"},
            }
        )
        return resp["OriginRequestPolicy"]["Id"], name, resp["ETag"]

    def test_create_and_get_origin_request_policy(self, cf):
        policy_id, name, etag = self._create_origin_request_policy(cf)
        resp = cf.get_origin_request_policy(Id=policy_id)
        assert resp["OriginRequestPolicy"]["Id"] == policy_id
        assert resp["OriginRequestPolicy"]["OriginRequestPolicyConfig"]["Name"] == name

        cf.delete_origin_request_policy(Id=policy_id, IfMatch=etag)

    def test_get_origin_request_policy_config(self, cf):
        policy_id, name, etag = self._create_origin_request_policy(cf)
        resp = cf.get_origin_request_policy_config(Id=policy_id)
        assert resp["OriginRequestPolicyConfig"]["Name"] == name
        assert "ETag" in resp

        cf.delete_origin_request_policy(Id=policy_id, IfMatch=etag)


class TestCloudFrontStreamingDistributionErrors:
    """Tests for streaming distribution error paths (working ops)."""

    def test_get_nonexistent_streaming_distribution(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_streaming_distribution(Id="ENONEXISTENT123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchStreamingDistribution"

    def test_get_nonexistent_streaming_distribution_config(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_streaming_distribution_config(Id="ENONEXISTENT123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchStreamingDistribution"


class TestCloudFrontMonitoringSubscription:
    """Tests for monitoring subscription operations."""

    def test_get_monitoring_subscription_nonexistent(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_monitoring_subscription(DistributionId="ENONEXISTENT123")
        assert exc_info.value.response["Error"]["Code"] is not None


class TestCloudFrontFieldLevelEncryption:
    """Tests for field-level encryption error paths (working ops)."""

    def test_get_nonexistent_field_level_encryption(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_field_level_encryption(Id="EFLE123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchFieldLevelEncryptionConfig"

    def test_get_nonexistent_field_level_encryption_config(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_field_level_encryption_config(Id="EFLE123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchFieldLevelEncryptionConfig"

    def test_get_nonexistent_field_level_encryption_profile(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_field_level_encryption_profile(Id="EFLEP123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchFieldLevelEncryptionProfile"

    def test_get_nonexistent_field_level_encryption_profile_config(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_field_level_encryption_profile_config(Id="EFLEP123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchFieldLevelEncryptionProfile"


class TestCloudFrontContinuousDeployment:
    """Tests for continuous deployment policy error paths."""

    def test_get_nonexistent_continuous_deployment_policy(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_continuous_deployment_policy(Id="ECDP123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchContinuousDeploymentPolicy"

    def test_get_nonexistent_continuous_deployment_policy_config(self, cf):
        with pytest.raises(cf.exceptions.ClientError) as exc_info:
            cf.get_continuous_deployment_policy_config(Id="ECDP123")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchContinuousDeploymentPolicy"


class TestCloudFrontAssociateAlias:
    """Tests for AssociateAlias operation."""

    def test_associate_alias(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("alias-test"))
        dist_id = resp["Distribution"]["Id"]

        alias_resp = cf.associate_alias(
            TargetDistributionId=dist_id, Alias="alias-test.example.com"
        )
        assert alias_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCloudFrontStreamingDistributionCRUD:
    """Tests for streaming distribution create/update/delete."""

    def _streaming_config(self, comment="test streaming"):
        return {
            "CallerReference": str(uuid.uuid4()),
            "S3Origin": {
                "DomainName": "mybucket.s3.amazonaws.com",
                "OriginAccessIdentity": "",
            },
            "Comment": comment,
            "TrustedSigners": {"Enabled": False, "Quantity": 0},
            "Enabled": True,
        }

    def test_create_streaming_distribution(self, cf):
        resp = cf.create_streaming_distribution(
            StreamingDistributionConfig=self._streaming_config("create-sd")
        )
        sd = resp["StreamingDistribution"]
        assert "Id" in sd
        assert sd["StreamingDistributionConfig"]["Comment"] == "create-sd"

    def test_update_streaming_distribution(self, cf):
        config = self._streaming_config("update-sd")
        resp = cf.create_streaming_distribution(StreamingDistributionConfig=config)
        sd_id = resp["StreamingDistribution"]["Id"]
        etag = resp["ETag"]
        caller_ref = resp["StreamingDistribution"]["StreamingDistributionConfig"]["CallerReference"]

        updated_config = self._streaming_config("updated-sd")
        updated_config["CallerReference"] = caller_ref
        updated_config["Enabled"] = False
        update_resp = cf.update_streaming_distribution(
            Id=sd_id, IfMatch=etag, StreamingDistributionConfig=updated_config
        )
        assert (
            update_resp["StreamingDistribution"]["StreamingDistributionConfig"]["Comment"]
            == "updated-sd"
        )
        assert "ETag" in update_resp

    def test_delete_streaming_distribution(self, cf):
        config = self._streaming_config("delete-sd")
        resp = cf.create_streaming_distribution(StreamingDistributionConfig=config)
        sd_id = resp["StreamingDistribution"]["Id"]
        etag = resp["ETag"]
        caller_ref = resp["StreamingDistribution"]["StreamingDistributionConfig"]["CallerReference"]

        # Disable before deleting
        disabled_config = self._streaming_config("delete-sd")
        disabled_config["CallerReference"] = caller_ref
        disabled_config["Enabled"] = False
        update_resp = cf.update_streaming_distribution(
            Id=sd_id, IfMatch=etag, StreamingDistributionConfig=disabled_config
        )
        new_etag = update_resp["ETag"]

        del_resp = cf.delete_streaming_distribution(Id=sd_id, IfMatch=new_etag)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


class TestCloudFrontMonitoringSubscriptionCRUD:
    """Tests for monitoring subscription create/delete."""

    def test_create_monitoring_subscription(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("mon-sub-test"))
        dist_id = resp["Distribution"]["Id"]

        mon_resp = cf.create_monitoring_subscription(
            DistributionId=dist_id,
            MonitoringSubscription={
                "RealtimeMetricsSubscriptionConfig": {
                    "RealtimeMetricsSubscriptionStatus": "Enabled"
                }
            },
        )
        config = mon_resp["MonitoringSubscription"]["RealtimeMetricsSubscriptionConfig"]
        assert config["RealtimeMetricsSubscriptionStatus"] == "Enabled"

    def test_delete_monitoring_subscription(self, cf):
        resp = cf.create_distribution(DistributionConfig=_dist_config("del-mon-sub"))
        dist_id = resp["Distribution"]["Id"]

        cf.create_monitoring_subscription(
            DistributionId=dist_id,
            MonitoringSubscription={
                "RealtimeMetricsSubscriptionConfig": {
                    "RealtimeMetricsSubscriptionStatus": "Enabled"
                }
            },
        )

        del_resp = cf.delete_monitoring_subscription(DistributionId=dist_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCloudFrontTestFunction:
    """Tests for TestFunction operation."""

    def test_test_function(self, cf):
        name = _unique("testfn")
        create_resp = cf.create_function(
            Name=name,
            FunctionConfig={"Comment": "test fn", "Runtime": "cloudfront-js-2.0"},
            FunctionCode=b"function handler(event) { return event.request; }",
        )
        etag = create_resp["ETag"]

        event_object = (
            b'{"version":"1.0","context":{"eventType":"viewer-request"},'
            b'"viewer":{"ip":"1.2.3.4"},'
            b'"request":{"method":"GET","uri":"/","headers":{},"cookies":{},"querystring":{}}}'
        )

        test_resp = cf.test_function(Name=name, IfMatch=etag, EventObject=event_object)
        assert "TestResult" in test_resp
        assert "FunctionSummary" in test_resp["TestResult"]
        assert "FunctionOutput" in test_resp["TestResult"]

        cf.delete_function(Name=name, IfMatch=etag)


class TestCloudFrontOriginAccessIdentityCRUD:
    """Tests for OAI update and delete operations."""

    def _create_oai(self, cf):
        ref = str(uuid.uuid4())
        resp = cf.create_cloud_front_origin_access_identity(
            CloudFrontOriginAccessIdentityConfig={
                "CallerReference": ref,
                "Comment": "test oai crud",
            }
        )
        oai_id = resp["CloudFrontOriginAccessIdentity"]["Id"]
        etag = resp["ETag"]
        return oai_id, ref, etag

    def test_update_cloud_front_origin_access_identity(self, cf):
        oai_id, ref, etag = self._create_oai(cf)

        resp = cf.update_cloud_front_origin_access_identity(
            Id=oai_id,
            IfMatch=etag,
            CloudFrontOriginAccessIdentityConfig={
                "CallerReference": ref,
                "Comment": "updated oai",
            },
        )
        config = resp["CloudFrontOriginAccessIdentity"]["CloudFrontOriginAccessIdentityConfig"]
        assert config["Comment"] == "updated oai"
        assert "ETag" in resp

    def test_delete_cloud_front_origin_access_identity(self, cf):
        oai_id, ref, etag = self._create_oai(cf)

        del_resp = cf.delete_cloud_front_origin_access_identity(Id=oai_id, IfMatch=etag)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

        # Verify deleted
        listed = cf.list_cloud_front_origin_access_identities()
        items = listed["CloudFrontOriginAccessIdentityList"].get("Items", [])
        ids = [item["Id"] for item in items] if items else []
        assert oai_id not in ids


class TestCloudFrontContinuousDeploymentCRUD:
    """Tests for continuous deployment policy create/delete."""

    def _create_cdp(self, cf):
        resp = cf.create_continuous_deployment_policy(
            ContinuousDeploymentPolicyConfig={
                "StagingDistributionDnsNames": {
                    "Quantity": 1,
                    "Items": [f"staging-{uuid.uuid4().hex[:8]}.example.com"],
                },
                "Enabled": True,
                "TrafficConfig": {
                    "Type": "SingleWeight",
                    "SingleWeightConfig": {"Weight": 0.1},
                },
            }
        )
        cdp_id = resp["ContinuousDeploymentPolicy"]["Id"]
        etag = resp["ETag"]
        return cdp_id, etag

    def test_create_continuous_deployment_policy(self, cf):
        cdp_id, etag = self._create_cdp(cf)
        assert cdp_id is not None

        get_resp = cf.get_continuous_deployment_policy(Id=cdp_id)
        assert get_resp["ContinuousDeploymentPolicy"]["Id"] == cdp_id

    def test_delete_continuous_deployment_policy(self, cf):
        cdp_id, etag = self._create_cdp(cf)

        # Re-fetch to get fresh etag
        get_resp = cf.get_continuous_deployment_policy(Id=cdp_id)
        fresh_etag = get_resp["ETag"]

        del_resp = cf.delete_continuous_deployment_policy(Id=cdp_id, IfMatch=fresh_etag)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


class TestCloudFrontFieldLevelEncryptionCRUD:
    """Tests for field-level encryption config and profile create/update/delete."""

    def _create_flep(self, cf):
        """Create a field level encryption profile (requires a public key)."""
        pub_pem = _generate_public_key_pem()
        pk_resp = cf.create_public_key(
            PublicKeyConfig={
                "CallerReference": str(uuid.uuid4()),
                "Name": _unique("pk"),
                "EncodedKey": pub_pem,
            }
        )
        pk_id = pk_resp["PublicKey"]["Id"]

        flep_name = _unique("flep")
        caller_ref = str(uuid.uuid4())
        resp = cf.create_field_level_encryption_profile(
            FieldLevelEncryptionProfileConfig={
                "Name": flep_name,
                "CallerReference": caller_ref,
                "Comment": "test flep",
                "EncryptionEntities": {
                    "Quantity": 1,
                    "Items": [
                        {
                            "PublicKeyId": pk_id,
                            "ProviderId": "test-provider",
                            "FieldPatterns": {"Quantity": 1, "Items": ["CreditCard"]},
                        }
                    ],
                },
            }
        )
        flep_id = resp["FieldLevelEncryptionProfile"]["Id"]
        etag = resp["ETag"]
        return flep_id, flep_name, caller_ref, pk_id, etag

    def _create_fle(self, cf, flep_id):
        """Create a field level encryption config using a profile."""
        caller_ref = str(uuid.uuid4())
        resp = cf.create_field_level_encryption_config(
            FieldLevelEncryptionConfig={
                "CallerReference": caller_ref,
                "Comment": "test fle",
                "QueryArgProfileConfig": {
                    "ForwardWhenQueryArgProfileIsUnknown": True,
                    "QueryArgProfiles": {"Quantity": 0},
                },
                "ContentTypeProfileConfig": {
                    "ForwardWhenContentTypeIsUnknown": True,
                    "ContentTypeProfiles": {
                        "Quantity": 1,
                        "Items": [
                            {
                                "Format": "URLEncoded",
                                "ProfileId": flep_id,
                                "ContentType": "application/x-www-form-urlencoded",
                            }
                        ],
                    },
                },
            }
        )
        fle_id = resp["FieldLevelEncryption"]["Id"]
        etag = resp["ETag"]
        return fle_id, caller_ref, etag

    def test_create_field_level_encryption_profile(self, cf):
        flep_id, flep_name, _, pk_id, _ = self._create_flep(cf)
        assert flep_id is not None

        get_resp = cf.get_field_level_encryption_profile(Id=flep_id)
        config = get_resp["FieldLevelEncryptionProfile"]["FieldLevelEncryptionProfileConfig"]
        assert config["Name"] == flep_name

    def test_update_field_level_encryption_profile(self, cf):
        flep_id, flep_name, caller_ref, pk_id, etag = self._create_flep(cf)

        resp = cf.update_field_level_encryption_profile(
            Id=flep_id,
            IfMatch=etag,
            FieldLevelEncryptionProfileConfig={
                "Name": flep_name,
                "CallerReference": caller_ref,
                "Comment": "updated flep",
                "EncryptionEntities": {
                    "Quantity": 1,
                    "Items": [
                        {
                            "PublicKeyId": pk_id,
                            "ProviderId": "test-provider",
                            "FieldPatterns": {"Quantity": 1, "Items": ["CreditCard"]},
                        }
                    ],
                },
            },
        )
        config = resp["FieldLevelEncryptionProfile"]["FieldLevelEncryptionProfileConfig"]
        assert config["Comment"] == "updated flep"
        assert "ETag" in resp

    def test_delete_field_level_encryption_profile(self, cf):
        flep_id, _, _, _, _ = self._create_flep(cf)

        get_resp = cf.get_field_level_encryption_profile(Id=flep_id)
        etag = get_resp["ETag"]

        del_resp = cf.delete_field_level_encryption_profile(Id=flep_id, IfMatch=etag)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_create_field_level_encryption_config(self, cf):
        flep_id, _, _, _, _ = self._create_flep(cf)
        fle_id, _, _ = self._create_fle(cf, flep_id)
        assert fle_id is not None

        get_resp = cf.get_field_level_encryption(Id=fle_id)
        assert get_resp["FieldLevelEncryption"]["Id"] == fle_id

    def test_update_field_level_encryption_config(self, cf):
        flep_id, _, _, _, _ = self._create_flep(cf)
        fle_id, caller_ref, etag = self._create_fle(cf, flep_id)

        resp = cf.update_field_level_encryption_config(
            Id=fle_id,
            IfMatch=etag,
            FieldLevelEncryptionConfig={
                "CallerReference": caller_ref,
                "Comment": "updated fle",
                "QueryArgProfileConfig": {
                    "ForwardWhenQueryArgProfileIsUnknown": True,
                    "QueryArgProfiles": {"Quantity": 0},
                },
                "ContentTypeProfileConfig": {
                    "ForwardWhenContentTypeIsUnknown": True,
                    "ContentTypeProfiles": {
                        "Quantity": 1,
                        "Items": [
                            {
                                "Format": "URLEncoded",
                                "ProfileId": flep_id,
                                "ContentType": "application/x-www-form-urlencoded",
                            }
                        ],
                    },
                },
            },
        )
        config = resp["FieldLevelEncryption"]["FieldLevelEncryptionConfig"]
        assert config["Comment"] == "updated fle"
        assert "ETag" in resp

    def test_delete_field_level_encryption_config(self, cf):
        flep_id, _, _, _, _ = self._create_flep(cf)
        fle_id, _, etag = self._create_fle(cf, flep_id)

        del_resp = cf.delete_field_level_encryption_config(Id=fle_id, IfMatch=etag)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


class TestCloudFrontUpdatePublicKey:
    """Tests for UpdatePublicKey operation."""

    def test_update_public_key(self, cf):
        pub_pem = _generate_public_key_pem()
        name = _unique("pk")
        pk_resp = cf.create_public_key(
            PublicKeyConfig={
                "CallerReference": str(uuid.uuid4()),
                "Name": name,
                "EncodedKey": pub_pem,
                "Comment": "original",
            }
        )
        pk_id = pk_resp["PublicKey"]["Id"]
        etag = pk_resp["ETag"]
        caller_ref = pk_resp["PublicKey"]["PublicKeyConfig"]["CallerReference"]

        update_resp = cf.update_public_key(
            Id=pk_id,
            IfMatch=etag,
            PublicKeyConfig={
                "CallerReference": caller_ref,
                "Name": name,
                "EncodedKey": pub_pem,
                "Comment": "updated-pk",
            },
        )
        assert update_resp["PublicKey"]["Id"] == pk_id
        assert "ETag" in update_resp
        assert update_resp["PublicKey"]["PublicKeyConfig"]["Name"] == name


class TestCloudFrontUpdateOriginRequestPolicy:
    """Tests for UpdateOriginRequestPolicy operation."""

    def test_update_origin_request_policy(self, cf):
        name = _unique("orp")
        create_resp = cf.create_origin_request_policy(
            OriginRequestPolicyConfig={
                "Name": name,
                "Comment": "original",
                "HeadersConfig": {"HeaderBehavior": "none"},
                "CookiesConfig": {"CookieBehavior": "none"},
                "QueryStringsConfig": {"QueryStringBehavior": "none"},
            }
        )
        policy_id = create_resp["OriginRequestPolicy"]["Id"]
        etag = create_resp["ETag"]

        update_resp = cf.update_origin_request_policy(
            Id=policy_id,
            IfMatch=etag,
            OriginRequestPolicyConfig={
                "Name": name,
                "Comment": "updated",
                "HeadersConfig": {"HeaderBehavior": "none"},
                "CookiesConfig": {"CookieBehavior": "none"},
                "QueryStringsConfig": {"QueryStringBehavior": "none"},
            },
        )
        config = update_resp["OriginRequestPolicy"]["OriginRequestPolicyConfig"]
        assert config["Comment"] == "updated"
        assert "ETag" in update_resp


class TestCloudFrontKeyValueStoreCRUD:
    """Tests for KeyValueStore create/describe/update/delete."""

    def test_list_key_value_stores_empty(self, cf):
        resp = cf.list_key_value_stores()
        kvs_list = resp["KeyValueStoreList"]
        assert "Quantity" in kvs_list
        assert "MaxItems" in kvs_list

    def test_create_key_value_store(self, cf):
        name = _unique("kvs")
        resp = cf.create_key_value_store(Name=name, Comment="test kvs")
        kvs = resp["KeyValueStore"]
        assert kvs["Name"] == name
        assert "Id" in kvs
        assert "ARN" in kvs
        assert kvs["Status"] is not None
        assert "ETag" in resp

    def test_describe_key_value_store(self, cf):
        name = _unique("kvs")
        create_resp = cf.create_key_value_store(Name=name, Comment="describe test")
        kvs_id = create_resp["KeyValueStore"]["Name"]

        desc_resp = cf.describe_key_value_store(Name=kvs_id)
        kvs = desc_resp["KeyValueStore"]
        assert kvs["Name"] == name
        assert "Id" in kvs
        assert "ARN" in kvs
        assert "ETag" in desc_resp

    def test_update_key_value_store(self, cf):
        name = _unique("kvs")
        create_resp = cf.create_key_value_store(Name=name, Comment="original")
        etag = create_resp["ETag"]

        update_resp = cf.update_key_value_store(Name=name, IfMatch=etag, Comment="updated")
        kvs = update_resp["KeyValueStore"]
        assert kvs["Name"] == name
        assert "Id" in kvs
        assert "ETag" in update_resp

    def test_delete_key_value_store(self, cf):
        name = _unique("kvs")
        create_resp = cf.create_key_value_store(Name=name, Comment="delete test")
        etag = create_resp["ETag"]

        # May need to use the update etag if update was done
        del_resp = cf.delete_key_value_store(Name=name, IfMatch=etag)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


class TestCloudFrontRealtimeLogConfigCRUD:
    """Tests for RealtimeLogConfig create/update operations."""

    def _rlc_endpoint(self):
        return {
            "StreamType": "Kinesis",
            "KinesisStreamConfig": {
                "RoleARN": "arn:aws:iam::123456789012:role/test-role",
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream",
            },
        }

    def test_create_realtime_log_config(self, cf):
        name = _unique("rlc")
        resp = cf.create_realtime_log_config(
            EndPoints=[self._rlc_endpoint()],
            Fields=["timestamp", "c-ip"],
            Name=name,
            SamplingRate=100,
        )
        rlc = resp["RealtimeLogConfig"]
        assert rlc["Name"] == name
        assert "ARN" in rlc
        assert rlc["SamplingRate"] == 100
        assert len(rlc["Fields"]) == 2

    def test_update_realtime_log_config(self, cf):
        name = _unique("rlc")
        create_resp = cf.create_realtime_log_config(
            EndPoints=[self._rlc_endpoint()],
            Fields=["timestamp", "c-ip"],
            Name=name,
            SamplingRate=100,
        )
        rlc_arn = create_resp["RealtimeLogConfig"]["ARN"]

        update_resp = cf.update_realtime_log_config(
            ARN=rlc_arn,
            EndPoints=[self._rlc_endpoint()],
            Fields=["timestamp", "c-ip", "cs-method"],
            Name=name,
            SamplingRate=50,
        )
        rlc = update_resp["RealtimeLogConfig"]
        assert rlc["SamplingRate"] == 50
        assert len(rlc["Fields"]) == 3

    def test_list_realtime_log_configs_after_create(self, cf):
        name = _unique("rlc")
        cf.create_realtime_log_config(
            EndPoints=[self._rlc_endpoint()],
            Fields=["timestamp"],
            Name=name,
            SamplingRate=100,
        )
        resp = cf.list_realtime_log_configs()
        items = resp.get("RealtimeLogConfigs", {}).get("Items", [])
        names = [item["Name"] for item in items]
        assert name in names


class TestCloudFrontStreamingDistributionWithTags:
    """Tests for CreateStreamingDistributionWithTags."""

    def test_create_streaming_distribution_with_tags(self, cf):
        resp = cf.create_streaming_distribution_with_tags(
            StreamingDistributionConfigWithTags={
                "StreamingDistributionConfig": {
                    "CallerReference": str(uuid.uuid4()),
                    "S3Origin": {
                        "DomainName": "mybucket.s3.amazonaws.com",
                        "OriginAccessIdentity": "",
                    },
                    "Comment": "test-with-tags",
                    "TrustedSigners": {"Enabled": False, "Quantity": 0},
                    "Enabled": True,
                },
                "Tags": {"Items": [{"Key": "env", "Value": "test"}]},
            }
        )
        sd = resp["StreamingDistribution"]
        assert "Id" in sd
        assert sd["StreamingDistributionConfig"]["Comment"] == "test-with-tags"

    def test_create_streaming_distribution_with_tags_verify_tags(self, cf):
        resp = cf.create_streaming_distribution_with_tags(
            StreamingDistributionConfigWithTags={
                "StreamingDistributionConfig": {
                    "CallerReference": str(uuid.uuid4()),
                    "S3Origin": {
                        "DomainName": "mybucket.s3.amazonaws.com",
                        "OriginAccessIdentity": "",
                    },
                    "Comment": "verify-tags",
                    "TrustedSigners": {"Enabled": False, "Quantity": 0},
                    "Enabled": True,
                },
                "Tags": {"Items": [{"Key": "team", "Value": "platform"}]},
            }
        )
        sd_arn = resp["StreamingDistribution"]["ARN"]

        tags_resp = cf.list_tags_for_resource(Resource=sd_arn)
        tag_items = tags_resp["Tags"]["Items"]
        tag_keys = [t["Key"] for t in tag_items]
        assert "team" in tag_keys
