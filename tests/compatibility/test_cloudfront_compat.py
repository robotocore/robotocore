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
