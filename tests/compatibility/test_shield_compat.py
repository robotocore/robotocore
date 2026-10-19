"""Shield compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def shield():
    return make_client("shield")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _make_resource_arn():
    return f"arn:aws:ec2:us-east-1:123456789012:eip-allocation/eipalloc-{uuid.uuid4().hex[:8]}"


class TestShieldSubscription:
    """Tests for Shield subscription operations."""

    def test_create_subscription(self, shield):
        resp = shield.create_subscription()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_subscription(self, shield):
        shield.create_subscription()
        resp = shield.describe_subscription()
        sub = resp["Subscription"]
        assert "StartTime" in sub
        assert "EndTime" in sub
        assert "TimeCommitmentInSeconds" in sub
        assert "AutoRenew" in sub
        assert "SubscriptionArn" in sub


class TestShieldProtectionOperations:
    """Tests for Shield protection create, describe, list, delete."""

    def test_create_and_describe_protection(self, shield):
        name = _unique("prot")
        arn = _make_resource_arn()
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        assert protection_id

        desc = shield.describe_protection(ProtectionId=protection_id)
        prot = desc["Protection"]
        assert prot["Id"] == protection_id
        assert prot["Name"] == name
        assert prot["ResourceArn"] == arn

        # Cleanup
        shield.delete_protection(ProtectionId=protection_id)

    def test_list_protections(self, shield):
        name = _unique("prot")
        arn = _make_resource_arn()
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]

        listed = shield.list_protections()
        names = [p["Name"] for p in listed["Protections"]]
        assert name in names

        # Cleanup
        shield.delete_protection(ProtectionId=protection_id)

    def test_delete_protection(self, shield):
        name = _unique("prot")
        arn = _make_resource_arn()
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]

        shield.delete_protection(ProtectionId=protection_id)

        # Verify deletion
        with pytest.raises(shield.exceptions.ResourceNotFoundException):
            shield.describe_protection(ProtectionId=protection_id)


class TestShieldResource:
    """Tests for Shield resource tagging operations."""

    def test_tag_resource(self, shield):
        name = _unique("tag")
        arn = _make_resource_arn()
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        # Get the protection ARN for tagging
        desc = shield.describe_protection(ProtectionId=protection_id)
        protection_arn = desc["Protection"]["ProtectionArn"]
        try:
            tag_resp = shield.tag_resource(
                ResourceARN=protection_arn,
                Tags=[{"Key": "Env", "Value": "test"}, {"Key": "Team", "Value": "dev"}],
            )
            assert tag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify tags are present
            tags_resp = shield.list_tags_for_resource(ResourceARN=protection_arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp.get("Tags", [])}
            assert tag_map["Env"] == "test"
            assert tag_map["Team"] == "dev"
        finally:
            shield.delete_protection(ProtectionId=protection_id)

    def test_untag_resource(self, shield):
        name = _unique("untag")
        arn = _make_resource_arn()
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        desc = shield.describe_protection(ProtectionId=protection_id)
        protection_arn = desc["Protection"]["ProtectionArn"]
        try:
            shield.tag_resource(
                ResourceARN=protection_arn,
                Tags=[{"Key": "Env", "Value": "test"}, {"Key": "Team", "Value": "dev"}],
            )
            untag_resp = shield.untag_resource(ResourceARN=protection_arn, TagKeys=["Env"])
            assert untag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify only Team tag remains
            tags_resp = shield.list_tags_for_resource(ResourceARN=protection_arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp.get("Tags", [])}
            assert "Env" not in tag_map
            assert tag_map["Team"] == "dev"
        finally:
            shield.delete_protection(ProtectionId=protection_id)


class TestShieldProtectionGroup:
    """Tests for Shield protection group operations."""

    def test_create_protection_group(self, shield):
        gid = _unique("pg")
        resp = shield.create_protection_group(
            ProtectionGroupId=gid,
            Aggregation="SUM",
            Pattern="ALL",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        shield.delete_protection_group(ProtectionGroupId=gid)

    def test_describe_protection_group(self, shield):
        gid = _unique("pg")
        shield.create_protection_group(
            ProtectionGroupId=gid,
            Aggregation="SUM",
            Pattern="ALL",
        )
        try:
            resp = shield.describe_protection_group(ProtectionGroupId=gid)
            pg = resp["ProtectionGroup"]
            assert pg["ProtectionGroupId"] == gid
            assert pg["Aggregation"] == "SUM"
            assert pg["Pattern"] == "ALL"
        finally:
            shield.delete_protection_group(ProtectionGroupId=gid)

    def test_list_protection_groups(self, shield):
        gid = _unique("pg")
        shield.create_protection_group(
            ProtectionGroupId=gid,
            Aggregation="SUM",
            Pattern="ALL",
        )
        try:
            resp = shield.list_protection_groups()
            assert "ProtectionGroups" in resp
            ids = [pg["ProtectionGroupId"] for pg in resp["ProtectionGroups"]]
            assert gid in ids
        finally:
            shield.delete_protection_group(ProtectionGroupId=gid)

    def test_delete_protection_group(self, shield):
        gid = _unique("pg")
        shield.create_protection_group(
            ProtectionGroupId=gid,
            Aggregation="SUM",
            Pattern="ALL",
        )
        resp = shield.delete_protection_group(ProtectionGroupId=gid)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_protection_group(self, shield):
        gid = _unique("pg")
        shield.create_protection_group(
            ProtectionGroupId=gid,
            Aggregation="SUM",
            Pattern="ALL",
        )
        try:
            resp = shield.update_protection_group(
                ProtectionGroupId=gid,
                Aggregation="MEAN",
                Pattern="ALL",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            desc = shield.describe_protection_group(ProtectionGroupId=gid)
            assert desc["ProtectionGroup"]["Aggregation"] == "MEAN"
        finally:
            shield.delete_protection_group(ProtectionGroupId=gid)

    def test_list_resources_in_protection_group(self, shield):
        gid = _unique("pg")
        shield.create_protection_group(
            ProtectionGroupId=gid,
            Aggregation="SUM",
            Pattern="ALL",
        )
        try:
            resp = shield.list_resources_in_protection_group(ProtectionGroupId=gid)
            assert "ResourceArns" in resp
            assert isinstance(resp["ResourceArns"], list)
        finally:
            shield.delete_protection_group(ProtectionGroupId=gid)


class TestShieldSubscriptionAdvanced:
    """Tests for advanced subscription and state operations."""

    def test_get_subscription_state(self, shield):
        resp = shield.get_subscription_state()
        assert "SubscriptionState" in resp
        assert resp["SubscriptionState"] in ("ACTIVE", "INACTIVE")

    def test_describe_attack_statistics(self, shield):
        resp = shield.describe_attack_statistics()
        assert "TimeRange" in resp
        assert "DataItems" in resp
        assert isinstance(resp["DataItems"], list)

    def test_list_attacks(self, shield):
        resp = shield.list_attacks()
        assert "AttackSummaries" in resp
        assert isinstance(resp["AttackSummaries"], list)

    def test_describe_drt_access(self, shield):
        shield.create_subscription()
        resp = shield.describe_drt_access()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_emergency_contact_settings(self, shield):
        shield.create_subscription()
        resp = shield.describe_emergency_contact_settings()
        assert "EmergencyContactList" in resp
        assert isinstance(resp["EmergencyContactList"], list)

    def test_update_emergency_contact_settings(self, shield):
        shield.create_subscription()
        resp = shield.update_emergency_contact_settings(
            EmergencyContactList=[
                {"EmailAddress": "oncall@example.com"},
            ]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        desc = shield.describe_emergency_contact_settings()
        emails = [c["EmailAddress"] for c in desc["EmergencyContactList"]]
        assert "oncall@example.com" in emails

    def test_list_tags_for_resource(self, shield):
        name = _unique("ltag")
        arn = _make_resource_arn()
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        desc = shield.describe_protection(ProtectionId=protection_id)
        protection_arn = desc["Protection"]["ProtectionArn"]
        try:
            tags_resp = shield.list_tags_for_resource(ResourceARN=protection_arn)
            assert "Tags" in tags_resp
            assert isinstance(tags_resp["Tags"], list)
        finally:
            shield.delete_protection(ProtectionId=protection_id)

    def test_describe_attack_nonexistent(self, shield):
        with pytest.raises(shield.exceptions.ClientError):
            shield.describe_attack(AttackId="nonexistent-attack-id")

    def test_delete_subscription(self, shield):
        shield.create_subscription()
        resp = shield.delete_subscription()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_subscription(self, shield):
        shield.create_subscription()
        resp = shield.update_subscription(AutoRenew="ENABLED")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestShieldDRTAndProactive:
    """Tests for DRT and proactive engagement operations."""

    def test_associate_drt_role(self, shield):
        shield.create_subscription()
        resp = shield.associate_drt_role(RoleArn="arn:aws:iam::123456789012:role/ShieldDRT")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_drt_role(self, shield):
        shield.create_subscription()
        shield.associate_drt_role(RoleArn="arn:aws:iam::123456789012:role/ShieldDRT")
        resp = shield.disassociate_drt_role()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_associate_drt_log_bucket(self, shield):
        shield.create_subscription()
        shield.associate_drt_role(RoleArn="arn:aws:iam::123456789012:role/ShieldDRT")
        resp = shield.associate_drt_log_bucket(LogBucket="my-shield-logs-bucket")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_drt_log_bucket(self, shield):
        shield.create_subscription()
        shield.associate_drt_role(RoleArn="arn:aws:iam::123456789012:role/ShieldDRT")
        shield.associate_drt_log_bucket(LogBucket="my-shield-logs-bucket")
        resp = shield.disassociate_drt_log_bucket(LogBucket="my-shield-logs-bucket")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_associate_proactive_engagement_details(self, shield):
        shield.create_subscription()
        resp = shield.associate_proactive_engagement_details(
            EmergencyContactList=[
                {"EmailAddress": "security@example.com"},
            ]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enable_proactive_engagement(self, shield):
        shield.create_subscription()
        shield.associate_proactive_engagement_details(
            EmergencyContactList=[
                {"EmailAddress": "security@example.com"},
            ]
        )
        resp = shield.enable_proactive_engagement()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disable_proactive_engagement(self, shield):
        shield.create_subscription()
        shield.associate_proactive_engagement_details(
            EmergencyContactList=[
                {"EmailAddress": "security@example.com"},
            ]
        )
        shield.enable_proactive_engagement()
        resp = shield.disable_proactive_engagement()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enable_application_layer_automatic_response(self, shield):
        shield.create_subscription()
        arn = _make_resource_arn()
        name = _unique("alb")
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        try:
            resp = shield.enable_application_layer_automatic_response(
                ResourceArn=arn,
                Action={"Block": {}},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            shield.delete_protection(ProtectionId=protection_id)

    def test_disable_application_layer_automatic_response(self, shield):
        shield.create_subscription()
        arn = _make_resource_arn()
        name = _unique("alb")
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        try:
            shield.enable_application_layer_automatic_response(
                ResourceArn=arn,
                Action={"Block": {}},
            )
            resp = shield.disable_application_layer_automatic_response(
                ResourceArn=arn,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            shield.delete_protection(ProtectionId=protection_id)

    def test_update_application_layer_automatic_response(self, shield):
        shield.create_subscription()
        arn = _make_resource_arn()
        name = _unique("alb")
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        try:
            shield.enable_application_layer_automatic_response(
                ResourceArn=arn,
                Action={"Block": {}},
            )
            resp = shield.update_application_layer_automatic_response(
                ResourceArn=arn,
                Action={"Count": {}},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            shield.delete_protection(ProtectionId=protection_id)

    def test_associate_health_check(self, shield):
        shield.create_subscription()
        arn = _make_resource_arn()
        name = _unique("hc")
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        health_check_arn = "arn:aws:route53:::healthcheck/12345678-1234-1234-1234-123456789012"
        try:
            resp = shield.associate_health_check(
                ProtectionId=protection_id,
                HealthCheckArn=health_check_arn,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            shield.delete_protection(ProtectionId=protection_id)

    def test_disassociate_health_check(self, shield):
        shield.create_subscription()
        arn = _make_resource_arn()
        name = _unique("hc-dis")
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        health_check_arn = "arn:aws:route53:::healthcheck/12345678-1234-1234-1234-123456789012"
        try:
            shield.associate_health_check(
                ProtectionId=protection_id,
                HealthCheckArn=health_check_arn,
            )
            resp = shield.disassociate_health_check(
                ProtectionId=protection_id,
                HealthCheckArn=health_check_arn,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            shield.delete_protection(ProtectionId=protection_id)
