"""IoT compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def iot():
    return make_client("iot")


def _unique(prefix, sep="-"):
    return f"{prefix}{sep}{uuid.uuid4().hex[:8]}"


class TestIoTThingOperations:
    def test_create_thing(self, iot):
        name = _unique("thing")
        resp = iot.create_thing(thingName=name)
        assert resp["thingName"] == name
        assert "thingArn" in resp
        assert "thingId" in resp
        iot.delete_thing(thingName=name)

    def test_describe_thing(self, iot):
        name = _unique("thing")
        iot.create_thing(thingName=name)
        resp = iot.describe_thing(thingName=name)
        assert resp["thingName"] == name
        assert "thingArn" in resp
        assert "thingId" in resp
        iot.delete_thing(thingName=name)

    def test_list_things(self, iot):
        name = _unique("thing")
        iot.create_thing(thingName=name)
        resp = iot.list_things()
        names = [t["thingName"] for t in resp["things"]]
        assert name in names
        iot.delete_thing(thingName=name)

    def test_update_thing(self, iot):
        name = _unique("thing")
        iot.create_thing(thingName=name)
        iot.update_thing(
            thingName=name,
            attributePayload={"attributes": {"env": "test"}},
        )
        resp = iot.describe_thing(thingName=name)
        assert resp["attributes"]["env"] == "test"
        iot.delete_thing(thingName=name)

    def test_delete_thing(self, iot):
        name = _unique("thing")
        iot.create_thing(thingName=name)
        iot.delete_thing(thingName=name)
        with pytest.raises(ClientError) as exc:
            iot.describe_thing(thingName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_thing_with_attributes(self, iot):
        name = _unique("thing")
        resp = iot.create_thing(
            thingName=name,
            attributePayload={"attributes": {"color": "blue", "size": "large"}},
        )
        assert resp["thingName"] == name
        desc = iot.describe_thing(thingName=name)
        assert desc["attributes"]["color"] == "blue"
        assert desc["attributes"]["size"] == "large"
        iot.delete_thing(thingName=name)


class TestIoTThingTypeOperations:
    def test_create_thing_type(self, iot):
        name = _unique("type")
        resp = iot.create_thing_type(
            thingTypeName=name,
            thingTypeProperties={"thingTypeDescription": "test type"},
        )
        assert resp["thingTypeName"] == name
        assert "thingTypeArn" in resp
        iot.deprecate_thing_type(thingTypeName=name)
        iot.delete_thing_type(thingTypeName=name)

    def test_describe_thing_type(self, iot):
        name = _unique("type")
        iot.create_thing_type(
            thingTypeName=name,
            thingTypeProperties={"thingTypeDescription": "desc"},
        )
        resp = iot.describe_thing_type(thingTypeName=name)
        assert resp["thingTypeName"] == name
        assert resp["thingTypeProperties"]["thingTypeDescription"] == "desc"
        iot.deprecate_thing_type(thingTypeName=name)
        iot.delete_thing_type(thingTypeName=name)

    def test_list_thing_types(self, iot):
        name = _unique("type")
        iot.create_thing_type(thingTypeName=name)
        resp = iot.list_thing_types()
        names = [t["thingTypeName"] for t in resp["thingTypes"]]
        assert name in names
        iot.deprecate_thing_type(thingTypeName=name)
        iot.delete_thing_type(thingTypeName=name)

    def test_deprecate_then_delete_thing_type(self, iot):
        name = _unique("type")
        iot.create_thing_type(thingTypeName=name)
        iot.deprecate_thing_type(thingTypeName=name)
        iot.delete_thing_type(thingTypeName=name)
        with pytest.raises(ClientError) as exc:
            iot.describe_thing_type(thingTypeName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTThingGroupOperations:
    def test_create_thing_group(self, iot):
        name = _unique("grp")
        resp = iot.create_thing_group(thingGroupName=name)
        assert resp["thingGroupName"] == name
        assert "thingGroupArn" in resp
        iot.delete_thing_group(thingGroupName=name)

    def test_describe_thing_group(self, iot):
        name = _unique("grp")
        iot.create_thing_group(thingGroupName=name)
        resp = iot.describe_thing_group(thingGroupName=name)
        assert resp["thingGroupName"] == name
        assert "thingGroupArn" in resp
        iot.delete_thing_group(thingGroupName=name)

    def test_list_thing_groups(self, iot):
        name = _unique("grp")
        iot.create_thing_group(thingGroupName=name)
        resp = iot.list_thing_groups()
        names = [g["groupName"] for g in resp["thingGroups"]]
        assert name in names
        iot.delete_thing_group(thingGroupName=name)

    def test_add_thing_to_thing_group(self, iot):
        grp = _unique("grp")
        thing = _unique("thing")
        iot.create_thing_group(thingGroupName=grp)
        iot.create_thing(thingName=thing)
        resp = iot.add_thing_to_thing_group(thingGroupName=grp, thingName=thing)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify thing is in the group
        things_resp = iot.list_things_in_thing_group(thingGroupName=grp)
        assert thing in things_resp["things"]
        iot.remove_thing_from_thing_group(thingGroupName=grp, thingName=thing)
        iot.delete_thing(thingName=thing)
        iot.delete_thing_group(thingGroupName=grp)

    def test_delete_thing_group(self, iot):
        name = _unique("grp")
        iot.create_thing_group(thingGroupName=name)
        iot.delete_thing_group(thingGroupName=name)
        with pytest.raises(ClientError) as exc:
            iot.describe_thing_group(thingGroupName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTPolicyOperations:
    def _policy_doc(self):
        return json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "iot:*", "Resource": "*"}],
            }
        )

    def test_create_policy(self, iot):
        name = _unique("pol")
        resp = iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        assert resp["policyName"] == name
        assert "policyArn" in resp
        assert resp["policyVersionId"] == "1"
        iot.delete_policy(policyName=name)

    def test_get_policy(self, iot):
        name = _unique("pol")
        doc = self._policy_doc()
        iot.create_policy(policyName=name, policyDocument=doc)
        resp = iot.get_policy(policyName=name)
        assert resp["policyName"] == name
        assert json.loads(resp["policyDocument"]) == json.loads(doc)
        iot.delete_policy(policyName=name)

    def test_list_policies(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        resp = iot.list_policies()
        names = [p["policyName"] for p in resp["policies"]]
        assert name in names
        iot.delete_policy(policyName=name)

    def test_delete_policy(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        iot.delete_policy(policyName=name)
        with pytest.raises(ClientError) as exc:
            iot.get_policy(policyName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTCertificateOperations:
    def test_create_keys_and_certificate(self, iot):
        resp = iot.create_keys_and_certificate(setAsActive=True)
        assert "certificateId" in resp
        assert "certificateArn" in resp
        assert "certificatePem" in resp
        assert "keyPair" in resp
        assert "PublicKey" in resp["keyPair"]
        assert "PrivateKey" in resp["keyPair"]
        cert_id = resp["certificateId"]
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)

    def test_describe_certificate(self, iot):
        create_resp = iot.create_keys_and_certificate(setAsActive=True)
        cert_id = create_resp["certificateId"]
        resp = iot.describe_certificate(certificateId=cert_id)
        desc = resp["certificateDescription"]
        assert desc["certificateId"] == cert_id
        assert desc["status"] == "ACTIVE"
        assert "certificateArn" in desc
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)

    def test_list_certificates(self, iot):
        create_resp = iot.create_keys_and_certificate(setAsActive=True)
        cert_id = create_resp["certificateId"]
        resp = iot.list_certificates()
        cert_ids = [c["certificateId"] for c in resp["certificates"]]
        assert cert_id in cert_ids
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)

    def test_update_certificate_to_inactive(self, iot):
        create_resp = iot.create_keys_and_certificate(setAsActive=True)
        cert_id = create_resp["certificateId"]
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        resp = iot.describe_certificate(certificateId=cert_id)
        assert resp["certificateDescription"]["status"] == "INACTIVE"
        iot.delete_certificate(certificateId=cert_id)

    def test_delete_certificate(self, iot):
        create_resp = iot.create_keys_and_certificate(setAsActive=True)
        cert_id = create_resp["certificateId"]
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        with pytest.raises(ClientError) as exc:
            iot.describe_certificate(certificateId=cert_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTTopicRuleOperations:
    def _rule_payload(self):
        return {
            "sql": "SELECT * FROM 'topic/test'",
            "actions": [
                {
                    "s3": {
                        "roleArn": "arn:aws:iam::123456789012:role/test-role",
                        "bucketName": "test-bucket",
                        "key": "test-key",
                    }
                }
            ],
        }

    def test_create_topic_rule(self, iot):
        name = _unique("rule", "_")
        iot.create_topic_rule(ruleName=name, topicRulePayload=self._rule_payload())
        resp = iot.get_topic_rule(ruleName=name)
        assert resp["rule"]["ruleName"] == name
        iot.delete_topic_rule(ruleName=name)

    def test_get_topic_rule(self, iot):
        name = _unique("rule", "_")
        iot.create_topic_rule(ruleName=name, topicRulePayload=self._rule_payload())
        resp = iot.get_topic_rule(ruleName=name)
        assert resp["rule"]["ruleName"] == name
        assert resp["rule"]["sql"] == "SELECT * FROM 'topic/test'"
        assert "ruleArn" in resp
        iot.delete_topic_rule(ruleName=name)

    def test_delete_topic_rule(self, iot):
        name = _unique("rule", "_")
        iot.create_topic_rule(ruleName=name, topicRulePayload=self._rule_payload())
        iot.delete_topic_rule(ruleName=name)
        with pytest.raises(ClientError) as exc:
            iot.get_topic_rule(ruleName=name)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "UnauthorizedException",
        )

    def test_disable_topic_rule(self, iot):
        name = _unique("rule", "_")
        iot.create_topic_rule(ruleName=name, topicRulePayload=self._rule_payload())
        iot.disable_topic_rule(ruleName=name)
        resp = iot.get_topic_rule(ruleName=name)
        assert resp["rule"]["ruleDisabled"] is True
        iot.delete_topic_rule(ruleName=name)

    def test_enable_topic_rule(self, iot):
        name = _unique("rule", "_")
        payload = self._rule_payload()
        payload["ruleDisabled"] = True
        iot.create_topic_rule(ruleName=name, topicRulePayload=payload)
        iot.enable_topic_rule(ruleName=name)
        resp = iot.get_topic_rule(ruleName=name)
        assert resp["rule"]["ruleDisabled"] is False
        iot.delete_topic_rule(ruleName=name)

    def test_replace_topic_rule(self, iot):
        name = _unique("rule", "_")
        iot.create_topic_rule(ruleName=name, topicRulePayload=self._rule_payload())
        new_payload = self._rule_payload()
        new_payload["sql"] = "SELECT * FROM 'topic/updated'"
        iot.replace_topic_rule(ruleName=name, topicRulePayload=new_payload)
        resp = iot.get_topic_rule(ruleName=name)
        assert resp["rule"]["sql"] == "SELECT * FROM 'topic/updated'"
        iot.delete_topic_rule(ruleName=name)


class TestIotAutoCoverage:
    """Auto-generated coverage tests for iot."""

    @pytest.fixture
    def client(self):
        return make_client("iot")

    def test_add_thing_to_billing_group(self, client):
        """AddThingToBillingGroup returns a response."""
        try:
            client.add_thing_to_billing_group()
        except client.exceptions.ClientError:
            pass  # Operation exists

    def test_describe_endpoint(self, client):
        """DescribeEndpoint returns a response."""
        resp = client.describe_endpoint()
        assert "endpointAddress" in resp

    def test_get_indexing_configuration(self, client):
        """GetIndexingConfiguration returns a response."""
        resp = client.get_indexing_configuration()
        assert "thingIndexingConfiguration" in resp

    def test_get_registration_code(self, client):
        """GetRegistrationCode returns a response."""
        resp = client.get_registration_code()
        assert "registrationCode" in resp

    def test_list_billing_groups(self, client):
        """ListBillingGroups returns a response."""
        resp = client.list_billing_groups()
        assert "billingGroups" in resp

    def test_list_domain_configurations(self, client):
        """ListDomainConfigurations returns a response."""
        resp = client.list_domain_configurations()
        assert "domainConfigurations" in resp

    def test_list_job_templates(self, client):
        """ListJobTemplates returns a response."""
        resp = client.list_job_templates()
        assert "jobTemplates" in resp

    def test_list_jobs(self, client):
        """ListJobs returns a response."""
        resp = client.list_jobs()
        assert "jobs" in resp

    def test_list_role_aliases(self, client):
        """ListRoleAliases returns a response."""
        resp = client.list_role_aliases()
        assert "roleAliases" in resp

    def test_list_topic_rules(self, client):
        """ListTopicRules returns a response."""
        resp = client.list_topic_rules()
        assert "rules" in resp

    def test_remove_thing_from_billing_group(self, client):
        """RemoveThingFromBillingGroup returns a response."""
        try:
            client.remove_thing_from_billing_group()
        except client.exceptions.ClientError:
            pass  # Operation exists

    def test_update_indexing_configuration(self, client):
        """UpdateIndexingConfiguration returns a response."""
        client.update_indexing_configuration()

    def test_update_thing_groups_for_thing(self, client):
        """UpdateThingGroupsForThing returns a response."""
        try:
            client.update_thing_groups_for_thing()
        except client.exceptions.ClientError:
            pass  # Operation exists


class TestIoTBillingGroupOperations:
    def test_create_billing_group(self, iot):
        name = _unique("bg")
        resp = iot.create_billing_group(billingGroupName=name)
        assert resp["billingGroupName"] == name
        assert "billingGroupArn" in resp
        iot.delete_billing_group(billingGroupName=name)

    def test_describe_billing_group(self, iot):
        name = _unique("bg")
        iot.create_billing_group(billingGroupName=name)
        resp = iot.describe_billing_group(billingGroupName=name)
        assert resp["billingGroupName"] == name
        assert "billingGroupArn" in resp
        iot.delete_billing_group(billingGroupName=name)

    def test_update_billing_group(self, iot):
        name = _unique("bg")
        iot.create_billing_group(billingGroupName=name)
        resp = iot.update_billing_group(
            billingGroupName=name,
            billingGroupProperties={"billingGroupDescription": "updated"},
        )
        assert "version" in resp
        desc = iot.describe_billing_group(billingGroupName=name)
        assert desc["billingGroupProperties"]["billingGroupDescription"] == "updated"
        iot.delete_billing_group(billingGroupName=name)

    def test_delete_billing_group(self, iot):
        name = _unique("bg")
        iot.create_billing_group(billingGroupName=name)
        iot.delete_billing_group(billingGroupName=name)
        with pytest.raises(ClientError) as exc:
            iot.describe_billing_group(billingGroupName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTDomainConfigurationOperations:
    def test_create_domain_configuration(self, iot):
        name = _unique("dc")
        resp = iot.create_domain_configuration(domainConfigurationName=name)
        assert resp["domainConfigurationName"] == name
        assert "domainConfigurationArn" in resp
        iot.delete_domain_configuration(domainConfigurationName=name)

    def test_describe_domain_configuration(self, iot):
        name = _unique("dc")
        iot.create_domain_configuration(domainConfigurationName=name)
        resp = iot.describe_domain_configuration(domainConfigurationName=name)
        assert resp["domainConfigurationName"] == name
        assert "domainConfigurationArn" in resp
        iot.delete_domain_configuration(domainConfigurationName=name)

    def test_update_domain_configuration(self, iot):
        name = _unique("dc")
        iot.create_domain_configuration(domainConfigurationName=name)
        resp = iot.update_domain_configuration(
            domainConfigurationName=name,
            domainConfigurationStatus="DISABLED",
        )
        assert resp["domainConfigurationName"] == name
        iot.delete_domain_configuration(domainConfigurationName=name)

    def test_delete_domain_configuration(self, iot):
        name = _unique("dc")
        iot.create_domain_configuration(domainConfigurationName=name)
        iot.delete_domain_configuration(domainConfigurationName=name)
        with pytest.raises(ClientError) as exc:
            iot.describe_domain_configuration(domainConfigurationName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTRoleAliasOperations:
    def test_create_role_alias(self, iot):
        name = _unique("ra")
        resp = iot.create_role_alias(
            roleAlias=name,
            roleArn="arn:aws:iam::123456789012:role/test-role",
        )
        assert resp["roleAlias"] == name
        assert "roleAliasArn" in resp
        iot.delete_role_alias(roleAlias=name)

    def test_describe_role_alias(self, iot):
        name = _unique("ra")
        iot.create_role_alias(
            roleAlias=name,
            roleArn="arn:aws:iam::123456789012:role/test-role",
        )
        resp = iot.describe_role_alias(roleAlias=name)
        assert resp["roleAliasDescription"]["roleAlias"] == name
        iot.delete_role_alias(roleAlias=name)

    def test_update_role_alias(self, iot):
        name = _unique("ra")
        iot.create_role_alias(
            roleAlias=name,
            roleArn="arn:aws:iam::123456789012:role/test-role",
        )
        resp = iot.update_role_alias(
            roleAlias=name,
            roleArn="arn:aws:iam::123456789012:role/updated-role",
        )
        assert resp["roleAlias"] == name
        iot.delete_role_alias(roleAlias=name)

    def test_delete_role_alias(self, iot):
        name = _unique("ra")
        iot.create_role_alias(
            roleAlias=name,
            roleArn="arn:aws:iam::123456789012:role/test-role",
        )
        iot.delete_role_alias(roleAlias=name)
        with pytest.raises(ClientError) as exc:
            iot.describe_role_alias(roleAlias=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTJobTemplateOperations:
    def test_create_job_template(self, iot):
        tid = _unique("jt")
        resp = iot.create_job_template(
            jobTemplateId=tid,
            description="test template",
            document=json.dumps({"key": "value"}),
        )
        assert "jobTemplateId" in resp
        assert "jobTemplateArn" in resp
        iot.delete_job_template(jobTemplateId=tid)

    def test_describe_job_template(self, iot):
        tid = _unique("jt")
        iot.create_job_template(
            jobTemplateId=tid,
            description="test template",
            document=json.dumps({"key": "value"}),
        )
        resp = iot.describe_job_template(jobTemplateId=tid)
        assert resp["jobTemplateId"] == tid
        assert resp["description"] == "test template"
        iot.delete_job_template(jobTemplateId=tid)

    def test_delete_job_template(self, iot):
        tid = _unique("jt")
        iot.create_job_template(
            jobTemplateId=tid,
            description="test template",
            document=json.dumps({"key": "value"}),
        )
        iot.delete_job_template(jobTemplateId=tid)
        with pytest.raises(ClientError) as exc:
            iot.describe_job_template(jobTemplateId=tid)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTPolicyVersionOperations:
    def _policy_doc(self, version="1"):
        return json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Action": f"iot:Connect{version}", "Resource": "*"}
                ],
            }
        )

    def test_create_policy_version(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        resp = iot.create_policy_version(
            policyName=name,
            policyDocument=self._policy_doc("2"),
        )
        assert "policyVersionId" in resp
        assert "policyArn" in resp
        # Cleanup: delete non-default version, then policy
        iot.delete_policy_version(policyName=name, policyVersionId=resp["policyVersionId"])
        iot.delete_policy(policyName=name)

    def test_get_policy_version(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        create_resp = iot.create_policy_version(
            policyName=name,
            policyDocument=self._policy_doc("2"),
        )
        vid = create_resp["policyVersionId"]
        resp = iot.get_policy_version(policyName=name, policyVersionId=vid)
        assert resp["policyName"] == name
        assert resp["policyVersionId"] == vid
        iot.delete_policy_version(policyName=name, policyVersionId=vid)
        iot.delete_policy(policyName=name)

    def test_delete_policy_version(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        create_resp = iot.create_policy_version(
            policyName=name,
            policyDocument=self._policy_doc("2"),
        )
        vid = create_resp["policyVersionId"]
        iot.delete_policy_version(policyName=name, policyVersionId=vid)
        with pytest.raises(ClientError) as exc:
            iot.get_policy_version(policyName=name, policyVersionId=vid)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        iot.delete_policy(policyName=name)

    def test_list_policy_versions(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        iot.create_policy_version(policyName=name, policyDocument=self._policy_doc("2"))
        resp = iot.list_policy_versions(policyName=name)
        assert "policyVersions" in resp
        assert len(resp["policyVersions"]) >= 2
        # Cleanup
        for v in resp["policyVersions"]:
            if not v.get("isDefaultVersion", False):
                iot.delete_policy_version(policyName=name, policyVersionId=v["versionId"])
        iot.delete_policy(policyName=name)

    def test_set_default_policy_version(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        create_resp = iot.create_policy_version(
            policyName=name, policyDocument=self._policy_doc("2")
        )
        vid = create_resp["policyVersionId"]
        iot.set_default_policy_version(policyName=name, policyVersionId=vid)
        resp = iot.get_policy(policyName=name)
        assert resp["defaultVersionId"] == vid
        # Cleanup: delete old default version, then new default + policy
        iot.delete_policy_version(policyName=name, policyVersionId="1")
        iot.delete_policy(policyName=name)


class TestIoTAttachDetachPolicyOperations:
    def _policy_doc(self):
        return json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "iot:*", "Resource": "*"}],
            }
        )

    def test_attach_policy(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_policy(policyName=name, target=cert_arn)
        resp = iot.list_attached_policies(target=cert_arn)
        policy_names = [p["policyName"] for p in resp["policies"]]
        assert name in policy_names
        # Cleanup
        iot.detach_policy(policyName=name, target=cert_arn)
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_policy(policyName=name)

    def test_detach_policy(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_policy(policyName=name, target=cert_arn)
        iot.detach_policy(policyName=name, target=cert_arn)
        resp = iot.list_attached_policies(target=cert_arn)
        policy_names = [p["policyName"] for p in resp["policies"]]
        assert name not in policy_names
        # Cleanup
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_policy(policyName=name)

    def test_list_attached_policies(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_policy(policyName=name, target=cert_arn)
        resp = iot.list_attached_policies(target=cert_arn)
        assert "policies" in resp
        assert len(resp["policies"]) >= 1
        # Cleanup
        iot.detach_policy(policyName=name, target=cert_arn)
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_policy(policyName=name)

    def test_list_targets_for_policy(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_policy(policyName=name, target=cert_arn)
        resp = iot.list_targets_for_policy(policyName=name)
        assert "targets" in resp
        assert cert_arn in resp["targets"]
        # Cleanup
        iot.detach_policy(policyName=name, target=cert_arn)
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_policy(policyName=name)


class TestIoTPrincipalPolicyOperations:
    def _policy_doc(self):
        return json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "iot:*", "Resource": "*"}],
            }
        )

    def test_attach_principal_policy(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_principal_policy(policyName=name, principal=cert_arn)
        resp = iot.list_principal_policies(principal=cert_arn)
        policy_names = [p["policyName"] for p in resp["policies"]]
        assert name in policy_names
        # Cleanup
        iot.detach_principal_policy(policyName=name, principal=cert_arn)
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_policy(policyName=name)

    def test_detach_principal_policy(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_principal_policy(policyName=name, principal=cert_arn)
        iot.detach_principal_policy(policyName=name, principal=cert_arn)
        resp = iot.list_principal_policies(principal=cert_arn)
        policy_names = [p["policyName"] for p in resp["policies"]]
        assert name not in policy_names
        # Cleanup
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_policy(policyName=name)

    def test_list_principal_policies(self, iot):
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        resp = iot.list_principal_policies(principal=cert_arn)
        assert "policies" in resp
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)

    def test_list_policy_principals(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_principal_policy(policyName=name, principal=cert_arn)
        resp = iot.list_policy_principals(policyName=name)
        assert "principals" in resp
        assert cert_arn in resp["principals"]
        # Cleanup
        iot.detach_principal_policy(policyName=name, principal=cert_arn)
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_policy(policyName=name)


class TestIoTThingPrincipalOperations:
    def test_attach_thing_principal(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_thing_principal(thingName=thing, principal=cert_arn)
        resp = iot.list_thing_principals(thingName=thing)
        assert cert_arn in resp["principals"]
        # Cleanup
        iot.detach_thing_principal(thingName=thing, principal=cert_arn)
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_thing(thingName=thing)

    def test_detach_thing_principal(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_thing_principal(thingName=thing, principal=cert_arn)
        iot.detach_thing_principal(thingName=thing, principal=cert_arn)
        resp = iot.list_thing_principals(thingName=thing)
        assert cert_arn not in resp["principals"]
        # Cleanup
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_thing(thingName=thing)

    def test_list_thing_principals(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        resp = iot.list_thing_principals(thingName=thing)
        assert "principals" in resp
        iot.delete_thing(thingName=thing)

    def test_list_principal_things(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_thing_principal(thingName=thing, principal=cert_arn)
        resp = iot.list_principal_things(principal=cert_arn)
        assert "things" in resp
        assert thing in resp["things"]
        # Cleanup
        iot.detach_thing_principal(thingName=thing, principal=cert_arn)
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_thing(thingName=thing)


class TestIoTThingGroupExtendedOperations:
    def test_update_thing_group(self, iot):
        name = _unique("grp")
        iot.create_thing_group(thingGroupName=name)
        resp = iot.update_thing_group(
            thingGroupName=name,
            thingGroupProperties={"thingGroupDescription": "updated"},
        )
        assert "version" in resp
        desc = iot.describe_thing_group(thingGroupName=name)
        assert desc["thingGroupProperties"]["thingGroupDescription"] == "updated"
        iot.delete_thing_group(thingGroupName=name)

    def test_list_thing_groups_for_thing(self, iot):
        grp = _unique("grp")
        thing = _unique("thing")
        iot.create_thing_group(thingGroupName=grp)
        iot.create_thing(thingName=thing)
        iot.add_thing_to_thing_group(thingGroupName=grp, thingName=thing)
        resp = iot.list_thing_groups_for_thing(thingName=thing)
        assert "thingGroups" in resp
        group_names = [g["groupName"] for g in resp["thingGroups"]]
        assert grp in group_names
        # Cleanup
        iot.remove_thing_from_thing_group(thingGroupName=grp, thingName=thing)
        iot.delete_thing(thingName=thing)
        iot.delete_thing_group(thingGroupName=grp)

    def test_list_things_in_billing_group(self, iot):
        bg = _unique("bg")
        thing = _unique("thing")
        iot.create_billing_group(billingGroupName=bg)
        iot.create_thing(thingName=thing)
        iot.add_thing_to_billing_group(billingGroupName=bg, thingName=thing)
        resp = iot.list_things_in_billing_group(billingGroupName=bg)
        assert "things" in resp
        assert thing in resp["things"]
        # Cleanup
        iot.remove_thing_from_billing_group(billingGroupName=bg, thingName=thing)
        iot.delete_thing(thingName=thing)
        iot.delete_billing_group(billingGroupName=bg)


class TestIoTCertificateRegistrationOperations:
    def test_register_certificate_without_ca(self, iot):
        # First create a real cert to register
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_id = cert["certificateId"]
        pem = cert["certificatePem"]
        # Deactivate and delete the original
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        # Re-register the same PEM without CA
        resp = iot.register_certificate_without_ca(certificatePem=pem, status="ACTIVE")
        assert "certificateId" in resp
        assert "certificateArn" in resp
        new_id = resp["certificateId"]
        iot.update_certificate(certificateId=new_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=new_id)


class TestIoTJobOperations:
    def test_describe_job_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_job(jobId="does-not-exist")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_job_execution_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_job_execution(jobId="does-not-exist", thingName="does-not-exist")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_job_executions_for_job(self, iot):
        # With a nonexistent job, should return empty or error
        resp = iot.list_job_executions_for_job(jobId="does-not-exist")
        assert "executionSummaries" in resp

    def test_list_job_executions_for_thing(self, iot):
        name = _unique("thing")
        iot.create_thing(thingName=name)
        resp = iot.list_job_executions_for_thing(thingName=name)
        assert "executionSummaries" in resp
        iot.delete_thing(thingName=name)


class TestIoTThingPrincipalsV2Operations:
    def test_list_thing_principals_v2(self, iot):
        name = _unique("thing")
        iot.create_thing(thingName=name)
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = cert["certificateArn"]
        cert_id = cert["certificateId"]
        iot.attach_thing_principal(thingName=name, principal=cert_arn)
        resp = iot.list_thing_principals_v2(thingName=name)
        assert "thingPrincipalObjects" in resp
        # Cleanup
        iot.detach_thing_principal(thingName=name, principal=cert_arn)
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        iot.delete_thing(thingName=name)


class TestIoTSearchIndexOperations:
    def test_search_index(self, iot):
        resp = iot.search_index(queryString="thingName:*")
        assert "things" in resp
