"""IoT compatibility tests."""

import datetime
import json
import uuid

import pytest
from botocore.exceptions import ClientError
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

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

    def test_create_thing_type_with_searchable_attributes(self, iot):
        name = _unique("type")
        resp = iot.create_thing_type(
            thingTypeName=name,
            thingTypeProperties={
                "thingTypeDescription": "with attrs",
                "searchableAttributes": ["attr1", "attr2"],
            },
        )
        assert resp["thingTypeName"] == name
        desc = iot.describe_thing_type(thingTypeName=name)
        assert "attr1" in desc["thingTypeProperties"]["searchableAttributes"]
        assert "attr2" in desc["thingTypeProperties"]["searchableAttributes"]
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

    def test_list_thing_types_with_name_filter(self, iot):
        name = _unique("type")
        iot.create_thing_type(thingTypeName=name)
        resp = iot.list_thing_types(thingTypeName=name)
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

    def test_deprecate_and_undeprecate_thing_type(self, iot):
        name = _unique("type")
        iot.create_thing_type(thingTypeName=name)
        iot.deprecate_thing_type(thingTypeName=name)
        desc = iot.describe_thing_type(thingTypeName=name)
        assert desc["thingTypeMetadata"]["deprecated"] is True
        # Undeprecate
        iot.deprecate_thing_type(thingTypeName=name, undoDeprecate=True)
        desc2 = iot.describe_thing_type(thingTypeName=name)
        assert desc2["thingTypeMetadata"]["deprecated"] is False
        # Cleanup
        iot.deprecate_thing_type(thingTypeName=name)
        iot.delete_thing_type(thingTypeName=name)

    def test_create_thing_with_thing_type(self, iot):
        tt_name = _unique("type")
        iot.create_thing_type(thingTypeName=tt_name)
        thing_name = _unique("thing")
        resp = iot.create_thing(thingName=thing_name, thingTypeName=tt_name)
        assert resp["thingName"] == thing_name
        desc = iot.describe_thing(thingName=thing_name)
        assert desc["thingTypeName"] == tt_name
        iot.delete_thing(thingName=thing_name)
        iot.deprecate_thing_type(thingTypeName=tt_name)
        iot.delete_thing_type(thingTypeName=tt_name)


class TestIoTThingFilterOperations:
    def test_list_things_by_thing_type(self, iot):
        tt = _unique("type")
        iot.create_thing_type(thingTypeName=tt)
        t1 = _unique("thing")
        t2 = _unique("thing")
        iot.create_thing(thingName=t1, thingTypeName=tt)
        iot.create_thing(thingName=t2)
        resp = iot.list_things(thingTypeName=tt)
        names = [t["thingName"] for t in resp["things"]]
        assert t1 in names
        assert t2 not in names
        iot.delete_thing(thingName=t1)
        iot.delete_thing(thingName=t2)
        iot.deprecate_thing_type(thingTypeName=tt)
        iot.delete_thing_type(thingTypeName=tt)

    def test_list_things_by_attribute(self, iot):
        t1 = _unique("thing")
        iot.create_thing(thingName=t1)
        iot.update_thing(
            thingName=t1,
            attributePayload={"attributes": {"env": "staging"}},
        )
        resp = iot.list_things(attributeName="env", attributeValue="staging")
        names = [t["thingName"] for t in resp["things"]]
        assert t1 in names
        iot.delete_thing(thingName=t1)

    def test_describe_thing_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_thing(thingName="nonexistent-thing-xyz")
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

    def test_create_thing_group_with_properties(self, iot):
        name = _unique("grp")
        resp = iot.create_thing_group(
            thingGroupName=name,
            thingGroupProperties={
                "thingGroupDescription": "test desc",
                "attributePayload": {"attributes": {"key1": "val1"}},
            },
        )
        assert resp["thingGroupName"] == name
        desc = iot.describe_thing_group(thingGroupName=name)
        assert desc["thingGroupProperties"]["thingGroupDescription"] == "test desc"
        iot.delete_thing_group(thingGroupName=name)

    def test_create_nested_thing_group(self, iot):
        parent = _unique("parent")
        child = _unique("child")
        iot.create_thing_group(thingGroupName=parent)
        resp = iot.create_thing_group(thingGroupName=child, parentGroupName=parent)
        assert resp["thingGroupName"] == child
        desc = iot.describe_thing_group(thingGroupName=child)
        assert desc["thingGroupMetadata"]["parentGroupName"] == parent
        iot.delete_thing_group(thingGroupName=child)
        iot.delete_thing_group(thingGroupName=parent)

    def test_remove_thing_from_thing_group(self, iot):
        grp = _unique("grp")
        thing = _unique("thing")
        iot.create_thing_group(thingGroupName=grp)
        iot.create_thing(thingName=thing)
        iot.add_thing_to_thing_group(thingGroupName=grp, thingName=thing)
        iot.remove_thing_from_thing_group(thingGroupName=grp, thingName=thing)
        things_resp = iot.list_things_in_thing_group(thingGroupName=grp)
        assert thing not in things_resp["things"]
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


class TestIoTEndpointAndConfigOperations:
    """Tests for endpoint, indexing, and registration code operations."""

    def test_describe_endpoint(self, iot):
        resp = iot.describe_endpoint()
        assert "endpointAddress" in resp

    def test_describe_endpoint_data_type(self, iot):
        resp = iot.describe_endpoint(endpointType="iot:Data")
        assert "endpointAddress" in resp

    def test_describe_endpoint_ats_type(self, iot):
        resp = iot.describe_endpoint(endpointType="iot:Data-ATS")
        assert "endpointAddress" in resp

    def test_describe_endpoint_credential_provider(self, iot):
        resp = iot.describe_endpoint(endpointType="iot:CredentialProvider")
        assert "endpointAddress" in resp
        assert "credentials" in resp["endpointAddress"]

    def test_describe_endpoint_jobs(self, iot):
        resp = iot.describe_endpoint(endpointType="iot:Jobs")
        assert "endpointAddress" in resp
        assert "jobs" in resp["endpointAddress"]

    def test_get_indexing_configuration(self, iot):
        resp = iot.get_indexing_configuration()
        assert "thingIndexingConfiguration" in resp

    def test_update_indexing_configuration(self, iot):
        resp = iot.update_indexing_configuration(
            thingIndexingConfiguration={"thingIndexingMode": "OFF"}
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_registration_code(self, iot):
        resp = iot.get_registration_code()
        assert "registrationCode" in resp
        assert len(resp["registrationCode"]) > 0

    def test_search_index(self, iot):
        resp = iot.search_index(queryString="thingName:*")
        assert "things" in resp

    def test_list_certificates_by_ca(self, iot):
        resp = iot.list_certificates_by_ca(caCertificateId="a" * 64)
        assert "certificates" in resp

    def test_list_billing_groups(self, iot):
        resp = iot.list_billing_groups()
        assert "billingGroups" in resp

    def test_list_domain_configurations(self, iot):
        resp = iot.list_domain_configurations()
        assert "domainConfigurations" in resp

    def test_list_job_templates(self, iot):
        resp = iot.list_job_templates()
        assert "jobTemplates" in resp

    def test_list_jobs(self, iot):
        resp = iot.list_jobs()
        assert "jobs" in resp

    def test_list_role_aliases(self, iot):
        resp = iot.list_role_aliases()
        assert "roleAliases" in resp

    def test_list_topic_rules(self, iot):
        resp = iot.list_topic_rules()
        assert "rules" in resp


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

    def test_create_billing_group_with_properties(self, iot):
        name = _unique("bg")
        resp = iot.create_billing_group(
            billingGroupName=name,
            billingGroupProperties={"billingGroupDescription": "with props"},
        )
        assert resp["billingGroupName"] == name
        desc = iot.describe_billing_group(billingGroupName=name)
        assert desc["billingGroupProperties"]["billingGroupDescription"] == "with props"
        iot.delete_billing_group(billingGroupName=name)

    def test_describe_billing_group_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_billing_group(billingGroupName="nonexistent-bg-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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

    def test_create_domain_configuration_with_domain_name(self, iot):
        name = _unique("dc")
        resp = iot.create_domain_configuration(
            domainConfigurationName=name, domainName="test.example.com"
        )
        assert resp["domainConfigurationName"] == name
        desc = iot.describe_domain_configuration(domainConfigurationName=name)
        assert desc["domainName"] == "test.example.com"
        iot.delete_domain_configuration(domainConfigurationName=name)

    def test_update_domain_configuration_authorizer_config(self, iot):
        name = _unique("dc")
        iot.create_domain_configuration(domainConfigurationName=name)
        iot.update_domain_configuration(
            domainConfigurationName=name,
            authorizerConfig={"allowAuthorizerOverride": True},
        )
        desc = iot.describe_domain_configuration(domainConfigurationName=name)
        assert desc["authorizerConfig"]["allowAuthorizerOverride"] is True
        iot.delete_domain_configuration(domainConfigurationName=name)

    def test_describe_domain_configuration_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_domain_configuration(domainConfigurationName="nonexistent-dc-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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

    def test_create_role_alias_with_duration(self, iot):
        name = _unique("ra")
        resp = iot.create_role_alias(
            roleAlias=name,
            roleArn="arn:aws:iam::123456789012:role/test-role",
            credentialDurationSeconds=1800,
        )
        assert resp["roleAlias"] == name
        desc = iot.describe_role_alias(roleAlias=name)
        assert desc["roleAliasDescription"]["credentialDurationSeconds"] == 1800
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

    def test_describe_role_alias_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_role_alias(roleAlias="nonexistent-ra-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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

    def test_update_role_alias_credential_duration(self, iot):
        name = _unique("ra")
        iot.create_role_alias(
            roleAlias=name,
            roleArn="arn:aws:iam::123456789012:role/test-role",
            credentialDurationSeconds=3600,
        )
        iot.update_role_alias(roleAlias=name, credentialDurationSeconds=900)
        desc = iot.describe_role_alias(roleAlias=name)
        assert desc["roleAliasDescription"]["credentialDurationSeconds"] == 900
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


class TestIoTPolicyVersionSetDefaultOperations:
    def _policy_doc(self, action="iot:Connect"):
        return json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": action, "Resource": "*"}],
            }
        )

    def test_create_policy_version_set_as_default(self, iot):
        name = _unique("pol")
        iot.create_policy(policyName=name, policyDocument=self._policy_doc())
        resp = iot.create_policy_version(
            policyName=name,
            policyDocument=self._policy_doc("iot:Publish"),
            setAsDefault=True,
        )
        assert resp["isDefaultVersion"] is True
        assert resp["policyVersionId"] == "2"
        pol = iot.get_policy(policyName=name)
        assert pol["defaultVersionId"] == "2"
        # Cleanup
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

    def test_update_thing_groups_for_thing_add(self, iot):
        grp = _unique("grp")
        thing = _unique("thing")
        iot.create_thing_group(thingGroupName=grp)
        iot.create_thing(thingName=thing)
        resp = iot.update_thing_groups_for_thing(thingName=thing, thingGroupsToAdd=[grp])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        groups = iot.list_thing_groups_for_thing(thingName=thing)
        group_names = [g["groupName"] for g in groups["thingGroups"]]
        assert grp in group_names
        # Cleanup
        iot.remove_thing_from_thing_group(thingGroupName=grp, thingName=thing)
        iot.delete_thing(thingName=thing)
        iot.delete_thing_group(thingGroupName=grp)

    def test_update_thing_groups_for_thing_remove(self, iot):
        grp = _unique("grp")
        thing = _unique("thing")
        iot.create_thing_group(thingGroupName=grp)
        iot.create_thing(thingName=thing)
        iot.add_thing_to_thing_group(thingGroupName=grp, thingName=thing)
        resp = iot.update_thing_groups_for_thing(thingName=thing, thingGroupsToRemove=[grp])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        groups = iot.list_thing_groups_for_thing(thingName=thing)
        group_names = [g["groupName"] for g in groups["thingGroups"]]
        assert grp not in group_names
        iot.delete_thing(thingName=thing)
        iot.delete_thing_group(thingGroupName=grp)

    def test_list_thing_groups_by_parent(self, iot):
        parent = _unique("parent")
        child = _unique("child")
        iot.create_thing_group(thingGroupName=parent)
        iot.create_thing_group(thingGroupName=child, parentGroupName=parent)
        resp = iot.list_thing_groups(parentGroup=parent)
        names = [g["groupName"] for g in resp["thingGroups"]]
        assert child in names
        iot.delete_thing_group(thingGroupName=child)
        iot.delete_thing_group(thingGroupName=parent)

    def test_list_thing_groups_by_name_prefix(self, iot):
        prefix = _unique("pfx")
        grp = f"{prefix}-grp"
        iot.create_thing_group(thingGroupName=grp)
        resp = iot.list_thing_groups(namePrefixFilter=prefix)
        names = [g["groupName"] for g in resp["thingGroups"]]
        assert grp in names
        iot.delete_thing_group(thingGroupName=grp)

    def test_add_thing_to_billing_group(self, iot):
        bg = _unique("bg")
        thing = _unique("thing")
        iot.create_billing_group(billingGroupName=bg)
        iot.create_thing(thingName=thing)
        resp = iot.add_thing_to_billing_group(billingGroupName=bg, thingName=thing)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        things_resp = iot.list_things_in_billing_group(billingGroupName=bg)
        assert thing in things_resp["things"]
        # Cleanup
        iot.remove_thing_from_billing_group(billingGroupName=bg, thingName=thing)
        iot.delete_thing(thingName=thing)
        iot.delete_billing_group(billingGroupName=bg)

    def test_remove_thing_from_billing_group(self, iot):
        bg = _unique("bg")
        thing = _unique("thing")
        iot.create_billing_group(billingGroupName=bg)
        iot.create_thing(thingName=thing)
        iot.add_thing_to_billing_group(billingGroupName=bg, thingName=thing)
        resp = iot.remove_thing_from_billing_group(billingGroupName=bg, thingName=thing)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        things_resp = iot.list_things_in_billing_group(billingGroupName=bg)
        assert thing not in things_resp["things"]
        iot.delete_thing(thingName=thing)
        iot.delete_billing_group(billingGroupName=bg)

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

    def test_create_certificate_from_csr(self, iot):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-device")]))
            .sign(key, hashes.SHA256())
        )
        csr_pem = csr.public_bytes(serialization.Encoding.PEM).decode()
        resp = iot.create_certificate_from_csr(certificateSigningRequest=csr_pem, setAsActive=True)
        assert "certificateId" in resp
        assert "certificateArn" in resp
        assert "certificatePem" in resp
        cert_id = resp["certificateId"]
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)

    def test_update_certificate_to_revoked(self, iot):
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_id = cert["certificateId"]
        iot.update_certificate(certificateId=cert_id, newStatus="REVOKED")
        resp = iot.describe_certificate(certificateId=cert_id)
        assert resp["certificateDescription"]["status"] == "REVOKED"
        iot.delete_certificate(certificateId=cert_id)


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

    def test_create_job(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        thing_arn = iot.describe_thing(thingName=thing)["thingArn"]
        job_id = _unique("job")
        resp = iot.create_job(
            jobId=job_id,
            targets=[thing_arn],
            document=json.dumps({"action": "test"}),
            description="test job",
        )
        assert resp["jobId"] == job_id
        assert "jobArn" in resp
        # Cleanup
        iot.delete_job(jobId=job_id, force=True)
        iot.delete_thing(thingName=thing)

    def test_describe_job(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        thing_arn = iot.describe_thing(thingName=thing)["thingArn"]
        job_id = _unique("job")
        iot.create_job(
            jobId=job_id,
            targets=[thing_arn],
            document=json.dumps({"action": "test"}),
            description="describe me",
        )
        resp = iot.describe_job(jobId=job_id)
        assert resp["job"]["jobId"] == job_id
        assert resp["job"]["description"] == "describe me"
        assert resp["job"]["status"] in ("QUEUED", "IN_PROGRESS")
        assert "jobArn" in resp["job"]
        # Cleanup
        iot.delete_job(jobId=job_id, force=True)
        iot.delete_thing(thingName=thing)

    def test_list_jobs_with_created_job(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        thing_arn = iot.describe_thing(thingName=thing)["thingArn"]
        job_id = _unique("job")
        iot.create_job(
            jobId=job_id,
            targets=[thing_arn],
            document=json.dumps({"action": "list"}),
        )
        resp = iot.list_jobs()
        job_ids = [j["jobId"] for j in resp["jobs"]]
        assert job_id in job_ids
        # Cleanup
        iot.delete_job(jobId=job_id, force=True)
        iot.delete_thing(thingName=thing)

    def test_cancel_job(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        thing_arn = iot.describe_thing(thingName=thing)["thingArn"]
        job_id = _unique("job")
        iot.create_job(
            jobId=job_id,
            targets=[thing_arn],
            document=json.dumps({"action": "cancel"}),
        )
        resp = iot.cancel_job(jobId=job_id)
        assert resp["jobId"] == job_id
        # Verify cancelled
        desc = iot.describe_job(jobId=job_id)
        assert desc["job"]["status"] == "CANCELED"
        # Cleanup
        iot.delete_job(jobId=job_id, force=True)
        iot.delete_thing(thingName=thing)

    def test_delete_job(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        thing_arn = iot.describe_thing(thingName=thing)["thingArn"]
        job_id = _unique("job")
        iot.create_job(
            jobId=job_id,
            targets=[thing_arn],
            document=json.dumps({"action": "delete"}),
        )
        iot.delete_job(jobId=job_id, force=True)
        with pytest.raises(ClientError) as exc:
            iot.describe_job(jobId=job_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        iot.delete_thing(thingName=thing)

    def test_get_job_document(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        thing_arn = iot.describe_thing(thingName=thing)["thingArn"]
        job_id = _unique("job")
        doc = json.dumps({"action": "firmware_update", "version": "2.0"})
        iot.create_job(
            jobId=job_id,
            targets=[thing_arn],
            document=doc,
        )
        resp = iot.get_job_document(jobId=job_id)
        assert json.loads(resp["document"]) == {"action": "firmware_update", "version": "2.0"}
        iot.delete_job(jobId=job_id, force=True)
        iot.delete_thing(thingName=thing)

    def test_cancel_job_execution(self, iot):
        thing = _unique("thing")
        iot.create_thing(thingName=thing)
        thing_arn = iot.describe_thing(thingName=thing)["thingArn"]
        job_id = _unique("job")
        iot.create_job(
            jobId=job_id,
            targets=[thing_arn],
            document=json.dumps({"action": "cancel_exec"}),
        )
        resp = iot.cancel_job_execution(jobId=job_id, thingName=thing, force=True)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        iot.delete_job(jobId=job_id, force=True)
        iot.delete_thing(thingName=thing)


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


def _make_ca_cert():
    """Generate a self-signed CA certificate and its private key."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.UTC))
        .not_valid_after(datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    pem = cert.public_bytes(serialization.Encoding.PEM).decode()
    return key, pem


def _make_verification_cert(ca_key, registration_code):
    """Generate a verification certificate signed by the CA with registration code as CN."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, registration_code)])
    issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.UTC))
        .not_valid_after(datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=365))
        .sign(ca_key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def _make_device_cert(ca_key):
    """Generate a device certificate signed by the CA."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-device")])
    issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.UTC))
        .not_valid_after(datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=365))
        .sign(ca_key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


class TestIoTCACertificateErrorOperations:
    def test_describe_ca_certificate_not_found(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_ca_certificate(certificateId="a" * 64)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTCACertificateOperations:
    def test_describe_ca_certificate_after_registration(self, iot):
        """Describe a CA certificate that was successfully registered."""
        reg_code = iot.get_registration_code()["registrationCode"]
        ca_key, ca_pem = _make_ca_cert()
        verification_pem = _make_verification_cert(ca_key, reg_code)
        resp = iot.register_ca_certificate(
            caCertificate=ca_pem,
            verificationCertificate=verification_pem,
            setAsActive=True,
        )
        ca_id = resp["certificateId"]
        desc = iot.describe_ca_certificate(certificateId=ca_id)
        assert desc["certificateDescription"]["certificateId"] == ca_id
        assert desc["certificateDescription"]["status"] == "ACTIVE"
        assert "certificateArn" in desc["certificateDescription"]
        iot.update_ca_certificate(certificateId=ca_id, newStatus="INACTIVE")
        iot.delete_ca_certificate(certificateId=ca_id)

    def test_update_ca_certificate_status(self, iot):
        """Update CA certificate status from ACTIVE to INACTIVE."""
        reg_code = iot.get_registration_code()["registrationCode"]
        ca_key, ca_pem = _make_ca_cert()
        verification_pem = _make_verification_cert(ca_key, reg_code)
        resp = iot.register_ca_certificate(
            caCertificate=ca_pem,
            verificationCertificate=verification_pem,
            setAsActive=True,
        )
        ca_id = resp["certificateId"]
        iot.update_ca_certificate(certificateId=ca_id, newStatus="INACTIVE")
        desc = iot.describe_ca_certificate(certificateId=ca_id)
        assert desc["certificateDescription"]["status"] == "INACTIVE"
        iot.delete_ca_certificate(certificateId=ca_id)

    def test_register_ca_certificate(self, iot):
        """RegisterCACertificate registers a CA cert using the registration code."""
        reg_code_resp = iot.get_registration_code()
        registration_code = reg_code_resp["registrationCode"]
        ca_key, ca_pem = _make_ca_cert()
        verification_pem = _make_verification_cert(ca_key, registration_code)
        resp = iot.register_ca_certificate(
            caCertificate=ca_pem,
            verificationCertificate=verification_pem,
        )
        assert "certificateId" in resp
        assert "certificateArn" in resp
        ca_cert_id = resp["certificateId"]
        # Cleanup
        iot.update_ca_certificate(certificateId=ca_cert_id, newStatus="INACTIVE")
        iot.delete_ca_certificate(certificateId=ca_cert_id)

    def test_register_certificate(self, iot):
        """RegisterCertificate registers a device cert signed by a registered CA."""
        reg_code_resp = iot.get_registration_code()
        registration_code = reg_code_resp["registrationCode"]
        ca_key, ca_pem = _make_ca_cert()
        verification_pem = _make_verification_cert(ca_key, registration_code)
        ca_resp = iot.register_ca_certificate(
            caCertificate=ca_pem,
            verificationCertificate=verification_pem,
            setAsActive=True,
            allowAutoRegistration=True,
        )
        ca_cert_id = ca_resp["certificateId"]
        device_pem = _make_device_cert(ca_key)
        resp = iot.register_certificate(
            certificatePem=device_pem,
            caCertificatePem=ca_pem,
        )
        assert "certificateId" in resp
        assert "certificateArn" in resp
        dev_cert_id = resp["certificateId"]
        # Cleanup
        iot.update_certificate(certificateId=dev_cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=dev_cert_id)
        iot.update_ca_certificate(certificateId=ca_cert_id, newStatus="INACTIVE")
        iot.delete_ca_certificate(certificateId=ca_cert_id)

    def test_register_certificate_without_ca(self, iot):
        """RegisterCertificateWithoutCA registers a cert PEM directly."""
        cert = iot.create_keys_and_certificate(setAsActive=True)
        cert_id = cert["certificateId"]
        pem = cert["certificatePem"]
        iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=cert_id)
        resp = iot.register_certificate_without_ca(certificatePem=pem, status="ACTIVE")
        assert "certificateId" in resp
        assert "certificateArn" in resp
        new_id = resp["certificateId"]
        iot.update_certificate(certificateId=new_id, newStatus="INACTIVE")
        iot.delete_certificate(certificateId=new_id)


class TestIoTAuthorizerOperations:
    def test_list_authorizers(self, iot):
        resp = iot.list_authorizers()
        assert "authorizers" in resp

    def test_describe_authorizer(self, iot):
        name = _unique("auth")
        iot.create_authorizer(
            authorizerName=name,
            authorizerFunctionArn="arn:aws:lambda:us-east-1:123456789012:function:auth",
        )
        resp = iot.describe_authorizer(authorizerName=name)
        assert resp["authorizerDescription"]["authorizerName"] == name
        assert "authorizerArn" in resp["authorizerDescription"]
        iot.delete_authorizer(authorizerName=name)


class TestIoTCustomMetricOperations:
    def test_list_custom_metrics(self, iot):
        resp = iot.list_custom_metrics()
        assert "metricNames" in resp

    def test_describe_custom_metric(self, iot):
        name = _unique("metric")
        iot.create_custom_metric(
            metricName=name,
            metricType="string-list",
            clientRequestToken="tok",
        )
        resp = iot.describe_custom_metric(metricName=name)
        assert resp["metricName"] == name
        assert resp["metricType"] == "string-list"
        iot.delete_custom_metric(metricName=name)


class TestIoTDimensionOperations:
    def test_list_dimensions(self, iot):
        resp = iot.list_dimensions()
        assert "dimensionNames" in resp

    def test_describe_dimension(self, iot):
        name = _unique("dim")
        iot.create_dimension(
            name=name,
            type="TOPIC_FILTER",
            stringValues=["topic/*"],
            clientRequestToken="tok",
        )
        resp = iot.describe_dimension(name=name)
        assert resp["name"] == name
        assert resp["type"] == "TOPIC_FILTER"
        iot.delete_dimension(name=name)


class TestIoTFleetMetricOperations:
    def test_list_fleet_metrics(self, iot):
        resp = iot.list_fleet_metrics()
        assert "fleetMetrics" in resp

    def test_describe_fleet_metric(self, iot):
        name = _unique("fleet")
        iot.create_fleet_metric(
            metricName=name,
            queryString="*",
            period=300,
            aggregationField="registry.version",
            aggregationType={"name": "Statistics", "values": ["count"]},
            indexName="AWS_Things",
        )
        resp = iot.describe_fleet_metric(metricName=name)
        assert resp["metricName"] == name
        assert resp["queryString"] == "*"
        iot.delete_fleet_metric(metricName=name)


class TestIoTProvisioningTemplateOperations:
    def test_list_provisioning_templates(self, iot):
        resp = iot.list_provisioning_templates()
        assert "templates" in resp

    def test_describe_provisioning_template(self, iot):
        name = _unique("prov")
        iot.create_provisioning_template(
            templateName=name,
            templateBody='{"Parameters":{},"Resources":{}}',
            provisioningRoleArn="arn:aws:iam::123456789012:role/prov",
        )
        resp = iot.describe_provisioning_template(templateName=name)
        assert resp["templateName"] == name
        assert "templateArn" in resp
        iot.delete_provisioning_template(templateName=name)


class TestIoTSecurityProfileOperations:
    def test_list_security_profiles(self, iot):
        resp = iot.list_security_profiles()
        assert "securityProfileIdentifiers" in resp

    def test_describe_security_profile(self, iot):
        name = _unique("secprof")
        iot.create_security_profile(
            securityProfileName=name,
            securityProfileDescription="test",
        )
        resp = iot.describe_security_profile(securityProfileName=name)
        assert resp["securityProfileName"] == name
        assert "securityProfileArn" in resp
        iot.delete_security_profile(securityProfileName=name)


class TestIoTDescribeAndListOperations:
    def test_describe_role_alias(self, iot):
        """DescribeRoleAlias with fake alias returns error."""
        with pytest.raises(ClientError) as exc:
            iot.describe_role_alias(roleAlias="fake-role-alias")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTMitigationActions:
    """Tests for IoT mitigation action operations."""

    def test_create_mitigation_action(self, iot):
        name = _unique("mitact")
        resp = iot.create_mitigation_action(
            actionName=name,
            roleArn="arn:aws:iam::123456789012:role/mitigate",
            actionParams={
                "addThingsToThingGroupParams": {
                    "thingGroupNames": ["test-group"],
                }
            },
        )
        assert "actionArn" in resp
        assert "actionId" in resp
        iot.delete_mitigation_action(actionName=name)

    def test_describe_mitigation_action(self, iot):
        name = _unique("mitact")
        iot.create_mitigation_action(
            actionName=name,
            roleArn="arn:aws:iam::123456789012:role/mitigate",
            actionParams={
                "addThingsToThingGroupParams": {
                    "thingGroupNames": ["test-group"],
                }
            },
        )
        resp = iot.describe_mitigation_action(actionName=name)
        assert resp["actionName"] == name
        assert "actionArn" in resp
        iot.delete_mitigation_action(actionName=name)

    def test_list_mitigation_actions(self, iot):
        resp = iot.list_mitigation_actions()
        assert "actionIdentifiers" in resp

    def test_delete_mitigation_action(self, iot):
        name = _unique("mitact")
        iot.create_mitigation_action(
            actionName=name,
            roleArn="arn:aws:iam::123456789012:role/mitigate",
            actionParams={
                "addThingsToThingGroupParams": {
                    "thingGroupNames": ["test-group"],
                }
            },
        )
        iot.delete_mitigation_action(actionName=name)
        # Verify deletion
        resp = iot.list_mitigation_actions()
        names = [a["actionName"] for a in resp["actionIdentifiers"]]
        assert name not in names


class TestIoTScheduledAuditOperations:
    """Tests for IoT scheduled audit operations."""

    def test_create_scheduled_audit(self, iot):
        name = _unique("schedaudit")
        resp = iot.create_scheduled_audit(
            frequency="DAILY",
            targetCheckNames=["CA_CERTIFICATE_EXPIRING_CHECK"],
            scheduledAuditName=name,
        )
        assert "scheduledAuditArn" in resp
        iot.delete_scheduled_audit(scheduledAuditName=name)

    def test_describe_scheduled_audit(self, iot):
        name = _unique("schedaudit")
        iot.create_scheduled_audit(
            frequency="DAILY",
            targetCheckNames=["CA_CERTIFICATE_EXPIRING_CHECK"],
            scheduledAuditName=name,
        )
        resp = iot.describe_scheduled_audit(scheduledAuditName=name)
        assert resp["scheduledAuditName"] == name
        assert resp["frequency"] == "DAILY"
        iot.delete_scheduled_audit(scheduledAuditName=name)

    def test_list_scheduled_audits(self, iot):
        resp = iot.list_scheduled_audits()
        assert "scheduledAudits" in resp

    def test_delete_scheduled_audit(self, iot):
        name = _unique("schedaudit")
        iot.create_scheduled_audit(
            frequency="DAILY",
            targetCheckNames=["CA_CERTIFICATE_EXPIRING_CHECK"],
            scheduledAuditName=name,
        )
        iot.delete_scheduled_audit(scheduledAuditName=name)
        resp = iot.list_scheduled_audits()
        names = [a["scheduledAuditName"] for a in resp["scheduledAudits"]]
        assert name not in names


class TestIoTSecurityProfileAdvanced:
    """Tests for IoT security profile attach/detach operations."""

    def test_attach_security_profile(self, iot):
        name = _unique("secprof")
        iot.create_security_profile(
            securityProfileName=name,
            securityProfileDescription="test attach",
        )
        resp = iot.attach_security_profile(
            securityProfileName=name,
            securityProfileTargetArn="arn:aws:iot:us-east-1:123456789012:all/things",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        iot.detach_security_profile(
            securityProfileName=name,
            securityProfileTargetArn="arn:aws:iot:us-east-1:123456789012:all/things",
        )
        iot.delete_security_profile(securityProfileName=name)

    def test_detach_security_profile(self, iot):
        name = _unique("secprof")
        iot.create_security_profile(
            securityProfileName=name,
            securityProfileDescription="test detach",
        )
        iot.attach_security_profile(
            securityProfileName=name,
            securityProfileTargetArn="arn:aws:iot:us-east-1:123456789012:all/things",
        )
        resp = iot.detach_security_profile(
            securityProfileName=name,
            securityProfileTargetArn="arn:aws:iot:us-east-1:123456789012:all/things",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        iot.delete_security_profile(securityProfileName=name)

    def test_update_security_profile(self, iot):
        name = _unique("secprof")
        iot.create_security_profile(
            securityProfileName=name,
            securityProfileDescription="original",
        )
        resp = iot.update_security_profile(
            securityProfileName=name,
            securityProfileDescription="updated",
        )
        assert resp["securityProfileName"] == name
        assert resp["securityProfileDescription"] == "updated"
        iot.delete_security_profile(securityProfileName=name)

    def test_create_security_profile(self, iot):
        name = _unique("secprof")
        resp = iot.create_security_profile(
            securityProfileName=name,
            securityProfileDescription="test create",
        )
        assert "securityProfileName" in resp
        assert "securityProfileArn" in resp
        iot.delete_security_profile(securityProfileName=name)

    def test_delete_security_profile(self, iot):
        name = _unique("secprof")
        iot.create_security_profile(
            securityProfileName=name,
            securityProfileDescription="test delete",
        )
        iot.delete_security_profile(securityProfileName=name)
        # Verify it was deleted - list should not contain it
        resp = iot.list_security_profiles()
        names = [p["name"] for p in resp["securityProfileIdentifiers"]]
        assert name not in names


class TestIoTCustomMetricCrud:
    """Tests for IoT custom metric CRUD operations."""

    def test_create_custom_metric(self, iot):
        name = _unique("metric")
        resp = iot.create_custom_metric(
            metricName=name,
            metricType="number",
            clientRequestToken=uuid.uuid4().hex,
        )
        assert "metricName" in resp
        assert "metricArn" in resp
        iot.delete_custom_metric(metricName=name)

    def test_update_custom_metric(self, iot):
        name = _unique("metric")
        iot.create_custom_metric(
            metricName=name,
            metricType="number",
            clientRequestToken=uuid.uuid4().hex,
        )
        resp = iot.update_custom_metric(
            metricName=name,
            displayName="Updated Metric",
        )
        assert resp["metricName"] == name
        assert resp["displayName"] == "Updated Metric"
        iot.delete_custom_metric(metricName=name)

    def test_delete_custom_metric(self, iot):
        name = _unique("metric")
        iot.create_custom_metric(
            metricName=name,
            metricType="number",
            clientRequestToken=uuid.uuid4().hex,
        )
        iot.delete_custom_metric(metricName=name)
        resp = iot.list_custom_metrics()
        assert name not in resp["metricNames"]


class TestIoTAuthorizerCrud:
    """Tests for IoT authorizer CRUD operations."""

    def test_create_authorizer(self, iot):
        name = _unique("auth")
        resp = iot.create_authorizer(
            authorizerName=name,
            authorizerFunctionArn="arn:aws:lambda:us-east-1:123456789012:function:auth",
        )
        assert "authorizerName" in resp
        assert "authorizerArn" in resp
        iot.delete_authorizer(authorizerName=name)

    def test_update_authorizer(self, iot):
        name = _unique("auth")
        iot.create_authorizer(
            authorizerName=name,
            authorizerFunctionArn="arn:aws:lambda:us-east-1:123456789012:function:auth",
        )
        resp = iot.update_authorizer(
            authorizerName=name,
            authorizerFunctionArn="arn:aws:lambda:us-east-1:123456789012:function:auth2",
        )
        assert resp["authorizerName"] == name
        iot.delete_authorizer(authorizerName=name)

    def test_delete_authorizer(self, iot):
        name = _unique("auth")
        iot.create_authorizer(
            authorizerName=name,
            authorizerFunctionArn="arn:aws:lambda:us-east-1:123456789012:function:auth",
        )
        iot.delete_authorizer(authorizerName=name)
        resp = iot.list_authorizers()
        names = [a["authorizerName"] for a in resp["authorizers"]]
        assert name not in names


class TestIoTAuditOperations:
    """Tests for IoT audit-related list/describe operations."""

    def test_describe_account_audit_configuration(self, iot):
        resp = iot.describe_account_audit_configuration()
        assert "auditCheckConfigurations" in resp

    def test_update_account_audit_configuration(self, iot):
        resp = iot.update_account_audit_configuration(
            auditCheckConfigurations={"CA_CERTIFICATE_EXPIRING_CHECK": {"enabled": True}}
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_audit_findings(self, iot):
        resp = iot.list_audit_findings()
        assert "findings" in resp

    def test_list_audit_tasks(self, iot):
        resp = iot.list_audit_tasks(
            startTime=datetime.datetime(2020, 1, 1),
            endTime=datetime.datetime(2030, 1, 1),
        )
        assert "tasks" in resp

    def test_list_audit_suppressions(self, iot):
        resp = iot.list_audit_suppressions()
        assert "suppressions" in resp

    def test_list_active_violations(self, iot):
        resp = iot.list_active_violations()
        assert "activeViolations" in resp


class TestIoTProvisioningTemplateAdvanced:
    """Tests for provisioning template version operations."""

    def test_create_provisioning_template_version(self, iot):
        name = _unique("prov")
        iot.create_provisioning_template(
            templateName=name,
            templateBody='{"Parameters":{},"Resources":{}}',
            provisioningRoleArn="arn:aws:iam::123456789012:role/prov",
        )
        resp = iot.create_provisioning_template_version(
            templateName=name,
            templateBody='{"Parameters":{},"Resources":{"thing":{}}}',
        )
        assert "templateName" in resp
        assert "versionId" in resp
        iot.delete_provisioning_template(templateName=name)

    def test_describe_provisioning_template_version(self, iot):
        name = _unique("prov")
        iot.create_provisioning_template(
            templateName=name,
            templateBody='{"Parameters":{},"Resources":{}}',
            provisioningRoleArn="arn:aws:iam::123456789012:role/prov",
        )
        ver = iot.create_provisioning_template_version(
            templateName=name,
            templateBody='{"Parameters":{},"Resources":{"thing":{}}}',
        )
        resp = iot.describe_provisioning_template_version(
            templateName=name,
            versionId=ver["versionId"],
        )
        assert "versionId" in resp
        assert "templateBody" in resp
        iot.delete_provisioning_template(templateName=name)

    def test_list_provisioning_template_versions(self, iot):
        name = _unique("prov")
        iot.create_provisioning_template(
            templateName=name,
            templateBody='{"Parameters":{},"Resources":{}}',
            provisioningRoleArn="arn:aws:iam::123456789012:role/prov",
        )
        resp = iot.list_provisioning_template_versions(templateName=name)
        assert "versions" in resp
        assert len(resp["versions"]) >= 1
        iot.delete_provisioning_template(templateName=name)

    def test_update_provisioning_template(self, iot):
        name = _unique("prov")
        iot.create_provisioning_template(
            templateName=name,
            templateBody='{"Parameters":{},"Resources":{}}',
            provisioningRoleArn="arn:aws:iam::123456789012:role/prov",
        )
        resp = iot.update_provisioning_template(
            templateName=name,
            description="updated description",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        iot.delete_provisioning_template(templateName=name)

    def test_delete_provisioning_template(self, iot):
        name = _unique("prov")
        iot.create_provisioning_template(
            templateName=name,
            templateBody='{"Parameters":{},"Resources":{}}',
            provisioningRoleArn="arn:aws:iam::123456789012:role/prov",
        )
        iot.delete_provisioning_template(templateName=name)
        resp = iot.list_provisioning_templates()
        names = [t["templateName"] for t in resp["templates"]]
        assert name not in names


class TestIoTMiscListOperations:
    """Tests for miscellaneous list operations."""

    def test_list_streams(self, iot):
        resp = iot.list_streams()
        assert "streams" in resp

    def test_list_ota_updates(self, iot):
        resp = iot.list_ota_updates()
        assert "otaUpdates" in resp

    def test_list_ca_certificates(self, iot):
        resp = iot.list_ca_certificates()
        assert "certificates" in resp

    def test_list_managed_job_templates(self, iot):
        resp = iot.list_managed_job_templates()
        assert "managedJobTemplates" in resp

    def test_list_things_in_thing_group(self, iot):
        group_name = _unique("grp")
        iot.create_thing_group(thingGroupName=group_name)
        resp = iot.list_things_in_thing_group(thingGroupName=group_name)
        assert "things" in resp
        iot.delete_thing_group(thingGroupName=group_name)

    def test_get_v2_logging_options(self, iot):
        resp = iot.get_v2_logging_options()
        # Response may have defaultLogLevel or not, but the call should succeed
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_deprecate_thing_type(self, iot):
        name = _unique("ttype")
        iot.create_thing_type(thingTypeName=name)
        resp = iot.deprecate_thing_type(thingTypeName=name, undoDeprecate=False)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Cleanup - need to wait or force delete
        iot.deprecate_thing_type(thingTypeName=name, undoDeprecate=True)

    def test_delete_thing_type(self, iot):
        name = _unique("ttype")
        iot.create_thing_type(thingTypeName=name)
        iot.deprecate_thing_type(thingTypeName=name, undoDeprecate=False)
        # Note: AWS requires 5 min wait before delete; moto may allow immediate
        try:
            iot.delete_thing_type(thingTypeName=name)
        except ClientError:
            # Expected if immediate delete not allowed
            iot.deprecate_thing_type(thingTypeName=name, undoDeprecate=True)


class TestIoTWorkingButUntestedOps:
    """Tests for operations confirmed working by probe but not yet covered."""

    def test_delete_job_execution_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.delete_job_execution(jobId="fake-job-id", thingName="fake-thing", executionNumber=1)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_ota_update_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.delete_ota_update(otaUpdateId="fake-ota-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_stream_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.delete_stream(streamId="fake-stream-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_audit_finding_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_audit_finding(findingId="fake-finding-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_audit_mitigation_actions_task_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_audit_mitigation_actions_task(taskId="fake-task-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_audit_suppression_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_audit_suppression(
                checkName="FAKE_CHECK",
                resourceIdentifier={
                    "deviceCertificateId": "a" * 64,
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_audit_task_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_audit_task(taskId="fake-task-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_detect_mitigation_actions_task_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_detect_mitigation_actions_task(taskId="fake-task-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_managed_job_template_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_managed_job_template(templateName="fake-template")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_stream_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_stream(streamId="fake-stream-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_thing_registration_task_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.describe_thing_registration_task(taskId="fake-task-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_statistics(self, iot):
        resp = iot.get_statistics(queryString="*")
        assert "statistics" in resp
        assert "count" in resp["statistics"]

    def test_get_topic_rule_destination_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.get_topic_rule_destination(
                arn="arn:aws:iot:us-east-1:123456789012:ruledestination/fake"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_audit_mitigation_actions_executions(self, iot):
        resp = iot.list_audit_mitigation_actions_executions(
            taskId="fake-task-id", findingId="fake-finding-id"
        )
        assert "actionsExecutions" in resp

    def test_list_audit_mitigation_actions_tasks(self, iot):
        now = datetime.datetime.now(datetime.UTC)
        start = now - datetime.timedelta(days=1)
        resp = iot.list_audit_mitigation_actions_tasks(startTime=start, endTime=now)
        assert "tasks" in resp

    def test_list_detect_mitigation_actions_executions(self, iot):
        resp = iot.list_detect_mitigation_actions_executions()
        assert "actionsExecutions" in resp

    def test_list_detect_mitigation_actions_tasks(self, iot):
        now = datetime.datetime.now(datetime.UTC)
        start = now - datetime.timedelta(days=1)
        resp = iot.list_detect_mitigation_actions_tasks(startTime=start, endTime=now)
        assert "tasks" in resp

    def test_list_metric_values(self, iot):
        now = datetime.datetime.now(datetime.UTC)
        start = now - datetime.timedelta(days=1)
        resp = iot.list_metric_values(
            thingName="fake-thing",
            metricName="fake-metric",
            startTime=start,
            endTime=now,
        )
        assert "metricDatumList" in resp

    def test_list_related_resources_for_audit_finding(self, iot):
        resp = iot.list_related_resources_for_audit_finding(findingId="fake-finding-id")
        assert "relatedResources" in resp

    def test_list_violation_events(self, iot):
        now = datetime.datetime.now(datetime.UTC)
        start = now - datetime.timedelta(days=1)
        resp = iot.list_violation_events(startTime=start, endTime=now)
        assert "violationEvents" in resp

    def test_update_fleet_metric_nonexistent(self, iot):
        with pytest.raises(ClientError) as exc:
            iot.update_fleet_metric(
                metricName="fake-metric",
                indexName="AWS_Things",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTDimensionCrud:
    """Tests for Dimension update and delete operations."""

    def test_update_dimension(self, iot):
        """UpdateDimension changes stringValues."""
        name = _unique("dim")
        iot.create_dimension(
            name=name,
            type="TOPIC_FILTER",
            stringValues=["topic/original/*"],
            clientRequestToken="tok1",
        )
        try:
            resp = iot.update_dimension(name=name, stringValues=["topic/updated/*"])
            assert resp["name"] == name
            assert "topic/updated/*" in resp["stringValues"]
        finally:
            iot.delete_dimension(name=name)

    def test_update_dimension_reflected_in_describe(self, iot):
        """DescribeDimension reflects UpdateDimension changes."""
        name = _unique("dim")
        iot.create_dimension(
            name=name,
            type="TOPIC_FILTER",
            stringValues=["topic/a/*"],
            clientRequestToken="tok2",
        )
        try:
            iot.update_dimension(name=name, stringValues=["topic/b/*"])
            resp = iot.describe_dimension(name=name)
            assert "topic/b/*" in resp["stringValues"]
        finally:
            iot.delete_dimension(name=name)

    def test_delete_dimension(self, iot):
        """DeleteDimension removes the dimension."""
        name = _unique("dim")
        iot.create_dimension(
            name=name,
            type="TOPIC_FILTER",
            stringValues=["topic/del/*"],
            clientRequestToken="tok3",
        )
        iot.delete_dimension(name=name)
        with pytest.raises(ClientError) as exc:
            iot.describe_dimension(name=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_dimension_removes_from_list(self, iot):
        """DeleteDimension removes the dimension from ListDimensions."""
        name = _unique("dim")
        iot.create_dimension(
            name=name,
            type="TOPIC_FILTER",
            stringValues=["topic/listdel/*"],
            clientRequestToken="tok4",
        )
        iot.delete_dimension(name=name)
        resp = iot.list_dimensions()
        assert name not in resp.get("dimensionNames", [])


class TestIoTEventConfigAndLogging:
    """Tests for event configurations and logging."""

    def test_list_ca_certificates(self, iot):
        """ListCACertificates returns response."""
        resp = iot.list_ca_certificates()
        assert "certificates" in resp


class TestIoTStreamOperations:
    """Tests for IoT Stream CRUD."""

    def test_update_stream_nonexistent(self, iot):
        """UpdateStream on nonexistent stream raises error."""
        with pytest.raises(ClientError) as exc:
            iot.update_stream(
                streamId="nonexistent-stream",
                files=[{"fileId": 1, "s3Location": {"bucket": "b", "key": "k"}}],
                roleArn="arn:aws:iam::123456789012:role/fake",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTSecurityProfileTargets:
    """Tests for security profile target operations."""

    def test_list_security_profiles_for_target(self, iot):
        """ListSecurityProfilesForTarget returns response."""
        resp = iot.list_security_profiles_for_target(
            securityProfileTargetArn="arn:aws:iot:us-east-1:123456789012:all/things"
        )
        assert "securityProfileTargetMappings" in resp

    def test_list_targets_for_security_profile(self, iot):
        """ListTargetsForSecurityProfile returns response."""
        name = _unique("sp")
        iot.create_security_profile(securityProfileName=name)
        resp = iot.list_targets_for_security_profile(securityProfileName=name)
        assert "securityProfileTargets" in resp
        iot.delete_security_profile(securityProfileName=name)


class TestIoTScheduledAuditUpdate:
    """Tests for UpdateScheduledAudit."""

    def test_update_scheduled_audit(self, iot):
        """UpdateScheduledAudit updates the audit schedule."""
        name = _unique("sa")
        iot.create_scheduled_audit(
            scheduledAuditName=name,
            frequency="DAILY",
            targetCheckNames=["CA_CERTIFICATE_EXPIRING_CHECK"],
        )
        resp = iot.update_scheduled_audit(
            scheduledAuditName=name,
            frequency="WEEKLY",
            dayOfWeek="MON",
        )
        assert "scheduledAuditArn" in resp
        desc = iot.describe_scheduled_audit(scheduledAuditName=name)
        assert desc["frequency"] == "WEEKLY"
        iot.delete_scheduled_audit(scheduledAuditName=name)


class TestIoTMitigationActionUpdate:
    """Tests for UpdateMitigationAction."""

    def test_update_mitigation_action(self, iot):
        """UpdateMitigationAction updates the action."""
        name = _unique("ma")
        iot.create_mitigation_action(
            actionName=name,
            roleArn="arn:aws:iam::123456789012:role/fake",
            actionParams={
                "publishFindingToSnsParams": {
                    "topicArn": "arn:aws:sns:us-east-1:123456789012:topic"
                }
            },
        )
        resp = iot.update_mitigation_action(
            actionName=name,
            roleArn="arn:aws:iam::123456789012:role/fake2",
        )
        assert "actionArn" in resp
        iot.delete_mitigation_action(actionName=name)


class TestIoTDefaultAuthorizerOperations:
    """Tests for SetDefaultAuthorizer, DescribeDefaultAuthorizer."""

    def test_set_and_describe_default_authorizer(self, iot):
        """Set a default authorizer and describe it."""
        auth_name = _unique("auth")
        iot.create_authorizer(
            authorizerName=auth_name,
            authorizerFunctionArn="arn:aws:lambda:us-east-1:123456789012:function:fake",
        )
        iot.set_default_authorizer(authorizerName=auth_name)
        resp = iot.describe_default_authorizer()
        assert resp["authorizerDescription"]["authorizerName"] == auth_name
        iot.delete_authorizer(authorizerName=auth_name)


class TestIoTProvisioningTemplateVersionDelete:
    """Tests for DeleteProvisioningTemplateVersion."""

    def test_delete_provisioning_template_version(self, iot):
        """Create a provisioning template, add a version, then delete the version."""
        tpl_name = _unique("tpl")
        body = json.dumps(
            {
                "Parameters": {"SerialNumber": {"Type": "String"}},
                "Resources": {
                    "thing": {
                        "Type": "AWS::IoT::Thing",
                        "Properties": {"ThingName": {"Ref": "SerialNumber"}},
                    }
                },
            }
        )
        iot.create_provisioning_template(
            templateName=tpl_name,
            templateBody=body,
            provisioningRoleArn="arn:aws:iam::123456789012:role/fake",
        )
        v_resp = iot.create_provisioning_template_version(
            templateName=tpl_name,
            templateBody=body,
        )
        version_id = v_resp["versionId"]
        iot.delete_provisioning_template_version(templateName=tpl_name, versionId=version_id)
        # Verify the version is gone
        versions_resp = iot.list_provisioning_template_versions(templateName=tpl_name)
        version_ids = [v["versionId"] for v in versions_resp["versions"]]
        assert version_id not in version_ids
        iot.delete_provisioning_template(templateName=tpl_name)


class TestIoTOTAUpdateOperations:
    """Tests for CreateOTAUpdate, GetOTAUpdate, DeleteOTAUpdate."""

    def _ota_files(self):
        return [
            {
                "fileName": "firmware.bin",
                "fileLocation": {
                    "s3Location": {
                        "bucket": "test-bucket",
                        "key": "firmware.bin",
                        "version": "1",
                    }
                },
                "codeSigning": {},
            }
        ]

    def test_get_ota_update_nonexistent(self, iot):
        """GetOTAUpdate returns ResourceNotFoundException for missing update."""
        with pytest.raises(ClientError) as exc:
            iot.get_ota_update(otaUpdateId="nonexistent-ota-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_and_get_ota_update(self, iot):
        """Create an OTA update and verify via GetOTAUpdate."""
        ota_id = _unique("ota")
        resp = iot.create_ota_update(
            otaUpdateId=ota_id,
            targets=["arn:aws:iot:us-east-1:123456789012:thing/fake"],
            files=self._ota_files(),
            roleArn="arn:aws:iam::123456789012:role/fake",
        )
        assert resp["otaUpdateId"] == ota_id
        assert "otaUpdateArn" in resp
        get_resp = iot.get_ota_update(otaUpdateId=ota_id)
        assert get_resp["otaUpdateInfo"]["otaUpdateId"] == ota_id
        iot.delete_ota_update(otaUpdateId=ota_id)

    def test_delete_ota_update_verified(self, iot):
        """Delete an OTA update and verify it's gone."""
        ota_id = _unique("ota")
        iot.create_ota_update(
            otaUpdateId=ota_id,
            targets=["arn:aws:iot:us-east-1:123456789012:thing/fake"],
            files=self._ota_files(),
            roleArn="arn:aws:iam::123456789012:role/fake",
        )
        iot.delete_ota_update(otaUpdateId=ota_id)
        with pytest.raises(ClientError) as exc:
            iot.get_ota_update(otaUpdateId=ota_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIoTStreamCrudOperations:
    """Tests for CreateStream, DescribeStream, UpdateStream, DeleteStream."""

    def test_create_and_describe_stream(self, iot):
        """Create a stream and verify via DescribeStream."""
        stream_id = _unique("stream")
        resp = iot.create_stream(
            streamId=stream_id,
            description="test stream",
            files=[
                {
                    "fileId": 1,
                    "s3Location": {"bucket": "test-bucket", "key": "test-key"},
                }
            ],
            roleArn="arn:aws:iam::123456789012:role/fake",
        )
        assert resp["streamId"] == stream_id
        assert "streamArn" in resp
        desc = iot.describe_stream(streamId=stream_id)
        assert desc["streamInfo"]["streamId"] == stream_id
        assert desc["streamInfo"]["description"] == "test stream"
        iot.delete_stream(streamId=stream_id)

    def test_delete_stream_verified(self, iot):
        """Delete a stream and verify it's gone."""
        stream_id = _unique("stream")
        iot.create_stream(
            streamId=stream_id,
            description="test stream",
            files=[
                {
                    "fileId": 1,
                    "s3Location": {"bucket": "test-bucket", "key": "test-key"},
                }
            ],
            roleArn="arn:aws:iam::123456789012:role/fake",
        )
        iot.delete_stream(streamId=stream_id)
        with pytest.raises(ClientError) as exc:
            iot.describe_stream(streamId=stream_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_stream(self, iot):
        """Update a stream's description."""
        stream_id = _unique("stream")
        iot.create_stream(
            streamId=stream_id,
            description="original",
            files=[
                {
                    "fileId": 1,
                    "s3Location": {"bucket": "test-bucket", "key": "test-key"},
                }
            ],
            roleArn="arn:aws:iam::123456789012:role/fake",
        )
        iot.update_stream(streamId=stream_id, description="updated")
        desc = iot.describe_stream(streamId=stream_id)
        assert desc["streamInfo"]["description"] == "updated"
        iot.delete_stream(streamId=stream_id)
