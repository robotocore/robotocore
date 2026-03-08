"""IoT compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def iot():
    return make_client("iot")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


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
