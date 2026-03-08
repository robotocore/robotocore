"""AWS Organizations compatibility tests."""

import json
import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def orgs():
    return make_client("organizations")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture(autouse=True)
def _ensure_no_org(orgs):
    """Ensure no organization exists before and after each test."""
    # Clean up any pre-existing org
    _cleanup_org(orgs)
    yield
    _cleanup_org(orgs)


def _cleanup_org(client):
    """Best-effort cleanup of an existing organization."""
    try:
        client.describe_organization()
    except Exception:
        return  # No org exists

    # Remove non-master accounts
    try:
        accounts = client.list_accounts()["Accounts"]
        for acct in accounts:
            if acct["Name"] != "master":
                try:
                    client.remove_account_from_organization(AccountId=acct["Id"])
                except Exception:
                    pass
    except Exception:
        pass

    # Delete OUs (must delete children first)
    try:
        roots = client.list_roots()["Roots"]
        for root in roots:
            _delete_ous_recursive(client, root["Id"])
    except Exception:
        pass

    # Delete non-default policies
    try:
        for ptype in ["SERVICE_CONTROL_POLICY", "TAG_POLICY"]:
            policies = client.list_policies(Filter=ptype).get("Policies", [])
            for p in policies:
                if not p["AwsManaged"]:
                    try:
                        client.delete_policy(PolicyId=p["Id"])
                    except Exception:
                        pass
    except Exception:
        pass

    try:
        client.delete_organization()
    except Exception:
        pass


def _delete_ous_recursive(client, parent_id):
    """Recursively delete organizational units under a parent."""
    try:
        ous = client.list_organizational_units_for_parent(ParentId=parent_id)["OrganizationalUnits"]
        for ou in ous:
            _delete_ous_recursive(client, ou["Id"])
            try:
                client.delete_organizational_unit(OrganizationalUnitId=ou["Id"])
            except Exception:
                pass
    except Exception:
        pass


class TestOrganizationsLifecycle:
    def test_create_and_describe_organization(self, orgs):
        resp = orgs.create_organization(FeatureSet="ALL")
        org = resp["Organization"]
        assert "Id" in org
        assert org["FeatureSet"] == "ALL"
        assert "MasterAccountId" in org

        desc = orgs.describe_organization()
        assert desc["Organization"]["Id"] == org["Id"]
        assert desc["Organization"]["FeatureSet"] == "ALL"

    def test_list_accounts_includes_master(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.list_accounts()
        accounts = resp["Accounts"]
        assert len(accounts) >= 1
        master = accounts[0]
        assert master["Name"] == "master"
        assert master["Status"] == "ACTIVE"

    def test_list_roots(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.list_roots()
        roots = resp["Roots"]
        assert len(roots) == 1
        root = roots[0]
        assert root["Id"].startswith("r-")
        assert root["Name"] == "Root"

    def test_create_and_delete_organization(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        orgs.describe_organization()  # Should not raise
        orgs.delete_organization()
        with pytest.raises(orgs.exceptions.AWSOrganizationsNotInUseException):
            orgs.describe_organization()

    def test_create_account(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        email = f"{_unique('test')}@example.com"
        resp = orgs.create_account(Email=email, AccountName=_unique("TestAcct"))
        status = resp["CreateAccountStatus"]
        assert status["State"] == "SUCCEEDED"
        assert "AccountId" in status

        accounts = orgs.list_accounts()["Accounts"]
        account_ids = [a["Id"] for a in accounts]
        assert status["AccountId"] in account_ids

    def test_list_create_account_status(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        email = f"{_unique('test')}@example.com"
        orgs.create_account(Email=email, AccountName=_unique("Acct"))
        resp = orgs.list_create_account_status()
        statuses = resp["CreateAccountStatuses"]
        assert len(statuses) >= 1
        assert statuses[0]["State"] in ("SUCCEEDED", "IN_PROGRESS")


class TestOrganizationsOUOperations:
    def test_create_ou(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        name = _unique("OU")
        resp = orgs.create_organizational_unit(ParentId=root_id, Name=name)
        ou = resp["OrganizationalUnit"]
        assert ou["Name"] == name
        assert ou["Id"].startswith("ou-")

    def test_list_organizational_units_for_parent(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        name1 = _unique("OU1")
        name2 = _unique("OU2")
        orgs.create_organizational_unit(ParentId=root_id, Name=name1)
        orgs.create_organizational_unit(ParentId=root_id, Name=name2)
        resp = orgs.list_organizational_units_for_parent(ParentId=root_id)
        ou_names = [ou["Name"] for ou in resp["OrganizationalUnits"]]
        assert name1 in ou_names
        assert name2 in ou_names

    def test_list_children(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        ou = orgs.create_organizational_unit(ParentId=root_id, Name=_unique("OU"))
        ou_id = ou["OrganizationalUnit"]["Id"]
        resp = orgs.list_children(ParentId=root_id, ChildType="ORGANIZATIONAL_UNIT")
        child_ids = [c["Id"] for c in resp["Children"]]
        assert ou_id in child_ids

    def test_move_account(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        ou = orgs.create_organizational_unit(ParentId=root_id, Name=_unique("Dest"))
        ou_id = ou["OrganizationalUnit"]["Id"]

        email = f"{_unique('move')}@example.com"
        acct = orgs.create_account(Email=email, AccountName=_unique("MoveAcct"))
        acct_id = acct["CreateAccountStatus"]["AccountId"]

        orgs.move_account(AccountId=acct_id, SourceParentId=root_id, DestinationParentId=ou_id)

        children = orgs.list_children(ParentId=ou_id, ChildType="ACCOUNT")
        child_ids = [c["Id"] for c in children["Children"]]
        assert acct_id in child_ids


class TestOrganizationsPolicyOperations:
    def test_enable_scp_and_list_policies(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]

        resp = orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")
        assert resp["Root"]["Id"] == root_id

        policies = orgs.list_policies(Filter="SERVICE_CONTROL_POLICY")["Policies"]
        # At minimum the default FullAWSAccess policy should exist
        assert len(policies) >= 1

    def test_create_policy(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")

        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        name = _unique("Policy")
        resp = orgs.create_policy(
            Content=policy_doc,
            Description="Test SCP",
            Name=name,
            Type="SERVICE_CONTROL_POLICY",
        )
        summary = resp["Policy"]["PolicySummary"]
        assert summary["Name"] == name
        assert summary["Type"] == "SERVICE_CONTROL_POLICY"
        assert summary["Id"].startswith("p-")

        policies = orgs.list_policies(Filter="SERVICE_CONTROL_POLICY")["Policies"]
        policy_names = [p["Name"] for p in policies]
        assert name in policy_names

    def test_describe_policy(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")

        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        name = _unique("DescPol")
        created = orgs.create_policy(
            Content=policy_doc,
            Description="Describe test",
            Name=name,
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]

        resp = orgs.describe_policy(PolicyId=policy_id)
        assert resp["Policy"]["PolicySummary"]["Id"] == policy_id
        assert resp["Policy"]["PolicySummary"]["Name"] == name
        assert resp["Policy"]["Content"] is not None

    def test_update_policy(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")

        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        created = orgs.create_policy(
            Content=policy_doc,
            Description="Original",
            Name=_unique("UpdPol"),
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]

        new_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Deny", "Action": "iam:*", "Resource": "*"}],
            }
        )
        resp = orgs.update_policy(
            PolicyId=policy_id, Description="Updated", Content=new_doc, Name="RenamedPolicy"
        )
        assert resp["Policy"]["PolicySummary"]["Description"] == "Updated"
        assert resp["Policy"]["PolicySummary"]["Name"] == "RenamedPolicy"

    def test_attach_policy(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")

        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        created = orgs.create_policy(
            Content=policy_doc,
            Description="Attach test",
            Name=_unique("AttPol"),
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]

        # Attach to the root
        orgs.attach_policy(PolicyId=policy_id, TargetId=root_id)

        # Verify it appears in policies for target
        policies = orgs.list_policies_for_target(TargetId=root_id, Filter="SERVICE_CONTROL_POLICY")[
            "Policies"
        ]
        policy_ids = [p["Id"] for p in policies]
        assert policy_id in policy_ids

    def test_detach_policy(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")

        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        created = orgs.create_policy(
            Content=policy_doc,
            Description="Detach test",
            Name=_unique("DetPol"),
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]

        # Attach then detach from root
        orgs.attach_policy(PolicyId=policy_id, TargetId=root_id)
        orgs.detach_policy(PolicyId=policy_id, TargetId=root_id)

        # Verify it no longer appears (except default FullAWSAccess)
        policies = orgs.list_policies_for_target(TargetId=root_id, Filter="SERVICE_CONTROL_POLICY")[
            "Policies"
        ]
        policy_ids = [p["Id"] for p in policies]
        assert policy_id not in policy_ids

    def test_list_delegated_administrators(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.list_delegated_administrators()
        assert "DelegatedAdministrators" in resp
        assert isinstance(resp["DelegatedAdministrators"], list)

    def test_list_aws_service_access(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.list_aws_service_access_for_organization()
        assert "EnabledServicePrincipals" in resp
        assert isinstance(resp["EnabledServicePrincipals"], list)


class TestOrganizationsTags:
    def test_tag_and_list_tags(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]

        orgs.tag_resource(
            ResourceId=root_id,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "project", "Value": "robotocore"},
            ],
        )

        resp = orgs.list_tags_for_resource(ResourceId=root_id)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["project"] == "robotocore"

    def test_untag_resource(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]

        orgs.tag_resource(
            ResourceId=root_id,
            Tags=[
                {"Key": "remove-me", "Value": "yes"},
                {"Key": "keep-me", "Value": "yes"},
            ],
        )

        orgs.untag_resource(ResourceId=root_id, TagKeys=["remove-me"])

        resp = orgs.list_tags_for_resource(ResourceId=root_id)
        tag_keys = [t["Key"] for t in resp["Tags"]]
        assert "remove-me" not in tag_keys
        assert "keep-me" in tag_keys

    def test_tag_organizational_unit(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        ou = orgs.create_organizational_unit(ParentId=root_id, Name=_unique("TagOU"))
        ou_id = ou["OrganizationalUnit"]["Id"]

        orgs.tag_resource(ResourceId=ou_id, Tags=[{"Key": "team", "Value": "platform"}])

        resp = orgs.list_tags_for_resource(ResourceId=ou_id)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["team"] == "platform"


class TestOrganizationsAutoCoverage:
    """Auto-generated coverage tests for organizations."""

    @pytest.fixture
    def client(self):
        return make_client("organizations")

    def test_list_aws_service_access_for_organization(self, client):
        """ListAWSServiceAccessForOrganization returns a response."""
        resp = client.list_aws_service_access_for_organization()
        assert "EnabledServicePrincipals" in resp
