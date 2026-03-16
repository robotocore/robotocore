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
                    pass  # best-effort cleanup
    except Exception:
        pass  # best-effort cleanup

    # Delete OUs (must delete children first)
    try:
        roots = client.list_roots()["Roots"]
        for root in roots:
            _delete_ous_recursive(client, root["Id"])
    except Exception:
        pass  # best-effort cleanup

    # Delete non-default policies
    try:
        for ptype in ["SERVICE_CONTROL_POLICY", "TAG_POLICY"]:
            policies = client.list_policies(Filter=ptype).get("Policies", [])
            for p in policies:
                if not p["AwsManaged"]:
                    try:
                        client.delete_policy(PolicyId=p["Id"])
                    except Exception:
                        pass  # best-effort cleanup
    except Exception:
        pass  # best-effort cleanup

    try:
        client.delete_organization()
    except Exception:
        pass  # best-effort cleanup


def _delete_ous_recursive(client, parent_id):
    """Recursively delete organizational units under a parent."""
    try:
        ous = client.list_organizational_units_for_parent(ParentId=parent_id)["OrganizationalUnits"]
        for ou in ous:
            _delete_ous_recursive(client, ou["Id"])
            try:
                client.delete_organizational_unit(OrganizationalUnitId=ou["Id"])
            except Exception:
                pass  # best-effort cleanup
    except Exception:
        pass  # best-effort cleanup


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


class TestOrganizationsServiceAccess:
    """Tests for AWS service access operations."""

    def test_enable_aws_service_access(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        orgs.enable_aws_service_access(ServicePrincipal="ssm.amazonaws.com")
        resp = orgs.list_aws_service_access_for_organization()
        principals = [p["ServicePrincipal"] for p in resp["EnabledServicePrincipals"]]
        assert "ssm.amazonaws.com" in principals

    def test_disable_aws_service_access(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        orgs.enable_aws_service_access(ServicePrincipal="ssm.amazonaws.com")
        orgs.disable_aws_service_access(ServicePrincipal="ssm.amazonaws.com")
        resp = orgs.list_aws_service_access_for_organization()
        principals = [p["ServicePrincipal"] for p in resp["EnabledServicePrincipals"]]
        assert "ssm.amazonaws.com" not in principals

    def test_list_aws_service_access_for_organization(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.list_aws_service_access_for_organization()
        assert "EnabledServicePrincipals" in resp
        assert isinstance(resp["EnabledServicePrincipals"], list)


class TestOrganizationsDelegatedAdmin:
    """Tests for delegated administrator operations."""

    def test_register_delegated_administrator(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        email = f"{_unique('deleg')}@example.com"
        acct = orgs.create_account(Email=email, AccountName=_unique("DelegAcct"))
        acct_id = acct["CreateAccountStatus"]["AccountId"]
        orgs.enable_aws_service_access(ServicePrincipal="ssm.amazonaws.com")
        orgs.register_delegated_administrator(
            AccountId=acct_id, ServicePrincipal="ssm.amazonaws.com"
        )
        resp = orgs.list_delegated_administrators(ServicePrincipal="ssm.amazonaws.com")
        admin_ids = [a["Id"] for a in resp["DelegatedAdministrators"]]
        assert acct_id in admin_ids

    def test_deregister_delegated_administrator(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        email = f"{_unique('deleg2')}@example.com"
        acct = orgs.create_account(Email=email, AccountName=_unique("DeregAcct"))
        acct_id = acct["CreateAccountStatus"]["AccountId"]
        orgs.enable_aws_service_access(ServicePrincipal="ssm.amazonaws.com")
        orgs.register_delegated_administrator(
            AccountId=acct_id, ServicePrincipal="ssm.amazonaws.com"
        )
        orgs.deregister_delegated_administrator(
            AccountId=acct_id, ServicePrincipal="ssm.amazonaws.com"
        )
        resp = orgs.list_delegated_administrators(ServicePrincipal="ssm.amazonaws.com")
        admin_ids = [a["Id"] for a in resp["DelegatedAdministrators"]]
        assert acct_id not in admin_ids

    def test_list_delegated_services_for_account(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        email = f"{_unique('svc')}@example.com"
        acct = orgs.create_account(Email=email, AccountName=_unique("SvcAcct"))
        acct_id = acct["CreateAccountStatus"]["AccountId"]
        orgs.enable_aws_service_access(ServicePrincipal="ssm.amazonaws.com")
        orgs.register_delegated_administrator(
            AccountId=acct_id, ServicePrincipal="ssm.amazonaws.com"
        )
        resp = orgs.list_delegated_services_for_account(AccountId=acct_id)
        assert "DelegatedServices" in resp
        principals = [s["ServicePrincipal"] for s in resp["DelegatedServices"]]
        assert "ssm.amazonaws.com" in principals


class TestOrganizationsOUCRUD:
    """Tests for full OU lifecycle including delete."""

    def test_delete_organizational_unit(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        name = _unique("DelOU")
        ou = orgs.create_organizational_unit(ParentId=root_id, Name=name)
        ou_id = ou["OrganizationalUnit"]["Id"]

        orgs.delete_organizational_unit(OrganizationalUnitId=ou_id)

        ous = orgs.list_organizational_units_for_parent(ParentId=root_id)["OrganizationalUnits"]
        ou_ids = [o["Id"] for o in ous]
        assert ou_id not in ou_ids

    def test_delete_organizational_unit_not_found(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        with pytest.raises(orgs.exceptions.ClientError) as exc_info:
            orgs.delete_organizational_unit(OrganizationalUnitId="ou-0000-00000000")
        # Server returns an error (code varies by implementation)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] >= 400

    def test_ou_full_lifecycle(self, orgs):
        """Create -> describe -> update -> list -> delete OU."""
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        name = _unique("LifeOU")
        ou = orgs.create_organizational_unit(ParentId=root_id, Name=name)
        ou_id = ou["OrganizationalUnit"]["Id"]
        assert ou["OrganizationalUnit"]["Name"] == name

        desc = orgs.describe_organizational_unit(OrganizationalUnitId=ou_id)
        assert desc["OrganizationalUnit"]["Id"] == ou_id

        new_name = _unique("RenamedOU")
        upd = orgs.update_organizational_unit(OrganizationalUnitId=ou_id, Name=new_name)
        assert upd["OrganizationalUnit"]["Name"] == new_name

        ous = orgs.list_organizational_units_for_parent(ParentId=root_id)["OrganizationalUnits"]
        assert ou_id in [o["Id"] for o in ous]

        orgs.delete_organizational_unit(OrganizationalUnitId=ou_id)
        ous_after = orgs.list_organizational_units_for_parent(ParentId=root_id)[
            "OrganizationalUnits"
        ]
        assert ou_id not in [o["Id"] for o in ous_after]


class TestOrganizationsOUExtra:
    """Tests for OU describe/update operations."""

    def test_describe_organizational_unit(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        name = _unique("DescOU")
        ou = orgs.create_organizational_unit(ParentId=root_id, Name=name)
        ou_id = ou["OrganizationalUnit"]["Id"]
        resp = orgs.describe_organizational_unit(OrganizationalUnitId=ou_id)
        assert resp["OrganizationalUnit"]["Id"] == ou_id
        assert resp["OrganizationalUnit"]["Name"] == name

    def test_update_organizational_unit(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        ou = orgs.create_organizational_unit(ParentId=root_id, Name=_unique("OldOU"))
        ou_id = ou["OrganizationalUnit"]["Id"]
        new_name = _unique("NewOU")
        resp = orgs.update_organizational_unit(OrganizationalUnitId=ou_id, Name=new_name)
        assert resp["OrganizationalUnit"]["Name"] == new_name


class TestOrganizationsAccountOps:
    """Tests for account-related operations."""

    def test_describe_account(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        email = f"{_unique('desc')}@example.com"
        acct = orgs.create_account(Email=email, AccountName=_unique("DescAcct"))
        acct_id = acct["CreateAccountStatus"]["AccountId"]
        resp = orgs.describe_account(AccountId=acct_id)
        assert resp["Account"]["Id"] == acct_id
        assert resp["Account"]["Status"] == "ACTIVE"

    def test_list_accounts_for_parent(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        resp = orgs.list_accounts_for_parent(ParentId=root_id)
        assert "Accounts" in resp
        assert len(resp["Accounts"]) >= 1  # at least master

    def test_list_parents(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        accounts = orgs.list_accounts()["Accounts"]
        master_id = accounts[0]["Id"]
        resp = orgs.list_parents(ChildId=master_id)
        assert "Parents" in resp
        assert len(resp["Parents"]) == 1
        assert resp["Parents"][0]["Type"] == "ROOT"

    def test_describe_create_account_status(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        email = f"{_unique('cas')}@example.com"
        create_resp = orgs.create_account(Email=email, AccountName=_unique("CasAcct"))
        request_id = create_resp["CreateAccountStatus"]["Id"]
        resp = orgs.describe_create_account_status(CreateAccountRequestId=request_id)
        assert resp["CreateAccountStatus"]["Id"] == request_id
        assert resp["CreateAccountStatus"]["State"] in ("SUCCEEDED", "IN_PROGRESS")

    def test_close_account(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        email = f"{_unique('close')}@example.com"
        acct = orgs.create_account(Email=email, AccountName=_unique("CloseAcct"))
        acct_id = acct["CreateAccountStatus"]["AccountId"]
        orgs.close_account(AccountId=acct_id)
        resp = orgs.describe_account(AccountId=acct_id)
        assert resp["Account"]["Status"] in ("SUSPENDED", "PENDING_CLOSURE")


class TestOrganizationsPolicyCRUD:
    """Tests for full policy lifecycle including delete and OU attach."""

    def test_delete_policy(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        name = _unique("DelPol")
        created = orgs.create_policy(
            Content=policy_doc,
            Description="Delete test",
            Name=name,
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]

        orgs.delete_policy(PolicyId=policy_id)

        policies = orgs.list_policies(Filter="SERVICE_CONTROL_POLICY")["Policies"]
        policy_ids = [p["Id"] for p in policies]
        assert policy_id not in policy_ids

    def test_policy_full_lifecycle(self, orgs):
        """Create -> describe -> update -> list -> delete policy."""
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        name = _unique("LifePol")
        created = orgs.create_policy(
            Content=policy_doc,
            Description="Lifecycle",
            Name=name,
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]
        assert created["Policy"]["PolicySummary"]["Name"] == name

        desc = orgs.describe_policy(PolicyId=policy_id)
        assert desc["Policy"]["PolicySummary"]["Id"] == policy_id

        new_name = _unique("UpdatedPol")
        upd = orgs.update_policy(PolicyId=policy_id, Name=new_name, Description="Updated")
        assert upd["Policy"]["PolicySummary"]["Name"] == new_name
        assert upd["Policy"]["PolicySummary"]["Description"] == "Updated"

        policies = orgs.list_policies(Filter="SERVICE_CONTROL_POLICY")["Policies"]
        assert policy_id in [p["Id"] for p in policies]

        orgs.delete_policy(PolicyId=policy_id)
        policies_after = orgs.list_policies(Filter="SERVICE_CONTROL_POLICY")["Policies"]
        assert policy_id not in [p["Id"] for p in policies_after]

    def test_attach_policy_to_ou(self, orgs):
        """Attach and detach a policy to/from an OU."""
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")

        ou = orgs.create_organizational_unit(ParentId=root_id, Name=_unique("PolOU"))
        ou_id = ou["OrganizationalUnit"]["Id"]

        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        created = orgs.create_policy(
            Content=policy_doc,
            Description="OU attach",
            Name=_unique("OUPol"),
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]

        orgs.attach_policy(PolicyId=policy_id, TargetId=ou_id)

        # Verify via list_targets_for_policy
        targets = orgs.list_targets_for_policy(PolicyId=policy_id)["Targets"]
        target_ids = [t["TargetId"] for t in targets]
        assert ou_id in target_ids

        # Verify via list_policies_for_target
        pols = orgs.list_policies_for_target(TargetId=ou_id, Filter="SERVICE_CONTROL_POLICY")[
            "Policies"
        ]
        pol_ids = [p["Id"] for p in pols]
        assert policy_id in pol_ids

        # Detach and verify
        orgs.detach_policy(PolicyId=policy_id, TargetId=ou_id)
        pols_after = orgs.list_policies_for_target(TargetId=ou_id, Filter="SERVICE_CONTROL_POLICY")[
            "Policies"
        ]
        pol_ids_after = [p["Id"] for p in pols_after]
        assert policy_id not in pol_ids_after

    def test_attach_policy_to_account(self, orgs):
        """Attach a policy to a member account."""
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")

        email = f"{_unique('polacct')}@example.com"
        acct = orgs.create_account(Email=email, AccountName=_unique("PolAcct"))
        acct_id = acct["CreateAccountStatus"]["AccountId"]

        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        )
        created = orgs.create_policy(
            Content=policy_doc,
            Description="Account attach",
            Name=_unique("AcctPol"),
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]

        orgs.attach_policy(PolicyId=policy_id, TargetId=acct_id)

        pols = orgs.list_policies_for_target(TargetId=acct_id, Filter="SERVICE_CONTROL_POLICY")[
            "Policies"
        ]
        pol_ids = [p["Id"] for p in pols]
        assert policy_id in pol_ids

    def test_tag_policy(self, orgs):
        """Tag a policy resource."""
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
            Description="Tag test",
            Name=_unique("TagPol"),
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]

        orgs.tag_resource(ResourceId=policy_id, Tags=[{"Key": "env", "Value": "test"}])

        resp = orgs.list_tags_for_resource(ResourceId=policy_id)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"

        orgs.untag_resource(ResourceId=policy_id, TagKeys=["env"])
        resp2 = orgs.list_tags_for_resource(ResourceId=policy_id)
        tag_keys = [t["Key"] for t in resp2["Tags"]]
        assert "env" not in tag_keys


class TestOrganizationsRemoveAccount:
    """Tests for removing accounts from an organization."""

    def test_remove_account_from_organization(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        email = f"{_unique('rem')}@example.com"
        acct = orgs.create_account(Email=email, AccountName=_unique("RemAcct"))
        acct_id = acct["CreateAccountStatus"]["AccountId"]

        # Verify account exists
        accounts_before = orgs.list_accounts()["Accounts"]
        acct_ids_before = [a["Id"] for a in accounts_before]
        assert acct_id in acct_ids_before

        orgs.remove_account_from_organization(AccountId=acct_id)

        # Verify account is removed
        accounts_after = orgs.list_accounts()["Accounts"]
        acct_ids_after = [a["Id"] for a in accounts_after]
        assert acct_id not in acct_ids_after


class TestOrganizationsPolicyExtra:
    """Tests for additional policy operations."""

    def test_disable_policy_type(self, orgs):
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="TAG_POLICY")
        resp = orgs.disable_policy_type(RootId=root_id, PolicyType="TAG_POLICY")
        assert resp["Root"]["Id"] == root_id
        policy_types = [pt["Type"] for pt in resp["Root"].get("PolicyTypes", [])]
        assert "TAG_POLICY" not in [
            pt
            for pt in policy_types
            if any(
                p.get("Status") == "ENABLED"
                for p in resp["Root"].get("PolicyTypes", [])
                if p["Type"] == "TAG_POLICY"
            )
        ]

    def test_list_targets_for_policy(self, orgs):
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
            Description="Targets test",
            Name=_unique("TgtPol"),
            Type="SERVICE_CONTROL_POLICY",
        )
        policy_id = created["Policy"]["PolicySummary"]["Id"]
        orgs.attach_policy(PolicyId=policy_id, TargetId=root_id)
        resp = orgs.list_targets_for_policy(PolicyId=policy_id)
        assert "Targets" in resp
        target_ids = [t["TargetId"] for t in resp["Targets"]]
        assert root_id in target_ids


class TestOrganizationsHandshakeOperations:
    """Tests for handshake operations: invite, describe, cancel, list."""

    def test_list_handshakes_for_account(self, orgs):
        """ListHandshakesForAccount returns handshake list (may be empty)."""
        resp = orgs.list_handshakes_for_account()
        assert "Handshakes" in resp
        assert isinstance(resp["Handshakes"], list)

    def test_list_handshakes_for_organization(self, orgs):
        """ListHandshakesForOrganization returns handshake list."""
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.list_handshakes_for_organization()
        assert "Handshakes" in resp
        assert isinstance(resp["Handshakes"], list)

    def test_invite_account_and_describe_handshake(self, orgs):
        """InviteAccountToOrganization creates a handshake that can be described."""
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.invite_account_to_organization(Target={"Id": "333333333333", "Type": "ACCOUNT"})
        handshake = resp["Handshake"]
        assert "Id" in handshake
        assert handshake["State"] == "OPEN"
        assert handshake["Action"] == "INVITE"

        # Describe the handshake by ID
        desc = orgs.describe_handshake(HandshakeId=handshake["Id"])
        assert desc["Handshake"]["Id"] == handshake["Id"]
        assert desc["Handshake"]["State"] == "OPEN"

    def test_invite_account_appears_in_list(self, orgs):
        """InviteAccountToOrganization handshake shows in list operations."""
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.invite_account_to_organization(Target={"Id": "444444444444", "Type": "ACCOUNT"})
        handshake_id = resp["Handshake"]["Id"]

        # Should appear in org handshakes
        org_handshakes = orgs.list_handshakes_for_organization()
        hs_ids = [h["Id"] for h in org_handshakes["Handshakes"]]
        assert handshake_id in hs_ids

        # Should appear in account handshakes
        acct_handshakes = orgs.list_handshakes_for_account()
        acct_hs_ids = [h["Id"] for h in acct_handshakes["Handshakes"]]
        assert handshake_id in acct_hs_ids

    def test_cancel_handshake(self, orgs):
        """CancelHandshake changes state to CANCELED."""
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.invite_account_to_organization(Target={"Id": "555555555555", "Type": "ACCOUNT"})
        handshake_id = resp["Handshake"]["Id"]

        cancel_resp = orgs.cancel_handshake(HandshakeId=handshake_id)
        assert cancel_resp["Handshake"]["Id"] == handshake_id
        assert cancel_resp["Handshake"]["State"] == "CANCELED"

    def test_describe_handshake_not_found(self, orgs):
        """DescribeHandshake raises error for nonexistent handshake."""
        orgs.create_organization(FeatureSet="ALL")
        with pytest.raises(orgs.exceptions.ClientError) as exc_info:
            orgs.describe_handshake(HandshakeId="h-0000000000")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] >= 400


class TestOrganizationsEffectivePolicy:
    """Tests for DescribeEffectivePolicy operation."""

    def test_describe_effective_policy(self, orgs):
        """DescribeEffectivePolicy returns effective policy for caller account."""
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")

        resp = orgs.describe_effective_policy(PolicyType="SERVICE_CONTROL_POLICY")
        assert "EffectivePolicy" in resp
        effective = resp["EffectivePolicy"]
        assert "PolicyContent" in effective
        assert effective["PolicyType"] == "SERVICE_CONTROL_POLICY"

    def test_describe_effective_policy_with_target(self, orgs):
        """DescribeEffectivePolicy returns policy for specific target account."""
        orgs.create_organization(FeatureSet="ALL")
        root_id = orgs.list_roots()["Roots"][0]["Id"]
        orgs.enable_policy_type(RootId=root_id, PolicyType="SERVICE_CONTROL_POLICY")

        master_id = orgs.list_accounts()["Accounts"][0]["Id"]
        resp = orgs.describe_effective_policy(
            PolicyType="SERVICE_CONTROL_POLICY", TargetId=master_id
        )
        assert "EffectivePolicy" in resp
        assert resp["EffectivePolicy"]["TargetId"] == master_id


class TestOrganizationsEnableAllFeatures:
    """Tests for EnableAllFeatures operation."""

    def test_enable_all_features(self, orgs):
        """EnableAllFeatures on an ALL-features org returns a handshake."""
        orgs.create_organization(FeatureSet="ALL")
        resp = orgs.enable_all_features()
        assert "Handshake" in resp
        handshake = resp["Handshake"]
        assert "Id" in handshake
        assert handshake["Action"] == "ENABLE_ALL_FEATURES"


class TestOrganizationsHandshakeOps:
    """Tests for AcceptHandshake and DeclineHandshake operations."""

    def test_decline_handshake(self, orgs):
        """DeclineHandshake transitions an OPEN handshake to DECLINED."""
        orgs.create_organization(FeatureSet="ALL")
        invite_resp = orgs.invite_account_to_organization(
            Target={"Id": "111111111111", "Type": "ACCOUNT"}
        )
        handshake_id = invite_resp["Handshake"]["Id"]

        resp = orgs.decline_handshake(HandshakeId=handshake_id)
        assert "Handshake" in resp
        assert resp["Handshake"]["State"] == "DECLINED"
        assert resp["Handshake"]["Id"] == handshake_id

    def test_accept_handshake(self, orgs):
        """AcceptHandshake transitions an OPEN handshake to ACCEPTED."""
        orgs.create_organization(FeatureSet="ALL")
        invite_resp = orgs.invite_account_to_organization(
            Target={"Id": "222222222222", "Type": "ACCOUNT"}
        )
        handshake_id = invite_resp["Handshake"]["Id"]

        resp = orgs.accept_handshake(HandshakeId=handshake_id)
        assert "Handshake" in resp
        assert resp["Handshake"]["State"] == "ACCEPTED"
        assert resp["Handshake"]["Id"] == handshake_id
