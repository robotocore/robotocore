"""Organizations CreateAccount can assign pre-registered account ids."""

import os
import uuid

import boto3

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


def _client(service: str, account: str = "123456789012", **kw):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=kw.pop("region_name", "us-east-1"),
        aws_access_key_id=kw.pop("aws_access_key_id", account),
        aws_secret_access_key="test",
        **kw,
    )


class TestOrganizationReplay:
    """Replaying a real organization from Terraform needs CreateAccount to reuse known ids."""

    def test_create_account_uses_registered_id(self):
        import requests

        mgmt, wanted = "990000000001", "990000000002"
        email = f"acct-{uuid.uuid4().hex[:6]}@example.com"
        resp = requests.post(
            f"{ENDPOINT_URL}/_robotocore/organizations/account-ids",
            json={"emails": {email: wanted}},
            timeout=10,
        )
        assert resp.status_code == 200
        orgs = _client("organizations", mgmt)
        orgs.create_organization(FeatureSet="ALL")
        status = orgs.create_account(Email=email, AccountName="Replayed")["CreateAccountStatus"]
        assert status["AccountId"] == wanted
        described = _client("organizations", wanted).describe_organization()["Organization"]
        assert described["MasterAccountId"] == mgmt
