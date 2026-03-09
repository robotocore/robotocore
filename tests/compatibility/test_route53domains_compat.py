"""Route 53 Domains compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client

CONTACT = {
    "FirstName": "Test",
    "LastName": "User",
    "ContactType": "PERSON",
    "AddressLine1": "123 Main St",
    "City": "Seattle",
    "State": "WA",
    "CountryCode": "US",
    "ZipCode": "98101",
    "PhoneNumber": "+1.2065551234",
    "Email": "test@example.com",
}


def _unique_domain():
    return f"{uuid.uuid4().hex[:12]}.com"


def _register(client, domain_name):
    return client.register_domain(
        DomainName=domain_name,
        DurationInYears=2,
        AdminContact=CONTACT,
        RegistrantContact=CONTACT,
        TechContact=CONTACT,
    )


@pytest.fixture
def route53domains():
    return make_client("route53domains", region_name="us-east-1")


@pytest.fixture(scope="module")
def shared_domain():
    """Reuse an already-registered domain from earlier tests (avoids domain limit)."""
    client = make_client("route53domains", region_name="us-east-1")
    # Get any existing domain from the list
    domains = client.list_domains()["Domains"]
    if domains:
        domain = domains[0]["DomainName"]
    else:
        # Fallback: register one if none exist
        domain = f"shared-{uuid.uuid4().hex[:8]}.com"
        client.register_domain(
            DomainName=domain,
            DurationInYears=2,
            AdminContact=CONTACT,
            RegistrantContact=CONTACT,
            TechContact=CONTACT,
        )
    # Get an operation ID from existing operations
    ops = client.list_operations()["Operations"]
    op_id = ops[0]["OperationId"] if ops else None
    return {"client": client, "domain": domain, "operation_id": op_id}


class TestRoute53DomainsOperations:
    def test_list_domains(self, route53domains):
        """ListDomains returns a list of domains."""
        response = route53domains.list_domains()
        assert "Domains" in response
        assert isinstance(response["Domains"], list)

    def test_list_operations(self, route53domains):
        """ListOperations returns a list of operations."""
        response = route53domains.list_operations()
        assert "Operations" in response
        assert isinstance(response["Operations"], list)

    def test_list_domains_status_code(self, route53domains):
        """ListDomains returns HTTP 200."""
        response = route53domains.list_domains()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_operations_status_code(self, route53domains):
        """ListOperations returns HTTP 200."""
        response = route53domains.list_operations()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_register_domain(self, route53domains):
        """RegisterDomain registers a domain and returns an operation ID."""
        domain = _unique_domain()
        resp = _register(route53domains, domain)
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_domain(self, route53domains):
        """DeleteDomain deletes a registered domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.delete_domain(DomainName=domain)
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_domain_detail(self, route53domains):
        """GetDomainDetail returns details for a registered domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.get_domain_detail(DomainName=domain)
        assert "DomainName" in resp
        assert resp["DomainName"] == domain

    def test_update_domain_nameservers(self, route53domains):
        """UpdateDomainNameservers updates nameservers for a domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.update_domain_nameservers(
            DomainName=domain,
            Nameservers=[
                {"Name": "ns1.example.com"},
                {"Name": "ns2.example.com"},
            ],
        )
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_operation_detail(self, route53domains):
        """GetOperationDetail returns details for an operation."""
        domain = _unique_domain()
        reg = _register(route53domains, domain)
        op_id = reg["OperationId"]
        resp = route53domains.get_operation_detail(OperationId=op_id)
        assert "OperationId" in resp
        assert resp["OperationId"] == op_id

    def test_check_domain_availability(self, route53domains):
        """CheckDomainAvailability returns availability status."""
        resp = route53domains.check_domain_availability(DomainName="example-test-12345.com")
        assert "Availability" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_check_domain_transferability(self, route53domains):
        """CheckDomainTransferability returns transferability info."""
        resp = route53domains.check_domain_transferability(DomainName="example-test-12345.com")
        assert "Transferability" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_domain_suggestions(self, route53domains):
        """GetDomainSuggestions returns domain name suggestions."""
        resp = route53domains.get_domain_suggestions(
            DomainName="example.com",
            SuggestionCount=5,
            OnlyAvailable=True,
        )
        assert "SuggestionsList" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_prices(self, route53domains):
        """ListPrices returns pricing information."""
        resp = route53domains.list_prices()
        assert "Prices" in resp
        assert isinstance(resp["Prices"], list)

    def test_view_billing(self, route53domains):
        """ViewBilling returns billing records."""
        resp = route53domains.view_billing()
        assert "BillingRecords" in resp
        assert isinstance(resp["BillingRecords"], list)

    def test_update_tags_for_domain(self, route53domains):
        """UpdateTagsForDomain adds tags to a domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.update_tags_for_domain(
            DomainName=domain,
            TagsToUpdate=[{"Key": "env", "Value": "test"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_tags_for_domain(self, route53domains):
        """ListTagsForDomain returns tags for a domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        route53domains.update_tags_for_domain(
            DomainName=domain,
            TagsToUpdate=[{"Key": "env", "Value": "test"}],
        )
        resp = route53domains.list_tags_for_domain(DomainName=domain)
        assert "TagList" in resp
        assert isinstance(resp["TagList"], list)

    def test_delete_tags_for_domain(self, route53domains):
        """DeleteTagsForDomain removes tags from a domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        route53domains.update_tags_for_domain(
            DomainName=domain,
            TagsToUpdate=[{"Key": "env", "Value": "test"}],
        )
        resp = route53domains.delete_tags_for_domain(
            DomainName=domain,
            TagsToDelete=["env"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enable_domain_auto_renew(self, route53domains):
        """EnableDomainAutoRenew enables auto-renew."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.enable_domain_auto_renew(DomainName=domain)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disable_domain_auto_renew(self, route53domains):
        """DisableDomainAutoRenew disables auto-renew."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.disable_domain_auto_renew(DomainName=domain)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enable_domain_transfer_lock(self, route53domains):
        """EnableDomainTransferLock enables transfer lock."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.enable_domain_transfer_lock(DomainName=domain)
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disable_domain_transfer_lock(self, route53domains):
        """DisableDomainTransferLock disables transfer lock."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.disable_domain_transfer_lock(DomainName=domain)
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_retrieve_domain_auth_code(self, route53domains):
        """RetrieveDomainAuthCode returns auth code for a domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.retrieve_domain_auth_code(DomainName=domain)
        assert "AuthCode" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_contact_reachability_status(self, route53domains):
        """GetContactReachabilityStatus returns reachability info."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.get_contact_reachability_status(domainName=domain)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_domain_contact(self, route53domains):
        """UpdateDomainContact updates contact info for a domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        updated_contact = CONTACT.copy()
        updated_contact["FirstName"] = "Updated"
        resp = route53domains.update_domain_contact(
            DomainName=domain,
            AdminContact=updated_contact,
        )
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_domain_contact_privacy(self, route53domains):
        """UpdateDomainContactPrivacy updates privacy settings."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.update_domain_contact_privacy(
            DomainName=domain,
            AdminPrivacy=True,
            RegistrantPrivacy=True,
            TechPrivacy=True,
        )
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_renew_domain(self, route53domains):
        """RenewDomain renews a registered domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.renew_domain(
            DomainName=domain,
            CurrentExpiryYear=2027,
            DurationInYears=1,
        )
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_transfer_domain_to_another_aws_account(self, route53domains):
        """TransferDomainToAnotherAwsAccount initiates a transfer."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.transfer_domain_to_another_aws_account(
            DomainName=domain,
            AccountId="987654321012",
        )
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_accept_domain_transfer_from_another_aws_account(self, route53domains):
        """AcceptDomainTransferFromAnotherAwsAccount accepts a transfer."""
        domain = _unique_domain()
        _register(route53domains, domain)
        transfer = route53domains.transfer_domain_to_another_aws_account(
            DomainName=domain,
            AccountId="987654321012",
        )
        password = transfer.get("Password", "dummy")
        resp = route53domains.accept_domain_transfer_from_another_aws_account(
            DomainName=domain,
            Password=password,
        )
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_cancel_domain_transfer_to_another_aws_account(self, route53domains):
        """CancelDomainTransferToAnotherAwsAccount cancels a pending transfer."""
        domain = _unique_domain()
        _register(route53domains, domain)
        route53domains.transfer_domain_to_another_aws_account(
            DomainName=domain,
            AccountId="987654321012",
        )
        resp = route53domains.cancel_domain_transfer_to_another_aws_account(
            DomainName=domain,
        )
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_reject_domain_transfer_from_another_aws_account(self, route53domains):
        """RejectDomainTransferFromAnotherAwsAccount rejects a transfer."""
        domain = _unique_domain()
        _register(route53domains, domain)
        route53domains.transfer_domain_to_another_aws_account(
            DomainName=domain,
            AccountId="987654321012",
        )
        resp = route53domains.reject_domain_transfer_from_another_aws_account(
            DomainName=domain,
        )
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_transfer_domain(self, route53domains):
        """TransferDomain validates input and returns an error for invalid domains."""
        domain = _unique_domain()
        with pytest.raises(Exception) as exc_info:
            route53domains.transfer_domain(
                DomainName=domain,
                DurationInYears=1,
                AdminContact=CONTACT,
                RegistrantContact=CONTACT,
                TechContact=CONTACT,
            )
        # Server responds with InvalidInput - proves the operation is implemented
        assert "InvalidInput" in str(type(exc_info.value).__name__) or "InvalidInput" in str(
            exc_info.value
        )

    def test_resend_contact_reachability_email(self, route53domains, shared_domain):
        """ResendContactReachabilityEmail resends verification email."""
        domain = shared_domain["domain"]
        resp = route53domains.resend_contact_reachability_email(domainName=domain)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_push_domain(self, route53domains, shared_domain):
        """PushDomain pushes a domain to another registrar."""
        domain = shared_domain["domain"]
        resp = route53domains.push_domain(DomainName=domain, Target="ANOTHER_REGISTRAR")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_resend_operation_authorization(self, route53domains, shared_domain):
        """ResendOperationAuthorization resends auth for an operation."""
        op_id = shared_domain["operation_id"]
        resp = route53domains.resend_operation_authorization(OperationId=op_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_associate_delegation_signer_to_domain(self, route53domains):
        """AssociateDelegationSignerToDomain is implemented."""
        with pytest.raises(Exception) as exc_info:
            route53domains.associate_delegation_signer_to_domain(
                DomainName="nonexistent-domain-12345.com",
                SigningAttributes={
                    "Algorithm": 13,
                    "Flags": 257,
                    "PublicKey": "dGVzdA==",
                },
            )
        assert "InvalidInput" in str(type(exc_info.value).__name__) or "InvalidInput" in str(
            exc_info.value
        )

    def test_disassociate_delegation_signer_from_domain(self, route53domains):
        """DisassociateDelegationSignerFromDomain is implemented (returns InvalidInput)."""
        with pytest.raises(Exception) as exc_info:
            route53domains.disassociate_delegation_signer_from_domain(
                DomainName="nonexistent-domain-12345.com",
                Id="fake-signer-id",
            )
        assert "InvalidInput" in str(type(exc_info.value).__name__) or "InvalidInput" in str(
            exc_info.value
        )
