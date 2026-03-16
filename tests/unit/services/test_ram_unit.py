"""Unit tests for RAM backend behavior that can't be tested reliably in compat tests."""

import pytest
from moto.ram.exceptions import OperationNotPermittedException
from moto.ram.models import ram_backends

ACCOUNT_ID = "123456789012"
REGION = "us-east-1"


class TestEnableSharingWithAwsOrganization:
    def test_raises_when_no_organization_exists(self):
        """Without an org, enable_sharing raises OperationNotPermittedException."""
        backend = ram_backends[ACCOUNT_ID][REGION]
        # Ensure no org exists
        assert backend.organizations_backend.org is None
        with pytest.raises(OperationNotPermittedException):
            backend.enable_sharing_with_aws_organization()

    def test_succeeds_when_organization_exists(self):
        """With an org, enable_sharing_with_aws_organization returns True."""
        backend = ram_backends[ACCOUNT_ID][REGION]
        orgs_backend = backend.organizations_backend
        orgs_backend.create_organization(region=REGION, feature_set="ALL")
        try:
            result = backend.enable_sharing_with_aws_organization()
            assert result["returnValue"] is True
        finally:
            orgs_backend.delete_organization()
