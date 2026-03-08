"""Glacier compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def glacier():
    return make_client("glacier")


def _uid():
    return uuid.uuid4().hex[:8]


class TestGlacierVaultOperations:
    def test_list_vaults_empty_or_populated(self, glacier):
        """list_vaults returns a VaultList."""
        response = glacier.list_vaults(accountId="-")
        assert "VaultList" in response
        assert isinstance(response["VaultList"], list)

    def test_create_vault(self, glacier):
        """create_vault returns 201."""
        name = f"test-vault-{_uid()}"
        response = glacier.create_vault(accountId="-", vaultName=name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 201
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_describe_vault(self, glacier):
        """describe_vault returns vault details."""
        name = f"desc-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.describe_vault(accountId="-", vaultName=name)
        assert response["VaultName"] == name
        assert "VaultARN" in response
        assert "CreationDate" in response
        assert response["NumberOfArchives"] == 0
        assert response["SizeInBytes"] == 0
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_describe_vault_arn_format(self, glacier):
        """describe_vault returns a properly formatted ARN."""
        name = f"arn-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.describe_vault(accountId="-", vaultName=name)
        arn = response["VaultARN"]
        assert arn.startswith("arn:aws:glacier:")
        assert name in arn
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_delete_vault(self, glacier):
        """delete_vault returns 204 and removes the vault."""
        name = f"del-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.delete_vault(accountId="-", vaultName=name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
        # Verify it no longer appears in list
        vaults = glacier.list_vaults(accountId="-")
        names = [v["VaultName"] for v in vaults["VaultList"]]
        assert name not in names

    def test_list_vaults_includes_created_vault(self, glacier):
        """A newly created vault appears in list_vaults."""
        name = f"list-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.list_vaults(accountId="-")
        names = [v["VaultName"] for v in response["VaultList"]]
        assert name in names
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_create_multiple_vaults(self, glacier):
        """Multiple vaults can be created and listed."""
        name_a = f"multi-a-{_uid()}"
        name_b = f"multi-b-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name_a)
        glacier.create_vault(accountId="-", vaultName=name_b)
        response = glacier.list_vaults(accountId="-")
        names = [v["VaultName"] for v in response["VaultList"]]
        assert name_a in names
        assert name_b in names
        glacier.delete_vault(accountId="-", vaultName=name_a)
        glacier.delete_vault(accountId="-", vaultName=name_b)

    def test_describe_vault_has_inventory_date(self, glacier):
        """describe_vault includes LastInventoryDate."""
        name = f"inv-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.describe_vault(accountId="-", vaultName=name)
        assert "LastInventoryDate" in response
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_create_vault_idempotent(self, glacier):
        """Creating the same vault twice succeeds (idempotent)."""
        name = f"idem-vault-{_uid()}"
        r1 = glacier.create_vault(accountId="-", vaultName=name)
        r2 = glacier.create_vault(accountId="-", vaultName=name)
        assert r1["ResponseMetadata"]["HTTPStatusCode"] == 201
        assert r2["ResponseMetadata"]["HTTPStatusCode"] == 201
        glacier.delete_vault(accountId="-", vaultName=name)
