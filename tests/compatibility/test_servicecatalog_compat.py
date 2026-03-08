"""Service Catalog compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def servicecatalog():
    return make_client("servicecatalog")


def _uid(prefix="test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestServiceCatalogListOperations:
    def test_list_portfolios(self, servicecatalog):
        response = servicecatalog.list_portfolios()
        assert "PortfolioDetails" in response
        assert isinstance(response["PortfolioDetails"], list)


class TestServiceCatalogPortfolioCRUD:
    def test_create_and_describe_portfolio(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            desc = servicecatalog.describe_portfolio(Id=pid)
            assert desc["PortfolioDetail"]["DisplayName"] == name
            assert desc["PortfolioDetail"]["ProviderName"] == "TestProvider"
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_list_portfolios_after_create(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            listed = servicecatalog.list_portfolios()
            ids = [p["Id"] for p in listed["PortfolioDetails"]]
            assert pid in ids
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_delete_portfolio(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        servicecatalog.delete_portfolio(Id=pid)
        listed = servicecatalog.list_portfolios()
        ids = [p["Id"] for p in listed["PortfolioDetails"]]
        assert pid not in ids

    def test_portfolio_has_arn(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            assert "ARN" in resp["PortfolioDetail"]
            assert "portfolio" in resp["PortfolioDetail"]["ARN"]
        finally:
            servicecatalog.delete_portfolio(Id=pid)


class TestServiceCatalogDescribeProduct:
    def test_describe_product_nonexistent(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_product(Id="prod-doesnotexist123")
        assert exc.value.response["Error"]["Code"] in ("ResourceNotFoundException",)


class TestServiceCatalogProductCRUD:
    def test_create_and_delete_product(self, servicecatalog):
        name = _uid("product")
        resp = servicecatalog.create_product(
            Name=name,
            Owner="TestOwner",
            ProductType="CLOUD_FORMATION_TEMPLATE",
            ProvisioningArtifactParameters={
                "Name": "v1",
                "Info": {"LoadTemplateFromURL": "https://example.com/template.json"},
                "Type": "CLOUD_FORMATION_TEMPLATE",
            },
            IdempotencyToken=uuid.uuid4().hex,
        )
        prod_id = resp["ProductViewDetail"]["ProductViewSummary"]["ProductId"]
        assert prod_id is not None
        servicecatalog.delete_product(Id=prod_id)


class TestServiceCatalogPortfolioShare:
    def test_create_and_delete_portfolio_share(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            share_resp = servicecatalog.create_portfolio_share(
                PortfolioId=pid,
                AccountId="987654321098",
            )
            assert share_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            del_resp = servicecatalog.delete_portfolio_share(
                PortfolioId=pid,
                AccountId="987654321098",
            )
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_list_portfolio_access(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            servicecatalog.create_portfolio_share(
                PortfolioId=pid,
                AccountId="987654321098",
            )
            access = servicecatalog.list_portfolio_access(PortfolioId=pid)
            assert "AccountIds" in access
            assert "987654321098" in access["AccountIds"]
            servicecatalog.delete_portfolio_share(
                PortfolioId=pid,
                AccountId="987654321098",
            )
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_describe_portfolio_shares(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            servicecatalog.create_portfolio_share(
                PortfolioId=pid,
                AccountId="987654321098",
            )
            shares = servicecatalog.describe_portfolio_shares(
                PortfolioId=pid,
                Type="ACCOUNT",
            )
            assert "PortfolioShareDetails" in shares
            servicecatalog.delete_portfolio_share(
                PortfolioId=pid,
                AccountId="987654321098",
            )
        finally:
            servicecatalog.delete_portfolio(Id=pid)
