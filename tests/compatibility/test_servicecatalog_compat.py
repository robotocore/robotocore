"""Service Catalog compatibility tests."""

import uuid

import pytest

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
