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

    def test_describe_product_nonexistent_by_name(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_product(Name="nonexistent-product-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_product_by_id(self, servicecatalog):
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
        try:
            desc = servicecatalog.describe_product(Id=prod_id)
            assert "ProductViewSummary" in desc
            assert desc["ProductViewSummary"]["Name"] == name
            assert desc["ProductViewSummary"]["Owner"] == "TestOwner"
            assert desc["ProductViewSummary"]["ProductId"] == prod_id
        finally:
            servicecatalog.delete_product(Id=prod_id)

    def test_describe_product_by_name(self, servicecatalog):
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
        try:
            desc = servicecatalog.describe_product(Name=name)
            assert desc["ProductViewSummary"]["Name"] == name
            assert desc["ProductViewSummary"]["ProductId"] == prod_id
        finally:
            servicecatalog.delete_product(Id=prod_id)

    def test_describe_product_has_provisioning_artifacts_key(self, servicecatalog):
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
        try:
            desc = servicecatalog.describe_product(Id=prod_id)
            assert "ProvisioningArtifacts" in desc
            assert isinstance(desc["ProvisioningArtifacts"], list)
        finally:
            servicecatalog.delete_product(Id=prod_id)


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

    def test_create_product_has_product_arn(self, servicecatalog):
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
        try:
            assert "ProductARN" in resp["ProductViewDetail"]
            assert "product" in resp["ProductViewDetail"]["ProductARN"]
        finally:
            servicecatalog.delete_product(Id=prod_id)

    def test_create_product_has_status(self, servicecatalog):
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
        try:
            assert "Status" in resp["ProductViewDetail"]
        finally:
            servicecatalog.delete_product(Id=prod_id)

    def test_create_product_returns_provisioning_artifact_detail(self, servicecatalog):
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
        try:
            assert "ProvisioningArtifactDetail" in resp
        finally:
            servicecatalog.delete_product(Id=prod_id)

    def test_create_product_returns_tags(self, servicecatalog):
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
            Tags=[{"Key": "env", "Value": "test"}],
            IdempotencyToken=uuid.uuid4().hex,
        )
        prod_id = resp["ProductViewDetail"]["ProductViewSummary"]["ProductId"]
        try:
            assert "Tags" in resp
            assert isinstance(resp["Tags"], list)
        finally:
            servicecatalog.delete_product(Id=prod_id)


class TestServiceCatalogPortfolioDetails:
    def test_portfolio_has_created_time(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            assert "CreatedTime" in resp["PortfolioDetail"]
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_portfolio_has_id(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            assert pid is not None
            assert len(pid) > 0
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_describe_portfolio_has_tags_key(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            desc = servicecatalog.describe_portfolio(Id=pid)
            assert "Tags" in desc
            assert isinstance(desc["Tags"], list)
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_create_portfolio_with_description(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            Description="A test portfolio description",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            desc = servicecatalog.describe_portfolio(Id=pid)
            assert desc["PortfolioDetail"]["Description"] == "A test portfolio description"
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_create_portfolio_with_tags(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            Tags=[{"Key": "env", "Value": "test"}],
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            desc = servicecatalog.describe_portfolio(Id=pid)
            assert "Tags" in desc
        finally:
            servicecatalog.delete_portfolio(Id=pid)


class TestServiceCatalogPortfolioAccessEmpty:
    def test_list_portfolio_access_empty(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            access = servicecatalog.list_portfolio_access(PortfolioId=pid)
            assert "AccountIds" in access
            assert access["AccountIds"] == []
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_describe_portfolio_shares_empty(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            shares = servicecatalog.describe_portfolio_shares(
                PortfolioId=pid,
                Type="ACCOUNT",
            )
            assert "PortfolioShareDetails" in shares
            assert shares["PortfolioShareDetails"] == []
        finally:
            servicecatalog.delete_portfolio(Id=pid)


class TestServiceCatalogListOpsEmpty:
    """Tests for list operations that return empty results."""

    def test_list_accepted_portfolio_shares(self, servicecatalog):
        resp = servicecatalog.list_accepted_portfolio_shares()
        assert "PortfolioDetails" in resp
        assert isinstance(resp["PortfolioDetails"], list)

    def test_list_record_history(self, servicecatalog):
        resp = servicecatalog.list_record_history()
        assert "RecordDetails" in resp
        assert isinstance(resp["RecordDetails"], list)

    def test_list_service_actions(self, servicecatalog):
        resp = servicecatalog.list_service_actions()
        assert "ServiceActionSummaries" in resp
        assert isinstance(resp["ServiceActionSummaries"], list)

    def test_list_tag_options(self, servicecatalog):
        resp = servicecatalog.list_tag_options()
        assert "TagOptionDetails" in resp
        assert isinstance(resp["TagOptionDetails"], list)

    def test_list_provisioned_product_plans(self, servicecatalog):
        resp = servicecatalog.list_provisioned_product_plans()
        assert "ProvisionedProductPlans" in resp
        assert isinstance(resp["ProvisionedProductPlans"], list)


class TestServiceCatalogListOpsWithResources:
    """Tests for list operations that need existing resources."""

    @pytest.fixture
    def portfolio_and_product(self, servicecatalog):
        """Create a portfolio with an associated product."""
        pid = servicecatalog.create_portfolio(
            DisplayName=_uid("lp-pf"),
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )["PortfolioDetail"]["Id"]
        prod = servicecatalog.create_product(
            Name=_uid("lp-prod"),
            Owner="TestOwner",
            ProductType="CLOUD_FORMATION_TEMPLATE",
            ProvisioningArtifactParameters={
                "Name": "v1",
                "Info": {"LoadTemplateFromURL": "https://example.com/t.json"},
                "Type": "CLOUD_FORMATION_TEMPLATE",
            },
            IdempotencyToken=uuid.uuid4().hex,
        )
        prod_id = prod["ProductViewDetail"]["ProductViewSummary"]["ProductId"]
        servicecatalog.associate_product_with_portfolio(ProductId=prod_id, PortfolioId=pid)
        pa_resp = servicecatalog.list_provisioning_artifacts(ProductId=prod_id)
        pa_id = pa_resp["ProvisioningArtifactDetails"][0]["Id"]
        yield {"portfolio_id": pid, "product_id": prod_id, "pa_id": pa_id}
        servicecatalog.disassociate_product_from_portfolio(ProductId=prod_id, PortfolioId=pid)
        servicecatalog.delete_product(Id=prod_id)
        servicecatalog.delete_portfolio(Id=pid)

    def test_list_constraints_for_portfolio(self, servicecatalog, portfolio_and_product):
        resp = servicecatalog.list_constraints_for_portfolio(
            PortfolioId=portfolio_and_product["portfolio_id"]
        )
        assert "ConstraintDetails" in resp
        assert isinstance(resp["ConstraintDetails"], list)

    def test_list_launch_paths(self, servicecatalog, portfolio_and_product):
        resp = servicecatalog.list_launch_paths(ProductId=portfolio_and_product["product_id"])
        assert "LaunchPathSummaries" in resp
        assert isinstance(resp["LaunchPathSummaries"], list)

    def test_list_portfolios_for_product(self, servicecatalog, portfolio_and_product):
        resp = servicecatalog.list_portfolios_for_product(
            ProductId=portfolio_and_product["product_id"]
        )
        assert "PortfolioDetails" in resp
        assert isinstance(resp["PortfolioDetails"], list)
        pf_ids = [p["Id"] for p in resp["PortfolioDetails"]]
        assert portfolio_and_product["portfolio_id"] in pf_ids

    def test_list_principals_for_portfolio(self, servicecatalog, portfolio_and_product):
        resp = servicecatalog.list_principals_for_portfolio(
            PortfolioId=portfolio_and_product["portfolio_id"]
        )
        assert "Principals" in resp
        assert isinstance(resp["Principals"], list)

    def test_list_provisioning_artifacts(self, servicecatalog, portfolio_and_product):
        resp = servicecatalog.list_provisioning_artifacts(
            ProductId=portfolio_and_product["product_id"]
        )
        assert "ProvisioningArtifactDetails" in resp
        assert isinstance(resp["ProvisioningArtifactDetails"], list)
        assert len(resp["ProvisioningArtifactDetails"]) >= 1
        pa = resp["ProvisioningArtifactDetails"][0]
        assert "Id" in pa
        assert "Name" in pa

    def test_list_budgets_for_resource(self, servicecatalog, portfolio_and_product):
        resp = servicecatalog.list_budgets_for_resource(
            ResourceId=portfolio_and_product["product_id"]
        )
        assert "Budgets" in resp
        assert isinstance(resp["Budgets"], list)

    def test_list_service_actions_for_provisioning_artifact(
        self, servicecatalog, portfolio_and_product
    ):
        resp = servicecatalog.list_service_actions_for_provisioning_artifact(
            ProductId=portfolio_and_product["product_id"],
            ProvisioningArtifactId=portfolio_and_product["pa_id"],
        )
        assert "ServiceActionSummaries" in resp
        assert isinstance(resp["ServiceActionSummaries"], list)

    def test_list_organization_portfolio_access(self, servicecatalog, portfolio_and_product):
        resp = servicecatalog.list_organization_portfolio_access(
            PortfolioId=portfolio_and_product["portfolio_id"],
            OrganizationNodeType="ACCOUNT",
        )
        assert "OrganizationNodes" in resp
        assert isinstance(resp["OrganizationNodes"], list)

    def test_list_resources_for_tag_option(self, servicecatalog):
        resp = servicecatalog.list_resources_for_tag_option(TagOptionId="to-fake")
        assert "ResourceDetails" in resp
        assert isinstance(resp["ResourceDetails"], list)

    def test_list_provisioning_artifacts_for_service_action(self, servicecatalog):
        resp = servicecatalog.list_provisioning_artifacts_for_service_action(
            ServiceActionId="act-fake123"
        )
        assert "ProvisioningArtifactViews" in resp
        assert isinstance(resp["ProvisioningArtifactViews"], list)

    def test_list_stack_instances_for_provisioned_product(self, servicecatalog):
        resp = servicecatalog.list_stack_instances_for_provisioned_product(
            ProvisionedProductId="pp-fake"
        )
        assert "StackInstances" in resp
        assert isinstance(resp["StackInstances"], list)


class TestServiceCatalogDescribeOps:
    """Tests for Describe operations."""

    @pytest.fixture
    def product_with_artifact(self, servicecatalog):
        """Create a product and return its IDs."""
        prod = servicecatalog.create_product(
            Name=_uid("desc-prod"),
            Owner="TestOwner",
            ProductType="CLOUD_FORMATION_TEMPLATE",
            ProvisioningArtifactParameters={
                "Name": "v1",
                "Info": {"LoadTemplateFromURL": "https://example.com/t.json"},
                "Type": "CLOUD_FORMATION_TEMPLATE",
            },
            IdempotencyToken=uuid.uuid4().hex,
        )
        prod_id = prod["ProductViewDetail"]["ProductViewSummary"]["ProductId"]
        pa_resp = servicecatalog.list_provisioning_artifacts(ProductId=prod_id)
        pa_id = pa_resp["ProvisioningArtifactDetails"][0]["Id"]
        yield {"product_id": prod_id, "pa_id": pa_id}
        servicecatalog.delete_product(Id=prod_id)

    def test_describe_product_as_admin(self, servicecatalog, product_with_artifact):
        resp = servicecatalog.describe_product_as_admin(Id=product_with_artifact["product_id"])
        assert "ProductViewDetail" in resp
        assert "ProvisioningArtifactSummaries" in resp
        assert isinstance(resp["ProvisioningArtifactSummaries"], list)
        assert "Tags" in resp
        assert "Budgets" in resp

    def test_describe_provisioning_artifact(self, servicecatalog, product_with_artifact):
        resp = servicecatalog.describe_provisioning_artifact(
            ProductId=product_with_artifact["product_id"],
            ProvisioningArtifactId=product_with_artifact["pa_id"],
        )
        assert "ProvisioningArtifactDetail" in resp
        assert "Info" in resp
        assert "Status" in resp
        pa = resp["ProvisioningArtifactDetail"]
        assert "Id" in pa
        assert pa["Id"] == product_with_artifact["pa_id"]

    def test_describe_provisioning_parameters(self, servicecatalog, product_with_artifact):
        resp = servicecatalog.describe_provisioning_parameters(
            ProductId=product_with_artifact["product_id"],
            ProvisioningArtifactId=product_with_artifact["pa_id"],
        )
        assert "ProvisioningArtifactParameters" in resp
        assert "ConstraintSummaries" in resp
        assert isinstance(resp["ConstraintSummaries"], list)

    def test_describe_copy_product_status(self, servicecatalog):
        """DescribeCopyProductStatus with fake token returns result."""
        resp = servicecatalog.describe_copy_product_status(CopyProductToken="fake-token")
        assert "CopyProductStatus" in resp

    def test_describe_portfolio_share_status(self, servicecatalog):
        """DescribePortfolioShareStatus with fake token returns result."""
        resp = servicecatalog.describe_portfolio_share_status(PortfolioShareToken="fake-token")
        assert "PortfolioShareToken" in resp
        assert "Status" in resp

    def test_describe_service_action_execution_parameters(self, servicecatalog):
        """DescribeServiceActionExecutionParameters returns params list."""
        resp = servicecatalog.describe_service_action_execution_parameters(
            ProvisionedProductId="pp-fake",
            ServiceActionId="act-fake",
        )
        assert "ServiceActionParameters" in resp
        assert isinstance(resp["ServiceActionParameters"], list)


class TestServiceCatalogDescribeNotFound:
    """Tests for Describe operations that return ResourceNotFoundException."""

    def test_describe_constraint_not_found(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_constraint(Id="cs-fake123")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_product_view_not_found(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_product_view(Id="prodview-fake123")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_provisioned_product_not_found(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_provisioned_product(Id="pp-fake123")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_provisioned_product_plan_not_found(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_provisioned_product_plan(PlanId="pp-fake123")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_record_not_found(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_record(Id="rec-fake123")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_service_action_not_found(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_service_action(Id="act-fake123")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_tag_option_not_found(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_tag_option(Id="to-fake123")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_provisioned_product_outputs_not_found(self, servicecatalog):
        with pytest.raises(ClientError) as exc:
            servicecatalog.get_provisioned_product_outputs(ProvisionedProductId="pp-fake123")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


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

    def test_portfolio_share_multiple_accounts(self, servicecatalog):
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
            servicecatalog.create_portfolio_share(
                PortfolioId=pid,
                AccountId="111222333444",
            )
            access = servicecatalog.list_portfolio_access(PortfolioId=pid)
            assert "987654321098" in access["AccountIds"]
            assert "111222333444" in access["AccountIds"]
            servicecatalog.delete_portfolio_share(
                PortfolioId=pid,
                AccountId="987654321098",
            )
            servicecatalog.delete_portfolio_share(
                PortfolioId=pid,
                AccountId="111222333444",
            )
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_delete_share_removes_from_access_list(self, servicecatalog):
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
            servicecatalog.delete_portfolio_share(
                PortfolioId=pid,
                AccountId="987654321098",
            )
            access = servicecatalog.list_portfolio_access(PortfolioId=pid)
            assert "987654321098" not in access["AccountIds"]
        finally:
            servicecatalog.delete_portfolio(Id=pid)
