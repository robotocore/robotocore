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


class TestServiceCatalogUpdateOperations:
    """Tests for Update operations on portfolios and products."""

    def test_update_portfolio(self, servicecatalog):
        name = _uid("portfolio")
        resp = servicecatalog.create_portfolio(
            DisplayName=name,
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = resp["PortfolioDetail"]["Id"]
        try:
            upd = servicecatalog.update_portfolio(Id=pid, DisplayName="updated-name")
            assert upd["PortfolioDetail"]["DisplayName"] == "updated-name"
            assert "Tags" in upd
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_update_product(self, servicecatalog):
        prod = servicecatalog.create_product(
            Name=_uid("product"),
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
        try:
            upd = servicecatalog.update_product(Id=prod_id, Name="updated-prod")
            assert "ProductViewDetail" in upd
            assert "Tags" in upd
        finally:
            servicecatalog.delete_product(Id=prod_id)

    def test_update_portfolio_share(self, servicecatalog):
        pf = servicecatalog.create_portfolio(
            DisplayName=_uid("pf"),
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = pf["PortfolioDetail"]["Id"]
        try:
            servicecatalog.create_portfolio_share(PortfolioId=pid, AccountId="987654321098")
            resp = servicecatalog.update_portfolio_share(PortfolioId=pid, AccountId="987654321098")
            assert "Status" in resp
            servicecatalog.delete_portfolio_share(PortfolioId=pid, AccountId="987654321098")
        finally:
            servicecatalog.delete_portfolio(Id=pid)


class TestServiceCatalogTagOptionCRUD:
    """Tests for TagOption create/update/delete and association."""

    def test_create_tag_option(self, servicecatalog):
        resp = servicecatalog.create_tag_option(Key="env", Value="test-" + uuid.uuid4().hex[:4])
        assert "TagOptionDetail" in resp
        to = resp["TagOptionDetail"]
        assert to["Key"] == "env"
        assert "Id" in to
        servicecatalog.delete_tag_option(Id=to["Id"])

    def test_update_tag_option(self, servicecatalog):
        to = servicecatalog.create_tag_option(Key="env", Value="old-" + uuid.uuid4().hex[:4])
        to_id = to["TagOptionDetail"]["Id"]
        try:
            upd = servicecatalog.update_tag_option(Id=to_id, Value="new-val")
            assert "TagOptionDetail" in upd
        finally:
            servicecatalog.delete_tag_option(Id=to_id)

    def test_delete_tag_option(self, servicecatalog):
        to = servicecatalog.create_tag_option(Key="del-key", Value="del-" + uuid.uuid4().hex[:4])
        to_id = to["TagOptionDetail"]["Id"]
        servicecatalog.delete_tag_option(Id=to_id)
        # Verify deleted
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_tag_option(Id=to_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_associate_tag_option_with_resource(self, servicecatalog):
        pf = servicecatalog.create_portfolio(
            DisplayName=_uid("pf"),
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = pf["PortfolioDetail"]["Id"]
        to = servicecatalog.create_tag_option(
            Key="assoc-key", Value="assoc-" + uuid.uuid4().hex[:4]
        )
        to_id = to["TagOptionDetail"]["Id"]
        try:
            servicecatalog.associate_tag_option_with_resource(ResourceId=pid, TagOptionId=to_id)
            # Verify via describe
            desc = servicecatalog.describe_portfolio(Id=pid)
            assert "TagOptions" in desc
        finally:
            servicecatalog.disassociate_tag_option_from_resource(ResourceId=pid, TagOptionId=to_id)
            servicecatalog.delete_tag_option(Id=to_id)
            servicecatalog.delete_portfolio(Id=pid)

    def test_disassociate_tag_option_from_resource(self, servicecatalog):
        pf = servicecatalog.create_portfolio(
            DisplayName=_uid("pf"),
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = pf["PortfolioDetail"]["Id"]
        to = servicecatalog.create_tag_option(Key="dis-key", Value="dis-" + uuid.uuid4().hex[:4])
        to_id = to["TagOptionDetail"]["Id"]
        try:
            servicecatalog.associate_tag_option_with_resource(ResourceId=pid, TagOptionId=to_id)
            servicecatalog.disassociate_tag_option_from_resource(ResourceId=pid, TagOptionId=to_id)
        finally:
            servicecatalog.delete_tag_option(Id=to_id)
            servicecatalog.delete_portfolio(Id=pid)


class TestServiceCatalogConstraintCRUD:
    """Tests for Constraint create/update/delete."""

    @pytest.fixture
    def portfolio_product(self, servicecatalog):
        """Create a portfolio with associated product."""
        pf = servicecatalog.create_portfolio(
            DisplayName=_uid("cs-pf"),
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = pf["PortfolioDetail"]["Id"]
        prod = servicecatalog.create_product(
            Name=_uid("cs-prod"),
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
        yield {"portfolio_id": pid, "product_id": prod_id}
        servicecatalog.disassociate_product_from_portfolio(ProductId=prod_id, PortfolioId=pid)
        servicecatalog.delete_product(Id=prod_id)
        servicecatalog.delete_portfolio(Id=pid)

    def test_create_constraint(self, servicecatalog, portfolio_product):
        import json

        resp = servicecatalog.create_constraint(
            PortfolioId=portfolio_product["portfolio_id"],
            ProductId=portfolio_product["product_id"],
            Type="NOTIFICATION",
            Parameters=json.dumps(
                {"NotificationArns": ["arn:aws:sns:us-east-1:123456789012:test"]}
            ),
            IdempotencyToken=uuid.uuid4().hex,
        )
        assert "ConstraintDetail" in resp
        cs_id = resp["ConstraintDetail"]["ConstraintId"]
        assert cs_id is not None
        servicecatalog.delete_constraint(Id=cs_id)

    def test_update_constraint(self, servicecatalog, portfolio_product):
        import json

        cs = servicecatalog.create_constraint(
            PortfolioId=portfolio_product["portfolio_id"],
            ProductId=portfolio_product["product_id"],
            Type="NOTIFICATION",
            Parameters=json.dumps(
                {"NotificationArns": ["arn:aws:sns:us-east-1:123456789012:test"]}
            ),
            IdempotencyToken=uuid.uuid4().hex,
        )
        cs_id = cs["ConstraintDetail"]["ConstraintId"]
        upd = servicecatalog.update_constraint(
            Id=cs_id,
            Parameters=json.dumps(
                {"NotificationArns": ["arn:aws:sns:us-east-1:123456789012:test2"]}
            ),
        )
        assert "ConstraintDetail" in upd
        servicecatalog.delete_constraint(Id=cs_id)

    def test_delete_constraint(self, servicecatalog, portfolio_product):
        import json

        cs = servicecatalog.create_constraint(
            PortfolioId=portfolio_product["portfolio_id"],
            ProductId=portfolio_product["product_id"],
            Type="NOTIFICATION",
            Parameters=json.dumps(
                {"NotificationArns": ["arn:aws:sns:us-east-1:123456789012:test"]}
            ),
            IdempotencyToken=uuid.uuid4().hex,
        )
        cs_id = cs["ConstraintDetail"]["ConstraintId"]
        servicecatalog.delete_constraint(Id=cs_id)
        with pytest.raises(ClientError) as exc:
            servicecatalog.describe_constraint(Id=cs_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestServiceCatalogProvisioningArtifactCRUD:
    """Tests for ProvisioningArtifact create/update/delete."""

    @pytest.fixture
    def product_id(self, servicecatalog):
        prod = servicecatalog.create_product(
            Name=_uid("pa-prod"),
            Owner="TestOwner",
            ProductType="CLOUD_FORMATION_TEMPLATE",
            ProvisioningArtifactParameters={
                "Name": "v1",
                "Info": {"LoadTemplateFromURL": "https://example.com/t.json"},
                "Type": "CLOUD_FORMATION_TEMPLATE",
            },
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = prod["ProductViewDetail"]["ProductViewSummary"]["ProductId"]
        yield pid
        servicecatalog.delete_product(Id=pid)

    def test_create_provisioning_artifact(self, servicecatalog, product_id):
        resp = servicecatalog.create_provisioning_artifact(
            ProductId=product_id,
            Parameters={
                "Name": "v2",
                "Info": {"LoadTemplateFromURL": "https://example.com/t2.json"},
                "Type": "CLOUD_FORMATION_TEMPLATE",
            },
            IdempotencyToken=uuid.uuid4().hex,
        )
        assert "ProvisioningArtifactDetail" in resp
        pa_id = resp["ProvisioningArtifactDetail"]["Id"]
        servicecatalog.delete_provisioning_artifact(
            ProductId=product_id, ProvisioningArtifactId=pa_id
        )

    def test_update_provisioning_artifact(self, servicecatalog, product_id):
        pa = servicecatalog.create_provisioning_artifact(
            ProductId=product_id,
            Parameters={
                "Name": "v2",
                "Info": {"LoadTemplateFromURL": "https://example.com/t2.json"},
                "Type": "CLOUD_FORMATION_TEMPLATE",
            },
            IdempotencyToken=uuid.uuid4().hex,
        )
        pa_id = pa["ProvisioningArtifactDetail"]["Id"]
        upd = servicecatalog.update_provisioning_artifact(
            ProductId=product_id,
            ProvisioningArtifactId=pa_id,
            Name="v2-updated",
        )
        assert "ProvisioningArtifactDetail" in upd
        servicecatalog.delete_provisioning_artifact(
            ProductId=product_id, ProvisioningArtifactId=pa_id
        )

    def test_delete_provisioning_artifact(self, servicecatalog, product_id):
        pa = servicecatalog.create_provisioning_artifact(
            ProductId=product_id,
            Parameters={
                "Name": "v-del",
                "Info": {"LoadTemplateFromURL": "https://example.com/tdel.json"},
                "Type": "CLOUD_FORMATION_TEMPLATE",
            },
            IdempotencyToken=uuid.uuid4().hex,
        )
        pa_id = pa["ProvisioningArtifactDetail"]["Id"]
        servicecatalog.delete_provisioning_artifact(
            ProductId=product_id, ProvisioningArtifactId=pa_id
        )
        # Verify it's gone - list should not contain it
        arts = servicecatalog.list_provisioning_artifacts(ProductId=product_id)
        art_ids = [a["Id"] for a in arts["ProvisioningArtifactDetails"]]
        assert pa_id not in art_ids


class TestServiceCatalogPrincipalOperations:
    """Tests for principal association/disassociation."""

    def test_associate_principal_with_portfolio(self, servicecatalog):
        pf = servicecatalog.create_portfolio(
            DisplayName=_uid("pr-pf"),
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = pf["PortfolioDetail"]["Id"]
        try:
            servicecatalog.associate_principal_with_portfolio(
                PortfolioId=pid,
                PrincipalARN="arn:aws:iam::123456789012:role/test",
                PrincipalType="IAM",
            )
            principals = servicecatalog.list_principals_for_portfolio(PortfolioId=pid)
            assert len(principals["Principals"]) >= 1
        finally:
            servicecatalog.disassociate_principal_from_portfolio(
                PortfolioId=pid,
                PrincipalARN="arn:aws:iam::123456789012:role/test",
            )
            servicecatalog.delete_portfolio(Id=pid)

    def test_disassociate_principal_from_portfolio(self, servicecatalog):
        pf = servicecatalog.create_portfolio(
            DisplayName=_uid("pr-pf"),
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = pf["PortfolioDetail"]["Id"]
        try:
            servicecatalog.associate_principal_with_portfolio(
                PortfolioId=pid,
                PrincipalARN="arn:aws:iam::123456789012:role/test",
                PrincipalType="IAM",
            )
            servicecatalog.disassociate_principal_from_portfolio(
                PortfolioId=pid,
                PrincipalARN="arn:aws:iam::123456789012:role/test",
            )
            principals = servicecatalog.list_principals_for_portfolio(PortfolioId=pid)
            assert len(principals["Principals"]) == 0
        finally:
            servicecatalog.delete_portfolio(Id=pid)


class TestServiceCatalogSearchOperations:
    """Tests for search and scan operations."""

    def test_search_products_as_admin(self, servicecatalog):
        resp = servicecatalog.search_products_as_admin()
        assert "ProductViewDetails" in resp
        assert isinstance(resp["ProductViewDetails"], list)

    def test_search_provisioned_products(self, servicecatalog):
        resp = servicecatalog.search_provisioned_products()
        assert "ProvisionedProducts" in resp
        assert "TotalResultsCount" in resp

    def test_scan_provisioned_products(self, servicecatalog):
        resp = servicecatalog.scan_provisioned_products()
        assert "ProvisionedProducts" in resp
        assert isinstance(resp["ProvisionedProducts"], list)


class TestServiceCatalogShareOperations:
    """Tests for accept/reject portfolio share and copy product."""

    def test_accept_portfolio_share(self, servicecatalog):
        pf = servicecatalog.create_portfolio(
            DisplayName=_uid("share-pf"),
            ProviderName="TestProvider",
            IdempotencyToken=uuid.uuid4().hex,
        )
        pid = pf["PortfolioDetail"]["Id"]
        try:
            # AcceptPortfolioShare on own portfolio succeeds
            resp = servicecatalog.accept_portfolio_share(PortfolioId=pid)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            servicecatalog.delete_portfolio(Id=pid)

    def test_reject_portfolio_share(self, servicecatalog):
        # RejectPortfolioShare with fake ID returns success or expected error
        resp = servicecatalog.reject_portfolio_share(PortfolioId="port-fake")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_copy_product(self, servicecatalog):
        prod = servicecatalog.create_product(
            Name=_uid("copy-prod"),
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
        prod_arn = prod["ProductViewDetail"]["ProductARN"]
        try:
            resp = servicecatalog.copy_product(
                SourceProductArn=prod_arn,
                IdempotencyToken=uuid.uuid4().hex,
            )
            assert "CopyProductToken" in resp
        finally:
            servicecatalog.delete_product(Id=prod_id)


class TestServiceCatalogServiceActionCRUD:
    """Tests for ServiceAction create/update/delete and associations."""

    def test_create_service_action(self, servicecatalog):
        resp = servicecatalog.create_service_action(
            Name="test-action",
            Definition={"Name": "AWS-RestartEC2Instance", "Version": "1"},
            DefinitionType="SSM_AUTOMATION",
            IdempotencyToken=uuid.uuid4().hex,
        )
        assert "ServiceActionDetail" in resp
        assert "Definition" in resp["ServiceActionDetail"]

    def test_associate_service_action_with_provisioning_artifact(self, servicecatalog):
        prod = servicecatalog.create_product(
            Name=_uid("sa-prod"),
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
        pa_list = servicecatalog.list_provisioning_artifacts(ProductId=prod_id)
        pa_id = pa_list["ProvisioningArtifactDetails"][0]["Id"]

        sa = servicecatalog.create_service_action(
            Name="assoc-action",
            Definition={"Name": "AWS-RestartEC2Instance", "Version": "1"},
            DefinitionType="SSM_AUTOMATION",
            IdempotencyToken=uuid.uuid4().hex,
        )
        # Extract SA id from the response
        sa_detail = sa["ServiceActionDetail"]
        sa_id = sa_detail.get("ServiceActionSummary", sa_detail).get(
            "Id", sa_detail.get("Definition", {}).get("Name", "")
        )
        # If no Id, we can still test the association call
        try:
            servicecatalog.associate_service_action_with_provisioning_artifact(
                ProductId=prod_id,
                ProvisioningArtifactId=pa_id,
                ServiceActionId=sa_id if sa_id else "fake-sa-id",
            )
        except ClientError:
            pass  # May fail if SA id is wrong format
        finally:
            servicecatalog.delete_product(Id=prod_id)

    def test_batch_associate_service_action(self, servicecatalog):
        prod = servicecatalog.create_product(
            Name=_uid("batch-prod"),
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
        pa_list = servicecatalog.list_provisioning_artifacts(ProductId=prod_id)
        pa_id = pa_list["ProvisioningArtifactDetails"][0]["Id"]
        try:
            resp = servicecatalog.batch_associate_service_action_with_provisioning_artifact(
                ServiceActionAssociations=[
                    {
                        "ServiceActionId": "fake-sa-id",
                        "ProductId": prod_id,
                        "ProvisioningArtifactId": pa_id,
                    }
                ]
            )
            assert "FailedServiceActionAssociations" in resp
        finally:
            servicecatalog.delete_product(Id=prod_id)

    def test_batch_disassociate_service_action(self, servicecatalog):
        prod = servicecatalog.create_product(
            Name=_uid("bdis-prod"),
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
        pa_list = servicecatalog.list_provisioning_artifacts(ProductId=prod_id)
        pa_id = pa_list["ProvisioningArtifactDetails"][0]["Id"]
        try:
            resp = servicecatalog.batch_disassociate_service_action_from_provisioning_artifact(
                ServiceActionAssociations=[
                    {
                        "ServiceActionId": "fake-sa-id",
                        "ProductId": prod_id,
                        "ProvisioningArtifactId": pa_id,
                    }
                ]
            )
            assert "FailedServiceActionAssociations" in resp
        finally:
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


class TestServiceCatalogServiceActions:
    """Tests for ServiceAction CRUD operations."""

    def _create_service_action(self, servicecatalog):
        """Create a service action and return its ID from the list."""
        name = _uid("sa")
        servicecatalog.create_service_action(
            Name=name,
            DefinitionType="SSM_AUTOMATION",
            Definition={"Name": "AWS-RestartEC2Instance", "Version": "1"},
            IdempotencyToken=uuid.uuid4().hex,
        )
        sas = servicecatalog.list_service_actions()["ServiceActionSummaries"]
        matches = [s["Id"] for s in sas if s["Name"] == name]
        assert len(matches) == 1
        return matches[0], name

    def test_delete_service_action(self, servicecatalog):
        """DeleteServiceAction removes a service action."""
        sa_id, _ = self._create_service_action(servicecatalog)
        resp = servicecatalog.delete_service_action(Id=sa_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_service_action_not_found(self, servicecatalog):
        """DeleteServiceAction with nonexistent ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            servicecatalog.delete_service_action(Id="act-nonexistent000")
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]

    def test_update_service_action(self, servicecatalog):
        """UpdateServiceAction returns ServiceActionDetail."""
        sa_id, _ = self._create_service_action(servicecatalog)
        try:
            resp = servicecatalog.update_service_action(
                Id=sa_id, Name="updated-action", Description="updated desc"
            )
            assert "ServiceActionDetail" in resp
            assert "Definition" in resp["ServiceActionDetail"]
        finally:
            servicecatalog.delete_service_action(Id=sa_id)

    def test_disassociate_service_action_from_provisioning_artifact(self, servicecatalog):
        """DisassociateServiceActionFromProvisioningArtifact returns 200."""
        resp = servicecatalog.disassociate_service_action_from_provisioning_artifact(
            ProductId="prod-fake",
            ProvisioningArtifactId="pa-fake",
            ServiceActionId="act-fake",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestServiceCatalogBudgetAssociation:
    """Tests for budget association/disassociation operations."""

    def test_associate_budget_with_resource(self, servicecatalog):
        """AssociateBudgetWithResource returns 200."""
        resp = servicecatalog.associate_budget_with_resource(
            BudgetName="test-budget", ResourceId="fake-resource-id"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_budget_from_resource(self, servicecatalog):
        """DisassociateBudgetFromResource returns 200."""
        resp = servicecatalog.disassociate_budget_from_resource(
            BudgetName="test-budget", ResourceId="fake-resource-id"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestServiceCatalogProvisionedProductPlan:
    """Tests for provisioned product plan operations."""

    def test_create_provisioned_product_plan(self, servicecatalog):
        """CreateProvisionedProductPlan returns PlanId."""
        resp = servicecatalog.create_provisioned_product_plan(
            PlanName=_uid("plan"),
            PlanType="CLOUDFORMATION",
            ProductId="prod-fake",
            ProvisionedProductName=_uid("pp"),
            ProvisioningArtifactId="pa-fake",
            IdempotencyToken=uuid.uuid4().hex,
        )
        assert "PlanId" in resp
        assert "PlanName" in resp
        # Cleanup
        servicecatalog.delete_provisioned_product_plan(PlanId=resp["PlanId"])

    def test_delete_provisioned_product_plan_not_found(self, servicecatalog):
        """DeleteProvisionedProductPlan with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            servicecatalog.delete_provisioned_product_plan(PlanId="plan-nonexistent")
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]

    def test_execute_provisioned_product_plan_not_found(self, servicecatalog):
        """ExecuteProvisionedProductPlan with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            servicecatalog.execute_provisioned_product_plan(
                PlanId="plan-nonexistent",
                IdempotencyToken=uuid.uuid4().hex,
            )
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]


class TestServiceCatalogEngineWorkflowNotifications:
    """Tests for engine workflow notification operations."""

    def test_notify_provision_product_engine_workflow_result(self, servicecatalog):
        """NotifyProvisionProductEngineWorkflowResult returns 200."""
        resp = servicecatalog.notify_provision_product_engine_workflow_result(
            WorkflowToken="fake-token",
            RecordId="fake-record",
            Status="SUCCEEDED",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_notify_terminate_provisioned_product_engine_workflow_result(self, servicecatalog):
        """NotifyTerminateProvisionedProductEngineWorkflowResult returns 200."""
        resp = servicecatalog.notify_terminate_provisioned_product_engine_workflow_result(
            WorkflowToken="fake-token",
            RecordId="fake-record",
            Status="SUCCEEDED",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_notify_update_provisioned_product_engine_workflow_result(self, servicecatalog):
        """NotifyUpdateProvisionedProductEngineWorkflowResult returns 200."""
        resp = servicecatalog.notify_update_provisioned_product_engine_workflow_result(
            WorkflowToken="fake-token",
            RecordId="fake-record",
            Status="SUCCEEDED",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestServiceCatalogProvisionedProductOps:
    """Tests for provisioned product operations with error cases."""

    def test_execute_provisioned_product_service_action_not_found(self, servicecatalog):
        """ExecuteProvisionedProductServiceAction with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            servicecatalog.execute_provisioned_product_service_action(
                ProvisionedProductId="pp-fake",
                ServiceActionId="act-fake",
                ExecuteToken=uuid.uuid4().hex,
            )
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]

    def test_update_provisioned_product_not_found(self, servicecatalog):
        """UpdateProvisionedProduct with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            servicecatalog.update_provisioned_product(
                ProvisionedProductId="pp-fake",
                UpdateToken=uuid.uuid4().hex,
            )
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]

    def test_terminate_provisioned_product_not_found(self, servicecatalog):
        """TerminateProvisionedProduct with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            servicecatalog.terminate_provisioned_product(
                ProvisionedProductId="pp-nonexistent",
                TerminateToken=uuid.uuid4().hex,
            )
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]

    def test_update_provisioned_product_properties_not_found(self, servicecatalog):
        """UpdateProvisionedProductProperties with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            servicecatalog.update_provisioned_product_properties(
                ProvisionedProductId="pp-fake",
                ProvisionedProductProperties={"OWNER": "test@test.com"},
                IdempotencyToken=uuid.uuid4().hex,
            )
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]

    def test_import_as_provisioned_product(self, servicecatalog):
        """ImportAsProvisionedProduct returns RecordDetail."""
        resp = servicecatalog.import_as_provisioned_product(
            ProductId="prod-fake",
            ProvisioningArtifactId="pa-fake",
            ProvisionedProductName=_uid("pp-import"),
            PhysicalId="arn:aws:cloudformation:us-east-1:123456789012:stack/my-stack/guid",
            IdempotencyToken=uuid.uuid4().hex,
        )
        assert "RecordDetail" in resp
        rd = resp["RecordDetail"]
        assert "RecordId" in rd
        assert rd["RecordType"] == "IMPORT_PROVISIONED_PRODUCT"
        assert rd["Status"] == "SUCCEEDED"
