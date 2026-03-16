"""Security Hub compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def securityhub():
    return make_client("securityhub")


class TestSecurityHubOperations:
    def test_enable_describe_list_disable(self, securityhub):
        # Enable Security Hub
        enable_resp = securityhub.enable_security_hub(
            EnableDefaultStandards=False,
        )
        assert enable_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        try:
            # Describe hub
            describe_resp = securityhub.describe_hub()
            assert describe_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "HubArn" in describe_resp

            # List members
            members_resp = securityhub.list_members()
            assert members_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Members" in members_resp
        finally:
            # Disable Security Hub (may fail if parallel state reset occurred)
            try:
                securityhub.disable_security_hub()
            except Exception:
                pass  # best-effort cleanup


class TestSecurityhubAutoCoverage:
    """Auto-generated coverage tests for securityhub."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_get_administrator_account(self, client):
        """GetAdministratorAccount returns a 200 response."""
        resp = client.get_administrator_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_findings(self, client):
        """GetFindings returns a response."""
        resp = client.get_findings()
        assert "Findings" in resp

    def test_get_master_account(self, client):
        """GetMasterAccount returns a 200 response."""
        resp = client.get_master_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_organization_configuration(self, client):
        """DescribeOrganizationConfiguration requires org admin access."""
        with pytest.raises(ClientError) as exc:
            client.describe_organization_configuration()
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException"

    def test_update_organization_configuration(self, client):
        """UpdateOrganizationConfiguration without hub returns error."""
        with pytest.raises(ClientError) as exc:
            client.update_organization_configuration(AutoEnable=True)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "AccessDeniedException",
        )

    def test_get_members(self, client):
        """GetMembers returns UnprocessedAccounts for unknown accounts."""
        resp = client.get_members(AccountIds=["999999999999"])
        assert "Members" in resp
        assert "UnprocessedAccounts" in resp

    def test_enable_organization_admin_account_no_org(self, client):
        """EnableOrganizationAdminAccount fails without AWS Organizations."""
        with pytest.raises(ClientError) as exc:
            client.enable_organization_admin_account(AdminAccountId="123456789012")
        assert exc.value.response["Error"]["Code"] in (
            "AWSOrganizationsNotInUseException",
            "InvalidAccessException",
        )


class TestSecurityHubDescribeHub:
    """Tests for DescribeHub response fields."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # Already enabled
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_describe_hub_returns_hub_arn(self, hub_client):
        """DescribeHub returns HubArn field."""
        resp = hub_client.describe_hub()
        assert "HubArn" in resp
        assert "securityhub" in resp["HubArn"]

    def test_describe_hub_returns_subscribed_at(self, hub_client):
        """DescribeHub returns SubscribedAt timestamp."""
        resp = hub_client.describe_hub()
        assert "SubscribedAt" in resp
        assert len(resp["SubscribedAt"]) > 0

    def test_describe_hub_returns_auto_enable_controls(self, hub_client):
        """DescribeHub returns AutoEnableControls field."""
        resp = hub_client.describe_hub()
        assert "AutoEnableControls" in resp
        assert isinstance(resp["AutoEnableControls"], bool)

    def test_describe_hub_returns_control_finding_generator(self, hub_client):
        """DescribeHub returns ControlFindingGenerator field."""
        resp = hub_client.describe_hub()
        assert "ControlFindingGenerator" in resp


class TestSecurityHubEnableWithTags:
    """Tests for EnableSecurityHub with Tags parameter."""

    @pytest.fixture
    def client(self):
        client = make_client("securityhub")
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_enable_security_hub_with_tags(self, client):
        """EnableSecurityHub accepts Tags parameter."""
        resp = client.enable_security_hub(
            EnableDefaultStandards=False,
            Tags={"env": "test", "team": "security"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enable_then_describe_hub(self, client):
        """EnableSecurityHub then DescribeHub returns consistent HubArn."""
        client.enable_security_hub(EnableDefaultStandards=False)
        resp = client.describe_hub()
        assert resp["HubArn"].startswith("arn:aws:securityhub:")
        assert "hub/default" in resp["HubArn"]


class TestSecurityHubFindings:
    """Tests for SecurityHub findings operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def _make_finding(self, finding_id=None, severity="HIGH", title="Test Finding"):
        finding_id = finding_id or f"finding-{uuid.uuid4().hex[:8]}"
        return {
            "SchemaVersion": "2018-10-08",
            "Id": finding_id,
            "ProductArn": (
                "arn:aws:securityhub:us-east-1:123456789012:product/123456789012/default"
            ),
            "GeneratorId": "test-generator",
            "AwsAccountId": "123456789012",
            "Types": ["Software and Configuration Checks"],
            "CreatedAt": "2024-01-01T00:00:00Z",
            "UpdatedAt": "2024-01-01T00:00:00Z",
            "Severity": {"Label": severity},
            "Title": title,
            "Description": "Test finding for compat tests",
            "Resources": [
                {
                    "Type": "AwsEc2Instance",
                    "Id": f"i-{uuid.uuid4().hex[:8]}",
                    "Region": "us-east-1",
                }
            ],
        }

    def test_batch_import_findings(self, client):
        """BatchImportFindings imports findings successfully."""
        finding = self._make_finding()
        resp = client.batch_import_findings(Findings=[finding])
        assert resp["SuccessCount"] == 1
        assert resp["FailedCount"] == 0
        assert resp["FailedFindings"] == []

    def test_batch_import_multiple_findings(self, client):
        """BatchImportFindings handles multiple findings at once."""
        findings = [
            self._make_finding(severity="HIGH", title="Finding A"),
            self._make_finding(severity="CRITICAL", title="Finding B"),
            self._make_finding(severity="LOW", title="Finding C"),
        ]
        resp = client.batch_import_findings(Findings=findings)
        assert resp["SuccessCount"] == 3
        assert resp["FailedCount"] == 0

    def test_get_findings_with_severity_filter(self, client):
        """GetFindings can filter by severity label."""
        # Import a finding with a unique severity
        finding = self._make_finding(severity="CRITICAL", title="Critical Test")
        client.batch_import_findings(Findings=[finding])

        resp = client.get_findings(
            Filters={"SeverityLabel": [{"Value": "CRITICAL", "Comparison": "EQUALS"}]}
        )
        assert "Findings" in resp
        # We imported at least one CRITICAL finding
        titles = [f["Title"] for f in resp["Findings"]]
        assert "Critical Test" in titles

    def test_get_findings_returns_imported(self, client):
        """GetFindings returns previously imported findings."""
        unique_id = f"finding-{uuid.uuid4().hex[:8]}"
        finding = self._make_finding(finding_id=unique_id, title="Imported Finding")
        client.batch_import_findings(Findings=[finding])

        resp = client.get_findings()
        assert "Findings" in resp
        found_ids = [f["Id"] for f in resp["Findings"]]
        assert unique_id in found_ids

    def test_get_findings_with_max_results(self, client):
        """GetFindings respects MaxResults parameter."""
        # Import several findings
        for i in range(3):
            finding = self._make_finding(title=f"MaxRes Finding {i}")
            client.batch_import_findings(Findings=[finding])

        resp = client.get_findings(MaxResults=1)
        assert "Findings" in resp
        assert len(resp["Findings"]) <= 1

    def test_get_findings_with_title_filter(self, client):
        """GetFindings accepts Title filter parameter."""
        finding = self._make_finding(title="TitleFilterTest")
        client.batch_import_findings(Findings=[finding])

        resp = client.get_findings(
            Filters={"Title": [{"Value": "TitleFilterTest", "Comparison": "EQUALS"}]}
        )
        assert "Findings" in resp
        assert isinstance(resp["Findings"], list)

    def test_get_findings_with_sort_criteria(self, client):
        """GetFindings accepts SortCriteria parameter."""
        finding = self._make_finding(title="SortTest")
        client.batch_import_findings(Findings=[finding])

        resp = client.get_findings(SortCriteria=[{"Field": "Title", "SortOrder": "asc"}])
        assert "Findings" in resp
        assert isinstance(resp["Findings"], list)

    def test_batch_import_preserves_finding_fields(self, client):
        """BatchImportFindings preserves finding fields in GetFindings."""
        unique_id = f"preserve-{uuid.uuid4().hex[:8]}"
        finding = self._make_finding(
            finding_id=unique_id, severity="INFORMATIONAL", title="PreserveTest"
        )
        client.batch_import_findings(Findings=[finding])

        # Retrieve all findings and look for ours
        resp = client.get_findings()
        found_ids = [f["Id"] for f in resp["Findings"]]
        assert unique_id in found_ids
        found = next(f for f in resp["Findings"] if f["Id"] == unique_id)
        assert found["Title"] == "PreserveTest"
        assert found["Severity"]["Label"] == "INFORMATIONAL"

    def test_get_findings_with_resource_type_filter(self, client):
        """GetFindings accepts ResourceType filter."""
        finding = self._make_finding(title="ResourceTypeTest")
        client.batch_import_findings(Findings=[finding])

        resp = client.get_findings(
            Filters={"ResourceType": [{"Value": "AwsEc2Instance", "Comparison": "EQUALS"}]}
        )
        assert "Findings" in resp
        assert isinstance(resp["Findings"], list)


class TestSecurityHubMembers:
    """Tests for SecurityHub member operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # Already enabled
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    @staticmethod
    def _random_account_id():
        """Generate a random 12-digit AWS account ID."""
        return str(uuid.uuid4().int)[:12]

    def test_create_members(self, hub_client):
        """CreateMembers registers member accounts."""
        acct_id = self._random_account_id()
        resp = hub_client.create_members(
            AccountDetails=[
                {"AccountId": acct_id, "Email": "member1@example.com"},
            ]
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_create_and_list_members(self, hub_client):
        """CreateMembers then ListMembers shows the created member."""
        acct_id = self._random_account_id()
        hub_client.create_members(
            AccountDetails=[
                {"AccountId": acct_id, "Email": "member2@example.com"},
            ]
        )
        resp = hub_client.list_members()
        assert "Members" in resp
        account_ids = [m["AccountId"] for m in resp["Members"]]
        assert acct_id in account_ids

    def test_create_members_multiple(self, hub_client):
        """CreateMembers handles multiple accounts."""
        acct_a = self._random_account_id()
        acct_b = self._random_account_id()
        resp = hub_client.create_members(
            AccountDetails=[
                {"AccountId": acct_a, "Email": "a@example.com"},
                {"AccountId": acct_b, "Email": "b@example.com"},
            ]
        )
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_get_members_multiple_accounts(self, hub_client):
        """GetMembers returns results for multiple account IDs."""
        acct_a = self._random_account_id()
        acct_b = self._random_account_id()
        hub_client.create_members(
            AccountDetails=[
                {"AccountId": acct_a, "Email": "x@example.com"},
                {"AccountId": acct_b, "Email": "y@example.com"},
            ]
        )
        resp = hub_client.get_members(AccountIds=[acct_a, acct_b])
        assert "Members" in resp
        assert "UnprocessedAccounts" in resp
        member_ids = [m["AccountId"] for m in resp["Members"]]
        assert acct_a in member_ids
        assert acct_b in member_ids

    def test_list_members_returns_member_fields(self, hub_client):
        """ListMembers returns members with expected fields."""
        acct_id = self._random_account_id()
        hub_client.create_members(
            AccountDetails=[
                {"AccountId": acct_id, "Email": "fields@example.com"},
            ]
        )
        resp = hub_client.list_members()
        assert "Members" in resp
        # Find our member
        member = next((m for m in resp["Members"] if m["AccountId"] == acct_id), None)
        assert member is not None
        assert "AccountId" in member

    def test_list_members_empty_when_none_created(self, hub_client):
        """ListMembers returns empty list initially (or existing members)."""
        resp = hub_client.list_members()
        assert "Members" in resp
        assert isinstance(resp["Members"], list)


class TestSecurityHubActionTargets:
    """Tests for SecurityHub action target CRUD operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # Already enabled
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_describe_action_targets_empty(self, hub_client):
        """DescribeActionTargets returns empty list when none exist."""
        resp = hub_client.describe_action_targets()
        assert "ActionTargets" in resp
        assert isinstance(resp["ActionTargets"], list)

    def test_create_action_target(self, hub_client):
        """CreateActionTarget returns an ActionTargetArn."""
        suffix = uuid.uuid4().hex[:8]
        resp = hub_client.create_action_target(
            Name=f"Test-{suffix}",
            Description="Test action target",
            Id=suffix,
        )
        assert "ActionTargetArn" in resp
        assert suffix in resp["ActionTargetArn"]
        assert "action/custom/" in resp["ActionTargetArn"]

    def test_create_and_describe_action_target(self, hub_client):
        """Created action target appears in DescribeActionTargets."""
        suffix = uuid.uuid4().hex[:8]
        create_resp = hub_client.create_action_target(
            Name=f"Desc-{suffix}",
            Description="Describe test",
            Id=suffix,
        )
        arn = create_resp["ActionTargetArn"]

        resp = hub_client.describe_action_targets(ActionTargetArns=[arn])
        assert "ActionTargets" in resp
        assert len(resp["ActionTargets"]) == 1
        assert resp["ActionTargets"][0]["ActionTargetArn"] == arn
        assert resp["ActionTargets"][0]["Name"] == f"Desc-{suffix}"

    def test_update_action_target(self, hub_client):
        """UpdateActionTarget updates name and description."""
        suffix = uuid.uuid4().hex[:8]
        create_resp = hub_client.create_action_target(
            Name=f"Orig-{suffix}",
            Description="Original",
            Id=suffix,
        )
        arn = create_resp["ActionTargetArn"]

        update_resp = hub_client.update_action_target(
            ActionTargetArn=arn,
            Name=f"Updated-{suffix}",
            Description="Updated description",
        )
        assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify the update
        desc_resp = hub_client.describe_action_targets(ActionTargetArns=[arn])
        assert desc_resp["ActionTargets"][0]["Name"] == f"Updated-{suffix}"
        assert desc_resp["ActionTargets"][0]["Description"] == "Updated description"

    def test_delete_action_target(self, hub_client):
        """DeleteActionTarget removes the target and returns its ARN."""
        suffix = uuid.uuid4().hex[:8]
        create_resp = hub_client.create_action_target(
            Name=f"Del-{suffix}",
            Description="To delete",
            Id=suffix,
        )
        arn = create_resp["ActionTargetArn"]

        del_resp = hub_client.delete_action_target(ActionTargetArn=arn)
        assert "ActionTargetArn" in del_resp
        assert del_resp["ActionTargetArn"] == arn

    def test_delete_action_target_not_found(self, hub_client):
        """DeleteActionTarget raises error for nonexistent target."""
        fake_arn = "arn:aws:securityhub:us-east-1:123456789012:action/custom/nonexistent999"
        with pytest.raises(ClientError) as exc:
            hub_client.delete_action_target(ActionTargetArn=fake_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidAccessException",
        )


class TestSecurityHubProductSubscriptions:
    """Tests for SecurityHub product subscription operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # Already enabled
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_list_enabled_products_for_import(self, hub_client):
        """ListEnabledProductsForImport returns product subscriptions list."""
        resp = hub_client.list_enabled_products_for_import()
        assert "ProductSubscriptions" in resp
        assert isinstance(resp["ProductSubscriptions"], list)

    def test_enable_import_findings_for_product(self, hub_client):
        """EnableImportFindingsForProduct returns a subscription ARN."""
        product_arn = "arn:aws:securityhub:us-east-1::product/aws/inspector"
        # Handle already-enabled case from prior test runs
        try:
            resp = hub_client.enable_import_findings_for_product(ProductArn=product_arn)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                # Already enabled — just verify listing works instead
                list_resp = hub_client.list_enabled_products_for_import()
                assert "ProductSubscriptions" in list_resp
                return
            raise
        assert "ProductSubscriptionArn" in resp
        assert "product-subscription/" in resp["ProductSubscriptionArn"]

    def test_enable_then_list_products(self, hub_client):
        """Enabled product appears in ListEnabledProductsForImport."""
        product_arn = "arn:aws:securityhub:us-east-1::product/aws/macie"
        try:
            enable_resp = hub_client.enable_import_findings_for_product(ProductArn=product_arn)
            sub_arn = enable_resp["ProductSubscriptionArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                # Already enabled — verify it's in the list
                list_resp = hub_client.list_enabled_products_for_import()
                assert "ProductSubscriptions" in list_resp
                assert isinstance(list_resp["ProductSubscriptions"], list)
                return
            raise

        list_resp = hub_client.list_enabled_products_for_import()
        assert "ProductSubscriptions" in list_resp
        assert sub_arn in list_resp["ProductSubscriptions"]

    def test_disable_import_findings_for_product(self, hub_client):
        """DisableImportFindingsForProduct removes a product subscription."""
        # Use a unique product to avoid conflicts with other tests
        product_arn = "arn:aws:securityhub:us-east-1::product/aws/guardduty"
        try:
            enable_resp = hub_client.enable_import_findings_for_product(ProductArn=product_arn)
            sub_arn = enable_resp["ProductSubscriptionArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                # Already enabled — find its subscription ARN from listing
                list_resp = hub_client.list_enabled_products_for_import()
                assert "ProductSubscriptions" in list_resp
                assert len(list_resp["ProductSubscriptions"]) > 0
                return
            raise

        disable_resp = hub_client.disable_import_findings_for_product(
            ProductSubscriptionArn=sub_arn
        )
        assert disable_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSecurityHubTags:
    """Tests for SecurityHub tag operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # Already enabled
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def _get_hub_arn(self, client):
        resp = client.describe_hub()
        return resp["HubArn"]

    def test_list_tags_for_resource(self, hub_client):
        """ListTagsForResource returns Tags dict for hub."""
        hub_arn = self._get_hub_arn(hub_client)
        resp = hub_client.list_tags_for_resource(ResourceArn=hub_arn)
        assert "Tags" in resp
        assert isinstance(resp["Tags"], dict)

    def test_tag_resource(self, hub_client):
        """TagResource adds tags to the hub."""
        hub_arn = self._get_hub_arn(hub_client)
        tag_key = f"key-{uuid.uuid4().hex[:6]}"
        resp = hub_client.tag_resource(ResourceArn=hub_arn, Tags={tag_key: "testvalue"})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify tag was added
        tags_resp = hub_client.list_tags_for_resource(ResourceArn=hub_arn)
        assert tag_key in tags_resp["Tags"]
        assert tags_resp["Tags"][tag_key] == "testvalue"

    def test_untag_resource(self, hub_client):
        """UntagResource removes tags from the hub."""
        hub_arn = self._get_hub_arn(hub_client)
        tag_key = f"rm-{uuid.uuid4().hex[:6]}"

        # Add a tag first
        hub_client.tag_resource(ResourceArn=hub_arn, Tags={tag_key: "toremove"})

        # Remove it
        resp = hub_client.untag_resource(ResourceArn=hub_arn, TagKeys=[tag_key])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify removal
        tags_resp = hub_client.list_tags_for_resource(ResourceArn=hub_arn)
        assert tag_key not in tags_resp["Tags"]

    def test_tag_resource_multiple_tags(self, hub_client):
        """TagResource can add multiple tags at once."""
        hub_arn = self._get_hub_arn(hub_client)
        prefix = uuid.uuid4().hex[:4]
        tags = {f"{prefix}-a": "val1", f"{prefix}-b": "val2"}
        resp = hub_client.tag_resource(ResourceArn=hub_arn, Tags=tags)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        tags_resp = hub_client.list_tags_for_resource(ResourceArn=hub_arn)
        assert f"{prefix}-a" in tags_resp["Tags"]
        assert f"{prefix}-b" in tags_resp["Tags"]

    def test_list_tags_for_nonexistent_resource(self, hub_client):
        """ListTagsForResource for nonexistent resource returns empty tags."""
        fake_arn = "arn:aws:securityhub:us-east-1:123456789012:hub/nonexistent"
        resp = hub_client.list_tags_for_resource(ResourceArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp.get("Tags", {}), dict)


class TestSecurityHubBatchUpdateFindings:
    """Tests for BatchUpdateFindings operation."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def _make_finding(self, finding_id=None):
        finding_id = finding_id or f"finding-{uuid.uuid4().hex[:8]}"
        return {
            "SchemaVersion": "2018-10-08",
            "Id": finding_id,
            "ProductArn": (
                "arn:aws:securityhub:us-east-1:123456789012:product/123456789012/default"
            ),
            "GeneratorId": "test-generator",
            "AwsAccountId": "123456789012",
            "Types": ["Software and Configuration Checks"],
            "CreatedAt": "2024-01-01T00:00:00Z",
            "UpdatedAt": "2024-01-01T00:00:00Z",
            "Severity": {"Label": "MEDIUM"},
            "Title": "BatchUpdate Test",
            "Description": "Finding for batch update test",
            "Resources": [
                {
                    "Type": "AwsEc2Instance",
                    "Id": f"i-{uuid.uuid4().hex[:8]}",
                    "Region": "us-east-1",
                }
            ],
        }

    def test_batch_update_findings_with_note(self, client):
        """BatchUpdateFindings adds a note to a finding."""
        finding_id = f"batchupd-{uuid.uuid4().hex[:8]}"
        product_arn = "arn:aws:securityhub:us-east-1:123456789012:product/123456789012/default"
        finding = self._make_finding(finding_id=finding_id)
        client.batch_import_findings(Findings=[finding])

        resp = client.batch_update_findings(
            FindingIdentifiers=[{"Id": finding_id, "ProductArn": product_arn}],
            Note={"Text": "Investigation complete", "UpdatedBy": "security-team"},
        )
        assert "ProcessedFindings" in resp
        assert "UnprocessedFindings" in resp
        processed_ids = [f["Id"] for f in resp["ProcessedFindings"]]
        assert finding_id in processed_ids

    def test_batch_update_findings_severity(self, client):
        """BatchUpdateFindings can update severity."""
        finding_id = f"sev-{uuid.uuid4().hex[:8]}"
        product_arn = "arn:aws:securityhub:us-east-1:123456789012:product/123456789012/default"
        finding = self._make_finding(finding_id=finding_id)
        client.batch_import_findings(Findings=[finding])

        resp = client.batch_update_findings(
            FindingIdentifiers=[{"Id": finding_id, "ProductArn": product_arn}],
            Severity={"Label": "CRITICAL"},
        )
        assert len(resp["ProcessedFindings"]) == 1
        assert resp["UnprocessedFindings"] == []

    def test_batch_update_findings_workflow(self, client):
        """BatchUpdateFindings can update workflow status."""
        finding_id = f"wf-{uuid.uuid4().hex[:8]}"
        product_arn = "arn:aws:securityhub:us-east-1:123456789012:product/123456789012/default"
        finding = self._make_finding(finding_id=finding_id)
        client.batch_import_findings(Findings=[finding])

        resp = client.batch_update_findings(
            FindingIdentifiers=[{"Id": finding_id, "ProductArn": product_arn}],
            Workflow={"Status": "RESOLVED"},
        )
        assert "ProcessedFindings" in resp
        assert len(resp["ProcessedFindings"]) >= 1


class TestSecurityHubProducts:
    """Tests for DescribeProducts and related operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_describe_products_returns_list(self, client):
        """DescribeProducts returns a Products list."""
        resp = client.describe_products()
        assert "Products" in resp
        assert isinstance(resp["Products"], list)

    def test_describe_products_with_max_results(self, client):
        """DescribeProducts accepts MaxResults parameter."""
        resp = client.describe_products(MaxResults=5)
        assert "Products" in resp
        assert isinstance(resp["Products"], list)

    def test_describe_products_v2_returns_list(self, client):
        """DescribeProductsV2 returns a ProductsV2 list."""
        resp = client.describe_products_v2()
        assert "ProductsV2" in resp
        assert isinstance(resp["ProductsV2"], list)


class TestSecurityHubStandards:
    """Tests for standards-related operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_describe_standards_returns_list(self, client):
        """DescribeStandards returns Standards list."""
        resp = client.describe_standards()
        assert "Standards" in resp
        assert isinstance(resp["Standards"], list)

    def test_get_enabled_standards_returns_list(self, client):
        """GetEnabledStandards returns StandardsSubscriptions list."""
        resp = client.get_enabled_standards()
        assert "StandardsSubscriptions" in resp
        assert isinstance(resp["StandardsSubscriptions"], list)

    def test_describe_standards_controls_nonexistent(self, client):
        """DescribeStandardsControls raises error for nonexistent subscription."""
        fake_arn = (
            "arn:aws:securityhub:us-east-1:123456789012:"
            "subscription/aws-foundational-security-best-practices/v/1.0.0"
        )
        with pytest.raises(ClientError) as exc:
            client.describe_standards_controls(StandardsSubscriptionArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_standards_control_associations(self, client):
        """ListStandardsControlAssociations returns associations list."""
        resp = client.list_standards_control_associations(SecurityControlId="IAM.1")
        assert "StandardsControlAssociationSummaries" in resp
        assert isinstance(resp["StandardsControlAssociationSummaries"], list)


class TestSecurityHubInsights:
    """Tests for insights operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_get_insights_returns_list(self, client):
        """GetInsights returns Insights list."""
        resp = client.get_insights()
        assert "Insights" in resp
        assert isinstance(resp["Insights"], list)

    def test_get_insight_results_nonexistent(self, client):
        """GetInsightResults raises error for nonexistent insight."""
        fake_arn = (
            "arn:aws:securityhub:us-east-1:123456789012:insight/123456789012/custom/nonexistent"
        )
        with pytest.raises(ClientError) as exc:
            client.get_insight_results(InsightArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestSecurityHubInvitations:
    """Tests for invitations operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_get_invitations_count(self, client):
        """GetInvitationsCount returns an integer count."""
        resp = client.get_invitations_count()
        assert "InvitationsCount" in resp
        assert isinstance(resp["InvitationsCount"], int)

    def test_list_invitations_returns_list(self, client):
        """ListInvitations returns Invitations list."""
        resp = client.list_invitations()
        assert "Invitations" in resp
        assert isinstance(resp["Invitations"], list)


class TestSecurityHubAutomationRules:
    """Tests for automation rules operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_list_automation_rules_returns_list(self, client):
        """ListAutomationRules returns AutomationRulesMetadata list."""
        resp = client.list_automation_rules()
        assert "AutomationRulesMetadata" in resp
        assert isinstance(resp["AutomationRulesMetadata"], list)


class TestSecurityHubConfigurationPolicies:
    """Tests for configuration policy operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_list_configuration_policies_returns_list(self, client):
        """ListConfigurationPolicies returns summaries list."""
        resp = client.list_configuration_policies()
        assert "ConfigurationPolicySummaries" in resp
        assert isinstance(resp["ConfigurationPolicySummaries"], list)

    def test_list_configuration_policy_associations_returns_list(self, client):
        """ListConfigurationPolicyAssociations returns summaries list."""
        resp = client.list_configuration_policy_associations()
        assert "ConfigurationPolicyAssociationSummaries" in resp
        assert isinstance(resp["ConfigurationPolicyAssociationSummaries"], list)

    def test_get_configuration_policy_nonexistent(self, client):
        """GetConfigurationPolicy raises error for nonexistent policy."""
        with pytest.raises(ClientError) as exc:
            client.get_configuration_policy(Identifier="fake-policy-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestSecurityHubFindingAggregators:
    """Tests for finding aggregator operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_list_finding_aggregators_returns_list(self, client):
        """ListFindingAggregators returns FindingAggregators list."""
        resp = client.list_finding_aggregators()
        assert "FindingAggregators" in resp
        assert isinstance(resp["FindingAggregators"], list)

    def test_get_finding_aggregator_nonexistent(self, client):
        """GetFindingAggregator raises error for nonexistent aggregator."""
        fake_arn = "arn:aws:securityhub:us-east-1:123456789012:finding-aggregator/nonexistent"
        with pytest.raises(ClientError) as exc:
            client.get_finding_aggregator(FindingAggregatorArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestSecurityHubAdminAccounts:
    """Tests for organization admin account operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_list_organization_admin_accounts_returns_list(self, client):
        """ListOrganizationAdminAccounts returns AdminAccounts list."""
        resp = client.list_organization_admin_accounts()
        assert "AdminAccounts" in resp
        assert isinstance(resp["AdminAccounts"], list)


class TestSecurityHubSecurityControls:
    """Tests for security control definitions."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_list_security_control_definitions_returns_list(self, client):
        """ListSecurityControlDefinitions returns definitions list."""
        resp = client.list_security_control_definitions()
        assert "SecurityControlDefinitions" in resp
        assert isinstance(resp["SecurityControlDefinitions"], list)

    def test_get_security_control_definition(self, client):
        """GetSecurityControlDefinition returns a definition for a known control."""
        resp = client.get_security_control_definition(SecurityControlId="IAM.1")
        assert "SecurityControlDefinition" in resp
        defn = resp["SecurityControlDefinition"]
        assert "SecurityControlId" in defn
        assert defn["SecurityControlId"] == "IAM.1"


class TestSecurityHubFindingHistory:
    """Tests for finding history operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_get_finding_history_returns_records(self, client):
        """GetFindingHistory returns Records list."""
        resp = client.get_finding_history(
            FindingIdentifier={
                "Id": "nonexistent-finding",
                "ProductArn": (
                    "arn:aws:securityhub:us-east-1:123456789012:product/123456789012/default"
                ),
            }
        )
        assert "Records" in resp
        assert isinstance(resp["Records"], list)


class TestSecurityHubV2Operations:
    """Tests for SecurityHub V2 API operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_get_findings_v2_returns_list(self, client):
        """GetFindingsV2 returns Findings list."""
        resp = client.get_findings_v2()
        assert "Findings" in resp
        assert isinstance(resp["Findings"], list)

    def test_list_aggregators_v2_returns_list(self, client):
        """ListAggregatorsV2 returns AggregatorsV2 list."""
        resp = client.list_aggregators_v2()
        assert "AggregatorsV2" in resp
        assert isinstance(resp["AggregatorsV2"], list)

    def test_list_connectors_v2_returns_list(self, client):
        """ListConnectorsV2 returns Connectors list."""
        resp = client.list_connectors_v2()
        assert "Connectors" in resp
        assert isinstance(resp["Connectors"], list)

    def test_get_resources_v2_returns_list(self, client):
        """GetResourcesV2 returns Resources list."""
        resp = client.get_resources_v2()
        assert "Resources" in resp
        assert isinstance(resp["Resources"], list)

    def test_describe_security_hub_v2(self, client):
        """DescribeSecurityHubV2 returns a response (200 or error depending on state)."""
        try:
            resp = client.describe_security_hub_v2()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as exc:
            # Raises InvalidAccessException when hub is not enabled
            assert exc.response["Error"]["Code"] == "InvalidAccessException"

    def test_get_automation_rule_v2_nonexistent(self, client):
        """GetAutomationRuleV2 raises error for nonexistent rule."""
        with pytest.raises(ClientError) as exc:
            client.get_automation_rule_v2(Identifier="fake-rule-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_connector_v2_nonexistent(self, client):
        """GetConnectorV2 raises error for nonexistent connector."""
        with pytest.raises(ClientError) as exc:
            client.get_connector_v2(ConnectorId="fake-connector-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_automation_rules_v2_error(self, client):
        """ListAutomationRulesV2 raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.list_automation_rules_v2()
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_aggregator_v2_nonexistent(self, client):
        """GetAggregatorV2 raises ResourceNotFoundException for fake ARN."""
        fake_arn = "arn:aws:securityhub:us-east-1:123456789012:aggregator/fake"
        with pytest.raises(ClientError) as exc:
            client.get_aggregator_v2(AggregatorV2Arn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_finding_statistics_v2(self, client):
        """GetFindingStatisticsV2 returns GroupByResults."""
        resp = client.get_finding_statistics_v2(GroupByRules=[{"GroupByField": "SeverityLabel"}])
        assert "GroupByResults" in resp
        assert isinstance(resp["GroupByResults"], list)

    def test_get_findings_trends_v2(self, client):
        """GetFindingsTrendsV2 returns TrendsMetrics."""
        import datetime

        now = datetime.datetime.now(datetime.UTC)
        start = now - datetime.timedelta(days=7)
        resp = client.get_findings_trends_v2(StartTime=start, EndTime=now)
        assert "TrendsMetrics" in resp
        assert isinstance(resp["TrendsMetrics"], list)

    def test_get_resources_statistics_v2(self, client):
        """GetResourcesStatisticsV2 returns GroupByResults."""
        resp = client.get_resources_statistics_v2(GroupByRules=[{"GroupByField": "ResourceType"}])
        assert "GroupByResults" in resp
        assert isinstance(resp["GroupByResults"], list)

    def test_get_resources_trends_v2(self, client):
        """GetResourcesTrendsV2 returns TrendsMetrics."""
        import datetime

        now = datetime.datetime.now(datetime.UTC)
        start = now - datetime.timedelta(days=7)
        resp = client.get_resources_trends_v2(StartTime=start, EndTime=now)
        assert "TrendsMetrics" in resp
        assert isinstance(resp["TrendsMetrics"], list)


class TestSecurityHubV2Crud:
    """Tests for SecurityHub V2 create/delete operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_batch_update_findings_v2(self, client):
        """BatchUpdateFindingsV2 returns response with ProcessedFindings."""
        resp = client.batch_update_findings_v2()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_aggregator_v2(self, client):
        """CreateAggregatorV2 creates and returns an aggregator."""
        resp = client.create_aggregator_v2(RegionLinkingMode="ALL_REGIONS")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_ticket_v2(self, client):
        """CreateTicketV2 returns a response."""
        resp = client.create_ticket_v2(
            ConnectorId="fake-connector-id",
            FindingMetadataUid="fake-finding-uid",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_aggregator_v2_nonexistent(self, client):
        """DeleteAggregatorV2 raises ResourceNotFoundException for fake ARN."""
        fake_arn = "arn:aws:securityhub:us-east-1:123456789012:aggregator/fake"
        with pytest.raises(ClientError) as exc:
            client.delete_aggregator_v2(AggregatorV2Arn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_automation_rule_v2_nonexistent(self, client):
        """DeleteAutomationRuleV2 raises ResourceNotFoundException for fake ID."""
        with pytest.raises(ClientError) as exc:
            client.delete_automation_rule_v2(Identifier="fake-rule-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_connector_v2_nonexistent(self, client):
        """DeleteConnectorV2 raises ResourceNotFoundException for fake ID."""
        with pytest.raises(ClientError) as exc:
            client.delete_connector_v2(ConnectorId="fake-connector-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_enable_disable_security_hub_v2(self, client):
        """EnableSecurityHubV2 and DisableSecurityHubV2 work."""
        enable_resp = client.enable_security_hub_v2()
        assert enable_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        disable_resp = client.disable_security_hub_v2()
        assert disable_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_register_connector_v2(self, client):
        """RegisterConnectorV2 returns a response."""
        resp = client.register_connector_v2(
            AuthCode="fake-auth-code",
            AuthState="fake-auth-state",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_aggregator_v2_nonexistent(self, client):
        """UpdateAggregatorV2 raises ResourceNotFoundException for fake ARN."""
        fake_arn = "arn:aws:securityhub:us-east-1:123456789012:aggregator/fake"
        with pytest.raises(ClientError) as exc:
            client.update_aggregator_v2(
                AggregatorV2Arn=fake_arn,
                RegionLinkingMode="ALL_REGIONS",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_automation_rule_v2_nonexistent(self, client):
        """UpdateAutomationRuleV2 raises ResourceNotFoundException for fake ID."""
        with pytest.raises(ClientError) as exc:
            client.update_automation_rule_v2(Identifier="fake-rule-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_connector_v2_nonexistent(self, client):
        """UpdateConnectorV2 raises ResourceNotFoundException for fake ID."""
        with pytest.raises(ClientError) as exc:
            client.update_connector_v2(ConnectorId="fake-connector-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestSecurityHubInsightCrud:
    """Tests for insight create/update/delete lifecycle."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_create_insight(self, hub_client):
        """CreateInsight returns an InsightArn."""
        suffix = uuid.uuid4().hex[:8]
        resp = hub_client.create_insight(
            Name=f"test-insight-{suffix}",
            Filters={
                "SeverityLabel": [{"Value": "HIGH", "Comparison": "EQUALS"}],
            },
            GroupByAttribute="ResourceType",
        )
        assert "InsightArn" in resp
        assert "insight" in resp["InsightArn"]

    def test_create_then_delete_insight(self, hub_client):
        """CreateInsight then DeleteInsight removes the insight."""
        suffix = uuid.uuid4().hex[:8]
        create_resp = hub_client.create_insight(
            Name=f"del-insight-{suffix}",
            Filters={
                "SeverityLabel": [{"Value": "CRITICAL", "Comparison": "EQUALS"}],
            },
            GroupByAttribute="ResourceType",
        )
        arn = create_resp["InsightArn"]

        del_resp = hub_client.delete_insight(InsightArn=arn)
        assert "InsightArn" in del_resp
        assert del_resp["InsightArn"] == arn

    def test_update_insight(self, hub_client):
        """UpdateInsight changes insight name."""
        suffix = uuid.uuid4().hex[:8]
        create_resp = hub_client.create_insight(
            Name=f"upd-insight-{suffix}",
            Filters={
                "SeverityLabel": [{"Value": "MEDIUM", "Comparison": "EQUALS"}],
            },
            GroupByAttribute="ResourceType",
        )
        arn = create_resp["InsightArn"]

        upd_resp = hub_client.update_insight(
            InsightArn=arn,
            Name=f"updated-{suffix}",
        )
        assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_insight_nonexistent(self, hub_client):
        """DeleteInsight for nonexistent insight raises ResourceNotFoundException."""
        fake_arn = (
            "arn:aws:securityhub:us-east-1:123456789012:insight/123456789012/custom/nonexistent999"
        )
        with pytest.raises(ClientError) as exc:
            hub_client.delete_insight(InsightArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_insight_nonexistent(self, hub_client):
        """UpdateInsight for nonexistent insight raises ResourceNotFoundException."""
        fake_arn = (
            "arn:aws:securityhub:us-east-1:123456789012:insight/123456789012/custom/nonexistent999"
        )
        with pytest.raises(ClientError) as exc:
            hub_client.update_insight(InsightArn=fake_arn, Name="nope")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestSecurityHubFindingAggregatorCrud:
    """Tests for finding aggregator create/update/delete lifecycle."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_create_finding_aggregator(self, hub_client):
        """CreateFindingAggregator returns aggregator ARN and mode."""
        resp = hub_client.create_finding_aggregator(
            RegionLinkingMode="ALL_REGIONS",
        )
        assert "FindingAggregatorArn" in resp
        assert resp["RegionLinkingMode"] == "ALL_REGIONS"

    def test_delete_finding_aggregator_nonexistent(self, hub_client):
        """DeleteFindingAggregator for nonexistent raises ResourceNotFoundException."""
        fake_arn = "arn:aws:securityhub:us-east-1:123456789012:finding-aggregator/nonexistent999"
        with pytest.raises(ClientError) as exc:
            hub_client.delete_finding_aggregator(FindingAggregatorArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_finding_aggregator_nonexistent(self, hub_client):
        """UpdateFindingAggregator for nonexistent raises ResourceNotFoundException."""
        fake_arn = "arn:aws:securityhub:us-east-1:123456789012:finding-aggregator/nonexistent999"
        with pytest.raises(ClientError) as exc:
            hub_client.update_finding_aggregator(
                FindingAggregatorArn=fake_arn,
                RegionLinkingMode="ALL_REGIONS",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestSecurityHubConfigurationOps:
    """Tests for SecurityHub configuration operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_update_security_hub_configuration(self, hub_client):
        """UpdateSecurityHubConfiguration toggles AutoEnableControls."""
        resp = hub_client.update_security_hub_configuration(
            AutoEnableControls=False,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_standards_control(self, hub_client):
        """UpdateStandardsControl disables a control."""
        arn = (
            "arn:aws:securityhub:us-east-1:123456789012:"
            "control/aws-foundational-security-best-practices/v/1.0.0/IAM.1"
        )
        resp = hub_client.update_standards_control(
            StandardsControlArn=arn,
            ControlStatus="DISABLED",
            DisabledReason="testing",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_security_control(self, hub_client):
        """UpdateSecurityControl updates control parameters."""
        resp = hub_client.update_security_control(
            SecurityControlId="IAM.1",
            Parameters={
                "MaxPasswordAge": {
                    "ValueType": "CUSTOM",
                    "Value": {"Integer": 60},
                }
            },
            LastUpdateReason="testing",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_findings(self, hub_client):
        """UpdateFindings adds a note to findings matching filter."""
        resp = hub_client.update_findings(
            Filters={
                "Id": [{"Value": "finding-fake", "Comparison": "EQUALS"}],
            },
            Note={"Text": "test note", "UpdatedBy": "tester"},
            RecordState="ACTIVE",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSecurityHubBatchControlOps:
    """Tests for batch security control operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_batch_get_security_controls(self, hub_client):
        """BatchGetSecurityControls returns controls and unprocessed."""
        resp = hub_client.batch_get_security_controls(
            SecurityControlIds=["IAM.1"],
        )
        assert "SecurityControls" in resp
        assert "UnprocessedIds" in resp

    def test_batch_get_standards_control_associations(self, hub_client):
        """BatchGetStandardsControlAssociations returns associations."""
        standards_arn = (
            "arn:aws:securityhub:::standards/aws-foundational-security-best-practices/v/1.0.0"
        )
        resp = hub_client.batch_get_standards_control_associations(
            StandardsControlAssociationIds=[
                {
                    "SecurityControlId": "IAM.1",
                    "StandardsArn": standards_arn,
                }
            ],
        )
        assert "StandardsControlAssociationDetails" in resp
        assert "UnprocessedAssociations" in resp

    def test_batch_update_standards_control_associations(self, hub_client):
        """BatchUpdateStandardsControlAssociations processes updates."""
        standards_arn = (
            "arn:aws:securityhub:::standards/aws-foundational-security-best-practices/v/1.0.0"
        )
        resp = hub_client.batch_update_standards_control_associations(
            StandardsControlAssociationUpdates=[
                {
                    "SecurityControlId": "IAM.1",
                    "StandardsArn": standards_arn,
                    "AssociationStatus": "DISABLED",
                    "UpdatedReason": "testing",
                }
            ],
        )
        assert "UnprocessedAssociationUpdates" in resp

    def test_batch_get_configuration_policy_associations(self, hub_client):
        """BatchGetConfigurationPolicyAssociations returns associations."""
        resp = hub_client.batch_get_configuration_policy_associations(
            ConfigurationPolicyAssociationIdentifiers=[
                {"Target": {"AccountId": "123456789012"}},
            ],
        )
        assert "ConfigurationPolicyAssociations" in resp
        assert "UnprocessedConfigurationPolicyAssociations" in resp


class TestSecurityHubMemberActions:
    """Tests for member invite/dissociate/delete operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_invite_members(self, hub_client):
        """InviteMembers returns UnprocessedAccounts."""
        resp = hub_client.invite_members(AccountIds=["222233334444"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_delete_members(self, hub_client):
        """DeleteMembers returns UnprocessedAccounts."""
        resp = hub_client.delete_members(AccountIds=["222233334444"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_disassociate_members(self, hub_client):
        """DisassociateMembers returns empty or result."""
        resp = hub_client.disassociate_members(AccountIds=["222233334444"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_from_administrator_account(self, hub_client):
        """DisassociateFromAdministratorAccount succeeds."""
        resp = hub_client.disassociate_from_administrator_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_from_master_account(self, hub_client):
        """DisassociateFromMasterAccount succeeds (deprecated op)."""
        resp = hub_client.disassociate_from_master_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSecurityHubInvitationActions:
    """Tests for invitation accept/decline/delete operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_accept_administrator_invitation(self, hub_client):
        """AcceptAdministratorInvitation succeeds."""
        resp = hub_client.accept_administrator_invitation(
            AdministratorId="222233334444",
            InvitationId="inv-fake12345",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_accept_invitation(self, hub_client):
        """AcceptInvitation succeeds (deprecated op)."""
        resp = hub_client.accept_invitation(
            MasterId="222233334444",
            InvitationId="inv-fake12345",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_decline_invitations(self, hub_client):
        """DeclineInvitations returns UnprocessedAccounts."""
        resp = hub_client.decline_invitations(AccountIds=["222233334444"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_delete_invitations(self, hub_client):
        """DeleteInvitations returns UnprocessedAccounts."""
        resp = hub_client.delete_invitations(AccountIds=["222233334444"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)


class TestSecurityHubOrganizationOps:
    """Tests for organization admin operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_disable_organization_admin_account_no_org(self, client):
        """DisableOrganizationAdminAccount without orgs raises error."""
        with pytest.raises(ClientError) as exc:
            client.disable_organization_admin_account(
                AdminAccountId="222233334444",
            )
        assert exc.value.response["Error"]["Code"] in (
            "AWSOrganizationsNotInUseException",
            "InvalidAccessException",
        )


class TestSecurityHubConfigurationPolicyCrud:
    """Tests for configuration policy delete/update."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_delete_configuration_policy_nonexistent(self, hub_client):
        """DeleteConfigurationPolicy for nonexistent raises error."""
        with pytest.raises(ClientError) as exc:
            hub_client.delete_configuration_policy(Identifier="fake-policy-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_configuration_policy_nonexistent(self, hub_client):
        """UpdateConfigurationPolicy for nonexistent raises error."""
        with pytest.raises(ClientError) as exc:
            hub_client.update_configuration_policy(
                Identifier="fake-policy-id",
                Name="updated",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_configuration_policy_disassociation(self, hub_client):
        """StartConfigurationPolicyDisassociation returns 200."""
        resp = hub_client.start_configuration_policy_disassociation(
            ConfigurationPolicyIdentifier="fake-policy-id",
            Target={"AccountId": "123456789012"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSecurityHubAutomationRuleCrud:
    """Tests for automation rule CRUD operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_create_automation_rule(self, hub_client):
        """CreateAutomationRule returns a RuleArn."""
        resp = hub_client.create_automation_rule(
            RuleOrder=1,
            RuleName=f"rule-{uuid.uuid4().hex[:8]}",
            RuleStatus="ENABLED",
            Description="test automation rule",
            Criteria={},
            Actions=[
                {
                    "Type": "FINDING_FIELDS_UPDATE",
                    "FindingFieldsUpdate": {
                        "Note": {"Text": "auto-note", "UpdatedBy": "rule"},
                    },
                }
            ],
        )
        assert "RuleArn" in resp
        assert "automation-rule" in resp["RuleArn"]

    def test_batch_get_automation_rules(self, hub_client):
        """BatchGetAutomationRules returns Rules and UnprocessedAutomationRules."""
        resp = hub_client.batch_get_automation_rules(
            AutomationRulesArns=[
                "arn:aws:securityhub:us-east-1:123456789012:automation-rule/fake-id"
            ],
        )
        assert "Rules" in resp
        assert "UnprocessedAutomationRules" in resp

    def test_batch_delete_automation_rules(self, hub_client):
        """BatchDeleteAutomationRules returns Processed and Unprocessed lists."""
        resp = hub_client.batch_delete_automation_rules(
            AutomationRulesArns=[
                "arn:aws:securityhub:us-east-1:123456789012:automation-rule/fake-id"
            ],
        )
        assert "ProcessedAutomationRules" in resp
        assert "UnprocessedAutomationRules" in resp

    def test_batch_update_automation_rules(self, hub_client):
        """BatchUpdateAutomationRules returns Processed and Unprocessed lists."""
        resp = hub_client.batch_update_automation_rules(
            UpdateAutomationRulesRequestItems=[
                {
                    "RuleArn": (
                        "arn:aws:securityhub:us-east-1:123456789012:automation-rule/fake-id"
                    ),
                    "RuleStatus": "DISABLED",
                }
            ],
        )
        assert "ProcessedAutomationRules" in resp
        assert "UnprocessedAutomationRules" in resp

    def test_create_automation_rule_then_batch_get(self, hub_client):
        """Created automation rule can be retrieved via BatchGetAutomationRules."""
        create_resp = hub_client.create_automation_rule(
            RuleOrder=2,
            RuleName=f"rule-{uuid.uuid4().hex[:8]}",
            RuleStatus="ENABLED",
            Description="test retrieve",
            Criteria={},
            Actions=[
                {
                    "Type": "FINDING_FIELDS_UPDATE",
                    "FindingFieldsUpdate": {
                        "Note": {"Text": "note", "UpdatedBy": "rule"},
                    },
                }
            ],
        )
        rule_arn = create_resp["RuleArn"]
        get_resp = hub_client.batch_get_automation_rules(
            AutomationRulesArns=[rule_arn],
        )
        assert len(get_resp["Rules"]) >= 1
        arns = [r["RuleArn"] for r in get_resp["Rules"]]
        assert rule_arn in arns


class TestSecurityHubStandardsBatchOps:
    """Tests for BatchEnableStandards and BatchDisableStandards."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_batch_enable_standards(self, hub_client):
        """BatchEnableStandards returns StandardsSubscriptions list."""
        resp = hub_client.batch_enable_standards(
            StandardsSubscriptionRequests=[
                {
                    "StandardsArn": (
                        "arn:aws:securityhub:::standards/"
                        "aws-foundational-security-best-practices/v/1.0.0"
                    ),
                }
            ],
        )
        assert "StandardsSubscriptions" in resp
        assert isinstance(resp["StandardsSubscriptions"], list)
        assert len(resp["StandardsSubscriptions"]) >= 1

    def test_batch_enable_then_disable_standards(self, hub_client):
        """BatchEnableStandards then BatchDisableStandards removes subscription."""
        enable_resp = hub_client.batch_enable_standards(
            StandardsSubscriptionRequests=[
                {
                    "StandardsArn": (
                        "arn:aws:securityhub:::standards/"
                        "aws-foundational-security-best-practices/v/1.0.0"
                    ),
                }
            ],
        )
        sub_arn = enable_resp["StandardsSubscriptions"][0]["StandardsSubscriptionArn"]
        disable_resp = hub_client.batch_disable_standards(
            StandardsSubscriptionArns=[sub_arn],
        )
        assert "StandardsSubscriptions" in disable_resp
        assert isinstance(disable_resp["StandardsSubscriptions"], list)

    def test_batch_disable_standards_nonexistent(self, hub_client):
        """BatchDisableStandards for nonexistent subscription raises error."""
        with pytest.raises(ClientError) as exc:
            hub_client.batch_disable_standards(
                StandardsSubscriptionArns=[
                    "arn:aws:securityhub:us-east-1:123456789012:subscription/nonexistent/v/1.0.0"
                ],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestSecurityHubConfigurationPolicyCreate:
    """Tests for CreateConfigurationPolicy and association operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # best-effort cleanup
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass  # best-effort cleanup

    def test_create_configuration_policy(self, hub_client):
        """CreateConfigurationPolicy returns an Arn and Id."""
        resp = hub_client.create_configuration_policy(
            Name=f"policy-{uuid.uuid4().hex[:8]}",
            ConfigurationPolicy={
                "SecurityHub": {
                    "ServiceEnabled": True,
                    "EnabledStandardIdentifiers": [],
                }
            },
        )
        assert "Arn" in resp
        assert "Id" in resp
        assert "Name" in resp

    def test_create_then_get_configuration_policy(self, hub_client):
        """Created policy can be retrieved via GetConfigurationPolicy."""
        name = f"policy-{uuid.uuid4().hex[:8]}"
        create_resp = hub_client.create_configuration_policy(
            Name=name,
            ConfigurationPolicy={
                "SecurityHub": {
                    "ServiceEnabled": True,
                    "EnabledStandardIdentifiers": [],
                }
            },
        )
        policy_id = create_resp["Id"]
        get_resp = hub_client.get_configuration_policy(Identifier=policy_id)
        assert get_resp["Id"] == policy_id
        assert get_resp["Name"] == name

    def test_get_configuration_policy_association_nonexistent(self, hub_client):
        """GetConfigurationPolicyAssociation raises error for unassociated target."""
        with pytest.raises(ClientError) as exc:
            hub_client.get_configuration_policy_association(
                Target={"AccountId": "999999999999"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_configuration_policy_association(self, hub_client):
        """StartConfigurationPolicyAssociation returns association details."""
        resp = hub_client.start_configuration_policy_association(
            ConfigurationPolicyIdentifier="fake-policy-id",
            Target={"AccountId": "123456789012"},
        )
        assert "ConfigurationPolicyId" in resp
        assert "TargetId" in resp
        assert "AssociationStatus" in resp


class TestSecurityHubV2AutomationAndConnectors:
    """Tests for V2 automation rule and connector CRUD ops."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_create_automation_rule_v2(self, client):
        """CreateAutomationRuleV2 returns RuleArn and RuleId."""
        resp = client.create_automation_rule_v2(
            RuleOrder=1,
            RuleName=f"v2-rule-{uuid.uuid4().hex[:8]}",
            RuleStatus="ENABLED",
            Description="test v2 automation rule",
            Criteria={"OcsfFindingCriteria": {}},
            Actions=[
                {
                    "Type": "FINDING_FIELDS_UPDATE",
                    "FindingFieldsUpdate": {"SeverityId": 2},
                }
            ],
        )
        assert "RuleArn" in resp
        assert "RuleId" in resp

    def test_create_connector_v2(self, client):
        """CreateConnectorV2 returns ConnectorArn and ConnectorId."""
        resp = client.create_connector_v2(
            Name=f"conn-{uuid.uuid4().hex[:8]}",
            Provider={
                "ServiceNow": {
                    "InstanceName": "test-instance",
                    "SecretArn": ("arn:aws:secretsmanager:us-east-1:123456789012:secret:test-key"),
                }
            },
        )
        assert "ConnectorArn" in resp
        assert "ConnectorId" in resp
        assert "ConnectorStatus" in resp
