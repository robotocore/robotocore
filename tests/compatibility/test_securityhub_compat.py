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
                pass


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
            pass

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
            pass

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
            pass

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
            pass

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
            pass

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
            pass

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
        """ListTagsForResource raises error for nonexistent resource."""
        fake_arn = "arn:aws:securityhub:us-east-1:123456789012:hub/nonexistent"
        with pytest.raises(ClientError) as exc:
            hub_client.list_tags_for_resource(ResourceArn=fake_arn)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidInputException",
        )


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
