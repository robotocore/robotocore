"""Inspector2 compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def inspector2():
    return make_client("inspector2")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestInspector2FindingsOperations:
    def test_list_findings(self, inspector2):
        resp = inspector2.list_findings()
        assert "findings" in resp
        assert isinstance(resp["findings"], list)

    def test_batch_get_account_status(self, inspector2):
        resp = inspector2.batch_get_account_status(accountIds=["123456789012"])
        assert "accounts" in resp
        assert "failedAccounts" in resp
        assert isinstance(resp["accounts"], list)


class TestInspector2FilterOperations:
    def test_list_filters(self, inspector2):
        resp = inspector2.list_filters()
        assert "filters" in resp
        assert isinstance(resp["filters"], list)

    def test_create_filter(self, inspector2):
        name = _unique("filter")
        resp = inspector2.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        assert "arn" in resp
        assert resp["arn"]


class TestInspector2OrganizationOperations:
    def test_list_members(self, inspector2):
        resp = inspector2.list_members()
        assert "members" in resp
        assert isinstance(resp["members"], list)

    def test_describe_organization_configuration(self, inspector2):
        resp = inspector2.describe_organization_configuration()
        assert "autoEnable" in resp
        auto_enable = resp["autoEnable"]
        assert "ec2" in auto_enable
        assert "ecr" in auto_enable
        assert "lambda" in auto_enable


class TestInspector2AutoCoverage:
    """Auto-generated coverage tests for inspector2."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_list_delegated_admin_accounts(self, client):
        """ListDelegatedAdminAccounts returns a response."""
        resp = client.list_delegated_admin_accounts()
        assert "delegatedAdminAccounts" in resp


class TestInspector2MemberOperations:
    """Tests for Member operations: AssociateMember, GetMember, DisassociateMember."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_associate_member(self, client):
        """AssociateMember associates a member account."""
        resp = client.associate_member(accountId="210987654321")
        assert "accountId" in resp
        assert resp["accountId"] == "210987654321"

    def test_get_member(self, client):
        """GetMember retrieves a member account after association."""
        client.associate_member(accountId="210987654321")
        resp = client.get_member(accountId="210987654321")
        assert "member" in resp
        member = resp["member"]
        assert "accountId" in member

    def test_disassociate_member(self, client):
        """DisassociateMember removes a member account."""
        client.associate_member(accountId="210987654321")
        resp = client.disassociate_member(accountId="210987654321")
        assert "accountId" in resp
        assert resp["accountId"] == "210987654321"


class TestInspector2DelegatedAdminOperations:
    """Tests for EnableDelegatedAdminAccount, DisableDelegatedAdminAccount."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_enable_delegated_admin_account(self, client):
        """EnableDelegatedAdminAccount enables a delegated admin."""
        resp = client.enable_delegated_admin_account(delegatedAdminAccountId="210987654321")
        assert "delegatedAdminAccountId" in resp
        assert resp["delegatedAdminAccountId"] == "210987654321"

    def test_disable_delegated_admin_account(self, client):
        """DisableDelegatedAdminAccount disables a delegated admin."""
        client.enable_delegated_admin_account(delegatedAdminAccountId="210987654321")
        resp = client.disable_delegated_admin_account(delegatedAdminAccountId="210987654321")
        assert "delegatedAdminAccountId" in resp
        assert resp["delegatedAdminAccountId"] == "210987654321"


class TestInspector2DeleteFilterOperation:
    """Test for DeleteFilter."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_delete_filter(self, client):
        """DeleteFilter removes a previously created filter."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        resp = client.delete_filter(arn=arn)
        assert "arn" in resp
        assert resp["arn"] == arn


class TestInspector2UpdateOrgConfigOperation:
    """Test for UpdateOrganizationConfiguration."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_update_organization_configuration(self, client):
        """UpdateOrganizationConfiguration updates auto-enable settings."""
        resp = client.update_organization_configuration(
            autoEnable={"ec2": True, "ecr": True, "lambda": False}
        )
        assert "autoEnable" in resp
        auto_enable = resp["autoEnable"]
        assert "ec2" in auto_enable
        assert "ecr" in auto_enable


class TestInspector2TagOperations:
    """Tests for TagResource, UntagResource, and ListTagsForResource."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_tag_resource_and_list_tags(self, client):
        """TagResource adds tags, ListTagsForResource reads them."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        client.tag_resource(resourceArn=arn, tags={"env": "test"})
        resp = client.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp

    def test_list_tags_for_resource(self, client):
        """ListTagsForResource on a filter with no extra tags."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        resp = client.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp

    def test_untag_resource(self, client):
        """UntagResource removes a tag from a filter."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        client.tag_resource(resourceArn=arn, tags={"env": "test", "team": "backend"})
        resp = client.untag_resource(resourceArn=arn, tagKeys=["env"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestInspector2EnableDisableOperations:
    """Tests for Enable and Disable operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_enable_ec2_scanning(self, client):
        """Enable EC2 scanning for an account."""
        resp = client.enable(resourceTypes=["EC2"], accountIds=["123456789012"])
        assert "accounts" in resp
        assert "failedAccounts" in resp
        assert isinstance(resp["accounts"], list)
        if resp["accounts"]:
            acct = resp["accounts"][0]
            assert acct["accountId"] == "123456789012"
            assert acct["status"] == "ENABLED"
            assert "resourceStatus" in acct
            assert acct["resourceStatus"]["ec2"] == "ENABLED"

    def test_disable_ec2_scanning(self, client):
        """Disable EC2 scanning for an account."""
        # Disable all resource types first to get a clean state
        try:
            client.disable(
                resourceTypes=["EC2", "ECR", "LAMBDA"],
                accountIds=["123456789012"],
            )
        except Exception:
            pass
        client.enable(resourceTypes=["EC2"], accountIds=["123456789012"])
        resp = client.disable(resourceTypes=["EC2"], accountIds=["123456789012"])
        assert "accounts" in resp
        assert isinstance(resp["accounts"], list)
        if resp["accounts"]:
            acct = resp["accounts"][0]
            assert acct["accountId"] == "123456789012"
            assert acct["resourceStatus"]["ec2"] == "DISABLED"

    def test_enable_multiple_resource_types(self, client):
        """Enable scanning for multiple resource types."""
        resp = client.enable(
            resourceTypes=["EC2", "ECR"],
            accountIds=["123456789012"],
        )
        assert "accounts" in resp
        assert "failedAccounts" in resp
        if resp["accounts"]:
            status = resp["accounts"][0]["resourceStatus"]
            assert status["ec2"] == "ENABLED"
            assert status["ecr"] == "ENABLED"

    def test_enable_returns_empty_failed_accounts(self, client):
        """Enable returns empty failedAccounts when operation succeeds."""
        resp = client.enable(resourceTypes=["EC2"], accountIds=["123456789012"])
        assert resp["failedAccounts"] == []

    def test_disable_returns_empty_failed_accounts(self, client):
        """Disable returns empty failedAccounts when operation succeeds."""
        client.enable(resourceTypes=["EC2"], accountIds=["123456789012"])
        resp = client.disable(resourceTypes=["EC2"], accountIds=["123456789012"])
        assert resp["failedAccounts"] == []


class TestInspector2FilterAdvanced:
    """Advanced filter operation tests."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_create_filter_returns_arn(self, client):
        """CreateFilter returns a valid-looking ARN."""
        name = _unique("filter")
        resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = resp["arn"]
        assert "arn:" in arn
        assert "inspector2" in arn.lower() or "filter" in arn.lower()

    def test_list_filters_includes_created(self, client):
        """ListFilters includes a newly created filter."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        resp = client.list_filters()
        arns = [f["arn"] for f in resp["filters"]]
        assert arn in arns

    def test_create_filter_with_suppress_action(self, client):
        """CreateFilter with SUPPRESS action."""
        name = _unique("filter")
        resp = client.create_filter(
            action="SUPPRESS",
            filterCriteria={},
            name=name,
        )
        assert "arn" in resp
        assert resp["arn"]

    def test_delete_filter_removes_from_list(self, client):
        """DeleteFilter removes the filter from ListFilters results."""
        name = _unique("filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        client.delete_filter(arn=arn)
        resp = client.list_filters()
        arns = [f["arn"] for f in resp["filters"]]
        assert arn not in arns


class TestInspector2FilterDetails:
    """Tests verifying filter details returned by ListFilters."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_list_filters_returns_filter_details(self, client):
        """ListFilters returns filter with name and action."""
        name = _unique("detail-filter")
        create_resp = client.create_filter(
            action="SUPPRESS",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        try:
            resp = client.list_filters()
            matching = [f for f in resp["filters"] if f["arn"] == arn]
            assert len(matching) == 1
            filt = matching[0]
            assert filt["name"] == name
            assert filt["action"] == "SUPPRESS"
        finally:
            client.delete_filter(arn=arn)

    def test_create_multiple_filters_listed(self, client):
        """Multiple created filters all appear in ListFilters."""
        name1 = _unique("multi-f1")
        name2 = _unique("multi-f2")
        r1 = client.create_filter(action="NONE", filterCriteria={}, name=name1)
        r2 = client.create_filter(action="SUPPRESS", filterCriteria={}, name=name2)
        try:
            resp = client.list_filters()
            arns = [f["arn"] for f in resp["filters"]]
            assert r1["arn"] in arns
            assert r2["arn"] in arns
        finally:
            client.delete_filter(arn=r1["arn"])
            client.delete_filter(arn=r2["arn"])


class TestInspector2BatchGetAccountStatusDetails:
    """Detailed assertions on BatchGetAccountStatus."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_batch_get_account_status_structure(self, client):
        """BatchGetAccountStatus returns well-structured account info."""
        resp = client.batch_get_account_status(accountIds=["123456789012"])
        assert isinstance(resp["accounts"], list)
        assert isinstance(resp["failedAccounts"], list)
        if resp["accounts"]:
            acct = resp["accounts"][0]
            assert "accountId" in acct
            assert "state" in acct
            assert "resourceState" in acct

    def test_batch_get_account_status_empty_ids(self, client):
        """BatchGetAccountStatus with empty list returns empty accounts."""
        resp = client.batch_get_account_status(accountIds=[])
        assert "accounts" in resp
        assert "failedAccounts" in resp

    def test_list_findings_empty(self, client):
        """ListFindings with no findings returns empty list."""
        resp = client.list_findings()
        assert isinstance(resp["findings"], list)
        # Verify no nextToken when results are empty
        assert resp.get("nextToken") is None or resp.get("nextToken") == ""

    def test_list_members_structure(self, client):
        """ListMembers returns well-structured response."""
        resp = client.list_members()
        assert isinstance(resp["members"], list)

    def test_list_delegated_admin_accounts_structure(self, client):
        """ListDelegatedAdminAccounts returns structured response."""
        resp = client.list_delegated_admin_accounts()
        assert isinstance(resp["delegatedAdminAccounts"], list)


class TestInspector2ConfigAndPermissions:
    """Tests for GetConfiguration, ListAccountPermissions, and related ops."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_get_configuration(self, client):
        """GetConfiguration returns ECR scan configuration."""
        resp = client.get_configuration()
        assert "ecrConfiguration" in resp

    def test_list_account_permissions(self, client):
        """ListAccountPermissions returns permissions list."""
        resp = client.list_account_permissions()
        assert "permissions" in resp
        assert isinstance(resp["permissions"], list)

    def test_batch_get_free_trial_info(self, client):
        """BatchGetFreeTrialInfo returns account trial info."""
        resp = client.batch_get_free_trial_info(accountIds=["123456789012"])
        assert "accounts" in resp
        assert "failedAccounts" in resp
        assert isinstance(resp["accounts"], list)

    def test_get_delegated_admin_account(self, client):
        """GetDelegatedAdminAccount returns response."""
        resp = client.get_delegated_admin_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_ec2_deep_inspection_configuration(self, client):
        """GetEc2DeepInspectionConfiguration returns package paths and status."""
        resp = client.get_ec2_deep_inspection_configuration()
        assert "packagePaths" in resp
        assert "status" in resp

    def test_get_encryption_key(self, client):
        """GetEncryptionKey returns KMS key ID for scan type."""
        resp = client.get_encryption_key(scanType="NETWORK", resourceType="AWS_EC2_INSTANCE")
        assert "kmsKeyId" in resp

    def test_get_findings_report_status(self, client):
        """GetFindingsReportStatus returns report status."""
        resp = client.get_findings_report_status()
        assert "status" in resp

    def test_get_sbom_export(self, client):
        """GetSbomExport returns export status."""
        resp = client.get_sbom_export(reportId="test-report-id")
        assert "reportId" in resp
        assert "status" in resp

    def test_list_usage_totals(self, client):
        """ListUsageTotals returns usage totals list."""
        resp = client.list_usage_totals()
        assert "totals" in resp
        assert isinstance(resp["totals"], list)


class TestInspector2CisAndCoverage:
    """Tests for CIS scan and coverage operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_list_cis_scan_configurations(self, client):
        """ListCisScanConfigurations returns scan configurations."""
        resp = client.list_cis_scan_configurations()
        assert "scanConfigurations" in resp
        assert isinstance(resp["scanConfigurations"], list)

    def test_list_cis_scans(self, client):
        """ListCisScans returns scans list."""
        resp = client.list_cis_scans()
        assert "scans" in resp
        assert isinstance(resp["scans"], list)

    def test_list_code_security_integrations(self, client):
        """ListCodeSecurityIntegrations returns integrations list."""
        resp = client.list_code_security_integrations()
        assert "integrations" in resp
        assert isinstance(resp["integrations"], list)

    def test_list_coverage(self, client):
        """ListCoverage returns covered resources."""
        resp = client.list_coverage()
        assert "coveredResources" in resp
        assert isinstance(resp["coveredResources"], list)

    def test_list_coverage_statistics(self, client):
        """ListCoverageStatistics returns counts and totals."""
        resp = client.list_coverage_statistics()
        assert "countsByGroup" in resp
        assert "totalCounts" in resp

    def test_list_finding_aggregations(self, client):
        """ListFindingAggregations returns aggregation type and responses."""
        resp = client.list_finding_aggregations(aggregationType="FINDING_TYPE")
        assert "aggregationType" in resp
        assert resp["aggregationType"] == "FINDING_TYPE"
        assert "responses" in resp
        assert isinstance(resp["responses"], list)


class TestInspector2CisScanReports:
    """Tests for CIS scan report and result detail operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_get_cis_scan_report(self, client):
        """GetCisScanReport returns status for a non-existent scan."""
        resp = client.get_cis_scan_report(
            scanArn="arn:aws:inspector2:us-east-1:123456789012:cis-scan/fake-scan-id"
        )
        assert "status" in resp
        assert resp["status"] == "NO_FINDINGS_FOUND"

    def test_get_cis_scan_result_details(self, client):
        """GetCisScanResultDetails returns empty results for non-existent scan."""
        resp = client.get_cis_scan_result_details(
            scanArn="arn:aws:inspector2:us-east-1:123456789012:cis-scan/fake",
            accountId="123456789012",
            targetResourceId="i-1234567890abcdef0",
        )
        assert "scanResultDetails" in resp
        assert isinstance(resp["scanResultDetails"], list)

    def test_get_clusters_for_image(self, client):
        """GetClustersForImage returns empty cluster list for non-existent image."""
        resp = client.get_clusters_for_image(
            filter={"resourceId": "arn:aws:ecr:us-east-1:123456789012:repository/test-repo"}
        )
        assert "cluster" in resp
        assert isinstance(resp["cluster"], list)

    def test_get_code_security_integration(self, client):
        """GetCodeSecurityIntegration returns 200 for non-existent integration."""
        resp = client.get_code_security_integration(
            integrationArn="arn:aws:inspector2:us-east-1:123456789012:code-security-integration/fake"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_code_security_scan_configuration(self, client):
        """GetCodeSecurityScanConfiguration returns 200 for non-existent config."""
        resp = client.get_code_security_scan_configuration(
            scanConfigurationArn="arn:aws:inspector2:us-east-1:123456789012:code-security-scan-config/fake"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_cis_scan_results_aggregated_by_checks(self, client):
        """ListCisScanResultsAggregatedByChecks returns check aggregations."""
        resp = client.list_cis_scan_results_aggregated_by_checks(
            scanArn="arn:aws:inspector2:us-east-1:123456789012:cis-scan/fake"
        )
        assert "checkAggregations" in resp
        assert isinstance(resp["checkAggregations"], list)

    def test_list_cis_scan_results_aggregated_by_target_resource(self, client):
        """ListCisScanResultsAggregatedByTargetResource returns target aggregations."""
        resp = client.list_cis_scan_results_aggregated_by_target_resource(
            scanArn="arn:aws:inspector2:us-east-1:123456789012:cis-scan/fake"
        )
        assert "targetResourceAggregations" in resp
        assert isinstance(resp["targetResourceAggregations"], list)

    def test_list_code_security_scan_configuration_associations(self, client):
        """ListCodeSecurityScanConfigurationAssociations returns associations."""
        resp = client.list_code_security_scan_configuration_associations(
            scanConfigurationArn="arn:aws:inspector2:us-east-1:123456789012:code-security-scan-config/fake"
        )
        assert "associations" in resp
        assert isinstance(resp["associations"], list)

    def test_list_code_security_scan_configurations(self, client):
        """ListCodeSecurityScanConfigurations returns configurations list."""
        resp = client.list_code_security_scan_configurations()
        assert "configurations" in resp
        assert isinstance(resp["configurations"], list)


class TestInspector2ConfigurationUpdates:
    """Tests for configuration update operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_update_configuration(self, client):
        """UpdateConfiguration updates ECR scan configuration."""
        resp = client.update_configuration(
            ecrConfiguration={
                "rescanDuration": "LIFETIME",
            }
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_ec2_deep_inspection_configuration(self, client):
        """UpdateEc2DeepInspectionConfiguration updates deep inspection paths."""
        resp = client.update_ec2_deep_inspection_configuration(
            packagePaths=["/usr/lib"],
        )
        assert "packagePaths" in resp
        assert "status" in resp

    def test_update_org_ec2_deep_inspection_configuration(self, client):
        """UpdateOrgEc2DeepInspectionConfiguration updates org deep inspection."""
        resp = client.update_org_ec2_deep_inspection_configuration(
            orgPackagePaths=["/usr/lib"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_encryption_key(self, client):
        """UpdateEncryptionKey sets encryption key for scan type."""
        resp = client.update_encryption_key(
            kmsKeyId="arn:aws:kms:us-east-1:123456789012:key/fake-key-id",
            scanType="NETWORK",
            resourceType="AWS_EC2_INSTANCE",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_reset_encryption_key(self, client):
        """ResetEncryptionKey resets encryption key for scan type."""
        resp = client.reset_encryption_key(
            scanType="NETWORK",
            resourceType="AWS_EC2_INSTANCE",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestInspector2ReportOperations:
    """Tests for report and export operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_create_findings_report(self, client):
        """CreateFindingsReport generates a findings report."""
        resp = client.create_findings_report(
            reportFormat="CSV",
            s3Destination={
                "bucketName": "my-bucket",
                "kmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/fake",
            },
        )
        assert "reportId" in resp

    def test_cancel_findings_report(self, client):
        """CancelFindingsReport cancels a report."""
        create_resp = client.create_findings_report(
            reportFormat="CSV",
            s3Destination={
                "bucketName": "my-bucket",
                "kmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/fake",
            },
        )
        report_id = create_resp["reportId"]
        resp = client.cancel_findings_report(reportId=report_id)
        assert "reportId" in resp
        assert resp["reportId"] == report_id

    def test_create_sbom_export(self, client):
        """CreateSbomExport starts an SBOM export."""
        resp = client.create_sbom_export(
            reportFormat="CYCLONEDX_1_4",
            s3Destination={
                "bucketName": "my-bucket",
                "kmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/fake",
            },
        )
        assert "reportId" in resp

    def test_cancel_sbom_export(self, client):
        """CancelSbomExport cancels an SBOM export."""
        create_resp = client.create_sbom_export(
            reportFormat="CYCLONEDX_1_4",
            s3Destination={
                "bucketName": "my-bucket",
                "kmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/fake",
            },
        )
        report_id = create_resp["reportId"]
        resp = client.cancel_sbom_export(reportId=report_id)
        assert "reportId" in resp
        assert resp["reportId"] == report_id


class TestInspector2MemberDeepInspection:
    """Tests for member EC2 deep inspection batch operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_batch_get_member_ec2_deep_inspection_status(self, client):
        """BatchGetMemberEc2DeepInspectionStatus returns account statuses."""
        resp = client.batch_get_member_ec2_deep_inspection_status()
        assert "accountIds" in resp
        assert isinstance(resp["accountIds"], list)
        assert "failedAccountIds" in resp

    def test_batch_update_member_ec2_deep_inspection_status(self, client):
        """BatchUpdateMemberEc2DeepInspectionStatus updates member statuses."""
        resp = client.batch_update_member_ec2_deep_inspection_status(
            accountIds=[{"accountId": "210987654321", "activateDeepInspection": True}]
        )
        assert "accountIds" in resp
        assert "failedAccountIds" in resp


class TestInspector2CisScanManagement:
    """Tests for CIS scan configuration management."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_update_cis_scan_configuration(self, client):
        """UpdateCisScanConfiguration creates then updates a config."""
        # First create a CIS scan config
        create_resp = client.create_cis_scan_configuration(
            scanName=_unique("cis-scan"),
            schedule={"oneTime": {}},
            securityLevel="LEVEL_1",
            targets={
                "accountIds": ["123456789012"],
                "targetResourceTags": {"env": ["test"]},
            },
        )
        scan_config_arn = create_resp["scanConfigurationArn"]
        # Now update it
        resp = client.update_cis_scan_configuration(
            scanConfigurationArn=scan_config_arn,
            scanName=_unique("cis-scan-upd"),
            securityLevel="LEVEL_2",
            schedule={"oneTime": {}},
            targets={
                "accountIds": ["123456789012"],
                "targetResourceTags": {"env": ["test"]},
            },
        )
        assert "scanConfigurationArn" in resp
        assert resp["scanConfigurationArn"] == scan_config_arn

    def test_delete_cis_scan_configuration(self, client):
        """DeleteCisScanConfiguration removes a config."""
        create_resp = client.create_cis_scan_configuration(
            scanName=_unique("cis-del"),
            schedule={"oneTime": {}},
            securityLevel="LEVEL_1",
            targets={
                "accountIds": ["123456789012"],
                "targetResourceTags": {"env": ["test"]},
            },
        )
        scan_config_arn = create_resp["scanConfigurationArn"]
        resp = client.delete_cis_scan_configuration(scanConfigurationArn=scan_config_arn)
        assert "scanConfigurationArn" in resp
        assert resp["scanConfigurationArn"] == scan_config_arn


class TestInspector2CisSessionOps:
    """Tests for CIS session operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_start_cis_session(self, client):
        """StartCisSession starts a CIS session."""
        resp = client.start_cis_session(
            scanJobId="scan-fake-id",
            message={
                "sessionToken": "fake-token",
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_send_cis_session_health(self, client):
        """SendCisSessionHealth sends health for a CIS session."""
        resp = client.send_cis_session_health(
            scanJobId="scan-fake-id",
            sessionToken="fake-token",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_cis_session(self, client):
        """StopCisSession stops a CIS session."""
        resp = client.stop_cis_session(
            scanJobId="scan-fake-id",
            sessionToken="fake-token",
            message={
                "status": "SUCCESS",
                "progress": {"totalChecks": 10, "successfulChecks": 10, "failedChecks": 0},
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestInspector2CodeSecurityOps:
    """Tests for Code Security Integration operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_create_code_security_integration(self, client):
        """CreateCodeSecurityIntegration creates an integration."""
        resp = client.create_code_security_integration(
            name="test-integration",
            type="GITHUB",
        )
        assert "integrationArn" in resp

    def test_delete_code_security_integration(self, client):
        """DeleteCodeSecurityIntegration with fake ARN returns OK."""
        fake_arn = "arn:aws:inspector2:us-east-1:123456789012:code-security-integration/fake"
        resp = client.delete_code_security_integration(
            integrationArn=fake_arn,
        )
        assert "integrationArn" in resp

    def test_delete_code_security_scan_configuration(self, client):
        """DeleteCodeSecurityScanConfiguration with fake ARN returns OK."""
        fake_arn = "arn:aws:inspector2:us-east-1:123456789012:code-security-scan-configuration/fake"
        resp = client.delete_code_security_scan_configuration(
            scanConfigurationArn=fake_arn,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestInspector2FilterUpdateOp:
    """Tests for UpdateFilter operation."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_update_filter(self, client):
        """UpdateFilter changes a filter's action."""
        name = _unique("upd-filter")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        resp = client.update_filter(filterArn=arn, action="SUPPRESS")
        assert "arn" in resp
        assert resp["arn"] == arn

    def test_update_filter_name(self, client):
        """UpdateFilter can change the filter name."""
        name = _unique("upd-name")
        create_resp = client.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        arn = create_resp["arn"]
        new_name = _unique("new-name")
        resp = client.update_filter(filterArn=arn, name=new_name)
        assert "arn" in resp
        assert resp["arn"] == arn


class TestInspector2BatchCodeOps:
    """Tests for batch code security operations."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_batch_get_code_snippet(self, client):
        """BatchGetCodeSnippet returns results and errors."""
        resp = client.batch_get_code_snippet(
            findingArns=["arn:aws:inspector2:us-east-1:123456789012:finding/fake"],
        )
        assert "codeSnippetResults" in resp
        assert "errors" in resp
        assert isinstance(resp["codeSnippetResults"], list)
        assert isinstance(resp["errors"], list)

    def test_batch_get_finding_details(self, client):
        """BatchGetFindingDetails returns details and errors."""
        resp = client.batch_get_finding_details(
            findingArns=["arn:aws:inspector2:us-east-1:123456789012:finding/fake"],
        )
        assert "findingDetails" in resp
        assert "errors" in resp
        assert isinstance(resp["findingDetails"], list)
        assert isinstance(resp["errors"], list)

    def test_search_vulnerabilities(self, client):
        """SearchVulnerabilities returns vulnerabilities list."""
        resp = client.search_vulnerabilities(
            filterCriteria={"vulnerabilityIds": ["CVE-2024-0001"]},
        )
        assert "vulnerabilities" in resp
        assert isinstance(resp["vulnerabilities"], list)


class TestInspector2CisSessionTelemetry:
    """Tests for SendCisSessionTelemetry."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_send_cis_session_telemetry(self, client):
        """SendCisSessionTelemetry sends telemetry data."""
        resp = client.send_cis_session_telemetry(
            scanJobId="fake-job-id",
            sessionToken="fake-token",
            messages=[
                {
                    "cisRuleDetails": b"test-details",
                    "ruleId": "rule-1",
                    "status": "PASSED",
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestInspector2CodeSecurityScanConfig:
    """Tests for code security scan configuration CRUD."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_create_code_security_scan_configuration(self, client):
        """CreateCodeSecurityScanConfiguration returns scan config ARN."""
        resp = client.create_code_security_scan_configuration(
            name=_unique("scan-cfg"),
            level="STANDARD",
            configuration={"ruleSetCategories": ["SAST"]},
        )
        assert "scanConfigurationArn" in resp
        assert resp["scanConfigurationArn"]

    def test_update_code_security_scan_configuration(self, client):
        """UpdateCodeSecurityScanConfiguration updates a config."""
        fake_arn = "arn:aws:inspector2:us-east-1:123456789012:code-security-scan-config/fake"
        resp = client.update_code_security_scan_configuration(
            scanConfigurationArn=fake_arn,
            configuration={"ruleSetCategories": ["SAST"]},
        )
        assert "scanConfigurationArn" in resp

    def test_get_code_security_scan(self, client):
        """GetCodeSecurityScan returns scan details."""
        resp = client.get_code_security_scan(
            scanId="fake-scan-id",
            resource={"projectId": "test-project-id"},
        )
        assert "scanId" in resp
        assert "status" in resp

    def test_start_code_security_scan(self, client):
        """StartCodeSecurityScan starts a scan."""
        resp = client.start_code_security_scan(
            resource={"projectId": "test-project-id"},
        )
        assert "scanId" in resp
        assert "status" in resp

    def test_update_code_security_integration(self, client):
        """UpdateCodeSecurityIntegration updates an integration."""
        fake_arn = "arn:aws:inspector2:us-east-1:123456789012:code-security-integration/fake"
        resp = client.update_code_security_integration(
            integrationArn=fake_arn,
            details={"github": {"code": "test-code", "installationId": "test-id"}},
        )
        assert "integrationArn" in resp
        assert "status" in resp


class TestInspector2BatchScanConfigAssociations:
    """Tests for batch associate/disassociate code security scan configuration."""

    @pytest.fixture
    def client(self):
        return make_client("inspector2")

    def test_batch_associate_code_security_scan_configuration(self, client):
        """BatchAssociateCodeSecurityScanConfiguration returns results."""
        resp = client.batch_associate_code_security_scan_configuration(
            associateConfigurationRequests=[
                {
                    "resource": {"projectId": "test-project"},
                    "scanConfigurationArn": (
                        "arn:aws:inspector2:us-east-1:123456789012:code-security-scan-config/fake"
                    ),
                }
            ],
        )
        assert "failedAssociations" in resp
        assert "successfulAssociations" in resp

    def test_batch_disassociate_code_security_scan_configuration(self, client):
        """BatchDisassociateCodeSecurityScanConfiguration returns results."""
        resp = client.batch_disassociate_code_security_scan_configuration(
            disassociateConfigurationRequests=[
                {
                    "resource": {"projectId": "test-project"},
                    "scanConfigurationArn": (
                        "arn:aws:inspector2:us-east-1:123456789012:code-security-scan-config/fake"
                    ),
                }
            ],
        )
        assert "failedAssociations" in resp
        assert "successfulAssociations" in resp
