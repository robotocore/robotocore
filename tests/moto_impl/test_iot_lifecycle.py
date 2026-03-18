"""Resource lifecycle tests for iot (auto-generated)."""

import logging

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "iot",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_audit_mitigation_actions_task_lifecycle(client):
    """Test AuditMitigationActionsTask CRUD lifecycle."""
    # CREATE
    create_resp = client.start_audit_mitigation_actions_task(
        taskId="test-id-1",
        target={},
        auditCheckToActionsMapping={},
        clientRequestToken="test-string",
    )
    assert isinstance(create_resp.get("taskId"), str)
    assert len(create_resp.get("taskId", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_audit_mitigation_actions_task(
        taskId="test-id-1",
    )
    assert isinstance(desc_resp.get("taskStatistics", {}), dict)
    assert isinstance(desc_resp.get("target", {}), dict)
    assert isinstance(desc_resp.get("auditCheckToActionsMapping", {}), dict)
    assert isinstance(desc_resp.get("actionsDefinition", []), list)


def test_audit_mitigation_actions_task_not_found(client):
    """Test that describing a non-existent AuditMitigationActionsTask raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_audit_mitigation_actions_task(
            taskId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_audit_suppression_lifecycle(client):
    """Test AuditSuppression CRUD lifecycle."""
    ri = {"clientId": "test-client-1"}
    # Cleanup in case of previous test run leaving state
    try:
        client.delete_audit_suppression(checkName="test-name-1", resourceIdentifier=ri)
    except ClientError as e:
        logging.debug("pre-cleanup skipped: %s", e)
    # CREATE
    client.create_audit_suppression(
        checkName="test-name-1",
        resourceIdentifier=ri,
        clientRequestToken="test-string",
    )

    # DESCRIBE
    desc_resp = client.describe_audit_suppression(
        checkName="test-name-1",
        resourceIdentifier=ri,
    )
    assert isinstance(desc_resp.get("checkName"), str)
    assert len(desc_resp.get("checkName", "")) > 0
    assert isinstance(desc_resp.get("resourceIdentifier", {}), dict)

    # DELETE
    client.delete_audit_suppression(
        checkName="test-name-1",
        resourceIdentifier=ri,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_audit_suppression(
            checkName="test-name-1",
            resourceIdentifier=ri,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_audit_suppression_not_found(client):
    """Test that describing a non-existent AuditSuppression raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_audit_suppression(
            checkName="nonexistent-check",
            resourceIdentifier={"clientId": "nonexistent-client-xyz"},
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_authorizer_lifecycle(client):
    """Test Authorizer CRUD lifecycle."""
    # CREATE
    create_resp = client.create_authorizer(
        authorizerName="test-name-1",
        authorizerFunctionArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("authorizerName"), str)
    assert len(create_resp.get("authorizerName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_authorizer(
        authorizerName="test-name-1",
    )
    assert isinstance(desc_resp.get("authorizerDescription", {}), dict)

    # DELETE
    client.delete_authorizer(
        authorizerName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_authorizer(
            authorizerName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_authorizer_not_found(client):
    """Test that describing a non-existent Authorizer raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_authorizer(
            authorizerName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_billing_group_lifecycle(client):
    """Test BillingGroup CRUD lifecycle."""
    # CREATE
    create_resp = client.create_billing_group(
        billingGroupName="test-name-1",
    )
    assert isinstance(create_resp.get("billingGroupName"), str)
    assert len(create_resp.get("billingGroupName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_billing_group(
        billingGroupName="test-name-1",
    )
    assert isinstance(desc_resp.get("billingGroupName"), str)
    assert len(desc_resp.get("billingGroupName", "")) > 0
    assert isinstance(desc_resp.get("billingGroupProperties", {}), dict)
    assert isinstance(desc_resp.get("billingGroupMetadata", {}), dict)

    # DELETE
    client.delete_billing_group(
        billingGroupName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_billing_group(
            billingGroupName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_billing_group_not_found(client):
    """Test that describing a non-existent BillingGroup raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_billing_group(
            billingGroupName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_ca_certificate_lifecycle(client):
    """Test CACertificate CRUD lifecycle."""
    # CREATE
    create_resp = client.register_ca_certificate(
        caCertificate="test-string",
    )
    assert isinstance(create_resp.get("certificateId"), str)
    assert len(create_resp.get("certificateId", "")) > 0

    certificate_id = create_resp["certificateId"]

    # DESCRIBE
    desc_resp = client.describe_ca_certificate(
        certificateId=certificate_id,
    )
    assert isinstance(desc_resp.get("certificateDescription", {}), dict)
    assert isinstance(desc_resp.get("registrationConfig", {}), dict)

    # DELETE
    client.delete_ca_certificate(
        certificateId=certificate_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_ca_certificate(
            certificateId=certificate_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_ca_certificate_not_found(client):
    """Test that describing a non-existent CACertificate raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_ca_certificate(
            certificateId="a" * 64,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_certificate_lifecycle(client):
    """Test Certificate CRUD lifecycle."""
    # CREATE
    create_resp = client.register_certificate(
        certificatePem="test-string",
    )
    assert isinstance(create_resp.get("certificateId"), str)
    assert len(create_resp.get("certificateId", "")) > 0

    certificate_id = create_resp["certificateId"]

    # DESCRIBE
    desc_resp = client.describe_certificate(
        certificateId=certificate_id,
    )
    assert isinstance(desc_resp.get("certificateDescription", {}), dict)

    # DELETE
    client.delete_certificate(
        certificateId=certificate_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_certificate(
            certificateId=certificate_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_certificate_not_found(client):
    """Test that describing a non-existent Certificate raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_certificate(
            certificateId="a" * 64,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_certificate_provider_lifecycle(client):
    """Test CertificateProvider CRUD lifecycle."""
    # CREATE
    create_resp = client.create_certificate_provider(
        certificateProviderName="test-name-1",
        lambdaFunctionArn="arn:aws:iam::123456789012:role/test-role",
        accountDefaultForOperations=["CreateCertificateFromCsr"],
    )
    assert isinstance(create_resp.get("certificateProviderName"), str)
    assert len(create_resp.get("certificateProviderName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_certificate_provider(
        certificateProviderName="test-name-1",
    )
    assert isinstance(desc_resp.get("certificateProviderName"), str)
    assert len(desc_resp.get("certificateProviderName", "")) > 0
    assert isinstance(desc_resp.get("accountDefaultForOperations", []), list)

    # DELETE
    client.delete_certificate_provider(
        certificateProviderName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_certificate_provider(
            certificateProviderName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_certificate_provider_not_found(client):
    """Test that describing a non-existent CertificateProvider raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_certificate_provider(
            certificateProviderName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_command_lifecycle(client):
    """Test Command CRUD lifecycle."""
    # CREATE
    create_resp = client.create_command(
        commandId="test-id-1",
    )
    assert isinstance(create_resp.get("commandId"), str)
    assert len(create_resp.get("commandId", "")) > 0

    # DESCRIBE
    desc_resp = client.get_command(
        commandId="test-id-1",
    )
    assert isinstance(desc_resp.get("commandId"), str)
    assert len(desc_resp.get("commandId", "")) > 0
    assert isinstance(desc_resp.get("mandatoryParameters", []), list)
    assert isinstance(desc_resp.get("payload", {}), dict)
    assert isinstance(desc_resp.get("preprocessor", {}), dict)

    # DELETE
    client.delete_command(
        commandId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_command(
            commandId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_command_not_found(client):
    """Test that describing a non-existent Command raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_command(
            commandId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_custom_metric_lifecycle(client):
    """Test CustomMetric CRUD lifecycle."""
    # CREATE
    create_resp = client.create_custom_metric(
        metricName="test-name-1",
        metricType="string-list",
        clientRequestToken="test-string",
    )
    assert isinstance(create_resp.get("metricName"), str)
    assert len(create_resp.get("metricName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_custom_metric(
        metricName="test-name-1",
    )
    assert isinstance(desc_resp.get("metricName"), str)
    assert len(desc_resp.get("metricName", "")) > 0

    # DELETE
    client.delete_custom_metric(
        metricName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_custom_metric(
            metricName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_custom_metric_not_found(client):
    """Test that describing a non-existent CustomMetric raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_custom_metric(
            metricName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_detect_mitigation_actions_task_lifecycle(client):
    """Test DetectMitigationActionsTask CRUD lifecycle."""
    # CREATE
    create_resp = client.start_detect_mitigation_actions_task(
        taskId="test-id-1",
        target={},
        actions=["test-string"],
        clientRequestToken="test-string",
    )
    assert isinstance(create_resp.get("taskId"), str)
    assert len(create_resp.get("taskId", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_detect_mitigation_actions_task(
        taskId="test-id-1",
    )
    assert isinstance(desc_resp.get("taskSummary", {}), dict)


def test_detect_mitigation_actions_task_not_found(client):
    """Test that describing a non-existent DetectMitigationActionsTask raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_detect_mitigation_actions_task(
            taskId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_dimension_lifecycle(client):
    """Test Dimension CRUD lifecycle."""
    # CREATE
    create_resp = client.create_dimension(
        name="test-name-1",
        type="TOPIC_FILTER",
        stringValues=["test-string"],
        clientRequestToken="test-string",
    )
    assert isinstance(create_resp.get("name"), str)
    assert len(create_resp.get("name", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_dimension(
        name="test-name-1",
    )
    assert isinstance(desc_resp.get("name"), str)
    assert len(desc_resp.get("name", "")) > 0
    assert isinstance(desc_resp.get("stringValues", []), list)

    # DELETE
    client.delete_dimension(
        name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_dimension(
            name="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_dimension_not_found(client):
    """Test that describing a non-existent Dimension raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_dimension(
            name="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_domain_configuration_lifecycle(client):
    """Test DomainConfiguration CRUD lifecycle."""
    # CREATE
    create_resp = client.create_domain_configuration(
        domainConfigurationName="test-name-1",
    )
    assert isinstance(create_resp.get("domainConfigurationName"), str)
    assert len(create_resp.get("domainConfigurationName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_domain_configuration(
        domainConfigurationName="test-name-1",
    )
    assert isinstance(desc_resp.get("domainConfigurationName"), str)
    assert len(desc_resp.get("domainConfigurationName", "")) > 0
    assert isinstance(desc_resp.get("serverCertificates", []), list)
    assert isinstance(desc_resp.get("authorizerConfig", {}), dict)
    assert isinstance(desc_resp.get("tlsConfig", {}), dict)
    assert isinstance(desc_resp.get("serverCertificateConfig", {}), dict)
    assert isinstance(desc_resp.get("clientCertificateConfig", {}), dict)

    # DELETE
    client.delete_domain_configuration(
        domainConfigurationName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_domain_configuration(
            domainConfigurationName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_domain_configuration_not_found(client):
    """Test that describing a non-existent DomainConfiguration raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_domain_configuration(
            domainConfigurationName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_fleet_metric_lifecycle(client):
    """Test FleetMetric CRUD lifecycle."""
    # CREATE
    create_resp = client.create_fleet_metric(
        metricName="test-name-1",
        queryString="test-string",
        aggregationType={"name": "Statistics"},
        period=60,
        aggregationField="test-string",
    )
    assert isinstance(create_resp.get("metricName"), str)
    assert len(create_resp.get("metricName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_fleet_metric(
        metricName="test-name-1",
    )
    assert isinstance(desc_resp.get("metricName"), str)
    assert len(desc_resp.get("metricName", "")) > 0
    assert isinstance(desc_resp.get("aggregationType", {}), dict)

    # DELETE
    client.delete_fleet_metric(
        metricName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_fleet_metric(
            metricName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_fleet_metric_not_found(client):
    """Test that describing a non-existent FleetMetric raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_fleet_metric(
            metricName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_job_lifecycle(client):
    """Test Job CRUD lifecycle."""
    # CREATE
    create_resp = client.create_job(
        jobId="test-id-1",
        targets=["test-string"],
    )
    assert isinstance(create_resp.get("jobId"), str)
    assert len(create_resp.get("jobId", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_job(
        jobId="test-id-1",
    )
    assert isinstance(desc_resp.get("job", {}), dict)

    # DELETE
    client.delete_job(
        jobId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_job(
            jobId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_job_not_found(client):
    """Test that describing a non-existent Job raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_job(
            jobId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_job_template_lifecycle(client):
    """Test JobTemplate CRUD lifecycle."""
    # CREATE
    create_resp = client.create_job_template(
        jobTemplateId="test-id-1",
        description="test-string",
    )
    assert isinstance(create_resp.get("jobTemplateId"), str)
    assert len(create_resp.get("jobTemplateId", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_job_template(
        jobTemplateId="test-id-1",
    )
    assert isinstance(desc_resp.get("jobTemplateId"), str)
    assert len(desc_resp.get("jobTemplateId", "")) > 0
    assert isinstance(desc_resp.get("presignedUrlConfig", {}), dict)
    assert isinstance(desc_resp.get("jobExecutionsRolloutConfig", {}), dict)
    assert isinstance(desc_resp.get("abortConfig", {}), dict)
    assert isinstance(desc_resp.get("timeoutConfig", {}), dict)
    assert isinstance(desc_resp.get("jobExecutionsRetryConfig", {}), dict)
    assert isinstance(desc_resp.get("maintenanceWindows", []), list)
    assert isinstance(desc_resp.get("destinationPackageVersions", []), list)

    # DELETE
    client.delete_job_template(
        jobTemplateId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_job_template(
            jobTemplateId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_job_template_not_found(client):
    """Test that describing a non-existent JobTemplate raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_job_template(
            jobTemplateId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_mitigation_action_lifecycle(client):
    """Test MitigationAction CRUD lifecycle."""
    # CREATE
    client.create_mitigation_action(
        actionName="test-name-1",
        roleArn="arn:aws:iam::123456789012:role/test-role",
        actionParams={},
    )

    # DESCRIBE
    desc_resp = client.describe_mitigation_action(
        actionName="test-name-1",
    )
    assert isinstance(desc_resp.get("actionName"), str)
    assert len(desc_resp.get("actionName", "")) > 0
    assert isinstance(desc_resp.get("actionParams", {}), dict)

    # DELETE
    client.delete_mitigation_action(
        actionName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_mitigation_action(
            actionName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_mitigation_action_not_found(client):
    """Test that describing a non-existent MitigationAction raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_mitigation_action(
            actionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_ota_update_lifecycle(client):
    """Test OTAUpdate CRUD lifecycle."""
    # CREATE
    create_resp = client.create_ota_update(
        otaUpdateId="test-id-1",
        targets=["test-string"],
        files=[{}],
        roleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("otaUpdateId"), str)
    assert len(create_resp.get("otaUpdateId", "")) > 0

    # DESCRIBE
    desc_resp = client.get_ota_update(
        otaUpdateId="test-id-1",
    )
    assert isinstance(desc_resp.get("otaUpdateInfo", {}), dict)

    # DELETE
    client.delete_ota_update(
        otaUpdateId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_ota_update(
            otaUpdateId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_ota_update_not_found(client):
    """Test that describing a non-existent OTAUpdate raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_ota_update(
            otaUpdateId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_package_lifecycle(client):
    """Test Package CRUD lifecycle."""
    # CREATE
    create_resp = client.create_package(
        packageName="test-name-1",
    )
    assert isinstance(create_resp.get("packageName"), str)
    assert len(create_resp.get("packageName", "")) > 0

    # DESCRIBE
    desc_resp = client.get_package(
        packageName="test-name-1",
    )
    assert isinstance(desc_resp.get("packageName"), str)
    assert len(desc_resp.get("packageName", "")) > 0

    # DELETE
    client.delete_package(
        packageName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_package(
            packageName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_package_not_found(client):
    """Test that describing a non-existent Package raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_package(
            packageName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_package_version_lifecycle(client):
    """Test PackageVersion CRUD lifecycle."""
    # Pre-cleanup + create package prerequisite
    try:
        client.delete_package(packageName="test-pkg-for-version")
    except ClientError as e:
        logging.debug("pre-cleanup skipped: %s", e)
    client.create_package(packageName="test-pkg-for-version")
    # CREATE
    create_resp = client.create_package_version(
        packageName="test-pkg-for-version",
        versionName="1.0.0",
    )
    assert isinstance(create_resp.get("packageName"), str)
    assert len(create_resp.get("packageName", "")) > 0
    assert isinstance(create_resp.get("versionName"), str)
    assert len(create_resp.get("versionName", "")) > 0
    assert isinstance(create_resp.get("attributes", {}), dict)

    # DESCRIBE
    desc_resp = client.get_package_version(
        packageName="test-pkg-for-version",
        versionName="1.0.0",
    )
    assert isinstance(desc_resp.get("packageName"), str)
    assert len(desc_resp.get("packageName", "")) > 0
    assert isinstance(desc_resp.get("versionName"), str)
    assert len(desc_resp.get("versionName", "")) > 0
    assert isinstance(desc_resp.get("attributes", {}), dict)
    assert isinstance(desc_resp.get("artifact", {}), dict)
    assert isinstance(desc_resp.get("sbom", {}), dict)

    # DELETE
    client.delete_package_version(
        packageName="test-pkg-for-version",
        versionName="1.0.0",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_package_version(
            packageName="test-pkg-for-version",
            versionName="1.0.0",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_package_version_not_found(client):
    """Test that describing a non-existent PackageVersion raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_package_version(
            packageName="fake-id",
            versionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_policy_lifecycle(client):
    """Test Policy CRUD lifecycle."""
    # CREATE
    create_resp = client.create_policy(
        policyName="test-name-1",
        policyDocument="test-string",
    )
    assert isinstance(create_resp.get("policyName"), str)
    assert len(create_resp.get("policyName", "")) > 0

    # DESCRIBE
    desc_resp = client.get_policy(
        policyName="test-name-1",
    )
    assert isinstance(desc_resp.get("policyName"), str)
    assert len(desc_resp.get("policyName", "")) > 0

    # DELETE
    client.delete_policy(
        policyName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_policy(
            policyName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_policy_not_found(client):
    """Test that describing a non-existent Policy raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_policy(
            policyName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_policy_version_lifecycle(client):
    """Test PolicyVersion CRUD lifecycle."""
    # Pre-cleanup + create policy prerequisite
    try:
        client.delete_policy(policyName="test-policy-for-version")
    except ClientError as e:
        logging.debug("pre-cleanup skipped: %s", e)
    client.create_policy(
        policyName="test-policy-for-version",
        policyDocument='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}',
    )
    # CREATE (adds a new version to existing policy)
    create_resp = client.create_policy_version(
        policyName="test-policy-for-version",
        policyDocument='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:Connect","Resource":"*"}]}',
    )
    assert isinstance(create_resp.get("policyVersionId"), str)
    assert len(create_resp.get("policyVersionId", "")) > 0

    policy_version_id = create_resp["policyVersionId"]

    # DESCRIBE
    desc_resp = client.get_policy_version(
        policyName="test-policy-for-version",
        policyVersionId=policy_version_id,
    )
    assert isinstance(desc_resp.get("policyName"), str)
    assert len(desc_resp.get("policyName", "")) > 0
    assert isinstance(desc_resp.get("policyVersionId"), str)
    assert len(desc_resp.get("policyVersionId", "")) > 0

    # DELETE
    client.delete_policy_version(
        policyName="test-policy-for-version",
        policyVersionId=policy_version_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_policy_version(
            policyName="test-policy-for-version",
            policyVersionId=policy_version_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_policy_version_not_found(client):
    """Test that describing a non-existent PolicyVersion raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_policy_version(
            policyName="fake-id",
            policyVersionId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_provisioning_template_lifecycle(client):
    """Test ProvisioningTemplate CRUD lifecycle."""
    # CREATE
    create_resp = client.create_provisioning_template(
        templateName="test-name-1",
        templateBody="test-string",
        provisioningRoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("templateName"), str)
    assert len(create_resp.get("templateName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_provisioning_template(
        templateName="test-name-1",
    )
    assert isinstance(desc_resp.get("templateName"), str)
    assert len(desc_resp.get("templateName", "")) > 0
    assert isinstance(desc_resp.get("preProvisioningHook", {}), dict)

    # DELETE
    client.delete_provisioning_template(
        templateName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_provisioning_template(
            templateName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_provisioning_template_not_found(client):
    """Test that describing a non-existent ProvisioningTemplate raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_provisioning_template(
            templateName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_provisioning_template_version_lifecycle(client):
    """Test ProvisioningTemplateVersion CRUD lifecycle."""
    # Pre-cleanup + create provisioning template prerequisite
    try:
        client.delete_provisioning_template(templateName="test-tmpl-for-version")
    except ClientError as e:
        logging.debug("pre-cleanup skipped: %s", e)
    client.create_provisioning_template(
        templateName="test-tmpl-for-version",
        templateBody='{"Parameters":{},"Resources":{}}',
        provisioningRoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    # CREATE
    create_resp = client.create_provisioning_template_version(
        templateName="test-tmpl-for-version",
        templateBody='{"Parameters":{},"Resources":{"thing":{"Type":"AWS::IoT::Thing","Properties":{}}}}',
    )
    assert isinstance(create_resp.get("templateName"), str)
    assert len(create_resp.get("templateName", "")) > 0
    assert isinstance(create_resp.get("versionId"), int)

    version_id = create_resp["versionId"]

    # DESCRIBE
    desc_resp = client.describe_provisioning_template_version(
        templateName="test-tmpl-for-version",
        versionId=version_id,
    )
    assert isinstance(desc_resp.get("versionId"), int)

    # DELETE
    client.delete_provisioning_template_version(
        templateName="test-tmpl-for-version",
        versionId=version_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_provisioning_template_version(
            templateName="test-tmpl-for-version",
            versionId=version_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_provisioning_template_version_not_found(client):
    """Test that describing a non-existent ProvisioningTemplateVersion raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_provisioning_template_version(
            templateName="fake-id",
            versionId=99999,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_role_alias_lifecycle(client):
    """Test RoleAlias CRUD lifecycle."""
    # CREATE
    create_resp = client.create_role_alias(
        roleAlias="test-string",
        roleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("roleAlias"), str)
    assert len(create_resp.get("roleAlias", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_role_alias(
        roleAlias="test-string",
    )
    assert isinstance(desc_resp.get("roleAliasDescription", {}), dict)

    # DELETE
    client.delete_role_alias(
        roleAlias="test-string",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_role_alias(
            roleAlias="test-string",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_role_alias_not_found(client):
    """Test that describing a non-existent RoleAlias raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_role_alias(
            roleAlias="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_scheduled_audit_lifecycle(client):
    """Test ScheduledAudit CRUD lifecycle."""
    # CREATE
    client.create_scheduled_audit(
        frequency="DAILY",
        targetCheckNames=["test-string"],
        scheduledAuditName="test-name-1",
    )

    # DESCRIBE
    desc_resp = client.describe_scheduled_audit(
        scheduledAuditName="test-name-1",
    )
    assert isinstance(desc_resp.get("targetCheckNames", []), list)
    assert isinstance(desc_resp.get("scheduledAuditName"), str)
    assert len(desc_resp.get("scheduledAuditName", "")) > 0

    # DELETE
    client.delete_scheduled_audit(
        scheduledAuditName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_scheduled_audit(
            scheduledAuditName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_scheduled_audit_not_found(client):
    """Test that describing a non-existent ScheduledAudit raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_scheduled_audit(
            scheduledAuditName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_security_profile_lifecycle(client):
    """Test SecurityProfile CRUD lifecycle."""
    # CREATE
    create_resp = client.create_security_profile(
        securityProfileName="test-name-1",
    )
    assert isinstance(create_resp.get("securityProfileName"), str)
    assert len(create_resp.get("securityProfileName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_security_profile(
        securityProfileName="test-name-1",
    )
    assert isinstance(desc_resp.get("securityProfileName"), str)
    assert len(desc_resp.get("securityProfileName", "")) > 0
    assert isinstance(desc_resp.get("behaviors", []), list)
    assert isinstance(desc_resp.get("alertTargets", {}), dict)
    assert isinstance(desc_resp.get("additionalMetricsToRetain", []), list)
    assert isinstance(desc_resp.get("additionalMetricsToRetainV2", []), list)
    assert isinstance(desc_resp.get("metricsExportConfig", {}), dict)

    # DELETE
    client.delete_security_profile(
        securityProfileName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_security_profile(
            securityProfileName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_security_profile_not_found(client):
    """Test that describing a non-existent SecurityProfile raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_security_profile(
            securityProfileName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_stream_lifecycle(client):
    """Test Stream CRUD lifecycle."""
    # CREATE
    create_resp = client.create_stream(
        streamId="test-id-1",
        files=[{}],
        roleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("streamId"), str)
    assert len(create_resp.get("streamId", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_stream(
        streamId="test-id-1",
    )
    assert isinstance(desc_resp.get("streamInfo", {}), dict)

    # DELETE
    client.delete_stream(
        streamId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_stream(
            streamId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_stream_not_found(client):
    """Test that describing a non-existent Stream raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_stream(
            streamId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_thing_lifecycle(client):
    """Test Thing CRUD lifecycle."""
    # CREATE
    create_resp = client.create_thing(
        thingName="test-name-1",
    )
    assert isinstance(create_resp.get("thingName"), str)
    assert len(create_resp.get("thingName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_thing(
        thingName="test-name-1",
    )
    assert isinstance(desc_resp.get("thingName"), str)
    assert len(desc_resp.get("thingName", "")) > 0
    assert isinstance(desc_resp.get("attributes", {}), dict)

    # DELETE
    client.delete_thing(
        thingName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_thing(
            thingName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_thing_not_found(client):
    """Test that describing a non-existent Thing raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_thing(
            thingName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_thing_group_lifecycle(client):
    """Test ThingGroup CRUD lifecycle."""
    # CREATE
    create_resp = client.create_thing_group(
        thingGroupName="test-name-1",
    )
    assert isinstance(create_resp.get("thingGroupName"), str)
    assert len(create_resp.get("thingGroupName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_thing_group(
        thingGroupName="test-name-1",
    )
    assert isinstance(desc_resp.get("thingGroupName"), str)
    assert len(desc_resp.get("thingGroupName", "")) > 0
    assert isinstance(desc_resp.get("thingGroupProperties", {}), dict)
    assert isinstance(desc_resp.get("thingGroupMetadata", {}), dict)

    # DELETE
    client.delete_thing_group(
        thingGroupName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_thing_group(
            thingGroupName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_thing_group_not_found(client):
    """Test that describing a non-existent ThingGroup raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_thing_group(
            thingGroupName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_thing_registration_task_lifecycle(client):
    """Test ThingRegistrationTask CRUD lifecycle."""
    # CREATE
    create_resp = client.start_thing_registration_task(
        templateBody="test-string",
        inputFileBucket="test-string",
        inputFileKey="test-string",
        roleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("taskId"), str)
    assert len(create_resp.get("taskId", "")) > 0

    task_id = create_resp["taskId"]

    # DESCRIBE
    desc_resp = client.describe_thing_registration_task(
        taskId=task_id,
    )
    assert isinstance(desc_resp.get("taskId"), str)
    assert len(desc_resp.get("taskId", "")) > 0

    # DELETE
    client.stop_thing_registration_task(
        taskId=task_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_thing_registration_task(
            taskId=task_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_thing_registration_task_not_found(client):
    """Test that describing a non-existent ThingRegistrationTask raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_thing_registration_task(
            taskId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_thing_type_lifecycle(client):
    """Test ThingType CRUD lifecycle."""
    # CREATE
    create_resp = client.create_thing_type(
        thingTypeName="test-name-1",
    )
    assert isinstance(create_resp.get("thingTypeName"), str)
    assert len(create_resp.get("thingTypeName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_thing_type(
        thingTypeName="test-name-1",
    )
    assert isinstance(desc_resp.get("thingTypeName"), str)
    assert len(desc_resp.get("thingTypeName", "")) > 0
    assert isinstance(desc_resp.get("thingTypeProperties", {}), dict)
    assert isinstance(desc_resp.get("thingTypeMetadata", {}), dict)

    # DELETE
    client.delete_thing_type(
        thingTypeName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_thing_type(
            thingTypeName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_thing_type_not_found(client):
    """Test that describing a non-existent ThingType raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_thing_type(
            thingTypeName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_topic_rule_lifecycle(client):
    """Test TopicRule CRUD lifecycle."""
    # CREATE
    client.create_topic_rule(
        ruleName="testname1",
        topicRulePayload={
            "sql": "SELECT * FROM 'test/topic'",
            "actions": [
                {"lambda": {"functionArn": "arn:aws:lambda:us-east-1:123456789012:function:test"}}
            ],
        },
    )

    # DESCRIBE
    desc_resp = client.get_topic_rule(
        ruleName="testname1",
    )
    assert isinstance(desc_resp.get("rule", {}), dict)

    # DELETE
    client.delete_topic_rule(
        ruleName="testname1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_topic_rule(
            ruleName="testname1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_topic_rule_not_found(client):
    """Test that describing a non-existent TopicRule raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_topic_rule(
            ruleName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_topic_rule_destination_lifecycle(client):
    """Test TopicRuleDestination CRUD lifecycle."""
    # CREATE
    create_resp = client.create_topic_rule_destination(
        destinationConfiguration={
            "httpUrlConfiguration": {"confirmationUrl": "https://example.com/confirm"}
        },
    )
    assert isinstance(create_resp.get("topicRuleDestination", {}), dict)
    dest_arn = create_resp["topicRuleDestination"]["arn"]

    # DESCRIBE
    desc_resp = client.get_topic_rule_destination(
        arn=dest_arn,
    )
    assert isinstance(desc_resp.get("topicRuleDestination", {}), dict)

    # DELETE
    client.delete_topic_rule_destination(
        arn=dest_arn,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_topic_rule_destination(
            arn=dest_arn,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_topic_rule_destination_not_found(client):
    """Test that describing a non-existent TopicRuleDestination raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_topic_rule_destination(
            arn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
