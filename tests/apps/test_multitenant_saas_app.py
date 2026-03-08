"""
Multi-Tenant SaaS Platform Tests

Simulates a B2B SaaS platform with strict tenant isolation:
- DynamoDB for tenant data (partition key prefix isolation)
- SSM Parameter Store for per-tenant configuration
- Secrets Manager for per-tenant database credentials
- S3 for per-tenant file storage (prefix isolation)
- SQS for tenant onboarding task queues
- CloudWatch for per-tenant usage metrics
"""

import json

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tenant_data_table(dynamodb, unique_name):
    table_name = f"tenant-data-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "tenant_id", "KeyType": "HASH"},
            {"AttributeName": "entity_key", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "tenant_id", "AttributeType": "S"},
            {"AttributeName": "entity_key", "AttributeType": "S"},
            {"AttributeName": "entity_type", "AttributeType": "S"},
            {"AttributeName": "created_at", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-entity-type",
                "KeySchema": [
                    {"AttributeName": "entity_type", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def tenant_bucket(s3, unique_name):
    bucket_name = f"tenant-files-{unique_name}"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name
    # Cleanup: delete all objects then bucket
    resp = s3.list_objects_v2(Bucket=bucket_name)
    for obj in resp.get("Contents", []):
        s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
    s3.delete_bucket(Bucket=bucket_name)


@pytest.fixture
def onboarding_queue(sqs, unique_name):
    queue_name = f"onboarding-{unique_name}"
    resp = sqs.create_queue(QueueName=queue_name)
    url = resp["QueueUrl"]
    yield url
    sqs.delete_queue(QueueUrl=url)


@pytest.fixture
def tenant_configs(ssm, unique_name):
    prefix = f"/tenants/{unique_name}"
    params = []
    configs = {
        "tenant-a": {
            "plan_tier": "starter",
            "max_users": "10",
            "features_enabled": "billing,reports",
        },
        "tenant-b": {
            "plan_tier": "enterprise",
            "max_users": "500",
            "features_enabled": "billing,reports,sso,audit,api_access",
        },
    }
    for tenant, settings in configs.items():
        for key, value in settings.items():
            param_name = f"{prefix}/{tenant}/{key}"
            ssm.put_parameter(Name=param_name, Value=value, Type="String")
            params.append(param_name)
    yield prefix, configs
    for param_name in params:
        ssm.delete_parameter(Name=param_name)


@pytest.fixture
def tenant_secrets(secretsmanager, unique_name):
    secrets = {}
    for tenant in ("tenant-a", "tenant-b"):
        name = f"{unique_name}/{tenant}/db-credentials"
        value = json.dumps(
            {
                "host": f"{tenant}-db.internal.example.com",
                "port": 5432,
                "db_name": f"saas_{tenant.replace('-', '_')}",
                "username": f"{tenant}_app",
                "password": f"secret-{tenant}-initial",
            }
        )
        secretsmanager.create_secret(Name=name, SecretString=value)
        secrets[tenant] = name
    yield secrets
    for name in secrets.values():
        secretsmanager.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


@pytest.fixture
def usage_namespace(unique_name):
    return f"SaaS/{unique_name}"


# ---------------------------------------------------------------------------
# Test Classes
# ---------------------------------------------------------------------------


class TestTenantIsolation:
    """DynamoDB tenant data isolation tests."""

    def test_create_tenant_entities(self, dynamodb, tenant_data_table):
        """Create entities for two tenants, verify correct counts per tenant."""
        table = tenant_data_table
        # Tenant A: 2 users + 1 project = 3 entities
        for item in [
            {
                "tenant_id": {"S": "tenant-a"},
                "entity_key": {"S": "USER#1"},
                "entity_type": {"S": "USER"},
                "created_at": {"S": "2026-01-01T00:00:00Z"},
                "display_name": {"S": "Alice"},
            },
            {
                "tenant_id": {"S": "tenant-a"},
                "entity_key": {"S": "USER#2"},
                "entity_type": {"S": "USER"},
                "created_at": {"S": "2026-01-02T00:00:00Z"},
                "display_name": {"S": "Bob"},
            },
            {
                "tenant_id": {"S": "tenant-a"},
                "entity_key": {"S": "PROJECT#1"},
                "entity_type": {"S": "PROJECT"},
                "created_at": {"S": "2026-01-03T00:00:00Z"},
                "display_name": {"S": "Alpha Project"},
            },
        ]:
            dynamodb.put_item(TableName=table, Item=item)

        # Tenant B: 1 user + 1 project = 2 entities
        for item in [
            {
                "tenant_id": {"S": "tenant-b"},
                "entity_key": {"S": "USER#1"},
                "entity_type": {"S": "USER"},
                "created_at": {"S": "2026-01-01T00:00:00Z"},
                "display_name": {"S": "Charlie"},
            },
            {
                "tenant_id": {"S": "tenant-b"},
                "entity_key": {"S": "PROJECT#1"},
                "entity_type": {"S": "PROJECT"},
                "created_at": {"S": "2026-01-02T00:00:00Z"},
                "display_name": {"S": "Beta Project"},
            },
        ]:
            dynamodb.put_item(TableName=table, Item=item)

        # Query tenant-a
        resp_a = dynamodb.query(
            TableName=table,
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": {"S": "tenant-a"}},
        )
        assert resp_a["Count"] == 3

        # Query tenant-b
        resp_b = dynamodb.query(
            TableName=table,
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": {"S": "tenant-b"}},
        )
        assert resp_b["Count"] == 2

    def test_tenant_data_isolation(self, dynamodb, tenant_data_table):
        """Query for one tenant must never return another tenant's data."""
        table = tenant_data_table
        dynamodb.put_item(
            TableName=table,
            Item={
                "tenant_id": {"S": "tenant-a"},
                "entity_key": {"S": "USER#1"},
                "entity_type": {"S": "USER"},
                "created_at": {"S": "2026-01-01T00:00:00Z"},
                "secret_data": {"S": "tenant-a-confidential"},
            },
        )
        dynamodb.put_item(
            TableName=table,
            Item={
                "tenant_id": {"S": "tenant-b"},
                "entity_key": {"S": "USER#1"},
                "entity_type": {"S": "USER"},
                "created_at": {"S": "2026-01-01T00:00:00Z"},
                "secret_data": {"S": "tenant-b-confidential"},
            },
        )

        resp = dynamodb.query(
            TableName=table,
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": {"S": "tenant-a"}},
        )
        tenant_ids = {item["tenant_id"]["S"] for item in resp["Items"]}
        assert tenant_ids == {"tenant-a"}, "Tenant-b data leaked into tenant-a query"
        assert resp["Items"][0]["secret_data"]["S"] == "tenant-a-confidential"

    def test_cross_tenant_entity_query(self, dynamodb, tenant_data_table):
        """GSI query by entity_type spans tenants (admin view), then filter by tenant."""
        table = tenant_data_table
        items = [
            {
                "tenant_id": {"S": "tenant-a"},
                "entity_key": {"S": "USER#1"},
                "entity_type": {"S": "USER"},
                "created_at": {"S": "2026-01-01T00:00:00Z"},
            },
            {
                "tenant_id": {"S": "tenant-a"},
                "entity_key": {"S": "USER#2"},
                "entity_type": {"S": "USER"},
                "created_at": {"S": "2026-01-02T00:00:00Z"},
            },
            {
                "tenant_id": {"S": "tenant-b"},
                "entity_key": {"S": "USER#1"},
                "entity_type": {"S": "USER"},
                "created_at": {"S": "2026-01-03T00:00:00Z"},
            },
            {
                "tenant_id": {"S": "tenant-a"},
                "entity_key": {"S": "PROJECT#1"},
                "entity_type": {"S": "PROJECT"},
                "created_at": {"S": "2026-01-04T00:00:00Z"},
            },
        ]
        for item in items:
            dynamodb.put_item(TableName=table, Item=item)

        # Admin view: all USERs across tenants via GSI
        resp = dynamodb.query(
            TableName=table,
            IndexName="by-entity-type",
            KeyConditionExpression="entity_type = :et",
            ExpressionAttributeValues={":et": {"S": "USER"}},
        )
        assert resp["Count"] == 3
        returned_tenants = {item["tenant_id"]["S"] for item in resp["Items"]}
        assert returned_tenants == {"tenant-a", "tenant-b"}

        # Filter to tenant-a only
        resp_filtered = dynamodb.query(
            TableName=table,
            IndexName="by-entity-type",
            KeyConditionExpression="entity_type = :et",
            FilterExpression="tenant_id = :tid",
            ExpressionAttributeValues={
                ":et": {"S": "USER"},
                ":tid": {"S": "tenant-a"},
            },
        )
        assert resp_filtered["Count"] == 2
        for item in resp_filtered["Items"]:
            assert item["tenant_id"]["S"] == "tenant-a"

    def test_tenant_entity_update(self, dynamodb, tenant_data_table):
        """Update tenant-a entity without affecting tenant-b's same entity key."""
        table = tenant_data_table
        for tenant in ("tenant-a", "tenant-b"):
            dynamodb.put_item(
                TableName=table,
                Item={
                    "tenant_id": {"S": tenant},
                    "entity_key": {"S": "USER#1"},
                    "entity_type": {"S": "USER"},
                    "created_at": {"S": "2026-01-01T00:00:00Z"},
                    "display_name": {"S": f"Original-{tenant}"},
                },
            )

        # Update tenant-a's USER#1
        dynamodb.update_item(
            TableName=table,
            Key={"tenant_id": {"S": "tenant-a"}, "entity_key": {"S": "USER#1"}},
            UpdateExpression="SET display_name = :dn",
            ExpressionAttributeValues={":dn": {"S": "Updated-Alice"}},
        )

        # Verify tenant-a updated
        resp_a = dynamodb.get_item(
            TableName=table,
            Key={"tenant_id": {"S": "tenant-a"}, "entity_key": {"S": "USER#1"}},
        )
        assert resp_a["Item"]["display_name"]["S"] == "Updated-Alice"

        # Verify tenant-b unchanged
        resp_b = dynamodb.get_item(
            TableName=table,
            Key={"tenant_id": {"S": "tenant-b"}, "entity_key": {"S": "USER#1"}},
        )
        assert resp_b["Item"]["display_name"]["S"] == "Original-tenant-b"


class TestTenantConfiguration:
    """SSM Parameter Store per-tenant configuration tests."""

    def test_per_tenant_config_tree(self, ssm, tenant_configs):
        """Each tenant has its own config subtree with correct values."""
        prefix, expected = tenant_configs

        for tenant, settings in expected.items():
            resp = ssm.get_parameters_by_path(Path=f"{prefix}/{tenant}/", Recursive=False)
            params = {p["Name"].split("/")[-1]: p["Value"] for p in resp["Parameters"]}
            assert params["plan_tier"] == settings["plan_tier"]
            assert params["max_users"] == settings["max_users"]
            assert params["features_enabled"] == settings["features_enabled"]

    def test_update_tenant_plan(self, ssm, tenant_configs):
        """Overwrite a tenant's plan tier and verify the change."""
        prefix, _ = tenant_configs
        param_name = f"{prefix}/tenant-a/plan_tier"

        ssm.put_parameter(Name=param_name, Value="business", Type="String", Overwrite=True)

        resp = ssm.get_parameter(Name=param_name)
        assert resp["Parameter"]["Value"] == "business"

    def test_config_hierarchy(self, ssm, tenant_configs, unique_name):
        """Global config params appear alongside tenant-specific params with recursive fetch."""
        prefix, _ = tenant_configs
        global_param = f"{prefix}/tenant-a/global/maintenance_mode"
        ssm.put_parameter(Name=global_param, Value="false", Type="String")

        try:
            resp = ssm.get_parameters_by_path(Path=f"{prefix}/tenant-a/", Recursive=True)
            param_names = {p["Name"] for p in resp["Parameters"]}
            # Should include both regular config and the nested global param
            assert global_param in param_names
            assert f"{prefix}/tenant-a/plan_tier" in param_names
            assert len(resp["Parameters"]) >= 4  # 3 config + 1 global
        finally:
            ssm.delete_parameter(Name=global_param)

    def test_tenant_feature_flags(self, ssm, tenant_configs):
        """Parse comma-separated feature flags from config, verify per-tenant features."""
        prefix, expected = tenant_configs

        for tenant, settings in expected.items():
            resp = ssm.get_parameter(Name=f"{prefix}/{tenant}/features_enabled")
            features = resp["Parameter"]["Value"].split(",")
            expected_features = settings["features_enabled"].split(",")
            assert set(features) == set(expected_features)

        # Tenant-b (enterprise) has more features than tenant-a (starter)
        resp_a = ssm.get_parameter(Name=f"{prefix}/tenant-a/features_enabled")
        resp_b = ssm.get_parameter(Name=f"{prefix}/tenant-b/features_enabled")
        features_a = set(resp_a["Parameter"]["Value"].split(","))
        features_b = set(resp_b["Parameter"]["Value"].split(","))
        assert features_a < features_b, "Enterprise should have superset of starter features"


class TestTenantStorage:
    """S3 prefix-based tenant file isolation tests."""

    def test_tenant_file_upload(self, s3, tenant_bucket):
        """Upload files per-tenant prefix, list verifies isolation."""
        s3.put_object(
            Bucket=tenant_bucket,
            Key="tenant-a/docs/file1.txt",
            Body=b"Tenant A document",
        )
        s3.put_object(
            Bucket=tenant_bucket,
            Key="tenant-b/docs/file1.txt",
            Body=b"Tenant B document",
        )

        resp = s3.list_objects_v2(Bucket=tenant_bucket, Prefix="tenant-a/")
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert keys == ["tenant-a/docs/file1.txt"]
        assert resp["KeyCount"] == 1

    def test_tenant_storage_quota(self, s3, tenant_bucket):
        """Sum object sizes to compute per-tenant storage usage."""
        sizes = [100, 200, 300, 400, 500]
        for i, size in enumerate(sizes):
            s3.put_object(
                Bucket=tenant_bucket,
                Key=f"tenant-a/data/file{i}.bin",
                Body=b"x" * size,
            )

        resp = s3.list_objects_v2(Bucket=tenant_bucket, Prefix="tenant-a/")
        total_size = sum(obj["Size"] for obj in resp["Contents"])
        assert total_size == sum(sizes)
        assert resp["KeyCount"] == 5

    def test_cross_tenant_file_isolation(self, s3, tenant_bucket):
        """Neither tenant can see the other's files via prefix listing."""
        for tenant in ("tenant-a", "tenant-b"):
            for i in range(3):
                s3.put_object(
                    Bucket=tenant_bucket,
                    Key=f"{tenant}/files/item{i}.txt",
                    Body=f"Data for {tenant} item {i}".encode(),
                )

        for tenant in ("tenant-a", "tenant-b"):
            resp = s3.list_objects_v2(Bucket=tenant_bucket, Prefix=f"{tenant}/")
            assert resp["KeyCount"] == 3
            for obj in resp["Contents"]:
                assert obj["Key"].startswith(f"{tenant}/"), (
                    f"File {obj['Key']} leaked into {tenant} listing"
                )


class TestTenantOnboarding:
    """SQS + SecretsManager onboarding workflow tests."""

    def test_onboarding_task_queue(self, sqs, onboarding_queue):
        """Send onboarding tasks as JSON messages, receive and verify all task types."""
        tasks = ["create_db", "seed_data", "configure_dns"]
        for task_type in tasks:
            sqs.send_message(
                QueueUrl=onboarding_queue,
                MessageBody=json.dumps(
                    {"task_type": task_type, "tenant_id": "tenant-new", "priority": "high"}
                ),
            )

        received_tasks = []
        for _ in range(5):  # poll a few times to collect all
            resp = sqs.receive_message(
                QueueUrl=onboarding_queue, MaxNumberOfMessages=10, WaitTimeSeconds=1
            )
            for msg in resp.get("Messages", []):
                body = json.loads(msg["Body"])
                received_tasks.append(body["task_type"])
                sqs.delete_message(QueueUrl=onboarding_queue, ReceiptHandle=msg["ReceiptHandle"])
            if len(received_tasks) >= 3:
                break

        assert set(received_tasks) == set(tasks)

    def test_tenant_credential_provisioning(self, secretsmanager, tenant_secrets):
        """Retrieve tenant-specific DB credentials, verify tenant-specific values."""
        resp = secretsmanager.get_secret_value(SecretId=tenant_secrets["tenant-a"])
        creds = json.loads(resp["SecretString"])

        assert creds["host"] == "tenant-a-db.internal.example.com"
        assert creds["port"] == 5432
        assert creds["db_name"] == "saas_tenant_a"
        assert creds["username"] == "tenant-a_app"

    def test_rotate_tenant_credentials(self, secretsmanager, tenant_secrets):
        """Update one tenant's secret, verify other tenant's is unchanged."""
        # Rotate tenant-a password
        new_creds = json.dumps(
            {
                "host": "tenant-a-db.internal.example.com",
                "port": 5432,
                "db_name": "saas_tenant_a",
                "username": "tenant-a_app",
                "password": "rotated-new-password",
            }
        )
        secretsmanager.update_secret(SecretId=tenant_secrets["tenant-a"], SecretString=new_creds)

        # Verify tenant-a has new password
        resp_a = secretsmanager.get_secret_value(SecretId=tenant_secrets["tenant-a"])
        assert json.loads(resp_a["SecretString"])["password"] == "rotated-new-password"

        # Verify tenant-b is unchanged
        resp_b = secretsmanager.get_secret_value(SecretId=tenant_secrets["tenant-b"])
        assert json.loads(resp_b["SecretString"])["password"] == "secret-tenant-b-initial"


class TestTenantMetrics:
    """CloudWatch per-tenant usage metrics and end-to-end tests."""

    def test_per_tenant_usage_metrics(self, cloudwatch, usage_namespace):
        """Publish per-tenant metrics with dimensions, query back by tenant."""
        cloudwatch.put_metric_data(
            Namespace=usage_namespace,
            MetricData=[
                {
                    "MetricName": "ApiCalls",
                    "Dimensions": [{"Name": "TenantId", "Value": "tenant-a"}],
                    "Value": 100,
                    "Unit": "Count",
                },
                {
                    "MetricName": "StorageBytes",
                    "Dimensions": [{"Name": "TenantId", "Value": "tenant-a"}],
                    "Value": 5000,
                    "Unit": "Bytes",
                },
            ],
        )
        cloudwatch.put_metric_data(
            Namespace=usage_namespace,
            MetricData=[
                {
                    "MetricName": "ApiCalls",
                    "Dimensions": [{"Name": "TenantId", "Value": "tenant-b"}],
                    "Value": 50,
                    "Unit": "Count",
                },
            ],
        )

        resp = cloudwatch.get_metric_statistics(
            Namespace=usage_namespace,
            MetricName="ApiCalls",
            Dimensions=[{"Name": "TenantId", "Value": "tenant-a"}],
            StartTime="2026-01-01T00:00:00Z",
            EndTime="2027-01-01T00:00:00Z",
            Period=86400,
            Statistics=["Sum"],
        )
        assert len(resp["Datapoints"]) >= 1
        total = sum(dp["Sum"] for dp in resp["Datapoints"])
        assert total == 100

    def test_aggregate_platform_metrics(self, cloudwatch, usage_namespace):
        """Publish platform-wide metric (no tenant dimension), query it back."""
        cloudwatch.put_metric_data(
            Namespace=usage_namespace,
            MetricData=[
                {
                    "MetricName": "TotalTenants",
                    "Value": 2,
                    "Unit": "Count",
                },
            ],
        )

        resp = cloudwatch.get_metric_statistics(
            Namespace=usage_namespace,
            MetricName="TotalTenants",
            Dimensions=[],
            StartTime="2026-01-01T00:00:00Z",
            EndTime="2027-01-01T00:00:00Z",
            Period=86400,
            Statistics=["Sum"],
        )
        assert len(resp["Datapoints"]) >= 1
        total = sum(dp["Sum"] for dp in resp["Datapoints"])
        assert total == 2

    def test_tenant_alarm(self, cloudwatch, usage_namespace):
        """Create a per-tenant alarm, force it to ALARM state, verify."""
        alarm_name = f"{usage_namespace}-tenant-a-high-api"
        cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace=usage_namespace,
            MetricName="ApiCalls",
            Dimensions=[{"Name": "TenantId", "Value": "tenant-a"}],
            Statistic="Sum",
            Period=300,
            EvaluationPeriods=1,
            Threshold=1000,
            ComparisonOperator="GreaterThanThreshold",
        )

        cloudwatch.set_alarm_state(
            AlarmName=alarm_name,
            StateValue="ALARM",
            StateReason="Testing tenant alarm",
        )

        resp = cloudwatch.describe_alarms(AlarmNames=[alarm_name])
        alarms = resp["MetricAlarms"]
        assert len(alarms) == 1
        assert alarms[0]["AlarmName"] == alarm_name
        assert alarms[0]["StateValue"] == "ALARM"

        # Cleanup
        cloudwatch.delete_alarms(AlarmNames=[alarm_name])

    def test_full_tenant_onboarding(
        self,
        dynamodb,
        tenant_data_table,
        s3,
        tenant_bucket,
        sqs,
        onboarding_queue,
        ssm,
        secretsmanager,
        cloudwatch,
        usage_namespace,
        unique_name,
    ):
        """End-to-end: onboard a new tenant across all services, verify everything."""
        tenant_id = "tenant-new"
        prefix = f"/tenants/{unique_name}"

        # Step 1: Create SSM config for new tenant
        config_params = {
            "plan_tier": "growth",
            "max_users": "50",
            "features_enabled": "billing,reports,api_access",
        }
        created_params = []
        for key, value in config_params.items():
            param_name = f"{prefix}/{tenant_id}/{key}"
            ssm.put_parameter(Name=param_name, Value=value, Type="String")
            created_params.append(param_name)

        # Step 2: Create SecretsManager credentials
        secret_name = f"{unique_name}/{tenant_id}/db-credentials"
        db_creds = {
            "host": f"{tenant_id}-db.internal.example.com",
            "port": 5432,
            "db_name": f"saas_{tenant_id.replace('-', '_')}",
            "username": f"{tenant_id}_app",
            "password": "initial-password",
        }
        secretsmanager.create_secret(Name=secret_name, SecretString=json.dumps(db_creds))

        # Step 3: Create S3 prefix by uploading welcome doc
        welcome_key = f"{tenant_id}/docs/welcome.txt"
        s3.put_object(
            Bucket=tenant_bucket,
            Key=welcome_key,
            Body=b"Welcome to the platform!",
        )

        # Step 4: Send onboarding tasks to SQS
        task_types = ["create_db", "seed_data", "configure_dns"]
        for task_type in task_types:
            sqs.send_message(
                QueueUrl=onboarding_queue,
                MessageBody=json.dumps({"task_type": task_type, "tenant_id": tenant_id}),
            )

        # Step 5: "Process" onboarding tasks (receive and delete)
        processed = []
        for _ in range(5):
            resp = sqs.receive_message(
                QueueUrl=onboarding_queue, MaxNumberOfMessages=10, WaitTimeSeconds=1
            )
            for msg in resp.get("Messages", []):
                body = json.loads(msg["Body"])
                processed.append(body["task_type"])
                sqs.delete_message(QueueUrl=onboarding_queue, ReceiptHandle=msg["ReceiptHandle"])
            if len(processed) >= 3:
                break
        assert set(processed) == set(task_types)

        # Step 6: Insert tenant entity in DynamoDB
        dynamodb.put_item(
            TableName=tenant_data_table,
            Item={
                "tenant_id": {"S": tenant_id},
                "entity_key": {"S": "USER#1"},
                "entity_type": {"S": "USER"},
                "created_at": {"S": "2026-03-08T00:00:00Z"},
                "display_name": {"S": "Admin User"},
                "role": {"S": "admin"},
            },
        )

        # Step 7: Publish usage metric
        cloudwatch.put_metric_data(
            Namespace=usage_namespace,
            MetricData=[
                {
                    "MetricName": "ApiCalls",
                    "Dimensions": [{"Name": "TenantId", "Value": tenant_id}],
                    "Value": 1,
                    "Unit": "Count",
                },
            ],
        )

        # ---- Verification ----

        # Verify DynamoDB
        ddb_resp = dynamodb.query(
            TableName=tenant_data_table,
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": {"S": tenant_id}},
        )
        assert ddb_resp["Count"] == 1
        assert ddb_resp["Items"][0]["display_name"]["S"] == "Admin User"

        # Verify S3
        s3_resp = s3.list_objects_v2(Bucket=tenant_bucket, Prefix=f"{tenant_id}/")
        assert s3_resp["KeyCount"] >= 1
        assert any(obj["Key"] == welcome_key for obj in s3_resp["Contents"])

        # Verify SSM
        ssm_resp = ssm.get_parameters_by_path(Path=f"{prefix}/{tenant_id}/", Recursive=False)
        ssm_params = {p["Name"].split("/")[-1]: p["Value"] for p in ssm_resp["Parameters"]}
        assert ssm_params["plan_tier"] == "growth"
        assert ssm_params["max_users"] == "50"

        # Verify SecretsManager
        secret_resp = secretsmanager.get_secret_value(SecretId=secret_name)
        stored_creds = json.loads(secret_resp["SecretString"])
        assert stored_creds["host"] == f"{tenant_id}-db.internal.example.com"
        assert stored_creds["db_name"] == "saas_tenant_new"

        # Cleanup test-specific resources
        for param_name in created_params:
            ssm.delete_parameter(Name=param_name)
        secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)
