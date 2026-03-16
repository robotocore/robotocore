"""
Multi-Tenant SaaS Platform
===========================

A B2B SaaS platform with strict tenant isolation, built entirely on AWS
primitives.  Think of it as a backend for a multi-tenant analytics product
(like Mixpanel or Amplitude) where each customer (tenant) has their own
isolated data, configuration, credentials, file storage, and usage quotas.

Services used:
- **DynamoDB** -- tenant data (partition-key isolation)
- **S3** -- per-tenant file storage (prefix isolation)
- **SSM Parameter Store** -- per-tenant configuration & feature flags
- **Secrets Manager** -- per-tenant database credentials / API keys
- **SQS** -- asynchronous onboarding task queue
- **CloudWatch** -- per-tenant usage metrics & quota enforcement

Only stdlib + boto3 are imported.  No robotocore / moto internals.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any

from .models import (
    PLAN_CATALOGUE,
    OnboardingTask,
    PlanDefinition,
    Tenant,
    TenantConfig,
    TenantEntity,
)


class QuotaExceededError(Exception):
    """Raised when a tenant exceeds a plan-level quota."""


class TenantNotFoundError(Exception):
    """Raised when an operation targets a non-existent tenant."""


class TenantSuspendedError(Exception):
    """Raised when an operation targets a suspended tenant."""


# ---------------------------------------------------------------------------
# Platform
# ---------------------------------------------------------------------------


class SaaSPlatform:
    """
    Core application object.  Owns all AWS clients and exposes high-level
    operations that enforce tenant isolation invariants.
    """

    def __init__(
        self,
        *,
        dynamodb,
        s3,
        ssm,
        secretsmanager,
        sqs,
        cloudwatch,
        table_name: str,
        bucket_name: str,
        queue_url: str,
        ssm_prefix: str,
        secret_prefix: str,
        metrics_namespace: str,
        plan_catalogue: dict[str, PlanDefinition] | None = None,
    ) -> None:
        # AWS clients
        self._ddb = dynamodb
        self._s3 = s3
        self._ssm = ssm
        self._sm = secretsmanager
        self._sqs = sqs
        self._cw = cloudwatch

        # Resource names
        self._table = table_name
        self._bucket = bucket_name
        self._queue_url = queue_url
        self._ssm_prefix = ssm_prefix  # e.g. /tenants/<unique>
        self._secret_prefix = secret_prefix  # e.g. <unique>
        self._ns = metrics_namespace  # CloudWatch namespace

        # Plan definitions
        self._plans = plan_catalogue or PLAN_CATALOGUE

        # In-memory tenant registry (source-of-truth is DynamoDB, but we
        # keep a local cache to avoid round-trips on every operation).
        self._tenants: dict[str, Tenant] = {}

        # Audit log (in-memory ring buffer)
        self._audit: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Tenant lifecycle
    # ------------------------------------------------------------------

    def provision_tenant(
        self,
        *,
        tenant_id: str,
        name: str,
        plan: str = "free",
        admin_email: str = "",
    ) -> Tenant:
        """
        Provision a brand-new tenant.  Creates:
        1. DynamoDB metadata entity (TENANT#<id>)
        2. SSM config tree (plan, features, limits)
        3. Secrets Manager credentials
        4. S3 welcome file (creates the prefix)
        5. SQS onboarding tasks
        6. CloudWatch baseline metric
        """
        plan_def = self._plans.get(plan)
        if plan_def is None:
            raise ValueError(f"Unknown plan: {plan}")

        tenant = Tenant(
            tenant_id=tenant_id,
            name=name,
            plan=plan,
            status="active",
            admin_email=admin_email,
        )

        # 1. DynamoDB tenant metadata
        self._ddb.put_item(
            TableName=self._table,
            Item={
                "tenant_id": {"S": tenant_id},
                "entity_key": {"S": f"TENANT#{tenant_id}"},
                "entity_type": {"S": "TENANT"},
                "created_at": {"S": tenant.created_at},
                "data": {
                    "S": json.dumps(
                        {
                            "name": name,
                            "plan": plan,
                            "status": "active",
                            "admin_email": admin_email,
                        }
                    )
                },
            },
        )

        # 2. SSM config
        self._write_tenant_config(tenant_id, plan_def)

        # 3. Secrets Manager credentials
        self._create_tenant_credentials(tenant_id)

        # 4. S3 welcome file
        self._s3.put_object(
            Bucket=self._bucket,
            Key=f"{tenant_id}/docs/welcome.txt",
            Body=b"Welcome to the platform!",
        )

        # 5. Onboarding tasks
        for task_type in ("create_db", "seed_data", "configure_dns", "send_welcome"):
            task = OnboardingTask(tenant_id=tenant_id, task_type=task_type)
            kwargs: dict[str, Any] = {
                "QueueUrl": self._queue_url,
                "MessageBody": json.dumps(
                    {
                        "tenant_id": task.tenant_id,
                        "task_type": task.task_type,
                        "status": task.status,
                        "created_at": task.created_at,
                    }
                ),
            }
            if self._queue_url.endswith(".fifo"):
                kwargs["MessageGroupId"] = tenant_id
            self._sqs.send_message(**kwargs)

        # 6. Baseline metric
        self._put_tenant_metric("ApiCalls", tenant_id, 0)

        # Cache
        self._tenants[tenant_id] = tenant
        self._audit_log("provision_tenant", tenant_id=tenant_id, actor="system")
        return tenant

    def deprovision_tenant(self, tenant_id: str) -> None:
        """
        Remove all resources for a tenant.  Order: data -> config -> creds -> files.
        """
        self._require_tenant(tenant_id)

        # 1. Delete all DynamoDB entities for this tenant
        self._delete_all_tenant_entities(tenant_id)

        # 2. Delete SSM config tree
        self._delete_tenant_config(tenant_id)

        # 3. Delete secret
        secret_name = f"{self._secret_prefix}/{tenant_id}/db-credentials"
        try:
            self._sm.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)
        except self._sm.exceptions.ResourceNotFoundException:
            pass  # best-effort cleanup

        # 4. Delete S3 objects under tenant prefix
        self._delete_tenant_files(tenant_id)

        # 5. Update cache
        if tenant_id in self._tenants:
            self._tenants[tenant_id].status = "deprovisioned"

        self._audit_log("deprovision_tenant", tenant_id=tenant_id, actor="system")

    def suspend_tenant(self, tenant_id: str) -> None:
        """Disable a tenant without deleting data."""
        tenant = self._require_tenant(tenant_id)
        tenant.status = "suspended"

        # Persist status change
        self._ddb.update_item(
            TableName=self._table,
            Key={
                "tenant_id": {"S": tenant_id},
                "entity_key": {"S": f"TENANT#{tenant_id}"},
            },
            UpdateExpression="SET #d = :d",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={
                ":d": {
                    "S": json.dumps(
                        {
                            "name": tenant.name,
                            "plan": tenant.plan,
                            "status": "suspended",
                            "admin_email": tenant.admin_email,
                        }
                    )
                }
            },
        )
        self._audit_log("suspend_tenant", tenant_id=tenant_id, actor="system")

    def reactivate_tenant(self, tenant_id: str) -> None:
        """Reactivate a suspended tenant."""
        tenant = self._require_tenant(tenant_id)
        tenant.status = "active"
        self._ddb.update_item(
            TableName=self._table,
            Key={
                "tenant_id": {"S": tenant_id},
                "entity_key": {"S": f"TENANT#{tenant_id}"},
            },
            UpdateExpression="SET #d = :d",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={
                ":d": {
                    "S": json.dumps(
                        {
                            "name": tenant.name,
                            "plan": tenant.plan,
                            "status": "active",
                            "admin_email": tenant.admin_email,
                        }
                    )
                }
            },
        )
        self._audit_log("reactivate_tenant", tenant_id=tenant_id, actor="system")

    # ------------------------------------------------------------------
    # Tenant admin queries
    # ------------------------------------------------------------------

    def get_tenant(self, tenant_id: str) -> Tenant:
        """Return cached tenant or fetch from DynamoDB."""
        if tenant_id in self._tenants:
            return self._tenants[tenant_id]
        resp = self._ddb.get_item(
            TableName=self._table,
            Key={
                "tenant_id": {"S": tenant_id},
                "entity_key": {"S": f"TENANT#{tenant_id}"},
            },
        )
        item = resp.get("Item")
        if item is None:
            raise TenantNotFoundError(tenant_id)
        data = json.loads(item["data"]["S"])
        tenant = Tenant(
            tenant_id=tenant_id,
            name=data.get("name", ""),
            plan=data.get("plan", "free"),
            status=data.get("status", "active"),
            created_at=item.get("created_at", {}).get("S", ""),
            admin_email=data.get("admin_email", ""),
        )
        self._tenants[tenant_id] = tenant
        return tenant

    def list_tenants(self) -> list[Tenant]:
        """Return all known tenants (from cache)."""
        return list(self._tenants.values())

    def search_tenants_by_plan(self, plan: str) -> list[Tenant]:
        """Filter cached tenants by plan tier."""
        return [t for t in self._tenants.values() if t.plan == plan]

    # ------------------------------------------------------------------
    # Data operations (DynamoDB, scoped to tenant)
    # ------------------------------------------------------------------

    def put_entity(self, entity: TenantEntity) -> None:
        """Insert or replace an entity for a tenant."""
        tenant = self._require_active_tenant(entity.tenant_id)
        self._check_quota_api_calls(tenant.tenant_id)

        self._ddb.put_item(
            TableName=self._table,
            Item={
                "tenant_id": {"S": entity.tenant_id},
                "entity_key": {"S": entity.entity_key},
                "entity_type": {"S": entity.entity_type},
                "created_at": {"S": entity.created_at},
                "updated_at": {"S": entity.updated_at},
                "data": {"S": json.dumps(entity.data)},
            },
        )
        self._record_api_call(entity.tenant_id)

    def get_entity(self, tenant_id: str, entity_key: str) -> TenantEntity | None:
        """Fetch a single entity by (tenant_id, entity_key)."""
        self._require_active_tenant(tenant_id)
        resp = self._ddb.get_item(
            TableName=self._table,
            Key={
                "tenant_id": {"S": tenant_id},
                "entity_key": {"S": entity_key},
            },
        )
        item = resp.get("Item")
        if item is None:
            return None
        self._record_api_call(tenant_id)
        return self._item_to_entity(item)

    def delete_entity(self, tenant_id: str, entity_key: str) -> None:
        """Delete a single entity."""
        self._require_active_tenant(tenant_id)
        self._ddb.delete_item(
            TableName=self._table,
            Key={
                "tenant_id": {"S": tenant_id},
                "entity_key": {"S": entity_key},
            },
        )
        self._record_api_call(tenant_id)

    def query_entities(
        self,
        tenant_id: str,
        *,
        entity_type: str | None = None,
        limit: int = 100,
    ) -> list[TenantEntity]:
        """Query all entities for a tenant, optionally filtered by type."""
        self._require_active_tenant(tenant_id)

        kwargs: dict[str, Any] = {
            "TableName": self._table,
            "KeyConditionExpression": "tenant_id = :tid",
            "ExpressionAttributeValues": {":tid": {"S": tenant_id}},
            "Limit": limit,
        }
        if entity_type:
            kwargs["FilterExpression"] = "entity_type = :et"
            kwargs["ExpressionAttributeValues"][":et"] = {"S": entity_type}

        resp = self._ddb.query(**kwargs)
        self._record_api_call(tenant_id)
        return [self._item_to_entity(i) for i in resp.get("Items", [])]

    def bulk_put_entities(self, entities: list[TenantEntity]) -> int:
        """Write up to 25 entities in a single batch (all same tenant)."""
        if not entities:
            return 0
        tenant_id = entities[0].tenant_id
        self._require_active_tenant(tenant_id)

        put_requests = []
        for e in entities[:25]:
            put_requests.append(
                {
                    "PutRequest": {
                        "Item": {
                            "tenant_id": {"S": e.tenant_id},
                            "entity_key": {"S": e.entity_key},
                            "entity_type": {"S": e.entity_type},
                            "created_at": {"S": e.created_at},
                            "updated_at": {"S": e.updated_at},
                            "data": {"S": json.dumps(e.data)},
                        }
                    }
                }
            )

        self._ddb.batch_write_item(RequestItems={self._table: put_requests})
        count = len(put_requests)
        for _ in range(count):
            self._record_api_call(tenant_id)
        return count

    def update_entity(
        self,
        tenant_id: str,
        entity_key: str,
        updates: dict[str, Any],
    ) -> TenantEntity | None:
        """Merge *updates* into the entity's data field and bump updated_at."""
        self._require_active_tenant(tenant_id)
        existing = self.get_entity(tenant_id, entity_key)
        if existing is None:
            return None

        merged = {**existing.data, **updates}
        now = datetime.now(UTC).isoformat()

        self._ddb.update_item(
            TableName=self._table,
            Key={
                "tenant_id": {"S": tenant_id},
                "entity_key": {"S": entity_key},
            },
            UpdateExpression="SET #d = :d, updated_at = :ua",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={
                ":d": {"S": json.dumps(merged)},
                ":ua": {"S": now},
            },
        )
        existing.data = merged
        existing.updated_at = now
        self._record_api_call(tenant_id)
        return existing

    # ------------------------------------------------------------------
    # Configuration (SSM)
    # ------------------------------------------------------------------

    def get_tenant_config(self, tenant_id: str) -> TenantConfig:
        """Read SSM params and return a TenantConfig."""
        self._require_tenant(tenant_id)
        path = f"{self._ssm_prefix}/{tenant_id}/"
        resp = self._ssm.get_parameters_by_path(Path=path, Recursive=True)
        params = {p["Name"].split("/")[-1]: p["Value"] for p in resp.get("Parameters", [])}

        plan_name = params.get("plan_tier", "free")
        plan_def = self._plans.get(plan_name, self._plans["free"])

        return TenantConfig(
            tenant_id=tenant_id,
            features=(
                params.get("features_enabled", "").split(",")
                if params.get("features_enabled")
                else plan_def.features[:]
            ),
            rate_limits={
                "max_api_calls_per_day": int(
                    params.get("max_api_calls_per_day", plan_def.max_api_calls_per_day)
                ),
            },
            storage_quota_mb=int(params.get("storage_quota_mb", plan_def.max_storage_mb)),
            max_users=int(params.get("max_users", plan_def.max_users)),
        )

    def set_tenant_config_param(self, tenant_id: str, key: str, value: str) -> None:
        """Write a single SSM parameter for a tenant."""
        self._require_tenant(tenant_id)
        param_name = f"{self._ssm_prefix}/{tenant_id}/{key}"
        self._ssm.put_parameter(Name=param_name, Value=value, Type="String", Overwrite=True)
        self._audit_log("set_config", tenant_id=tenant_id, key=key, value=value)

    def get_tenant_config_param(self, tenant_id: str, key: str) -> str | None:
        """Read a single SSM parameter."""
        self._require_tenant(tenant_id)
        param_name = f"{self._ssm_prefix}/{tenant_id}/{key}"
        try:
            resp = self._ssm.get_parameter(Name=param_name)
            return resp["Parameter"]["Value"]
        except self._ssm.exceptions.ParameterNotFound:
            return None

    def change_tenant_plan(self, tenant_id: str, new_plan: str) -> None:
        """
        Migrate a tenant to a new plan tier.  Updates SSM config and
        the DynamoDB metadata entity.
        """
        plan_def = self._plans.get(new_plan)
        if plan_def is None:
            raise ValueError(f"Unknown plan: {new_plan}")

        tenant = self._require_tenant(tenant_id)
        old_plan = tenant.plan
        tenant.plan = new_plan

        # Update SSM
        self._write_tenant_config(tenant_id, plan_def)

        # Update DynamoDB metadata
        self._ddb.update_item(
            TableName=self._table,
            Key={
                "tenant_id": {"S": tenant_id},
                "entity_key": {"S": f"TENANT#{tenant_id}"},
            },
            UpdateExpression="SET #d = :d",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={
                ":d": {
                    "S": json.dumps(
                        {
                            "name": tenant.name,
                            "plan": new_plan,
                            "status": tenant.status,
                            "admin_email": tenant.admin_email,
                        }
                    )
                }
            },
        )
        self._audit_log(
            "change_plan",
            tenant_id=tenant_id,
            old_plan=old_plan,
            new_plan=new_plan,
        )

    # ------------------------------------------------------------------
    # Credentials (Secrets Manager)
    # ------------------------------------------------------------------

    def get_tenant_credentials(self, tenant_id: str) -> dict[str, Any]:
        """Retrieve DB credentials for a tenant."""
        self._require_tenant(tenant_id)
        secret_name = f"{self._secret_prefix}/{tenant_id}/db-credentials"
        resp = self._sm.get_secret_value(SecretId=secret_name)
        return json.loads(resp["SecretString"])

    def rotate_tenant_credentials(self, tenant_id: str, new_password: str) -> None:
        """Update the password in a tenant's DB credential secret."""
        self._require_tenant(tenant_id)
        secret_name = f"{self._secret_prefix}/{tenant_id}/db-credentials"
        creds = self.get_tenant_credentials(tenant_id)
        creds["password"] = new_password
        self._sm.update_secret(SecretId=secret_name, SecretString=json.dumps(creds))
        self._audit_log("rotate_credentials", tenant_id=tenant_id, actor="system")

    # ------------------------------------------------------------------
    # File storage (S3)
    # ------------------------------------------------------------------

    def upload_file(self, tenant_id: str, path: str, body: bytes) -> str:
        """Upload a file under the tenant's S3 prefix.  Returns the full key."""
        self._require_active_tenant(tenant_id)
        key = f"{tenant_id}/{path}"
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body)
        self._record_api_call(tenant_id)
        return key

    def download_file(self, tenant_id: str, path: str) -> bytes:
        """Download a file from the tenant's S3 prefix."""
        self._require_active_tenant(tenant_id)
        key = f"{tenant_id}/{path}"
        resp = self._s3.get_object(Bucket=self._bucket, Key=key)
        self._record_api_call(tenant_id)
        return resp["Body"].read()

    def list_files(self, tenant_id: str, prefix: str = "") -> list[str]:
        """List file keys under a tenant's prefix."""
        self._require_tenant(tenant_id)
        full_prefix = f"{tenant_id}/{prefix}" if prefix else f"{tenant_id}/"
        resp = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=full_prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]

    def get_tenant_storage_bytes(self, tenant_id: str) -> int:
        """Sum the sizes of all objects under the tenant's prefix."""
        self._require_tenant(tenant_id)
        resp = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=f"{tenant_id}/")
        return sum(obj["Size"] for obj in resp.get("Contents", []))

    def export_tenant_data(self, tenant_id: str) -> str:
        """
        Export all DynamoDB entities for a tenant as a JSON file in S3.
        Returns the S3 key.
        """
        self._require_tenant(tenant_id)
        entities = self._query_all_tenant_entities(tenant_id)
        export_data = []
        for item in entities:
            export_data.append(
                {
                    "entity_key": item["entity_key"]["S"],
                    "entity_type": item.get("entity_type", {}).get("S", ""),
                    "data": item.get("data", {}).get("S", "{}"),
                    "created_at": item.get("created_at", {}).get("S", ""),
                }
            )
        key = f"{tenant_id}/exports/data-export-{int(time.time())}.json"
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=json.dumps(export_data, indent=2).encode(),
        )
        self._audit_log("export_data", tenant_id=tenant_id)
        return key

    # ------------------------------------------------------------------
    # Onboarding queue (SQS)
    # ------------------------------------------------------------------

    def queue_onboarding_task(self, tenant_id: str, task_type: str) -> None:
        """Enqueue a single onboarding task."""
        task = OnboardingTask(tenant_id=tenant_id, task_type=task_type)
        self._sqs.send_message(
            QueueUrl=self._queue_url,
            MessageBody=json.dumps(
                {
                    "tenant_id": task.tenant_id,
                    "task_type": task.task_type,
                    "status": task.status,
                    "created_at": task.created_at,
                }
            ),
        )

    def process_onboarding_tasks(self, max_tasks: int = 10) -> list[OnboardingTask]:
        """
        Receive and acknowledge onboarding tasks from the queue.
        Returns the list of processed tasks.
        """
        processed: list[OnboardingTask] = []
        for _ in range(5):
            resp = self._sqs.receive_message(
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=min(max_tasks - len(processed), 10),
                WaitTimeSeconds=1,
            )
            for msg in resp.get("Messages", []):
                body = json.loads(msg["Body"])
                task = OnboardingTask(
                    tenant_id=body["tenant_id"],
                    task_type=body["task_type"],
                    status="completed",
                    created_at=body.get("created_at", ""),
                )
                self._sqs.delete_message(
                    QueueUrl=self._queue_url,
                    ReceiptHandle=msg["ReceiptHandle"],
                )
                processed.append(task)
            if len(processed) >= max_tasks:
                break
        return processed

    # ------------------------------------------------------------------
    # Usage metrics & quota enforcement (CloudWatch)
    # ------------------------------------------------------------------

    def record_usage(
        self,
        tenant_id: str,
        metric_name: str,
        value: float,
        unit: str = "Count",
    ) -> None:
        """Publish a usage metric for a tenant."""
        self._put_tenant_metric(metric_name, tenant_id, value, unit)

    def get_usage(self, tenant_id: str, metric_name: str) -> float:
        """Get the sum of a metric for a tenant over the current period."""
        resp = self._cw.get_metric_statistics(
            Namespace=self._ns,
            MetricName=metric_name,
            Dimensions=[{"Name": "TenantId", "Value": tenant_id}],
            StartTime="2020-01-01T00:00:00Z",
            EndTime="2030-01-01T00:00:00Z",
            Period=86400 * 365,
            Statistics=["Sum"],
        )
        return sum(dp["Sum"] for dp in resp.get("Datapoints", []))

    def check_quota(self, tenant_id: str, metric_name: str, additional: float = 0) -> bool:
        """
        Return True if the tenant is within quota for the given metric.
        Raises QuotaExceededError if over.
        """
        config = self.get_tenant_config(tenant_id)
        current = self.get_usage(tenant_id, metric_name)

        limit_map = {
            "ApiCalls": config.rate_limits.get("max_api_calls_per_day", 999_999_999),
            "StorageBytes": config.storage_quota_mb * 1024 * 1024,
        }
        limit = limit_map.get(metric_name, 999_999_999)
        if current + additional > limit:
            raise QuotaExceededError(
                f"Tenant {tenant_id} exceeds {metric_name} quota: {current + additional} > {limit}"
            )
        return True

    def get_cross_tenant_aggregate(
        self,
        metric_name: str,
        tenant_ids: list[str],
    ) -> dict[str, float]:
        """Admin operation: aggregate a metric across multiple tenants."""
        result: dict[str, float] = {}
        for tid in tenant_ids:
            result[tid] = self.get_usage(tid, metric_name)
        return result

    def put_platform_metric(self, metric_name: str, value: float, unit: str = "Count") -> None:
        """Publish a platform-wide metric (no tenant dimension)."""
        self._cw.put_metric_data(
            Namespace=self._ns,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                }
            ],
        )

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------

    def get_audit_log(self) -> list[dict[str, Any]]:
        """Return the in-memory audit log."""
        return list(self._audit)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_tenant(self, tenant_id: str) -> Tenant:
        """Get tenant or raise TenantNotFoundError."""
        try:
            return self.get_tenant(tenant_id)
        except TenantNotFoundError:
            raise

    def _require_active_tenant(self, tenant_id: str) -> Tenant:
        """Get tenant and verify it is active."""
        tenant = self._require_tenant(tenant_id)
        if tenant.status == "suspended":
            raise TenantSuspendedError(f"Tenant {tenant_id} is suspended")
        if tenant.status == "deprovisioned":
            raise TenantNotFoundError(f"Tenant {tenant_id} is deprovisioned")
        return tenant

    def _write_tenant_config(self, tenant_id: str, plan_def: PlanDefinition) -> None:
        """Write SSM parameters for a tenant's plan."""
        params = {
            "plan_tier": plan_def.name,
            "max_users": str(plan_def.max_users),
            "features_enabled": ",".join(plan_def.features),
            "storage_quota_mb": str(plan_def.max_storage_mb),
            "max_api_calls_per_day": str(plan_def.max_api_calls_per_day),
        }
        for key, value in params.items():
            self._ssm.put_parameter(
                Name=f"{self._ssm_prefix}/{tenant_id}/{key}",
                Value=value,
                Type="String",
                Overwrite=True,
            )

    def _delete_tenant_config(self, tenant_id: str) -> None:
        """Delete all SSM parameters for a tenant."""
        path = f"{self._ssm_prefix}/{tenant_id}/"
        resp = self._ssm.get_parameters_by_path(Path=path, Recursive=True)
        for param in resp.get("Parameters", []):
            self._ssm.delete_parameter(Name=param["Name"])

    def _create_tenant_credentials(self, tenant_id: str) -> None:
        """Create a Secrets Manager secret for tenant DB credentials."""
        secret_name = f"{self._secret_prefix}/{tenant_id}/db-credentials"
        creds = {
            "host": f"{tenant_id}-db.internal.example.com",
            "port": 5432,
            "db_name": f"saas_{tenant_id.replace('-', '_')}",
            "username": f"{tenant_id}_app",
            "password": f"secret-{tenant_id}-initial",
        }
        self._sm.create_secret(Name=secret_name, SecretString=json.dumps(creds))

    def _delete_tenant_files(self, tenant_id: str) -> None:
        """Delete all S3 objects under tenant prefix."""
        resp = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=f"{tenant_id}/")
        for obj in resp.get("Contents", []):
            self._s3.delete_object(Bucket=self._bucket, Key=obj["Key"])

    def _delete_all_tenant_entities(self, tenant_id: str) -> None:
        """Delete every DynamoDB item for a tenant."""
        items = self._query_all_tenant_entities(tenant_id)
        for item in items:
            self._ddb.delete_item(
                TableName=self._table,
                Key={
                    "tenant_id": item["tenant_id"],
                    "entity_key": item["entity_key"],
                },
            )

    def _query_all_tenant_entities(self, tenant_id: str) -> list[dict]:
        """Return raw DynamoDB items for a tenant (paginated)."""
        items: list[dict] = []
        kwargs: dict[str, Any] = {
            "TableName": self._table,
            "KeyConditionExpression": "tenant_id = :tid",
            "ExpressionAttributeValues": {":tid": {"S": tenant_id}},
        }
        while True:
            resp = self._ddb.query(**kwargs)
            items.extend(resp.get("Items", []))
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        return items

    def _item_to_entity(self, item: dict) -> TenantEntity:
        """Convert a raw DynamoDB item to a TenantEntity."""
        data_str = item.get("data", {}).get("S", "{}")
        return TenantEntity(
            tenant_id=item["tenant_id"]["S"],
            entity_key=item["entity_key"]["S"],
            entity_type=item.get("entity_type", {}).get("S", ""),
            data=json.loads(data_str),
            created_at=item.get("created_at", {}).get("S", ""),
            updated_at=item.get("updated_at", {}).get("S", ""),
        )

    def _put_tenant_metric(
        self, metric_name: str, tenant_id: str, value: float, unit: str = "Count"
    ) -> None:
        self._cw.put_metric_data(
            Namespace=self._ns,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": [{"Name": "TenantId", "Value": tenant_id}],
                    "Value": value,
                    "Unit": unit,
                }
            ],
        )

    def _record_api_call(self, tenant_id: str) -> None:
        """Increment the ApiCalls metric for a tenant."""
        self._put_tenant_metric("ApiCalls", tenant_id, 1)

    def _check_quota_api_calls(self, tenant_id: str) -> None:
        """Raise QuotaExceededError if the tenant has hit its API call limit."""
        self.check_quota(tenant_id, "ApiCalls", additional=1)

    def _audit_log(self, action: str, **kwargs: Any) -> None:
        entry = {
            "action": action,
            "timestamp": datetime.now(UTC).isoformat(),
            **kwargs,
        }
        self._audit.append(entry)
