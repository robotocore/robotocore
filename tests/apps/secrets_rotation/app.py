"""
SecretsVault -- A multi-environment secrets management platform.

Provides a high-level SDK for managing secrets across dev/staging/prod
namespaces with rotation scheduling, audit logging, schema validation,
namespace isolation, and resource policy simulation.

Architecture:
    SecretsVault
      |-- AWS Secrets Manager  (secret storage, versioning, rotation)
      |-- AWS DynamoDB         (audit log, rotation history, policies)

Only depends on boto3 and stdlib. No robotocore/moto imports.
"""

from __future__ import annotations

import json
import secrets
import string
import time
import uuid
from typing import Any

from .models import (
    BUILTIN_TEMPLATES,
    AccessLogEntry,
    RotationRecord,
    Secret,
    SecretPolicy,
    SecretTemplate,
)


class ValidationError(Exception):
    """Raised when a secret value fails schema validation."""


class PolicyDeniedError(Exception):
    """Raised when a principal is denied access by resource policy."""


class SecretNotFoundError(Exception):
    """Raised when a secret does not exist."""


class SecretsVault:
    """
    Multi-environment secrets management platform.

    Wraps AWS Secrets Manager for secret storage/versioning and DynamoDB
    for audit logging and rotation tracking. Supports multiple namespaces
    (dev, staging, prod), schema validation via templates, rotation
    scheduling, and simulated resource policies.
    """

    def __init__(
        self,
        secretsmanager_client: Any,
        dynamodb_client: Any,
        audit_table_name: str = "secrets-audit-log",
        rotation_table_name: str = "secrets-rotation-history",
        policy_table_name: str = "secrets-policies",
    ) -> None:
        self._sm = secretsmanager_client
        self._ddb = dynamodb_client
        self._audit_table = audit_table_name
        self._rotation_table = rotation_table_name
        self._policy_table = policy_table_name

        # In-memory template registry (augmented with builtins)
        self._templates: dict[str, SecretTemplate] = dict(BUILTIN_TEMPLATES)

        # In-memory policy cache (also persisted to DDB for durability)
        self._policies: dict[str, SecretPolicy] = {}

    # ------------------------------------------------------------------
    # Bootstrap / teardown
    # ------------------------------------------------------------------

    def create_tables(self) -> None:
        """Create DynamoDB tables for audit, rotation history, and policies."""
        tables = [
            {
                "TableName": self._audit_table,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                ],
            },
            {
                "TableName": self._rotation_table,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                ],
            },
            {
                "TableName": self._policy_table,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                ],
            },
        ]
        for tbl in tables:
            try:
                self._ddb.create_table(
                    **tbl,
                    BillingMode="PAY_PER_REQUEST",
                )
            except self._ddb.exceptions.ResourceInUseException:
                pass  # table already exists

    def delete_tables(self) -> None:
        """Remove DynamoDB tables (cleanup)."""
        for name in [self._audit_table, self._rotation_table, self._policy_table]:
            try:
                self._ddb.delete_table(TableName=name)
            except Exception:
                pass  # best-effort cleanup

    # ------------------------------------------------------------------
    # Secret CRUD
    # ------------------------------------------------------------------

    def create_secret(
        self,
        name: str,
        namespace: str,
        secret_type: str,
        value: dict,
        tags: dict[str, str] | None = None,
        rotation_days: int = 90,
        ttl_seconds: int | None = None,
        description: str | None = None,
    ) -> Secret:
        """
        Create a new secret in the specified namespace.

        The secret value is validated against the registered template for
        ``secret_type`` before being stored. Tags are applied both as AWS
        resource tags and as metadata.
        """
        # Validate against template if one exists
        self._validate_value(secret_type, value)

        full_name = f"{namespace}/{name}"
        aws_tags = [
            {"Key": "Namespace", "Value": namespace},
            {"Key": "SecretType", "Value": secret_type},
        ]
        if tags:
            aws_tags.extend({"Key": k, "Value": v} for k, v in tags.items())

        kwargs: dict[str, Any] = {
            "Name": full_name,
            "SecretString": json.dumps(value),
            "Tags": aws_tags,
        }
        if description:
            kwargs["Description"] = description

        resp = self._sm.create_secret(**kwargs)
        version_id = resp.get("VersionId")

        now = time.time()
        secret = Secret(
            name=name,
            namespace=namespace,
            type=secret_type,
            value=value,
            version_id=version_id,
            tags=tags or {},
            created_at=now,
            last_rotated=now,
            rotation_days=rotation_days,
            ttl_seconds=ttl_seconds,
        )
        return secret

    def get_secret(
        self,
        name: str,
        namespace: str,
        version_id: str | None = None,
        version_stage: str | None = None,
        accessor: str = "anonymous",
    ) -> Secret:
        """
        Retrieve a secret value, logging the access to DynamoDB.

        Supports fetching specific versions by version ID or version stage.
        Checks resource policy before returning.
        """
        full_name = f"{namespace}/{name}"

        # Check resource policy
        self._check_policy(full_name, accessor)

        kwargs: dict[str, Any] = {"SecretId": full_name}
        if version_id:
            kwargs["VersionId"] = version_id
        if version_stage:
            kwargs["VersionStage"] = version_stage

        resp = self._sm.get_secret_value(**kwargs)
        value = json.loads(resp["SecretString"])
        vid = resp.get("VersionId")

        # Log the access
        self._log_access(
            AccessLogEntry(
                secret_name=full_name,
                accessor=accessor,
                version_accessed=vid,
            )
        )

        desc = self._sm.describe_secret(SecretId=full_name)
        tags_raw = desc.get("Tags", [])
        tags = {}
        secret_type = "unknown"
        for t in tags_raw:
            if t["Key"] == "SecretType":
                secret_type = t["Value"]
            elif t["Key"] != "Namespace":
                tags[t["Key"]] = t["Value"]

        return Secret(
            name=name,
            namespace=namespace,
            type=secret_type,
            value=value,
            version_id=vid,
            tags=tags,
        )

    def update_secret(
        self,
        name: str,
        namespace: str,
        new_value: dict,
        secret_type: str | None = None,
    ) -> str:
        """
        Update a secret's value. Returns the new version ID.

        If ``secret_type`` is provided, validates against the template.
        """
        full_name = f"{namespace}/{name}"
        if secret_type:
            self._validate_value(secret_type, new_value)

        resp = self._sm.update_secret(
            SecretId=full_name,
            SecretString=json.dumps(new_value),
        )
        return resp.get("VersionId", "")

    def delete_secret(
        self,
        name: str,
        namespace: str,
        force: bool = False,
        recovery_window_days: int = 7,
    ) -> dict:
        """
        Delete a secret. With ``force=True``, deletes immediately.
        Otherwise schedules deletion with a recovery window.
        """
        full_name = f"{namespace}/{name}"
        kwargs: dict[str, Any] = {"SecretId": full_name}
        if force:
            kwargs["ForceDeleteWithoutRecovery"] = True
        else:
            kwargs["RecoveryWindowInDays"] = recovery_window_days
        resp = self._sm.delete_secret(**kwargs)
        return {
            "deleted": True,
            "deletion_date": str(resp.get("DeletionDate", "")),
            "name": full_name,
        }

    def restore_secret(self, name: str, namespace: str) -> None:
        """Restore a secret that was scheduled for deletion."""
        full_name = f"{namespace}/{name}"
        self._sm.restore_secret(SecretId=full_name)

    def list_secrets(self, namespace: str) -> list[dict]:
        """
        List all secrets in a namespace.

        Uses the Secrets Manager list/describe APIs with tag filters.
        Returns a list of dicts with name, type, and tag info.
        """
        results = []
        paginator_kwargs: dict[str, Any] = {
            "Filters": [
                {"Key": "tag-key", "Values": ["Namespace"]},
                {"Key": "tag-value", "Values": [namespace]},
            ]
        }
        resp = self._sm.list_secrets(**paginator_kwargs)
        for s in resp.get("SecretList", []):
            tags_raw = s.get("Tags", [])
            tags = {}
            secret_type = "unknown"
            for t in tags_raw:
                if t["Key"] == "SecretType":
                    secret_type = t["Value"]
                elif t["Key"] != "Namespace":
                    tags[t["Key"]] = t["Value"]
            results.append(
                {
                    "name": s["Name"],
                    "type": secret_type,
                    "tags": tags,
                    "description": s.get("Description", ""),
                }
            )
        return results

    # ------------------------------------------------------------------
    # Rotation
    # ------------------------------------------------------------------

    def rotate_secret(
        self,
        name: str,
        namespace: str,
        secret_type: str,
        rotated_by: str = "system",
        new_value: dict | None = None,
    ) -> RotationRecord:
        """
        Rotate a secret by generating a new version with fresh credentials.

        If ``new_value`` is not provided, generates random credentials
        appropriate for the secret type. Records the rotation in DynamoDB.
        """
        full_name = f"{namespace}/{name}"

        # Get current version
        current = self._sm.get_secret_value(SecretId=full_name)
        old_version = current.get("VersionId", "unknown")
        old_value = json.loads(current["SecretString"])

        # Generate new value if not provided
        if new_value is None:
            new_value = self._generate_rotated_value(secret_type, old_value)

        # Validate the new value
        self._validate_value(secret_type, new_value)

        # Store new version
        resp = self._sm.update_secret(
            SecretId=full_name,
            SecretString=json.dumps(new_value),
        )
        new_version = resp.get("VersionId", str(uuid.uuid4()))

        # Record rotation
        now = time.time()
        record = RotationRecord(
            secret_name=full_name,
            old_version=old_version,
            new_version=new_version,
            rotated_at=now,
            rotated_by=rotated_by,
        )
        self._store_rotation_record(record)

        return record

    def emergency_rotate(
        self,
        name: str,
        namespace: str,
        secret_type: str,
        rotated_by: str = "emergency-system",
    ) -> RotationRecord:
        """
        Emergency rotation: generate new credentials and attempt to
        invalidate all previous versions by tagging them as deprecated.

        In real AWS you'd use put_secret_value with staging labels.
        Here we update the secret and record the emergency event.
        """
        full_name = f"{namespace}/{name}"

        # Get current state
        current = self._sm.get_secret_value(SecretId=full_name)
        old_version = current.get("VersionId", "unknown")
        old_value = json.loads(current["SecretString"])

        # Generate completely new credentials
        new_value = self._generate_rotated_value(secret_type, old_value)

        # Store as new version via put_secret_value (creates new version)
        client_token = str(uuid.uuid4())
        self._sm.put_secret_value(
            SecretId=full_name,
            SecretString=json.dumps(new_value),
            ClientRequestToken=client_token,
            VersionStages=["AWSCURRENT"],
        )

        now = time.time()
        record = RotationRecord(
            secret_name=full_name,
            old_version=old_version,
            new_version=client_token,
            rotated_at=now,
            rotated_by=rotated_by,
        )
        self._store_rotation_record(record)

        # Tag the secret to indicate emergency rotation
        try:
            self._sm.tag_resource(
                SecretId=full_name,
                Tags=[
                    {"Key": "EmergencyRotation", "Value": str(now)},
                    {"Key": "EmergencyRotatedBy", "Value": rotated_by},
                ],
            )
        except Exception:
            pass  # best-effort tagging

        return record

    def bulk_rotate(
        self,
        namespace: str,
        rotated_by: str = "bulk-system",
    ) -> list[RotationRecord]:
        """
        Rotate ALL secrets in a namespace. Returns a list of rotation records.
        """
        secrets_list = self.list_secrets(namespace)
        records = []
        for s in secrets_list:
            full_name = s["name"]
            # Extract the local name (strip namespace prefix)
            local_name = full_name
            if full_name.startswith(f"{namespace}/"):
                local_name = full_name[len(namespace) + 1 :]
            try:
                record = self.rotate_secret(
                    name=local_name,
                    namespace=namespace,
                    secret_type=s["type"],
                    rotated_by=rotated_by,
                )
                records.append(record)
            except Exception:
                pass  # skip secrets that fail rotation
        return records

    def get_rotation_history(self, name: str, namespace: str) -> list[RotationRecord]:
        """Query DynamoDB for all rotation records for a secret."""
        full_name = f"{namespace}/{name}"
        resp = self._ddb.query(
            TableName=self._rotation_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": f"SECRET#{full_name}"}},
            ScanIndexForward=False,
        )
        records = []
        for item in resp.get("Items", []):
            records.append(
                RotationRecord(
                    secret_name=full_name,
                    old_version=item.get("old_version", {}).get("S", ""),
                    new_version=item.get("new_version", {}).get("S", ""),
                    rotated_at=float(item.get("rotated_at", {}).get("N", "0")),
                    rotated_by=item.get("rotated_by", {}).get("S", "unknown"),
                )
            )
        return records

    def get_rotation_schedule(self, name: str, namespace: str, rotation_days: int = 90) -> dict:
        """
        Return rotation schedule info: last rotated timestamp and
        next rotation due timestamp.
        """
        history = self.get_rotation_history(name, namespace)
        if not history:
            return {
                "last_rotated": None,
                "next_due": None,
                "overdue": False,
            }
        latest = max(history, key=lambda r: r.rotated_at)
        next_due = latest.rotated_at + (rotation_days * 86400)
        return {
            "last_rotated": latest.rotated_at,
            "next_due": next_due,
            "overdue": time.time() > next_due,
        }

    # ------------------------------------------------------------------
    # Secret sharing (cross-environment copy)
    # ------------------------------------------------------------------

    def copy_secret(
        self,
        name: str,
        source_namespace: str,
        target_namespace: str,
        accessor: str = "system",
    ) -> Secret:
        """
        Copy a secret from one namespace to another.

        Reads the secret from source, creates it in target with the same
        value, type, and tags. Logs the access in the source namespace.
        """
        source = self.get_secret(name, source_namespace, accessor=accessor)
        return self.create_secret(
            name=name,
            namespace=target_namespace,
            secret_type=source.type,
            value=source.value,
            tags=source.tags,
        )

    # ------------------------------------------------------------------
    # Audit logging
    # ------------------------------------------------------------------

    def get_audit_log(
        self,
        secret_name: str | None = None,
        namespace: str | None = None,
        accessor: str | None = None,
    ) -> list[AccessLogEntry]:
        """
        Query the audit log. Filter by secret name or accessor.

        If ``accessor`` is specified, scans for entries by that principal.
        If ``secret_name`` and ``namespace`` are specified, queries by
        the secret's partition key.
        """
        if secret_name and namespace:
            full_name = f"{namespace}/{secret_name}"
            resp = self._ddb.query(
                TableName=self._audit_table,
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": f"SECRET#{full_name}"}},
                ScanIndexForward=False,
            )
        elif accessor:
            resp = self._ddb.scan(
                TableName=self._audit_table,
                FilterExpression="accessor = :acc",
                ExpressionAttributeValues={":acc": {"S": accessor}},
            )
        else:
            resp = self._ddb.scan(TableName=self._audit_table)

        entries = []
        for item in resp.get("Items", []):
            entries.append(
                AccessLogEntry(
                    secret_name=item.get("secret_name", {}).get("S", ""),
                    accessor=item.get("accessor", {}).get("S", ""),
                    timestamp=float(item.get("timestamp", {}).get("N", "0")),
                    version_accessed=item.get("version_accessed", {}).get("S"),
                )
            )
        return entries

    # ------------------------------------------------------------------
    # Templates & validation
    # ------------------------------------------------------------------

    def register_template(self, template: SecretTemplate) -> None:
        """Register a custom secret template for validation."""
        self._templates[template.type_name] = template

    def get_template(self, type_name: str) -> SecretTemplate | None:
        """Look up a registered template by type name."""
        return self._templates.get(type_name)

    def validate_secret_value(self, secret_type: str, value: dict) -> list[str]:
        """
        Validate a secret value against the template for its type.
        Returns a list of error messages (empty = valid).
        """
        template = self._templates.get(secret_type)
        if template is None:
            return []  # no template = no validation
        return template.validate(value)

    # ------------------------------------------------------------------
    # Resource policies
    # ------------------------------------------------------------------

    def set_policy(
        self,
        name: str,
        namespace: str,
        allowed_principals: list[str] | None = None,
        denied_principals: list[str] | None = None,
    ) -> SecretPolicy:
        """
        Set a resource policy on a secret, controlling which principals
        can access it.
        """
        full_name = f"{namespace}/{name}"
        policy = SecretPolicy(
            secret_name=full_name,
            allowed_principals=allowed_principals or [],
            denied_principals=denied_principals or [],
        )
        self._policies[full_name] = policy

        # Persist to DynamoDB
        self._ddb.put_item(
            TableName=self._policy_table,
            Item={
                "pk": {"S": f"POLICY#{full_name}"},
                "allowed": {"SS": allowed_principals or ["__none__"]},
                "denied": {"SS": denied_principals or ["__none__"]},
            },
        )
        return policy

    def get_policy(self, name: str, namespace: str) -> SecretPolicy | None:
        """Retrieve the resource policy for a secret."""
        full_name = f"{namespace}/{name}"
        if full_name in self._policies:
            return self._policies[full_name]

        # Try loading from DDB
        resp = self._ddb.get_item(
            TableName=self._policy_table,
            Key={"pk": {"S": f"POLICY#{full_name}"}},
        )
        item = resp.get("Item")
        if not item:
            return None

        allowed = list(item.get("allowed", {}).get("SS", []))
        denied = list(item.get("denied", {}).get("SS", []))
        allowed = [a for a in allowed if a != "__none__"]
        denied = [d for d in denied if d != "__none__"]

        policy = SecretPolicy(
            secret_name=full_name,
            allowed_principals=allowed,
            denied_principals=denied,
        )
        self._policies[full_name] = policy
        return policy

    # ------------------------------------------------------------------
    # Tag-based queries
    # ------------------------------------------------------------------

    def list_secrets_by_tag(self, tag_key: str, tag_value: str) -> list[dict]:
        """List secrets that have a specific tag key/value pair."""
        resp = self._sm.list_secrets(
            Filters=[
                {"Key": "tag-key", "Values": [tag_key]},
                {"Key": "tag-value", "Values": [tag_value]},
            ]
        )
        results = []
        for s in resp.get("SecretList", []):
            results.append(
                {
                    "name": s["Name"],
                    "tags": {t["Key"]: t["Value"] for t in s.get("Tags", [])},
                }
            )
        return results

    # ------------------------------------------------------------------
    # Expiry tracking
    # ------------------------------------------------------------------

    def check_expiry(
        self,
        name: str,
        namespace: str,
        ttl_seconds: int,
        created_at: float,
    ) -> dict:
        """
        Check whether a secret is expired or approaching expiry.

        Returns a dict with ``expired``, ``remaining_seconds``, and
        ``approaching`` (True if <10% of TTL remaining).
        """
        elapsed = time.time() - created_at
        remaining = max(0.0, ttl_seconds - elapsed)
        threshold = ttl_seconds * 0.10
        return {
            "expired": remaining <= 0,
            "remaining_seconds": remaining,
            "approaching": 0 < remaining <= threshold,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_value(self, secret_type: str, value: dict) -> None:
        """Raise ValidationError if value doesn't match template."""
        errors = self.validate_secret_value(secret_type, value)
        if errors:
            raise ValidationError("; ".join(errors))

    def _check_policy(self, full_name: str, accessor: str) -> None:
        """Raise PolicyDeniedError if accessor is denied by policy."""
        policy = self._policies.get(full_name)
        if policy is None:
            return  # no policy = open access
        if not policy.is_allowed(accessor):
            raise PolicyDeniedError(f"Principal '{accessor}' is denied access to '{full_name}'")

    def _log_access(self, entry: AccessLogEntry) -> None:
        """Write an access log entry to DynamoDB."""
        self._ddb.put_item(
            TableName=self._audit_table,
            Item={
                "pk": {"S": f"SECRET#{entry.secret_name}"},
                "sk": {"S": f"ACCESS#{entry.timestamp}#{uuid.uuid4().hex[:8]}"},
                "secret_name": {"S": entry.secret_name},
                "accessor": {"S": entry.accessor},
                "timestamp": {"N": str(entry.timestamp)},
                "version_accessed": {"S": entry.version_accessed or ""},
            },
        )

    def _store_rotation_record(self, record: RotationRecord) -> None:
        """Write a rotation record to DynamoDB."""
        self._ddb.put_item(
            TableName=self._rotation_table,
            Item={
                "pk": {"S": f"SECRET#{record.secret_name}"},
                "sk": {"S": f"ROTATION#{record.rotated_at}#{uuid.uuid4().hex[:8]}"},
                "old_version": {"S": record.old_version},
                "new_version": {"S": record.new_version},
                "rotated_at": {"N": str(record.rotated_at)},
                "rotated_by": {"S": record.rotated_by},
            },
        )

    @staticmethod
    def _generate_rotated_value(secret_type: str, old_value: dict) -> dict:
        """
        Generate a new secret value for rotation based on the secret type.

        For db_credentials: new random password, same host/port/username.
        For api_key: new random key, same service.
        For certificate: new placeholder cert/key bodies.
        For unknown types: add a _rotated_at timestamp to the old value.
        """
        if secret_type == "db_credentials":
            new_password = _generate_password(24)
            return {
                **old_value,
                "password": new_password,
            }
        elif secret_type == "api_key":
            new_key = f"ak-{secrets.token_hex(16)}"
            return {
                **old_value,
                "key": new_key,
            }
        elif secret_type == "certificate":
            return {
                **old_value,
                "cert_body": (
                    f"-----BEGIN CERTIFICATE-----\n"
                    f"{secrets.token_hex(64)}\n-----END CERTIFICATE-----"
                ),
                "private_key": (
                    f"-----BEGIN PRIVATE KEY-----\n"
                    f"{secrets.token_hex(64)}\n-----END PRIVATE KEY-----"
                ),
            }
        else:
            return {
                **old_value,
                "_rotated_at": time.time(),
            }


def _generate_password(length: int = 24) -> str:
    """Generate a cryptographically random password."""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))
