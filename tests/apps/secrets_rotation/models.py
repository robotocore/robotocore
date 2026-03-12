"""
Domain models for the secrets management platform.

Pure data classes with no AWS dependencies -- these represent the logical
entities that SecretsVault manages.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class Secret:
    """A managed secret with metadata."""

    name: str
    namespace: str
    type: str  # "db_credentials", "api_key", "certificate"
    value: dict
    version_id: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_rotated: float | None = None
    rotation_days: int = 90
    ttl_seconds: int | None = None  # optional expiry TTL

    @property
    def full_name(self) -> str:
        return f"{self.namespace}/{self.name}"

    @property
    def is_expired(self) -> bool:
        if self.ttl_seconds is None:
            return False
        return (time.time() - self.created_at) > self.ttl_seconds

    @property
    def seconds_until_expiry(self) -> float | None:
        if self.ttl_seconds is None:
            return None
        remaining = self.ttl_seconds - (time.time() - self.created_at)
        return max(0.0, remaining)

    @property
    def rotation_overdue(self) -> bool:
        if self.last_rotated is None:
            return False
        elapsed = time.time() - self.last_rotated
        return elapsed > (self.rotation_days * 86400)

    @property
    def next_rotation_due(self) -> float | None:
        if self.last_rotated is None:
            return None
        return self.last_rotated + (self.rotation_days * 86400)


@dataclass
class RotationRecord:
    """Tracks a single rotation event."""

    secret_name: str
    old_version: str
    new_version: str
    rotated_at: float = field(default_factory=time.time)
    rotated_by: str = "system"


@dataclass
class AccessLogEntry:
    """Records a single secret access event."""

    secret_name: str
    accessor: str
    timestamp: float = field(default_factory=time.time)
    version_accessed: str | None = None


@dataclass
class SecretTemplate:
    """Defines the expected schema for a secret type."""

    type_name: str
    required_fields: list[str]
    field_types: dict[str, str]  # field_name -> "str" | "int" | "bool"

    def validate(self, value: dict) -> list[str]:
        """Return list of validation errors, empty if valid."""
        errors = []
        for field_name in self.required_fields:
            if field_name not in value:
                errors.append(f"Missing required field: {field_name}")
        type_map = {"str": str, "int": int, "bool": bool, "float": float}
        for field_name, expected_type_name in self.field_types.items():
            if field_name in value:
                expected = type_map.get(expected_type_name)
                if expected and not isinstance(value[field_name], expected):
                    errors.append(
                        f"Field '{field_name}' expected {expected_type_name}, "
                        f"got {type(value[field_name]).__name__}"
                    )
        return errors


@dataclass
class SecretPolicy:
    """Simulated resource policy for a secret."""

    secret_name: str
    allowed_principals: list[str] = field(default_factory=list)
    denied_principals: list[str] = field(default_factory=list)

    def is_allowed(self, principal: str) -> bool:
        # Explicit deny wins
        if principal in self.denied_principals:
            return False
        # If allowlist is empty, allow all (no policy = open)
        if not self.allowed_principals:
            return True
        return principal in self.allowed_principals


# Built-in templates for common secret types
BUILTIN_TEMPLATES: dict[str, SecretTemplate] = {
    "db_credentials": SecretTemplate(
        type_name="db_credentials",
        required_fields=["host", "port", "username", "password"],
        field_types={
            "host": "str",
            "port": "int",
            "username": "str",
            "password": "str",
        },
    ),
    "api_key": SecretTemplate(
        type_name="api_key",
        required_fields=["key", "service"],
        field_types={
            "key": "str",
            "service": "str",
        },
    ),
    "certificate": SecretTemplate(
        type_name="certificate",
        required_fields=["cert_body", "private_key"],
        field_types={
            "cert_body": "str",
            "private_key": "str",
        },
    ),
}
