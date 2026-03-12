"""
Data models for the multi-tenant SaaS platform.

Pure Python dataclasses -- no AWS SDK imports.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# Plan definitions
# ---------------------------------------------------------------------------


@dataclass
class PlanDefinition:
    """Defines limits and features for a subscription plan tier."""

    name: str
    max_storage_mb: int
    max_users: int
    max_api_calls_per_day: int
    features: list[str] = field(default_factory=list)


# Canonical plan catalogue used by tests and the platform itself.
PLAN_CATALOGUE: dict[str, PlanDefinition] = {
    "free": PlanDefinition(
        name="free",
        max_storage_mb=100,
        max_users=3,
        max_api_calls_per_day=1_000,
        features=["billing"],
    ),
    "starter": PlanDefinition(
        name="starter",
        max_storage_mb=1_024,
        max_users=10,
        max_api_calls_per_day=10_000,
        features=["billing", "reports"],
    ),
    "pro": PlanDefinition(
        name="pro",
        max_storage_mb=10_240,
        max_users=50,
        max_api_calls_per_day=100_000,
        features=["billing", "reports", "api_access", "sso"],
    ),
    "enterprise": PlanDefinition(
        name="enterprise",
        max_storage_mb=102_400,
        max_users=500,
        max_api_calls_per_day=1_000_000,
        features=["billing", "reports", "api_access", "sso", "audit", "custom_branding"],
    ),
}


# ---------------------------------------------------------------------------
# Tenant
# ---------------------------------------------------------------------------


@dataclass
class Tenant:
    """Root aggregate for a single tenant."""

    tenant_id: str = field(default_factory=_new_id)
    name: str = ""
    plan: str = "free"
    status: str = "pending"  # pending | active | suspended | deprovisioned
    created_at: str = field(default_factory=_now_iso)
    admin_email: str = ""
    settings: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Tenant configuration
# ---------------------------------------------------------------------------


@dataclass
class TenantConfig:
    """Runtime configuration derived from the plan + overrides."""

    tenant_id: str
    features: list[str] = field(default_factory=list)
    rate_limits: dict[str, int] = field(default_factory=dict)
    storage_quota_mb: int = 100
    max_users: int = 3


# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------


@dataclass
class TenantUsage:
    """Aggregated usage counters for a billing period."""

    tenant_id: str
    api_calls: int = 0
    storage_bytes: int = 0
    compute_seconds: float = 0.0
    period: str = ""  # e.g. "2026-03"


# ---------------------------------------------------------------------------
# Onboarding tasks
# ---------------------------------------------------------------------------


@dataclass
class OnboardingTask:
    """A discrete step in the tenant onboarding pipeline."""

    tenant_id: str
    task_type: str  # create_db | seed_data | configure_dns | send_welcome
    status: str = "pending"  # pending | in_progress | completed | failed
    created_at: str = field(default_factory=_now_iso)


# ---------------------------------------------------------------------------
# Tenant entity (generic data record stored in DynamoDB)
# ---------------------------------------------------------------------------


@dataclass
class TenantEntity:
    """A single data record belonging to a tenant."""

    tenant_id: str
    entity_key: str
    entity_type: str  # USER | PROJECT | DOCUMENT | INVOICE ...
    data: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
