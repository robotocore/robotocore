"""EC2 capacity profile management for deterministic instance availability.

This module provides a configurable capacity model for EC2 instance types
across availability zones, enabling deterministic testing of:
- InsufficientInstanceCapacity errors
- Spot instance availability and pricing
- Instance launch workflows with capacity constraints
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CapacityProfile:
    """Capacity configuration for a specific (instance_type, az) pair.

    Attributes:
        instance_type: EC2 instance type (e.g., "g5.xlarge")
        availability_zone: AZ name (e.g., "us-east-1a")
        total_capacity: Maximum instances that can be launched
        available_capacity: Current available capacity (auto-managed)
        spot_available: Whether spot instances are available
        spot_price: Spot price (if available)
        enabled: Whether this offering exists (for unsupported errors)
    """

    instance_type: str
    availability_zone: str
    total_capacity: int = 10
    available_capacity: int = field(default=10)
    spot_available: bool = True
    spot_price: float = 0.05
    enabled: bool = True

    def __post_init__(self):
        if self.available_capacity > self.total_capacity:
            self.available_capacity = self.total_capacity

    def to_dict(self) -> dict[str, Any]:
        return {
            "instance_type": self.instance_type,
            "availability_zone": self.availability_zone,
            "total_capacity": self.total_capacity,
            "available_capacity": self.available_capacity,
            "spot_available": self.spot_available,
            "spot_price": self.spot_price,
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CapacityProfile:
        return cls(
            instance_type=data["instance_type"],
            availability_zone=data["availability_zone"],
            total_capacity=data.get("total_capacity", 10),
            available_capacity=data.get("available_capacity", data.get("total_capacity", 10)),
            spot_available=data.get("spot_available", True),
            spot_price=data.get("spot_price", 0.05),
            enabled=data.get("enabled", True),
        )


@dataclass
class SpotRequestState:
    """State tracking for a spot instance request.

    Models the lifecycle: pending-evaluation -> fulfilled/capacity-not-available
    """

    request_id: str
    instance_type: str
    availability_zone: str
    state: str = "open"  # open, active, cancelled, closed
    status: str = "pending-evaluation"
    status_message: str = "Your Spot request has been submitted for review."
    created_at: float = field(default_factory=time.time)
    instance_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "instance_type": self.instance_type,
            "availability_zone": self.availability_zone,
            "state": self.state,
            "status": self.status,
            "status_message": self.status_message,
            "created_at": self.created_at,
            "instance_id": self.instance_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SpotRequestState:
        return cls(
            request_id=data["request_id"],
            instance_type=data["instance_type"],
            availability_zone=data["availability_zone"],
            state=data.get("state", "open"),
            status=data.get("status", "pending-evaluation"),
            status_message=data.get("status_message", ""),
            created_at=data.get("created_at", time.time()),
            instance_id=data.get("instance_id"),
        )


class CapacityStore:
    """Thread-safe store for EC2 capacity profiles.

    Organized by (account_id, region) for multi-account/region isolation.
    """

    def __init__(self):
        # {(account_id, region): {("instance_type", "az"): CapacityProfile}}
        self._profiles: dict[tuple[str, str], dict[tuple[str, str], CapacityProfile]] = {}
        # {(account_id, region): {request_id: SpotRequestState}}
        self._spot_requests: dict[tuple[str, str], dict[str, SpotRequestState]] = {}
        self._lock = threading.RLock()
        self._chaos_override: dict[str, Any] | None = None

    def _get_key(self, instance_type: str, az: str) -> tuple[str, str]:
        return (instance_type, az)

    def _ensure_profile(
        self,
        account_id: str,
        region: str,
        instance_type: str,
        az: str,
    ) -> CapacityProfile:
        """Get or create a default capacity profile."""
        account_key = (account_id, region)
        profile_key = self._get_key(instance_type, az)

        with self._lock:
            if account_key not in self._profiles:
                self._profiles[account_key] = {}

            if profile_key not in self._profiles[account_key]:
                # Create default profile with unlimited capacity
                self._profiles[account_key][profile_key] = CapacityProfile(
                    instance_type=instance_type,
                    availability_zone=az,
                    total_capacity=1000,  # Default: effectively unlimited
                    available_capacity=1000,
                    spot_available=True,
                    spot_price=0.05,
                    enabled=True,
                )

            return self._profiles[account_key][profile_key]

    def get_profile(
        self,
        account_id: str,
        region: str,
        instance_type: str,
        az: str,
    ) -> CapacityProfile | None:
        """Get capacity profile for (instance_type, az), or None if not configured."""
        account_key = (account_id, region)
        profile_key = self._get_key(instance_type, az)

        with self._lock:
            if account_key not in self._profiles:
                return None
            return self._profiles[account_key].get(profile_key)

    def set_profile(
        self,
        account_id: str,
        region: str,
        profile: CapacityProfile,
    ) -> None:
        """Set capacity profile for (instance_type, az)."""
        account_key = (account_id, region)
        profile_key = self._get_key(profile.instance_type, profile.availability_zone)

        with self._lock:
            if account_key not in self._profiles:
                self._profiles[account_key] = {}
            self._profiles[account_key][profile_key] = profile
            logger.debug(
                "Set capacity profile for %s/%s: total=%d, available=%d",
                profile.instance_type,
                profile.availability_zone,
                profile.total_capacity,
                profile.available_capacity,
            )

    def delete_profile(
        self,
        account_id: str,
        region: str,
        instance_type: str,
        az: str,
    ) -> bool:
        """Delete capacity profile for (instance_type, az). Returns True if deleted."""
        account_key = (account_id, region)
        profile_key = self._get_key(instance_type, az)

        with self._lock:
            if account_key not in self._profiles:
                return False
            if profile_key in self._profiles[account_key]:
                del self._profiles[account_key][profile_key]
                return True
            return False

    def list_profiles(
        self,
        account_id: str,
        region: str,
    ) -> list[CapacityProfile]:
        """List all capacity profiles for an account/region."""
        account_key = (account_id, region)

        with self._lock:
            if account_key not in self._profiles:
                return []
            return list(self._profiles[account_key].values())

    def check_capacity(
        self,
        account_id: str,
        region: str,
        instance_type: str,
        az: str,
        count: int = 1,
    ) -> tuple[bool, str]:
        """Check if capacity is available for launch.

        Returns:
            (success, error_code) tuple. error_code is None on success.
        """
        # Check chaos override first. Overrides may carry an
        # instance_type/availability_zone scope: parallel compat shards share
        # one server, so an unscooped override would inject failures into
        # unrelated workers' RunInstances calls.
        if self._chaos_override_applies(instance_type, az):
            error_code = self._chaos_override.get("error_code")
            if error_code == "InsufficientInstanceCapacity":
                return False, "InsufficientInstanceCapacity"
            if error_code == "Unsupported":
                return False, "Unsupported"

        profile = self._ensure_profile(account_id, region, instance_type, az)

        if not profile.enabled:
            return False, "Unsupported"

        if profile.available_capacity < count:
            return False, "InsufficientInstanceCapacity"

        return True, ""

    def consume_capacity(
        self,
        account_id: str,
        region: str,
        instance_type: str,
        az: str,
        count: int = 1,
    ) -> bool:
        """Consume capacity for instance launch.

        Returns True if capacity was consumed, False if insufficient.
        """
        profile = self._ensure_profile(account_id, region, instance_type, az)

        with self._lock:
            if profile.available_capacity < count:
                return False
            profile.available_capacity -= count
            logger.debug(
                "Consumed %d capacity for %s/%s: %d remaining",
                count,
                instance_type,
                az,
                profile.available_capacity,
            )
            return True

    def release_capacity(
        self,
        account_id: str,
        region: str,
        instance_type: str,
        az: str,
        count: int = 1,
    ) -> None:
        """Release capacity back to the pool (e.g., on instance termination)."""
        profile = self._ensure_profile(account_id, region, instance_type, az)

        with self._lock:
            profile.available_capacity = min(
                profile.available_capacity + count,
                profile.total_capacity,
            )
            logger.debug(
                "Released %d capacity for %s/%s: %d available",
                count,
                instance_type,
                az,
                profile.available_capacity,
            )

    def check_spot_capacity(
        self,
        account_id: str,
        region: str,
        instance_type: str,
        az: str,
    ) -> tuple[bool, float | None]:
        """Check if spot capacity is available.

        Returns:
            (available, spot_price) tuple. spot_price is None if not available.
        """
        if self._chaos_override_applies(instance_type, az):
            error_code = self._chaos_override.get("error_code")
            if error_code == "InsufficientInstanceCapacity":
                return False, None
            if error_code == "Unsupported":
                return False, None

        profile = self._ensure_profile(account_id, region, instance_type, az)

        if not profile.enabled or not profile.spot_available:
            return False, None

        if profile.available_capacity < 1:
            return False, None

        return True, profile.spot_price

    def set_chaos_override(self, override: dict[str, Any] | None) -> None:
        """Set chaos override for capacity checks (for testing).

        The override may carry `instance_type`/`availability_zone` to scope it:
        under a parallel compat shard sharing one server, an unscooped
        InsufficientInstanceCapacity override injects failures into unrelated
        workers' launches. Unscooped overrides apply globally.
        """
        self._chaos_override = override

    def _chaos_override_applies(self, instance_type: str, az: str) -> bool:
        """Whether the stored override targets this instance_type/AZ pair."""
        if not self._chaos_override:
            return False
        scoped_type = self._chaos_override.get("instance_type")
        scoped_az = self._chaos_override.get("availability_zone")
        if scoped_type and scoped_type != instance_type:
            return False
        if scoped_az and scoped_az != az:
            return False
        return True

    def get_chaos_override(self) -> dict[str, Any] | None:
        """Get current chaos override."""
        return self._chaos_override

    def add_spot_request(
        self,
        account_id: str,
        region: str,
        request: SpotRequestState,
    ) -> None:
        """Add a spot request to tracking."""
        account_key = (account_id, region)

        with self._lock:
            if account_key not in self._spot_requests:
                self._spot_requests[account_key] = {}
            self._spot_requests[account_key][request.request_id] = request

    def get_spot_request(
        self,
        account_id: str,
        region: str,
        request_id: str,
    ) -> SpotRequestState | None:
        """Get spot request by ID."""
        account_key = (account_id, region)

        with self._lock:
            if account_key not in self._spot_requests:
                return None
            return self._spot_requests[account_key].get(request_id)

    def list_spot_requests(
        self,
        account_id: str,
        region: str,
    ) -> list[SpotRequestState]:
        """List all spot requests for an account/region."""
        account_key = (account_id, region)

        with self._lock:
            if account_key not in self._spot_requests:
                return []
            return list(self._spot_requests[account_key].values())

    def update_spot_request(
        self,
        account_id: str,
        region: str,
        request_id: str,
        **updates: Any,
    ) -> bool:
        """Update spot request fields."""
        account_key = (account_id, region)

        with self._lock:
            if account_key not in self._spot_requests:
                return False
            request = self._spot_requests[account_key].get(request_id)
            if request is None:
                return False
            for key, value in updates.items():
                if hasattr(request, key):
                    setattr(request, key, value)
            return True

    def clear_spot_request(
        self,
        account_id: str,
        region: str,
        request_id: str,
    ) -> bool:
        """Remove a spot request from tracking."""
        account_key = (account_id, region)

        with self._lock:
            if account_key not in self._spot_requests:
                return False
            if request_id in self._spot_requests[account_key]:
                del self._spot_requests[account_key][request_id]
                return True
            return False

    def reset(self, account_id: str | None = None, region: str | None = None) -> None:
        """Reset capacity store. If account_id/region specified, only reset that scope."""
        with self._lock:
            if account_id and region:
                account_key = (account_id, region)
                if account_key in self._profiles:
                    del self._profiles[account_key]
                if account_key in self._spot_requests:
                    del self._spot_requests[account_key]
            else:
                self._profiles.clear()
                self._spot_requests.clear()
            self._chaos_override = None

    def export_state(self) -> dict[str, Any]:
        """Export state for snapshot save."""
        with self._lock:
            return {
                "profiles": {
                    f"{acc}:{reg}": {k: v.to_dict() for k, v in profiles.items()}
                    for (acc, reg), profiles in self._profiles.items()
                },
                "spot_requests": {
                    f"{acc}:{reg}": {k: v.to_dict() for k, v in requests.items()}
                    for (acc, reg), requests in self._spot_requests.items()
                },
            }

    def load_state(self, state: dict[str, Any]) -> None:
        """Load state from snapshot."""
        with self._lock:
            self._profiles.clear()
            self._spot_requests.clear()

            profiles_data = state.get("profiles", {})
            for key_str, profiles in profiles_data.items():
                acc, reg = key_str.split(":", 1)
                account_key = (acc, reg)
                self._profiles[account_key] = {
                    tuple(k.split(":", 1)) if ":" in k[0] else k: CapacityProfile.from_dict(v)
                    for k, v in profiles.items()
                }

            requests_data = state.get("spot_requests", {})
            for key_str, requests in requests_data.items():
                acc, reg = key_str.split(":", 1)
                account_key = (acc, reg)
                self._spot_requests[account_key] = {
                    k: SpotRequestState.from_dict(v) for k, v in requests.items()
                }


# Singleton capacity store
_capacity_store: CapacityStore | None = None
_store_lock = threading.Lock()


def get_capacity_store() -> CapacityStore:
    """Get the singleton capacity store."""
    global _capacity_store
    with _store_lock:
        if _capacity_store is None:
            _capacity_store = CapacityStore()
        return _capacity_store


def reset_capacity_store() -> None:
    """Reset the capacity store (for testing)."""
    global _capacity_store
    with _store_lock:
        _capacity_store = None
