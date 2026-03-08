"""Fault injection rule definitions and matching engine."""

import random
import re
import threading
import time
import uuid


class FaultRule:
    """A single fault injection rule."""

    def __init__(
        self,
        *,
        rule_id: str | None = None,
        service: str | None = None,
        operation: str | None = None,
        region: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        status_code: int | None = None,
        latency_ms: int = 0,
        probability: float = 1.0,
        enabled: bool = True,
    ):
        self.rule_id = rule_id or uuid.uuid4().hex[:12]
        self.service = service  # None = match all
        self.operation = operation  # None = match all, supports regex
        self.region = region  # None = match all
        self.error_code = error_code  # e.g. "ThrottlingException"
        self.error_message = error_message or f"Injected by chaos rule {self.rule_id}"
        self.status_code = status_code or (429 if error_code == "ThrottlingException" else 500)
        self.latency_ms = latency_ms
        self.probability = max(0.0, min(1.0, probability))
        self.enabled = enabled
        self.created_at = time.time()
        self.match_count = 0
        self._op_pattern = re.compile(operation) if operation else None

    def matches(self, service: str, operation: str | None, region: str) -> bool:
        """Check if this rule matches the given request."""
        if not self.enabled:
            return False
        if self.service and self.service != service:
            return False
        if self.region and self.region != region:
            return False
        if self._op_pattern and operation:
            if not self._op_pattern.search(operation):
                return False
        elif self._op_pattern and not operation:
            return False
        if random.random() > self.probability:
            return False
        self.match_count += 1
        return True

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id,
            "service": self.service,
            "operation": self.operation,
            "region": self.region,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "status_code": self.status_code,
            "latency_ms": self.latency_ms,
            "probability": self.probability,
            "enabled": self.enabled,
            "created_at": self.created_at,
            "match_count": self.match_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FaultRule":
        rule = cls(
            rule_id=data.get("rule_id"),
            service=data.get("service"),
            operation=data.get("operation"),
            region=data.get("region"),
            error_code=data.get("error_code"),
            error_message=data.get("error_message"),
            status_code=data.get("status_code"),
            latency_ms=data.get("latency_ms", 0),
            probability=data.get("probability", 1.0),
            enabled=data.get("enabled", True),
        )
        if "created_at" in data:
            rule.created_at = data["created_at"]
        if "match_count" in data:
            rule.match_count = data["match_count"]
        return rule


class FaultRuleStore:
    """Thread-safe store for fault injection rules."""

    def __init__(self):
        self._rules: list[FaultRule] = []
        self._lock = threading.Lock()

    def add(self, rule: FaultRule) -> str:
        with self._lock:
            self._rules.append(rule)
        return rule.rule_id

    def remove(self, rule_id: str) -> bool:
        with self._lock:
            before = len(self._rules)
            self._rules = [r for r in self._rules if r.rule_id != rule_id]
            return len(self._rules) < before

    def clear(self) -> int:
        with self._lock:
            count = len(self._rules)
            self._rules.clear()
            return count

    def list_rules(self) -> list[dict]:
        with self._lock:
            return [r.to_dict() for r in self._rules]

    def find_matching(self, service: str, operation: str | None, region: str) -> FaultRule | None:
        """Find the first matching fault rule for a request."""
        with self._lock:
            for rule in self._rules:
                if rule.matches(service, operation, region):
                    return rule
        return None


# Singleton
_store = FaultRuleStore()


def get_fault_store() -> FaultRuleStore:
    return _store
