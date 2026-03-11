"""Dependency-aware startup orchestrator with health checks.

Manages the boot sequence for all robotocore components, ensuring they start
in the correct order based on declared dependencies, with health check polling
and graceful degradation for optional components.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum

logger = logging.getLogger(__name__)


class ComponentState(StrEnum):
    """Lifecycle state of a service component."""

    PENDING = "pending"
    STARTING = "starting"
    HEALTHY = "healthy"
    FAILED = "failed"
    STOPPED = "stopped"
    SKIPPED = "skipped"


@dataclass
class ServiceComponent:
    """A bootable component with dependencies and health checks."""

    name: str
    depends_on: list[str] = field(default_factory=list)
    start: Callable | None = None  # async function to start the component
    health_check: Callable[[], bool] | None = None  # returns True when ready
    stop: Callable | None = None  # async function to stop gracefully
    required: bool = True  # If False, failure doesn't abort boot
    timeout: float = 10.0  # Max seconds to wait for health check


@dataclass
class ComponentStatus:
    """Runtime status of a registered component."""

    name: str
    state: ComponentState = ComponentState.PENDING
    started_at: float | None = None
    ready_at: float | None = None
    stopped_at: float | None = None
    error: str | None = None
    required: bool = True

    @property
    def boot_duration_ms(self) -> float | None:
        if self.started_at is not None and self.ready_at is not None:
            return round((self.ready_at - self.started_at) * 1000, 1)
        return None

    def to_dict(self) -> dict:
        d: dict = {
            "name": self.name,
            "state": self.state.value,
            "required": self.required,
        }
        if self.boot_duration_ms is not None:
            d["boot_duration_ms"] = self.boot_duration_ms
        if self.error:
            d["error"] = self.error
        return d


@dataclass
class BootResult:
    """Result of a boot sequence."""

    success: bool
    components: dict[str, ComponentStatus] = field(default_factory=dict)
    total_duration_ms: float = 0.0
    failed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "total_duration_ms": self.total_duration_ms,
            "components": {n: s.to_dict() for n, s in self.components.items()},
            "failed": self.failed,
            "skipped": self.skipped,
        }


class CircularDependencyError(Exception):
    """Raised when component dependencies form a cycle."""

    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__(f"Circular dependency detected: {' -> '.join(cycle)}")


class BootOrchestrator:
    """Dependency-aware startup with health checks.

    Components are started in topological order of their dependencies.
    Health checks are polled with exponential backoff. Optional components
    that fail are skipped without aborting the boot.
    """

    def __init__(self) -> None:
        self._components: dict[str, ServiceComponent] = {}
        self._status: dict[str, ComponentStatus] = {}
        self._boot_result: BootResult | None = None
        self._booted = False

    def register(self, component: ServiceComponent) -> None:
        """Register a component for boot orchestration.

        Raises ValueError if a component with the same name already exists.
        """
        if component.name in self._components:
            raise ValueError(f"Component already registered: {component.name}")
        self._components[component.name] = component
        self._status[component.name] = ComponentStatus(
            name=component.name,
            required=component.required,
        )

    def _resolve_order(self) -> list[str]:
        """Topological sort of components by dependencies.

        Returns component names in the order they should be started.
        Raises CircularDependencyError if a cycle is detected.
        Raises ValueError if a dependency references an unknown component.
        """
        # Validate all dependencies reference known components
        for comp in self._components.values():
            for dep in comp.depends_on:
                if dep not in self._components:
                    raise ValueError(
                        f"Component '{comp.name}' depends on unknown component '{dep}'"
                    )

        # Kahn's algorithm for topological sort
        in_degree: dict[str, int] = {name: 0 for name in self._components}
        dependents: dict[str, list[str]] = defaultdict(list)

        for comp in self._components.values():
            for dep in comp.depends_on:
                in_degree[comp.name] += 1
                dependents[dep].append(comp.name)

        queue = [name for name, deg in in_degree.items() if deg == 0]
        queue.sort()  # Deterministic ordering for same-level components
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for dependent in sorted(dependents[node]):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
            queue.sort()

        if len(result) != len(self._components):
            # Find the cycle for a useful error message
            remaining = set(self._components) - set(result)
            cycle = self._find_cycle(remaining)
            raise CircularDependencyError(cycle)

        return result

    def _find_cycle(self, nodes: set[str]) -> list[str]:
        """Find a cycle among the given nodes for error reporting."""
        visited: set[str] = set()
        path: list[str] = []

        def dfs(node: str) -> list[str] | None:
            if node in visited:
                idx = path.index(node)
                return path[idx:] + [node]
            visited.add(node)
            path.append(node)
            for dep in self._components[node].depends_on:
                if dep in nodes:
                    result = dfs(dep)
                    if result:
                        return result
            path.pop()
            return None

        for node in sorted(nodes):
            visited.clear()
            path.clear()
            cycle = dfs(node)
            if cycle:
                return cycle
        return list(nodes)  # Fallback

    async def _wait_for_health(self, component: ServiceComponent, status: ComponentStatus) -> bool:
        """Poll health check with exponential backoff until healthy or timeout.

        Returns True if the component became healthy, False on timeout.
        """
        if component.health_check is None:
            # No health check means instant readiness
            status.state = ComponentState.HEALTHY
            status.ready_at = time.monotonic()
            return True

        deadline = time.monotonic() + component.timeout
        delay = 0.01  # Start at 10ms
        max_delay = 1.0

        while time.monotonic() < deadline:
            try:
                result = component.health_check()
                if asyncio.iscoroutine(result):
                    result = await result
                if result:
                    status.state = ComponentState.HEALTHY
                    status.ready_at = time.monotonic()
                    return True
            except Exception:
                pass  # Health check failed, retry

            await asyncio.sleep(min(delay, max(0, deadline - time.monotonic())))
            delay = min(delay * 2, max_delay)

        return False

    async def boot(self) -> BootResult:
        """Start all components in dependency order.

        Returns a BootResult summarizing the outcome. If a required component
        fails, subsequent components that depend on it are skipped.
        """
        if self._booted:
            raise RuntimeError("BootOrchestrator.boot() has already been called")

        boot_start = time.monotonic()
        failed_components: set[str] = set()
        result = BootResult(success=True, components=self._status)

        try:
            order = self._resolve_order()
        except (CircularDependencyError, ValueError) as e:
            result.success = False
            result.failed = [str(e)]
            result.total_duration_ms = round((time.monotonic() - boot_start) * 1000, 1)
            self._boot_result = result
            return result

        for name in order:
            component = self._components[name]
            status = self._status[name]

            # Check if any dependency failed
            unmet_deps = [d for d in component.depends_on if d in failed_components]
            if unmet_deps:
                status.state = ComponentState.SKIPPED
                status.error = f"Skipped due to failed dependencies: {', '.join(unmet_deps)}"
                result.skipped.append(name)
                failed_components.add(name)
                if component.required:
                    result.success = False
                logger.warning("Skipping component '%s': dependencies failed: %s", name, unmet_deps)
                continue

            # Start the component
            status.state = ComponentState.STARTING
            status.started_at = time.monotonic()

            try:
                if component.start is not None:
                    ret = component.start()
                    if asyncio.iscoroutine(ret):
                        await ret
            except Exception as e:
                status.state = ComponentState.FAILED
                status.error = str(e)
                logger.error("Component '%s' failed to start: %s", name, e)

                if component.required:
                    result.success = False
                    result.failed.append(name)
                    failed_components.add(name)
                    continue
                else:
                    result.failed.append(name)
                    failed_components.add(name)
                    continue

            # Wait for health check
            healthy = await self._wait_for_health(component, status)
            if not healthy:
                status.state = ComponentState.FAILED
                status.error = f"Health check timed out after {component.timeout}s"
                logger.error(
                    "Component '%s' health check timed out after %.1fs",
                    name,
                    component.timeout,
                )

                if component.required:
                    result.success = False
                    result.failed.append(name)
                    failed_components.add(name)
                else:
                    result.failed.append(name)
                    failed_components.add(name)

                continue

            duration = status.boot_duration_ms or 0
            logger.info("Component '%s' started (%.1fms)", name, duration)

        result.total_duration_ms = round((time.monotonic() - boot_start) * 1000, 1)
        self._boot_result = result
        self._booted = True
        return result

    async def shutdown(self) -> None:
        """Stop all started components in reverse dependency order."""
        if not self._booted:
            return

        try:
            order = self._resolve_order()
        except (CircularDependencyError, ValueError):
            # If resolution fails, stop in arbitrary order
            order = list(self._components)

        # Reverse order: dependents stop before their dependencies
        for name in reversed(order):
            component = self._components[name]
            status = self._status[name]

            if status.state not in (ComponentState.HEALTHY, ComponentState.STARTING):
                continue

            try:
                if component.stop is not None:
                    ret = component.stop()
                    if asyncio.iscoroutine(ret):
                        await ret
                status.state = ComponentState.STOPPED
                status.stopped_at = time.monotonic()
                logger.info("Component '%s' stopped", name)
            except Exception as e:
                status.state = ComponentState.FAILED
                status.error = f"Shutdown error: {e}"
                logger.error("Component '%s' failed to stop: %s", name, e)

    def get_status(self) -> dict:
        """Return component health status as a dict suitable for JSON."""
        components = {name: status.to_dict() for name, status in self._status.items()}
        boot_result = None
        if self._boot_result:
            boot_result = {
                "success": self._boot_result.success,
                "total_duration_ms": self._boot_result.total_duration_ms,
                "failed": self._boot_result.failed,
                "skipped": self._boot_result.skipped,
            }
        return {
            "booted": self._booted,
            "boot_result": boot_result,
            "components": components,
        }

    @property
    def booted(self) -> bool:
        return self._booted

    @property
    def boot_result(self) -> BootResult | None:
        return self._boot_result


# Global orchestrator instance
_orchestrator: BootOrchestrator | None = None


def get_orchestrator() -> BootOrchestrator:
    """Get or create the global BootOrchestrator instance."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = BootOrchestrator()
    return _orchestrator


def reset_orchestrator() -> None:
    """Reset the global orchestrator (for testing)."""
    global _orchestrator
    _orchestrator = None
