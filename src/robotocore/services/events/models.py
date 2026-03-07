"""EventBridge in-memory models: event buses, rules, targets, and archives."""

import threading
import time
from dataclasses import dataclass, field


@dataclass
class EventTarget:
    target_id: str
    arn: str
    role_arn: str = ""
    input: str | None = None
    input_path: str | None = None
    input_transformer: dict | None = None
    dead_letter_config: dict | None = None


@dataclass
class EventRule:
    name: str
    event_bus_name: str
    region: str
    account_id: str
    state: str = "ENABLED"
    description: str = ""
    event_pattern: dict | None = None
    schedule_expression: str | None = None
    targets: dict[str, EventTarget] = field(default_factory=dict)
    created: float = field(default_factory=time.time)
    dead_letter_config: dict | None = None

    @property
    def arn(self) -> str:
        return f"arn:aws:events:{self.region}:{self.account_id}:rule/{self.name}"

    def matches_event(self, event: dict) -> bool:
        """Check if an event matches this rule's event pattern."""
        if self.state != "ENABLED":
            return False
        if self.event_pattern is None:
            # Schedule-based rules don't match events
            return self.schedule_expression is not None
        return _match_pattern(self.event_pattern, event)


@dataclass
class EventArchive:
    name: str
    source_arn: str
    region: str
    account_id: str
    description: str = ""
    event_pattern: dict | None = None
    retention_days: int = 0  # 0 = indefinite
    state: str = "ENABLED"
    events: list[dict] = field(default_factory=list)
    created: float = field(default_factory=time.time)
    event_count: int = 0
    size_bytes: int = 0

    @property
    def arn(self) -> str:
        return (
            f"arn:aws:events:{self.region}:{self.account_id}"
            f":archive/{self.name}"
        )


@dataclass
class EventReplay:
    name: str
    archive_arn: str
    region: str
    account_id: str
    destination_arn: str
    start_time: float
    end_time: float
    state: str = "COMPLETED"
    events_replayed: int = 0
    created: float = field(default_factory=time.time)

    @property
    def arn(self) -> str:
        return (
            f"arn:aws:events:{self.region}:{self.account_id}"
            f":replay/{self.name}"
        )


@dataclass
class EventBus:
    name: str
    region: str
    account_id: str
    rules: dict[str, EventRule] = field(default_factory=dict)

    @property
    def arn(self) -> str:
        return f"arn:aws:events:{self.region}:{self.account_id}:event-bus/{self.name}"


class EventsStore:
    """Per-region EventBridge store."""

    def __init__(self):
        self.buses: dict[str, EventBus] = {}
        self.archives: dict[str, EventArchive] = {}
        self.replays: dict[str, EventReplay] = {}
        self.tags: dict[str, list[dict]] = {}  # ARN -> [{Key, Value}, ...]
        self.mutex = threading.RLock()
        # Create default bus
        self.buses["default"] = EventBus(
            name="default", region="us-east-1", account_id="123456789012"
        )

    def ensure_default_bus(self, region: str, account_id: str):
        with self.mutex:
            if "default" not in self.buses:
                self.buses["default"] = EventBus(
                    name="default", region=region, account_id=account_id
                )
            else:
                self.buses["default"].region = region
                self.buses["default"].account_id = account_id

    def create_event_bus(self, name: str, region: str, account_id: str) -> EventBus:
        with self.mutex:
            bus = EventBus(name=name, region=region, account_id=account_id)
            self.buses[name] = bus
            return bus

    def get_bus(self, name: str) -> EventBus | None:
        return self.buses.get(name)

    def delete_bus(self, name: str) -> bool:
        with self.mutex:
            if name == "default":
                return False  # Can't delete default bus
            return self.buses.pop(name, None) is not None

    def list_buses(self) -> list[EventBus]:
        return list(self.buses.values())

    def put_rule(
        self,
        name: str,
        bus_name: str,
        region: str,
        account_id: str,
        event_pattern: dict | None = None,
        schedule_expression: str | None = None,
        state: str = "ENABLED",
        description: str = "",
    ) -> EventRule:
        with self.mutex:
            bus = self.buses.get(bus_name)
            if not bus:
                bus = self.buses.get("default")
            rule = EventRule(
                name=name,
                event_bus_name=bus_name,
                region=region,
                account_id=account_id,
                state=state,
                description=description,
                event_pattern=event_pattern,
                schedule_expression=schedule_expression,
            )
            bus.rules[name] = rule
            return rule

    def get_rule(self, name: str, bus_name: str = "default") -> EventRule | None:
        bus = self.buses.get(bus_name)
        if bus:
            return bus.rules.get(name)
        return None

    def delete_rule(self, name: str, bus_name: str = "default") -> bool:
        with self.mutex:
            bus = self.buses.get(bus_name)
            if bus:
                return bus.rules.pop(name, None) is not None
            return False

    def list_rules(self, bus_name: str = "default", prefix: str | None = None) -> list[EventRule]:
        bus = self.buses.get(bus_name)
        if not bus:
            return []
        rules = list(bus.rules.values())
        if prefix:
            rules = [r for r in rules if r.name.startswith(prefix)]
        return rules

    def put_targets(self, rule_name: str, bus_name: str, targets: list[dict]) -> list[dict]:
        with self.mutex:
            bus = self.buses.get(bus_name)
            if not bus:
                return [
                    {"TargetId": t.get("Id", ""), "ErrorCode": "ResourceNotFoundException"}
                    for t in targets
                ]
            rule = bus.rules.get(rule_name)
            if not rule:
                return [
                    {"TargetId": t.get("Id", ""), "ErrorCode": "ResourceNotFoundException"}
                    for t in targets
                ]

            failed = []
            for t in targets:
                target = EventTarget(
                    target_id=t.get("Id", ""),
                    arn=t.get("Arn", ""),
                    role_arn=t.get("RoleArn", ""),
                    input=t.get("Input"),
                    input_path=t.get("InputPath"),
                    input_transformer=t.get("InputTransformer"),
                    dead_letter_config=t.get("DeadLetterConfig"),
                )
                rule.targets[target.target_id] = target
            return failed

    def remove_targets(self, rule_name: str, bus_name: str, target_ids: list[str]) -> list[dict]:
        with self.mutex:
            bus = self.buses.get(bus_name)
            if not bus:
                return []
            rule = bus.rules.get(rule_name)
            if not rule:
                return []
            failed = []
            for tid in target_ids:
                if tid in rule.targets:
                    del rule.targets[tid]
                else:
                    failed.append({"TargetId": tid, "ErrorCode": "ResourceNotFoundException"})
            return failed

    def list_targets(self, rule_name: str, bus_name: str = "default") -> list[EventTarget]:
        bus = self.buses.get(bus_name)
        if not bus:
            return []
        rule = bus.rules.get(rule_name)
        if not rule:
            return []
        return list(rule.targets.values())

    # -- Archive operations --

    def create_archive(
        self,
        name: str,
        source_arn: str,
        region: str,
        account_id: str,
        description: str = "",
        event_pattern: dict | None = None,
        retention_days: int = 0,
    ) -> EventArchive:
        with self.mutex:
            archive = EventArchive(
                name=name,
                source_arn=source_arn,
                region=region,
                account_id=account_id,
                description=description,
                event_pattern=event_pattern,
                retention_days=retention_days,
            )
            self.archives[name] = archive
            return archive

    def get_archive(self, name: str) -> EventArchive | None:
        return self.archives.get(name)

    def delete_archive(self, name: str) -> bool:
        with self.mutex:
            return self.archives.pop(name, None) is not None

    def list_archives(
        self, prefix: str | None = None
    ) -> list[EventArchive]:
        archives = list(self.archives.values())
        if prefix:
            archives = [
                a for a in archives if a.name.startswith(prefix)
            ]
        return archives

    def archive_event(self, event: dict, bus_name: str) -> None:
        """Store event in matching archives."""
        with self.mutex:
            bus = self.buses.get(bus_name)
            if not bus:
                return
            bus_arn = bus.arn
            for archive in self.archives.values():
                if archive.source_arn != bus_arn:
                    continue
                if archive.state != "ENABLED":
                    continue
                if archive.event_pattern:
                    if not _match_pattern(
                        archive.event_pattern, event
                    ):
                        continue
                import json

                event_json = json.dumps(event)
                archive.events.append(event)
                archive.event_count += 1
                archive.size_bytes += len(event_json.encode())

    # -- Replay operations --

    def create_replay(
        self,
        name: str,
        archive_arn: str,
        region: str,
        account_id: str,
        destination_arn: str,
        start_time: float,
        end_time: float,
    ) -> EventReplay:
        with self.mutex:
            replay = EventReplay(
                name=name,
                archive_arn=archive_arn,
                region=region,
                account_id=account_id,
                destination_arn=destination_arn,
                start_time=start_time,
                end_time=end_time,
            )
            self.replays[name] = replay
            return replay

    def get_replay(self, name: str) -> EventReplay | None:
        return self.replays.get(name)

    # -- Tag operations --

    def tag_resource(self, resource_arn: str, tags: list[dict]) -> None:
        """Add or overwrite tags on a resource."""
        with self.mutex:
            existing = {t["Key"]: t for t in self.tags.get(resource_arn, [])}
            for tag in tags:
                existing[tag["Key"]] = tag
            self.tags[resource_arn] = list(existing.values())

    def untag_resource(self, resource_arn: str, tag_keys: list[str]) -> None:
        """Remove tags by key from a resource."""
        with self.mutex:
            current = self.tags.get(resource_arn, [])
            self.tags[resource_arn] = [t for t in current if t["Key"] not in tag_keys]

    def list_tags_for_resource(self, resource_arn: str) -> list[dict]:
        """Return tags for a resource ARN."""
        return list(self.tags.get(resource_arn, []))


def _match_pattern(pattern: dict, event: dict) -> bool:
    """Match an EventBridge event pattern against an event.

    Pattern matching rules:
    - Each key in the pattern must exist in the event
    - String values in pattern arrays match exact event values
    - Nested objects are matched recursively
    - Prefix matching: {"prefix": "val"}
    - Anything-but: {"anything-but": ["val1", "val2"]}
    - Numeric matching: {"numeric": [">", 100]}
    - Exists matching: {"exists": true/false}
    """
    for key, pattern_value in pattern.items():
        event_value = event.get(key)

        if isinstance(pattern_value, dict):
            # Nested object matching
            if not isinstance(event_value, dict):
                return False
            if not _match_pattern(pattern_value, event_value):
                return False
        elif isinstance(pattern_value, list):
            if not _match_value_list(pattern_value, event_value):
                return False
        else:
            if event_value != pattern_value:
                return False

    return True


def _match_value_list(pattern_values: list, event_value) -> bool:
    """Match a list of possible values against an event value."""
    if event_value is None:
        # Check for exists: false
        for pv in pattern_values:
            if isinstance(pv, dict) and pv.get("exists") is False:
                return True
        return False

    for pv in pattern_values:
        if isinstance(pv, dict):
            # Complex matcher
            if "prefix" in pv:
                if isinstance(event_value, str) and event_value.startswith(pv["prefix"]):
                    return True
            elif "suffix" in pv:
                if isinstance(event_value, str) and event_value.endswith(pv["suffix"]):
                    return True
            elif "anything-but" in pv:
                excluded = pv["anything-but"]
                if isinstance(excluded, list):
                    if event_value not in excluded:
                        return True
                else:
                    if event_value != excluded:
                        return True
            elif "numeric" in pv:
                ops = pv["numeric"]
                if _match_numeric(ops, event_value):
                    return True
            elif "exists" in pv:
                if pv["exists"] is True and event_value is not None:
                    return True
                if pv["exists"] is False and event_value is None:
                    return True
        else:
            # Simple value match
            if event_value == pv:
                return True
            # If event value is a list, check if any element matches
            if isinstance(event_value, list) and pv in event_value:
                return True

    return False


def _match_numeric(ops: list, value) -> bool:
    """Match numeric comparison operators."""
    if not isinstance(value, (int, float)):
        return False
    i = 0
    while i < len(ops):
        op = ops[i]
        if i + 1 >= len(ops):
            return False
        num = ops[i + 1]
        if op == ">" and not (value > num):
            return False
        elif op == ">=" and not (value >= num):
            return False
        elif op == "<" and not (value < num):
            return False
        elif op == "<=" and not (value <= num):
            return False
        elif op == "=" and not (value == num):
            return False
        i += 2
    return True
