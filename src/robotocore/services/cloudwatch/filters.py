"""CloudWatch Logs metric filters and subscription filters.

Metric filter CRUD + pattern matching engine.
Subscription filter CRUD + delivery.
"""

import json
import logging
import re
import threading
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class MetricTransformation:
    metric_name: str
    metric_namespace: str
    metric_value: str
    default_value: float | None = None


@dataclass
class MetricFilter:
    filter_name: str
    log_group_name: str
    filter_pattern: str
    metric_transformations: list[MetricTransformation] = field(default_factory=list)
    creation_time: int = 0


@dataclass
class SubscriptionFilter:
    filter_name: str
    log_group_name: str
    filter_pattern: str
    destination_arn: str
    role_arn: str = ""
    distribution: str = "ByLogStream"
    creation_time: int = 0


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class FilterStore:
    """In-memory store for metric filters and subscription filters."""

    def __init__(self) -> None:
        self.metric_filters: dict[str, dict[str, MetricFilter]] = {}
        # {log_group_name: {filter_name: MetricFilter}}
        self.subscription_filters: dict[str, dict[str, SubscriptionFilter]] = {}
        # {log_group_name: {filter_name: SubscriptionFilter}}
        self._lock = threading.Lock()

    def put_metric_filter(
        self,
        log_group_name: str,
        filter_name: str,
        filter_pattern: str,
        metric_transformations: list[dict],
    ) -> MetricFilter:
        with self._lock:
            if log_group_name not in self.metric_filters:
                self.metric_filters[log_group_name] = {}

            transforms = [
                MetricTransformation(
                    metric_name=mt.get("metricName", ""),
                    metric_namespace=mt.get("metricNamespace", ""),
                    metric_value=mt.get("metricValue", "1"),
                    default_value=mt.get("defaultValue"),
                )
                for mt in metric_transformations
            ]

            mf = MetricFilter(
                filter_name=filter_name,
                log_group_name=log_group_name,
                filter_pattern=filter_pattern,
                metric_transformations=transforms,
            )
            self.metric_filters[log_group_name][filter_name] = mf
            return mf

    def delete_metric_filter(self, log_group_name: str, filter_name: str) -> bool:
        with self._lock:
            group_filters = self.metric_filters.get(log_group_name, {})
            if filter_name in group_filters:
                del group_filters[filter_name]
                return True
            return False

    def describe_metric_filters(
        self,
        log_group_name: str | None = None,
        filter_name_prefix: str | None = None,
    ) -> list[MetricFilter]:
        with self._lock:
            results: list[MetricFilter] = []
            groups = (
                {log_group_name: self.metric_filters.get(log_group_name, {})}
                if log_group_name
                else self.metric_filters
            )
            for filters in groups.values():
                for mf in filters.values():
                    if filter_name_prefix and not mf.filter_name.startswith(filter_name_prefix):
                        continue
                    results.append(mf)
            return results

    def put_subscription_filter(
        self,
        log_group_name: str,
        filter_name: str,
        filter_pattern: str,
        destination_arn: str,
        role_arn: str = "",
        distribution: str = "ByLogStream",
    ) -> SubscriptionFilter:
        with self._lock:
            if log_group_name not in self.subscription_filters:
                self.subscription_filters[log_group_name] = {}

            sf = SubscriptionFilter(
                filter_name=filter_name,
                log_group_name=log_group_name,
                filter_pattern=filter_pattern,
                destination_arn=destination_arn,
                role_arn=role_arn,
                distribution=distribution,
            )
            self.subscription_filters[log_group_name][filter_name] = sf
            return sf

    def delete_subscription_filter(self, log_group_name: str, filter_name: str) -> bool:
        with self._lock:
            group_filters = self.subscription_filters.get(log_group_name, {})
            if filter_name in group_filters:
                del group_filters[filter_name]
                return True
            return False

    def describe_subscription_filters(self, log_group_name: str) -> list[SubscriptionFilter]:
        with self._lock:
            return list(self.subscription_filters.get(log_group_name, {}).values())

    def get_subscription_filters_for_group(self, log_group_name: str) -> list[SubscriptionFilter]:
        """Get all subscription filters for a log group (for delivery)."""
        with self._lock:
            return list(self.subscription_filters.get(log_group_name, {}).values())


# Global store per region
_stores: dict[str, FilterStore] = {}
_store_lock = threading.Lock()


def get_filter_store(region: str = "us-east-1") -> FilterStore:
    with _store_lock:
        if region not in _stores:
            _stores[region] = FilterStore()
        return _stores[region]


# ---------------------------------------------------------------------------
# Filter pattern matching
# ---------------------------------------------------------------------------


def matches_filter_pattern(pattern: str, message: str) -> bool:
    """Evaluate whether a log message matches a CloudWatch filter pattern.

    Supports:
    - Empty pattern: matches everything
    - Term matching: space-separated terms are ANDed
    - Quoted strings: exact substring match
    - JSON extraction: { $.field = "value" }, { $.field > N }
    """
    if not pattern or pattern.strip() == "":
        return True

    pattern = pattern.strip()

    # JSON pattern: { $.field op value }
    json_match = re.match(
        r'\{\s*\$\.([\w.[\]0-9-]+)\s*(=|!=|>|>=|<|<=)\s*"?([^"}\s]*)"?\s*\}',
        pattern,
    )
    if json_match:
        return _match_json_pattern(json_match, message)

    # Term-based matching (AND of all terms)
    return _match_terms(pattern, message)


def _match_json_pattern(match: re.Match, message: str) -> bool:
    """Match a JSON extraction pattern against a log message."""
    field_path = match.group(1)
    operator = match.group(2)
    expected = match.group(3)

    try:
        data = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        return False

    # Navigate the field path (supports hyphens and array indices like items[0])
    parts = field_path.split(".")
    current = data
    for part in parts:
        # Check for array index: field[N]
        arr_match = re.match(r"([\w-]+)\[(\d+)\]", part)
        if arr_match:
            field_name = arr_match.group(1)
            index = int(arr_match.group(2))
            if isinstance(current, dict):
                current = current.get(field_name)
            else:
                return False
            if isinstance(current, list) and index < len(current):
                current = current[index]
            else:
                return False
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return False
        if current is None:
            return False

    actual = str(current)

    # Try numeric comparison
    try:
        actual_num = float(actual)
        expected_num = float(expected)
        if operator == "=":
            return actual_num == expected_num
        elif operator == "!=":
            return actual_num != expected_num
        elif operator == ">":
            return actual_num > expected_num
        elif operator == ">=":
            return actual_num >= expected_num
        elif operator == "<":
            return actual_num < expected_num
        elif operator == "<=":
            return actual_num <= expected_num
    except (ValueError, TypeError) as exc:
        logger.debug("_match_json_pattern: float failed (non-fatal): %s", exc)

    # String comparison
    if operator == "=":
        return actual == expected
    elif operator == "!=":
        return actual != expected
    return False


def _match_terms(pattern: str, message: str) -> bool:
    """Match space-separated terms (AND logic). Quoted strings match as substrings."""
    terms = _extract_terms(pattern)
    message_lower = message.lower()

    for term in terms:
        if not term:
            continue
        term_lower = term.lower()
        if term_lower not in message_lower:
            return False
    return True


def _extract_terms(pattern: str) -> list[str]:
    """Extract terms from a filter pattern, respecting quotes."""
    terms: list[str] = []
    current: list[str] = []
    in_quote = False
    quote_char = ""

    for ch in pattern:
        if ch in ('"', "'") and not in_quote:
            in_quote = True
            quote_char = ch
        elif ch == quote_char and in_quote:
            in_quote = False
            quote_char = ""
            terms.append("".join(current))
            current = []
        elif ch == " " and not in_quote:
            if current:
                terms.append("".join(current))
                current = []
        else:
            current.append(ch)

    if current:
        terms.append("".join(current))
    return terms


# ---------------------------------------------------------------------------
# Log event processing (called when PutLogEvents arrives)
# ---------------------------------------------------------------------------


def process_log_events(
    log_group_name: str,
    log_stream_name: str,
    events: list[dict],
    region: str,
    account_id: str,
) -> None:
    """Process log events through metric filters and subscription filters.

    Called after PutLogEvents to evaluate filters.
    """
    store = get_filter_store(region)

    # Process metric filters
    metric_filters = store.describe_metric_filters(log_group_name=log_group_name)
    for mf in metric_filters:
        for event in events:
            message = event.get("message", "")
            if matches_filter_pattern(mf.filter_pattern, message):
                _emit_metric_from_filter(mf, region, account_id)

    # Process subscription filters
    sub_filters = store.get_subscription_filters_for_group(log_group_name)
    for sf in sub_filters:
        matching_events = [
            e for e in events if matches_filter_pattern(sf.filter_pattern, e.get("message", ""))
        ]
        if matching_events:
            _deliver_to_subscription(sf, log_group_name, log_stream_name, matching_events, region)


def _emit_metric_from_filter(mf: MetricFilter, region: str, account_id: str) -> None:
    """Emit metric data from a matching metric filter."""
    try:
        from moto.backends import get_backend

        cw_backend = get_backend("cloudwatch")[account_id][region]
        for transform in mf.metric_transformations:
            try:
                value = float(transform.metric_value)
            except (ValueError, TypeError):
                value = 1.0
            cw_backend.put_metric_data(
                namespace=transform.metric_namespace,
                metric_data=[
                    {
                        "MetricName": transform.metric_name,
                        "Value": value,
                        "Unit": "Count",
                    }
                ],
            )
    except Exception:
        logger.debug("Failed to emit metric from filter %s", mf.filter_name, exc_info=True)


def _deliver_to_subscription(
    sf: SubscriptionFilter,
    log_group_name: str,
    log_stream_name: str,
    events: list[dict],
    region: str,
) -> None:
    """Deliver matching events to a subscription filter destination."""
    arn = sf.destination_arn
    logger.info(
        "Subscription filter %s delivering %d events to %s",
        sf.filter_name,
        len(events),
        arn,
    )
    # In a real implementation, this would deliver to Lambda/Kinesis/Firehose
    # For now, just log the delivery
