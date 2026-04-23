"""EventBridge Rule Scheduler -- fires scheduled rules on cron/rate expressions.

Background daemon thread that periodically checks all EventBridge rules with
ScheduleExpression and fires their targets when the schedule is due.
"""

import logging
import threading
import time
import uuid
from datetime import UTC, datetime

from robotocore.services.synthetics.scheduler import parse_cron_minutes, parse_rate_seconds

logger = logging.getLogger(__name__)

# Check interval in seconds
CHECK_INTERVAL = 5

# Singleton scheduler
_scheduler: "EventBridgeRuleScheduler | None" = None
_scheduler_lock = threading.Lock()


def get_rule_scheduler() -> "EventBridgeRuleScheduler":
    """Return the global EventBridgeRuleScheduler singleton."""
    global _scheduler
    with _scheduler_lock:
        if _scheduler is None:
            _scheduler = EventBridgeRuleScheduler()
        return _scheduler


class EventBridgeRuleScheduler:
    """Periodically fires EventBridge rules that have a ScheduleExpression."""

    def __init__(self) -> None:
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        # Track last-fired time per rule: (store_key, bus_name, rule_name) -> monotonic timestamp
        self._last_fired: dict[tuple[str, str, str], float] = {}

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(
                target=self._run_loop, daemon=True, name="eventbridge-rule-scheduler"
            )
            self._thread.start()
            logger.info("EventBridge rule scheduler started (interval=%ds)", CHECK_INTERVAL)

    def stop(self) -> None:
        with self._lock:
            self._running = False

    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def _run_loop(self) -> None:
        """Main scheduling loop."""
        while self._running:
            try:
                self._check_all_rules()
            except Exception:
                logger.exception("Error in EventBridge rule scheduler loop")
            time.sleep(CHECK_INTERVAL)

    def _check_all_rules(self) -> None:
        """Iterate all stores and check scheduled rules."""
        from robotocore.services.events.provider import _stores

        now = time.monotonic()

        for store_key, store in list(_stores.items()):
            account_id, region = store_key

            for bus in list(store.buses.values()):
                for rule in list(bus.rules.values()):
                    if rule.state != "ENABLED":
                        continue
                    if not rule.schedule_expression:
                        continue
                    if not rule.targets:
                        continue

                    # Parse interval from schedule expression
                    expr = rule.schedule_expression
                    interval = parse_rate_seconds(expr) or parse_cron_minutes(expr)
                    if interval is None:
                        continue

                    key = (store_key, bus.name, rule.name)
                    last = self._last_fired.get(key, -float("inf"))
                    if now - last >= interval:
                        self._last_fired[key] = now
                        self._fire_rule(rule, region, account_id, store)

    def _fire_rule(self, rule, region: str, account_id: str, store) -> None:
        """Fire a scheduled rule by dispatching to its targets."""
        from robotocore.services.events.provider import _dispatch_to_targets

        event = {
            "version": "0",
            "id": str(uuid.uuid4()),
            "source": "aws.events",
            "account": account_id,
            "time": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "region": region,
            "resources": [rule.arn],
            "detail-type": "Scheduled Event",
            "detail": {},
        }

        try:
            _dispatch_to_targets(rule, event, region, account_id, store)
            logger.debug(
                "Fired scheduled rule %s (%s) with %d targets",
                rule.name,
                rule.schedule_expression,
                len(rule.targets),
            )
        except Exception:
            logger.exception("Error firing scheduled rule %s", rule.name)
