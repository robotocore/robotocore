//! EventBridge rule scheduler — fires rules that carry a `ScheduleExpression`.
//!
//! Port of the Python-era `EventBridgeRuleScheduler` thread: every five
//! seconds it walks every (account, region) store's rules, parses the
//! interval out of `rate(...)`/`cron(...)` expressions, and fires due rules
//! into their targets with a synthetic "Scheduled Event".

use crate::handler::{global_state, EventsHandler};
use crate::models::Rule;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Check interval in seconds (era `CHECK_INTERVAL`).
const CHECK_INTERVAL_SECS: u64 = 5;

/// `rate(N minutes|hours|days)` → seconds; `rate(0 …)` disables.
pub fn parse_rate_seconds(expression: &str) -> Option<u64> {
    let inner = expression.trim().strip_prefix("rate(")?.strip_suffix(')')?;
    let mut parts = inner.split_whitespace();
    let value: u64 = parts.next()?.parse().ok()?;
    let unit = parts.next()?.trim_end_matches('s');
    if value == 0 {
        return None;
    }
    let multiplier = match unit {
        "minute" => 60,
        "hour" => 3600,
        "day" => 86400,
        _ => 60,
    };
    Some(value * multiplier)
}

/// `cron(...)` → approximate interval in seconds (era port): `0/N` and
/// `*/N` minute-steps, a fixed minute with hourly hours, and a 5-minute
/// default for complex patterns.
pub fn parse_cron_minutes(expression: &str) -> Option<u64> {
    let inner = expression
        .trim()
        .strip_prefix("cron(")?
        .strip_suffix(')')?
        .trim();
    let fields: Vec<&str> = inner.split_whitespace().collect();
    if fields.len() < 5 {
        return Some(300);
    }
    let minute_field = fields[0];
    if let Some((_, step)) = minute_field.split_once('/') {
        if let Ok(step) = step.parse::<u64>() {
            return Some((step * 60).max(60));
        }
    }
    if let Some(step) = minute_field.strip_prefix("*/") {
        if let Ok(step) = step.parse::<u64>() {
            return Some((step * 60).max(60));
        }
    }
    if minute_field.chars().all(|c| c.is_ascii_digit()) && fields[1] == "*" {
        return Some(3600);
    }
    Some(300)
}

/// Interval seconds of a `ScheduleExpression` (rate first, then cron).
pub fn interval_seconds(expression: &str) -> Option<u64> {
    parse_rate_seconds(expression).or_else(|| parse_cron_minutes(expression))
}

/// Fires every scheduled rule on its configured cadence.
#[derive(Default)]
pub struct RuleScheduler {
    /// Last fire time keyed by (account, region, bus, rule name).
    last_fired: HashMap<(u64, String, String, String), Instant>,
}

impl RuleScheduler {
    /// One pass over all stores; returns how many rules fired.
    pub fn tick(&mut self) -> usize {
        let mut fired = 0;
        let scopes: Vec<(u64, String)> = crate::handler::GLOBAL_STATE
            .read()
            .keys()
            .cloned()
            .collect();
        for (account, region) in scopes {
            let state = global_state(account, &region);
            for rule in state.rules.read().values().cloned().collect::<Vec<_>>() {
                if *rule.state.read() != "ENABLED" {
                    continue;
                }
                let Some(expr) = rule.schedule_expression.clone() else {
                    continue;
                };
                if rule.targets.read().is_empty() {
                    continue;
                }
                let Some(interval) = interval_seconds(&expr) else {
                    continue;
                };
                let key = (
                    account,
                    region.clone(),
                    rule.bus_name.clone(),
                    rule.name.clone(),
                );
                let due = self
                    .last_fired
                    .get(&key)
                    .map(|t| t.elapsed().as_secs() >= interval)
                    .unwrap_or(true);
                if !due {
                    continue;
                }
                self.last_fired.insert(key, Instant::now());
                fire_rule(&rule, account, &region);
                fired += 1;
            }
        }
        fired
    }
}

/// Fire one scheduled rule by dispatching a synthetic Scheduled Event (era
/// `_fire_rule`).
fn fire_rule(rule: &Arc<Rule>, account: u64, region: &str) {
    let event = json!({
        "version": "0",
        "id": uuid::Uuid::new_v4().to_string(),
        "source": "aws.events",
        "account": account.to_string(),
        "time": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "region": region,
        "resources": [rule.arn],
        "detail-type": "Scheduled Event",
        "detail": {},
    });
    EventsHandler::dispatch_to_targets(rule, &event, account, region, 0);
}

/// Start the scheduler thread: every five seconds a guarded `tick` fires due
/// rules; a panic in one pass must not kill the thread.
pub fn start() {
    std::thread::Builder::new()
        .name("eventbridge-rule-scheduler".into())
        .spawn(|| {
            let mut scheduler = RuleScheduler::default();
            loop {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    scheduler.tick();
                }));
                std::thread::sleep(Duration::from_secs(CHECK_INTERVAL_SECS));
            }
        })
        .ok();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::EventsHandler;
    use crate::protocol::AwsRequest;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "events".into(),
            operation: operation.into(),
            account: 123456789012,
            region: "us-east-1".into(),
            params,
            body: bytes::Bytes::new(),
        }
    }

    // -- expression parsing (era scheduler.py ports) ---------------------------

    #[test]
    fn parse_rate_seconds_matches_the_era_parser() {
        assert_eq!(parse_rate_seconds("rate(5 minutes)"), Some(300));
        assert_eq!(parse_rate_seconds("rate(1 hour)"), Some(3600));
        assert_eq!(parse_rate_seconds("rate(1 day)"), Some(86400));
        assert_eq!(parse_rate_seconds("rate(3 minutes)"), Some(180));
        assert_eq!(parse_rate_seconds("rate(0 minutes)"), None, "disabled");
        assert_eq!(parse_rate_seconds("cron(0/5 * * * ? *)"), None);
    }

    #[test]
    fn parse_cron_minutes_matches_the_era_parser() {
        assert_eq!(parse_cron_minutes("cron(0/5 * * * ? *)"), Some(300));
        assert_eq!(parse_cron_minutes("cron(*/2 * * * ? *)"), Some(120));
        assert_eq!(parse_cron_minutes("cron(0 * * * ? *)"), Some(3600));
        assert_eq!(parse_cron_minutes("cron(15 10 * * ? *)"), Some(300));
        assert_eq!(parse_cron_minutes("rate(5 minutes)"), None);
    }

    #[test]
    fn interval_seconds_prefers_rate_over_cron() {
        assert_eq!(interval_seconds("cron(0/5 * * * ? *)"), Some(300));
        assert_eq!(interval_seconds("rate(2 hours)"), Some(7200));
        assert_eq!(interval_seconds("* * * * *"), None);
    }

    // -- firing a due scheduled rule -------------------------------------------

    fn probe_log() -> &'static parking_lot::Mutex<Vec<String>> {
        static LOG: once_cell::sync::Lazy<parking_lot::Mutex<Vec<String>>> =
            once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(Vec::new()));
        &LOG
    }

    #[test]
    fn tick_fires_due_scheduled_rules_with_the_scheduled_event() {
        let arn = "arn:aws:ecs:us-east-1:123456789012:cluster/fp5-sched-target";
        // Recording hook: the re-registration is harmless (in-process tests
        // and the gateway never register this kind differently).
        crate::register_hook(
            "ecs",
            Arc::new(|arn: &str, payload: &str, _a: u64, _r: &str| {
                probe_log().lock().push(format!("{arn}|{payload}"));
                Ok(())
            }),
        );
        let handler = EventsHandler::new();
        handler.handle(make_req(
            "PutRule",
            json!({
                "Name": "fp5-rate-rule",
                "ScheduleExpression": "rate(1 minute)",
            }),
        ));
        handler.handle(make_req(
            "PutTargets",
            json!({
                "Rule": "fp5-rate-rule",
                "Targets": [{"Id": "t1", "Arn": arn}],
            }),
        ));

        let mut scheduler = RuleScheduler::default();
        let fired = scheduler.tick();
        assert!(fired >= 1, "first pass fires the due schedule-only rule");
        let log = probe_log().lock();
        let last = log
            .last()
            .expect("target hook delivered the schedule")
            .clone();
        drop(log);
        let (hit_arn, payload) = last.split_once('|').unwrap();
        assert_eq!(hit_arn, arn);
        let event: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(event["source"], "aws.events");
        assert_eq!(event["detail-type"], "Scheduled Event");
        assert_eq!(event["region"], "us-east-1");
        assert_eq!(
            event["resources"][0],
            "arn:aws:events:us-east-1:123456789012:rule/fp5-rate-rule"
        );

        // The interval gate: an immediate second pass stays silent.
        assert_eq!(
            scheduler.tick(),
            0,
            "within the rate interval nothing fires"
        );
        handler.handle(make_req("DeleteRule", json!({"Name": "fp5-rate-rule"})));
    }
}
