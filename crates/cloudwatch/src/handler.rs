//! CloudWatch operation handler.

use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

use crate::models::{CloudwatchState, DatumStats, MetricDatum};
use crate::protocol::{AwsRequest, AwsResponse};

pub struct CloudwatchHandler {
    pub(crate) state: RwLock<HashMap<(u64, String), CloudwatchState>>,
}

/// Extract a string from a JSON scalar (query protocol places values as strings too).
fn as_str(v: Option<&Value>) -> Option<String> {
    match v {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

/// Extract an f64 from a number or numeric string.
fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// Extract an i64 from a number or numeric string. Query-protocol requests
/// encode scalar integers as strings (protocol.rs stores scalars as
/// Value::String), so alarm parameters must parse both shapes — a bare
/// `Value::as_i64` silently degraded them to the API defaults on the query
/// wire. A float-form string ("60.0") is tolerated with truncation, matching
/// the wire shapes botocore actually emits for integer members.
fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s
            .trim()
            .parse::<i64>()
            .ok()
            .or_else(|| s.trim().parse::<f64>().ok().map(|f| f as i64)),
        _ => None,
    }
}

/// Extract a bool from JSON or string ("true"/"false").
fn as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::String(s) if s.eq_ignore_ascii_case("true") => Some(true),
        Value::String(s) if s.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    }
}

/// Read a field of a Value object as a canonical string ("", when missing).
fn field_str(v: &Value, key: &str) -> String {
    as_str(v.get(key)).unwrap_or_default()
}

/// Read a field of a Value object as a canonical string with fallback.
fn field_str_or(v: &Value, key: &str, fallback: &str) -> String {
    match as_str(v.get(key)) {
        Some(s) if !s.is_empty() => s,
        _ => fallback.to_string(),
    }
}

/// Unit decision for bare numeric epochs, at the parse boundary. botocore
/// ships CloudWatch timestamps as epoch-SECONDS numbers in the JSON wire
/// body — live capture (botocore 1.43.116, `Content-Type:
/// application/x-amz-json-1.0`, `X-Amz-Target:
/// GraniteServiceVersion20100801.PutMetricData`): body
/// `{"Namespace": "W", "MetricData": [{"MetricName": "M", "Value": 1,
/// "Timestamp": 1792974600}]}`, and GetMetricStatistics carries
/// `{"StartTime": 1792974600, "EndTime": 1792974900}` — while datapoints are
/// stored as epoch-MILLISECONDS (`MetricDatum::timestamp`). A magnitude
/// under 1e11 cannot be a present-day ms epoch (1e11 ms = Mar 1973), so it
/// is seconds and is scaled x1000; >= 1e11 is already ms and passes through
/// unchanged (the twin's pre-existing ms behavior, which moto does not
/// accept — moto reads all numeric timestamps as seconds).
fn epoch_scalar_to_millis(seconds_or_millis: f64) -> Option<i64> {
    let ms = if seconds_or_millis.abs() >= 1e11 {
        seconds_or_millis
    } else {
        seconds_or_millis * 1000.0
    };
    Some(ms as i64)
}

/// Parse an ISO8601 timestamp (or a bare numeric epoch, per
/// [`epoch_scalar_to_millis`]) into epoch milliseconds.
fn parse_iso_ts(s: &str) -> Option<i64> {
    if let Ok(n) = s.trim().parse::<f64>() {
        return epoch_scalar_to_millis(n);
    }
    let t = s.replace(' ', "T");
    chrono::DateTime::parse_from_rfc3339(&t)
        .ok()
        .map(|d| d.timestamp_millis())
}

fn default_ts_str() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn ts_str(millis: i64) -> String {
    chrono::DateTime::from_timestamp_millis(millis)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339()
}

fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Dimensions from a [{"Name":..,"Value":..}] array value.
fn dims_from_value(v: Option<&Value>) -> Vec<(String, String)> {
    v.and_then(|d| d.as_array())
        .map(|arr| {
            arr.iter()
                .map(|d| (field_str(d, "Name"), field_str(d, "Value")))
                .collect()
        })
        .unwrap_or_default()
}

fn dims_json(dims: &[(String, String)]) -> Value {
    Value::Array(
        dims.iter()
            .map(|(n, v)| json!({ "Name": n, "Value": v }))
            .collect(),
    )
}

/// Strings list param: handles JSON arrays and single scalars.
fn str_list(params: &Value, key: &str) -> Vec<String> {
    match params.get(key) {
        Some(Value::Array(arr)) => arr.iter().filter_map(|v| as_str(Some(v))).collect(),
        Some(v) => as_str(Some(v)).into_iter().collect(),
        None => vec![],
    }
}

fn str_param(params: &Value, key: &str) -> String {
    as_str(params.get(key)).unwrap_or_default()
}

fn str_list_from_params(params: &Value, key: &str) -> Vec<Value> {
    match params.get(key) {
        Some(Value::Array(a)) => a.clone(),
        Some(other) => vec![other.clone()],
        None => vec![],
    }
}

/// Recursively reconstruct nested JSON structures from flattened query-protocol
/// keys like "MetricData.member.1.Dimensions.member.1.Name".
fn unflatten(params: &Value, prefix: &str) -> Option<Value> {
    let mut has = false;
    let mut names: Vec<String> = Vec::new();
    if let Some(obj) = params.as_object() {
        for k in obj.keys() {
            if k == prefix {
                return Some(obj[k].clone());
            }
            let sub = format!("{prefix}.");
            if k.starts_with(&sub) {
                has = true;
                let first = k[sub.len()..].split('.').next().unwrap_or("").to_string();
                if !first.is_empty() && !names.contains(&first) {
                    names.push(first);
                }
            }
        }
    }
    if !has {
        return None;
    }
    if let Some(pos) = names.iter().position(|n| n == "member") {
        // List under this prefix: indices look like "member.N[.subkey]".
        names.remove(pos);
        let member_pref = format!("{prefix}.member.");
        let mut indices: Vec<u32> = Vec::new();
        if let Some(obj) = params.as_object() {
            for k in obj.keys() {
                if let Some(rest) = k.strip_prefix(&member_pref) {
                    let idx_end = rest.find('.').unwrap_or(rest.len());
                    if let Ok(idx) = rest[..idx_end].parse::<u32>() {
                        if !indices.contains(&idx) {
                            indices.push(idx);
                        }
                    }
                }
            }
        }
        indices.sort_unstable();
        let mut arr = Vec::new();
        for idx in indices {
            arr.push(unflatten(params, &format!("{member_pref}{idx}")).unwrap_or(Value::Null));
        }
        // A request prefix with BOTH indexed members and sibling scalars is
        // not a well-formed wire shape — the members carry everything, so
        // the array wins (the dead object-rebuild below never returned).
        return Some(Value::Array(arr));
    }
    let mut obj = Map::new();
    for name in &names {
        if let Some(v) = unflatten(params, &format!("{prefix}.{name}")) {
            obj.insert(name.clone(), v);
        }
    }
    Some(Value::Object(obj))
}

enum RuleListAction {
    Delete,
    Disable,
    Enable,
}

/// Subset predicate shared by ListMetrics / GetMetricStatistics /
/// alarm evaluation: a candidate datum matches when its dimension set
/// CONTAINS every (Name, Value) pair of the filter.
fn dims_subset(datum: &[(String, String)], filter: &[(String, String)]) -> bool {
    filter.iter().all(|f| datum.contains(f))
}

/// De-duplication key for ListMetrics: (namespace, metric name, dimensions).
type MetricSet = Vec<(String, String, Vec<(String, String)>)>;

/// One good (data-bearing) evaluation window of a metric alarm.
#[derive(Clone, Copy)]
struct AlarmWindow {
    value: f64,
    /// Bucket end, epoch millis.
    timestamp: i64,
}

/// Comparison of one aggregated statistic against the alarm threshold.
/// Operator names are the PutMetricAlarm API's enum values.
fn is_breaching(value: f64, threshold: f64, comparison_operator: &str) -> bool {
    match comparison_operator {
        "GreaterThanThreshold" => value > threshold,
        "GreaterThanOrEqualToThreshold" => value >= threshold,
        "LessThanThreshold" => value < threshold,
        "LessThanOrEqualToThreshold" => value <= threshold,
        _ => false,
    }
}

/// Human phrasing of the comparison operator in NewStateReason strings.
fn comparison_phrase(comparison_operator: &str) -> &'static str {
    match comparison_operator {
        "GreaterThanThreshold" => "greater than",
        "GreaterThanOrEqualToThreshold" => "greater than or equal to",
        "LessThanThreshold" => "less than",
        "LessThanOrEqualToThreshold" => "less than or equal to",
        _ => "compared to",
    }
}

/// Print a metric value the way real CloudWatch prints datapoints in reason
/// strings: one decimal place, so integral values read "10.0".
fn reason_value(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{v:.1}")
    } else {
        format!("{v}")
    }
}

/// Aggregate one `Period`-wide window of the alarm's metric; `None` when the
/// window carries no datapoints or the statistic cannot be computed (a
/// "missing" period). Windows match on namespace + metric name, and a
/// candidate datapoint matches when its dimension set CONTAINS the alarm's
/// dimension set — the same `dims_subset` predicate `GetMetricStatistics`
/// filters with (the real-AWS matching semantics: a datum carries the
/// alarm's dimensions, possibly with extra splits). Without the containment
/// filter two alarms watching the same metric in one namespace evaluated
/// each other's dimension splits; a dimension-less alarm keeps plain
/// namespace+metric matching. moto cites nothing here: 5.2.4 has no alarm
/// evaluation, and its `MetricDatumBase::filter` serves
/// get_metric_statistics / get_metric_data shape filtering.
fn alarm_window_stat(
    metrics: &[MetricDatum],
    namespace: &str,
    metric_name: &str,
    filter_dims: &[(String, String)],
    stat: &str,
    start_ms: i64,
    end_ms: i64,
) -> Option<f64> {
    let mut values: Vec<f64> = Vec::new();
    let mut aggs: Vec<DatumStats> = Vec::new();
    for md in metrics {
        if md.namespace == namespace
            && md.name == metric_name
            && dims_subset(&md.dimensions, filter_dims)
            && md.timestamp > start_ms
            && md.timestamp <= end_ms
        {
            if let Some(v) = md.single_value() {
                values.push(v);
            }
            if let Some(s) = md.stats {
                aggs.push(s);
            }
        }
    }
    if values.is_empty() && aggs.is_empty() {
        return None;
    }
    agg_stat(stat, &values, &aggs)
}

/// The datapoint list part of a reason string: "10.0 (ts), 3.0 (ts)" in
/// newest-first window order, like real CloudWatch's "Threshold Crossed".
fn reason_datapoints(windows: &[AlarmWindow]) -> String {
    windows
        .iter()
        .map(|w| format!("{} ({})", reason_value(w.value), ts_str(w.timestamp)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Reason string for a state decided by the good windows, mirroring real
/// CloudWatch's "Threshold Crossed: 1 datapoint [10.0 (ts)] was greater than
/// the threshold (5.0)." — a non-breach inserts " not", and missing windows
/// under TreatMissingData get the trailing "…{n} missing datapoint(s)
/// treated as {breaching|notBreaching}." annotation.
fn threshold_reason(
    windows: &[AlarmWindow],
    breached: bool,
    phrase: &str,
    threshold_str: &str,
    missing: usize,
    treat: &str,
) -> String {
    let n = windows.len();
    let unit = if n == 1 { "" } else { "s" };
    let was = if n == 1 { "was" } else { "were" };
    let not = if breached { "" } else { " not" };
    let mut reason = format!(
        "Threshold Crossed: {n} datapoint{unit} [{}] {was}{not} {phrase} the threshold ({threshold_str}).",
        reason_datapoints(windows)
    );
    if missing > 0 && matches!(treat, "breaching" | "notBreaching") {
        let munit = if missing == 1 { "" } else { "s" };
        reason.push_str(&format!(
            " {missing} missing datapoint{munit} treated as {treat}."
        ));
    }
    reason
}

/// (state, NewStateReason) this alarm would move to given the current
/// datapoint store, evaluated over the trailing `EvaluationPeriods` windows
/// of `Period` seconds ending at `now_ms`. `None` when the alarm cannot be
/// evaluated (metric-math alarms, unknown statistics) or — with
/// `TreatMissingData=ignore` and any missing datapoint in the evaluation
/// range — must not move: real CloudWatch maintains the current alarm state
/// ("ignore – The current alarm state is maintained.", AlarmThatSendsEmail
/// docs). moto 5.2.4 has no alarm evaluation to mirror — `Alarm` in
/// moto/cloudwatch/models.py only ever stores state — so the twin mirrors
/// the documented real-CloudWatch alarm rules instead.
///
/// Window semantics for `(state, reason)` (TreatMissingData handling per the
/// docs above):
/// - `EvaluationPeriods` windows; the alarm moves to ALARM when
///   `DatapointsToAlarm` (default = EvaluationPeriods) of them breach and
///   back to OK when `EvaluationPeriods - DatapointsToAlarm + 1` of them are
///   not breaching — a *count* over the whole range, AlarmThatSendsEmail's
///   "the alarm goes to ALARM when M out of N datapoints are breaching",
///   not a newest-streak of good windows.
/// - Fewer resolving datapoints than that: missing windows count as
///   "breaching"/"notBreaching" per TreatMissingData, "missing" (the boto3
///   default) transitions to INSUFFICIENT_DATA, and "ignore" refuses to
///   move the alarm at all (the None answer above).
fn alarm_candidate(
    metrics: &[MetricDatum],
    alarm: &Value,
    now_ms: i64,
) -> Option<(String, String)> {
    let namespace = field_str(alarm, "Namespace");
    let metric_name = field_str(alarm, "MetricName");
    let stat = field_str(alarm, "Statistic");
    let operator = field_str(alarm, "ComparisonOperator");
    if namespace.is_empty()
        || metric_name.is_empty()
        || operator.is_empty()
        || stat.is_empty()
        // Metric-math alarms (a Metrics list of MetricDataQuery entries) have
        // no single Namespace/MetricName+Statistic pair to aggregate — they
        // are never evaluated by the simple evaluation.
        || !alarm["Metrics"]
            .as_array()
            .map(|m| m.is_empty())
            .unwrap_or(true)
    {
        // ExtendedStatistic-only alarms (p99 etc., Statistic absent) fall
        // into this gate and are NEVER evaluated: no state transition, no
        // notifications, forever. Known absence — the simple evaluation has
        // no percentile store; extending it would need a percentile
        // aggregator over the window stats. Documented here so the
        // `DescribeAlarms` 200 (which reads as success) is not mistaken for
        // a live alarm.
        return None;
    }
    let threshold = alarm["Threshold"].as_f64().unwrap_or(0.0);
    let period_ms = as_i64(&alarm["Period"]).unwrap_or(60).max(1) * 1000;
    let eval_periods = as_i64(&alarm["EvaluationPeriods"]).unwrap_or(1).max(1) as usize;
    let datapoints_to_alarm = as_i64(&alarm["DatapointsToAlarm"])
        .map(|m| m.max(1) as usize)
        .unwrap_or(eval_periods);
    let treat = field_str_or(alarm, "TreatMissingData", "missing");
    // The alarm's own dimension set: a candidate datapoint matches when its
    // dimension set CONTAINS these — the dims_subset predicate
    // get_metric_statistics filters with (real-AWS matching semantics; a
    // dimension-less alarm keeps plain namespace+metric matching).
    let alarm_dims = dims_from_value(alarm.get("Dimensions"));

    // Anchor at max(now, the newest datapoint for the metric): caller-supplied
    // Timestamps that run ahead of the evaluator's clock would otherwise fall
    // outside every trailing window and read as missing at the exact moment
    // they should breach. Windows are (start, end] so a datapoint stamped at
    // the anchor point lands in the newest window.
    let newest = metrics
        .iter()
        .filter(|md| {
            md.namespace == namespace
                && md.name == metric_name
                && dims_subset(&md.dimensions, &alarm_dims)
        })
        .map(|md| md.timestamp)
        .max()
        .unwrap_or(i64::MIN);
    let anchor = now_ms.max(newest);

    // Newest window first.
    let mut good: Vec<AlarmWindow> = Vec::new();
    for i in 0..eval_periods {
        let end = anchor - (i as i64) * period_ms;
        let start = end - period_ms;
        if let Some(v) = alarm_window_stat(
            metrics,
            &namespace,
            &metric_name,
            &alarm_dims,
            &stat,
            start,
            end,
        ) {
            good.push(AlarmWindow {
                value: v,
                timestamp: end,
            });
        }
    }

    let phrase = comparison_phrase(&operator);
    let threshold_str = reason_value(threshold);
    let missing = eval_periods - good.len();
    // TreatMissingData=ignore ("The current alarm state is maintained." —
    // AlarmThatSendsEmail docs): any missing datapoint anywhere in the
    // evaluation range holds the alarm in its current state. This gate also
    // covers the full-range case — when every window has data the count
    // below decides normally.
    if treat == "ignore" && missing > 0 {
        return None;
    }

    // Real CloudWatch counts, not streaks: split the evaluated windows into
    // breaching and not-breaching datapoints and let the counts decide. With
    // TreatMissingData=breaching the missing windows pad the breach side and
    // with notBreaching the clear side; one side settling implies ALARM/OK
    // exactly (both sides are mutually exclusive because the counts over the
    // N-window range always sum to N). Anything else — only reachable with
    // the default "missing" treat — is INSUFFICIENT_DATA.
    let breaching_windows: Vec<AlarmWindow> = good
        .iter()
        .filter(|w| is_breaching(w.value, threshold, &operator))
        .cloned()
        .collect();
    let clear_windows: Vec<AlarmWindow> = good
        .iter()
        .filter(|w| !is_breaching(w.value, threshold, &operator))
        .cloned()
        .collect();
    let breaches = breaching_windows.len()
        + match treat.as_str() {
            "breaching" => missing,
            _ => 0,
        };
    let clears = clear_windows.len()
        + match treat.as_str() {
            "notBreaching" => missing,
            _ => 0,
        };
    let clear_needed = eval_periods.saturating_sub(datapoints_to_alarm) + 1;

    let (state, reason) = if breaches >= datapoints_to_alarm {
        if breaching_windows.is_empty() {
            let unit = if missing == 1 { "" } else { "s" };
            (
                "ALARM",
                format!(
                    "Threshold Crossed: {missing} missing datapoint{unit} treated as breaching."
                ),
            )
        } else {
            (
                "ALARM",
                threshold_reason(
                    &breaching_windows,
                    true,
                    phrase,
                    &threshold_str,
                    missing,
                    &treat,
                ),
            )
        }
    } else if clears >= clear_needed {
        if clear_windows.is_empty() {
            let unit = if missing == 1 { "" } else { "s" };
            (
                "OK",
                format!(
                    "Threshold not Crossed: {missing} missing datapoint{unit} treated as notBreaching."
                ),
            )
        } else {
            (
                "OK",
                threshold_reason(
                    &clear_windows,
                    false,
                    phrase,
                    &threshold_str,
                    missing,
                    &treat,
                ),
            )
        }
    } else {
        // Neither side settled: only the "missing" treat can get here
        // (breaching/notBreaching convert every window into a count).
        let unit = if missing == 1 { "" } else { "s" };
        let verb = if missing == 1 { "was" } else { "were" };
        (
            "INSUFFICIENT_DATA",
            format!("Insufficient Data: {missing} datapoint{unit} {verb} unknown."),
        )
    };
    Some((state.to_string(), reason))
}

/// The alarm state envelope the twin publishes on every state transition —
/// the documented real-CloudWatch SNS/Lambda payload with the AlarmName,
/// AlarmDescription, AWSAccountId, NewStateValue, NewStateReason,
/// StateChangeTime, Region and OldStateValue fields alarm consumers parse
/// (`NewStateValue` is what gates the ALARM/OK branches in the notification
/// suites). moto publishes no alarm envelope at all — like the evaluation
/// rules above, the shape follows the real CloudWatch notification contract.
fn alarm_notification(
    alarm: &Value,
    old_state: &str,
    new_state: &str,
    reason: &str,
    region: &str,
    account: u64,
) -> Value {
    json!({
        "AlarmName": field_str(alarm, "AlarmName"),
        "AlarmDescription": field_str(alarm, "AlarmDescription"),
        "AWSAccountId": account.to_string(),
        "NewStateValue": new_state,
        "NewStateReason": reason,
        "StateChangeTime": default_ts_str(),
        "Region": region,
        "OldStateValue": old_state,
    })
}

/// The action-ARN list + notification envelope for an alarm entering
/// `new_state`; `None` when no actions are configured (nothing to deliver).
fn state_notification(
    alarm: &Value,
    old_state: &str,
    new_state: &str,
    reason: &str,
    region: &str,
    account: u64,
) -> Option<(Vec<String>, Value)> {
    let actions_key = match new_state {
        "ALARM" => "AlarmActions",
        "OK" => "OKActions",
        _ => "InsufficientDataActions",
    };
    let arns = str_list(alarm, actions_key);
    if arns.is_empty() {
        return None;
    }
    Some((
        arns,
        alarm_notification(alarm, old_state, new_state, reason, region, account),
    ))
}

/// Freshness gate for the delivery loop (S2-c): a staged transition may go
/// stale in the lock-free window before/while it delivers — the twin's alarm
/// actions are synchronous, so a Lambda target can re-enter the twin,
/// transition or delete the same alarm, and every later notification of that
/// transition would then contradict the alarm's real state (real CloudWatch
/// notifies once per transition). A delivery is current only while the alarm
/// still exists and its StateValue still equals the state it was staged for.
fn delivery_is_current(state: &CloudwatchState, alarm_name: &str, target_state: &str) -> bool {
    let state_value = state
        .alarms
        .read()
        .get(alarm_name)
        .map(|a| field_str(a, "StateValue"));
    state_value
        .unwrap_or_else(|| {
            state
                .composite_alarms
                .read()
                .get(alarm_name)
                .map(|a| field_str(a, "StateValue"))
                .unwrap_or_default()
        })
        .as_str()
        == target_state
}

/// One MetricDataQuery MetricStat source for GetMetricData collection.
struct MetricQuerySource {
    namespace: String,
    metric_name: String,
    dimensions: Vec<(String, String)>,
    period: i64,
    stat: String,
    /// The requested [StartTime, EndTime) window (epoch millis, half-open
    /// like moto's `get_metric_data` filter — `start <= t < end`); None
    /// bounds mean "no bound from this actor".
    start_ms: Option<i64>,
    end_ms: Option<i64>,
}

impl MetricQuerySource {
    fn dims_ok(datum: &[(String, String)], expected: &[(String, String)]) -> bool {
        expected.iter().all(|f| datum.contains(f))
    }

    /// Bucket the matching datapoints into `out[id]`, keyed by the query
    /// `Id` that result lookups and math expressions refer to.
    fn collect(&self, id: &str, metrics: &[MetricDatum], out: &mut BTreeMap<String, Value>) {
        let mut filtered: Vec<&MetricDatum> = metrics
            .iter()
            .filter(|md| {
                md.namespace == self.namespace
                    && md.name == self.metric_name
                    && Self::dims_ok(&md.dimensions, &self.dimensions)
                    // The requested window bounds the store (moto's
                    // get_metric_data filters `start <= t < end` before
                    // bucketing — otherwise old datapoints dilute every
                    // statistic).
                    && self.start_ms.map(|s| md.timestamp >= s).unwrap_or(true)
                    && self.end_ms.map(|e| md.timestamp < e).unwrap_or(true)
            })
            .collect();
        if filtered.is_empty() {
            return;
        }
        filtered.sort_by_key(|md| md.timestamp);
        let mut values: Vec<f64> = Vec::new();
        let mut timestamps: Vec<i64> = Vec::new();
        let period_ms = self.period.max(1) * 1000;
        let mut bucket_start = filtered[0].timestamp;
        let mut idx = 0usize;
        while bucket_start <= filtered.last().unwrap().timestamp {
            let bucket_end = bucket_start.saturating_add(period_ms);
            let mut bucket_values: Vec<f64> = Vec::new();
            let mut aggs: Vec<DatumStats> = Vec::new();
            while idx < filtered.len() && filtered[idx].timestamp < bucket_end {
                let md = filtered[idx];
                if let Some(v) = md.single_value() {
                    bucket_values.push(v);
                }
                if let Some(s) = md.stats {
                    aggs.push(s);
                }
                idx += 1;
            }
            if !bucket_values.is_empty() || !aggs.is_empty() {
                if let Some(v) = agg_stat(&self.stat, &bucket_values, &aggs) {
                    values.push(v);
                    timestamps.push(bucket_start);
                }
            }
            bucket_start = bucket_end;
        }
        out.insert(
            id.to_string(),
            json!({ "Values": values, "Timestamps": timestamps }),
        );
    }
}

/// Aggregate a bucket of values according to a standard statistic.
fn agg_stat(stat: &str, values: &[f64], agg: &[DatumStats]) -> Option<f64> {
    match stat {
        "SampleCount" => {
            Some(values.len() as f64 + agg.iter().map(|a| a.sample_count).sum::<f64>())
        }
        "Sum" => Some(values.iter().sum::<f64>() + agg.iter().map(|a| a.sum).sum::<f64>()),
        "Average" => {
            let sample_count =
                values.len() as f64 + agg.iter().map(|a| a.sample_count).sum::<f64>();
            if sample_count <= 0.0 {
                None
            } else {
                Some(
                    (values.iter().sum::<f64>() + agg.iter().map(|a| a.sum).sum::<f64>())
                        / sample_count,
                )
            }
        }
        "Minimum" => values
            .iter()
            .copied()
            .chain(agg.iter().map(|a| a.minimum))
            .reduce(f64::min),
        "Maximum" => values
            .iter()
            .copied()
            .chain(agg.iter().map(|a| a.maximum))
            .reduce(f64::max),
        _ => None,
    }
}

/// Tokenize a metric math expression.
fn tokenize_expression(expr: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut buf = String::new();
    for c in expr.chars() {
        if c.is_alphanumeric() || c == '_' {
            buf.push(c);
        } else {
            if !buf.is_empty() {
                tokens.push(buf.clone());
                buf.clear();
            }
            if !c.is_whitespace() {
                tokens.push(c.to_string());
            }
        }
    }
    if !buf.is_empty() {
        tokens.push(buf);
    }
    tokens
}

fn eval_sum(
    tokens: &[String],
    pos: &mut usize,
    data: &BTreeMap<String, Value>,
) -> Option<Vec<f64>> {
    let mut left = eval_product(tokens, pos, data)?;
    while *pos < tokens.len() && (tokens[*pos] == "+" || tokens[*pos] == "-") {
        let op = tokens[*pos].clone();
        *pos += 1;
        let right = eval_product(tokens, pos, data)?;
        left = zip_values(&left, &right, &op);
    }
    Some(left)
}

fn eval_product(
    tokens: &[String],
    pos: &mut usize,
    data: &BTreeMap<String, Value>,
) -> Option<Vec<f64>> {
    let mut left = eval_factor(tokens, pos, data)?;
    while *pos < tokens.len() && (tokens[*pos] == "*" || tokens[*pos] == "/") {
        let op = tokens[*pos].clone();
        *pos += 1;
        let right = eval_factor(tokens, pos, data)?;
        left = zip_values(&left, &right, &op);
    }
    Some(left)
}

fn eval_factor(
    tokens: &[String],
    pos: &mut usize,
    data: &BTreeMap<String, Value>,
) -> Option<Vec<f64>> {
    if *pos >= tokens.len() {
        return None;
    }
    match tokens[*pos].as_str() {
        "(" => {
            *pos += 1;
            let v = eval_sum(tokens, pos, data)?;
            if *pos < tokens.len() && tokens[*pos] == ")" {
                *pos += 1;
            }
            Some(v)
        }
        "-" => {
            *pos += 1;
            let v = eval_factor(tokens, pos, data)?;
            Some(v.into_iter().map(|x| -x).collect())
        }
        ")" => None,
        tok => {
            *pos += 1;
            if let Ok(n) = tok.parse::<f64>() {
                return Some(vec![n]);
            }
            data.get(tok).map(|v| {
                v["Values"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|x| x.as_f64()).collect())
                    .unwrap_or_default()
            })
        }
    }
}

/// Elementwise arithmetic over two value arrays with length-1 broadcast.
fn zip_values(left: &[f64], right: &[f64], op: &str) -> Vec<f64> {
    let apply = |a: f64, b: f64| match op {
        "+" => a + b,
        "-" => a - b,
        "*" => a * b,
        "/" => {
            if b == 0.0 {
                0.0
            } else {
                a / b
            }
        }
        _ => a,
    };
    if right.len() == 1 && !right.is_empty() && left.len() > 1 {
        return left.iter().map(|a| apply(*a, right[0])).collect();
    }
    if left.len() == 1 && !left.is_empty() && right.len() > 1 {
        return right.iter().map(|b| apply(left[0], *b)).collect();
    }
    left.iter()
        .zip(right.iter())
        .map(|(a, b)| apply(*a, *b))
        .chain(
            left.iter()
                .skip(right.len())
                .map(|&a| apply(a, 0.0))
                .take(0),
        )
        .collect()
}

impl CloudwatchHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_state(&self, account: u64, region: &str) -> CloudwatchState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    /// View of request params: raw JSON object with flattened query-protocol
    /// member keys reconstructed into nested arrays/objects.
    fn params(&self, req: &AwsRequest) -> Value {
        let mut out = req.params.clone();
        if let Some(obj) = req.params.as_object() {
            let mut roots: Vec<String> = Vec::new();
            for k in obj.keys() {
                if k.contains('.') {
                    let root = k.split('.').next().unwrap_or("").to_string();
                    if !root.is_empty() && !roots.contains(&root) {
                        roots.push(root);
                    }
                }
            }
            for root in roots {
                if !obj.contains_key(&root) {
                    if let Some(v) = unflatten(&req.params, &root) {
                        if let Some(out_obj) = out.as_object_mut() {
                            out_obj.insert(root.clone(), v);
                        }
                    }
                }
            }
        }
        out
    }

    fn ts_param(params: &Value, key: &str) -> i64 {
        match params.get(key) {
            Some(Value::String(s)) => parse_iso_ts(s),
            // GetMetricStatistics StartTime/EndTime arrive as epoch-seconds
            // floats on the JSON wire (unit decision: epoch_scalar_to_millis).
            Some(Value::Number(n)) => n.as_f64().and_then(epoch_scalar_to_millis),
            _ => None,
        }
        .unwrap_or_else(now_millis)
    }

    /// Strict variant: `None` when the member is absent/malformed (moto
    /// answers InvalidParameterValue for a missing StartTime/EndTime — the
    /// old default-to-now silently evaluated the WRONG window).
    fn ts_param_strict(params: &Value, key: &str) -> Option<i64> {
        match params.get(key) {
            Some(Value::String(s)) => parse_iso_ts(s),
            Some(Value::Number(n)) => n.as_f64().and_then(epoch_scalar_to_millis),
            _ => None,
        }
    }

    /// (ActionPrefix, AlarmNamePrefix, AlarmNames, StateValue) filters.
    fn alarm_filters(
        params: &Value,
    ) -> (Option<String>, Option<String>, Vec<String>, Option<String>) {
        (
            as_str(params.get("ActionPrefix")),
            as_str(params.get("AlarmNamePrefix")),
            str_list(params, "AlarmNames"),
            as_str(params.get("StateValue")),
        )
    }

    /// Filter alarm/composite values by the DescribeAlarms parameters.
    fn filter_alarms(
        items: Vec<Value>,
        action_prefix: &Option<String>,
        name_prefix: &Option<String>,
        names: &[String],
        state_filter: &Option<String>,
    ) -> Vec<Value> {
        items
            .into_iter()
            .filter(|a| {
                if let Some(p) = action_prefix {
                    let actions = str_list(a, "AlarmActions");
                    if !actions.iter().any(|x| x.starts_with(p.as_str())) {
                        return false;
                    }
                }
                if let Some(p) = name_prefix {
                    if !field_str(a, "AlarmName").starts_with(p.as_str()) {
                        return false;
                    }
                }
                if !names.is_empty() && !names.contains(&field_str(a, "AlarmName")) {
                    return false;
                }
                if let Some(sv) = state_filter {
                    if &field_str(a, "StateValue") != sv {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let params = self.params(&req);
        let req = AwsRequest { params, ..req };
        match req.operation.as_str() {
            "PutMetricData" => self.put_metric_data(&req),
            "GetMetricStatistics" => self.get_metric_statistics(&req),
            "GetMetricData" => self.get_metric_data(&req),
            "ListMetrics" => self.list_metrics(&req),
            "DescribeAlarms" => self.describe_alarms(&req),
            "PutMetricAlarm" => self.put_metric_alarm(&req),
            "DeleteAlarms" => self.delete_alarms(&req),
            "DescribeAlarmsForMetric" => self.describe_alarms_for_metric(&req),
            "SetAlarmState" => self.set_alarm_state(&req),
            "EnableAlarmActions" => self.set_alarm_actions(&req, true),
            "DisableAlarmActions" => self.set_alarm_actions(&req, false),
            "DescribeAlarmHistory" => AwsResponse::json(200, json!({ "AlarmHistoryItems": [] })),
            "PutCompositeAlarm" => self.put_composite_alarm(&req),
            "PutDashboard" => self.put_dashboard(&req),
            "GetDashboard" => self.get_dashboard(&req),
            "ListDashboards" => self.list_dashboards(&req),
            "DeleteDashboards" => self.delete_dashboards(&req),
            "ListTagsForResource" => self.list_tags(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "PutAnomalyDetector" => self.put_anomaly_detector(&req),
            "DescribeAnomalyDetectors" => self.describe_anomaly_detectors(&req),
            "DeleteAnomalyDetector" => self.delete_anomaly_detector(&req),
            "PutInsightRule" => self.put_insight_rule(&req),
            "DescribeInsightRules" => self.describe_insight_rules(&req),
            "DeleteInsightRules" => self.rule_list_action(&req, RuleListAction::Delete),
            "DisableInsightRules" => self.rule_list_action(&req, RuleListAction::Disable),
            "EnableInsightRules" => self.rule_list_action(&req, RuleListAction::Enable),
            "GetInsightRuleReport" => AwsResponse::json(
                200,
                json!({
                    "KeyLabels": [],
                    "AggregationStatistic": "Sum",
                    "AggregateValue": 0.0,
                    "ApproximateUniqueCount": 0,
                    "Contributors": [],
                    "MetricDatapoints": []
                }),
            ),
            "PutManagedInsightRules" => AwsResponse::json(200, json!({ "Failures": [] })),
            "ListManagedInsightRules" => AwsResponse::json(200, json!({ "ManagedRules": [] })),
            "PutMetricStream" => self.put_metric_stream(&req),
            "GetMetricStream" => self.get_metric_stream(&req),
            "DeleteMetricStream" => self.delete_metric_stream(&req),
            "StartMetricStreams" => self.set_stream_state(&req, "running"),
            "StopMetricStreams" => self.set_stream_state(&req, "stopped"),
            "ListMetricStreams" => self.list_metric_streams(&req),
            "GetMetricWidgetImage" => AwsResponse::json(
                200,
                json!({
                    "MetricWidgetImage":
                        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
                }),
            ),
            "DescribeAlarmContributors" => {
                AwsResponse::json(200, json!({ "AlarmContributors": [] }))
            }
            "PutAlarmMuteRule" => self.put_alarm_mute_rule(&req),
            "GetAlarmMuteRule" => self.get_alarm_mute_rule(&req),
            "DeleteAlarmMuteRule" => self.delete_alarm_mute_rule(&req),
            "ListAlarmMuteRules" => AwsResponse::json(200, json!({})),
            other => Self::error_for(
                &req,
                400,
                "ValidationException",
                &format!("The operation {} is not implemented", other),
            ),
        }
    }

    // ---- metrics ----

    fn put_metric_data(&self, req: &AwsRequest) -> AwsResponse {
        let namespace = str_param(&req.params, "Namespace");
        let items = match req.params.get("MetricData") {
            Some(Value::Array(a)) => a.clone(),
            _ => vec![],
        };
        let state = self.get_state(req.account, &req.region);
        let mut metrics = state.metrics.write();
        // Namespaces this call touched: alarm evaluation runs after the write
        // lock is released so metric alarms watch their metric again here.
        let mut touched_namespaces: Vec<String> = Vec::new();
        for dp in &items {
            let name = field_str(dp, "MetricName");
            if name.is_empty() {
                continue;
            }
            // moto models.py _validate_parameters_put_metric_data: a Value
            // AND StatisticValues together is an InvalidParamCombination,
            // and a StatisticValues object missing any of its four members
            // is a missing required parameter (the silent 0.0 fallback
            // poisoned Minimum/Maximum aggregation).
            let has_value = dp.get("Value").is_some();
            let has_stats = dp.get("StatisticValues").is_some();
            if has_value && has_stats {
                return Self::error_for(
                    req,
                    400,
                    "InvalidParameterCombination",
                    "Value and StatisticValues cannot be used together",
                );
            }
            if let Some(sv) = dp.get("StatisticValues") {
                for member in ["Sum", "SampleCount", "Minimum", "Maximum"] {
                    if sv.get(member).is_none() {
                        return Self::error_for(
                            req,
                            400,
                            "MissingRequiredParameter",
                            &format!("Missing required parameter in MetricData.StatisticValues: {member}"),
                        );
                    }
                }
            }
            // A NaN value cannot serialize into the snapshot (serde_json
            // renders it `null` on save) — the (account, region) partition
            // was silently DROPPED from state save. Reject at the wire.
            let nan_value = |v: Option<f64>| v.map(|f| f.is_nan()).unwrap_or(false);
            if nan_value(dp.get("Value").and_then(as_f64)) {
                return Self::error_for(
                    req,
                    400,
                    "InvalidParameterValue",
                    "Value must be a finite number",
                );
            }
            if let Some(sv) = dp.get("StatisticValues") {
                for member in ["Sum", "SampleCount", "Minimum", "Maximum"] {
                    if nan_value(sv.get(member).and_then(as_f64)) {
                        return Self::error_for(
                            req,
                            400,
                            "InvalidParameterValue",
                            &format!("StatisticValues.{member} must be a finite number"),
                        );
                    }
                }
            }
            let dimensions = dims_from_value(dp.get("Dimensions"));
            let value = dp.get("Value").and_then(as_f64);
            let stats = dp.get("StatisticValues").map(|sv| DatumStats {
                sample_count: as_f64(sv.get("SampleCount").unwrap_or(&json!(0.0))).unwrap_or(0.0),
                sum: as_f64(sv.get("Sum").unwrap_or(&json!(0.0))).unwrap_or(0.0),
                minimum: as_f64(sv.get("Minimum").unwrap_or(&json!(0.0))).unwrap_or(0.0),
                maximum: as_f64(sv.get("Maximum").unwrap_or(&json!(0.0))).unwrap_or(0.0),
            });
            let timestamp: i64 = match dp.get("Timestamp") {
                Some(Value::String(s)) => parse_iso_ts(s),
                // botocore sends MetricData[].Timestamp as epoch-seconds
                // floats (unit decision: epoch_scalar_to_millis).
                Some(Value::Number(n)) => n.as_f64().and_then(epoch_scalar_to_millis),
                _ => None,
            }
            .unwrap_or_else(now_millis);

            if !namespace.is_empty() && !touched_namespaces.contains(&namespace) {
                touched_namespaces.push(namespace.clone());
            }

            if let Some(values) = dp.get("Values").and_then(|v| v.as_array()) {
                let counts_v = dp.get("Counts").and_then(|c| c.as_array());
                // moto validates Values and Counts lengths match.
                if let Some(counts) = counts_v {
                    if counts.len() != values.len() {
                        return Self::error_for(
                            req,
                            400,
                            "InvalidParameter",
                            &format!(
                                "The parameter Counts must be the same length as Values ({} vs {})",
                                counts.len(),
                                values.len()
                            ),
                        );
                    }
                }
                for (i, v) in values.iter().enumerate() {
                    // Count=0 contributes ZERO datapoints (the old max(1)
                    // turned a legit "no occurrences" count into a datum).
                    let count = counts_v
                        .and_then(|c| c.get(i))
                        .and_then(as_f64)
                        .unwrap_or(1.0);
                    if count.is_nan() || count != count.floor() {
                        return Self::error_for(
                            req,
                            400,
                            "InvalidParameterValue",
                            "Counts must be positive integers",
                        );
                    }
                    for _ in 0..(count.max(0.0) as usize) {
                        metrics.push(MetricDatum {
                            namespace: namespace.clone(),
                            name: name.clone(),
                            unit: as_str(dp.get("Unit")),
                            dimensions: dimensions.clone(),
                            timestamp,
                            value: as_f64(v),
                            stats: None,
                        });
                    }
                }
            } else if value.is_some() || stats.is_some() {
                metrics.push(MetricDatum {
                    namespace: namespace.clone(),
                    name,
                    unit: as_str(dp.get("Unit")),
                    dimensions,
                    timestamp,
                    value,
                    stats,
                });
            }
        }
        drop(metrics);
        // Evaluate the metric alarms of the namespaces this call produced
        // datapoints for — synchronously on the PutMetricData path (real
        // CloudWatch evaluates on a one-minute cadence; the twin keeps
        // in-order semantics by doing it here), then send one notification
        // per state transition.
        if !touched_namespaces.is_empty() {
            self.evaluate_alarms(req.account, &req.region, &touched_namespaces);
        }
        AwsResponse::json(200, json!({}))
    }

    /// Evaluate the metric alarms watching one of `namespaces` against the
    /// account/region metric store and deliver the state's action ARNs for
    /// every alarm whose state actually changes — repeated evaluations in
    /// the same state never re-notify (real CloudWatch: one notification per
    /// transition). Deliveries happen after the alarm store lock is released
    /// and re-validate the alarm's current state before each action ARN
    /// ([`delivery_is_current`]), because a Lambda action may re-enter this
    /// handler while the notification pass is in flight. moto's own backend
    /// never evaluates alarms (5.2.4 keeps `Alarm.state_value` untouched —
    /// see models.py/update_state), so the twin's transitions follow the
    /// real CloudWatch alarm rules above.
    fn evaluate_alarms(&self, account: u64, region: &str, namespaces: &[String]) {
        let state = self.get_state(account, region);
        let mut deliveries: Vec<(String, String, Vec<String>, Value)> = Vec::new();
        {
            let metrics = state.metrics.read();
            let mut alarms = state.alarms.write();
            for (alarm_name, alarm) in alarms.iter_mut() {
                let ns = field_str(alarm, "Namespace");
                if !namespaces.contains(&ns) {
                    continue;
                }
                let Some((target, reason)) = alarm_candidate(&metrics, alarm, now_millis()) else {
                    continue;
                };
                let old_state = field_str(alarm, "StateValue");
                if old_state == target {
                    continue;
                }
                alarm["StateValue"] = json!(target);
                alarm["StateReason"] = json!(reason);
                alarm["StateUpdatedTimestamp"] = json!(default_ts_str());
                if !alarm["ActionsEnabled"].as_bool().unwrap_or(true) {
                    continue;
                }
                if let Some((arns, message)) =
                    state_notification(alarm, &old_state, &target, &reason, region, account)
                {
                    deliveries.push((alarm_name.clone(), target, arns, message));
                }
            }
        }
        for (alarm_name, target, arns, message) in &deliveries {
            crate::bridge::deliver_actions_if_valid(arns, message, account, region, || {
                delivery_is_current(&state, alarm_name, target)
            });
        }
    }

    fn dims_subset(datum: &[(String, String)], filter: &[(String, String)]) -> bool {
        dims_subset(datum, filter)
    }

    fn list_metrics(&self, req: &AwsRequest) -> AwsResponse {
        let namespace = str_param(&req.params, "Namespace");
        let metric_name = str_param(&req.params, "MetricName");
        let filter_dims = dims_from_value(req.params.get("Dimensions"));

        let state = self.get_state(req.account, &req.region);
        let metrics = state.metrics.read();
        let mut seen: MetricSet = Vec::new();
        let mut out: Vec<Value> = Vec::new();
        for md in metrics.iter() {
            if !namespace.is_empty() && md.namespace != namespace {
                continue;
            }
            if !metric_name.is_empty() && md.name != metric_name {
                continue;
            }
            if !Self::dims_subset(&md.dimensions, &filter_dims) {
                continue;
            }
            let key = (md.namespace.clone(), md.name.clone(), md.dimensions.clone());
            if seen.contains(&key) {
                continue;
            }
            seen.push(key);
            out.push(json!({
                "Namespace": md.namespace,
                "MetricName": md.name,
                "Dimensions": dims_json(&md.dimensions),
            }));
        }
        AwsResponse::json(
            200,
            json!({
                "Metrics": out,
                "NextToken": Value::Null
            }),
        )
    }

    /// Error shaped for the incoming protocol: JSON-protocol requests
    /// (`X-Amz-Target` bodies) get the JSON envelope, form-encoded
    /// query-protocol requests get the AWS `<ErrorResponse>` XML.
    fn error_for(req: &AwsRequest, status: u16, code: &str, message: &str) -> AwsResponse {
        // The sniff rides the gateway-parsed params (`Action` exists only for
        // a form-encoded query body — the gateway consumes the raw body, so a
        // body-substring check misses real query posts). Mirrors the sqs
        // handler's chooser.
        let is_query = req
            .params
            .get("Action")
            .and_then(|v| v.as_str())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        if is_query {
            AwsResponse::query_error(status, code, message)
        } else {
            AwsResponse::error(status, code, message)
        }
    }

    fn get_metric_statistics(&self, req: &AwsRequest) -> AwsResponse {
        let namespace = str_param(&req.params, "Namespace");
        let metric_name = str_param(&req.params, "MetricName");
        // moto: start/end are required and must be ordered
        // (InvalidParameterValue otherwise) — defaulting missing markers to
        // "now" evaluated empty ranges against the wrong domain silently.
        let start = Self::ts_param_strict(&req.params, "StartTime")
            .ok_or(())
            .or_else(|_| {
                Err(Self::error_for(
                    req,
                    400,
                    "InvalidParameterValue",
                    "StartTime is required and must be a valid timestamp",
                ))
            });
        let start = match start {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        let end = Self::ts_param_strict(&req.params, "EndTime")
            .ok_or(())
            .or_else(|_| {
                Err(Self::error_for(
                    req,
                    400,
                    "InvalidParameterValue",
                    "EndTime is required and must be a valid timestamp",
                ))
            });
        let end = match end {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        if start >= end {
            return Self::error_for(
                req,
                400,
                "InvalidParameterValue",
                "StartTime must be before EndTime",
            );
        }
        let period = req
            .params
            .get("Period")
            .and_then(as_f64)
            .unwrap_or(60.0)
            .max(1.0) as i64;
        let statistics = str_list(&req.params, "Statistics");
        let ext_stats = str_list(&req.params, "ExtendedStatistics");
        let unit_filter = as_str(req.params.get("Unit"));
        let filter_dims = dims_from_value(req.params.get("Dimensions"));

        // moto: statistics or extended statistics must be specified (HTTP 400
        // InvalidParameterCombination).
        if statistics.is_empty() && ext_stats.is_empty() {
            return Self::error_for(
                req,
                400,
                "InvalidParameterCombinationException",
                "Must specify either Statistics or ExtendedStatistics",
            );
        }

        let state = self.get_state(req.account, &req.region);
        let metrics = state.metrics.read();
        let mut filtered: Vec<&MetricDatum> = metrics
            .iter()
            .filter(|md| {
                md.namespace == namespace
                    && md.name == metric_name
                    && md.timestamp >= start
                    && md.timestamp < end
            })
            .collect();
        if let Some(u) = &unit_filter {
            filtered.retain(|md| md.unit.as_ref() == Some(u));
        }
        if !filter_dims.is_empty() {
            filtered.retain(|md| Self::dims_subset(&md.dimensions, &filter_dims));
        }
        filtered.sort_by_key(|md| md.timestamp);

        if filtered.is_empty() {
            return AwsResponse::json(
                200,
                json!({
                    "Label": metric_name,
                    "Datapoints": []
                }),
            );
        }

        let period_ms = period * 1000;
        let first_ts = filtered[0].timestamp;
        let last_ts = filtered.last().unwrap().timestamp;
        let mut datapoints: Vec<Value> = Vec::new();
        let mut bucket_start = first_ts;
        let mut idx = 0usize;
        while bucket_start <= last_ts {
            let bucket_end = bucket_start.saturating_add(period_ms);
            let mut values: Vec<f64> = Vec::new();
            let mut aggs: Vec<DatumStats> = Vec::new();
            let mut unit: Option<String> = None;
            while idx < filtered.len() && filtered[idx].timestamp < bucket_end {
                let md = filtered[idx];
                if let Some(v) = md.single_value() {
                    values.push(v);
                }
                if let Some(s) = md.stats {
                    aggs.push(s);
                }
                if unit.is_none() {
                    unit = md.unit.clone();
                }
                idx += 1;
            }
            if !values.is_empty() || !aggs.is_empty() {
                let mut dp = Map::new();
                dp.insert("Timestamp".to_string(), json!(ts_str(bucket_start)));
                for stat in &statistics {
                    if let Some(v) = agg_stat(stat, &values, &aggs) {
                        dp.insert(stat.clone(), json!(v));
                    }
                }
                if !ext_stats.is_empty() {
                    let mut sorted = MetricDatum::percentile_samples(&values, &aggs);
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let mut ext = Map::new();
                    for s in &ext_stats {
                        if let Some(rest) = s.strip_prefix('p') {
                            if let Ok(pct) = rest.parse::<f64>() {
                                let n = sorted.len();
                                if n > 0 {
                                    let idx_f = (pct / 100.0) * (n as f64 - 1.0);
                                    let lower = idx_f.floor() as usize;
                                    let upper = (lower + 1).min(n - 1);
                                    let frac = idx_f - lower as f64;
                                    let v = sorted[lower] * (1.0 - frac) + sorted[upper] * frac;
                                    ext.insert(s.clone(), json!(v));
                                }
                            }
                        }
                    }
                    if !ext.is_empty() {
                        dp.insert("ExtendedStatistics".to_string(), Value::Object(ext));
                    }
                }
                if let Some(u) = &unit {
                    dp.insert("Unit".to_string(), json!(u));
                }
                datapoints.push(Value::Object(dp));
            }
            bucket_start = bucket_end;
        }
        AwsResponse::json(
            200,
            json!({
                "Label": metric_name,
                "Datapoints": datapoints
            }),
        )
    }

    fn get_metric_data(&self, req: &AwsRequest) -> AwsResponse {
        // moto's get_metric_data rejects start >= end (InvalidParameterValue);
        // the [start, end) window bounds every MetricStat source before the
        // collection pass. Missing markers read "no bound" — the twin keeps
        // its whole-store fallback for the legacy callers that sent none.
        let opt_ts = |key: &str| -> Option<i64> {
            match req.params.get(key) {
                Some(Value::String(s)) => parse_iso_ts(s),
                Some(Value::Number(n)) => n.as_f64().and_then(epoch_scalar_to_millis),
                _ => None,
            }
        };
        let start_ms = opt_ts("StartTime");
        let end_ms = opt_ts("EndTime");
        if let (Some(s), Some(e)) = (start_ms, end_ms) {
            if s >= e {
                return Self::error_for(
                    req,
                    400,
                    "InvalidParameterValue",
                    "StartTime must be less than EndTime",
                );
            }
        }
        let queries: Vec<Value> = match req.params.get("MetricDataQueries") {
            Some(Value::Array(a)) => a.clone(),
            _ => vec![],
        };
        let state = self.get_state(req.account, &req.region);
        let metrics = state.metrics.read();

        // First pass: collect bucket values for MetricStat queries.
        let mut collected: BTreeMap<String, Value> = BTreeMap::new();
        for q in &queries {
            let id = field_str(q, "Id");
            if id.is_empty() || q.get("MetricStat").is_none() {
                continue;
            }
            let stat_obj = q.get("MetricStat").cloned().unwrap_or(Value::Null);
            let metric = stat_obj.get("Metric").cloned().unwrap_or(Value::Null);
            let source = MetricQuerySource {
                namespace: field_str(&metric, "Namespace"),
                metric_name: field_str(&metric, "MetricName"),
                dimensions: dims_from_value(metric.get("Dimensions")),
                period: stat_obj.get("Period").and_then(as_f64).unwrap_or(60.0) as i64,
                stat: field_str_or(&stat_obj, "Stat", "Average"),
                start_ms,
                end_ms,
            };
            source.collect(&id, &metrics, &mut collected);
        }

        // Second pass: emit results (evaluating expressions). A query with
        // ReturnData false only feeds expressions (moto: results_to_return).
        let mut results: Vec<Value> = Vec::new();
        for q in &queries {
            if q.get("ReturnData").and_then(as_bool) == Some(false) {
                continue;
            }
            let id = field_str(q, "Id");
            let label = field_str_or(q, "Label", &id);
            let expression = as_str(q.get("Expression"));

            let (values, timestamps): (Vec<f64>, Vec<i64>) = if let Some(expr) = &expression {
                let collected = &collected;
                let tokens = tokenize_expression(expr);
                let mut pos = 0usize;
                if let Some(vals) = eval_sum(&tokens, &mut pos, collected) {
                    let ts: Vec<i64> = expr
                        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                        .find(|w| collected.contains_key(*w))
                        .and_then(|w| collected[w]["Timestamps"].as_array().cloned())
                        .map(|t| t.iter().filter_map(|x| x.as_i64()).collect())
                        .unwrap_or_default();
                    (vals, ts)
                } else {
                    (vec![], vec![])
                }
            } else if let Some(entry) = collected.get(&id) {
                (
                    entry["Values"]
                        .as_array()
                        .map(|a| a.iter().filter_map(|x| x.as_f64()).collect())
                        .unwrap_or_default(),
                    entry["Timestamps"]
                        .as_array()
                        .map(|a| a.iter().filter_map(|x| x.as_i64()).collect())
                        .unwrap_or_default(),
                )
            } else {
                (vec![], vec![])
            };

            results.push(json!({
                "Id": id,
                "Label": label,
                "Timestamps": timestamps.into_iter().map(ts_str).collect::<Vec<_>>(),
                "Values": values,
                "StatusCode": "Complete"
            }));
        }
        AwsResponse::json(
            200,
            json!({
                "MetricDataResults": results,
                "Messages": []
            }),
        )
    }

    // ---- alarms ----

    fn put_metric_alarm(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "AlarmName");
        if name.is_empty() {
            return Self::error_for(req, 400, "ValidationError", "AlarmName is required");
        }
        let arn = format!(
            "arn:aws:cloudwatch:{}:{}:alarm:{}",
            req.region, req.account, name
        );
        let now = default_ts_str();
        let mut alarm = json!({
            "AlarmName": name,
            "AlarmArn": arn,
            "AlarmConfigurationUpdatedTimestamp": now.clone(),
            "ActionsEnabled": req.params.get("ActionsEnabled").and_then(as_bool).unwrap_or(true),
            "OKActions": req.params.get("OKActions").unwrap_or(&json!([])).clone(),
            "AlarmActions": req.params.get("AlarmActions").unwrap_or(&json!([])).clone(),
            "InsufficientDataActions": req.params
                .get("InsufficientDataActions")
                .unwrap_or(&json!([]))
                .clone(),
            // State reset on every re-put mirrors moto: put_metric_alarm
            // constructs a fresh Alarm whose __init__ seeds StateValue "OK"
            // / "Unchecked: Initial alarm creation"
            // (moto/cloudwatch/models.py), and moto never delivers, so the
            // reset has no notification side effects there. The twin's
            // delivery makes the reset observable (alarms with actions
            // notice the next evaluation from this OK base); real AWS
            // preserves state across PutMetricAlarm (API_PutMetricAlarm) —
            // divergence deliberately inherited from the oracle.
            "StateValue": "OK",
            "StateReason": "Unchecked: Initial alarm creation",
            "StateReasonData": "{}",
            "StateUpdatedTimestamp": now,
            "MetricName": str_param(&req.params, "MetricName"),
            "Namespace": str_param(&req.params, "Namespace"),
            "Dimensions": req.params.get("Dimensions").unwrap_or(&json!([])).clone(),
            "Period": req.params.get("Period").and_then(as_i64).unwrap_or(60),
            "EvaluationPeriods": req.params.get("EvaluationPeriods").and_then(as_i64).unwrap_or(1),
            "Threshold": req.params.get("Threshold").and_then(as_f64).unwrap_or(0.0),
            "ComparisonOperator": str_param(&req.params, "ComparisonOperator"),
            "Metrics": req.params.get("Metrics").unwrap_or(&json!([])).clone(),
        });
        if let Some(stat) = as_str(req.params.get("Statistic")) {
            alarm["Statistic"] = json!(stat);
        }
        if let Some(es) = as_str(req.params.get("ExtendedStatistic")) {
            alarm["ExtendedStatistic"] = json!(es);
        }
        if let Some(dta) = req.params.get("DatapointsToAlarm").and_then(as_i64) {
            let eval_periods = req
                .params
                .get("EvaluationPeriods")
                .and_then(as_i64)
                .unwrap_or(1);
            // real-AWS validation: M must be <= N — an M>N alarm can never
            // reach ALARM (M datapoints cannot collect within N windows).
            if dta > eval_periods {
                return Self::error_for(
                    req,
                    400,
                    "ValidationError",
                    "DatapointsToAlarm must be less than or equal to EvaluationPeriods",
                );
            }
            alarm["DatapointsToAlarm"] = json!(dta);
        }
        if let Some(desc) = as_str(req.params.get("AlarmDescription")) {
            alarm["AlarmDescription"] = json!(desc);
        }
        if let Some(treat) = as_str(req.params.get("TreatMissingData")) {
            alarm["TreatMissingData"] = json!(treat);
        }
        if let Some(unit) = as_str(req.params.get("Unit")) {
            alarm["Unit"] = json!(unit);
        }
        if let Some(elscp) = as_str(req.params.get("EvaluateLowSampleCountPercentile")) {
            alarm["EvaluateLowSampleCountPercentile"] = json!(elscp);
        }
        if let Some(tmid) = as_str(req.params.get("ThresholdMetricId")) {
            alarm["ThresholdMetricId"] = json!(tmid);
        }
        let alarm_arn = field_str(&alarm, "AlarmArn");
        let state = self.get_state(req.account, &req.region);
        state.alarms.write().insert(name, alarm);

        let tags = str_list_from_params(&req.params, "Tags");
        if !tags.is_empty() {
            let mut all_tags = state.tags.write();
            let entry = all_tags.entry(alarm_arn).or_default();
            for t in tags {
                let key = field_str(&t, "Key");
                if !key.is_empty() {
                    entry.retain(|e| field_str(e, "Key") != key);
                }
                entry.push(t);
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn describe_alarms(&self, req: &AwsRequest) -> AwsResponse {
        let alarm_types = str_list(&req.params, "AlarmTypes");
        let state = self.get_state(req.account, &req.region);
        let (action_prefix, name_prefix, names, state_filter) = Self::alarm_filters(&req.params);
        let want_metric = alarm_types.is_empty() || alarm_types.iter().any(|t| t == "MetricAlarm");
        let want_composite = alarm_types.iter().any(|t| t == "CompositeAlarm");

        let composite_items: Vec<Value> = if want_composite {
            let composites: Vec<Value> = state.composite_alarms.read().values().cloned().collect();
            Self::filter_alarms(
                composites,
                &action_prefix,
                &name_prefix,
                &names,
                &state_filter,
            )
        } else {
            vec![]
        };

        let metric_items: Vec<Value> = if want_metric {
            let alarm_values: Vec<Value> = state.alarms.read().values().cloned().collect();
            Self::filter_alarms(
                alarm_values,
                &action_prefix,
                &name_prefix,
                &names,
                &state_filter,
            )
        } else {
            vec![]
        };

        AwsResponse::json(
            200,
            json!({
                "MetricAlarms": metric_items,
                "CompositeAlarms": composite_items,
                "NextToken": Value::Null
            }),
        )
    }

    fn delete_alarms(&self, req: &AwsRequest) -> AwsResponse {
        let names = str_list(&req.params, "AlarmNames");
        let state = self.get_state(req.account, &req.region);
        {
            let mut alarms = state.alarms.write();
            for n in &names {
                alarms.remove(n);
            }
        }
        {
            let mut composites = state.composite_alarms.write();
            for n in &names {
                composites.remove(n);
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn describe_alarms_for_metric(&self, req: &AwsRequest) -> AwsResponse {
        let namespace = str_param(&req.params, "Namespace");
        let metric_name = str_param(&req.params, "MetricName");
        let dims = dims_from_value(req.params.get("Dimensions"));
        let state = self.get_state(req.account, &req.region);
        let alarms = state.alarms.read();
        let items: Vec<Value> = alarms
            .values()
            .filter(|a| {
                field_str(a, "Namespace") == namespace && field_str(a, "MetricName") == metric_name
            })
            .filter(|a| {
                if dims.is_empty() {
                    return true;
                }
                let adims: Vec<(String, String)> = a
                    .get("Dimensions")
                    .and_then(|d| d.as_array())
                    .map(|arr| {
                        arr.iter()
                            .map(|d| (field_str(d, "Name"), field_str(d, "Value")))
                            .collect()
                    })
                    .unwrap_or_default();
                adims.len() == dims.len() && dims.iter().all(|f| adims.contains(f))
            })
            .cloned()
            .collect();
        AwsResponse::json(
            200,
            json!({
                "MetricAlarms": items,
                "NextToken": Value::Null
            }),
        )
    }

    fn set_alarm_state(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "AlarmName");
        let state = self.get_state(req.account, &req.region);
        // Guard scoping as in evaluate_alarms: both alarm-store write guards
        // live only inside this block. Delivery below must run with the
        // store lock-free — a Lambda action receives the alarm envelope and
        // is free to re-enter the twin (DescribeAlarms/PutMetricData), which
        // parks a second thread on these write guards while this thread
        // waits inside the synchronous invocation; the bounded lambda
        // executor would kill the function at Timeout+5 and the notification
        // would be silently lost.
        let (response, delivery) = {
            let mut alarms = state.alarms.write();
            let mut composites = state.composite_alarms.write();
            let alarm = alarms.get_mut(&name).or_else(|| composites.get_mut(&name));
            let mut delivery: Option<(String, String, Vec<String>, Value)> = None;
            let response = match alarm {
                Some(alarm) => {
                    let value =
                        as_str(req.params.get("StateValue")).unwrap_or_else(|| "OK".to_string());
                    if value != "OK" && value != "ALARM" && value != "INSUFFICIENT_DATA" {
                        return Self::error_for(req, 400, "ValidationError",
                            &format!("1 validation error detected: Value '{value}' at 'stateValue' failed to satisfy constraint: Member must satisfy enum value set: [INSUFFICIENT_DATA, ALARM, OK]"));
                    }
                    if let Some(reason) = as_str(req.params.get("StateReason")) {
                        alarm["StateReason"] = json!(reason);
                    }
                    if let Some(reason_data) = as_str(req.params.get("StateReasonData")) {
                        alarm["StateReasonData"] = json!(reason_data);
                    }
                    let old_state = field_str(alarm, "StateValue");
                    alarm["StateValue"] = json!(value);
                    alarm["StateUpdatedTimestamp"] = json!(default_ts_str());
                    // SetAlarmState invokes the alarm's actions for the new
                    // state when the state actually differs ("When the
                    // updated state differs from the previous value, the
                    // action configured for the appropriate state is
                    // invoked" — API_SetAlarmState).
                    if value != old_state && alarm["ActionsEnabled"].as_bool().unwrap_or(true) {
                        let reason = field_str(alarm, "StateReason");
                        if let Some((arns, message)) = state_notification(
                            alarm,
                            &old_state,
                            &value,
                            &reason,
                            &req.region,
                            req.account,
                        ) {
                            delivery = Some((name.clone(), value.clone(), arns, message));
                        }
                    }
                    AwsResponse::json(200, json!({}))
                }
                None => Self::error_for(
                    req,
                    400,
                    "ResourceNotFoundException",
                    &format!("Alarm {name} not found"),
                ),
            };
            (response, delivery)
        };
        // Deliver after the block released both write guards; each ARN is
        // re-validated against the alarm's current state first
        // ([`delivery_is_current`]) because the lock-free window above lets
        // another caller or a nested hook move or delete the alarm.
        if let Some((_, target, arns, message)) = delivery {
            crate::bridge::deliver_actions_if_valid(
                &arns,
                &message,
                req.account,
                &req.region,
                || delivery_is_current(&state, &name, &target),
            );
        }
        response
    }

    fn set_alarm_actions(&self, req: &AwsRequest, enabled: bool) -> AwsResponse {
        let names = str_list(&req.params, "AlarmNames");
        let state = self.get_state(req.account, &req.region);
        {
            let mut alarms = state.alarms.write();
            for name in &names {
                if let Some(alarm) = alarms.get_mut(name) {
                    alarm["ActionsEnabled"] = json!(enabled);
                }
            }
        }
        {
            let mut composites = state.composite_alarms.write();
            for name in &names {
                if let Some(alarm) = composites.get_mut(name) {
                    alarm["ActionsEnabled"] = json!(enabled);
                }
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn put_composite_alarm(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "AlarmName");
        if name.is_empty() {
            return Self::error_for(req, 400, "ValidationError", "AlarmName is required");
        }
        let rule = str_param(&req.params, "AlarmRule");
        if rule.is_empty() {
            return Self::error_for(req, 400, "ValidationError", "AlarmRule is required");
        }
        let arn = format!(
            "arn:aws:cloudwatch:{}:{}:alarm:{}",
            req.region, req.account, name
        );
        let alarm = json!({
            "AlarmName": name.clone(),
            "AlarmArn": arn,
            "AlarmRule": rule,
            "AlarmDescription": str_param(&req.params, "AlarmDescription"),
            "ActionsEnabled": req.params.get("ActionsEnabled").and_then(as_bool).unwrap_or(true),
            "AlarmActions": req.params.get("AlarmActions").unwrap_or(&json!([])).clone(),
            "OKActions": req.params.get("OKActions").unwrap_or(&json!([])).clone(),
            "InsufficientDataActions": req.params
                .get("InsufficientDataActions")
                .unwrap_or(&json!([]))
                .clone(),
            "StateValue": "INSUFFICIENT_DATA",
            "StateReason": "Unchecked: Initial state",
        });
        let state = self.get_state(req.account, &req.region);
        state.composite_alarms.write().insert(name, alarm);
        AwsResponse::json(200, json!({}))
    }

    // ---- dashboards ----

    fn put_dashboard(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "DashboardName");
        let body = str_param(&req.params, "DashboardBody");
        if serde_json::from_str::<Value>(&body).is_err() {
            return Self::error_for(
                req,
                400,
                "InvalidParameterValue",
                "The value of DashboardBody must be valid JSON",
            );
        }
        let state = self.get_state(req.account, &req.region);
        let entry = json!({
            "DashboardName": name.clone(),
            "DashboardArn": format!("arn:aws:cloudwatch:{}:{}:dashboard/{}", req.region, req.account, name),
            "DashboardBody": body.clone(),
            "LastModified": default_ts_str(),
            "Size": body.len(),
        });
        state.dashboards.write().insert(name, entry);
        AwsResponse::json(200, json!({}))
    }

    fn get_dashboard(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "DashboardName");
        let state = self.get_state(req.account, &req.region);
        let found = state.dashboards.read().get(&name).cloned();
        match found {
            Some(entry) => AwsResponse::json(
                200,
                json!({
                    "DashboardName": entry.get("DashboardName").cloned().unwrap_or(json!(name)),
                    "DashboardArn": entry.get("DashboardArn").cloned().unwrap_or(Value::Null),
                    "DashboardBody": entry.get("DashboardBody").cloned().unwrap_or(Value::Null)
                }),
            ),
            None => Self::error_for(
                req,
                400,
                "ResourceNotFound",
                &format!("Dashboard {name} does not exist"),
            ),
        }
    }

    fn list_dashboards(&self, req: &AwsRequest) -> AwsResponse {
        let prefix = str_param(&req.params, "DashboardNamePrefix");
        let state = self.get_state(req.account, &req.region);
        let dashboards = state.dashboards.read();
        let entries: Vec<Value> = dashboards
            .values()
            .filter(|d| prefix.is_empty() || field_str(d, "DashboardName").starts_with(&prefix))
            .map(|d| {
                json!({
                    "DashboardName": d.get("DashboardName").cloned().unwrap_or(Value::Null),
                    "DashboardArn": d.get("DashboardArn").cloned().unwrap_or(Value::Null),
                    "LastModified": d.get("LastModified").cloned().unwrap_or(Value::Null),
                    // `Size` is a long on the wire. Derived from the body so
                    // entries restored from older snapshots (which stored it
                    // as a string) answer a number too.
                    "Size": field_str(d, "DashboardBody").len()
                })
            })
            .collect();
        AwsResponse::json(200, json!({ "DashboardEntries": entries }))
    }

    fn delete_dashboards(&self, req: &AwsRequest) -> AwsResponse {
        let names = str_list(&req.params, "DashboardNames");
        let state = self.get_state(req.account, &req.region);
        let mut dashboards = state.dashboards.write();
        for name in &names {
            if !dashboards.contains_key(name) {
                return Self::error_for(
                    req,
                    400,
                    "ResourceNotFound",
                    &format!("Dashboard {name} does not exist"),
                );
            }
        }
        for name in &names {
            dashboards.remove(name);
        }
        AwsResponse::json(200, json!({}))
    }

    // ---- tags ----

    fn list_tags(&self, req: &AwsRequest) -> AwsResponse {
        let arn = str_param(&req.params, "ResourceARN");
        let state = self.get_state(req.account, &req.region);
        let tags = state.tags.read().get(&arn).cloned().unwrap_or_default();
        AwsResponse::json(200, json!({ "Tags": tags }))
    }

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = str_param(&req.params, "ResourceARN");
        let tags = str_list_from_params(&req.params, "Tags");
        let state = self.get_state(req.account, &req.region);
        let mut all_tags = state.tags.write();
        let entry = all_tags.entry(arn).or_default();
        for t in tags {
            let key = field_str(&t, "Key");
            if !key.is_empty() {
                entry.retain(|e| field_str(e, "Key") != key);
            }
            entry.push(t);
        }
        AwsResponse::json(200, json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = str_param(&req.params, "ResourceARN");
        let keys = str_list(&req.params, "TagKeys");
        let state = self.get_state(req.account, &req.region);
        let mut all_tags = state.tags.write();
        if let Some(tags) = all_tags.get_mut(&arn) {
            tags.retain(|t| {
                let k = field_str(t, "Key");
                !keys.contains(&k)
            });
        }
        AwsResponse::json(200, json!({}))
    }

    // ---- anomaly detectors ----

    fn put_anomaly_detector(&self, req: &AwsRequest) -> AwsResponse {
        let namespace = str_param(&req.params, "Namespace");
        let metric_name = str_param(&req.params, "MetricName");
        let stat = field_str_or(&req.params, "Stat", "Average");
        let dims = dims_from_value(req.params.get("Dimensions"));
        let state = self.get_state(req.account, &req.region);
        let mut detectors = state.anomaly_detectors.write();
        // Replace any existing detector for the same (ns, name, stat, dims).
        detectors.retain(|d| {
            !(field_str(d, "Namespace") == namespace
                && field_str(d, "MetricName") == metric_name
                && field_str(d, "Stat") == stat
                && (dims.is_empty() || dims_from_value(d.get("Dimensions")) == dims))
        });
        detectors.push(json!({
            "Namespace": namespace,
            "MetricName": metric_name,
            "Dimensions": dims_json(&dims),
            "Stat": stat,
            "Configuration": req.params.get("Configuration").unwrap_or(&json!({})).clone(),
            "StateValue": "PENDING_TRAINING"
        }));
        AwsResponse::json(200, json!({}))
    }

    fn describe_anomaly_detectors(&self, req: &AwsRequest) -> AwsResponse {
        let namespace = str_param(&req.params, "Namespace");
        let metric_name = str_param(&req.params, "MetricName");
        let state = self.get_state(req.account, &req.region);
        let detectors = state.anomaly_detectors.read();
        let items: Vec<Value> = detectors
            .iter()
            .filter(|d| {
                (namespace.is_empty() || field_str(d, "Namespace") == namespace)
                    && (metric_name.is_empty() || field_str(d, "MetricName") == metric_name)
            })
            .cloned()
            .collect();
        AwsResponse::json(200, json!({ "AnomalyDetectors": items }))
    }

    fn delete_anomaly_detector(&self, req: &AwsRequest) -> AwsResponse {
        let namespace = as_str(req.params.get("Namespace"));
        let metric_name = as_str(req.params.get("MetricName"));
        let stat = as_str(req.params.get("Stat"));
        let dims = dims_from_value(req.params.get("Dimensions"));
        // No identifier given: accept and return 200 without deleting (py behaviour).
        if namespace.is_none() && metric_name.is_none() && stat.is_none() {
            return AwsResponse::json(200, json!({}));
        }
        let state = self.get_state(req.account, &req.region);
        let mut detectors = state.anomaly_detectors.write();
        detectors.retain(|d| {
            if namespace.clone().is_some() && as_str(d.get("Namespace")) != namespace {
                return true;
            }
            if metric_name.clone().is_some() && as_str(d.get("MetricName")) != metric_name {
                return true;
            }
            if stat.clone().is_some() && as_str(d.get("Stat")) != stat {
                return true;
            }
            if !dims.is_empty() && dims_from_value(d.get("Dimensions")) != dims {
                return true;
            }
            false
        });
        AwsResponse::json(200, json!({}))
    }

    // ---- insight rules ----

    fn put_insight_rule(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "RuleName");
        if name.is_empty() {
            return Self::error_for(req, 400, "ValidationError", "RuleName is required");
        }
        let state = self.get_state(req.account, &req.region);
        let rule = json!({
            "Name": name,
            "State": field_str_or(&req.params, "RuleState", "ENABLED"),
            "Schema": json!({"Name": "CloudWatchLogRule", "Version": 1}),
            "Definition": str_param(&req.params, "RuleDefinition"),
            "ManagedRule": false
        });
        state.insight_rules.write().insert(name, rule);
        AwsResponse::json(200, json!({}))
    }

    fn describe_insight_rules(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let rules: Vec<Value> = state.insight_rules.read().values().cloned().collect();
        AwsResponse::json(200, json!({ "InsightRules": rules }))
    }

    fn rule_list_action(&self, req: &AwsRequest, action: RuleListAction) -> AwsResponse {
        let names = str_list(&req.params, "RuleNames");
        let state = self.get_state(req.account, &req.region);
        let mut rules = state.insight_rules.write();
        let mut failures = Vec::new();
        for name in &names {
            match rules.get_mut(name) {
                Some(rule) => match action {
                    RuleListAction::Delete => {
                        rules.remove(name);
                    }
                    RuleListAction::Disable => rule["State"] = json!("DISABLED"),
                    RuleListAction::Enable => rule["State"] = json!("ENABLED"),
                },
                None => failures.push(json!({
                    "Name": name,
                    "ErrorCode": "ResourceNotFound",
                    "ExceptionDescription": format!("Insight rule {name} not found")
                })),
            }
        }
        AwsResponse::json(200, json!({ "Failures": failures }))
    }

    // ---- metric streams ----

    fn put_metric_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "Name");
        if name.is_empty() {
            return Self::error_for(req, 400, "ValidationError", "Name is required");
        }
        let arn = format!(
            "arn:aws:cloudwatch:{}:{}:metric-stream/{}",
            req.region, req.account, name
        );
        let now = default_ts_str();
        let stream = json!({
            "Arn": arn.clone(),
            "Name": name.clone(),
            "FirehoseArn": str_param(&req.params, "FirehoseArn"),
            "RoleArn": str_param(&req.params, "RoleArn"),
            "OutputFormat": field_str_or(&req.params, "OutputFormat", "json"),
            "IncludeFilters": req.params.get("IncludeFilters").unwrap_or(&json!([])).clone(),
            "ExcludeFilters": req.params.get("ExcludeFilters").unwrap_or(&json!([])).clone(),
            "StatisticsConfigurations": req.params
                .get("StatisticsConfigurations")
                .unwrap_or(&json!([]))
                .clone(),
            "State": "running",
            "CreationDate": now.clone(),
            "LastUpdateDate": now,
            "IncludeLinkedAccountsMetrics": req.params
                .get("IncludeLinkedAccountsMetrics")
                .and_then(as_bool)
                .unwrap_or(false)
        });
        let state = self.get_state(req.account, &req.region);
        state.metric_streams.write().insert(name, stream);
        AwsResponse::json(200, json!({ "Arn": arn }))
    }

    fn get_metric_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "Name");
        let state = self.get_state(req.account, &req.region);
        let found = state.metric_streams.read().get(&name).cloned();
        match found {
            Some(stream) => AwsResponse::json(200, stream),
            None => Self::error_for(
                req,
                400,
                "ResourceNotFoundException",
                &format!("MetricStream {name} does not exist"),
            ),
        }
    }

    fn delete_metric_stream(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "Name");
        let state = self.get_state(req.account, &req.region);
        state.metric_streams.write().remove(&name);
        AwsResponse::json(200, json!({}))
    }

    fn set_stream_state(&self, req: &AwsRequest, new_state: &str) -> AwsResponse {
        let names = str_list(&req.params, "Names");
        let state = self.get_state(req.account, &req.region);
        let mut streams = state.metric_streams.write();
        for name in &names {
            if let Some(stream) = streams.get_mut(name) {
                stream["State"] = json!(new_state);
            }
        }
        AwsResponse::json(200, json!({}))
    }

    fn list_metric_streams(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let entries: Vec<Value> = state.metric_streams.read().values().cloned().collect();
        AwsResponse::json(200, json!({ "Entries": entries }))
    }

    // ---- alarm mute rules ----

    fn put_alarm_mute_rule(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "Name");
        if name.is_empty() {
            return Self::error_for(req, 400, "ValidationError", "Name is required");
        }
        let state = self.get_state(req.account, &req.region);
        let rule = json!({
            "Name": name,
            "Rule": req.params.get("Rule").cloned().unwrap_or(Value::Null)
        });
        state.mute_rules.write().insert(name, rule);
        AwsResponse::json(200, json!({}))
    }

    fn get_alarm_mute_rule(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "AlarmMuteRuleName");
        let state = self.get_state(req.account, &req.region);
        let found = state.mute_rules.read().get(&name).cloned();
        match found {
            Some(rule) => AwsResponse::json(200, rule),
            None => Self::error_for(
                req,
                400,
                "ResourceNotFoundException",
                &format!("Mute rule {name} does not exist"),
            ),
        }
    }

    fn delete_alarm_mute_rule(&self, req: &AwsRequest) -> AwsResponse {
        let name = str_param(&req.params, "AlarmMuteRuleName");
        let state = self.get_state(req.account, &req.region);
        state.mute_rules.write().remove(&name);
        AwsResponse::json(200, json!({}))
    }
}

impl Default for CloudwatchHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "cloudwatch".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
        }
    }

    fn parse(resp: &AwsResponse) -> Value {
        serde_json::from_str(&resp.body).unwrap()
    }

    #[test]
    fn test_put_and_list_metrics_with_dimensions() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "NS",
                "MetricData": [
                    {"MetricName": "Req", "Value": 1.0,
                     "Dimensions": [{"Name": "Env", "Value": "prod"}]},
                    {"MetricName": "Req", "Value": 2.0,
                     "Dimensions": [{"Name": "Env", "Value": "staging"}]}
                ]
            }),
        ));
        let resp = handler.handle(make_req(
            "ListMetrics",
            json!({
                "Namespace": "NS",
                "MetricName": "Req",
                "Dimensions": [{"Name": "Env", "Value": "prod"}]
            }),
        ));
        let body = parse(&resp);
        let metrics = body["Metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0]["Dimensions"][0]["Value"], "prod");
    }

    #[test]
    fn test_get_metric_statistics_extended_percentiles() {
        let handler = CloudwatchHandler::new();
        let start = chrono::Utc::now();
        let now = start.to_rfc3339();
        let end = (start + chrono::Duration::minutes(5)).to_rfc3339();
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "NS",
                "MetricData": [
                    {"MetricName": "Lat", "Value": 10.0, "Timestamp": now},
                    {"MetricName": "Lat", "Value": 20.0, "Timestamp": now},
                    {"MetricName": "Lat", "Value": 30.0, "Timestamp": now}
                ]
            }),
        ));
        let resp = handler.handle(make_req(
            "GetMetricStatistics",
            json!({
                "Namespace": "NS", "MetricName": "Lat",
                "StartTime": now, "EndTime": end,
                "Period": 300, "Statistics": ["Average"],
                "ExtendedStatistics": ["p50", "p99"]
            }),
        ));
        let body = parse(&resp);
        let dps = body["Datapoints"].as_array().unwrap();
        assert_eq!(dps.len(), 1);
        assert_eq!(dps[0]["Average"], 20.0);
        assert_eq!(dps[0]["ExtendedStatistics"]["p50"], 20.0);
        assert!((dps[0]["ExtendedStatistics"]["p99"].as_f64().unwrap() - 29.8).abs() < 1e-6);
    }

    fn put_and_window(handler: &CloudwatchHandler) -> (String, String) {
        let start = chrono::Utc::now();
        let now = start.to_rfc3339();
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "GMD",
                "MetricData": [
                    {"MetricName": "Reads", "Value": 3.0, "Timestamp": now},
                    {"MetricName": "Reads", "Value": 4.0, "Timestamp": now},
                    {"MetricName": "Writes", "Value": 5.0, "Timestamp": now}
                ]
            }),
        ));
        (
            (start - chrono::Duration::minutes(5)).to_rfc3339(),
            (start + chrono::Duration::minutes(5)).to_rfc3339(),
        )
    }

    fn stat_query(id: &str, metric: &str, stat: &str) -> Value {
        json!({
            "Id": id,
            "MetricStat": {
                "Metric": {"Namespace": "GMD", "MetricName": metric},
                "Period": 600,
                "Stat": stat
            }
        })
    }

    #[test]
    fn get_metric_data_returns_each_metric_stat_by_id() {
        let handler = CloudwatchHandler::new();
        let (start, end) = put_and_window(&handler);
        // Two queries on the same metric must not share one result.
        let body = parse(&handler.handle(make_req(
            "GetMetricData",
            json!({
                "StartTime": start, "EndTime": end,
                "MetricDataQueries": [
                    stat_query("sum", "Reads", "Sum"),
                    stat_query("max", "Reads", "Maximum")
                ]
            }),
        )));
        let results = body["MetricDataResults"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["Id"], "sum");
        assert_eq!(results[0]["Values"], json!([7.0]));
        assert_eq!(results[0]["Timestamps"].as_array().unwrap().len(), 1);
        assert_eq!(results[1]["Id"], "max");
        assert_eq!(results[1]["Values"], json!([4.0]));
    }

    #[test]
    fn get_metric_data_expression_over_hidden_queries() {
        let handler = CloudwatchHandler::new();
        let (start, end) = put_and_window(&handler);
        let mut reads = stat_query("reads", "Reads", "Sum");
        reads["ReturnData"] = json!(false);
        // Query-protocol callers send the flag as a string.
        let mut writes = stat_query("writes", "Writes", "Sum");
        writes["ReturnData"] = json!("false");
        let body = parse(&handler.handle(make_req(
            "GetMetricData",
            json!({
                "StartTime": start, "EndTime": end,
                "MetricDataQueries": [
                    reads,
                    writes,
                    {"Id": "total", "Expression": "reads + writes", "Label": "Total"}
                ]
            }),
        )));
        let results = body["MetricDataResults"].as_array().unwrap();
        assert_eq!(results.len(), 1, "ReturnData false queries are omitted");
        assert_eq!(results[0]["Id"], "total");
        assert_eq!(results[0]["Label"], "Total");
        assert_eq!(results[0]["Values"], json!([12.0]));
        assert_eq!(results[0]["Timestamps"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_statistic_values_aggregation() {
        let handler = CloudwatchHandler::new();
        let start = chrono::Utc::now();
        let now = start.to_rfc3339();
        let end = (start + chrono::Duration::minutes(5)).to_rfc3339();
        handler.handle(make_req("PutMetricData", json!({
            "Namespace": "NS",
            "MetricData": [{
                "MetricName": "Agg",
                "StatisticValues": {"SampleCount": 10.0, "Sum": 500.0, "Minimum": 10.0, "Maximum": 100.0},
                "Timestamp": now
            }]
        })));
        let resp = handler.handle(make_req(
            "GetMetricStatistics",
            json!({
                "Namespace": "NS", "MetricName": "Agg",
                "StartTime": now, "EndTime": end,
                "Period": 300,
                "Statistics": ["Average", "Sum", "SampleCount"]
            }),
        ));
        let body = parse(&resp);
        let dp = &body["Datapoints"][0];
        assert_eq!(dp["SampleCount"], 10.0);
        assert_eq!(dp["Sum"], 500.0);
        assert_eq!(dp["Average"], 50.0);
        // Statistics not requested must be omitted.
        assert_eq!(dp.get("Minimum"), None);
        assert_eq!(dp.get("Maximum"), None);
    }

    #[test]
    fn test_alarm_state_and_actions() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "a", "Namespace": "NS", "MetricName": "M",
                "ComparisonOperator": "GreaterThanThreshold",
                "EvaluationPeriods": 1, "Threshold": 5.0, "Statistic": "Average"
            }),
        ));
        handler.handle(make_req(
            "SetAlarmState",
            json!({
                "AlarmName": "a", "StateValue": "ALARM", "StateReason": "testing"
            }),
        ));
        let desc = handler.handle(make_req(
            "DescribeAlarms",
            json!({
                "AlarmNames": ["a"], "AlarmTypes": ["MetricAlarm"]
            }),
        ));
        let body = parse(&desc);
        let alarm = &body["MetricAlarms"][0];
        assert_eq!(alarm["StateValue"], "ALARM");
        assert_eq!(alarm["StateReason"], "testing");

        handler.handle(make_req(
            "DisableAlarmActions",
            json!({ "AlarmNames": ["a"] }),
        ));
        let desc2 = handler.handle(make_req("DescribeAlarms", json!({ "AlarmNames": ["a"] })));
        assert_eq!(parse(&desc2)["MetricAlarms"][0]["ActionsEnabled"], false);
    }

    #[test]
    fn test_composite_alarm_describe_alarms() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutCompositeAlarm",
            json!({
                "AlarmName": "comp", "AlarmRule": r#"ALARM("child")"#
            }),
        ));
        let desc = handler.handle(make_req(
            "DescribeAlarms",
            json!({
                "AlarmTypes": ["CompositeAlarm"]
            }),
        ));
        let body = parse(&desc);
        assert_eq!(body["CompositeAlarms"].as_array().unwrap().len(), 1);
        assert_eq!(body["MetricAlarms"].as_array().unwrap().len(), 0);
        assert_eq!(
            body["CompositeAlarms"][0]["StateValue"],
            "INSUFFICIENT_DATA"
        );
    }

    #[test]
    fn list_dashboards_size_is_a_number() {
        let handler = CloudwatchHandler::new();
        let body = r#"{"widgets":[]}"#;
        handler.handle(make_req(
            "PutDashboard",
            json!({ "DashboardName": "d", "DashboardBody": body }),
        ));
        let list = parse(&handler.handle(make_req("ListDashboards", json!({}))));
        assert_eq!(list["DashboardEntries"][0]["Size"], json!(body.len()));
    }

    #[test]
    fn test_metric_stream_lifecycle() {
        let handler = CloudwatchHandler::new();
        let put = handler.handle(make_req(
            "PutMetricStream",
            json!({
                "Name": "stream",
                "FirehoseArn": "arn:aws:firehose:us-east-1:123456789012:deliverystream/t",
                "RoleArn": "arn:aws:iam::123456789012:role/t",
                "OutputFormat": "json",
                "IncludeFilters": [{"Namespace": "AWS/EC2"}]
            }),
        ));
        let body = parse(&put);
        assert!(body["Arn"].as_str().unwrap().contains("stream"));

        handler.handle(make_req(
            "StopMetricStreams",
            json!({ "Names": ["stream"] }),
        ));
        let got = handler.handle(make_req("GetMetricStream", json!({ "Name": "stream" })));
        assert_eq!(parse(&got)["State"], "stopped");

        let list = handler.handle(make_req("ListMetricStreams", json!({})));
        assert_eq!(parse(&list)["Entries"].as_array().unwrap().len(), 1);

        handler.handle(make_req("DeleteMetricStream", json!({ "Name": "stream" })));
        assert_eq!(
            handler
                .handle(make_req("GetMetricStream", json!({ "Name": "stream" })))
                .status,
            400
        );
    }

    /// botocore sends CloudWatch timestamps as epoch-SECONDS numbers in the
    /// JSON body (live wire: `"Timestamp": 1792974600` in the
    /// PutMetricData body, `"StartTime": 1792974600, "EndTime": 1792974900`
    /// in GetMetricStatistics — x-amz-json-1.0), while datapoints are stored
    /// as epoch-milliseconds; taking the number raw used to skew both sides
    /// by 1000x, so GetMetricStatistics answered zero datapoints for a
    /// window the caller had just produced. Parse boundaries resolve the
    /// unit: < 1e11 is seconds, >= 1e11 is already ms (the twin keeps ms
    /// numeric epochs, which moto does not accept).
    #[test]
    fn cloudwatch_timestamp_units_seconds_accepted() {
        let handler = CloudwatchHandler::new();
        // seconds float — exactly what botocore puts on the wire
        let secs: f64 = 1_700_000_000.5;
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "NS",
                "MetricData": [{"MetricName": "Sec", "Value": 42.0, "Timestamp": secs}]
            }),
        ));
        // window in epoch-seconds, like botocore's GetMetricStatistics body
        let resp = handler.handle(make_req(
            "GetMetricStatistics",
            json!({
                "Namespace": "NS", "MetricName": "Sec",
                "StartTime": secs - 30.0, "EndTime": secs + 30.0,
                "Period": 60, "Statistics": ["Average"]
            }),
        ));
        let body = parse(&resp);
        let dps = body["Datapoints"].as_array().unwrap();
        assert_eq!(dps.len(), 1, "the epoch-seconds datapoint must be found");
        assert_eq!(dps[0]["Average"], 42.0);

        // a fractional-seconds string epoch is accepted as seconds too
        let resp = handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "NS",
                "MetricData": [{"MetricName": "Str", "Value": 9.0,
                                "Timestamp": (secs - 5.0).to_string()}]
            }),
        ));
        assert_eq!(resp.status, 200);
        let resp2 = handler.handle(make_req(
            "GetMetricStatistics",
            json!({
                "Namespace": "NS", "MetricName": "Str",
                "StartTime": secs - 30.0, "EndTime": secs + 30.0,
                "Period": 60, "Statistics": ["Average"]
            }),
        ));
        let body2 = parse(&resp2);
        let dps2 = body2["Datapoints"].as_array().unwrap();
        assert_eq!(dps2.len(), 1);
        assert_eq!(dps2[0]["Average"], 9.0);

        // an already-milliseconds magnitude keeps its pre-existing pass-through
        let ms: i64 = 1_700_100_000_500;
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "NS",
                "MetricData": [{"MetricName": "Ms", "Value": 3.0, "Timestamp": ms}]
            }),
        ));
        let resp3 = handler.handle(make_req(
            "GetMetricStatistics",
            json!({
                "Namespace": "NS", "MetricName": "Ms",
                "StartTime": ms - 30_000i64, "EndTime": ms + 30_000i64,
                "Period": 60, "Statistics": ["Average"]
            }),
        ));
        let body3 = parse(&resp3);
        let dps3 = body3["Datapoints"].as_array().unwrap();
        assert_eq!(dps3.len(), 1, "epoch-ms numbers keep their ms meaning");
        assert_eq!(dps3[0]["Average"], 3.0);
    }

    // ---- alarm evaluation + action delivery ----

    /// The tests below evaluate alarms over synthetic windows. Every window
    /// is `Period` seconds wide, so a fresh datapoint written with an
    /// explicit future Timestamp only carries data for the very newest
    /// window (no repeated datapoints share an aggregated Sum bucket).
    fn ts_of(secs_from_now: i64) -> String {
        (chrono::Utc::now() + chrono::Duration::seconds(secs_from_now)).to_rfc3339()
    }

    fn make_req_acct(account: u64, operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            account,
            ..make_req(operation, params)
        }
    }

    /// Threshold breach → ALARM transition → SNS AlarmActions delivery; the
    /// same transition state never re-notifies, recovery delivers OKActions,
    /// disabled actions update state silently, SetAlarmState invokes the
    /// state's actions exactly once per transition, the (default)
    /// TreatMissingData=missing path delivers InsufficientDataActions, and a
    /// re-entrant Lambda action (the hook calls back into the twin's
    /// CloudWatch state synchronously) neither deadlocks nor lets the outer
    /// pass publish stale notifications for later ARNs of a multi-ARN action
    /// list. The alarm-action hook registry is process-global, so every
    /// delivery assertion lives in this single test (sole-registrant
    /// contract like the kinesis bridge test).
    #[test]
    fn alarm_evaluation_notifies_once_per_transition() {
        use std::sync::Arc;

        crate::bridge::reset_alarm_actions();
        let deliveries: Arc<parking_lot::Mutex<Vec<(String, String)>>> = Arc::default();
        let recorded = Arc::clone(&deliveries);
        // The re-entrancy arm: when set, the sns hook synchronously re-enters
        // the handler with a staged PutMetricData — the exact shape a Lambda
        // alarm action runs with (one take() disarms it, keeping the nested
        // deliveries one level deep).
        let armed: Arc<parking_lot::Mutex<Option<(Arc<CloudwatchHandler>, AwsRequest)>>> =
            Arc::default();
        let armed_hook = Arc::clone(&armed);
        crate::bridge::register_alarm_action(
            "sns",
            Arc::new(
                move |arn: &str, payload: &str, _account: u64, _region: &str| {
                    recorded.lock().push((arn.to_string(), payload.to_string()));
                    // Take first, then run: the mutex guard must be gone before
                    // the nested handle call starts, or the nested deliveries'
                    // own hook invocations re-lock the same (non-reentrant)
                    // mutex and hang.
                    let reenters = armed_hook.lock().take();
                    if let Some((nested_handler, nested_req)) = reenters {
                        let _ = nested_handler.handle(nested_req);
                    }
                    Ok(())
                },
            ),
        );

        let handler = Arc::new(CloudwatchHandler::new());
        let account = 447089000901u64;
        let topic_arn = "arn:aws:sns:us-east-1:447089000901:alarm-pings";
        let reentry_a = "arn:aws:sns:us-east-1:447089000901:reentry-a";
        let reentry_b = "arn:aws:sns:us-east-1:447089000901:reentry-b";
        handler.handle(make_req_acct(
            account,
            "PutMetricAlarm",
            json!({
                "AlarmName": "eval-a", "Namespace": "EvalNS", "MetricName": "Hits",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 5.0,
                "AlarmActions": [topic_arn], "OKActions": [topic_arn]
            }),
        ));
        // Second alarm, same namespace, with disabled actions: it must move
        // state without ever delivering.
        handler.handle(make_req_acct(
            account,
            "PutMetricAlarm",
            json!({
                "AlarmName": "eval-b", "Namespace": "EvalNS", "MetricName": "Drops",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 5.0,
                "AlarmActions": [topic_arn]
            }),
        ));
        handler.handle(make_req_acct(
            account,
            "DisableAlarmActions",
            json!({ "AlarmNames": ["eval-b"] }),
        ));
        // Third alarm: default TreatMissingData (missing) with only
        // InsufficientDataActions configured — its metric never receives
        // datapoints, so the first evaluation walks the
        // OK → INSUFFICIENT_DATA arm of the delivery matrix.
        handler.handle(make_req_acct(
            account,
            "PutMetricAlarm",
            json!({
                "AlarmName": "eval-icd", "Namespace": "EvalNS", "MetricName": "NeverPut",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 0.0,
                "InsufficientDataActions": [topic_arn]
            }),
        ));

        let count = |arn: &str, state: &str| {
            deliveries
                .lock()
                .iter()
                .filter(|(a, payload)| {
                    a == arn
                        && serde_json::from_str::<Value>(payload)
                            .ok()
                            .and_then(|e| e["NewStateValue"].as_str().map(String::from))
                            .as_deref()
                            == Some(state)
                })
                .count()
        };
        let envelope_of = |arn: &str, alarm: &str, state: &str| {
            deliveries
                .lock()
                .iter()
                .filter(|(a, _)| a == arn)
                .filter_map(|(_, payload)| serde_json::from_str::<Value>(payload).ok())
                .find(|e| e["AlarmName"] == alarm && e["NewStateValue"] == state)
                .expect("expected delivery envelope not found")
        };

        // Breach: OK → ALARM (+1 AlarmActions), eval-icd walks
        // OK → INSUFFICIENT_DATA (+1 InsufficientDataActions); disabled
        // eval-b moves silently. eval-a's envelope carries the alarm
        // envelope fields the suites parse (AlarmName, NewStateValue,
        // OldStateValue, AWS account, Region).
        handler.handle(make_req_acct(
            account,
            "PutMetricData",
            json!({
                "Namespace": "EvalNS",
                "MetricData": [
                    {"MetricName": "Hits", "Value": 10.0, "Timestamp": ts_of(0)},
                    {"MetricName": "Drops", "Value": 10.0, "Timestamp": ts_of(0)}
                ]
            }),
        ));
        assert_eq!(count(topic_arn, "ALARM"), 1);
        assert_eq!(count(topic_arn, "INSUFFICIENT_DATA"), 1);
        let icd = envelope_of(topic_arn, "eval-icd", "INSUFFICIENT_DATA");
        assert_eq!(icd["OldStateValue"], "OK");
        assert_eq!(
            icd["NewStateReason"],
            "Insufficient Data: 1 datapoint was unknown."
        );
        let first_alarm = envelope_of(topic_arn, "eval-a", "ALARM");
        assert_eq!(first_alarm["OldStateValue"], "OK");
        assert_eq!(first_alarm["AWSAccountId"], json!("447089000901"));
        assert_eq!(first_alarm["Region"], "us-east-1");
        assert!(first_alarm["NewStateReason"]
            .as_str()
            .unwrap()
            .contains("greater than the threshold (5.0)"));
        assert!(first_alarm["StateChangeTime"].as_str().is_some());

        // Same evaluation in the same state: no repeat notification (real
        // CloudWatch notifies once per transition).
        handler.handle(make_req_acct(
            account,
            "PutMetricData",
            json!({
                "Namespace": "EvalNS",
                "MetricData": [{"MetricName": "Hits", "Value": 10.0, "Timestamp": ts_of(61)}]
            }),
        ));
        assert_eq!(deliveries.lock().len(), 2);

        // Recovery: ALARM → OK via OKActions.
        handler.handle(make_req_acct(
            account,
            "PutMetricData",
            json!({
                "Namespace": "EvalNS",
                "MetricData": [{"MetricName": "Hits", "Value": 3.0, "Timestamp": ts_of(122)}]
            }),
        ));
        assert_eq!(count(topic_arn, "OK"), 1);
        let ok_env = envelope_of(topic_arn, "eval-a", "OK");
        assert_eq!(ok_env["OldStateValue"], "ALARM");

        // Still OK: no further delivery.
        handler.handle(make_req_acct(
            account,
            "PutMetricData",
            json!({
                "Namespace": "EvalNS",
                "MetricData": [{"MetricName": "Hits", "Value": 1.0, "Timestamp": ts_of(183)}]
            }),
        ));
        assert_eq!(deliveries.lock().len(), 3);

        // SetAlarmState invokes the transitioned state's actions exactly
        // once per actual transition (API_SetAlarmState). eval-a sits in
        // OK; the forced flip to ALARM delivers AlarmActions again.
        handler.handle(make_req_acct(
            account,
            "SetAlarmState",
            json!({ "AlarmName": "eval-a", "StateValue": "ALARM", "StateReason": "Forced for test" }),
        ));
        assert_eq!(count(topic_arn, "ALARM"), 2);
        let forced = deliveries
            .lock()
            .iter()
            .filter(|(a, _)| a == topic_arn)
            .filter_map(|(_, payload)| serde_json::from_str::<Value>(payload).ok())
            .find(|e| {
                e["AlarmName"] == "eval-a"
                    && e["NewStateValue"] == "ALARM"
                    && e["NewStateReason"] == "Forced for test"
            })
            .expect("forced envelope not found");
        assert_eq!(forced["OldStateValue"], "OK");
        // Same state again: nothing new.
        handler.handle(make_req_acct(
            account,
            "SetAlarmState",
            json!({ "AlarmName": "eval-a", "StateValue": "ALARM", "StateReason": "Same state" }),
        ));
        assert_eq!(count(topic_arn, "ALARM"), 2);
        // And back to OK.
        handler.handle(make_req_acct(
            account,
            "SetAlarmState",
            json!({ "AlarmName": "eval-a", "StateValue": "OK", "StateReason": "Manual OK" }),
        ));
        assert_eq!(count(topic_arn, "OK"), 2);
        assert_eq!(deliveries.lock().len(), 5);

        // ---- re-entrant action + stale-notification regression ----------
        // "reentrant" alarms two SNS ARNs. ARN A's hook re-enters the twin
        // synchronously (a PutMetricData that transitions the alarm
        // ALARM → OK); the outer pass must not deadlock on the alarm store
        // (the guards are out of scope by then) and ARN B's now-stale ALARM
        // notification must be skipped — ARN B still fires its own nested OK
        // notification.
        handler.handle(make_req_acct(
            account,
            "PutMetricAlarm",
            json!({
                "AlarmName": "reentrant", "Namespace": "ReentrantNS", "MetricName": "R",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 5.0,
                "TreatMissingData": "notBreaching",
                "AlarmActions": [reentry_a, reentry_b], "OKActions": [reentry_a, reentry_b]
            }),
        ));
        handler.handle(make_req_acct(
            account,
            "PutMetricData",
            json!({
                "Namespace": "ReentrantNS",
                "MetricData": [{"MetricName": "R", "Value": 10.0, "Timestamp": ts_of(0)}]
            }),
        ));
        assert_eq!(count(reentry_a, "ALARM"), 1);
        assert_eq!(count(reentry_b, "ALARM"), 1);
        // Drop back to OK with a fresh window, then arm the hook's re-entry
        // with a nested recovery put and breach again from the outer call.
        handler.handle(make_req_acct(
            account,
            "PutMetricData",
            json!({
                "Namespace": "ReentrantNS",
                "MetricData": [{"MetricName": "R", "Value": 3.0, "Timestamp": ts_of(61)}]
            }),
        ));
        assert_eq!(count(reentry_a, "OK"), 1);
        assert_eq!(count(reentry_b, "OK"), 1);
        *armed.lock() = Some((
            Arc::clone(&handler),
            make_req_acct(
                account,
                "PutMetricData",
                json!({
                    "Namespace": "ReentrantNS",
                    "MetricData": [{"MetricName": "R", "Value": 3.0, "Timestamp": ts_of(250)}]
                }),
            ),
        ));
        handler.handle(make_req_acct(
            account,
            "PutMetricData",
            json!({
                "Namespace": "ReentrantNS",
                "MetricData": [{"MetricName": "R", "Value": 10.0, "Timestamp": ts_of(122)}]
            }),
        ));
        // Outer OK → ALARM staged and delivered (ARN A publishes its copy
        // before the nested transition happens), the nested hook re-entry
        // flipped the alarm to OK and delivered both ARNs' nested
        // OKActions, and ARN B's stale ALARM copy got skipped by the
        // freshness gate.
        assert_eq!(count(reentry_a, "ALARM"), 2, "ARN A: breach + in-flight");
        assert_eq!(
            count(reentry_a, "OK"),
            2,
            "ARN A: recovery prior to re-entry + nested delivery"
        );
        assert_eq!(
            count(reentry_b, "ALARM"),
            1,
            "ARN B: the stale re-notification must be skipped, not delivered"
        );
        assert_eq!(
            count(reentry_b, "OK"),
            2,
            "ARN B: recovery prior to re-entry + nested delivery"
        );
        let desc = parse(&handler.handle(make_req_acct(
            account,
            "DescribeAlarms",
            json!({ "AlarmNames": ["reentrant"], "AlarmTypes": ["MetricAlarm"] }),
        )));
        assert_eq!(
            desc["MetricAlarms"][0]["StateValue"], "OK",
            "the alarm must settle on the nested transition's state"
        );
    }

    /// The alarm's own dimension set narrows evaluation the way
    /// GetMetricStatistics filters: a candidate datapoint matches when its
    /// dimension set CONTAINS the alarm's dimensions (extra splits on the
    /// datum are fine, missing ones are not). The alarm fires only on its
    /// own set: a sibling split of the same metric never moves it, a
    /// dimension-less alarm in the same namespace still matches everything.
    #[test]
    fn alarm_evaluation_respects_the_alarm_dimension_set() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "dim-web", "Namespace": "DimsNS", "MetricName": "Hits",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 5.0,
                "Dimensions": [{"Name": "Service", "Value": "web"}]
            }),
        ));
        // Dimension-less sibling alarm watching the same metric + namespace.
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "dim-all", "Namespace": "DimsNS", "MetricName": "Hits",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 5.0
            }),
        ));
        // Only the db split breaches — the dimensioned alarm must not see it.
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "DimsNS",
                "MetricData": [
                    {"MetricName": "Hits", "Value": 10.0, "Timestamp": ts_of(0),
                     "Dimensions": [{"Name": "Service", "Value": "db"}]}
                ]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({"AlarmNames": ["dim-web", "dim-all"]}),
        )));
        let alarms = desc["MetricAlarms"].as_array().unwrap();
        let by_name = |name: &str| {
            alarms
                .iter()
                .find(|a| a["AlarmName"] == name)
                .unwrap()
                .clone()
        };
        let dim_all = by_name("dim-all");
        assert_eq!(
            dim_all["StateValue"], "ALARM",
            "the dimension-less alarm keeps namespace+metric matching"
        );
        let dim_web = by_name("dim-web");
        assert!(
            dim_web["StateValue"] != "ALARM",
            "a sibling split of the same metric must not breach the alarm: {}",
            dim_web
        );

        // Its own split (+ an extra dimension on the datum, which the real
        // dimension-subset semantics allow) breaches it.
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "DimsNS",
                "MetricData": [
                    {"MetricName": "Hits", "Value": 10.0, "Timestamp": ts_of(61),
                     "Dimensions": [
                        {"Name": "Service", "Value": "web"},
                        {"Name": "api", "Value": "one"}
                     ]}
                ]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({"AlarmNames": ["dim-web"]}),
        )));
        assert_eq!(
            desc["MetricAlarms"][0]["StateValue"], "ALARM",
            "the alarm fires on its own dimension set"
        );
        assert!(
            desc["MetricAlarms"][0]["StateReason"]
                .as_str()
                .unwrap()
                .contains("greater than the threshold"),
            "reason: {}",
            desc["MetricAlarms"][0]["StateReason"]
        );
    }

    /// TreatMissingData=breaching: an alarm whose metric produced no
    /// datapoints at all evaluates as if the missing datapoints were
    /// breaching (moto keeps alarms static; this is the real CloudWatch
    /// TreatMissingData rule).
    #[test]
    fn breaching_treat_missing_data_transitions_without_datapoints() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "ghost-breacher", "Namespace": "MissingNS", "MetricName": "Ghost",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 0.0,
                "TreatMissingData": "breaching"
            }),
        ));
        // Some other metric in the same namespace drives the evaluation.
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "MissingNS",
                "MetricData": [{"MetricName": "Other", "Value": 1.0}]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["ghost-breacher"] }),
        )));
        let alarm = &desc["MetricAlarms"][0];
        assert_eq!(alarm["StateValue"], "ALARM");
        assert!(alarm["StateReason"]
            .as_str()
            .unwrap()
            .contains("treated as breaching"));
    }

    /// TreatMissingData=ignore ("The current alarm state is maintained." —
    /// archived AlarmThatSendsEmail docs): any missing datapoint in the
    /// evaluation range holds the alarm in its current state, even when the
    /// datapoints that do exist would breach — evaluating the good windows
    /// alone would turn a breaching gap into a transition.
    #[test]
    fn ignore_treat_missing_data_keeps_state_without_datapoints() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "ghost-ignorer", "Namespace": "MissingIgnoredNS", "MetricName": "Ghost",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 2,
                "Period": 60, "Statistic": "Sum", "Threshold": 0.0,
                "TreatMissingData": "ignore"
            }),
        ));
        // One breaching datapoint in a 2-window range: ignore holds the
        // initial state instead of evaluating the good window alone.
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "MissingIgnoredNS",
                "MetricData": [{"MetricName": "Ghost", "Value": 10.0, "Timestamp": ts_of(0)}]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["ghost-ignorer"] }),
        )));
        let alarm = &desc["MetricAlarms"][0];
        assert_eq!(alarm["StateValue"], "OK");
        assert_eq!(alarm["StateReason"], "Unchecked: Initial alarm creation");

        // A metric that never produced datapoints at all holds too.
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "ghost-ignorer-2", "Namespace": "MissingIgnoredNS", "MetricName": "NeverAsked",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 0.0,
                "TreatMissingData": "ignore"
            }),
        ));
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "MissingIgnoredNS",
                "MetricData": [{"MetricName": "Other", "Value": 1.0}]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["ghost-ignorer-2"] }),
        )));
        let alarm = &desc["MetricAlarms"][0];
        assert_eq!(alarm["StateValue"], "OK");
        assert_eq!(alarm["StateReason"], "Unchecked: Initial alarm creation");
    }

    /// The (unset) default TreatMissingData=missing: short datapoint data
    /// moves the alarm to INSUFFICIENT_DATA.
    #[test]
    fn default_treat_missing_transitions_to_insufficient_data() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "default-missing", "Namespace": "MissingDefaultNS", "MetricName": "Ghost",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 0.0
            }),
        ));
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "MissingDefaultNS",
                "MetricData": [{"MetricName": "Other", "Value": 1.0}]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["default-missing"] }),
        )));
        assert_eq!(desc["MetricAlarms"][0]["StateValue"], "INSUFFICIENT_DATA");
    }

    /// DatapointsToAlarm shortens the breach-set decision: with M=1 of
    /// EvaluationPeriods=2 a single fresh breaching window is enough, while
    /// the default (M=EvaluationPeriods) requires every window to breach.
    #[test]
    fn datapoints_to_alarm_decides_m_of_n_breach() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "m1", "Namespace": "MoN", "MetricName": "Tw",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 2,
                "DatapointsToAlarm": 1, "Period": 60, "Statistic": "Sum",
                "Threshold": 5.0, "TreatMissingData": "notBreaching"
            }),
        ));
        // The two trailing windows carry [10 (newest), 10] — both modes alarm.
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "MoN",
                "MetricData": [{"MetricName": "Tw", "Value": 10.0, "Timestamp": ts_of(0)}]
            }),
        ));
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "MoN",
                "MetricData": [{"MetricName": "Tw", "Value": 10.0, "Timestamp": ts_of(61)}]
            }),
        ));
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "m2", "Namespace": "MoN", "MetricName": "Tw",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 2,
                "Period": 60, "Statistic": "Sum",
                "Threshold": 5.0, "TreatMissingData": "notBreaching"
            }),
        ));
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "MoN",
                "MetricData": [{"MetricName": "Tw", "Value": 10.0, "Timestamp": ts_of(61)}]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["m1", "m2"] }),
        )));
        let alarms = desc["MetricAlarms"].as_array().unwrap();
        let state_of = |name: &str| {
            alarms.iter().find(|x| x["AlarmName"] == name).unwrap()["StateValue"].clone()
        };
        assert_eq!(state_of("m1"), "ALARM");
        assert_eq!(state_of("m2"), "ALARM", "both windows breaching → ALARM");

        // Newest window goes quiet — real semantics keep M=1 in ALARM whenever
        // any of the N trailing windows breaches (a count, not a
        // newest-window check); the default (M=2 of 2) drops to OK because
        // only 1 of 2 datapoints is breaching (clear_needed = N - M + 1 = 1).
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "MoN",
                "MetricData": [{"MetricName": "Tw", "Value": 0.0, "Timestamp": ts_of(122)}]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["m1", "m2"] }),
        )));
        let alarms = desc["MetricAlarms"].as_array().unwrap();
        let state_of = |name: &str| {
            alarms.iter().find(|x| x["AlarmName"] == name).unwrap()["StateValue"].clone()
        };
        assert_eq!(
            state_of("m1"),
            "ALARM",
            "M=1 holds ALARM while any window breaches"
        );
        assert_eq!(
            state_of("m2"),
            "OK",
            "M=2 of 2 needs every window breaching"
        );
    }

    /// The real CloudWatch M-of-N rule counts breaching datapoints across the
    /// whole trailing range — windows [10, 0, 10] with M=2/N=3 breach (the
    /// counter-example from the review: a "M newest good windows ALL
    /// breaching" streak rule answered OK there), and the same rule fills the
    /// breach side from missing windows treated as breaching.
    #[test]
    fn datapoints_count_across_the_whole_range() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "mont", "Namespace": "MoNCountA", "MetricName": "Tw",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 3,
                "DatapointsToAlarm": 2, "Period": 60, "Statistic": "Average",
                "Threshold": 5.0, "TreatMissingData": "notBreaching"
            }),
        ));
        // Trailing three windows arrive newest-first as [10, 0, 10]. The
        // stamps are placed inside each (start, end] bucket of Period=60
        // ending at the anchor (the newest stamp).
        for (secs, value) in [(120, 10.0), (30, 0.0), (-30, 10.0)] {
            handler.handle(make_req(
                "PutMetricData",
                json!({
                    "Namespace": "MoNCountA",
                    "MetricData": [{"MetricName": "Tw", "Value": value, "Timestamp": ts_of(secs)}]
                }),
            ));
        }
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["mont"] }),
        )));
        assert_eq!(desc["MetricAlarms"][0]["StateValue"], "ALARM");

        // The breaching-count side of TreatMissingData: one breaching window
        // plus one missing treated as breaching completes M=2 — the old
        // all-good-windows-breach rule answered OK here.
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "mnbr", "Namespace": "MoNCountB", "MetricName": "Tw",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 3,
                "DatapointsToAlarm": 2, "Period": 60, "Statistic": "Average",
                "Threshold": 5.0, "TreatMissingData": "breaching"
            }),
        ));
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "MoNCountB",
                "MetricData": [{"MetricName": "Tw", "Value": 10.0, "Timestamp": ts_of(1)}]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["mnbr"] }),
        )));
        assert_eq!(
            desc["MetricAlarms"][0]["StateValue"], "ALARM",
            "1 breaching + 1 treated-as-breaching missing fills M=2"
        );

        // And two clean windows (plus one missing treated as notBreaching)
        // clear the M=2 count to OK.
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "mclr", "Namespace": "MoNCountC", "MetricName": "Tw",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 3,
                "DatapointsToAlarm": 2, "Period": 60, "Statistic": "Average",
                "Threshold": 5.0, "TreatMissingData": "notBreaching"
            }),
        ));
        for secs in [120, 60, 1] {
            handler.handle(make_req(
                "PutMetricData",
                json!({
                    "Namespace": "MoNCountC",
                    "MetricData": [{"MetricName": "Tw", "Value": 1.0, "Timestamp": ts_of(secs)}]
                }),
            ));
        }
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["mclr"] }),
        )));
        assert_eq!(desc["MetricAlarms"][0]["StateValue"], "OK");
    }

    /// Query-protocol-shaped PutMetricAlarm: integer members arrive as
    /// strings on the query wire (protocol.rs stores scalars as
    /// Value::String), so Period / EvaluationPeriods / DatapointsToAlarm
    /// must parse string values — a bare `Value::as_i64` silently degraded
    /// them to the 1-of-1 defaults. The evaluation answers prove the parsed
    /// config: one breach of a 3-window range with DatapointsToAlarm=2 sits
    /// INSUFFICIENT_DATA (default "missing" treat) instead of going ALARM,
    /// and the second breach completes the 2-of-3 set.
    #[test]
    fn query_protocol_string_integers_configure_the_alarm() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "qproto", "Namespace": "QProtoNS", "MetricName": "Q",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": "3",
                "DatapointsToAlarm": "2", "Period": "60", "Statistic": "Sum",
                "Threshold": "5.0", "TreatMissingData": "missing"
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["qproto"] }),
        )));
        let alarm = &desc["MetricAlarms"][0];
        assert_eq!(alarm["EvaluationPeriods"], 3, "string \"3\" must parse");
        assert_eq!(alarm["DatapointsToAlarm"], 2, "string \"2\" must parse");
        assert_eq!(alarm["Period"], 60, "string \"60\" must parse");
        assert_eq!(alarm["Threshold"], 5.0, "string \"5.0\" must parse");

        // One breach window: 1 good short of the 2-window breach-set, treat
        // "missing" → INSUFFICIENT_DATA. A 1-of-1 degraded config would have
        // answered ALARM here.
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "QProtoNS",
                "MetricData": [{"MetricName": "Q", "Value": 10.0, "Timestamp": ts_of(0)}]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["qproto"] }),
        )));
        assert_eq!(desc["MetricAlarms"][0]["StateValue"], "INSUFFICIENT_DATA");
        assert_eq!(
            desc["MetricAlarms"][0]["StateReason"],
            "Insufficient Data: 2 datapoints were unknown."
        );

        // The second breach completes the 2-of-3 breach-set → ALARM.
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "QProtoNS",
                "MetricData": [{"MetricName": "Q", "Value": 10.0, "Timestamp": ts_of(61)}]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["qproto"] }),
        )));
        assert_eq!(desc["MetricAlarms"][0]["StateValue"], "ALARM");
    }

    /// Alarm evaluation candidates match with the GetMetricStatistics
    /// subset predicate: a datapoint's dimension set must CONTAIN the
    /// alarm's dimension set (this alarm's datapoint carries the exact
    /// set; the sibling-split and dimension-less datapoints are covered by
    /// `alarm_evaluation_respects_the_alarm_dimension_set`).
    #[test]
    fn dimensioned_alarms_evaluate_on_namespace_and_metric_name() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "dim-alarm", "Namespace": "DimNS", "MetricName": "ErrorRate",
                "Dimensions": [{"Name": "Service", "Value": "api"}],
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Average", "Threshold": 50.0
            }),
        ));
        handler.handle(make_req(
            "PutMetricData",
            json!({
                "Namespace": "DimNS",
                "MetricData": [{
                    "MetricName": "ErrorRate", "Value": 75.0,
                    "Dimensions": [{"Name": "Service", "Value": "api"}]
                }]
            }),
        ));
        let desc = parse(&handler.handle(make_req(
            "DescribeAlarms",
            json!({ "AlarmNames": ["dim-alarm"] }),
        )));
        assert_eq!(desc["MetricAlarms"][0]["StateValue"], "ALARM");
    }
}
