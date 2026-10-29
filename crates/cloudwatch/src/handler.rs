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
        if !names.is_empty() {
            // Scalar fields alongside a list: rebuild the object wrapper.
            let mut obj = Map::new();
            for name in &names {
                if let Some(v) = unflatten(params, &format!("{prefix}.{name}")) {
                    obj.insert(name.clone(), v);
                }
            }
            // Not reachable for well-formed request keys: the list *is* the prefix value.
            return Some(Value::Array(arr));
        }
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

/// De-duplication key for ListMetrics: (namespace, metric name, dimensions).
type MetricSet = Vec<(String, String, Vec<(String, String)>)>;

/// One good (data-bearing) evaluation window of a metric alarm.
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
/// "missing" period). Namespace + metric-name matching only — moto's alarm
/// evaluation filters `metric_data` by namespace and name and ignores
/// dimension matching (`MetricDatumBase::filter` with no dimension arg).
fn alarm_window_stat(
    metrics: &[MetricDatum],
    namespace: &str,
    metric_name: &str,
    stat: &str,
    start_ms: i64,
    end_ms: i64,
) -> Option<f64> {
    let mut values: Vec<f64> = Vec::new();
    let mut aggs: Vec<DatumStats> = Vec::new();
    for md in metrics {
        if md.namespace == namespace
            && md.name == metric_name
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
        reason.push_str(&format!(" {missing} missing datapoint{munit} treated as {treat}."));
    }
    reason
}

/// (state, NewStateReason) this alarm would move to given the current
/// datapoint store, evaluated over the trailing `EvaluationPeriods` windows
/// of `Period` seconds ending at `now_ms`. `None` when the alarm cannot be
/// evaluated (metric-math alarms, unknown statistics) or — with
/// `TreatMissingData=ignore` and no datapoints at all — must not move: real
/// CloudWatch keeps such an alarm in its existing state.
///
/// Window semantics for `(state, reason)` (TreatMissingData handling per the
/// real CloudWatch alarm rules; moto 5.2.4 has no evaluation code at all —
/// `Alarm` in moto/cloudwatch/models.py only ever stores state):
/// - `EvaluationPeriods` windows, `DatapointsToAlarm` (default =
///   EvaluationPeriods) of the most recent *good* windows decide the state:
///   all breaching → ALARM, else OK.
/// - Fewer good windows than that: missing windows count as
///   "breaching"/"notBreaching" per TreatMissingData, "missing" (the boto3
///   default) transitions to INSUFFICIENT_DATA, and "ignore" evaluates the
///   good windows as if the missing ones were removed.
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
        return None;
    }
    let threshold = alarm["Threshold"].as_f64().unwrap_or(0.0);
    let period_ms = alarm["Period"].as_i64().unwrap_or(60).max(1) * 1000;
    let eval_periods = alarm["EvaluationPeriods"].as_i64().unwrap_or(1).max(1) as usize;
    let datapoints_to_alarm = alarm["DatapointsToAlarm"]
        .as_i64()
        .map(|m| m.max(1) as usize)
        .unwrap_or(eval_periods);
    let treat = field_str_or(alarm, "TreatMissingData", "missing");

    // Anchor at max(now, the newest datapoint for the metric): caller-supplied
    // Timestamps that run ahead of the evaluator's clock would otherwise fall
    // outside every trailing window and read as missing at the exact moment
    // they should breach. Windows are (start, end] so a datapoint stamped at
    // the anchor point lands in the newest window.
    let newest = metrics
        .iter()
        .filter(|md| md.namespace == namespace && md.name == metric_name)
        .map(|md| md.timestamp)
        .max()
        .unwrap_or(i64::MIN);
    let anchor = now_ms.max(newest);

    // Newest window first.
    let mut good: Vec<AlarmWindow> = Vec::new();
    for i in 0..eval_periods {
        let end = anchor - (i as i64) * period_ms;
        let start = end - period_ms;
        if let Some(v) = alarm_window_stat(metrics, &namespace, &metric_name, &stat, start, end) {
            good.push(AlarmWindow {
                value: v,
                timestamp: end,
            });
        }
    }

    let phrase = comparison_phrase(&operator);
    let threshold_str = reason_value(threshold);
    let missing = eval_periods - good.len();
    let (state, reason) = if good.len() >= datapoints_to_alarm {
        // The M most recent good windows decide — older breaches age out.
        let recent = &good[..datapoints_to_alarm];
        let crossed = recent
            .iter()
            .all(|w| is_breaching(w.value, threshold, &operator));
        (
            if crossed { "ALARM" } else { "OK" },
            threshold_reason(recent, crossed, phrase, &threshold_str, missing, &treat),
        )
    } else {
        match treat.as_str() {
            // Missing windows pad the breach side of the M required: the
            // alarm enters ALARM when every good window breaches (missing
            // windows breach by rule).
            "breaching" => {
                let crossed = good
                    .iter()
                    .all(|w| is_breaching(w.value, threshold, &operator));
                if crossed {
                    let reason = if good.is_empty() {
                        let unit = if missing == 1 { "" } else { "s" };
                        format!(
                            "Threshold Crossed: {missing} missing datapoint{unit} treated as breaching."
                        )
                    } else {
                        threshold_reason(&good, crossed, phrase, &threshold_str, missing, &treat)
                    };
                    ("ALARM", reason)
                } else {
                    (
                        "OK",
                        threshold_reason(&good, crossed, phrase, &threshold_str, missing, &treat),
                    )
                }
            }
            // Missing windows count as not-breaching, which can never
            // complete an M-breach when the good windows are short.
            "notBreaching" => {
                if good.is_empty() {
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
                        threshold_reason(&good, false, phrase, &threshold_str, missing, &treat),
                    )
                }
            }
            // Missing windows are removed from the range of evaluation; with
            // no datapoints at all the alarm is not evaluated (its state
            // stays where it is — the None answer below).
            "ignore" => {
                if good.is_empty() {
                    return None;
                }
                let crossed = good
                    .iter()
                    .all(|w| is_breaching(w.value, threshold, &operator));
                (
                    if crossed { "ALARM" } else { "OK" },
                    threshold_reason(&good, crossed, phrase, &threshold_str, 0, &treat),
                )
            }
            // "missing" (and anything unmapped — boto3's default): good
            // windows short of the required count → INSUFFICIENT_DATA.
            _ => {
                let unit = if missing == 1 { "" } else { "s" };
                (
                    "INSUFFICIENT_DATA",
                    format!("Insufficient Data: {missing} datapoint{unit} were unknown."),
                )
            }
        }
    };
    Some((state.to_string(), reason))
}

/// The moto-style alarm state envelope the twin publishes on every state
/// transition — the SNS/Lambda payload with the AlarmName, AlarmDescription,
/// AWSAccountId, NewStateValue, NewStateReason, StateChangeTime, Region and
/// OldStateValue fields the alarm consumers parse (`NewStateValue` gates
/// "ALARM"/"OK" notifications like moto's documented alarm envelope).
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

/// One MetricDataQuery MetricStat source for GetMetricData collection.
struct MetricQuerySource {
    namespace: String,
    metric_name: String,
    dimensions: Vec<(String, String)>,
    period: i64,
    stat: String,
}

impl MetricQuerySource {
    fn dims_ok(datum: &[(String, String)], expected: &[(String, String)]) -> bool {
        expected.iter().all(|f| datum.contains(f))
    }

    fn collect(&self, metrics: &[MetricDatum], out: &mut BTreeMap<String, Value>) {
        let mut filtered: Vec<&MetricDatum> = metrics
            .iter()
            .filter(|md| {
                md.namespace == self.namespace
                    && md.name == self.metric_name
                    && Self::dims_ok(&md.dimensions, &self.dimensions)
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
            self.metric_key(),
            json!({ "Values": values, "Timestamps": timestamps }),
        );
    }

    fn metric_key(&self) -> String {
        format!("{}/{}", self.namespace, self.metric_name)
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
            other => AwsResponse::error(
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
                for (i, v) in values.iter().enumerate() {
                    let count = counts_v
                        .and_then(|c| c.get(i))
                        .and_then(as_f64)
                        .unwrap_or(1.0) as usize;
                    for _ in 0..count.max(1) {
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
    /// because a Lambda action may call back into this handler on this
    /// thread. moto's own backend never evaluates alarms (5.2.4 keeps
    /// `Alarm.state_value` untouched — see models.py/update_state), so the
    /// twin's transitions follow the real CloudWatch alarm rules above.
    fn evaluate_alarms(&self, account: u64, region: &str, namespaces: &[String]) {
        let state = self.get_state(account, region);
        let mut deliveries: Vec<(Vec<String>, Value)> = Vec::new();
        {
            let metrics = state.metrics.read();
            let mut alarms = state.alarms.write();
            for alarm in alarms.values_mut() {
                let ns = field_str(alarm, "Namespace");
                if !namespaces.contains(&ns) {
                    continue;
                }
                let Some((target, reason)) = alarm_candidate(&metrics, alarm, now_millis())
                else {
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
                    deliveries.push((arns, message));
                }
            }
        }
        for (arns, message) in &deliveries {
            crate::bridge::deliver_actions(arns, message, account, region);
        }
    }

    fn dims_subset(datum: &[(String, String)], filter: &[(String, String)]) -> bool {
        filter.iter().all(|f| datum.contains(f))
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
        let is_query =
            !req.body.is_empty() && String::from_utf8_lossy(req.body.as_ref()).contains("Action=");
        if is_query {
            AwsResponse::query_error(status, code, message)
        } else {
            AwsResponse::error(status, code, message)
        }
    }

    fn get_metric_statistics(&self, req: &AwsRequest) -> AwsResponse {
        let namespace = str_param(&req.params, "Namespace");
        let metric_name = str_param(&req.params, "MetricName");
        let start = Self::ts_param(&req.params, "StartTime");
        let end = Self::ts_param(&req.params, "EndTime");
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
            };
            source.collect(&metrics, &mut collected);
        }

        // Second pass: emit results (evaluating expressions).
        let mut results: Vec<Value> = Vec::new();
        for q in &queries {
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
            return AwsResponse::error(400, "ValidationError", "AlarmName is required");
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
            "StateValue": "OK",
            "StateReason": "Unchecked: Initial alarm creation",
            "StateReasonData": "{}",
            "StateUpdatedTimestamp": now,
            "MetricName": str_param(&req.params, "MetricName"),
            "Namespace": str_param(&req.params, "Namespace"),
            "Dimensions": req.params.get("Dimensions").unwrap_or(&json!([])).clone(),
            "Period": req.params.get("Period").and_then(|v| v.as_i64()).unwrap_or(60),
            "EvaluationPeriods": req.params.get("EvaluationPeriods").and_then(|v| v.as_i64()).unwrap_or(1),
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
        if let Some(dta) = req.params.get("DatapointsToAlarm").and_then(|v| v.as_i64()) {
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
        let mut alarms = state.alarms.write();
        let mut composites = state.composite_alarms.write();
        let alarm = alarms.get_mut(&name).or_else(|| composites.get_mut(&name));
        let mut delivery: Option<(Vec<String>, Value)> = None;
        let response = match alarm {
            Some(alarm) => {
                let value =
                    as_str(req.params.get("StateValue")).unwrap_or_else(|| "OK".to_string());
                if value != "OK" && value != "ALARM" && value != "INSUFFICIENT_DATA" {
                    return AwsResponse::error(400, "ValidationError",
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
                // SetAlarmState invokes the alarm's actions for the new state
                // when the state actually differs ("When the updated state
                // differs from the previous value, the action configured for
                // the appropriate state is invoked" — API_SetAlarmState).
                if value != old_state && alarm["ActionsEnabled"].as_bool().unwrap_or(true) {
                    let reason = field_str(alarm, "StateReason");
                    delivery = state_notification(
                        alarm, &old_state, &value, &reason, &req.region, req.account,
                    );
                }
                AwsResponse::json(200, json!({}))
            }
            None => AwsResponse::error(
                400,
                "ResourceNotFoundException",
                &format!("Alarm {name} not found"),
            ),
        };
        // Deliver with the alarm-state locks released (see evaluate_alarms).
        if let Some((arns, message)) = delivery {
            crate::bridge::deliver_actions(&arns, &message, req.account, &req.region);
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
            return AwsResponse::error(400, "ValidationError", "AlarmName is required");
        }
        let rule = str_param(&req.params, "AlarmRule");
        if rule.is_empty() {
            return AwsResponse::error(400, "ValidationError", "AlarmRule is required");
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
            return AwsResponse::error(
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
            "Size": body.len().to_string(),
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
            None => AwsResponse::error(
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
                    "Size": d.get("Size").cloned().unwrap_or(json!("0"))
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
                return AwsResponse::error(
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
            return AwsResponse::error(400, "ValidationError", "RuleName is required");
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
            return AwsResponse::error(400, "ValidationError", "Name is required");
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
            None => AwsResponse::error(
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
            return AwsResponse::error(400, "ValidationError", "Name is required");
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
            None => AwsResponse::error(
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
    /// disabled actions update state silently, and SetAlarmState invokes the
    /// state's actions exactly once per transition. The alarm-action hook
    /// registry is process-global, so every delivery assertion lives in this
    /// single test (sole-registrant contract like the kinesis bridge test).
    #[test]
    fn alarm_evaluation_notifies_once_per_transition() {
        use std::sync::Arc;

        crate::bridge::reset_alarm_actions();
        let deliveries: Arc<parking_lot::Mutex<Vec<(String, String)>>> = Arc::default();
        let recorded = Arc::clone(&deliveries);
        crate::bridge::register_alarm_action(
            "sns",
            Arc::new(move |arn: &str, payload: &str, _account: u64, _region: &str| {
                recorded.lock().push((arn.to_string(), payload.to_string()));
                Ok(())
            }),
        );

        let handler = CloudwatchHandler::new();
        let account = 447089000901u64;
        let topic_arn = "arn:aws:sns:us-east-1:447089000901:alarm-pings";
        handler.handle(make_req_acct(account, 
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
        handler.handle(make_req_acct(account, 
            "PutMetricAlarm",
            json!({
                "AlarmName": "eval-b", "Namespace": "EvalNS", "MetricName": "Drops",
                "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "Period": 60, "Statistic": "Sum", "Threshold": 5.0,
                "AlarmActions": [topic_arn]
            }),
        ));
        handler.handle(make_req_acct(account, 
            "DisableAlarmActions",
            json!({ "AlarmNames": ["eval-b"] }),
        ));

        // Breach: OK → ALARM and one AlarmActions delivery with the alarm
        // envelope fields (AlarmName, NewStateValue, OldStateValue, AWS…
        // account, Region).
        handler.handle(make_req_acct(account, 
            "PutMetricData",
            json!({
                "Namespace": "EvalNS",
                "MetricData": [
                    {"MetricName": "Hits", "Value": 10.0, "Timestamp": ts_of(0)},
                    {"MetricName": "Drops", "Value": 10.0, "Timestamp": ts_of(0)}
                ]
            }),
        ));
        let desc = parse(&handler.handle(make_req_acct(account, 
            "DescribeAlarms",
            json!({ "AlarmNames": ["eval-a", "eval-b"], "AlarmTypes": ["MetricAlarm"] }),
        )));
        let alarms = desc["MetricAlarms"].as_array().unwrap();
        let a = alarms.iter().find(|x| x["AlarmName"] == "eval-a").unwrap();
        let b = alarms.iter().find(|x| x["AlarmName"] == "eval-b").unwrap();
        assert_eq!(a["StateValue"], "ALARM");
        assert!(a["StateReason"].as_str().unwrap().contains("greater than the threshold (5.0)"));
        assert_eq!(b["StateValue"], "ALARM");

        let records = deliveries.lock();
        assert_eq!(records.len(), 1, "one transition → one delivery: {records:?}");
        let (arn, payload) = &records[0];
        assert_eq!(arn.as_str(), topic_arn);
        let envelope: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(envelope["NewStateValue"], "ALARM");
        assert_eq!(envelope["OldStateValue"], "OK");
        assert_eq!(envelope["AlarmName"], "eval-a");
        assert_eq!(envelope["AWSAccountId"], json!("447089000901"));
        assert_eq!(envelope["Region"], "us-east-1");
        assert!(envelope["NewStateReason"].as_str().is_some());
        assert!(envelope["StateChangeTime"].as_str().is_some());
        drop(records);

        // Same evaluation in the same state: no repeat notification (real
        // CloudWatch notifies once per transition).
        handler.handle(make_req_acct(account, 
            "PutMetricData",
            json!({
                "Namespace": "EvalNS",
                "MetricData": [{"MetricName": "Hits", "Value": 10.0, "Timestamp": ts_of(61)}]
            }),
        ));
        assert_eq!(deliveries.lock().len(), 1);

        // Recovery: ALARM → OK via OKActions.
        handler.handle(make_req_acct(account, 
            "PutMetricData",
            json!({
                "Namespace": "EvalNS",
                "MetricData": [{"MetricName": "Hits", "Value": 3.0, "Timestamp": ts_of(122)}]
            }),
        ));
        {
            let records = deliveries.lock();
            assert_eq!(records.len(), 2, "recovery delivers OKActions: {records:?}");
            let (_, payload) = &records[1];
            let envelope: Value = serde_json::from_str(payload).unwrap();
            assert_eq!(envelope["NewStateValue"], "OK");
            assert_eq!(envelope["OldStateValue"], "ALARM");
        }

        // Still OK: no further delivery.
        handler.handle(make_req_acct(account, 
            "PutMetricData",
            json!({
                "Namespace": "EvalNS",
                "MetricData": [{"MetricName": "Hits", "Value": 1.0, "Timestamp": ts_of(183)}]
            }),
        ));
        assert_eq!(deliveries.lock().len(), 2);

        // SetAlarmState invokes the transitioned state's actions exactly
        // once per actual transition (API_SetAlarmState). eval-a sits in
        // OK; the forced flip to ALARM delivers AlarmActions again.
        handler.handle(make_req_acct(account, 
            "SetAlarmState",
            json!({ "AlarmName": "eval-a", "StateValue": "ALARM", "StateReason": "Forced for test" }),
        ));
        {
            let records = deliveries.lock();
            assert_eq!(records.len(), 3);
            let (_, payload) = &records[2];
            let envelope: Value = serde_json::from_str(payload).unwrap();
            assert_eq!(envelope["NewStateValue"], "ALARM");
            assert_eq!(envelope["OldStateValue"], "OK");
            assert_eq!(envelope["NewStateReason"], "Forced for test");
        }
        // Same state again: nothing new.
        handler.handle(make_req_acct(account, 
            "SetAlarmState",
            json!({ "AlarmName": "eval-a", "StateValue": "ALARM", "StateReason": "Same state" }),
        ));
        assert_eq!(deliveries.lock().len(), 3);
        // And back to OK.
        handler.handle(make_req_acct(account, 
            "SetAlarmState",
            json!({ "AlarmName": "eval-a", "StateValue": "OK", "StateReason": "Manual OK" }),
        ));
        {
            let records = deliveries.lock();
            assert_eq!(records.len(), 4);
            let envelope: Value = serde_json::from_str(&records[3].1).unwrap();
            assert_eq!(envelope["NewStateValue"], "OK");
        }
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
        assert!(alarm["StateReason"].as_str().unwrap().contains("treated as breaching"));
    }

    /// TreatMissingData=ignore (and ignore-with-zero-datapoints) leaves the
    /// alarm in its current state — the only treat mode where evaluation
    /// declines to move it.
    #[test]
    fn ignore_treat_missing_data_keeps_state_without_datapoints() {
        let handler = CloudwatchHandler::new();
        handler.handle(make_req(
            "PutMetricAlarm",
            json!({
                "AlarmName": "ghost-ignorer", "Namespace": "MissingIgnoredNS", "MetricName": "Ghost",
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
            json!({ "AlarmNames": ["ghost-ignorer"] }),
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
            alarms
                .iter()
                .find(|x| x["AlarmName"] == name)
                .unwrap()["StateValue"]
                .clone()
        };
        assert_eq!(state_of("m1"), "ALARM");
        assert_eq!(state_of("m2"), "ALARM", "both windows breaching → ALARM");

        // Newest window goes quiet: M=1 alarms on the newest good window
        // alone; the default (M=2) needs both and drops to OK.
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
            alarms
                .iter()
                .find(|x| x["AlarmName"] == name)
                .unwrap()["StateValue"]
                .clone()
        };
        assert_eq!(state_of("m1"), "OK", "M=1 follows the newest window");
        assert_eq!(state_of("m2"), "OK", "M=2 needs every window breaching");
    }

    /// Alarm evaluation matches on namespace + metric name like moto —
    /// dimensioned alarms still evaluate dimensioned datapoints.
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
