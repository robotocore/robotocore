//! CloudWatch in-memory state models.
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Statistic values from PutMetricData (StatisticValues parameter).
#[derive(Debug, Clone, Copy)]
pub struct DatumStats {
    pub sample_count: f64,
    pub sum: f64,
    pub minimum: f64,
    pub maximum: f64,
}

/// A single datapoint stored by PutMetricData.
#[derive(Debug, Clone)]
pub struct MetricDatum {
    pub namespace: String,
    pub name: String,
    pub unit: Option<String>,
    pub dimensions: Vec<(String, String)>,
    /// Epoch milliseconds.
    pub timestamp: i64,
    pub value: Option<f64>,
    pub stats: Option<DatumStats>,
}

impl MetricDatum {
    /// The single scalar value, if this datum holds one.
    pub fn single_value(&self) -> Option<f64> {
        self.value
    }

    /// The average of aggregated statistics (for percentiles).
    pub fn agg_average(&self) -> Option<f64> {
        self.stats.map(|s| {
            if s.sample_count > 0.0 { s.sum / s.sample_count } else { 0.0 }
        })
    }

    /// Expanded sample values for percentile computation: single values plus
    /// the repeated average of aggregated datums (moto behaviour).
    pub fn percentile_samples(values: &[f64], aggs: &[DatumStats]) -> Vec<f64> {
        let mut out: Vec<f64> = values.to_vec();
        for a in aggs {
            let count = a.sample_count.max(0.0) as usize;
            if count > 0 {
                let avg = if a.sample_count > 0.0 { a.sum / a.sample_count } else { 0.0 };
                out.extend(std::iter::repeat_n(avg, count));
            }
        }
        out
    }
}

#[derive(Clone)]
pub struct CloudwatchState {
    pub metrics: Arc<RwLock<Vec<MetricDatum>>>,
    pub alarms: Arc<RwLock<HashMap<String, Value>>>,
    pub composite_alarms: Arc<RwLock<HashMap<String, Value>>>,
    pub anomaly_detectors: Arc<RwLock<Vec<Value>>>,
    pub insight_rules: Arc<RwLock<HashMap<String, Value>>>,
    pub metric_streams: Arc<RwLock<HashMap<String, Value>>>,
    pub mute_rules: Arc<RwLock<HashMap<String, Value>>>,
    pub dashboards: Arc<RwLock<HashMap<String, Value>>>,
    pub tags: Arc<RwLock<HashMap<String, Vec<Value>>>>,
}

impl CloudwatchState {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(Vec::new())),
            alarms: Arc::new(RwLock::new(HashMap::new())),
            composite_alarms: Arc::new(RwLock::new(HashMap::new())),
            anomaly_detectors: Arc::new(RwLock::new(Vec::new())),
            insight_rules: Arc::new(RwLock::new(HashMap::new())),
            metric_streams: Arc::new(RwLock::new(HashMap::new())),
            mute_rules: Arc::new(RwLock::new(HashMap::new())),
            dashboards: Arc::new(RwLock::new(HashMap::new())),
            tags: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for CloudwatchState {
    fn default() -> Self { Self::new() }
}
