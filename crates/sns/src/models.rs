//! SNS in-memory state models.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// An SNS subscription.
#[derive(Debug, Clone)]
pub struct Subscription {
    pub subscription_arn: String,
    pub topic_arn: String,
    pub protocol: String,
    pub endpoint: String,
    pub owner: u64,
    pub confirmed: bool,
    pub created: u64,
    /// User-set subscription attributes (RawMessageDelivery, FilterPolicy, FilterPolicyScope, ...)
    pub attributes: Arc<RwLock<HashMap<String, String>>>,
}

impl Subscription {
    pub fn attr(&self, name: &str) -> Option<String> {
        self.attributes.read().get(name).cloned()
    }

    pub fn set_attr(&self, name: &str, value: String) {
        self.attributes.write().insert(name.to_string(), value);
    }

    /// RawMessageDelivery as a boolean (`"true"` string attribute).
    pub fn raw_message_delivery(&self) -> bool {
        self.attr("RawMessageDelivery")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }
}

/// An SNS topic.
#[derive(Debug)]
pub struct Topic {
    pub name: String,
    pub arn: String,
    pub owner: u64,
    pub region: String,
    pub created: u64,
    pub fifo: bool,
    /// User-set topic attributes (DisplayName, Policy, FifoTopic, ContentBasedDeduplication,
    /// DeliveryPolicy, DataProtectionPolicy, ...)
    pub attributes: RwLock<HashMap<String, String>>,
    pub tags: RwLock<Vec<(String, String)>>,
}

impl Topic {
    pub fn new(name: String, account: u64, region: String, fifo: bool) -> Self {
        let arn = format!("arn:aws:sns:{}:{}:{}", region, account, name);
        Self {
            name,
            arn: arn.clone(),
            owner: account,
            region,
            created: chrono::Utc::now().timestamp() as u64,
            fifo,
            attributes: RwLock::new(HashMap::new()),
            tags: RwLock::new(vec![]),
        }
    }

    pub fn attr(&self, name: &str) -> Option<String> {
        self.attributes.read().get(name).cloned()
    }

    pub fn set_attr(&self, name: String, value: String) {
        self.attributes.write().insert(name, value);
    }

    pub fn display_name(&self) -> String {
        self.attr("DisplayName")
            .unwrap_or_else(|| self.name.clone())
    }

    /// Tag lookup helper (ordered list, last value wins per key).
    pub fn tag_value(&self, key: &str) -> Option<String> {
        self.tags
            .read()
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    }

    pub fn set_tag(&self, key: &str, value: &str) {
        let mut tags = self.tags.write();
        for (k, v) in tags.iter_mut() {
            if k == key {
                *k = key.to_string();
                *v = value.to_string();
                return;
            }
        }
        tags.push((key.to_string(), value.to_string()));
    }
}

/// A platform application (e.g. GCM push app).
#[derive(Debug)]
pub struct PlatformApplication {
    pub arn: String,
    pub name: String,
    pub platform: String,
    pub attributes: RwLock<HashMap<String, String>>,
}

/// A platform endpoint (a device registered with a platform application).
#[derive(Debug)]
pub struct PlatformEndpoint {
    pub arn: String,
    pub application_arn: String,
    pub token: String,
    pub custom_user_data: String,
    pub attributes: RwLock<HashMap<String, String>>,
}

/// An SMS sandbox phone number.
#[derive(Debug, Clone)]
pub struct SmsSandboxNumber {
    pub phone: String,
    pub verified: bool,
}

/// The SNS state store (per account+region).
#[derive(Clone)]
pub struct SnsState {
    pub topics: Arc<RwLock<HashMap<String, Arc<Topic>>>>,
    pub subscriptions: Arc<RwLock<Vec<Subscription>>>,
    pub platform_applications: Arc<RwLock<HashMap<String, Arc<PlatformApplication>>>>,
    pub platform_endpoints: Arc<RwLock<HashMap<String, Arc<PlatformEndpoint>>>>,
    /// SMS account attributes (DefaultSMSType, DefaultSenderID, ...)
    pub sms_attributes: Arc<RwLock<HashMap<String, String>>>,
    /// Opted-out phone numbers.
    pub opted_out: Arc<RwLock<std::collections::HashSet<String>>>,
    /// Created SMS sandbox phone numbers with their verification status.
    pub sms_sandbox_numbers: Arc<RwLock<HashMap<String, SmsSandboxNumber>>>,
}

impl SnsState {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(Vec::new())),
            platform_applications: Arc::new(RwLock::new(HashMap::new())),
            platform_endpoints: Arc::new(RwLock::new(HashMap::new())),
            sms_attributes: Arc::new(RwLock::new(HashMap::new())),
            opted_out: Arc::new(RwLock::new(std::collections::HashSet::new())),
            sms_sandbox_numbers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_topic(&self, arn: &str) -> Option<Arc<Topic>> {
        self.topics.read().get(arn).cloned()
    }

    pub fn put_topic(&self, topic: Arc<Topic>) {
        self.topics.write().insert(topic.arn.clone(), topic);
    }

    pub fn delete_topic(&self, arn: &str) -> Option<Arc<Topic>> {
        let topic = self.topics.write().remove(arn);
        if let Some(t) = &topic {
            self.subscriptions.write().retain(|s| s.topic_arn != t.arn);
        }
        topic
    }

    pub fn list_topics(&self) -> Vec<Arc<Topic>> {
        let mut topics: Vec<Arc<Topic>> = self.topics.read().values().cloned().collect();
        topics.sort_by(|a, b| a.arn.cmp(&b.arn));
        topics
    }

    pub fn add_subscription(&self, sub: Subscription) {
        self.subscriptions.write().push(sub);
    }

    pub fn list_subscriptions(&self) -> Vec<Subscription> {
        self.subscriptions.read().clone()
    }

    pub fn list_subscriptions_by_topic(&self, topic_arn: &str) -> Vec<Subscription> {
        self.subscriptions
            .read()
            .iter()
            .filter(|s| s.topic_arn == topic_arn)
            .cloned()
            .collect()
    }

    pub fn get_subscription(&self, arn: &str) -> Option<Subscription> {
        self.subscriptions
            .read()
            .iter()
            .find(|s| s.subscription_arn == arn)
            .cloned()
    }

    /// Update a subscription's attributes in place while holding the lock.
    pub fn update_subscription<F: FnOnce(&Subscription)>(&self, arn: &str, f: F) {
        let subs = self.subscriptions.read();
        if let Some(sub) = subs.iter().find(|s| s.subscription_arn == arn) {
            f(sub);
        }
    }

    pub fn remove_subscription(&self, arn: &str) {
        self.subscriptions
            .write()
            .retain(|s| s.subscription_arn != arn);
    }
}

impl Default for SnsState {
    fn default() -> Self {
        Self::new()
    }
}
