//! SNS operation handler.

use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{SmsSandboxNumber, SnsState, Subscription, Topic};
use crate::protocol::{AwsRequest, AwsResponse};
use sqs::SqsHandler as _;

/// The SNS service handler.
pub struct SnsHandler {
    state: RwLock<HashMap<(u64, String), SnsState>>,
    /// SQS handler used for cross-service delivery (shares the in-process SQS store).
    sqs: sqs::DefaultSqsHandler,
}

// ---------- param helpers ----------

fn pstr(req: &AwsRequest, key: &str) -> Option<String> {
    req.params
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn parse_entry_attrs(req: &AwsRequest) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(obj) = req.params.as_object() {
        for (k, v) in obj {
            // Both `Attributes.entry.N.key` (Subscribe/CreateTopic) and the
            // lowercase `attributes.entry.N.key` (SetSMSAttributes) shapes occur.
            let Some(base) = k.strip_suffix(".key") else {
                continue;
            };
            if !base.contains(".entry.") {
                continue;
            }
            let val_key = format!("{}.value", base);
            if let Value::String(keyval) = v {
                if let Some(Value::String(val)) = req.params.get(&val_key) {
                    out.insert(keyval.to_string(), val.to_string());
                }
            }
        }
    }
    out
}

/// XML-escape a text node payload.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn member_list_xml(field: &str, members: &[String]) -> String {
    let mut body = String::new();
    for m in members {
        body.push_str(&format!("<member>{}</member>", esc(m)));
    }
    format!("<{f}>{b}</{f}>", f = field, b = body)
}

impl SnsHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
            sqs: sqs::DefaultSqsHandler::new(),
        }
    }
}

impl Default for SnsHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl SnsHandler {
    fn get_state(&self, account: u64, region: &str) -> SnsState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    fn not_found(arne: &str) -> AwsResponse {
        AwsResponse::error(404, "NotFound", &format!("Topic {} not found", arne))
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let operation = req.operation.as_str();
        match operation {
            "CreateTopic" => self.create_topic(&req),
            "DeleteTopic" => self.delete_topic(&req),
            "GetTopicAttributes" => self.get_topic_attributes(&req),
            "SetTopicAttributes" => self.set_topic_attributes(&req),
            "ListTopics" => self.list_topics(&req),
            "Publish" => self.publish(&req),
            "PublishBatch" => self.publish_batch(&req),
            "Subscribe" => self.subscribe(&req),
            "ConfirmSubscription" => self.confirm_subscription(&req),
            "Unsubscribe" => self.unsubscribe(&req),
            "ListSubscriptions" => self.list_subscriptions(&req),
            "ListSubscriptionsByTopic" => self.list_subscriptions_by_topic(&req),
            "GetSubscriptionAttributes" => self.get_subscription_attributes(&req),
            "SetSubscriptionAttributes" => self.set_subscription_attributes(&req),
            "AddPermission" => self.add_permission(&req),
            "RemovePermission" => self.remove_permission(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "CreatePlatformApplication" => self.create_platform_application(&req),
            "ListPlatformApplications" => self.list_platform_applications(&req),
            "DeletePlatformApplication" => self.delete_platform_application(&req),
            "GetPlatformApplication" => self.get_platform_application(&req),
            "UpdatePlatformApplication" => self.update_platform_application(&req),
            "GetPlatformApplicationAttributes" => self.get_platform_application_attributes(&req),
            "SetPlatformApplicationAttributes" => self.set_platform_application_attributes(&req),
            "CreatePlatformEndpoint" => self.create_platform_endpoint(&req),
            "GetEndpointAttributes" => self.get_endpoint_attributes(&req),
            "SetEndpointAttributes" => self.set_endpoint_attributes(&req),
            "DeleteEndpoint" => self.delete_endpoint(&req),
            "ListEndpointsByPlatformApplication" => {
                self.list_endpoints_by_platform_application(&req)
            }
            "GetSMSAttributes" => self.get_sms_attributes(&req),
            "SetSMSAttributes" => self.set_sms_attributes(&req),
            "ListPhoneNumbersOptedOut" => self.list_phone_numbers_opted_out(&req),
            "CheckIfPhoneNumberIsOptedOut" => self.check_if_phone_number_is_opted_out(&req),
            "OptInPhoneNumber" => self.opt_in_phone_number(&req),
            "ListOriginationNumbers" => self.list_origination_numbers(&req),
            "GetSMSSandboxAccountStatus" => self.get_sms_sandbox_account_status(&req),
            "ListSMSSandboxPhoneNumbers" => self.list_sms_sandbox_phone_numbers(&req),
            "CreateSMSSandboxPhoneNumber" => self.create_sms_sandbox_phone_number(&req),
            "VerifySMSSandboxPhoneNumber" => self.verify_sms_sandbox_phone_number(&req),
            "DeleteSMSSandboxPhoneNumber" => self.delete_sms_sandbox_phone_number(&req),
            "PutDataProtectionPolicy" => self.put_data_protection_policy(&req),
            "GetDataProtectionPolicy" => self.get_data_protection_policy(&req),
            other => AwsResponse::error(
                400,
                "InvalidAction",
                &format!("The action {} is not supported", other),
            ),
        }
    }

    // ---------- topics ----------

    fn create_topic(&self, req: &AwsRequest) -> AwsResponse {
        let name = pstr(req, "Name").unwrap_or_default();

        if name.is_empty() {
            return AwsResponse::error(400, "InvalidParameter", "Topic name cannot be empty");
        }

        let attrs = parse_entry_attrs(req);
        let fifo = attrs
            .get("FifoTopic")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or_else(|| name.ends_with(".fifo"));
        if fifo && !name.ends_with(".fifo") {
            return AwsResponse::error(
                400,
                "InvalidParameter",
                "FIFO topic name must end with .fifo",
            );
        }
        let topic = Arc::new(Topic::new(
            name.clone(),
            req.account,
            req.region.clone(),
            fifo,
        ));
        for (k, v) in attrs {
            topic.set_attr(k, v);
        }
        if fifo {
            // Persist as attributes so GetTopicAttributes reports what AWS reports.
            topic.set_attr("FifoTopic".into(), "true".into());
            if !topic
                .attributes
                .read()
                .contains_key("ContentBasedDeduplication")
            {
                topic.set_attr("ContentBasedDeduplication".into(), "false".into());
            }
        }

        // Tags on creation: Tags.member.N.Key/Value
        let mut i = 1;
        let state = self.get_state(req.account, &req.region);
        loop {
            let key_name = format!("Tags.member.{}.Key", i);
            let val_name = format!("Tags.member.{}.Value", i);
            match (pstr(req, &key_name), pstr(req, &val_name)) {
                (Some(k), Some(v)) => {
                    topic.set_tag(&k, &v);
                    i += 1;
                }
                _ => break,
            }
        }

        state.put_topic(topic.clone());

        AwsResponse::query_success(
            "CreateTopic",
            format!("<TopicArn>{}</TopicArn>", esc(&topic.arn)),
        )
    }

    fn delete_topic(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn").unwrap_or_default();
        // DeleteTopic is idempotent: deleting a nonexistent topic succeeds.
        let state = self.get_state(req.account, &req.region);
        state.delete_topic(&arn);
        AwsResponse::query_success("DeleteTopic", String::new())
    }

    fn get_topic_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };

        let mut entries: Vec<(String, String)> = Vec::new();
        let stored = topic.attributes.read().clone();
        let sub_count = state.list_subscriptions_by_topic(&arn).len();

        for (k, v) in &stored {
            entries.push((k.clone(), v.clone()));
        }
        // Computed fields overlay stored attributes (they cannot be overwritten).
        entries.push(("TopicArn".into(), topic.arn.clone()));
        entries.push(("Owner".into(), topic.owner.to_string()));
        entries.push(("DisplayName".into(), topic.display_name()));
        entries.push(("SubscriptionsConfirmed".into(), sub_count.to_string()));
        entries.push(("SubscriptionsPending".into(), "0".into()));
        entries.push(("SubscriptionsDeleted".into(), "0".into()));
        if topic.fifo {
            let cbd = stored
                .get("ContentBasedDeduplication")
                .cloned()
                .unwrap_or_else(|| "false".into());
            entries.push(("FifoTopic".into(), "true".into()));
            entries.push(("ContentBasedDeduplication".into(), cbd));
        }
        if !stored.contains_key("EffectiveDeliveryPolicy") {
            entries.push((
                "EffectiveDeliveryPolicy".into(),
                json!({
                    "http": {
                        "defaultHealthyRetryPolicy": {
                            "minDelayTarget": 20,
                            "maxDelayTarget": 20,
                            "numRetries": 3,
                            "numMaxDelayRetries": 0,
                            "numNoDelayRetries": 0,
                            "numMinDelayRetries": 0,
                            "backoffFunction": "linear",
                        },
                        "disableSubscriptionOverrides": false,
                    }
                })
                .to_string(),
            ));
        }

        let mut body = String::from("<Attributes>");
        for (k, v) in &entries {
            body.push_str(&format!(
                "<entry><key>{}</key><value>{}</value></entry>",
                esc(k),
                esc(v)
            ));
        }
        body.push_str("</Attributes>");
        AwsResponse::query_success("GetTopicAttributes", body)
    }

    fn set_topic_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };

        let attr_name = pstr(req, "AttributeName").unwrap_or_default();
        let attr_value = pstr(req, "AttributeValue").unwrap_or_default();
        if !attr_name.is_empty() {
            topic.set_attr(attr_name, attr_value);
        }
        AwsResponse::query_success("SetTopicAttributes", String::new())
    }

    fn list_topics(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let topics = state.list_topics();

        let mut body = String::from("<Topics>");
        for topic in &topics {
            body.push_str(&format!(
                "<member><TopicArn>{}</TopicArn></member>",
                esc(&topic.arn)
            ));
        }
        body.push_str("</Topics>");
        AwsResponse::query_success("ListTopics", body)
    }

    // ---------- publish / delivery ----------

    /// Read publish-style message attributes. Return map of name → {DataType, StringValue}.
    fn parse_message_attributes(req: &AwsRequest) -> Map<String, Value> {
        let mut out = Map::new();
        let mut i = 1;
        while let Some(name) = pstr(req, &format!("MessageAttributes.entry.{}.Name", i)) {
            let data_type = pstr(
                req,
                &format!("MessageAttributes.entry.{}.Value.DataType", i),
            )
            .unwrap_or_else(|| "String".into());
            let string_value = pstr(
                req,
                &format!("MessageAttributes.entry.{}.Value.StringValue", i),
            )
            .unwrap_or_default();
            out.insert(
                name,
                json!({"DataType": data_type, "StringValue": string_value}),
            );
            i += 1;
        }
        out
    }

    /// Pure JSON-publish entries as pre-parsed lists (not used by the query protocol,
    /// but harmless to support).
    #[allow(clippy::too_many_arguments)]
    fn publish_common(
        &self,
        state: &SnsState,
        topic: &Arc<crate::models::Topic>,
        message: &str,
        subject: Option<String>,
        message_attributes: &Map<String, Value>,
        message_structure: Option<&str>,
        message_group_id: Option<String>,
        message_dedup_id: Option<String>,
    ) -> Result<String, AwsResponse> {
        let _ = message_dedup_id;
        if topic.fifo && message_group_id.is_none() {
            return Err(AwsResponse::error(
                400,
                "InvalidParameter",
                "The MessageGroupId parameter is required for FIFO topics.",
            ));
        }

        // MessageStructure=json: select per-protocol message.
        let per_protocol: Option<Map<String, Value>> = match message_structure {
            Some("json") => {
                let parsed: Option<Map<String, Value>> = serde_json::from_str::<Value>(message)
                    .ok()
                    .and_then(|v| v.as_object().cloned());
                match parsed {
                    Some(obj) => Some(obj),
                    None => return Err(AwsResponse::error(
                        400,
                        "InvalidParameter",
                        "Invalid parameter: Message Structure - JSON message body failed to parse",
                    )),
                }
            }
            _ => None,
        };

        let message_id = uuid::Uuid::new_v4().to_string();
        for sub in state.list_subscriptions_by_topic(&topic.arn) {
            if !sub.confirmed {
                continue;
            }
            if !matches_filter(&sub, message_attributes, message) {
                continue;
            }
            let delivery_message = match &per_protocol {
                Some(obj) => obj
                    .get(&sub.protocol)
                    .or_else(|| obj.get("default"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(message)
                    .to_string(),
                None => message.to_string(),
            };
            self.deliver(
                &sub,
                &delivery_message,
                subject.as_deref(),
                message_attributes,
                &message_id,
                &topic.arn,
                &topic.region,
            );
        }

        Ok(message_id)
    }

    fn publish(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn")
            .or_else(|| pstr(req, "TargetArn"))
            .unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };

        let message = pstr(req, "Message").unwrap_or_default();
        let attrs = Self::parse_message_attributes(req);
        let message_id = match self.publish_common(
            &state,
            &topic,
            &message,
            pstr(req, "Subject"),
            &attrs,
            pstr(req, "MessageStructure").as_deref(),
            pstr(req, "MessageGroupId"),
            pstr(req, "MessageDeduplicationId"),
        ) {
            Ok(id) => id,
            Err(resp) => return resp,
        };
        AwsResponse::query_success(
            "Publish",
            format!("<MessageId>{}</MessageId>", esc(&message_id)),
        )
    }

    fn publish_batch(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };

        let mut i = 1;
        let mut successful = Vec::new();
        while let Some(id) = pstr(req, &format!("PublishBatchRequestEntries.member.{}.Id", i)) {
            let message = pstr(
                req,
                &format!("PublishBatchRequestEntries.member.{}.Message", i),
            )
            .unwrap_or_default();
            let subject = pstr(
                req,
                &format!("PublishBatchRequestEntries.member.{}.Subject", i),
            );
            let message_id = self
                .publish_common(
                    &state,
                    &topic,
                    &message,
                    subject,
                    &Map::new(),
                    None,
                    pstr(
                        req,
                        &format!("PublishBatchRequestEntries.member.{}.MessageGroupId", i),
                    ),
                    pstr(
                        req,
                        &format!(
                            "PublishBatchRequestEntries.member.{}.MessageDeduplicationId",
                            i
                        ),
                    ),
                )
                .unwrap_or_default();
            successful.push(format!(
                "<member><Id>{}</Id><MessageId>{}</MessageId></member>",
                esc(&id),
                esc(&message_id)
            ));
            i += 1;
        }

        let body = format!("<Successful>{}</Successful><Failed/>", successful.join(""));
        AwsResponse::query_success("PublishBatch", body)
    }

    /// Deliver a notification to one subscription (SQS only; other protocols are no-ops
    /// in-process like the Python truth).
    #[allow(clippy::too_many_arguments)]
    fn deliver(
        &self,
        sub: &Subscription,
        message: &str,
        subject: Option<&str>,
        message_attributes: &Map<String, Value>,
        message_id: &str,
        topic_arn: &str,
        region: &str,
    ) {
        if sub.protocol != "sqs" {
            return;
        }
        // Endpoint is a queue ARN: arn:aws:sqs:<region>:<account>:<name>
        let parts: Vec<&str> = sub.endpoint.split(':').collect();
        if parts.len() < 6 {
            return;
        }
        let queue_region = parts[3];
        let queue_account: u64 = parts[4].parse().unwrap_or(123456789012);
        let queue_name = sub.endpoint.rsplit(':').next().unwrap_or_default();

        let body = if sub.raw_message_delivery() {
            message.to_string()
        } else {
            let mut notification = json!({
                "Type": "Notification",
                "MessageId": message_id,
                "TopicArn": topic_arn,
                "Message": message,
                "Timestamp": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
                "SignatureVersion": "1",
                "Signature": "EXAMPLE",
                "SigningCertURL": format!("https://sns.{}.amazonaws.com/SimpleNotificationService.pem", region),
                "UnsubscribeURL": format!("https://sns.{}.amazonaws.com/?Action=Unsubscribe&SubscriptionArn={}", region, sub.subscription_arn),
            });
            if let Some(subject) = subject {
                notification["Subject"] = json!(subject);
            }
            if !message_attributes.is_empty() {
                let mut sns_attrs = Map::new();
                for (name, val) in message_attributes {
                    sns_attrs.insert(
                        name.clone(),
                        json!({
                            "Type": val.get("DataType").cloned().unwrap_or(json!("String")),
                            "Value": val.get("StringValue").cloned().unwrap_or(json!("")),
                        }),
                    );
                }
                notification["MessageAttributes"] = Value::Object(sns_attrs);
            }
            notification.to_string()
        };

        let queue_url = format!(
            "http://sqs.{}.localhost.robotocore.cloud:4566/{}/{}",
            queue_region, queue_account, queue_name
        );
        let params = json!({"QueueUrl": queue_url, "MessageBody": body});
        let _ = self.sqs.handle(sqs::AwsRequest {
            service: "sqs".into(),
            operation: "SendMessage".into(),
            account: queue_account,
            region: queue_region.to_string(),
            params,
            body: bytes::Bytes::new(),
        });
    }

    // ---------- subscriptions ----------

    fn subscribe(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };

        let protocol = pstr(req, "Protocol").unwrap_or_else(|| "email".into());
        let endpoint = pstr(req, "Endpoint").unwrap_or_default();

        let subscription_arn = format!("{}:{}", topic.arn, uuid::Uuid::new_v4());
        let confirmed = protocol != "http" && protocol != "https";

        let sub = Subscription {
            subscription_arn: subscription_arn.clone(),
            topic_arn: arn.clone(),
            protocol: protocol.clone(),
            endpoint,
            owner: req.account,
            confirmed,
            created: chrono::Utc::now().timestamp() as u64,
            attributes: Arc::new(RwLock::new(parse_entry_attrs(req))),
        };

        state.add_subscription(sub);
        AwsResponse::query_success(
            "Subscribe",
            format!(
                "<SubscriptionArn>{}</SubscriptionArn>",
                esc(&subscription_arn)
            ),
        )
    }

    fn confirm_subscription(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        // Confirm the first pending subscription on the topic (like the Python truth);
        // an invalid token therefore yields NotFound for fully-confirmed topics.
        let mut confirmed_arn: Option<String> = None;
        {
            let mut subs = state.subscriptions.write();
            if let Some(sub) = subs.iter_mut().find(|s| s.topic_arn == arn && !s.confirmed) {
                sub.confirmed = true;
                confirmed_arn = Some(sub.subscription_arn.clone());
            }
        }
        match confirmed_arn {
            Some(sub_arn) => AwsResponse::query_success(
                "ConfirmSubscription",
                format!("<SubscriptionArn>{}</SubscriptionArn>", esc(&sub_arn)),
            ),
            None => AwsResponse::error(
                404,
                "NotFound",
                &format!("No pending subscription for topic {}", arn),
            ),
        }
    }

    fn unsubscribe(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "SubscriptionArn").unwrap_or_default();
        if arn.is_empty() {
            return AwsResponse::error(400, "InvalidParameter", "Subscription arn cannot be empty");
        }
        let state = self.get_state(req.account, &req.region);
        state.remove_subscription(&arn);
        AwsResponse::query_success("Unsubscribe", String::new())
    }

    fn list_subscriptions(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        Self::subscriptions_xml("ListSubscriptions", &state.list_subscriptions())
    }

    fn list_subscriptions_by_topic(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if state.get_topic(&arn).is_none() {
            return Self::not_found(&arn);
        }
        Self::subscriptions_xml(
            "ListSubscriptionsByTopic",
            &state.list_subscriptions_by_topic(&arn),
        )
    }

    fn sub_member_xml(sub: &Subscription) -> String {
        format!(
            "<member><SubscriptionArn>{}</SubscriptionArn><Owner>{}</Owner><Protocol>{}</Protocol><Endpoint>{}</Endpoint><TopicArn>{}</TopicArn></member>",
            esc(&sub.subscription_arn),
            esc(&sub.owner.to_string()),
            esc(&sub.protocol),
            esc(&sub.endpoint),
            esc(&sub.topic_arn),
        )
    }

    fn subscriptions_xml(op: &str, subs: &[Subscription]) -> AwsResponse {
        let mut body = String::from("<Subscriptions>");
        for sub in subs {
            body.push_str(&Self::sub_member_xml(sub));
        }
        body.push_str("</Subscriptions>");
        AwsResponse::query_success(op, body)
    }

    fn get_subscription_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let sub_arn = pstr(req, "SubscriptionArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_subscription(&sub_arn) {
            Some(sub) => {
                let raw = sub.raw_message_delivery();
                let mut entities: Vec<(String, String)> = vec![
                    ("SubscriptionArn".into(), sub.subscription_arn.clone()),
                    ("TopicArn".into(), sub.topic_arn.clone()),
                    ("Protocol".into(), sub.protocol.clone()),
                    ("Endpoint".into(), sub.endpoint.clone()),
                    ("Owner".into(), sub.owner.to_string()),
                    ("RawMessageDelivery".into(), raw.to_string()),
                    ("PendingConfirmation".into(), (!sub.confirmed).to_string()),
                ];
                for (k, v) in sub.attributes.read().iter() {
                    if matches!(k.as_str(), "RawMessageDelivery" | "__confirmed__") {
                        continue;
                    }
                    entities.push((k.clone(), v.clone()));
                }
                // Normalize the boolean strings to lowercase like the Python truth.
                let mut body = String::from("<Attributes>");
                for (k, mut v) in entities {
                    if k == "RawMessageDelivery" || k == "PendingConfirmation" {
                        v = v.to_lowercase();
                    }
                    body.push_str(&format!(
                        "<entry><key>{}</key><value>{}</value></entry>",
                        esc(&k),
                        esc(&v)
                    ));
                }
                body.push_str("</Attributes>");
                AwsResponse::query_success("GetSubscriptionAttributes", body)
            }
            None => AwsResponse::error(
                404,
                "NotFound",
                &format!("Subscription {} not found", sub_arn),
            ),
        }
    }

    fn set_subscription_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let sub_arn = pstr(req, "SubscriptionArn").unwrap_or_default();
        let attr_name = pstr(req, "AttributeName").unwrap_or_default();
        let attr_value = pstr(req, "AttributeValue").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        match state.get_subscription(&sub_arn) {
            Some(sub) => {
                if !attr_name.is_empty() {
                    sub.set_attr(&attr_name, attr_value);
                }
                AwsResponse::query_success("SetSubscriptionAttributes", String::new())
            }
            None => AwsResponse::error(
                404,
                "NotFound",
                &format!("Subscription {} not found", sub_arn),
            ),
        }
    }

    // ---------- permissions ----------

    fn add_permission(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };
        let label = pstr(req, "Label").unwrap_or_default();

        let action_names: Vec<String> = req
            .params
            .get("ActionName")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let aws_account_ids: Vec<String> = req
            .params
            .get("AWSAccountId")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        let existing = topic
            .attr("Policy")
            .unwrap_or_else(|| json!({"Version": "2008-10-17", "Statement": []}).to_string());
        let mut policy: Value = serde_json::from_str(&existing)
            .unwrap_or_else(|_| json!({"Version": "2008-10-17", "Statement": []}));
        if !policy.is_object() {
            policy = json!({"Version": "2008-10-17", "Statement": []});
        }
        let obj = policy.as_object_mut().unwrap();
        let statements = obj
            .entry("Statement".to_string())
            .or_insert_with(|| json!([]));
        if let Some(existing_stmts) = statements.as_array() {
            if existing_stmts
                .iter()
                .any(|s| s.get("Sid").and_then(|v| v.as_str()) == Some(label.as_str()))
            {
                return AwsResponse::error(400, "InvalidParameter", "Statement already exists");
            }
        }
        let principals: Vec<String> = aws_account_ids
            .iter()
            .map(|aid| format!("arn:aws:iam::{}:root", aid))
            .collect();
        let actions: Vec<String> = action_names.iter().map(|a| format!("SNS:{}", a)).collect();
        let statement = json!({
            "Sid": label,
            "Effect": "Allow",
            "Principal": {"AWS": if principals.len() == 1 { json!(principals[0]) } else { json!(principals) }},
            "Action": if actions.len() == 1 { json!(actions[0]) } else { json!(actions) },
            "Resource": arn,
        });
        obj.get_mut("Statement")
            .and_then(|v| v.as_array_mut())
            .unwrap()
            .push(statement);
        topic.set_attr("Policy".into(), policy.to_string());
        AwsResponse::query_success("AddPermission", String::new())
    }

    fn remove_permission(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "TopicArn").unwrap_or_default();
        let label = pstr(req, "Label").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };
        let existing = topic
            .attr("Policy")
            .unwrap_or_else(|| json!({"Version": "2008-10-17", "Statement": []}).to_string());
        if let Ok(mut policy) = serde_json::from_str::<Value>(&existing) {
            if let Some(stmts) = policy
                .as_object_mut()
                .and_then(|o| o.get_mut("Statement"))
                .and_then(|v| v.as_array_mut())
            {
                stmts.retain(|s| s.get("Sid").and_then(|v| v.as_str()) != Some(label.as_str()));
            }
            topic.set_attr("Policy".into(), policy.to_string());
        }
        AwsResponse::query_success("RemovePermission", String::new())
    }

    // ---------- tags ----------

    fn tag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "ResourceArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };
        let mut i = 1;
        while let (Some(k), Some(v)) = (
            pstr(req, &format!("Tags.member.{}.Key", i)),
            pstr(req, &format!("Tags.member.{}.Value", i)),
        ) {
            topic.set_tag(&k, &v);
            i += 1;
        }
        AwsResponse::query_success("TagResource", String::new())
    }

    fn untag_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "ResourceArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };
        let mut keys: Vec<String> = Vec::new();
        if let Some(arr) = req.params.get("TagKeys").and_then(|v| v.as_array()) {
            for v in arr {
                if let Some(s) = v.as_str() {
                    keys.push(s.to_string());
                }
            }
        }
        let mut i = 1;
        while let Some(k) = pstr(req, &format!("TagKeys.member.{}", i)) {
            keys.push(k);
            i += 1;
        }
        topic.tags.write().retain(|(k, _)| !keys.contains(k));
        AwsResponse::query_success("UntagResource", String::new())
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "ResourceArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };
        let tags = topic.tags.read().clone();
        let mut body = String::from("<Tags>");
        for (k, v) in &tags {
            body.push_str(&format!(
                "<member><Key>{}</Key><Value>{}</Value></member>",
                esc(k),
                esc(v)
            ));
        }
        body.push_str("</Tags>");
        AwsResponse::query_success("ListTagsForResource", body)
    }

    // ---------- platform applications / endpoints ----------

    fn create_platform_application(&self, req: &AwsRequest) -> AwsResponse {
        let platform = pstr(req, "Platform").unwrap_or_default();
        let name = pstr(req, "Name").unwrap_or_default();
        let arn = format!(
            "arn:aws:sns:{}:{}:app/{}/{}",
            req.region, req.account, platform, name
        );
        let attrs = parse_entry_attrs(req);
        let app = Arc::new(crate::models::PlatformApplication {
            arn: arn.clone(),
            name,
            platform,
            attributes: RwLock::new(attrs),
        });
        self.get_state(req.account, &req.region)
            .platform_applications
            .write()
            .insert(arn.clone(), app);
        AwsResponse::query_success(
            "CreatePlatformApplication",
            format!(
                "<PlatformApplicationArn>{}</PlatformApplicationArn>",
                esc(&arn)
            ),
        )
    }

    fn list_platform_applications(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let apps = state.platform_applications.read();
        let mut body = String::from("<PlatformApplications>");
        for app in apps.values() {
            body.push_str("<member>");
            body.push_str(&format!(
                "<PlatformApplicationArn>{}</PlatformApplicationArn>",
                esc(&app.arn)
            ));
            body.push_str("<Attributes>");
            for (k, v) in app.attributes.read().iter() {
                body.push_str(&format!(
                    "<entry><key>{}</key><value>{}</value></entry>",
                    esc(k),
                    esc(v)
                ));
            }
            body.push_str("</Attributes>");
            body.push_str("</member>");
        }
        body.push_str("</PlatformApplications>");
        AwsResponse::query_success("ListPlatformApplications", body)
    }

    fn delete_platform_application(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "PlatformApplicationArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        {
            let mut apps = state.platform_applications.write();
            if apps.remove(&arn).is_some() {
                // Cascade: drop endpoints owned by this app.
                let mut eps = state.platform_endpoints.write();
                let orphaned: Vec<String> = eps
                    .iter()
                    .filter(|(_, ep)| ep.application_arn == arn)
                    .map(|(k, _)| k.clone())
                    .collect();
                for k in orphaned {
                    eps.remove(&k);
                }
            }
        }
        AwsResponse::query_success("DeletePlatformApplication", String::new())
    }

    fn get_platform_application(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "PlatformApplicationArn").unwrap_or_default();
        AwsResponse::query_success(
            "GetPlatformApplication",
            format!(
                "<PlatformApplication><PlatformApplicationArn>{}</PlatformApplicationArn></PlatformApplication>",
                esc(&arn)
            ),
        )
    }

    fn update_platform_application(&self, _req: &AwsRequest) -> AwsResponse {
        AwsResponse::query_success("UpdatePlatformApplication", String::new())
    }

    fn get_platform_application_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "PlatformApplicationArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let app = state.platform_applications.read().get(&arn).cloned();
        match app {
            Some(app) => {
                let mut body = String::from("<Attributes>");
                for (k, v) in app.attributes.read().iter() {
                    body.push_str(&format!(
                        "<entry><key>{}</key><value>{}</value></entry>",
                        esc(k),
                        esc(v)
                    ));
                }
                body.push_str("</Attributes>");
                AwsResponse::query_success("GetPlatformApplicationAttributes", body)
            }
            None => Self::not_found(&arn),
        }
    }

    fn set_platform_application_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "PlatformApplicationArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let app = state.platform_applications.read().get(&arn).cloned();
        if let Some(app) = app {
            for (k, v) in parse_entry_attrs(req) {
                app.attributes.write().insert(k, v);
            }
        }
        AwsResponse::query_success("SetPlatformApplicationAttributes", String::new())
    }

    fn create_platform_endpoint(&self, req: &AwsRequest) -> AwsResponse {
        let app_arn = pstr(req, "PlatformApplicationArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let app = state.platform_applications.read().get(&app_arn).cloned();
        let app = match app {
            Some(a) => a,
            None => {
                return AwsResponse::error(404, "NotFound", "PlatformApplication does not exist")
            }
        };
        let token = pstr(req, "Token").unwrap_or_default();
        let custom_user_data = pstr(req, "CustomUserData").unwrap_or_default();
        let attrs = parse_entry_attrs(req);
        let endpoint_id = uuid::Uuid::new_v4();
        let arn = app_arn.replace(":app/", ":endpoint/") + &format!("/{}", endpoint_id);
        let mut attributes = attrs;
        attributes
            .entry("Enabled".to_string())
            .or_insert_with(|| "true".to_string());
        attributes
            .entry("Token".to_string())
            .or_insert_with(|| token.clone());
        if !custom_user_data.is_empty() {
            attributes
                .entry("CustomUserData".to_string())
                .or_insert_with(|| custom_user_data.clone());
        }
        let ep = Arc::new(crate::models::PlatformEndpoint {
            arn: arn.clone(),
            application_arn: app.arn.clone(),
            token,
            custom_user_data,
            attributes: RwLock::new(attributes),
        });
        state.platform_endpoints.write().insert(arn.clone(), ep);
        AwsResponse::query_success(
            "CreatePlatformEndpoint",
            format!("<EndpointArn>{}</EndpointArn>", esc(&arn)),
        )
    }

    fn get_endpoint_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "EndpointArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let ep = state.platform_endpoints.read().get(&arn).cloned();
        match ep {
            Some(ep) => {
                let mut body = String::from("<Attributes>");
                for (k, v) in ep.attributes.read().iter() {
                    body.push_str(&format!(
                        "<entry><key>{}</key><value>{}</value></entry>",
                        esc(k),
                        esc(v)
                    ));
                }
                body.push_str("</Attributes>");
                AwsResponse::query_success("GetEndpointAttributes", body)
            }
            None => AwsResponse::error(
                400,
                "InvalidParameter",
                &format!("Endpoint {} does not exist", arn),
            ),
        }
    }

    fn set_endpoint_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "EndpointArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let ep = state.platform_endpoints.read().get(&arn).cloned();
        if let Some(ep) = ep {
            for (k, v) in parse_entry_attrs(req) {
                ep.attributes.write().insert(k, v);
            }
        }
        AwsResponse::query_success("SetEndpointAttributes", String::new())
    }

    fn delete_endpoint(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "EndpointArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        state.platform_endpoints.write().remove(&arn);
        AwsResponse::query_success("DeleteEndpoint", String::new())
    }

    fn list_endpoints_by_platform_application(&self, req: &AwsRequest) -> AwsResponse {
        let app_arn = pstr(req, "PlatformApplicationArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        if state.platform_applications.read().get(&app_arn).is_none() {
            return Self::not_found(&app_arn);
        }
        let eps = state.platform_endpoints.read();
        let mut body = String::from("<Endpoints>");
        for ep in eps.values() {
            if ep.application_arn != app_arn {
                continue;
            }
            body.push_str("<member>");
            body.push_str(&format!("<EndpointArn>{}</EndpointArn>", esc(&ep.arn)));
            body.push_str("<Attributes>");
            for (k, v) in ep.attributes.read().iter() {
                body.push_str(&format!(
                    "<entry><key>{}</key><value>{}</value></entry>",
                    esc(k),
                    esc(v)
                ));
            }
            body.push_str("</Attributes>");
            body.push_str("</member>");
        }
        body.push_str("</Endpoints>");
        AwsResponse::query_success("ListEndpointsByPlatformApplication", body)
    }

    // ---------- SMS attributes / opted out / sandbox ----------

    fn set_sms_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        for (k, v) in parse_entry_attrs(req) {
            state.sms_attributes.write().insert(k, v);
        }
        AwsResponse::query_success("SetSMSAttributes", String::new())
    }

    fn get_sms_attributes(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let requested: Vec<String> = req
            .params
            .get("attributes")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let map = state.sms_attributes.read();
        let mut body = String::from("<attributes>");
        for (k, v) in map.iter() {
            if !requested.is_empty() && !requested.contains(k) {
                continue;
            }
            body.push_str(&format!(
                "<entry><key>{}</key><value>{}</value></entry>",
                esc(k),
                esc(v)
            ));
        }
        body.push_str("</attributes>");
        AwsResponse::query_success("GetSMSAttributes", body)
    }

    fn list_phone_numbers_opted_out(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let mut numbers: Vec<String> = state.opted_out.read().iter().cloned().collect();
        numbers.sort();
        let body = member_list_xml("phoneNumbers", &numbers);
        AwsResponse::query_success("ListPhoneNumbersOptedOut", body)
    }

    fn check_if_phone_number_is_opted_out(&self, req: &AwsRequest) -> AwsResponse {
        let phone = pstr(req, "phoneNumber").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let opted = state.opted_out.read().contains(&phone);
        AwsResponse::query_success(
            "CheckIfPhoneNumberIsOptedOut",
            format!("<isOptedOut>{}</isOptedOut>", opted),
        )
    }

    fn opt_in_phone_number(&self, req: &AwsRequest) -> AwsResponse {
        let phone = pstr(req, "phoneNumber").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        state.opted_out.write().remove(&phone);
        AwsResponse::query_success("OptInPhoneNumber", String::new())
    }

    fn list_origination_numbers(&self, req: &AwsRequest) -> AwsResponse {
        let _ = req;
        AwsResponse::query_success("ListOriginationNumbers", "<PhoneNumbers/>".into())
    }

    fn get_sms_sandbox_account_status(&self, req: &AwsRequest) -> AwsResponse {
        let _ = req;
        AwsResponse::query_success(
            "GetSMSSandboxAccountStatus",
            "<IsInSandbox>true</IsInSandbox>".into(),
        )
    }

    fn list_sms_sandbox_phone_numbers(&self, req: &AwsRequest) -> AwsResponse {
        let state = self.get_state(req.account, &req.region);
        let nums = state.sms_sandbox_numbers.read();
        let mut members = Vec::new();
        for sandbox in nums.values() {
            members.push(format!(
                "<member><PhoneNumber>{}</PhoneNumber><Status>{}</Status></member>",
                esc(&sandbox.phone),
                esc(sandbox_status(sandbox.verified)),
            ));
        }
        let body = format!("<PhoneNumbers>{}</PhoneNumbers>", members.join(""));
        AwsResponse::query_success("ListSMSSandboxPhoneNumbers", body)
    }

    fn create_sms_sandbox_phone_number(&self, req: &AwsRequest) -> AwsResponse {
        let phone = pstr(req, "PhoneNumber").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        state.sms_sandbox_numbers.write().insert(
            phone.clone(),
            SmsSandboxNumber {
                phone,
                verified: false,
            },
        );
        AwsResponse::query_success("CreateSMSSandboxPhoneNumber", String::new())
    }

    fn verify_sms_sandbox_phone_number(&self, req: &AwsRequest) -> AwsResponse {
        let phone = pstr(req, "PhoneNumber").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let mut nums = state.sms_sandbox_numbers.write();
        match nums.get_mut(&phone) {
            Some(sandbox) => {
                sandbox.verified = true;
                AwsResponse::query_success("VerifySMSSandboxPhoneNumber", String::new())
            }
            None => AwsResponse::error(
                404,
                "ResourceNotFound",
                &format!(
                    "Could not find service sandbox phone number registered with phone number {}",
                    phone
                ),
            ),
        }
    }

    fn delete_sms_sandbox_phone_number(&self, req: &AwsRequest) -> AwsResponse {
        let phone = pstr(req, "PhoneNumber").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        state.sms_sandbox_numbers.write().remove(&phone);
        AwsResponse::query_success("DeleteSMSSandboxPhoneNumber", String::new())
    }

    // ---------- data protection policy ----------

    fn put_data_protection_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "ResourceArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };
        let policy = pstr(req, "DataProtectionPolicy").unwrap_or_default();
        topic.set_attr("DataProtectionPolicy".into(), policy);
        AwsResponse::query_success("PutDataProtectionPolicy", String::new())
    }

    fn get_data_protection_policy(&self, req: &AwsRequest) -> AwsResponse {
        let arn = pstr(req, "ResourceArn").unwrap_or_default();
        let state = self.get_state(req.account, &req.region);
        let topic = match state.get_topic(&arn) {
            Some(t) => t,
            None => return Self::not_found(&arn),
        };
        let policy = topic.attr("DataProtectionPolicy").unwrap_or_default();
        AwsResponse::query_success(
            "GetDataProtectionPolicy",
            format!(
                "<DataProtectionPolicy>{}</DataProtectionPolicy>",
                esc(&policy)
            ),
        )
    }
}

fn sandbox_status(verified: bool) -> &'static str {
    if verified {
        "Verified"
    } else {
        "Pending"
    }
}

// ---------- filter policy evaluation (port of the Python truth) ----------

fn rule_matches(rule: &Value, actual: &str) -> bool {
    match rule {
        Value::String(s) => actual == s,
        Value::Number(n) => match actual.parse::<f64>() {
            Ok(v) => v == n.as_f64().unwrap_or(f64::MIN),
            Err(_) => false,
        },
        Value::Object(obj) => {
            if let Some(prefix) = obj.get("prefix").and_then(|v| v.as_str()) {
                return actual.starts_with(prefix);
            }
            if let Some(suffix) = obj.get("suffix").and_then(|v| v.as_str()) {
                return actual.ends_with(suffix);
            }
            if let Some(conditions) = obj.get("numeric") {
                return numeric_check(conditions, actual);
            }
            if let Some(exists) = obj.get("exists") {
                return exists.as_bool() == Some(true);
            }
            if let Some(exclusions) = obj.get("anything-but") {
                return anything_but(exclusions, actual);
            }
            false
        }
        _ => false,
    }
}

fn numeric_check(conditions: &Value, actual: &str) -> bool {
    let Some(conditions) = conditions.as_array() else {
        return false;
    };
    let Ok(val) = actual.parse::<f64>() else {
        return false;
    };
    let mut i = 0;
    while i + 1 < conditions.len() {
        let op = conditions[i].as_str().unwrap_or_default();
        let Some(threshold) = conditions[i + 1].as_f64() else {
            return false;
        };
        let hit = match op {
            "=" => val == threshold,
            ">" => val > threshold,
            ">=" => val >= threshold,
            "<" => val < threshold,
            "<=" => val <= threshold,
            _ => false,
        };
        if !hit {
            return false;
        }
        i += 2;
    }
    true
}

fn anything_but(exclusion: &Value, actual: &str) -> bool {
    match exclusion {
        Value::Array(list) => {
            for v in list {
                let s = match v {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    _ => continue,
                };
                if s == actual {
                    return false;
                }
            }
            true
        }
        Value::Object(obj) => {
            if let Some(prefix) = obj.get("prefix").and_then(|v| v.as_str()) {
                return !actual.starts_with(prefix);
            }
            true
        }
        Value::String(s) => actual != s,
        Value::Number(n) => match actual.parse::<f64>() {
            Ok(v) => v != n.as_f64().unwrap_or(f64::MIN),
            Err(_) => true,
        },
        _ => true,
    }
}

/// Evaluate a subscription filter policy. `scope` selects MessageBody vs MessageAttributes.
fn matches_filter(
    sub: &Subscription,
    message_attributes: &Map<String, Value>,
    message: &str,
) -> bool {
    let Some(policy_json) = sub.attr("FilterPolicy") else {
        return true;
    };
    if policy_json.trim().is_empty() {
        return true;
    }
    let Ok(policy) = serde_json::from_str::<Value>(&policy_json) else {
        return true;
    };
    let Some(policy_obj) = policy.as_object() else {
        return true;
    };

    let scope = sub
        .attr("FilterPolicyScope")
        .unwrap_or_else(|| "MessageAttributes".into());
    if scope == "MessageBody" {
        let Ok(body) = serde_json::from_str::<Value>(message) else {
            // Body is not JSON: attribute filtering cannot be applied (skip in payload mode)
            return policy_obj.is_empty();
        };
        return policy_matches_value_source(policy_obj, |key| {
            body.get(key).map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
        });
    }

    policy_matches_value_source(policy_obj, |key| {
        message_attributes.get(key).and_then(|attr| {
            attr.get("StringValue")
                .or_else(|| attr.get("Value"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
    })
}

/// Core policy walker shared by both filter sources; `lookup` yields the value of a key.
fn policy_matches_value_source<F>(policy_obj: &Map<String, Value>, lookup: F) -> bool
where
    F: Fn(&str) -> Option<String>,
{
    for (key, rules_value) in policy_obj {
        let rules: Vec<Value> = match rules_value {
            Value::Array(list) => list.clone(),
            other => vec![other.clone()],
        };
        let has_exists_false = rules
            .iter()
            .any(|r| r.get("exists").and_then(|v| v.as_bool()) == Some(false));
        let Some(value) = lookup(key) else {
            if has_exists_false {
                continue;
            }
            return false;
        };
        let matched = rules.iter().any(|rule| {
            if rule.get("exists").and_then(|v| v.as_bool()) == Some(false) {
                return false;
            }
            rule_matches(rule, &value)
        });
        if !matched {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_req(operation: &str, params: Value) -> AwsRequest {
        AwsRequest {
            service: "sns".to_string(),
            operation: operation.to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: bytes::Bytes::new(),
        }
    }

    #[test]
    fn test_create_and_list_topics() {
        let handler = SnsHandler::new();

        let req = make_req("CreateTopic", json!({"Name": "test-topic"}));
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("test-topic"));

        let req = make_req("ListTopics", json!({}));
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("test-topic"));
    }

    #[test]
    fn test_publish_to_nonexistent_topic() {
        let handler = SnsHandler::new();
        let req = make_req(
            "Publish",
            json!({
                "TopicArn": "arn:aws:sns:us-east-1:123456789012:nonexistent",
                "Message": "hello"
            }),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 404);
        assert!(resp.body.contains("NotFound"));
    }

    #[test]
    fn test_subscribe_and_unsubscribe() {
        let handler = SnsHandler::new();

        let req = make_req("CreateTopic", json!({"Name": "test-sub-topic"}));
        let resp = handler.handle(req);
        let topic_arn = {
            use regex::Regex;
            let re = Regex::new(r"<TopicArn>(.*?)</TopicArn>").unwrap();
            re.captures(&resp.body)
                .unwrap()
                .get(1)
                .unwrap()
                .as_str()
                .to_string()
        };

        let req = make_req(
            "Subscribe",
            json!({
                "TopicArn": topic_arn,
                "Protocol": "sqs",
                "Endpoint": "arn:aws:sqs:us-east-1:123456789012:my-queue"
            }),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("SubscriptionArn"));

        // Subscription attributes round trip.
        let sub_arn = {
            use regex::Regex;
            let re = Regex::new(r"<SubscriptionArn>(.*?)</SubscriptionArn>").unwrap();
            re.captures(&resp.body)
                .unwrap()
                .get(1)
                .unwrap()
                .as_str()
                .to_string()
        };
        let req = make_req(
            "SetSubscriptionAttributes",
            json!({
                "SubscriptionArn": sub_arn,
                "AttributeName": "RawMessageDelivery",
                "AttributeValue": "true",
            }),
        );
        assert_eq!(handler.handle(req).status, 200);
        let resp = handler.handle(make_req(
            "GetSubscriptionAttributes",
            json!({"SubscriptionArn": sub_arn}),
        ));
        assert!(resp
            .body
            .contains("<key>RawMessageDelivery</key><value>true</value>"));
    }

    #[test]
    fn test_delete_topic_nonexistent_is_idempotent() {
        let handler = SnsHandler::new();
        let resp = handler.handle(make_req(
            "DeleteTopic",
            json!({"TopicArn": "arn:aws:sns:us-east-1:123456789012:nope"}),
        ));
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn test_sms_attributes_round_trip() {
        let handler = SnsHandler::new();
        let set = make_req(
            "SetSMSAttributes",
            json!({"attributes.entry.1.key": "DefaultSMSType", "attributes.entry.1.value": "Transactional"}),
        );
        assert_eq!(handler.handle(set).status, 200);
        let resp = handler.handle(make_req("GetSMSAttributes", json!({})));
        assert!(resp
            .body
            .contains("<key>DefaultSMSType</key><value>Transactional</value>"));
    }

    #[test]
    fn test_verify_sandbox_unknown_number() {
        let handler = SnsHandler::new();
        let resp = handler.handle(make_req(
            "VerifySMSSandboxPhoneNumber",
            json!({"PhoneNumber": "+19999999999", "OneTimePassword": "000000"}),
        ));
        assert_eq!(resp.status, 404);
        assert!(resp.body.contains("ResourceNotFound"));
    }

    #[test]
    fn test_filter_policy_string_match() {
        let handler = SnsHandler::new();
        // Create topic + SQS queue in the shared SQS store, subscribe, publish.
        let resp = handler.handle(make_req("CreateTopic", json!({"Name": "filter-test"})));
        let topic_arn = {
            use regex::Regex;
            let re = Regex::new(r"<TopicArn>(.*?)</TopicArn>").unwrap();
            re.captures(&resp.body)
                .unwrap()
                .get(1)
                .unwrap()
                .as_str()
                .to_string()
        };

        use sqs::SqsHandler as _;
        let create = handler.sqs.handle(sqs::AwsRequest {
            service: "sqs".into(),
            operation: "CreateQueue".into(),
            account: 123456789012,
            region: "us-east-1".into(),
            params: json!({"QueueName": "filter-test-queue"}),
            body: Bytes::new(),
        });
        let q: Value = serde_json::from_str(&create.body).unwrap();
        let q_arn = format!(
            "arn:aws:sqs:us-east-1:123456789012:{}",
            q["QueueUrl"].as_str().unwrap().rsplit('/').next().unwrap()
        );

        let resp = handler.handle(make_req(
            "Subscribe",
            json!({
                "TopicArn": topic_arn,
                "Protocol": "sqs",
                "Endpoint": q_arn,
                "Attributes.entry.1.key": "FilterPolicy",
                "Attributes.entry.1.value": r#"{"color": ["blue"]}"#,
            }),
        ));
        assert!(resp.body.contains("SubscriptionArn"));

        // Matching message
        let resp = handler.handle(make_req(
            "Publish",
            json!({
                "TopicArn": topic_arn,
                "Message": "blue msg",
                "MessageAttributes.entry.1.Name": "color",
                "MessageAttributes.entry.1.Value.DataType": "String",
                "MessageAttributes.entry.1.Value.StringValue": "blue",
            }),
        ));
        assert_eq!(resp.status, 200);

        let recv = handler.sqs.handle(sqs::AwsRequest {
            service: "sqs".into(),
            operation: "ReceiveMessage".into(),
            account: 123456789012,
            region: "us-east-1".into(),
            params: json!({"QueueUrl": q["QueueUrl"]}),
            body: Bytes::new(),
        });
        let got: Value = serde_json::from_str(&recv.body).unwrap();
        assert_eq!(got["Messages"].as_array().map(Vec::len), Some(1));
        let body = got["Messages"][0]["Body"].as_str().unwrap();
        let envelope: Value = serde_json::from_str(body).unwrap();
        assert_eq!(envelope["Type"], "Notification");
        assert_eq!(envelope["Message"], "blue msg");
        assert_eq!(envelope["MessageAttributes"]["color"]["Value"], "blue");

        // Non-matching message
        handler.handle(make_req(
            "Publish",
            json!({
                "TopicArn": topic_arn,
                "Message": "red msg",
                "MessageAttributes.entry.1.Name": "color",
                "MessageAttributes.entry.1.Value.DataType": "String",
                "MessageAttributes.entry.1.Value.StringValue": "red",
            }),
        ));
        let recv2 = handler.sqs.handle(sqs::AwsRequest {
            service: "sqs".into(),
            operation: "ReceiveMessage".into(),
            account: 123456789012,
            region: "us-east-1".into(),
            params: json!({"QueueUrl": q["QueueUrl"]}),
            body: Bytes::new(),
        });
        let got2: Value = serde_json::from_str(&recv2.body).unwrap();
        // ReceiveMessage on an empty queue carries no Messages member
        let delivered = got2["Messages"].as_array().map(Vec::len).unwrap_or(0);
        assert_eq!(delivered, 0);
    }
}
