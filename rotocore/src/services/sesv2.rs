//! SESv2 backend (rest-json paths under `/v2/email/...`; serves the `sesv2`
//! boto client's full compat-suite surface).
//!
//! State is per (account, region). Configuration sets carry their option
//! blocks inline (`SendingOptions`, `DeliveryOptions`, `ReputationOptions`,
//! `TrackingOptions`, `SuppressionOptions`, `VdmOptions`) plus the event
//! destination list, like moto.
//!
//! Errors follow the JSON convention: non-2xx with `{"__type": code,
//! "message": ...}` — `NotFoundException` / `AlreadyExistsException` codes
//! exactly as the suite asserts.

use std::collections::HashMap;

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, Slot};
use crate::{Request, Response};
use serde_json::{json, Map, Value};

fn percent(len: usize) -> String {
    const ALPHA: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = String::with_capacity(len);
    while out.len() < len {
        for b in uuid::Uuid::new_v4().as_bytes() {
            if out.len() == len {
                break;
            }
            out.push(ALPHA[(*b as usize) % ALPHA.len()] as char);
        }
    }
    out
}

fn ok_json(value: Value) -> Response {
    Response::json_status(200, &value)
}

fn not_found(message: String) -> Response {
    json_error(400, "NotFoundException", &message)
}

/// Names in the botocore model: ConfigurationSetAlreadyExistsException,
/// AlreadyExistsException (templates), CustomVerificationEmailTemplateAlreadyExists.
fn already_exists(code: &str, message: String) -> Response {
    json_error(400, code, &message)
}

fn with_scope<T>(
    slot: &Slot<SesV2State>,
    req: &Request,
    f: impl FnOnce(&mut SesV2State) -> T,
) -> T {
    let mut map = slot.write();
    f(map.entry((req.account, req.region.clone())).or_default())
}

fn body_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// nth path segment after `marker`.
fn seg_after(path: &str, marker: &str) -> String {
    let segs: Vec<&str> = path.split('/').collect();
    match segs.iter().position(|s| *s == marker) {
        Some(pos) => segs
            .get(pos + 1)
            .map(|s| decode_path_seg(s))
            .unwrap_or_default(),
        None => String::new(),
    }
}

fn decode_path_seg(s: &str) -> String {
    s.replace("%40", "@").replace('%', "%")
}

#[derive(Default)]
pub struct SesV2State {
    /// configuration set name → config-set dict (options + event destinations)
    configuration_sets: HashMap<String, Value>,
    /// identity name → identity dict (type, policies, dkim, tags)
    email_identities: HashMap<String, Value>,
    /// contact list name → contact list (description, topics, contacts)
    contact_lists: HashMap<String, Value>,
    /// template name → content
    email_templates: HashMap<String, Value>,
    dedicated_ip_pools: Vec<String>,
    /// custom verification email template name → template
    custom_templates: HashMap<String, Value>,
    /// suppressed email address → {EmailAddress, Reason, LastUpdateTime}
    suppression: HashMap<String, Value>,
    tenants: HashMap<String, Value>,
    multi_region_endpoints: HashMap<String, Value>,
    /// resource-level tags keyed by ResourceArn
    tags: HashMap<String, Value>,
    /// account-level VDM state
    account: Map<String, Value>,
}

impl SesV2State {
    fn config_set_json(name: &str) -> Value {
        json!({
            "ConfigurationSetName": name,
            "SendingOptions": { "SendingEnabled": false },
            "DeliveryOptions": { "TlsPolicy": "OPTIONAL" },
            "ReputationOptions": { "ReputationMetricsEnabled": false },
            "TrackingOptions": {},
            "SuppressionOptions": { "SuppressedReasons": [] },
            "VdmOptions": {},
            "ArchivingOptions": {},
            "EventDestinations": [],
            "Tags": [],
        })
    }
}

fn now_epoch() -> i64 {
    chrono::Utc::now().timestamp()
}

pub fn dispatch(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let op = derive_op(&req.method, &req.path);
    if op.is_empty() {
        return ok_json(json!({}));
    }
    handle(req, &body, &op)
}

/// `(method, segments, op)` — paths as sent by botocore, with the leading
/// `v2`/`email` literals intact. `{Param}` matches one non-empty segment.
fn op_table() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("POST", "v2/email/configuration-sets", "CreateConfigurationSet"),
        ("GET", "v2/email/configuration-sets", "ListConfigurationSets"),
        ("GET", "v2/email/configuration-sets/{ConfigurationSetName}", "GetConfigurationSet"),
        ("DELETE", "v2/email/configuration-sets/{ConfigurationSetName}", "DeleteConfigurationSet"),
        ("PUT", "v2/email/configuration-sets/{ConfigurationSetName}/sending", "PutConfigurationSetSendingOptions"),
        ("PUT", "v2/email/configuration-sets/{ConfigurationSetName}/reputation-options", "PutConfigurationSetReputationOptions"),
        ("PUT", "v2/email/configuration-sets/{ConfigurationSetName}/delivery-options", "PutConfigurationSetDeliveryOptions"),
        ("PUT", "v2/email/configuration-sets/{ConfigurationSetName}/tracking-options", "PutConfigurationSetTrackingOptions"),
        ("PUT", "v2/email/configuration-sets/{ConfigurationSetName}/suppression-options", "PutConfigurationSetSuppressionOptions"),
        ("PUT", "v2/email/configuration-sets/{ConfigurationSetName}/vdm-options", "PutConfigurationSetVdmOptions"),
        ("PUT", "v2/email/configuration-sets/{ConfigurationSetName}/archiving-options", "PutConfigurationSetArchivingOptions"),
        ("POST", "v2/email/configuration-sets/{ConfigurationSetName}/event-destinations", "CreateConfigurationSetEventDestination"),
        ("GET", "v2/email/configuration-sets/{ConfigurationSetName}/event-destinations", "GetConfigurationSetEventDestinations"),
        ("PUT", "v2/email/configuration-sets/{ConfigurationSetName}/event-destinations/{EventDestinationName}", "UpdateConfigurationSetEventDestination"),
        ("DELETE", "v2/email/configuration-sets/{ConfigurationSetName}/event-destinations/{EventDestinationName}", "DeleteConfigurationSetEventDestination"),
        ("POST", "v2/email/identities", "CreateEmailIdentity"),
        ("GET", "v2/email/identities", "ListEmailIdentities"),
        ("GET", "v2/email/identities/{EmailIdentity}", "GetEmailIdentity"),
        ("DELETE", "v2/email/identities/{EmailIdentity}", "DeleteEmailIdentity"),
        ("PUT", "v2/email/identities/{EmailIdentity}/configuration-set", "PutEmailIdentityConfigurationSetAttributes"),
        ("PUT", "v2/email/identities/{EmailIdentity}/dkim", "PutEmailIdentityDkimAttributes"),
        ("PUT", "v2/email/identities/{EmailIdentity}/dkim/signing", "PutEmailIdentityDkimSigningAttributes"),
        ("PUT", "v2/email/identities/{EmailIdentity}/feedback", "PutEmailIdentityFeedbackAttributes"),
        ("PUT", "v2/email/identities/{EmailIdentity}/mail-from", "PutEmailIdentityMailFromAttributes"),
        ("GET", "v2/email/identities/{EmailIdentity}/policies", "GetEmailIdentityPolicies"),
        ("POST", "v2/email/identities/{EmailIdentity}/policies/{PolicyName}", "CreateEmailIdentityPolicy"),
        ("PUT", "v2/email/identities/{EmailIdentity}/policies/{PolicyName}", "UpdateEmailIdentityPolicy"),
        ("DELETE", "v2/email/identities/{EmailIdentity}/policies/{PolicyName}", "DeleteEmailIdentityPolicy"),
        ("POST", "v2/email/contact-lists", "CreateContactList"),
        ("GET", "v2/email/contact-lists", "ListContactLists"),
        ("GET", "v2/email/contact-lists/{ContactListName}", "GetContactList"),
        ("PUT", "v2/email/contact-lists/{ContactListName}", "UpdateContactList"),
        ("DELETE", "v2/email/contact-lists/{ContactListName}", "DeleteContactList"),
        ("POST", "v2/email/contact-lists/{ContactListName}/contacts", "CreateContact"),
        ("POST", "v2/email/contact-lists/{ContactListName}/contacts/list", "ListContacts"),
        ("GET", "v2/email/contact-lists/{ContactListName}/contacts/{EmailAddress}", "GetContact"),
        ("PUT", "v2/email/contact-lists/{ContactListName}/contacts/{EmailAddress}", "UpdateContact"),
        ("DELETE", "v2/email/contact-lists/{ContactListName}/contacts/{EmailAddress}", "DeleteContact"),
        ("POST", "v2/email/templates", "CreateEmailTemplate"),
        ("GET", "v2/email/templates", "ListEmailTemplates"),
        ("GET", "v2/email/templates/{TemplateName}", "GetEmailTemplate"),
        ("PUT", "v2/email/templates/{TemplateName}", "UpdateEmailTemplate"),
        ("DELETE", "v2/email/templates/{TemplateName}", "DeleteEmailTemplate"),
        ("POST", "v2/email/templates/{TemplateName}/render", "TestRenderEmailTemplate"),
        ("POST", "v2/email/custom-verification-email-templates", "CreateCustomVerificationEmailTemplate"),
        ("GET", "v2/email/custom-verification-email-templates", "ListCustomVerificationEmailTemplates"),
        ("GET", "v2/email/custom-verification-email-templates/{TemplateName}", "GetCustomVerificationEmailTemplate"),
        ("PUT", "v2/email/custom-verification-email-templates/{TemplateName}", "UpdateCustomVerificationEmailTemplate"),
        ("DELETE", "v2/email/custom-verification-email-templates/{TemplateName}", "DeleteCustomVerificationEmailTemplate"),
        ("POST", "v2/email/dedicated-ip-pools", "CreateDedicatedIpPool"),
        ("GET", "v2/email/dedicated-ip-pools", "ListDedicatedIpPools"),
        ("GET", "v2/email/dedicated-ip-pools/{PoolName}", "GetDedicatedIpPool"),
        ("DELETE", "v2/email/dedicated-ip-pools/{PoolName}", "DeleteDedicatedIpPool"),
        ("PUT", "v2/email/dedicated-ip-pools/{PoolName}/scaling", "PutDedicatedIpPoolScalingAttributes"),
        ("GET", "v2/email/dedicated-ips", "GetDedicatedIps"),
        ("GET", "v2/email/dedicated-ips/{IP}", "GetDedicatedIp"),
        ("PUT", "v2/email/dedicated-ips/{IP}/pool", "PutDedicatedIpInPool"),
        ("PUT", "v2/email/dedicated-ips/{IP}/warmup", "PutDedicatedIpWarmupAttributes"),
        ("GET", "v2/email/account", "GetAccount"),
        ("PUT", "v2/email/account/details", "PutAccountDetails"),
        ("PUT", "v2/email/account/sending", "PutAccountSendingAttributes"),
        ("PUT", "v2/email/account/suppression", "PutAccountSuppressionAttributes"),
        ("PUT", "v2/email/account/vdm", "PutAccountVdmAttributes"),
        ("PUT", "v2/email/account/dedicated-ips/warmup", "PutAccountDedicatedIpWarmupAttributes"),
        ("POST", "v2/email/tags", "TagResource"),
        ("DELETE", "v2/email/tags", "UntagResource"),
        ("GET", "v2/email/tags", "ListTagsForResource"),
        ("POST", "v2/email/outbound-emails", "SendEmail"),
        ("POST", "v2/email/outbound-bulk-emails", "SendBulkEmail"),
        ("POST", "v2/email/outbound-custom-verification-emails", "SendCustomVerificationEmail"),
        ("GET", "v2/email/suppression/addresses", "ListSuppressedDestinations"),
        ("PUT", "v2/email/suppression/addresses", "PutSuppressedDestination"),
        ("GET", "v2/email/suppression/addresses/{EmailAddress}", "GetSuppressedDestination"),
        ("DELETE", "v2/email/suppression/addresses/{EmailAddress}", "DeleteSuppressedDestination"),
        ("GET", "v2/email/deliverability-dashboard", "GetDeliverabilityDashboardOptions"),
        ("PUT", "v2/email/deliverability-dashboard", "PutDeliverabilityDashboardOption"),
        ("GET", "v2/email/deliverability-dashboard/blacklist-report", "GetBlacklistReports"),
        ("POST", "v2/email/deliverability-dashboard/test", "CreateDeliverabilityTestReport"),
        ("GET", "v2/email/deliverability-dashboard/test-reports", "ListDeliverabilityTestReports"),
        ("GET", "v2/email/deliverability-dashboard/test-reports/{ReportId}", "GetDeliverabilityTestReport"),
        ("GET", "v2/email/deliverability-dashboard/campaigns/{CampaignId}", "GetDomainDeliverabilityCampaign"),
        ("GET", "v2/email/deliverability-dashboard/statistics-report/{Domain}", "GetDomainStatisticsReport"),
        ("GET", "v2/email/deliverability-dashboard/domains/{SubscribedDomain}/campaigns", "ListDomainDeliverabilityCampaigns"),
        ("POST", "v2/email/metrics/batch", "BatchGetMetricData"),
        ("POST", "v2/email/email-address-insights", "GetEmailAddressInsights"),
        ("GET", "v2/email/insights/{MessageId}", "GetMessageInsights"),
        ("POST", "v2/email/tenants", "CreateTenant"),
        ("POST", "v2/email/tenants/get", "GetTenant"),
        ("POST", "v2/email/tenants/delete", "DeleteTenant"),
        ("POST", "v2/email/tenants/list", "ListTenants"),
        ("POST", "v2/email/tenants/resources", "CreateTenantResourceAssociation"),
        ("POST", "v2/email/tenants/resources/list", "ListTenantResources"),
        ("POST", "v2/email/tenants/resources/delete", "DeleteTenantResourceAssociation"),
        ("POST", "v2/email/resources/tenants/list", "ListResourceTenants"),
        ("POST", "v2/email/export-jobs", "CreateExportJob"),
        ("GET", "v2/email/export-jobs/{JobId}", "GetExportJob"),
        ("PUT", "v2/email/export-jobs/{JobId}/cancel", "CancelExportJob"),
        ("POST", "v2/email/list-export-jobs", "ListExportJobs"),
        ("POST", "v2/email/import-jobs", "CreateImportJob"),
        ("GET", "v2/email/import-jobs/{JobId}", "GetImportJob"),
        ("POST", "v2/email/import-jobs/list", "ListImportJobs"),
        ("POST", "v2/email/multi-region-endpoints", "CreateMultiRegionEndpoint"),
        ("GET", "v2/email/multi-region-endpoints", "ListMultiRegionEndpoints"),
        ("GET", "v2/email/multi-region-endpoints/{EndpointName}", "GetMultiRegionEndpoint"),
        ("DELETE", "v2/email/multi-region-endpoints/{EndpointName}", "DeleteMultiRegionEndpoint"),
        ("GET", "v2/email/reputation/entities/{ReputationEntityType}/{ReputationEntityReference}", "GetReputationEntity"),
        ("POST", "v2/email/reputation/entities", "ListReputationEntities"),
        ("PUT", "v2/email/reputation/entities/{ReputationEntityType}/{ReputationEntityReference}/customer-managed-status", "UpdateReputationEntityCustomerManagedStatus"),
        ("PUT", "v2/email/reputation/entities/{ReputationEntityType}/{ReputationEntityReference}/policy", "UpdateReputationEntityPolicy"),
        ("POST", "v2/email/vdm/recommendations", "ListRecommendations"),
    ]
}

/// `{Param}` matches one non-empty segment.
fn segments_match(spec: &[&str], req: &[&str]) -> bool {
    spec.len() == req.len()
        && spec
            .iter()
            .zip(req.iter())
            .all(|(s, r)| (s.starts_with('{') && s.ends_with('}')) || s == r)
}

fn derive_op(method: &str, path: &str) -> String {
    let mut segs: Vec<&str> = path
        .trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .collect();
    if segs.len() > 3 {
        // Keep only `v2/email/...` onward (the router may prefix host style paths).
        if let Some(pos) = segs.iter().position(|s| *s == "v2") {
            segs = segs[pos..].to_vec();
        }
    }
    let refs: Vec<&str> = segs.iter().map(|s| *s).collect();
    for (method_spec, path_spec, op) in op_table() {
        if method_spec != method {
            continue;
        }
        let spec_segs: Vec<&str> = path_spec.split('/').filter(|x| !x.is_empty()).collect();
        if segments_match(&spec_segs, &refs) {
            return op.to_string();
        }
    }
    String::new()
}

pub fn handle(req: &Request, body: &Value, op: &str) -> Response {
    let slot = store::slot::<SesV2State>("sesv2");
    match op {
        // ---- Configuration sets ----
        "CreateConfigurationSet" => {
            let name = body_field(body, "ConfigurationSetName");
            let dup = {
                let map = slot.read();
                map.get(&(req.account, req.region.clone()))
                    .map(|st| st.configuration_sets.contains_key(&name))
                    .unwrap_or(false)
            };
            if dup {
                return already_exists(
                    "AlreadyExistsException",
                    format!("Configuration set {name} already exists"),
                );
            }
            let mut cs = SesV2State::config_set_json(&name);
            if let Some(t) = body.get("Tags") {
                cs["Tags"] = t.clone();
                with_scope(&slot, req, |st| {
                    st.tags.insert(
                        format!(
                            "arn:aws:ses:{}:{}:configuration-set/{name}",
                            req.region, req.account
                        ),
                        t.clone(),
                    );
                });
            }
            with_scope(&slot, req, |st| {
                st.configuration_sets.insert(name.clone(), cs.clone());
            });
            ok_json(json!({}))
        }
        "ListConfigurationSets" => {
            let names: Vec<Value> = with_scope(&slot, req, |st| {
                st.configuration_sets
                    .keys()
                    .map(|n| json!(n.clone()))
                    .collect()
            });
            ok_json(json!({ "ConfigurationSets": names }))
        }
        "GetConfigurationSet" => {
            let name = seg_after(&req.path, "configuration-sets");
            let cs = with_scope(&slot, req, |st| st.configuration_sets.get(&name).cloned());
            match cs {
                Some(c) => ok_json(c),
                None => not_found(format!("Configuration set {name} not found")),
            }
        }
        "DeleteConfigurationSet" => {
            let name = seg_after(&req.path, "configuration-sets");
            let removed = with_scope(&slot, req, |st| {
                st.configuration_sets.remove(&name).is_some()
            });
            if !removed {
                return not_found(format!("Configuration set {name} not found"));
            }
            with_scope(&slot, req, |st| {
                st.tags.remove(&format!(
                    "arn:aws:ses:{}:{}:configuration-set/{name}",
                    req.region, req.account
                ));
            });
            ok_json(json!({}))
        }
        // ---- Identity CRUD ----
        "CreateEmailIdentity" => {
            let name = body_field(body, "EmailIdentity");
            let identity_type = if name.contains('@') {
                "EMAIL_ADDRESS"
            } else {
                "DOMAIN"
            };
            let identity = json!({
                "IdentityName": name,
                "IdentityType": identity_type,
                "SendingEnabled": true,
                "DkimAttributes": { "SigningEnabled": true, "Status": "PENDING" },
                "Policies": {},
                "Tags": body.get("Tags").cloned().unwrap_or(json!([])),
            });
            with_scope(&slot, req, |st| {
                st.email_identities.insert(name.clone(), identity.clone());
            });
            ok_json(identity)
        }
        "ListEmailIdentities" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.email_identities
                    .values()
                    .map(|i| json!({ "IdentityName": i["IdentityName"], "IdentityType": i["IdentityType"] }))
                    .collect()
            });
            ok_json(json!({ "EmailIdentities": list }))
        }
        "GetEmailIdentity" => {
            let name = seg_after(&req.path, "identities");
            let got = with_scope(&slot, req, |st| st.email_identities.get(&name).cloned());
            match got {
                Some(i) => ok_json(i),
                None => not_found(format!("Email identity {name} not found")),
            }
        }
        "DeleteEmailIdentity" => {
            let name = seg_after(&req.path, "identities");
            let removed = with_scope(&slot, req, |st| st.email_identities.remove(&name).is_some());
            if !removed {
                return not_found(format!("Email identity {name} not found"));
            }
            ok_json(json!({}))
        }
        "GetEmailIdentityPolicies" => {
            let name = seg_after(&req.path, "identities");
            let policies = with_scope(&slot, req, |st| {
                st.email_identities
                    .get(&name)
                    .map(|i| i["Policies"].clone())
            });
            match policies {
                Some(p) => ok_json(json!({ "Policies": p })),
                None => not_found(format!("Email identity {name} not found")),
            }
        }
        "CreateEmailIdentityPolicy" | "UpdateEmailIdentityPolicy" => {
            let name = seg_after(&req.path, "identities");
            let policy_name = seg_after(&req.path, "policies");
            let policy = body_field(body, "Policy");
            let done = with_scope(&slot, req, |st| {
                st.email_identities.get_mut(&name).map(|i| {
                    let mut policies = i["Policies"].as_object().cloned().unwrap_or_default();
                    policies.insert(policy_name.clone(), json!(policy));
                    i["Policies"] = Value::Object(policies);
                })
            });
            match done {
                Some(_) => ok_json(json!({})),
                None => not_found(format!("Email identity {name} not found")),
            }
        }
        "DeleteEmailIdentityPolicy" => {
            let name = seg_after(&req.path, "identities");
            let policy_name = seg_after(&req.path, "policies");
            let done = with_scope(&slot, req, |st| {
                st.email_identities.get_mut(&name).map(|i| {
                    if let Some(policies) = i["Policies"].as_object_mut() {
                        policies.remove(&policy_name);
                    }
                })
            });
            match done {
                Some(_) => ok_json(json!({})),
                None => not_found(format!("Email identity {name} not found")),
            }
        }
        _ => handler_2(req, &body, op, slot),
    }
}

fn handler_2(req: &Request, body: &Value, op: &str, slot: Slot<SesV2State>) -> Response {
    match op {
        "PutEmailIdentityConfigurationSetAttributes" => {
            let name = seg_after(&req.path, "identities");
            let found = with_scope(&slot, req, |st| st.email_identities.contains_key(&name));
            if !found {
                return not_found(format!("Email identity {name} not found"));
            }
            ok_json(json!({}))
        }
        "PutEmailIdentityDkimAttributes"
        | "PutEmailIdentityFeedbackAttributes"
        | "PutEmailIdentityMailFromAttributes" => {
            let name = seg_after(&req.path, "identities");
            let found = with_scope(&slot, req, |st| st.email_identities.contains_key(&name));
            if !found {
                return not_found(format!("Email identity {name} not found"));
            }
            ok_json(json!({}))
        }
        "PutEmailIdentityDkimSigningAttributes" => {
            let name = seg_after(&req.path, "identities");
            let found = with_scope(&slot, req, |st| st.email_identities.contains_key(&name));
            if !found {
                return not_found(format!("Email identity {name} not found"));
            }
            ok_json(
                json!({ "DkimStatus": "PENDING", "DkimTokens": [percent(16), percent(16), percent(16)] }),
            )
        }
        _ => handler_3(req, body, op, slot),
    }
}

fn handler_3(req: &Request, body: &Value, op: &str, slot: Slot<SesV2State>) -> Response {
    let cs_name = |path: &str| seg_after(path, "configuration-sets");
    match op {
        // ---- Configuration set options (all persist into the set) ----
        "PutConfigurationSetSendingOptions" => {
            let name = cs_name(&req.path);
            let found = with_scope(&slot, req, |st| st.configuration_sets.contains_key(&name));
            if !found {
                return not_found(format!("Configuration set {name} not found"));
            }
            with_scope(&slot, req, |st| {
                if let Some(cs) = st.configuration_sets.get_mut(&name) {
                    cs["SendingOptions"] = json!({ "SendingEnabled": body.get("SendingEnabled").and_then(Value::as_bool).unwrap_or(false) });
                }
            });
            ok_json(json!({}))
        }
        "PutConfigurationSetReputationOptions" => {
            let name = cs_name(&req.path);
            let found = with_scope(&slot, req, |st| st.configuration_sets.contains_key(&name));
            if !found {
                return not_found(format!("Configuration set {name} not found"));
            }
            with_scope(&slot, req, |st| {
                if let Some(cs) = st.configuration_sets.get_mut(&name) {
                    cs["ReputationOptions"] = json!({ "ReputationMetricsEnabled": body.get("ReputationMetricsEnabled").and_then(Value::as_bool).unwrap_or(false) });
                }
            });
            ok_json(json!({}))
        }
        "PutConfigurationSetDeliveryOptions" => {
            let name = cs_name(&req.path);
            let found = with_scope(&slot, req, |st| st.configuration_sets.contains_key(&name));
            if !found {
                return not_found(format!("Configuration set {name} not found"));
            }
            with_scope(&slot, req, |st| {
                if let Some(cs) = st.configuration_sets.get_mut(&name) {
                    cs["DeliveryOptions"] = json!({ "TlsPolicy": body_field(body, "TlsPolicy") });
                }
            });
            ok_json(json!({}))
        }
        "PutConfigurationSetTrackingOptions" => {
            let name = cs_name(&req.path);
            let found = with_scope(&slot, req, |st| st.configuration_sets.contains_key(&name));
            if !found {
                return not_found(format!("Configuration set {name} not found"));
            }
            with_scope(&slot, req, |st| {
                if let Some(cs) = st.configuration_sets.get_mut(&name) {
                    cs["TrackingOptions"] =
                        json!({ "CustomRedirectDomain": body_field(body, "CustomRedirectDomain") });
                }
            });
            ok_json(json!({}))
        }
        "PutConfigurationSetSuppressionOptions" => {
            let name = cs_name(&req.path);
            let found = with_scope(&slot, req, |st| st.configuration_sets.contains_key(&name));
            if !found {
                return not_found(format!("Configuration set {name} not found"));
            }
            with_scope(&slot, req, |st| {
                if let Some(cs) = st.configuration_sets.get_mut(&name) {
                    cs["SuppressionOptions"] = json!({ "SuppressedReasons": body.get("SuppressedReasons").cloned().unwrap_or(json!([])) });
                }
            });
            ok_json(json!({}))
        }
        "PutConfigurationSetVdmOptions" => {
            let name = cs_name(&req.path);
            let found = with_scope(&slot, req, |st| st.configuration_sets.contains_key(&name));
            if !found {
                return not_found(format!("Configuration set {name} not found"));
            }
            with_scope(&slot, req, |st| {
                if let Some(cs) = st.configuration_sets.get_mut(&name) {
                    cs["VdmOptions"] = body.get("VdmOptions").cloned().unwrap_or(json!({}));
                }
            });
            ok_json(json!({}))
        }
        "PutConfigurationSetArchivingOptions" => {
            let name = cs_name(&req.path);
            let found = with_scope(&slot, req, |st| st.configuration_sets.contains_key(&name));
            if !found {
                return not_found(format!("Configuration set {name} not found"));
            }
            ok_json(json!({}))
        }
        // ---- Event destinations ----
        "CreateConfigurationSetEventDestination" => {
            let name = cs_name(&req.path);
            let dest_name = body_field(body, "EventDestinationName");
            let dest = body.get("EventDestination").cloned().unwrap_or(json!({}));
            let ok = with_scope(&slot, req, |st| {
                st.configuration_sets.get_mut(&name).map(|cs| {
                    if !cs["EventDestinations"].is_array() {
                        cs["EventDestinations"] = json!([]);
                    }
                    let mut d = dest.clone();
                    d["Name"] = json!(dest_name.clone());
                    cs["EventDestinations"].as_array_mut().unwrap().push(d);
                })
            });
            match ok {
                Some(_) => ok_json(json!({})),
                None => not_found(format!("Configuration set {name} not found")),
            }
        }
        "GetConfigurationSetEventDestinations" => {
            let name = cs_name(&req.path);
            let dests = with_scope(&slot, req, |st| {
                st.configuration_sets
                    .get(&name)
                    .map(|cs| cs["EventDestinations"].clone())
            });
            match dests {
                Some(d) => ok_json(json!({ "EventDestinations": d })),
                None => not_found(format!("Configuration set {name} not found")),
            }
        }
        "UpdateConfigurationSetEventDestination" => {
            let name = cs_name(&req.path);
            let dest_name = seg_after(&req.path, "event-destinations");
            let dest = body.get("EventDestination").cloned().unwrap_or(json!({}));
            let ok = with_scope(&slot, req, |st| {
                st.configuration_sets.get_mut(&name).map(|cs| {
                    if let Some(list) = cs["EventDestinations"].as_array_mut() {
                        for entry in list.iter_mut() {
                            if entry["Name"].as_str() == Some(&dest_name) {
                                *entry = {
                                    let mut d = dest.clone();
                                    d["Name"] = json!(dest_name.clone());
                                    d
                                };
                            }
                        }
                    }
                })
            });
            match ok {
                Some(_) => ok_json(json!({})),
                None => not_found(format!("Configuration set {name} not found")),
            }
        }
        "DeleteConfigurationSetEventDestination" => {
            let name = cs_name(&req.path);
            let dest_name = seg_after(&req.path, "event-destinations");
            let ok = with_scope(&slot, req, |st| {
                st.configuration_sets.get_mut(&name).map(|cs| {
                    if let Some(list) = cs["EventDestinations"].as_array_mut() {
                        list.retain(|e| e["Name"].as_str() != Some(&dest_name));
                    }
                })
            });
            match ok {
                Some(_) => ok_json(json!({})),
                None => not_found(format!("Configuration set {name} not found")),
            }
        }
        _ => handler_4(req, body, op, slot),
    }
}

fn handler_4(req: &Request, body: &Value, op: &str, slot: Slot<SesV2State>) -> Response {
    match op {
        // ---- Contact lists + contacts ----
        "CreateContactList" => {
            let name = body_field(body, "ContactListName");
            let list = json!({
                "ContactListName": name,
                "Description": body_field(body, "Description"),
                "Topics": body.get("Topics").cloned().unwrap_or(json!([])),
            });
            with_scope(&slot, req, |st| {
                st.contact_lists.insert(name.clone(), list.clone());
            });
            ok_json(json!({}))
        }
        "ListContactLists" => {
            let lists: Vec<Value> = with_scope(&slot, req, |st| {
                st.contact_lists.values().map(|cl| cl.clone()).collect()
            });
            ok_json(json!({ "ContactLists": lists }))
        }
        "GetContactList" => {
            let name = seg_after(&req.path, "contact-lists");
            let got = with_scope(&slot, req, |st| st.contact_lists.get(&name).cloned());
            match got {
                Some(c) => ok_json(c),
                None => not_found(format!("Contact list {name} not found")),
            }
        }
        "UpdateContactList" => {
            let name = seg_after(&req.path, "contact-lists");
            let ok = with_scope(&slot, req, |st| {
                st.contact_lists.get_mut(&name).map(|cl| {
                    if let Some(d) = body.get("Description").and_then(Value::as_str) {
                        cl["Description"] = json!(d);
                    }
                    if let Some(t) = body.get("Topics") {
                        cl["Topics"] = t.clone();
                    }
                })
            });
            match ok {
                Some(_) => ok_json(json!({})),
                None => not_found(format!("Contact list {name} not found")),
            }
        }
        "DeleteContactList" => {
            let name = seg_after(&req.path, "contact-lists");
            let removed = with_scope(&slot, req, |st| st.contact_lists.remove(&name).is_some());
            if !removed {
                return not_found(format!("Contact list {name} not found"));
            }
            ok_json(json!({}))
        }
        "CreateContact" => {
            let name = seg_after(&req.path, "contact-lists");
            let email = body_field(body, "EmailAddress");
            let exists = with_scope(&slot, req, |st| st.contact_lists.contains_key(&name));
            if !exists {
                return not_found(format!("Contact list {name} not found"));
            }
            let mut contact = json!({
                "EmailAddress": email,
                "TopicPreferences": [],
                "TopicDefaultPreferences": [],
                "UnsubscribeAll": false,
            });
            if let Some(p) = body.get("TopicPreferences") {
                contact["TopicPreferences"] = p.clone();
            }
            if let Some(b) = body.get("UnsubscribeAll") {
                contact["UnsubscribeAll"] = b.clone();
            }
            with_scope(&slot, req, |st| {
                if let Some(cl) = st.contact_lists.get_mut(&name) {
                    if !cl["Contacts"].is_array() {
                        cl["Contacts"] = json!([]);
                    }
                    cl["Contacts"].as_array_mut().unwrap().push(contact);
                }
            });
            ok_json(json!({}))
        }
        "ListContacts" => {
            let name = seg_after(&req.path, "contact-lists");
            let contacts = with_scope(&slot, req, |st| {
                st.contact_lists.get(&name).map(|cl| cl["Contacts"].clone())
            });
            match contacts {
                Some(c) => ok_json(json!({ "Contacts": c })),
                None => not_found(format!("Contact list {name} not found")),
            }
        }
        "GetContact" => {
            let name = seg_after(&req.path, "contact-lists");
            let email = seg_after(&req.path, "contacts");
            let contact = with_scope(&slot, req, |st| {
                st.contact_lists.get(&name).and_then(|cl| {
                    cl["Contacts"].as_array().and_then(|c| {
                        c.iter()
                            .find(|x| x["EmailAddress"].as_str() == Some(&email))
                            .cloned()
                    })
                })
            });
            match contact {
                Some(c) => ok_json(c),
                None => not_found(format!("Contact {email} not found in {name}")),
            }
        }
        "UpdateContact" => {
            let name = seg_after(&req.path, "contact-lists");
            let email = seg_after(&req.path, "contacts");
            let found = with_scope(&slot, req, |st| {
                st.contact_lists.get_mut(&name).and_then(|cl| {
                    cl["Contacts"].as_array_mut().and_then(|c| {
                        c.iter_mut()
                            .find(|x| x["EmailAddress"].as_str() == Some(&email))
                            .map(|contact| {
                                if let Some(b) = body.get("UnsubscribeAll") {
                                    contact["UnsubscribeAll"] = b.clone();
                                }
                                if let Some(p) = body.get("TopicPreferences") {
                                    contact["TopicPreferences"] = p.clone();
                                }
                            })
                    })
                })
            });
            match found {
                Some(_) => ok_json(json!({})),
                None => not_found(format!("Contact {email} not found in {name}")),
            }
        }
        "DeleteContact" => {
            let name = seg_after(&req.path, "contact-lists");
            let email = seg_after(&req.path, "contacts");
            let removed = with_scope(&slot, req, |st| {
                st.contact_lists
                    .get_mut(&name)
                    .and_then(|cl| {
                        cl["Contacts"].as_array_mut().map(|c| {
                            let before = c.len();
                            c.retain(|x| x["EmailAddress"].as_str() != Some(&email));
                            c.len() != before
                        })
                    })
                    .unwrap_or(false)
            });
            match removed {
                true => ok_json(json!({})),
                false => not_found(format!("Contact {email} not found in {name}")),
            }
        }
        _ => handler_5(req, body, op, slot),
    }
}

fn handler_5(req: &Request, body: &Value, op: &str, slot: Slot<SesV2State>) -> Response {
    match op {
        // ---- Email templates ----
        "CreateEmailTemplate" => {
            let name = body_field(body, "TemplateName");
            let dup = {
                let map = slot.read();
                map.get(&(req.account, req.region.clone()))
                    .map(|st| st.email_templates.contains_key(&name))
                    .unwrap_or(false)
            };
            if dup {
                return already_exists(
                    "AlreadyExistsException",
                    format!("The email template {name} already exists"),
                );
            }
            let content = body.get("TemplateContent").cloned().unwrap_or(json!({}));
            with_scope(&slot, req, |st| {
                st.email_templates.insert(name.clone(), content.clone());
            });
            ok_json(json!({}))
        }
        "ListEmailTemplates" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.email_templates
                    .keys()
                    .map(|n| json!({ "TemplateName": n.clone(), "CreatedTimestamp": now_epoch() }))
                    .collect()
            });
            ok_json(json!({ "TemplatesMetadata": list }))
        }
        "GetEmailTemplate" => {
            let name = seg_after(&req.path, "templates");
            let got = with_scope(&slot, req, |st| st.email_templates.get(&name).cloned());
            match got {
                Some(content) => {
                    ok_json(json!({ "TemplateName": name, "TemplateContent": content }))
                }
                None => not_found(format!("No template found for name {name}")),
            }
        }
        "UpdateEmailTemplate" => {
            let name = seg_after(&req.path, "templates");
            let found = with_scope(&slot, req, |st| st.email_templates.contains_key(&name));
            if !found {
                return not_found(format!(
                    "The email template with name {name} does not exist"
                ));
            }
            let content = body.get("TemplateContent").cloned().unwrap_or(json!({}));
            with_scope(&slot, req, |st| {
                st.email_templates.insert(name.clone(), content);
            });
            ok_json(json!({}))
        }
        "DeleteEmailTemplate" => {
            let name = seg_after(&req.path, "templates");
            with_scope(&slot, req, |st| {
                st.email_templates.remove(&name);
            });
            ok_json(json!({}))
        }
        "TestRenderEmailTemplate" => {
            let name = body_field(body, "TemplateName");
            let got = with_scope(&slot, req, |st| st.email_templates.get(&name).cloned());
            match got {
                Some(content) => ok_json(json!({ "RenderedTemplate": content })),
                None => not_found(format!("The email template {name} does not exist")),
            }
        }
        // ---- Custom verification email templates ----
        "CreateCustomVerificationEmailTemplate" => {
            let name = body_field(body, "TemplateName");
            let record = json!({
                "TemplateName": name,
                "FromEmailAddress": body_field(body, "FromEmailAddress"),
                "TemplateSubject": body_field(body, "TemplateSubject"),
                "TemplateContent": body_field(body, "TemplateContent"),
                "SuccessRedirectionURL": body_field(body, "SuccessRedirectionURL"),
                "FailureRedirectionURL": body_field(body, "FailureRedirectionURL"),
            });
            with_scope(&slot, req, |st| {
                st.custom_templates.insert(name.clone(), record.clone());
            });
            ok_json(json!({}))
        }
        "ListCustomVerificationEmailTemplates" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.custom_templates.values().map(|c| c.clone()).collect()
            });
            ok_json(json!({ "CustomVerificationEmailTemplates": list }))
        }
        "GetCustomVerificationEmailTemplate" => {
            let name = seg_after(&req.path, "custom-verification-email-templates");
            let got = with_scope(&slot, req, |st| st.custom_templates.get(&name).cloned());
            match got {
                Some(c) => ok_json(c),
                None => not_found(format!(
                    "Custom verification email template {name} does not exist"
                )),
            }
        }
        "UpdateCustomVerificationEmailTemplate" => {
            let name = seg_after(&req.path, "custom-verification-email-templates");
            let found = with_scope(&slot, req, |st| st.custom_templates.contains_key(&name));
            if !found {
                return not_found(format!(
                    "Custom verification email template {name} does not exist"
                ));
            }
            with_scope(&slot, req, |st| {
                if let Some(c) = st.custom_templates.get_mut(&name) {
                    for key in [
                        "FromEmailAddress",
                        "TemplateSubject",
                        "TemplateContent",
                        "SuccessRedirectionURL",
                        "FailureRedirectionURL",
                    ] {
                        if let Some(v) = body.get(key) {
                            c[key] = v.clone();
                        }
                    }
                }
            });
            ok_json(json!({}))
        }
        "DeleteCustomVerificationEmailTemplate" => {
            let name = seg_after(&req.path, "custom-verification-email-templates");
            let removed = with_scope(&slot, req, |st| st.custom_templates.remove(&name).is_some());
            if !removed {
                return not_found(format!(
                    "Custom verification email template {name} does not exist"
                ));
            }
            ok_json(json!({}))
        }
        _ => handler_6(req, body, op, slot),
    }
}

fn handler_6(req: &Request, body: &Value, op: &str, slot: Slot<SesV2State>) -> Response {
    match op {
        // ---- Dedicated IP pools + IPs ----
        "CreateDedicatedIpPool" => {
            let name = body_field(body, "PoolName");
            with_scope(&slot, req, |st| {
                if !st.dedicated_ip_pools.contains(&name) {
                    st.dedicated_ip_pools.push(name.clone());
                }
            });
            ok_json(json!({}))
        }
        "ListDedicatedIpPools" => {
            let names: Vec<Value> = with_scope(&slot, req, |st| {
                st.dedicated_ip_pools
                    .iter()
                    .map(|n| json!(n.clone()))
                    .collect()
            });
            ok_json(json!({ "DedicatedIpPools": names }))
        }
        "GetDedicatedIpPool" => {
            let name = seg_after(&req.path, "dedicated-ip-pools");
            let exists = with_scope(&slot, req, |st| st.dedicated_ip_pools.contains(&name));
            if !exists {
                return not_found(format!("Dedicated IP pool {name} not found"));
            }
            ok_json(json!({ "DedicatedIpPool": { "PoolName": name, "ScalingMode": "STANDARD" } }))
        }
        "DeleteDedicatedIpPool" => {
            let name = seg_after(&req.path, "dedicated-ip-pools");
            let removed = with_scope(&slot, req, |st| {
                let before = st.dedicated_ip_pools.len();
                st.dedicated_ip_pools.retain(|p| *p != name);
                st.dedicated_ip_pools.len() != before
            });
            if !removed {
                return not_found(format!("Dedicated IP pool {name} not found"));
            }
            ok_json(json!({}))
        }
        "PutDedicatedIpPoolScalingAttributes" => {
            let name = seg_after(&req.path, "dedicated-ip-pools");
            let exists = with_scope(&slot, req, |st| st.dedicated_ip_pools.contains(&name));
            if !exists {
                return not_found(format!("Dedicated IP pool {name} not found"));
            }
            ok_json(json!({}))
        }
        "GetDedicatedIps" => ok_json(json!({ "DedicatedIps": [] })),
        "GetDedicatedIp" => {
            let ip = seg_after(&req.path, "dedicated-ips");
            let resp = json_error(400, "NotFoundException", &format!("{ip} not available"));
            resp
        }
        "PutDedicatedIpInPool" | "PutDedicatedIpWarmupAttributes" => ok_json(json!({})),
        // ---- Account ops ----
        "GetAccount" => {
            let account_state = with_scope(&slot, req, |st| st.account.clone());
            let mut out = json!({
                "SendQuota": { "MaxSendRate": 14.0, "Max24HourSend": 200000.0, "SentLast24Hours": 0.0 },
                "SendingEnabled": true,
                "EnforcementStatus": "HEALTHY",
                "DedicatedIpAutoWarmupEnabled": true,
                "MailType": "MARKETING",
                "ProductionAccessEnabled": true,
            });
            for (k, v) in account_state {
                out[k] = v;
            }
            ok_json(out)
        }
        "PutAccountDetails"
        | "PutAccountSendingAttributes"
        | "PutAccountSuppressionAttributes"
        | "PutAccountVdmAttributes"
        | "PutAccountDedicatedIpWarmupAttributes" => {
            with_scope(&slot, req, |st| {
                for (k, v) in body.as_object().map(|m| m.clone()).unwrap_or_default() {
                    st.account.insert(k, v);
                }
            });
            ok_json(json!({}))
        }
        _ => handler_7(req, body, op, slot),
    }
}

fn handler_7(req: &Request, body: &Value, op: &str, slot: Slot<SesV2State>) -> Response {
    match op {
        // ---- Tags ----
        "TagResource" => {
            let arn = body_field(body, "ResourceArn");
            eprintln!(
                "DBG_TAG arn={arn:?} body_keys={:?}",
                body.as_object()
                    .map(|m| m.keys().cloned().collect::<Vec<_>>())
            );
            with_scope(&slot, req, |st| {
                let entry = st.tags.entry(arn.clone()).or_insert(json!({}));
                if !entry.is_object() {
                    *entry = json!({});
                }
                let map = entry.as_object_mut().unwrap();
                if let Some(tags) = body.get("Tags").and_then(Value::as_array) {
                    for t in tags {
                        if let (Some(k), Some(v)) = (
                            t.get("Key").and_then(Value::as_str),
                            t.get("Value").and_then(Value::as_str),
                        ) {
                            map.insert(k.to_string(), json!(v));
                        }
                    }
                }
            });
            ok_json(json!({}))
        }
        "UntagResource" => {
            let mut arn = body_field(body, "ResourceArn");
            if arn.is_empty() {
                arn = query_param(&req.query_string, "ResourceArn");
            }
            let mut keys: Vec<String> = body
                .get("TagKeys")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            for (k, v) in req
                .query_string
                .split('&')
                .filter_map(|kv| kv.split_once('='))
            {
                if k == "TagKeys" {
                    keys.push(urldecode_query(v));
                }
            }
            with_scope(&slot, req, |st| {
                if let Some(entry) = st.tags.get_mut(&arn) {
                    if let Some(map) = entry.as_object_mut() {
                        for kk in keys {
                            map.remove(&kk);
                        }
                    }
                }
            });
            ok_json(json!({}))
        }
        "ListTagsForResource" => {
            let mut arn = body_field(body, "ResourceArn");
            if arn.is_empty() {
                arn = query_param(&req.query_string, "ResourceArn");
            }
            let tags = with_scope(&slot, req, |st| {
                st.tags.get(&arn).cloned().unwrap_or(json!({}))
            });
            let list: Vec<Value> = tags
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| json!({ "Key": k, "Value": v }))
                        .collect()
                })
                .unwrap_or_default();
            ok_json(json!({ "Tags": list }))
        }
        // ---- Send operations ----
        "SendEmail" => ok_json(json!({ "MessageId": percent(32) })),
        "SendBulkEmail" => {
            let entries = body
                .get("BulkEmailEntries")
                .and_then(Value::as_array)
                .map(|a| a.len())
                .unwrap_or(0);
            let results: Vec<Value> = (0..entries)
                .map(|_| json!({ "Status": "SUCCESS", "MessageId": percent(32) }))
                .collect();
            ok_json(json!({ "BulkEmailEntryResults": results }))
        }
        "SendCustomVerificationEmail" => ok_json(json!({})),
        // ---- Suppression ----
        "ListSuppressedDestinations" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.suppression
                    .values()
                    .map(|s| {
                        json!({
                            "EmailAddress": s["EmailAddress"].clone(),
                            "Reason": s["Reason"].clone(),
                            "LastUpdateTime": now_epoch(),
                        })
                    })
                    .collect()
            });
            ok_json(json!({ "SuppressedDestinationSummaries": list }))
        }
        "PutSuppressedDestination" => {
            let email = body_field(body, "EmailAddress");
            let record = json!({
                "EmailAddress": email,
                "Reason": body_field(body, "Reason"),
                "LastUpdateTime": now_epoch(),
            });
            with_scope(&slot, req, |st| {
                st.suppression.insert(email.clone(), record.clone());
            });
            ok_json(json!({}))
        }
        "GetSuppressedDestination" => {
            let email = seg_after(&req.path, "addresses");
            let got = with_scope(&slot, req, |st| st.suppression.get(&email).cloned());
            match got {
                Some(s) => ok_json(json!({ "SuppressedDestination": s })),
                None => not_found(format!("Suppressed destination {email} not found")),
            }
        }
        "DeleteSuppressedDestination" => {
            let email = seg_after(&req.path, "addresses");
            with_scope(&slot, req, |st| {
                st.suppression.remove(&email);
            });
            ok_json(json!({}))
        }
        _ => handler_8(req, body, op, slot),
    }
}

fn handler_8(req: &Request, body: &Value, op: &str, slot: Slot<SesV2State>) -> Response {
    match op {
        // ---- Deliverability dashboard ----
        "GetDeliverabilityDashboardOptions" => {
            let enabled = with_scope(&slot, req, |st| {
                st.account
                    .get("DashboardEnabled")
                    .cloned()
                    .unwrap_or(json!(false))
            });
            ok_json(json!({ "DashboardEnabled": enabled, "AccountEnabled": enabled }))
        }
        "PutDeliverabilityDashboardOption" => {
            let enabled = body
                .get("DashboardEnabled")
                .cloned()
                .unwrap_or(json!(false));
            with_scope(&slot, req, |st| {
                st.account.insert("DashboardEnabled".to_string(), enabled);
            });
            ok_json(json!({}))
        }
        "GetBlacklistReports" => {
            let mut report = Map::new();
            if let Some(names) = names_list(body) {
                for n in names {
                    report.insert(
                        n,
                        json!([
                        { "Rbl": "spam", "LastSeenTime": now_epoch() } ]),
                    );
                }
            }
            ok_json(Value::Object({
                let mut o = Map::new();
                o.insert("BlacklistReport".to_string(), Value::Object(report));
                o
            }))
        }
        "CreateDeliverabilityTestReport" => {
            ok_json(json!({ "ReportId": format!("rep-{}", percent(16)), "Status": "COMPLETED" }))
        }
        "ListDeliverabilityTestReports" => ok_json(json!({ "DeliverabilityTestReports": [] })),
        "GetDeliverabilityTestReport" => ok_json(json!({
            "ReportId": seg_after(&req.path, "test-reports"),
            "Status": "COMPLETED",
        })),
        "GetDomainDeliverabilityCampaign" => ok_json(json!({ "DomainDeliverabilityCampaign": {
                "CampaignId": body_field(body, "CampaignId"),
                "EmailAddress": "campaign@example.com",
            } })),
        "GetDomainStatisticsReport" => ok_json(json!({ "OverallVolume": {} })),
        "ListDomainDeliverabilityCampaigns" => {
            ok_json(json!({ "DomainDeliverabilityCampaigns": [] }))
        }
        // ---- Metrics / insights ----
        "BatchGetMetricData" => {
            let queries = body
                .get("Queries")
                .and_then(Value::as_array)
                .map(|a| a.len())
                .unwrap_or(0);
            let results: Vec<Value> = (0..queries)
                .map(|i| json!({ "Id": format!("q{}", i), "Metric": "SEND", "Value": 0.0 }))
                .collect();
            ok_json(json!({ "Results": results }))
        }
        "GetEmailAddressInsights" => {
            ok_json(json!({ "EmailAddress": body_field(body, "EmailAddress") }))
        }
        "GetMessageInsights" => {
            let id = seg_after(&req.path, "insights");
            ok_json(json!({ "MessageId": id }))
        }
        // ---- Tenants ----
        "CreateTenant" => {
            let name = body_field(body, "TenantName");
            let id = format!("tenant-{}", percent(12));
            with_scope(&slot, req, |st| {
                st.tenants.insert(
                    name.clone(),
                    json!({ "TenantName": name, "TenantId": id.clone() }),
                );
            });
            ok_json(json!({ "TenantId": id }))
        }
        "GetTenant" => {
            let name = body_field(body, "TenantName");
            let exists = with_scope(&slot, req, |st| st.tenants.contains_key(&name));
            if !exists {
                return not_found(format!("Tenant {name} not found"));
            }
            ok_json(json!({ "TenantName": name }))
        }
        "DeleteTenant" => {
            let name = body_field(body, "TenantName");
            with_scope(&slot, req, |st| {
                st.tenants.remove(&name);
            });
            ok_json(json!({}))
        }
        "ListTenants" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.tenants.values().map(|t| t.clone()).collect()
            });
            ok_json(json!({ "Tenants": list }))
        }
        "CreateTenantResourceAssociation" | "DeleteTenantResourceAssociation" => ok_json(json!({})),
        "ListTenantResources" => ok_json(json!({ "TenantResources": [] })),
        "ListResourceTenants" => ok_json(json!({ "ResourceTenants": [] })),
        // ---- Export / import jobs ----
        "CreateExportJob" => {
            ok_json(json!({ "JobId": format!("job-{}", percent(16)), "Status": "PENDING" }))
        }
        "GetExportJob" => ok_json(json!({ "JobStatus": "COMPLETED" })),
        "CancelExportJob" => ok_json(json!({})),
        "ListExportJobs" => ok_json(json!({ "ExportJobs": [] })),
        "CreateImportJob" => ok_json(
            json!({ "JobId": format!("job-{}", percent(16)), "Status": "COMPLETED", "CreatedTimestamp": now_epoch() }),
        ),
        "GetImportJob" => ok_json(json!({
            "ImportDestination": body.get("ImportDestination").cloned().unwrap_or(json!({})),
            "JobStatus": "COMPLETED",
        })),
        "ListImportJobs" => ok_json(json!({ "ImportJobs": [] })),
        // ---- Multi-region endpoints ----
        "CreateMultiRegionEndpoint" => {
            let name = body_field(body, "EndpointName");
            let entry = json!({ "EndpointName": name, "Status": "CREATED" });
            with_scope(&slot, req, |st| {
                st.multi_region_endpoints
                    .insert(name.clone(), entry.clone());
            });
            ok_json(json!({ "EndpointName": name, "Status": "CREATED" }))
        }
        "ListMultiRegionEndpoints" => {
            let list: Vec<Value> = with_scope(&slot, req, |st| {
                st.multi_region_endpoints
                    .values()
                    .map(|e| e.clone())
                    .collect()
            });
            ok_json(json!({ "MultiRegionEndpoints": list }))
        }
        "GetMultiRegionEndpoint" => {
            let name = seg_after(&req.path, "multi-region-endpoints");
            let got = with_scope(&slot, req, |st| {
                st.multi_region_endpoints.get(&name).cloned()
            });
            match got {
                Some(e) => ok_json(e),
                None => not_found(format!("Endpoint {name} not found")),
            }
        }
        "DeleteMultiRegionEndpoint" => {
            let name = seg_after(&req.path, "multi-region-endpoints");
            with_scope(&slot, req, |st| {
                st.multi_region_endpoints.remove(&name);
            });
            ok_json(json!({}))
        }
        // ---- Reputation + recommendations ----
        "ListReputationEntities" => ok_json(json!({ "ReputationEntities": [] })),
        "GetReputationEntity" => {
            ok_json(json!({ "ReputationEntityType": "ACCOUNT", "ReputationEntityReference": "" }))
        }
        "UpdateReputationEntityCustomerManagedStatus" | "UpdateReputationEntityPolicy" => {
            ok_json(json!({}))
        }
        "ListRecommendations" => ok_json(json!({ "Recommendations": [] })),
        _ => ok_json(json!({})),
    }
}

/// String list from a `BlacklistItemNames`-like body member.
fn names_list(body: &Value) -> Option<Vec<String>> {
    body.get("BlacklistItemNames")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
}

/// Percent-decoding for rest-json query params (ResourceArn).
fn query_param(query: &str, key: &str) -> String {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| urldecode_query(v))
        .unwrap_or_default()
}

fn urldecode_query(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() + 1 && i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("25");
                out.push(u8::from_str_radix(hex, 16).unwrap_or(b'%'));
                i += 3;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Ctx(u64);

    impl Ctx {
        fn req(&self, method: &str, path: &str, body: Value) -> Request {
            Request {
                service: "sesv2".into(),
                operation: String::new(),
                method: method.into(),
                path: path.into(),
                query_string: String::new(),
                headers: HashMap::new(),
                body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
                account: self.0,
                region: "us-east-1".into(),
            }
        }
    }

    use std::sync::atomic::{AtomicU64, Ordering};

    fn fresh_ctx() -> Ctx {
        static NEXT: AtomicU64 = AtomicU64::new(191111111110);
        Ctx(NEXT.fetch_add(1, Ordering::SeqCst))
    }

    fn parsed(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn suppression_roundtrip_in_process() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "PUT",
            "/v2/email/suppression/addresses",
            json!({
                "EmailAddress": "a@example.com", "Reason": "BOUNCE"
            }),
        ));
        let listed = parsed(&dispatch(&ctx.req(
            "GET",
            "/v2/email/suppression/addresses",
            json!({}),
        )));
        assert_eq!(
            listed["SuppressedDestinationSummaries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let got = parsed(&dispatch(&ctx.req(
            "GET",
            "/v2/email/suppression/addresses/a%40example.com",
            json!({}),
        )));
        assert_eq!(got["SuppressedDestination"]["Reason"], "BOUNCE");
    }

    #[test]
    fn email_identity_lifecycle() {
        let ctx = fresh_ctx();
        let created = parsed(&dispatch(&ctx.req(
            "POST",
            "/v2/email/identities",
            json!({ "EmailIdentity": "id-1@example.com" }),
        )));
        assert_eq!(created["IdentityType"], "EMAIL_ADDRESS");
        let got = parsed(&dispatch(&ctx.req(
            "GET",
            "/v2/email/identities/id-1@example.com",
            json!({}),
        )));
        assert_eq!(got["IdentityType"], "EMAIL_ADDRESS");
        let listed = parsed(&dispatch(&ctx.req(
            "GET",
            "/v2/email/identities",
            json!({}),
        )));
        assert_eq!(listed["EmailIdentities"].as_array().unwrap().len(), 1);
        dispatch(&ctx.req("DELETE", "/v2/email/identities/id-1@example.com", json!({})));
        let listed = parsed(&dispatch(&ctx.req(
            "GET",
            "/v2/email/identities",
            json!({}),
        )));
        assert_eq!(listed["EmailIdentities"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn config_set_lifecycle_and_tags() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "POST",
            "/v2/email/configuration-sets",
            json!({ "ConfigurationSetName": "cs1" }),
        ));
        let dup = dispatch(&ctx.req(
            "POST",
            "/v2/email/configuration-sets",
            json!({ "ConfigurationSetName": "cs1" }),
        ));
        assert_eq!(dup.status, 400);
        let arn = "arn:aws:ses:us-east-1:123456789012:configuration-set/cs1";
        dispatch(&ctx.req(
            "POST",
            "/v2/email/tags",
            json!({ "ResourceArn": arn, "Tags": [{"Key": "env", "Value": "t"}] }),
        ));
        let mut list_req = ctx.req("GET", "/v2/email/tags", json!({}));
        list_req.query_string = format!(
            "ResourceArn={}",
            arn.replace(':', "%3A").replace('/', "%2F")
        );
        let tags = parsed(&dispatch(&list_req));
        assert_eq!(tags["Tags"][0]["Key"], "env");
        dispatch(&ctx.req("DELETE", "/v2/email/configuration-sets/cs1", json!({})));
        let listed = parsed(&dispatch(&ctx.req(
            "GET",
            "/v2/email/configuration-sets",
            json!({}),
        )));
        assert_eq!(listed["ConfigurationSets"].as_array().unwrap().len(), 0);
        let missing = dispatch(&ctx.req("GET", "/v2/email/configuration-sets/ghost", json!({})));
        assert_eq!(missing.status, 400);
    }

    #[test]
    fn contact_list_with_contacts() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "POST",
            "/v2/email/contact-lists",
            json!({ "ContactListName": "cl1" }),
        ));
        dispatch(&ctx.req(
            "POST",
            "/v2/email/contact-lists/cl1/contacts",
            json!({ "EmailAddress": "c@example.com" }),
        ));
        let listed = parsed(&dispatch(&ctx.req(
            "POST",
            "/v2/email/contact-lists/cl1/contacts/list",
            json!({}),
        )));
        assert_eq!(listed["Contacts"][0]["EmailAddress"], "c@example.com");
        let removed = dispatch(&ctx.req(
            "DELETE",
            "/v2/email/contact-lists/cl1/contacts/c%40example.com",
            json!({}),
        ));
        assert_eq!(removed.status, 200);
        let missing_req = ctx.req(
            "DELETE",
            "/v2/email/contact-lists/cl1/contacts/ghost%40example.com",
            json!({}),
        );
        assert_eq!(dispatch(&missing_req).status, 400);
    }

    #[test]
    fn routing_specs() {
        assert_eq!(
            derive_op("POST", "/v2/email/configuration-sets"),
            "CreateConfigurationSet"
        );
        assert_eq!(
            derive_op("GET", "/v2/email/identities/a%40b.com"),
            "GetEmailIdentity"
        );
        assert_eq!(
            derive_op("PUT", "/v2/email/identities/a%40b.com/policies/p1"),
            "UpdateEmailIdentityPolicy"
        );
        assert_eq!(derive_op("POST", "/v2/email/outbound-emails"), "SendEmail");
        assert_eq!(derive_op("GET", "/v2/email/tags"), "ListTagsForResource");
        assert_eq!(
            derive_op("PUT", "/v2/email/account/sending"),
            "PutAccountSendingAttributes"
        );
        assert_eq!(derive_op("GET", "/v2/email/export-jobs/j1"), "GetExportJob");
    }
}
