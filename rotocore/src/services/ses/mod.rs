//! SES (v1) backend — AWS query protocol served at `POST /` with the
//! `Action=...&Version=2010-12-01` urlencoded body. Pure-Rust port of the
//! moto `ses` service (`moto/ses/{responses,models}.py`), matching the
//! restored compat suite `tests/compatibility/test_ses_compat.py`.
//!
//! Semantics follow the moto fork: identities are SESv2-shared
//! `email_identities` (typed EmailAddress iff the value contains `@`),
//! templates carry a timestamp, receipt rule sets keep the
//! single-active-set invariant with insert-at-front / after-rule
//! positioning, unknown identities report `DkimVerificationStatus:
//! NotStarted`, and verification tokens are moto's fixed constants.
//!
//! Per the wave-2 ruling (docs/moto-parity-progress.md): operations absent
//! from the moto fork's surface (identity policies, tracking/delivery
//! options, rule repositioning, `SendBounce`, ...) are NOT implemented
//! here; `dispatch` answers `None` for them so the gateway's canonical 501
//! path serves them, exactly as the sidecar did.

use crate::proto::query_response_xml;
use crate::store::{self, Slot};
use crate::{Request, Response};
use std::collections::{BTreeMap, HashMap, HashSet};

pub const XMLNS: &str = "http://ses.amazonaws.com/doc/2010-12-01/";

/// moto's fixed DKIM tokens (`moto/ses/models.py::get_identity_dkim_attributes`).
const DKIM_TOKENS: [&str; 3] = [
    "vvjuipp74whm76gqoni7qmwwn4w4qusjiainivf6sf",
    "3frqe7jn4obpuxjpwpolz6ipb3k5nvt2nhjpik2oy",
    "wrqplteh7oodxnad7hsl4mixg2uavzneazxv5sxi2",
];

/// moto's fixed domain verification token (`verify_domain_identity` response).
const DOMAIN_TOKEN: &str = "QTKknzFg2J4ygwa+XvHAxUl1hyHoY0gVfZdfjIedHZ0=";
/// moto's fixed per-identity verification token
/// (`get_identity_verification_attributes`).
const IDENTITY_TOKEN: &str = "ILQMESfEW0p6i6gIJcEWvO65TP5hg6B99hGFZ2lxrIs=";

const RECIPIENT_LIMIT: usize = 50;

// ---------------------------------------------------------------------------
// Params: value tree parsed from the urlencoded body + query string.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq)]
enum Val {
    #[default]
    None,
    S(String),
    L(Vec<Val>),
    M(BTreeMap<String, Val>),
}

impl Val {
    fn get(&self, key: &str) -> Option<&Val> {
        match self {
            Val::M(m) => m.get(key),
            _ => None,
        }
    }
    fn string(&self) -> Option<&str> {
        match self {
            Val::S(s) => Some(s.as_str()),
            _ => None,
        }
    }
    fn bool(&self) -> Option<bool> {
        match self {
            Val::S(s) => Some(s.eq_ignore_ascii_case("true") || s == "1"),
            _ => None,
        }
    }
    /// Wrapped (`.member.N`) or flattened (`.N`) scalar-list items.
    fn strings(&self) -> Vec<String> {
        match self {
            Val::L(items) => items
                .iter()
                .filter_map(|v| match v {
                    Val::S(s) => Some(s.clone()),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }
}

/// Parse the request's urlencoded pairs (body first, then query string) into
/// a nested tree using botocore's query encoding (`Rule.Name`, wrapped
/// `.member.N` lists, flattened `.N` lists).
fn parse_params(req: &Request) -> (BTreeMap<String, Val>, HashSet<String>) {
    let mut tree = Val::M(BTreeMap::new());
    let mut raw_keys = HashSet::new();
    let mut pairs = decode_pairs(&req.body);
    pairs.extend(decode_pairs(req.query_string.as_bytes()));
    for (key, value) in pairs {
        if key.is_empty() || key == "Action" || key == "Version" {
            continue;
        }
        raw_keys.insert(key.clone());
        let segments: Vec<String> = key.split('.').map(str::to_string).collect();
        insert_val(&mut tree, &segments, &Val::S(value));
    }
    match tree {
        Val::M(m) => (m, raw_keys),
        _ => (BTreeMap::new(), raw_keys),
    }
}

fn decode_pairs(bytes: &[u8]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let s = String::from_utf8_lossy(bytes);
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        // botocore's HTTP layer flatteners use `urlencode`-style encoding:
        // spaces arrive as '+', and encoded '+' arrives as %2B — so decode
        // both here.
        out.push((form_decode(k), form_decode(v)));
    }
    out
}

fn form_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = s.get(i + 1..i + 3).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Insert `segments → value`, building nested maps plus 1-based numbered
/// lists (both wrapped `.member.N` and flattened `.N` botocore forms).
fn insert_val(node: &mut Val, path: &[String], value: &Val) {
    let Some(head) = path.first() else {
        *node = value.clone();
        return;
    };
    let rest = &path[1..];
    let (idx, inner): (Option<usize>, &[String]) = match rest.first() {
        Some(first) => match first.as_str() {
            "member" => match rest.get(1).and_then(|s| s.parse::<usize>().ok()) {
                Some(n) if n >= 1 => (Some(n), &rest[2..]),
                _ => (None, &rest[1..]),
            },
            s => match s.parse::<usize>() {
                Ok(n) if n >= 1 => (Some(n), &rest[1..]),
                _ => (None, rest),
            },
        },
        None => (None, &[][..]),
    };
    if !matches!(node, Val::M(_)) {
        *node = Val::M(BTreeMap::new());
    }
    if let Val::M(map) = node {
        let entry = map.entry(head.clone()).or_insert(Val::None);
        if let Some(n) = idx {
            if !matches!(entry, Val::L(_)) {
                *entry = Val::L(Vec::new());
            }
            if let Val::L(items) = entry {
                while items.len() < n {
                    items.push(Val::None);
                }
                if inner.is_empty() {
                    items[n - 1] = value.clone();
                } else {
                    insert_val(&mut items[n - 1], inner, value);
                }
            }
        } else if inner.is_empty() {
            *entry = value.clone();
        } else {
            insert_val(entry, inner, value);
        }
    }
}

// ---------------------------------------------------------------------------
// State model (mirrors moto's SESBackend fields).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct MailFromAttrs {
    mail_from_domain: Option<String>,
    behavior_on_mx_failure: Option<String>,
}

#[derive(Default)]
struct ConfigSet {
    reputation_metrics_enabled: Option<bool>,
}

#[derive(Default)]
struct Template {
    subject_part: String,
    text_part: String,
    html_part: String,
    created: String,
}

#[derive(Default)]
struct CustomTemplate {
    from_email_address: String,
    template_subject: String,
    template_content: String,
    success_redirection_url: String,
    failure_redirection_url: String,
}

#[derive(Default)]
struct RuleSet {
    created: String,
    is_active: bool,
    /// Receipt rules as parsed from the wire; serialized back verbatim.
    rules: Vec<Val>,
}

#[derive(Default)]
struct SesState {
    /// identity string → is-email-address flag (moto's `email_identities`).
    identities: BTreeMap<String, bool>,
    /// identity → notification config (`feedback_forwarding_enabled`,
    /// `Bounce`/`Complaint`/`Delivery` → topic ARN).
    notifications: HashMap<String, BTreeMap<String, String>>,
    /// identity → custom MAIL-FROM settings.
    mail_from: HashMap<String, MailFromAttrs>,
    /// configuration set name → set options.
    config_sets: BTreeMap<String, ConfigSet>,
    /// configuration set name → stored event destination.
    cs_event_destinations: HashMap<String, Val>,
    /// event destination name → present (moto's duplicate-name registry).
    event_destination_names: HashSet<String>,
    /// template name → stored template info.
    templates: BTreeMap<String, Template>,
    /// rule set name → receipt rule set.
    rule_sets: BTreeMap<String, RuleSet>,
    /// receipt filter name → (policy, cidr).
    receipt_filters: BTreeMap<String, (String, String)>,
    /// custom verification email template name → fields.
    custom_templates: BTreeMap<String, CustomTemplate>,
    sent_message_count: u64,
    rejected_messages_count: u64,
}

fn slot() -> Slot<SesState> {
    store::slot::<SesState>("ses")
}

// ---------------------------------------------------------------------------
// Errors: moto `ses/exceptions.py` ServiceException codes (HTTP 400).
// ---------------------------------------------------------------------------

struct SesError {
    code: String,
    message: String,
}

impl SesError {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

type OpResult = Result<String, SesError>;

/// AWS query-protocol error envelope in the moto SES layout.
fn error_response(code: &str, message: &str) -> Response {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="{XMLNS}">
  <Error>
    <Type>Sender</Type>
    <Code>{}</Code>
    <Message>{}</Message>
  </Error>
  <RequestId>{}</RequestId>
</ErrorResponse>"#,
        escape(code),
        escape(message),
        uuid::Uuid::new_v4()
    );
    Response::xml(400, body.into_bytes())
}

// ---------------------------------------------------------------------------
// XML building helpers.
// ---------------------------------------------------------------------------

/// `<Name>value</Name>` with XML escaping.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '"' => out.push_str("&quot;"),
            c => out.push(c),
        }
    }
    out
}

fn tag(name: &str, value: &str) -> String {
    format!("<{name}>{}</{name}>", escape(value))
}

fn bool_tag(name: &str, value: Option<bool>) -> String {
    match value {
        Some(b) => tag(name, if b { "true" } else { "false" }),
        None => String::new(),
    }
}

/// Render a parsed request `Val` back to wire XML (recursively):
/// `M` → child elements, `S` → text, `L` → `<member>` items. Keys keep the
/// PascalCase botocore sent, so `DescribeReceiptRule` round-trips rules.
fn render_val(v: &Val) -> String {
    match v {
        Val::None => String::new(),
        Val::S(s) => escape(s),
        Val::L(items) => {
            let mut out = String::new();
            for item in items {
                out.push_str("<member>");
                out.push_str(&render_val(item));
                out.push_str("</member>");
            }
            out
        }
        Val::M(map) => {
            let mut out = String::new();
            for (k, item) in map {
                out.push_str(&format!("<{}>{}</{}>", k, render_val(item), k));
            }
            out
        }
    }
}

/// `<{op}Response xmlns=...><{op}Result>{result}</{op}Result><ResponseMetadata
/// ...>` — the query-protocol envelope botocore parses.
fn response_xml(op: &str, result: &str) -> Response {
    let result_el = if result.is_empty() {
        format!("<{op}Result/>")
    } else {
        format!("<{op}Result>{result}</{op}Result>")
    };
    Response::xml(
        200,
        query_response_xml(op, Some(XMLNS), &result_el).into_bytes(),
    )
}

/// moto's random message id: lowercase-hex 16-8-4-4-4-12-6.
fn random_message_id() -> String {
    fn hex(len: usize) -> String {
        const ALPHA: &[u8] = b"0123456789abcdef";
        let mut out = String::with_capacity(len);
        for b in uuid::Uuid::new_v4().as_bytes().iter().cycle() {
            if out.len() >= len {
                break;
            }
            out.push(ALPHA[(*b as usize) % ALPHA.len()] as char);
        }
        out
    }
    format!(
        "{}-{}-{}-{}-{}-{}-{}",
        hex(16),
        hex(8),
        hex(4),
        hex(4),
        hex(4),
        hex(12),
        hex(6)
    )
}

/// `email.utils.parseaddr` for the single-address strings moto feeds it:
/// the value inside the angled brackets when present, else the trimmed value.
fn parse_addr(s: &str) -> String {
    let t = s.trim();
    if let Some(lt) = t.find('<') {
        if let Some(gt) = t[lt..].find('>') {
            return t[lt + 1..lt + gt].trim().to_string();
        }
    }
    t.to_string()
}

/// moto's `is_valid_address`: exactly two parts and a non-empty domain.
fn invalid_address_reason(addr: &str) -> Option<&'static str> {
    let address = parse_addr(addr);
    let (local, domain) = address.split_once('@').unwrap_or((address.as_str(), ""));
    if local.is_empty() || domain.is_empty() {
        Some("Missing domain")
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Dispatch.
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    let urlencoded = String::from_utf8_lossy(&req.body).contains("Action=")
        || req.query_string.contains("Action=");
    if !urlencoded {
        return None;
    }
    let op = req.operation.trim().to_string();
    if op.is_empty() {
        return None;
    }
    // The gateway seeds `operation` from the `Action=` key, so it equals the
    // botocore operation name. Unknown query actions raised
    // NotImplementedError in moto: answer None to fall through to the
    // gateway's canonical 501.
    match handle(req, &op) {
        Some(Ok(result)) => Some(response_xml(&op, &result)),
        Some(Err(e)) => Some(error_response(&e.code, &e.message)),
        None => None,
    }
}

/// Dispatch table: `Some(result)` when the backend owns the operation,
/// `None` when the moto fork does not route it.
fn handle(req: &Request, op: &str) -> Option<OpResult> {
    let (p, raw) = parse_params(req);
    let slots = slot();
    let mut map = slots.write();
    let state = map.entry((req.account, req.region.clone())).or_default();
    let result = match op {
        "ListIdentities" => op_list_identities(state, &p),
        "ListVerifiedEmailAddresses" => op_list_verified_email_addresses(state),
        "VerifyEmailIdentity" => op_verify_email_identity(state, &p),
        "VerifyEmailAddress" => op_verify_email_address(state, &p),
        "VerifyDomainIdentity" => op_verify_domain_identity(state, &p),
        "VerifyDomainDkim" => op_verify_domain_dkim(state, &p),
        "DeleteIdentity" => op_delete_identity(state, &p),
        "SendEmail" => op_send_email(state, &p, &raw),
        "SendTemplatedEmail" => op_send_templated_email(state, &p),
        "SendBulkTemplatedEmail" => op_send_bulk_templated_email(state, &p),
        "SendRawEmail" => op_send_raw_email(state, &p),
        "GetSendQuota" => op_get_send_quota(state),
        "GetSendStatistics" => op_get_send_statistics(state),
        "GetIdentityNotificationAttributes" => op_get_identity_notification_attributes(state, &p),
        "GetIdentityDkimAttributes" => op_get_dkim_attributes(state, &p),
        "GetIdentityVerificationAttributes" => op_get_verification_attributes(state, &p),
        "GetIdentityMailFromDomainAttributes" => op_get_mail_from_attributes(state, &p),
        "SetIdentityFeedbackForwardingEnabled" => {
            op_set_identity_feedback_forwarding_enabled(state, &p)
        }
        "SetIdentityNotificationTopic" => op_set_identity_notification_topic(state, &p),
        "SetIdentityDkimEnabled" => empty(),
        "SetIdentityMailFromDomain" => op_set_identity_mail_from_domain(state, &p),
        "CreateConfigurationSet" => op_create_configuration_set(state, &p),
        "DescribeConfigurationSet" => op_describe_configuration_set(state, &p),
        "DeleteConfigurationSet" => {
            state
                .config_sets
                .remove(&pstring(&p, "ConfigurationSetName"));
            empty()
        }
        "ListConfigurationSets" => op_list_configuration_sets(state),
        "CreateConfigurationSetEventDestination" => {
            op_create_configuration_set_event_destination(state, &p)
        }
        "UpdateConfigurationSetReputationMetricsEnabled" => op_update_reputation_metrics(state, &p),
        "CreateTemplate" => op_create_template(state, &p),
        "UpdateTemplate" => op_update_template(state, &p),
        "GetTemplate" => op_get_template(state, &p),
        "ListTemplates" => op_list_templates(state),
        "TestRenderTemplate" => op_test_render_template(state, &p),
        "DeleteTemplate" => {
            state.templates.remove(&pstring(&p, "TemplateName"));
            empty()
        }
        "CreateReceiptRuleSet" => op_create_receipt_rule_set(state, &p),
        "CreateReceiptRule" => op_create_receipt_rule(state, &p),
        "CloneReceiptRuleSet" => op_clone_receipt_rule_set(state, &p),
        "SetActiveReceiptRuleSet" => op_set_active_receipt_rule_set(state, &p),
        "DescribeReceiptRuleSet" => op_describe_receipt_rule_set(state, &p),
        "DescribeActiveReceiptRuleSet" => op_describe_active_receipt_rule_set(state),
        "DeleteReceiptRuleSet" => op_delete_receipt_rule_set(state, &p),
        "ListReceiptRuleSets" => op_list_receipt_rule_sets(state),
        "DescribeReceiptRule" => op_describe_receipt_rule(state, &p),
        "UpdateReceiptRule" => op_update_receipt_rule(state, &p),
        "DeleteReceiptRule" => op_delete_receipt_rule(state, &p),
        "CreateReceiptFilter" => op_create_receipt_filter(state, &p),
        "ListReceiptFilters" => op_list_receipt_filters(state),
        "DeleteReceiptFilter" => {
            state.receipt_filters.remove(&pstring(&p, "FilterName"));
            empty()
        }
        "CreateCustomVerificationEmailTemplate" => {
            op_create_custom_verification_email_template(state, &p)
        }
        "GetCustomVerificationEmailTemplate" => {
            op_get_custom_verification_email_template(state, &p)
        }
        "ListCustomVerificationEmailTemplates" => {
            op_list_custom_verification_email_templates(state)
        }
        "UpdateCustomVerificationEmailTemplate" => {
            op_update_custom_verification_email_template(state, &p)
        }
        "DeleteCustomVerificationEmailTemplate" => {
            state.custom_templates.remove(&pstring(&p, "TemplateName"));
            empty()
        }
        "SendCustomVerificationEmail" => op_send_custom_verification_email(state, &p),
        "GetAccountSendingEnabled" => Ok(tag("Enabled", "true")),
        "UpdateAccountSendingEnabled" => empty(),
        _ => return None,
    };
    Some(result)
}

fn empty() -> OpResult {
    Ok(String::new())
}

// ---------------------------------------------------------------------------
// Param accessors.
// ---------------------------------------------------------------------------

fn pstring(p: &BTreeMap<String, Val>, key: &str) -> String {
    p.get(key)
        .and_then(Val::string)
        .unwrap_or_default()
        .to_string()
}

// ---------------------------------------------------------------------------
// Identity operations.
// ---------------------------------------------------------------------------

/// moto's `_is_verified_address`: direct hit on the identity, or a hit on the
/// domain part of the address.
fn is_verified_addr(state: &SesState, source: &str) -> bool {
    let address = parse_addr(source);
    if state.identities.contains_key(&address) {
        return true;
    }
    let host = address.rsplit_once('@').map(|(_, h)| h).unwrap_or(&address);
    state.identities.contains_key(host)
}

/// moto's `create_email_identity_v2`: EmailAddress iff the value contains `@`.
fn create_email_identity(state: &mut SesState, identity: &str) {
    state
        .identities
        .entry(identity.to_string())
        .or_insert(identity.contains('@'));
}

fn op_verify_email_identity(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let address = parse_addr(&pstring(p, "EmailAddress"));
    create_email_identity(state, &address);
    empty()
}

fn op_verify_email_address(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let address = parse_addr(&pstring(p, "EmailAddress"));
    create_email_identity(state, &address);
    empty()
}

/// moto's `verify_domain`: domains are stored lowercased.
fn op_verify_domain_identity(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let domain = pstring(p, "Domain").to_lowercase();
    create_email_identity(state, &domain);
    Ok(tag("VerificationToken", DOMAIN_TOKEN))
}

fn op_verify_domain_dkim(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let domain = pstring(p, "Domain").to_lowercase();
    create_email_identity(state, &domain);
    let mut out = String::new();
    for t in DKIM_TOKENS {
        out.push_str(&tag("member", t));
    }
    Ok(format!("<DkimTokens>{out}</DkimTokens>"))
}

fn op_delete_identity(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let identity = pstring(p, "Identity");
    if is_verified_addr(state, &identity) {
        state.identities.remove(&identity);
    }
    empty()
}

fn op_list_identities(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let identity_type = p.get("IdentityType").and_then(Val::string).unwrap_or("");
    if !identity_type.is_empty() && identity_type != "EmailAddress" && identity_type != "Domain" {
        return Err(SesError::new(
            "ValidationError",
            format!(
                "Value '{identity_type}' at 'identityType' failed to satisfy constraint: Member must satisfy enum value set: [Domain, EmailAddress]"
            ),
        ));
    }
    let mut ids: Vec<String> = state
        .identities
        .iter()
        .filter(|(_, email)| match identity_type {
            "EmailAddress" => **email,
            "Domain" => !**email,
            _ => true,
        })
        .map(|(k, _)| k.clone())
        .collect();
    // moto ignores MaxItems; the restored suite asserts the page truncation
    // (`len(Identities) <= 2` for MaxItems=2), so honor it.
    if let Some(n) = p
        .get("MaxItems")
        .and_then(Val::string)
        .and_then(|s| s.parse::<usize>().ok())
    {
        ids.truncate(n);
    }
    let mut out = String::new();
    for id in ids {
        out.push_str(&tag("member", &id));
    }
    Ok(format!("<Identities>{out}</Identities>"))
}

fn op_list_verified_email_addresses(state: &SesState) -> OpResult {
    let mut out = String::new();
    for (id, email) in &state.identities {
        if *email {
            out.push_str(&tag("member", id));
        }
    }
    Ok(format!(
        "<VerifiedEmailAddresses>{out}</VerifiedEmailAddresses>"
    ))
}

/// Verified identities ∩ request, each `{VerificationStatus: Success, ...}`.
fn op_get_verification_attributes(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let requested = p.get("Identities").map(Val::strings).unwrap_or_default();
    let mut out = String::new();
    for id in state.identities.keys() {
        if !requested.iter().any(|r| r == id) {
            continue;
        }
        out.push_str(&format!(
            "<entry><key>{}</key><value>{}</value></entry>",
            escape(id),
            tag("VerificationStatus", "Success") + &tag("VerificationToken", IDENTITY_TOKEN)
        ));
    }
    Ok(format!(
        "<VerificationAttributes>{out}</VerificationAttributes>"
    ))
}

/// moto: known identities report `Success` (domain identities additionally
/// carry the fixed token triple); unknown ones report `NotStarted`.
fn op_get_dkim_attributes(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let requested = p.get("Identities").map(Val::strings).unwrap_or_default();
    let mut out = String::new();
    for ident in &requested {
        let tokens = match state.identities.get(ident) {
            Some(false) => DKIM_TOKENS
                .iter()
                .map(|t| tag("member", t))
                .collect::<String>()
                .to_string(),
            _ => String::new(),
        };
        let status = if state.identities.contains_key(ident) {
            "Success"
        } else {
            "NotStarted"
        };
        out.push_str(&format!(
            "<entry><key>{}</key><value><DkimEnabled>true</DkimEnabled>{}{}</value></entry>",
            escape(ident),
            tag("DkimVerificationStatus", status),
            if tokens.is_empty() {
                String::new()
            } else {
                format!("<DkimTokens>{tokens}</DkimTokens>")
            },
        ));
    }
    Ok(format!("<DkimAttributes>{out}</DkimAttributes>"))
}

// ---------------------------------------------------------------------------
// Notifications / MAIL-FROM.
// ---------------------------------------------------------------------------

fn op_get_identity_notification_attributes(
    state: &SesState,
    p: &BTreeMap<String, Val>,
) -> OpResult {
    let requested = p.get("Identities").map(Val::strings).unwrap_or_default();
    let mut out = String::new();
    for ident in &requested {
        // moto applies defaults even when the identity is not registered:
        // `sns_topics.get(identity, {})` with `feedback_forwarding_enabled`
        // defaulting to True.
        let forwarding = state
            .notifications
            .get(ident)
            .and_then(|c| c.get("feedback_forwarding_enabled"))
            .map(|v| v == "true")
            .unwrap_or(true);
        out.push_str(&format!(
            "<entry><key>{}</key><value>{}{}{}{}</value></entry>",
            escape(ident),
            bool_tag("ForwardingEnabled", Some(forwarding)),
            bool_tag("HeadersInBounceNotificationsEnabled", Some(false)),
            bool_tag("HeadersInComplaintNotificationsEnabled", Some(false)),
            bool_tag("HeadersInDeliveryNotificationsEnabled", Some(false)),
        ));
    }
    Ok(format!(
        "<NotificationAttributes>{out}</NotificationAttributes>"
    ))
}

fn op_set_identity_feedback_forwarding_enabled(
    state: &mut SesState,
    p: &BTreeMap<String, Val>,
) -> OpResult {
    let identity = pstring(p, "Identity");
    let enabled = p
        .get("ForwardingEnabled")
        .and_then(Val::bool)
        .unwrap_or(false)
        .to_string();
    state
        .notifications
        .entry(identity)
        .or_default()
        .insert("feedback_forwarding_enabled".to_string(), enabled);
    empty()
}

/// moto deletes the mapping when `SnsTopic` is absent; its bare `del` raises
/// KeyError for a missing key (the wedge behind the suite's 500), so this
/// port pops without failing.
fn op_set_identity_notification_topic(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let identity = pstring(p, "Identity");
    let not_type = pstring(p, "NotificationType");
    let entry = state.notifications.entry(identity).or_default();
    match p.get("SnsTopic").and_then(Val::string) {
        Some(topic) => {
            entry.insert(not_type, topic.to_string());
        }
        None => {
            entry.remove(&not_type);
        }
    }
    empty()
}

fn op_set_identity_mail_from_domain(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let identity = pstring(p, "Identity");
    if !is_verified_addr(state, &identity) {
        return Err(SesError::new(
            "InvalidParameterValue",
            format!("Identity '{identity}' does not exist."),
        ));
    }
    let mail_from = pstring(p, "MailFromDomain");
    if mail_from.is_empty() {
        state.mail_from.remove(&identity);
        return empty();
    }
    let domain_part = identity
        .rsplit_once('@')
        .map(|(_, d)| d)
        .unwrap_or(&identity);
    if !mail_from.ends_with(domain_part) {
        return Err(SesError::new(
            "InvalidParameterValue",
            format!(
                "Provided MAIL-FROM domain '{mail_from}' is not subdomain of the domain of the identity '{domain_part}'."
            ),
        ));
    }
    let behavior = pstring(p, "BehaviorOnMXFailure");
    if !behavior.is_empty() && behavior != "RejectMessage" && behavior != "UseDefaultValue" {
        return Err(SesError::new(
            "ValidationError",
            format!("1 validation error detected: Value '{behavior}' at 'behaviorOnMXFailure'failed to satisfy constraint: Member must satisfy enum value set: [RejectMessage, UseDefaultValue]"),
        ));
    }
    state.mail_from.insert(
        identity,
        MailFromAttrs {
            mail_from_domain: Some(mail_from),
            behavior_on_mx_failure: if behavior.is_empty() {
                None
            } else {
                Some(behavior)
            },
        },
    );
    empty()
}

fn op_get_mail_from_attributes(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let requested = p.get("Identities").map(Val::strings).unwrap_or_default();
    let mut out = String::new();
    for id in state.identities.keys() {
        if !requested.iter().any(|r| r == id) {
            continue;
        }
        let attrs = state.mail_from.get(id);
        let domain = attrs
            .and_then(|a| a.mail_from_domain.clone())
            .unwrap_or_default();
        let behavior = attrs
            .and_then(|a| a.behavior_on_mx_failure.clone())
            .unwrap_or_else(|| "UseDefaultValue".to_string());
        // moto leaves MailFromDomain/MailFromDomainStatus as None for unset
        // identities; the XML serializer drops None members.
        let domain_el = if domain.is_empty() {
            String::new()
        } else {
            tag("MailFromDomain", &domain) + &tag("MailFromDomainStatus", "Success")
        };
        out.push_str(&format!(
            "<entry><key>{}</key><value>{}{}</value></entry>",
            escape(id),
            domain_el,
            tag("BehaviorOnMXFailure", &behavior),
        ));
    }
    Ok(format!(
        "<MailFromDomainAttributes>{out}</MailFromDomainAttributes>"
    ))
}

// ---------------------------------------------------------------------------
// Send operations.
// ---------------------------------------------------------------------------

/// mimeType-accounting send for SendEmail: verify the SOURCE, validate the
/// addresses, count recipients.
fn op_send_email(
    state: &mut SesState,
    p: &BTreeMap<String, Val>,
    raw: &HashSet<String>,
) -> OpResult {
    // moto takes the Text body, switching to Html when that key was sent.
    let body_val = match raw.contains("Message.Body.Html.Data") {
        true => p
            .get("Message")
            .and_then(|m| m.get("Body"))
            .and_then(|b| b.get("Html")),
        false => p
            .get("Message")
            .and_then(|m| m.get("Body"))
            .and_then(|b| b.get("Text")),
    };
    let _body = body_val.and_then(Val::string).unwrap_or_default();
    let source = pstring(p, "Source");
    let destinations = p.get("Destination").unwrap_or(&Val::None);
    let addrs = ["ToAddresses", "CcAddresses", "BccAddresses"]
        .iter()
        .flat_map(|k| destinations.get(k).map(Val::strings).unwrap_or_default())
        .collect::<Vec<_>>();
    if addrs.len() > RECIPIENT_LIMIT {
        state.rejected_messages_count += 1;
        return Err(SesError::new("MessageRejected", "Too many recipients."));
    }
    if !is_verified_addr(state, &source) {
        state.rejected_messages_count += 1;
        return Err(SesError::new(
            "MessageRejected",
            format!("Email address not verified {source}"),
        ));
    }
    for addr in std::iter::once(&source).chain(addrs.iter()) {
        if let Some(reason) = invalid_address_reason(addr) {
            return Err(SesError::new("InvalidParameterValue", reason));
        }
    }
    state.sent_message_count += addrs.len() as u64;
    Ok(tag("MessageId", &random_message_id()))
}

fn op_send_templated_email(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let source = pstring(p, "Source");
    let template = pstring(p, "Template");
    let destinations = p.get("Destination").unwrap_or(&Val::None);
    let addrs = ["ToAddresses", "CcAddresses", "BccAddresses"]
        .iter()
        .flat_map(|k| destinations.get(k).map(Val::strings).unwrap_or_default())
        .collect::<Vec<_>>();
    if addrs.len() > RECIPIENT_LIMIT {
        state.rejected_messages_count += 1;
        return Err(SesError::new("MessageRejected", "Too many recipients."));
    }
    if !is_verified_addr(state, &source) {
        state.rejected_messages_count += 1;
        return Err(SesError::new(
            "MessageRejected",
            format!("Email address not verified {source}"),
        ));
    }
    for addr in std::iter::once(&source).chain(addrs.iter()) {
        if let Some(reason) = invalid_address_reason(addr) {
            return Err(SesError::new("InvalidParameterValue", reason));
        }
    }
    if !state.templates.contains_key(&template) {
        return Err(SesError::new(
            "TemplateDoesNotExist",
            format!("Template ({template}) does not exist"),
        ));
    }
    state.sent_message_count += addrs.len() as u64;
    Ok(tag("MessageId", &random_message_id()))
}

fn op_send_bulk_templated_email(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let source = pstring(p, "Source");
    let template = pstring(p, "Template");
    let dest_list: Vec<Val> = match p.get("Destinations") {
        Some(Val::L(items)) => items.clone(),
        _ => Vec::new(),
    };
    let total: usize = dest_list
        .iter()
        .map(|d| {
            ["ToAddresses", "CcAddresses", "BccAddresses"]
                .iter()
                .map(|k| {
                    d.get("Destination")
                        .and_then(|x| x.get(k))
                        .map(Val::strings)
                        .unwrap_or_default()
                        .len()
                })
                .sum::<usize>()
        })
        .sum();
    if dest_list.len() > RECIPIENT_LIMIT || total > RECIPIENT_LIMIT {
        state.rejected_messages_count += 1;
        return Err(SesError::new("MessageRejected", "Too many destinations."));
    }
    if !is_verified_addr(state, &source) {
        state.rejected_messages_count += 1;
        return Err(SesError::new(
            "MessageRejected",
            format!("Email address not verified {source}"),
        ));
    }
    if !state.templates.contains_key(&template) {
        return Err(SesError::new(
            "TemplateDoesNotExist",
            format!("Template ({template}) does not exist"),
        ));
    }
    state.sent_message_count += total as u64;
    let mut members = String::new();
    for _ in 0..dest_list.len() {
        members.push_str(&format!(
            "<member>{}</member>",
            tag("MessageId", &random_message_id())
        ));
    }
    Ok(format!("<Status>{members}</Status>"))
}

/// Raw MIME headers TO/CC/BCC extraction, mirroring moto's
/// `email.message_from_string` + `getaddresses` on those three headers.
fn raw_header_addresses(raw_data: &str) -> Vec<String> {
    let mut out = Vec::new();
    // only the header block (lines before the first blank line);
    let header_block = raw_data.split("\r\n\r\n").next().unwrap_or(raw_data);
    for line in header_block.split('\n') {
        let line = line.trim_end_matches('\r');
        let lower = line.to_lowercase();
        for h in ["to:", "cc:", "bcc:"] {
            if let Some(value) = lower.strip_prefix(h) {
                let start = line.len() - value.len();
                for part in line[start..].trim().split(',') {
                    let addr = parse_addr(part);
                    if !addr.is_empty() {
                        out.push(addr);
                    }
                }
            }
        }
    }
    out
}

fn first_from_header(raw_data: &str) -> Option<String> {
    for line in raw_data.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            break;
        }
        if let Some(v) = line.strip_prefix("From:") {
            return Some(parse_addr(v.trim()));
        }
    }
    None
}

fn op_send_raw_email(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    use base64::Engine;
    let source = p.get("Source").and_then(Val::string).map(str::to_string);
    let raw_data = p
        .get("RawMessage")
        .and_then(|m| m.get("Data"))
        .and_then(Val::string)
        .unwrap_or_default();
    // botocore ships query-protocol blob members base64-encoded.
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw_data)
        .unwrap_or_default();
    let raw_msg = String::from_utf8_lossy(&decoded).into_owned();
    // moto: verify the explicit Source, else the message's From header.
    let source_address = match &source {
        Some(s) => parse_addr(s),
        None => first_from_header(&raw_msg)
            .ok_or_else(|| SesError::new("MessageRejected", "Source not specified"))?,
    };
    if !is_verified_addr(state, &source_address) {
        state.rejected_messages_count += 1;
        return Err(SesError::new(
            "MessageRejected",
            format!(
                "Did not have authority to send from email {}",
                escape(&source_address)
            ),
        ));
    }
    let mut destinations: Vec<String> = p.get("Destinations").map(Val::strings).unwrap_or_default();
    destinations.extend(raw_header_addresses(&raw_msg));
    if destinations.len() > RECIPIENT_LIMIT {
        state.rejected_messages_count += 1;
        return Err(SesError::new("MessageRejected", "Too many recipients."));
    }
    for addr in std::iter::once(&source_address).chain(destinations.iter()) {
        if let Some(reason) = invalid_address_reason(addr) {
            return Err(SesError::new("InvalidParameterValue", reason));
        }
    }
    state.sent_message_count += destinations.len() as u64;
    Ok(tag("MessageId", &random_message_id()))
}

fn op_get_send_quota(state: &SesState) -> OpResult {
    Ok(format!(
        "<Max24HourSend>200.0</Max24HourSend><MaxSendRate>1.0</MaxSendRate><SentLast24Hours>{}</SentLast24Hours>",
        state.sent_message_count
    ))
}

fn op_get_send_statistics(state: &SesState) -> OpResult {
    let point = format!(
        "<Timestamp>{}</Timestamp><DeliveryAttempts>{}</DeliveryAttempts><Bounces>0</Bounces><Complaints>0</Complaints><Rejects>{}</Rejects>",
        crate::proto::now_iso(),
        state.sent_message_count,
        state.rejected_messages_count
    );
    Ok(format!(
        "<SendDataPoints><member>{point}</member></SendDataPoints>"
    ))
}

// ---------------------------------------------------------------------------
// Configuration sets.
// ---------------------------------------------------------------------------

fn op_create_configuration_set(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = p
        .get("ConfigurationSet")
        .and_then(|cs| cs.get("Name"))
        .and_then(Val::string)
        .unwrap_or_default()
        .to_string();
    if state.config_sets.contains_key(&name) {
        return Err(SesError::new(
            "ConfigurationSetAlreadyExists",
            format!("Configuration set <{name}> already exists"),
        ));
    }
    state.config_sets.insert(name, ConfigSet::default());
    empty()
}

fn op_describe_configuration_set(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = pstring(p, "ConfigurationSetName");
    if !state.config_sets.contains_key(&name) {
        return Err(SesError::new(
            "ConfigurationSetDoesNotExist",
            format!("Configuration set <{name}> does not exist."),
        ));
    }
    // moto reports the stored EventDestinations only when the caller asks for
    // the `eventDestinations` attribute.
    let want_dests = p
        .get("ConfigurationSetAttributeNames")
        .map(Val::strings)
        .unwrap_or_default()
        .iter()
        .any(|a| a.eq_ignore_ascii_case("eventDestinations"));
    let dest_el = if want_dests {
        match state.cs_event_destinations.get(&name) {
            Some(dest) => format!(
                "<EventDestinations><member>{}</member></EventDestinations>",
                render_val(dest)
            ),
            None => "<EventDestinations/>".to_string(),
        }
    } else {
        "<EventDestinations/>".to_string()
    };
    Ok(format!(
        "<ConfigurationSet>{}</ConfigurationSet>{dest_el}",
        tag("Name", &name)
    ))
}

fn op_list_configuration_sets(state: &SesState) -> OpResult {
    let mut out = String::new();
    for name in state.config_sets.keys() {
        out.push_str(&format!("<member>{}</member>", tag("Name", name)));
    }
    Ok(format!("<ConfigurationSets>{out}</ConfigurationSets>"))
}

fn op_create_configuration_set_event_destination(
    state: &mut SesState,
    p: &BTreeMap<String, Val>,
) -> OpResult {
    let name = pstring(p, "ConfigurationSetName");
    if !state.config_sets.contains_key(&name) {
        return Err(SesError::new(
            "ConfigurationSetDoesNotExist",
            "Invalid Configuration Set Name.",
        ));
    }
    let dest = p
        .get("EventDestination")
        .cloned()
        .unwrap_or(Val::M(BTreeMap::new()));
    let dest_name = dest
        .get("Name")
        .and_then(Val::string)
        .unwrap_or_default()
        .to_string();
    if state.event_destination_names.contains(&dest_name) {
        return Err(SesError::new(
            "EventDestinationAlreadyExists",
            "Duplicate Event destination Name.",
        ));
    }
    state.event_destination_names.insert(dest_name);
    state.cs_event_destinations.insert(name, dest);
    empty()
}

fn op_update_reputation_metrics(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = pstring(p, "ConfigurationSetName");
    let set = state.config_sets.get_mut(&name).ok_or_else(|| {
        SesError::new(
            "ConfigurationSetDoesNotExist",
            format!("Configuration set <{name}> does not exist"),
        )
    })?;
    set.reputation_metrics_enabled = p.get("Enabled").and_then(Val::bool);
    empty()
}

// ---------------------------------------------------------------------------
// Templates.
// ---------------------------------------------------------------------------

fn parse_template_info(p: &BTreeMap<String, Val>) -> (String, String, String, String) {
    let t = p.get("Template");
    let get = |key: &str| {
        t.and_then(|t| t.get(key))
            .and_then(Val::string)
            .unwrap_or_default()
            .to_string()
    };
    (
        get("TemplateName"),
        get("SubjectPart"),
        get("TextPart"),
        get("HtmlPart"),
    )
}

fn validate_template_name(name: &str) -> Result<(), SesError> {
    if name.is_empty() {
        return Err(SesError::new(
            "ValidationError",
            "1 validation error detected: Value null at 'template.templateName'failed to satisfy constraint: Member must not be null",
        ));
    }
    Ok(())
}

fn validate_template_subject(subject: &str) -> Result<(), SesError> {
    if subject.is_empty() {
        return Err(SesError::new(
            "InvalidParameterValue",
            "The subject must be specified.",
        ));
    }
    Ok(())
}

fn op_create_template(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let (name, subject, text, html) = parse_template_info(p);
    validate_template_name(&name)?;
    if state.templates.contains_key(&name) {
        return Err(SesError::new("AlreadyExists", "Duplicate Template Name."));
    }
    validate_template_subject(&subject)?;
    state.templates.insert(
        name,
        Template {
            subject_part: subject,
            text_part: text,
            html_part: html,
            created: crate::proto::now_iso(),
        },
    );
    empty()
}

fn op_update_template(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let (name, subject, text, html) = parse_template_info(p);
    validate_template_name(&name)?;
    if !state.templates.contains_key(&name) {
        return Err(SesError::new(
            "TemplateDoesNotExist",
            "Invalid Template Name.",
        ));
    }
    validate_template_subject(&subject)?;
    state.templates.insert(
        name,
        Template {
            subject_part: subject,
            text_part: text,
            html_part: html,
            created: crate::proto::now_iso(),
        },
    );
    empty()
}

fn op_get_template(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = pstring(p, "TemplateName");
    let Some(t) = state.templates.get(&name) else {
        return Err(SesError::new(
            "TemplateDoesNotExist",
            "Invalid Template Name.",
        ));
    };
    Ok(format!(
        "<Template>{}{}{}{}</Template>",
        tag("TemplateName", &name),
        tag("SubjectPart", &t.subject_part),
        tag("TextPart", &t.text_part),
        tag("HtmlPart", &t.html_part),
    ))
}

fn op_list_templates(state: &SesState) -> OpResult {
    let mut out = String::new();
    for (name, t) in &state.templates {
        out.push_str(&format!(
            "<member>{}{}</member>",
            tag("Name", name),
            tag("CreatedTimestamp", &t.created)
        ));
    }
    Ok(format!("<TemplatesMetadata>{out}</TemplatesMetadata>"))
}

/// moto's `render_template` for the `{{var}}` subset the suites exercise:
/// substitute each present rendering attribute, else fail like
/// `VarBlockProcessor`.
fn op_test_render_template(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = pstring(p, "TemplateName");
    let Some(t) = state.templates.get(&name) else {
        return Err(SesError::new(
            "TemplateDoesNotExist",
            "Invalid Template Name.",
        ));
    };
    let data_str = pstring(p, "TemplateData");
    let data: serde_json::Value = match serde_json::from_str(&data_str) {
        Ok(d) => d,
        Err(_) => {
            return Err(SesError::new(
                "InvalidRenderingParameterException",
                "Template rendering data is invalid",
            ))
        }
    };
    let substitute = |tpl: &str| -> Result<String, SesError> {
        let mut out = String::new();
        let mut rest = tpl;
        loop {
            match rest.find("{{") {
                Some(i) => {
                    let (pre, tail) = rest.split_at(i);
                    out.push_str(pre);
                    let end = tail
                        .find("}}")
                        .ok_or_else(|| SesError::new("ValidationError", "Unclosed variable"))?;
                    let var = tail[2..end].trim().to_string();
                    match data.get(&var).and_then(serde_json::Value::as_str) {
                        Some(v) => out.push_str(v),
                        None => {
                            return Err(SesError::new(
                                "MissingRenderingAttributeException",
                                format!("Attribute '{var}' is not present in the rendering data."),
                            ))
                        }
                    }
                    rest = &tail[end + 2..];
                }
                None => {
                    out.push_str(rest);
                    break;
                }
            }
        }
        Ok(out)
    };
    let subject = substitute(&t.subject_part)?;
    let text = substitute(&t.text_part)?;
    let html = substitute(&t.html_part)?;
    let rendered = format!(
        "Date: {}\r\nSubject: {}\r\nContent-Type: multipart/alternative; boundary=\"----boundary\"\r\n\r\n------boundary\r\nContent-Type: text/plain;charset=UTF-8\r\n\r\n{text}\r\n------boundary\r\nContent-Type: text/html;charset=UTF-8\r\n\r\n{html}\r\n------boundary--\r\n",
        crate::proto::now_iso(),
        escape(&subject)
    );
    Ok(tag("RenderedTemplate", &rendered))
}

// ---------------------------------------------------------------------------
// Receipt rule sets (moto's regex-validated name params).
// ---------------------------------------------------------------------------

const RULE_NAME_REGEX: &str = "^[a-zA-Z0-9_.-]+$";

/// moto's `_validate_name_param` — regex + first/last alnum + length 64.
fn validate_rule_name(param: &str, name: &str) -> Result<(), SesError> {
    let first = name.chars().next();
    let last = name.chars().last();
    let regex_ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-');
    if !regex_ok
        || first.map(|c| !c.is_ascii_alphanumeric()).unwrap_or(true)
        || last.map(|c| !c.is_ascii_alphanumeric()).unwrap_or(true)
    {
        return Err(SesError::new(
            "ValidationError",
            format!("Value at '{param}' failed to satisfy constraint: Member must satisfy regular expression pattern: {RULE_NAME_REGEX}"),
        ));
    }
    if name.len() > 64 {
        return Err(SesError::new(
            "ValidationError",
            format!("Not a valid {param}: {name}"),
        ));
    }
    Ok(())
}

fn op_create_receipt_rule_set(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = pstring(p, "RuleSetName");
    validate_rule_name("ruleSetName", &name)?;
    if state.rule_sets.contains_key(&name) {
        return Err(SesError::new(
            "AlreadyExists",
            format!("Rule set already exists: {name}"),
        ));
    }
    state.rule_sets.insert(
        name,
        RuleSet {
            created: crate::proto::now_iso(),
            is_active: false,
            rules: Vec::new(),
        },
    );
    empty()
}

fn op_create_receipt_rule(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let rule_set_name = pstring(p, "RuleSetName");
    let after = pstring(p, "After");
    let rule = p.get("Rule").cloned().unwrap_or(Val::M(BTreeMap::new()));
    let rule_name = rule
        .get("Name")
        .and_then(Val::string)
        .unwrap_or_default()
        .to_string();
    validate_rule_name("ruleSetName", &rule_set_name)?;
    validate_rule_name("rule.name", &rule_name)?;
    let Some(rs) = state.rule_sets.get_mut(&rule_set_name) else {
        return Err(SesError::new(
            "RuleSetDoesNotExist",
            format!("Rule set does not exist: {rule_set_name}"),
        ));
    };
    if !after.is_empty() && !rs.rules.iter().any(|r| val_name(r) == Some(&after)) {
        return Err(SesError::new(
            "RuleDoesNotExist",
            format!("Rule does not exist: {after}"),
        ));
    }
    if rs.rules.iter().any(|r| val_name(r) == Some(&rule_name)) {
        return Err(SesError::new(
            "AlreadyExists",
            format!("Rule already exists: {rule_name}"),
        ));
    }
    // moto inserts at the front when `After` is absent, else just after the
    // named rule.
    let pos = if after.is_empty() {
        0
    } else {
        rs.rules
            .iter()
            .position(|r| val_name(r) == Some(&after))
            .map(|i| i + 1)
            .unwrap_or(0)
    };
    rs.rules.insert(pos, rule);
    empty()
}

/// The rule's `Name` member.
fn val_name(rule: &Val) -> Option<&String> {
    match rule.get("Name") {
        Some(Val::S(s)) => Some(s),
        _ => None,
    }
}

fn op_clone_receipt_rule_set(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let original = pstring(p, "OriginalRuleSetName");
    let name = pstring(p, "RuleSetName");
    validate_rule_name("ruleSetName", &original)?;
    validate_rule_name("ruleSetName", &name)?;
    let Some(original_rs) = state.rule_sets.get(&original) else {
        return Err(SesError::new(
            "RuleSetDoesNotExist",
            format!("Rule set does not exist: {original}"),
        ));
    };
    if state.rule_sets.contains_key(&name) {
        return Err(SesError::new(
            "AlreadyExists",
            format!("Rule set already exists: {name}"),
        ));
    }
    state.rule_sets.insert(
        name,
        RuleSet {
            created: crate::proto::now_iso(),
            is_active: false,
            rules: original_rs.rules.clone(),
        },
    );
    empty()
}

fn rule_set_metadata(name: &str, rs: &RuleSet) -> String {
    format!(
        "<Name>{}</Name><CreatedTimestamp>{}</CreatedTimestamp>",
        escape(name),
        escape(&rs.created)
    )
}

fn rule_set_rules_xml(rs: &RuleSet) -> String {
    let mut out = String::new();
    for rule in &rs.rules {
        out.push_str(&format!("<member>{}</member>", render_val(rule)));
    }
    format!("<Rules>{out}</Rules>")
}

fn op_set_active_receipt_rule_set(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = pstring(p, "RuleSetName");
    if name.is_empty() {
        for rs in state.rule_sets.values_mut() {
            rs.is_active = false;
        }
        return empty();
    }
    validate_rule_name("ruleSetName", &name)?;
    if !state.rule_sets.contains_key(&name) {
        return Err(SesError::new(
            "RuleSetDoesNotExist",
            format!("Rule set does not exist: {name}"),
        ));
    }
    for (n, rs) in state.rule_sets.iter_mut() {
        rs.is_active = n == &name;
    }
    empty()
}

fn op_describe_receipt_rule_set(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = pstring(p, "RuleSetName");
    let Some(rs) = state.rule_sets.get(&name) else {
        return Err(SesError::new(
            "RuleSetDoesNotExist",
            format!("Rule set does not exist: {name}"),
        ));
    };
    Ok(format!(
        "<Metadata>{}</Metadata>{}",
        rule_set_metadata(&name, rs),
        rule_set_rules_xml(rs)
    ))
}

fn op_describe_active_receipt_rule_set(state: &SesState) -> OpResult {
    for (name, rs) in &state.rule_sets {
        if rs.is_active {
            return Ok(format!(
                "<Metadata>{}</Metadata>{}",
                rule_set_metadata(name, rs),
                rule_set_rules_xml(rs)
            ));
        }
    }
    empty()
}

fn op_delete_receipt_rule_set(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = pstring(p, "RuleSetName");
    validate_rule_name("ruleSetName", &name)?;
    let Some(rs) = state.rule_sets.get(&name) else {
        return empty();
    };
    if rs.is_active {
        return Err(SesError::new(
            "CannotDelete",
            format!("Cannot delete active rule set: {name}"),
        ));
    }
    state.rule_sets.remove(&name);
    empty()
}

fn op_list_receipt_rule_sets(state: &SesState) -> OpResult {
    // moto sorts rule sets by name.
    let mut out = String::new();
    for (name, rs) in &state.rule_sets {
        out.push_str(&format!("<member>{}</member>", rule_set_metadata(name, rs)));
    }
    Ok(format!("<RuleSets>{out}</RuleSets>"))
}

fn op_describe_receipt_rule(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let rule_set_name = pstring(p, "RuleSetName");
    let rule_name = pstring(p, "RuleName");
    let Some(rs) = state.rule_sets.get(&rule_set_name) else {
        return Err(SesError::new(
            "RuleSetDoesNotExist",
            format!("Rule set does not exist: {rule_set_name}"),
        ));
    };
    match rs.rules.iter().find(|r| val_name(r) == Some(&rule_name)) {
        Some(rule) => Ok(format!("<Rule>{}</Rule>", render_val(rule))),
        None => Err(SesError::new(
            "RuleDoesNotExist",
            format!("Rule does not exist: {rule_name}"),
        )),
    }
}

fn op_update_receipt_rule(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let rule_set_name = pstring(p, "RuleSetName");
    let rule = p.get("Rule").cloned().unwrap_or(Val::M(BTreeMap::new()));
    let rule_name = rule
        .get("Name")
        .and_then(Val::string)
        .unwrap_or_default()
        .to_string();
    let Some(rs) = state.rule_sets.get_mut(&rule_set_name) else {
        return Err(SesError::new(
            "RuleSetDoesNotExist",
            format!("Rule set does not exist: {rule_set_name}"),
        ));
    };
    match rs
        .rules
        .iter()
        .position(|r| val_name(r) == Some(&rule_name))
    {
        Some(i) => {
            rs.rules[i] = rule;
            empty()
        }
        None => Err(SesError::new(
            "RuleDoesNotExist",
            format!("Rule does not exist: {rule_name}"),
        )),
    }
}

fn op_delete_receipt_rule(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let rule_set_name = pstring(p, "RuleSetName");
    let rule_name = pstring(p, "RuleName");
    validate_rule_name("ruleSetName", &rule_set_name)?;
    validate_rule_name("rule.name", &rule_name)?;
    if let Some(rs) = state.rule_sets.get_mut(&rule_set_name) {
        rs.rules.retain(|r| val_name(r) != Some(&rule_name));
    }
    // moto: idempotent — deleting from a missing rule set succeeds silently.
    empty()
}

// ---------------------------------------------------------------------------
// Receipt filters.
// ---------------------------------------------------------------------------

fn op_create_receipt_filter(state: &mut SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let name = pchild_string(p, "Filter", "Name");
    let policy = pchild_string(p, "Filter", "IpFilter.Policy");
    let cidr = pchild_string(p, "Filter", "IpFilter.Cidr");
    if state.receipt_filters.contains_key(&name) {
        return Err(SesError::new(
            "AlreadyExists",
            format!("Receipt filter already exists: {name}"),
        ));
    }
    state.receipt_filters.insert(name, (policy, cidr));
    empty()
}

/// Nested dotted-key access under a top-level member
/// (`Filter.Name` → `p.get("Filter").get("Name")`).
fn pchild_string(p: &BTreeMap<String, Val>, parent: &str, path: &str) -> String {
    let mut node = p.get(parent);
    for seg in path.split('.') {
        node = match node {
            Some(n) => n.get(seg),
            None => None,
        };
    }
    node.and_then(Val::string).unwrap_or_default().to_string()
}

fn op_list_receipt_filters(state: &SesState) -> OpResult {
    let mut out = String::new();
    for (name, (policy, cidr)) in &state.receipt_filters {
        out.push_str(&format!(
            "<member>{}<IpFilter>{}{}</IpFilter></member>",
            tag("Name", name),
            tag("Policy", policy),
            tag("Cidr", cidr),
        ));
    }
    Ok(format!("<Filters>{out}</Filters>"))
}

// ---------------------------------------------------------------------------
// Custom verification email templates.
// ---------------------------------------------------------------------------

fn parse_custom_template_fields(p: &BTreeMap<String, Val>) -> CustomTemplate {
    CustomTemplate {
        from_email_address: pstring(p, "FromEmailAddress"),
        template_subject: pstring(p, "TemplateSubject"),
        template_content: pstring(p, "TemplateContent"),
        success_redirection_url: pstring(p, "SuccessRedirectionURL"),
        failure_redirection_url: pstring(p, "FailureRedirectionURL"),
    }
}

fn custom_template_xml(name: &str, t: &CustomTemplate) -> String {
    format!(
        "{}{}{}{}{}{}",
        tag("TemplateName", name),
        tag("FromEmailAddress", &t.from_email_address),
        tag("TemplateSubject", &t.template_subject),
        tag("TemplateContent", &t.template_content),
        tag("SuccessRedirectionURL", &t.success_redirection_url),
        tag("FailureRedirectionURL", &t.failure_redirection_url),
    )
}

fn op_create_custom_verification_email_template(
    state: &mut SesState,
    p: &BTreeMap<String, Val>,
) -> OpResult {
    let name = pstring(p, "TemplateName");
    if state.custom_templates.contains_key(&name) {
        return Err(SesError::new(
            "CustomVerificationEmailTemplateAlreadyExists",
            format!("Custom verification email template already exists: {name}"),
        ));
    }
    state
        .custom_templates
        .insert(name, parse_custom_template_fields(p));
    empty()
}

fn op_get_custom_verification_email_template(
    state: &SesState,
    p: &BTreeMap<String, Val>,
) -> OpResult {
    let name = pstring(p, "TemplateName");
    let Some(t) = state.custom_templates.get(&name) else {
        return Err(SesError::new(
            "CustomVerificationEmailTemplateDoesNotExist",
            format!("Custom verification email template does not exist: {name}"),
        ));
    };
    Ok(custom_template_xml(&name, t))
}

fn op_list_custom_verification_email_templates(state: &SesState) -> OpResult {
    let mut out = String::new();
    for (name, t) in &state.custom_templates {
        out.push_str(&format!(
            "<member>{}</member>",
            custom_template_xml(name, t)
        ));
    }
    Ok(format!(
        "<CustomVerificationEmailTemplates>{out}</CustomVerificationEmailTemplates>"
    ))
}

fn op_update_custom_verification_email_template(
    state: &mut SesState,
    p: &BTreeMap<String, Val>,
) -> OpResult {
    let name = pstring(p, "TemplateName");
    if !state.custom_templates.contains_key(&name) {
        return Err(SesError::new(
            "CustomVerificationEmailTemplateDoesNotExist",
            format!("Custom verification email template does not exist: {name}"),
        ));
    }
    state
        .custom_templates
        .insert(name, parse_custom_template_fields(p));
    empty()
}

fn op_send_custom_verification_email(state: &SesState, p: &BTreeMap<String, Val>) -> OpResult {
    let template_name = pstring(p, "TemplateName");
    if !state.custom_templates.contains_key(&template_name) {
        return Err(SesError::new(
            "CustomVerificationEmailTemplateDoesNotExist",
            format!("Custom verification email template does not exist: {template_name}"),
        ));
    }
    Ok(tag("MessageId", &random_message_id()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_wrapped_and_flattened_lists() {
        let body = b"Rule.Name=a&Rule.Recipients.member.1=x@y.z&Filter.IpFilter.Policy=Block";
        let mut pairs = decode_pairs(body);
        pairs.extend(decode_pairs(&b""[..]));
        let mut tree = Val::M(BTreeMap::new());
        for (key, value) in &pairs {
            let segments: Vec<String> = key.split('.').map(str::to_string).collect();
            insert_val(&mut tree, &segments, &Val::S(value.clone()));
        }
        if let Val::M(m) = tree.clone() {
            let rule = m.get("Rule").unwrap();
            assert_eq!(rule.get("Name").and_then(Val::string), Some("a"));
            assert_eq!(
                rule.get("Recipients").unwrap().strings(),
                vec!["x@y.z".to_string()]
            );
        }
    }

    #[test]
    fn response_envelope_matches_botocore_shape() {
        let resp = response_xml(
            "ListIdentities",
            "<Identities><member>a@b.c</member></Identities>",
        );
        let text = String::from_utf8(resp.body).unwrap();
        assert!(text.contains("<ListIdentitiesResult><Identities><member>a@b.c</member></Identities></ListIdentitiesResult>"));
        assert!(text.contains("<ResponseMetadata><RequestId>"));
        assert!(text.starts_with("<?xml"));
    }

    #[test]
    fn parsed_addr_takes_angle_brackets() {
        assert_eq!(parse_addr("Foo <foo@bar.com>"), "foo@bar.com");
        assert_eq!(parse_addr("bare@x.com"), "bare@x.com");
    }
}
