//! ELB (classic, v1) backend — pure-Rust port of the moto elb service.
//!
//! Wire contract: AWS query protocol, XML namespace
//! `http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/`. Semantics
//! mirror the vendored moto implementation (`moto/elb`) for every operation
//! the compat suite (`tests/compatibility/test_elb_compat.py`) exercises,
//! including moto's quirks: register/deregister responses come back with an
//! empty `Instances` list (moto serializes raw ids which the list-of-struct
//! shape drops), duplicate tag-key errors carry the `(key, count)` tuple in
//! the message, `TooManyTags` fires with the `LoadBalancerNotFound` code,
//! `SetLoadBalancerListenerSSLCertificate` never validates the certificate
//! ARN, and `DescribeLoadBalancerPolicyTypes` silently drops unknown type
//! names instead of raising.
//!
//! Cross-service reads/writes (default security group creation, SG
//! existence) go through the gateway loopback, like the ELBv2 port.

use super::elbv2::{
    create_default_security_group, elb_partition, fetch_default_subnets, fetch_subnets, iso_micro,
    p_bool, p_i64,
    p_list, p_string, p_struct_list, parse_params, ElbError, ElbVal, LoopbackBody,
};
use crate::store;
use crate::{Request, Response};
use bytes::Bytes;
use std::collections::BTreeMap;

pub const XMLNS: &str = "http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/";

type OpResult = Result<String, ElbError>;

// ---------------------------------------------------------------------------
// State: per-(account, region) ELB backend, mirroring moto's ELBBackend.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct ElbState {
    pub lbs: Vec<ElbClassicLb>,
}

#[derive(Clone, Debug)]
pub struct ElbClassicLb {
    pub name: String,
    pub dns_name: String,
    pub health_check: Option<ElbHealthCheck>,
    /// Registered instance ids, registration order.
    pub instance_ids: Vec<String>,
    pub availability_zones: Vec<String>,
    pub listeners: Vec<ElbClassicListener>,
    /// Backends mirror listeners by instance port (moto makes one per listener).
    pub backends: Vec<ElbBackend>,
    pub created_time: String,
    pub scheme: Option<String>,
    pub attributes: ElbAttributes,
    pub policies: Vec<ElbPolicy>,
    pub security_groups: Vec<String>,
    pub subnets: Vec<String>,
    pub vpc_id: Option<String>,
    /// Insertion-ordered (Key, Value) tags.
    pub tags: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct ElbAttributes {
    cross_zone_enabled: bool,
    access_log_enabled: bool,
    access_log_bucket: Option<String>,
    access_log_emit_interval: Option<String>,
    access_log_prefix: Option<String>,
    connection_draining_enabled: bool,
    /// moto only adds the key once the attribute block is passed.
    connection_draining_timeout: Option<String>,
    idle_timeout: i64,
    /// (("key", "value"), ...) tuples, insertion order (moto list).
    additional_attributes: Vec<(String, String)>,
}

impl Default for ElbAttributes {
    fn default() -> Self {
        ElbAttributes {
            cross_zone_enabled: false,
            access_log_enabled: false,
            access_log_bucket: None,
            access_log_emit_interval: None,
            access_log_prefix: None,
            connection_draining_enabled: false,
            connection_draining_timeout: None,
            idle_timeout: 60,
            additional_attributes: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ElbPolicy {
    AppCookie {
        policy_name: String,
        cookie_name: String,
    },
    LbCookie {
        policy_name: String,
        cookie_expiration_period: Option<i64>,
    },
    Other {
        policy_name: String,
        policy_type_name: String,
        attrs: Vec<(String, String)>,
    },
}

impl ElbPolicy {
    fn name(&self) -> &str {
        match self {
            ElbPolicy::AppCookie { policy_name, .. }
            | ElbPolicy::LbCookie { policy_name, .. }
            | ElbPolicy::Other { policy_name, .. } => policy_name,
        }
    }
    fn type_name(&self) -> &str {
        match self {
            ElbPolicy::AppCookie { .. } => "AppCookieStickinessPolicy",
            ElbPolicy::LbCookie { .. } => "LbCookieStickinessPolicy",
            ElbPolicy::Other { policy_type_name, .. } => policy_type_name,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ElbClassicListener {
    pub load_balancer_port: String,
    pub instance_port: String,
    pub protocol: String,
    pub ssl_certificate_id: Option<String>,
    pub policy_names: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ElbBackend {
    pub instance_port: String,
    pub policy_names: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ElbHealthCheck {
    pub timeout: String,
    pub healthy_threshold: String,
    pub unhealthy_threshold: String,
    pub interval: String,
    pub target: String,
}

// ---------------------------------------------------------------------------
// XML writers (moto QuerySerializer member ordering; self-closing empties).
// ---------------------------------------------------------------------------

pub fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            c => out.push(c),
        }
    }
    out
}

fn tag_esc(name: &str, body: &str) -> String {
    format!("<{name}>{}</{name}>", esc(body))
}

fn tag_raw(name: &str, body: &str) -> String {
    if body.is_empty() {
        format!("<{name}/>")
    } else {
        format!("<{name}>{body}</{name}>")
    }
}

/// DescribeLoadBalancers body for one LB (moto observed member order).
fn lb_description_xml(acct: u64, lb: &ElbClassicLb) -> String {
    let mut out = String::new();
    out.push_str(&tag_esc("LoadBalancerName", &lb.name));
    out.push_str(&tag_esc("DNSName", &lb.dns_name));
    out.push_str(&tag_esc("CanonicalHostedZoneName", &lb.dns_name));
    let mut listener_descs = String::new();
    for listener in &lb.listeners {
        let mut listener_xml = String::new();
        listener_xml.push_str(&tag_esc("Protocol", &listener.protocol));
        listener_xml.push_str(&tag_esc("LoadBalancerPort", &listener.load_balancer_port));
        listener_xml.push_str(&tag_esc("InstanceProtocol", &listener.protocol));
        listener_xml.push_str(&tag_esc("InstancePort", &listener.instance_port));
        if let Some(cert) = &listener.ssl_certificate_id {
            listener_xml.push_str(&tag_esc("SSLCertificateId", cert));
        }
        let mut policy_names = String::new();
        for name in &listener.policy_names {
            policy_names.push_str(&tag_esc("member", name));
        }
        listener_descs.push_str(&format!(
            "<member><Listener>{listener_xml}</Listener>{}{}</member>",
            "",
            tag_raw("PolicyNames", &policy_names)
        ));
    }
    out.push_str(&tag_raw("ListenerDescriptions", &listener_descs));
    out.push_str(&policies_xml(lb));
    let mut backends = String::new();
    for b in lb.backends.iter().filter(|b| !b.policy_names.is_empty()) {
        let mut policy_names = String::new();
        for name in &b.policy_names {
            policy_names.push_str(&tag_esc("member", name));
        }
        backends.push_str(&format!(
            "<member>{}{}</member>",
            tag_esc("InstancePort", &b.instance_port),
            tag_raw("PolicyNames", &policy_names)
        ));
    }
    out.push_str(&tag_raw("BackendServerDescriptions", &backends));
    let mut azs = String::new();
    for az in &lb.availability_zones {
        azs.push_str(&tag_esc("member", az));
    }
    out.push_str(&tag_raw("AvailabilityZones", &azs));
    let mut subs = String::new();
    for s in &lb.subnets {
        subs.push_str(&tag_esc("member", s));
    }
    out.push_str(&tag_raw("Subnets", &subs));
    if let Some(vpc) = &lb.vpc_id {
        out.push_str(&tag_esc("VPCId", vpc));
    }
    let mut instances = String::new();
    for id in &lb.instance_ids {
        instances.push_str(&format!("<member>{}</member>", tag_esc("InstanceId", id)));
    }
    out.push_str(&tag_raw("Instances", &instances));
    if let Some(hc) = &lb.health_check {
        let mut h = String::new();
        h.push_str(&tag_esc("Target", &hc.target));
        h.push_str(&tag_esc("Interval", &hc.interval));
        h.push_str(&tag_esc("Timeout", &hc.timeout));
        h.push_str(&tag_esc("UnhealthyThreshold", &hc.unhealthy_threshold));
        h.push_str(&tag_esc("HealthyThreshold", &hc.healthy_threshold));
        out.push_str(&tag_raw("HealthCheck", &h));
    }
    out.push_str(&format!(
        "<SourceSecurityGroup><OwnerAlias>{}</OwnerAlias><GroupName>default</GroupName></SourceSecurityGroup>",
        esc(&acct.to_string())
    ));
    let mut sgs = String::new();
    for sg in &lb.security_groups {
        sgs.push_str(&tag_esc("member", sg));
    }
    out.push_str(&tag_raw("SecurityGroups", &sgs));
    out.push_str(&tag_esc("CreatedTime", &lb.created_time));
    out.push_str(&tag_esc(
        "Scheme",
        lb.scheme.as_deref().unwrap_or("internet-facing"),
    ));
    out
}

/// Policies block via moto's `transform_policies`.
fn policies_xml(lb: &ElbClassicLb) -> String {
    let mut app = String::new();
    let mut lbc = String::new();
    let mut other = String::new();
    for policy in &lb.policies {
        match policy {
            ElbPolicy::AppCookie {
                policy_name,
                cookie_name,
            } => {
                app.push_str(&format!(
                    "<member>{}{}</member>",
                    tag_esc("PolicyName", policy_name),
                    tag_esc("CookieName", cookie_name)
                ));
            }
            ElbPolicy::LbCookie {
                policy_name,
                cookie_expiration_period,
            } => {
                lbc.push_str(&format!(
                    "<member>{}{}</member>",
                    tag_esc("PolicyName", policy_name),
                    tag_esc(
                        "CookieExpirationPeriod",
                        &cookie_expiration_period
                            .map(|p| p.to_string())
                            .unwrap_or_default()
                    )
                ));
            }
            ElbPolicy::Other { policy_name, .. } => {
                other.push_str(&format!("<member>{}</member>", policy_name));
            }
        }
    }
    let out = format!(
        "{}{}{}",
        tag_raw("AppCookieStickinessPolicies", &app),
        tag_raw("LBCookieStickinessPolicies", &lbc),
        tag_raw("OtherPolicies", &other)
    );
    tag_raw("Policies", &out)
}

/// LoadBalancerAttributes XML (moto member order; values as moto stores them).
fn attributes_xml(lb: &ElbClassicLb) -> String {
    let a = &lb.attributes;
    let mut out = format!(
        "<CrossZoneLoadBalancing>{}</CrossZoneLoadBalancing>",
        tag_esc("Enabled", if a.cross_zone_enabled { "true" } else { "false" })
    );
    let mut access = tag_esc(
        "Enabled",
        if a.access_log_enabled { "true" } else { "false" },
    );
    if let Some(bucket) = &a.access_log_bucket {
        access.push_str(&tag_esc("S3BucketName", bucket));
    }
    if let Some(interval) = &a.access_log_emit_interval {
        access.push_str(&tag_esc("EmitInterval", interval));
    }
    if let Some(prefix) = &a.access_log_prefix {
        access.push_str(&tag_esc("S3BucketPrefix", prefix));
    }
    out.push_str(&tag_raw("AccessLog", &access));
    let mut draining = tag_esc(
        "Enabled",
        if a.connection_draining_enabled { "true" } else { "false" },
    );
    if let Some(timeout) = &a.connection_draining_timeout {
        draining.push_str(&tag_esc("Timeout", timeout));
    }
    out.push_str(&tag_raw("ConnectionDraining", &draining));
    out.push_str(&format!(
        "<ConnectionSettings>{}</ConnectionSettings>",
        tag_esc("IdleTimeout", &a.idle_timeout.to_string())
    ));
    let mut additional = String::new();
    for (k, v) in &a.additional_attributes {
        additional.push_str(&format!(
            "<member>{}{}</member>",
            tag_esc("Key", k),
            tag_esc("Value", v)
        ));
    }
    out.push_str(&tag_raw("AdditionalAttributes", &additional));
    format!("<LoadBalancerAttributes>{out}</LoadBalancerAttributes>")
}

const POLICY_TYPES: [(&str, &str, &str); 6] = [
    (
        "SSLNegotiationPolicyType",
        "Listener policy that defines the ciphers and protocols that will be accepted by the load balancer. This policy can be associated only with HTTPS/SSL listeners.",
        "Protocol-SSLv3,Boolean,ZERO_OR_ONE|Protocol-TLSv1,Boolean,ZERO_OR_ONE|Protocol-TLSv1.1,Boolean,ZERO_OR_ONE|Protocol-TLSv1.2,Boolean,ZERO_OR_ONE",
    ),
    (
        "AppCookieStickinessPolicyType",
        "Stickiness policy with session lifetimes controlled by the lifetime of the application-generated cookie. This policy can be associated only with HTTP/HTTPS listeners.",
        "CookieName,String,ONE",
    ),
    (
        "LBCookieStickinessPolicyType",
        "Stickiness policy with session lifetimes controlled by the browser (user-agent) or a specified expiration period. This policy can be associated only with HTTP/HTTPS listeners.",
        "CookieExpirationPeriod,Long,ZERO_OR_ONE",
    ),
    (
        "ProxyProtocolPolicyType",
        "Policy that controls whether to include the IP address and port of the originating request for TCP messages. This policy operates on TCP/SSL listeners only.",
        "ProxyProtocol,Boolean,ONE",
    ),
    (
        "BackendServerAuthenticationPolicyType",
        "Policy that controls authentication to back-end server(s) and target groups.",
        "PublicKeyPolicyName,PolicyName,ONE_OR_MORE",
    ),
    (
        "PublicKeyPolicyType",
        "Policy containing a list of public keys to accept when authenticating the back-end server(s). This policy cannot be applied directly to back-end servers or listeners but must be part of a BackendServerAuthenticationPolicyType.",
        "PublicKey,String,ONE",
    ),
];

fn policy_types_xml(filter: &[String]) -> String {
    let mut items = String::new();
    for (name, description, attrs) in POLICY_TYPES.iter() {
        if !filter.is_empty() && !filter.iter().any(|f| f == name) {
            continue;
        }
        let mut attr_items = String::new();
        for attr in attrs.split('|') {
            let mut parts = attr.splitn(3, ',');
            let attr_name = parts.next().unwrap_or("");
            let attr_type = parts.next().unwrap_or("");
            let cardinality = parts.next().unwrap_or("");
            attr_items.push_str(&format!(
                "<member>{}{}{}</member>",
                tag_esc("AttributeName", attr_name),
                tag_esc("AttributeType", attr_type),
                tag_esc("Cardinality", cardinality)
            ));
        }
        items.push_str(&format!(
            "<member>{}{}{}</member>",
            tag_esc("PolicyTypeName", name),
            tag_esc("Description", description),
            tag_raw("PolicyAttributeTypeDescriptions", &attr_items)
        ));
    }
    tag_raw("PolicyTypeDescriptions", &items)
}

fn policies_descriptions_xml(lb: &ElbClassicLb, filter: &[String]) -> OpResult {
    let mut items = String::new();
    for policy in &lb.policies {
        if !filter.is_empty() && !filter.iter().any(|n| n == policy.name()) {
            continue;
        }
        let mut member = String::new();
        member.push_str(&tag_esc("PolicyName", policy.name()));
        member.push_str(&tag_esc("PolicyTypeName", policy.type_name()));
        if let ElbPolicy::Other { attrs, .. } = policy {
            if !attrs.is_empty() {
                let mut attr_items = String::new();
                for (aname, avalue) in attrs {
                    attr_items.push_str(&format!(
                        "<member>{}{}</member>",
                        tag_esc("AttributeName", aname),
                        tag_esc("AttributeValue", avalue)
                    ));
                }
                member.push_str(&tag_raw("PolicyAttributeDescriptions", &attr_items));
            }
        }
        items.push_str(&format!("<member>{member}</member>"));
    }
    if !filter.is_empty() && items.is_empty() {
        return Err(ElbError::svc(
            "PolicyNotFound",
            "There is no policy with name . for load balancer .",
        ));
    }
    Ok(tag_raw("PolicyDescriptions", &items))
}

// ---------------------------------------------------------------------------
// SG existence probe (mirrors moto's ec2-backend lookup).
// ---------------------------------------------------------------------------

fn val_non_empty(v: &ElbVal) -> bool {
    !matches!(v, ElbVal::None) && !matches!(v, ElbVal::S(s) if s.is_empty())
}

fn val_map_list(v: &ElbVal) -> Vec<BTreeMap<String, ElbVal>> {
    v.map_list()
}

fn sg_exists(req: &Request, id: &str) -> bool {
    let body = LoopbackBody::Form(format!(
        "Action=DescribeSecurityGroups&Version=2016-11-15&Filter.1.Name=group-id&Filter.1.Values.member.1={id}"
    ));
    matches!(super::elbv2::loopback(req, "ec2", &body), Some((200, _)))
}

fn instance_states(req: &Request, ids: &[String]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if ids.is_empty() {
        return out;
    }
    let mut pairs = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        pairs.push((format!("InstanceIds.member.{}", i + 1), id.clone()));
    }
    let body = LoopbackBody::Form(
        "Action=DescribeInstances".to_string() + &super::elbv2::encoded_pairs(&pairs),
    );
    if let Some((200, xml)) = super::elbv2::loopback(req, "ec2", &body) {
        out.extend(super::elbv2::instance_set_states(&xml));
    }
    out
}
// ---------------------------------------------------------------------------
// Ops.
// ---------------------------------------------------------------------------

fn tag_pairs(tags_in: &[BTreeMap<String, ElbVal>]) -> Vec<(String, String)> {
    tags_in
        .iter()
        .filter_map(|m| {
            let k = m.get("Key").and_then(|v| v.string())?.to_string();
            let v = m
                .get("Value")
                .and_then(|v| v.string())
                .unwrap_or_default()
                .to_string();
            Some((k, v))
        })
        .collect()
}

/// moto `ELBResponse._add_tags`: duplicate keys raise with the tuple text.
fn add_tags_checked(lb: &mut ElbClassicLb, tags: &[(String, String)]) -> Result<(), ElbError> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for (k, _) in tags {
        *counts.entry(k.as_str()).or_insert(0) += 1;
    }
    if let Some((k, c)) = counts.iter().max_by_key(|(_, c)| **c) {
        if *c > 1 {
            return Err(ElbError::svc(
                "DuplicateTagKeys",
                format!("Tag key was specified more than once: ('{k}', {c})"),
            ));
        }
    }
    for (k, v) in tags {
        // moto raises TooManyTags when the 10-tag quota is exceeded
        // (with the wrong error code — kept for parity).
        if lb.tags.len() >= 10 && !lb.tags.iter().any(|(ek, _)| ek == k) {
            return Err(ElbError::svc(
                "LoadBalancerNotFound",
                "The quota for the number of tags that can be assigned to a load balancer has been reached",
            ));
        }
        if let Some(slot) = lb.tags.iter_mut().find(|(ek, _)| ek == k) {
            slot.1 = v.clone();
        } else {
            lb.tags.push((k.clone(), v.clone()));
        }
    }
    Ok(())
}

fn lb_by<'a>(state: &'a mut ElbState, name: &str) -> Option<&'a mut ElbClassicLb> {
    state.lbs.iter_mut().find(|l| l.name == name)
}

fn op_create_load_balancer(
    req: &Request,
    state: &mut ElbState,
    p: &BTreeMap<String, ElbVal>,
) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let zones = p_list(p, "AvailabilityZones");
    let listeners_in = p_struct_list(p, "Listeners");
    let scheme = p_string(p, "Scheme").map(|s| s.to_string());
    let subnets = p_list(p, "Subnets");
    let security_groups = p_list(p, "SecurityGroups");
    let tags_in = p_struct_list(p, "Tags");

    if state.lbs.iter().any(|l| l.name == name) {
        return Err(ElbError::svc(
            "DuplicateLoadBalancerName",
            format!("The specified load balancer name already exists for this account: {name}"),
        ));
    }
    if listeners_in.is_empty() {
        return Err(ElbError::svc("ValidationError", "Listeners cannot be empty"));
    }

    // moto resolves the first vpc id from subnets or default subnets per zone.
    let mut subnet_ids = subnets.clone();
    if subnet_ids.is_empty() && !zones.is_empty() {
        let defaults = fetch_default_subnets(req);
        for zone in &zones {
            if let Some(id) = defaults.get(zone.as_str()) {
                subnet_ids.push(id.clone());
            }
        }
    }
    let mut vpc_id: Option<String> = None;
    let mut resolved_subnets: Vec<String> = Vec::new();
    if !subnet_ids.is_empty() {
        let info = fetch_subnets(req, &subnet_ids);
        for id in &subnet_ids {
            if let Some((Some(vpc), _)) = info.get(id) {
                if vpc_id.is_none() {
                    vpc_id = Some(vpc.clone());
                }
                resolved_subnets.push(id.clone());
            }
        }
    }

    let security_groups = if security_groups.is_empty() {
        create_default_security_group(req, vpc_id.as_deref())
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        for sg in &security_groups {
            if !sg_exists(req, sg) {
                return Err(ElbError::svc(
                    "ValidationError",
                    "One or more of the specified security groups do not exist.",
                ));
            }
        }
        security_groups.clone()
    };

    let listeners: Vec<ElbClassicListener> = listeners_in
        .iter()
        .map(|m| ElbClassicListener {
            protocol: m
                .get("Protocol")
                .and_then(|v| v.string())
                .unwrap_or_default()
                .to_uppercase(),
            load_balancer_port: m
                .get("LoadBalancerPort")
                .and_then(|v| v.string())
                .unwrap_or_default()
                .to_string(),
            instance_port: m
                .get("InstancePort")
                .and_then(|v| v.string())
                .unwrap_or_default()
                .to_string(),
            ssl_certificate_id: m
                .get("SSLCertificateId")
                .and_then(|v| v.string())
                .map(|s| s.to_string()),
            policy_names: Vec::new(),
        })
        .collect();
    let backends = listeners
        .iter()
        .map(|l| ElbBackend {
            instance_port: l.instance_port.clone(),
            policy_names: Vec::new(),
        })
        .collect();
    // moto hardcodes us-east-1 in the classic DNS name (kept for parity).
    let dns_name = format!("{name}.us-east-1.elb.amazonaws.com");
    let lb = ElbClassicLb {
        name: name.clone(),
        dns_name: dns_name.clone(),
        health_check: None,
        instance_ids: Vec::new(),
        availability_zones: zones,
        listeners,
        backends,
        created_time: iso_micro(),
        scheme,
        attributes: ElbAttributes::default(),
        policies: Vec::new(),
        security_groups,
        subnets: resolved_subnets,
        vpc_id,
        tags: Vec::new(),
    };
    let tags = tag_pairs(&tags_in);
    let lb = ElbClassicLb {
        tags,
        ..lb
    };
    state.lbs.push(lb);
    Ok(tag_esc("DNSName", &dns_name))
}


fn op_describe_load_balancers(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let names = p_list(p, "LoadBalancerNames");
    let matched: Vec<ElbClassicLb> = if names.is_empty() {
        state.lbs.clone()
    } else {
        let mut out = Vec::new();
        for name in &names {
            match state.lbs.iter().find(|l| l.name == *name) {
                Some(lb) => out.push(lb.clone()),
                None => {
                    return Err(ElbError::svc(
                        "LoadBalancerNotFound",
                        format!("The specified load balancer does not exist: {name}"),
                    ))
                }
            }
        }
        out
    };
    let mut body = String::new();
    let acct_lb: Vec<(u64, ElbClassicLb)> = matched.into_iter().map(|l| (0u64, l)).collect();
    let _ = acct_lb;
    for lb in &state.lbs {
        if names.is_empty() || names.iter().any(|n| n == &lb.name) {
            body.push_str(&format!(
                "<member>{}</member>",
                lb_description_xml(123456789012, lb)
            ));
        }
    }
    Ok(tag_raw("LoadBalancerDescriptions", &body))
}

fn op_delete_load_balancer(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    state.lbs.retain(|l| l.name != name);
    Ok(String::new())
}

fn op_create_load_balancer_listeners(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let listeners_in = p_struct_list(p, "Listeners");
    for m in &listeners_in {
        let protocol = m.get("Protocol").and_then(|v| v.string()).unwrap_or_default().to_string();
        let instance_port = m.get("InstancePort").and_then(|v| v.string()).unwrap_or_default().to_string();
        let lb_port = m.get("LoadBalancerPort").and_then(|v| v.string()).unwrap_or_default().to_string();
        let cert = m.get("SSLCertificateId").and_then(|v| v.string()).map(|s| s.to_string());
        let Some(lb) = lb_by(state, &name) else {
            continue;
        };
        let conflicting = lb
            .listeners
            .iter()
            .find(|l| l.load_balancer_port == lb_port)
            .map(|l| {
                l.protocol.to_lowercase() != protocol.to_lowercase()
                    || l.instance_port != instance_port
                    || l.ssl_certificate_id.clone().unwrap_or_default() != cert.clone().unwrap_or_default()
            })
            .unwrap_or(false);
        if conflicting {
            return Err(ElbError::svc(
                "DuplicateListener",
                format!(
                    "A listener already exists for {name} with LoadBalancerPort {lb_port}, but with a different InstancePort, Protocol, or SSLCertificateId"
                ),
            ));
        }
        let already = lb
            .listeners
            .iter()
            .any(|l| l.load_balancer_port == lb_port);
        let backend_port = instance_port.clone();
        if !already {
            lb.listeners.push(ElbClassicListener {
                load_balancer_port: lb_port.clone(),
                instance_port,
                protocol: protocol.to_uppercase(),
                ssl_certificate_id: cert,
                policy_names: Vec::new(),
            });
            lb.backends.push(ElbBackend {
                instance_port: backend_port,
                policy_names: Vec::new(),
            });
        }
    }
    Ok(String::new())
}

fn op_delete_load_balancer_listeners(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let ports = p_list(p, "LoadBalancerPorts");
    let Some(lb) = lb_by(state, &name) else {
        return Ok(String::new());
    };
    lb.listeners
        .retain(|l| !ports.iter().any(|port| port == &l.load_balancer_port));
    lb.backends = lb
        .listeners
        .iter()
        .map(|l| ElbBackend {
            instance_port: l.instance_port.clone(),
            policy_names: Vec::new(),
        })
        .collect();
    Ok(String::new())
}

fn op_configure_health_check(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let hc = p.get("HealthCheck").cloned().unwrap_or(ElbVal::None);
    let hc: BTreeMap<String, ElbVal> = match hc {
        ElbVal::M(m) => m,
        _ => BTreeMap::new(),
    };
    let target = hc
        .get("Target")
        .and_then(|v| v.string())
        .unwrap_or_default()
        .to_string();
    if !(target.starts_with("HTTP")
        || target.starts_with("TCP")
        || target.starts_with("HTTPS")
        || target.starts_with("SSL"))
    {
        return Err(ElbError::svc(
            "ValidationError",
            "HealthCheck Target must begin with one of HTTP, TCP, HTTPS, SSL",
        ));
    }
    let check = ElbHealthCheck {
        target: target.clone(),
        interval: hc.get("Interval").and_then(|v| v.string()).unwrap_or_default().to_string(),
        timeout: hc.get("Timeout").and_then(|v| v.string()).unwrap_or_default().to_string(),
        unhealthy_threshold: hc
            .get("UnhealthyThreshold")
            .and_then(|v| v.string())
            .unwrap_or_default()
            .to_string(),
        healthy_threshold: hc
            .get("HealthyThreshold")
            .and_then(|v| v.string())
            .unwrap_or_default()
            .to_string(),
    };
    if let Some(lb) = lb_by(state, &name) {
        lb.health_check = Some(check.clone());
    }
    Ok(format!(
        "<HealthCheck>{}{}</HealthCheck>",
        tag_esc("Target", &target),
        "".to_string()
            + &tag_esc("Interval", &check.interval)
            + &tag_esc("Timeout", &check.timeout)
            + &tag_esc("UnhealthyThreshold", &check.unhealthy_threshold)
            + &tag_esc("HealthyThreshold", &check.healthy_threshold)
    ))
}

fn op_register_instances(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let ids: Vec<String> = p_struct_list(p, "Instances")
        .iter()
        .filter_map(|m| m.get("InstanceId").and_then(|v| v.string()).map(|s| s.to_string()))
        .collect();
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::internal("'NoneType' object has no attribute 'instance_ids'"));
    };
    for id in ids {
        if !lb.instance_ids.contains(&id) {
            lb.instance_ids.push(id);
        }
    }
    // moto returns {"Instances": [instance_ids]} — a list-of-list the
    // serializer renders as an empty Instances element.
    Ok("<Instances/>".to_string())
}

fn op_deregister_instances(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let ids: Vec<String> = p_struct_list(p, "Instances")
        .iter()
        .filter_map(|m| m.get("InstanceId").and_then(|v| v.string()).map(|s| s.to_string()))
        .collect();
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::internal("'NoneType' object has no attribute 'instance_ids'"));
    };
    lb.instance_ids.retain(|id| !ids.contains(id));
    Ok("<Instances/>".to_string())
}

fn op_describe_instance_health(req: &Request, state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::svc(
            "LoadBalancerNotFound",
            format!("There is no ACTIVE Load Balancer named '{name}'"),
        ));
    };
    let provided: Vec<String> = p_struct_list(p, "Instances")
        .iter()
        .filter_map(|m| m.get("InstanceId").and_then(|v| v.string()).map(|s| s.to_string()))
        .collect();
    let ids = if provided.is_empty() {
        lb.instance_ids.clone()
    } else {
        provided.clone()
    };
    // moto only reports instances that exist in EC2 and are registered;
    // unregistered ids come back with State "Unknown".
    let probe_ids = ids.iter().filter(|i| lb.instance_ids.contains(i)).cloned().collect::<Vec<_>>();
    let states = instance_states(req, &probe_ids);
    let mut items = String::new();
    for id in &ids {
        if !lb.instance_ids.contains(id) {
            items.push_str(&format!(
                "<member>{instance}</member>",
                instance = format!(
                    "{}{}{}{}",
                    tag_esc("Description", "N/A"),
                    tag_esc("InstanceId", id),
                    tag_esc("State", "Unknown"),
                    tag_esc("ReasonCode", "N/A"),
                )
            ));
            continue;
        }
        match states.get(id) {
            // moto skips ids EC2 does not know about
            None => continue,
            Some(st) => {
                let state_text = if st == "running" || st == "pending" {
                    "InService"
                } else {
                    "OutOfService"
                };
                items.push_str(&format!(
                    "<member>{}{}{}{}</member>",
                    tag_esc("Description", "N/A"),
                    tag_esc("InstanceId", id),
                    tag_esc("State", state_text),
                    tag_esc("ReasonCode", "N/A"),
                ));
            }
        }
    }
    Ok(tag_raw("InstanceStates", &items))
}

fn op_add_tags(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let names = p_list(p, "LoadBalancerNames");
    let tags = tag_pairs(&p_struct_list(p, "Tags"));
    for name in &names {
        let Some(lb) = lb_by(state, name) else {
            return Err(ElbError::svc(
                "LoadBalancerNotFound",
                format!("The specified load balancer does not exist: {name}"),
            ));
        };
        add_tags_checked(lb, &tags)?;
    }
    Ok(String::new())
}

fn op_describe_tags(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let names = p_list(p, "LoadBalancerNames");
    let mut items = String::new();
    for name in &names {
        let Some(lb) = lb_by(state, name) else {
            return Err(ElbError::svc(
                "LoadBalancerNotFound",
                format!("The specified load balancer does not exist: {name}"),
            ));
        };
        let mut tags = String::new();
        for (k, v) in &lb.tags {
            tags.push_str(&format!(
                "<member>{}{}</member>",
                tag_esc("Key", k),
                tag_esc("Value", v)
            ));
        }
        items.push_str(&format!(
            "<member>{}{}</member>",
            tag_esc("LoadBalancerName", name),
            tag_raw("Tags", &tags)
        ));
    }
    Ok(tag_raw("TagDescriptions", &items))
}

fn op_remove_tags(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let names = p_list(p, "LoadBalancerNames");
    let keys: Vec<String> = p_struct_list(p, "Tags")
        .iter()
        .filter_map(|m| m.get("Key").and_then(|v| v.string()).map(|s| s.to_string()))
        .collect();
    for name in &names {
        let Some(lb) = lb_by(state, name) else {
            return Err(ElbError::svc(
                "LoadBalancerNotFound",
                format!("The specified load balancer does not exist: {name}"),
            ));
        };
        lb.tags.retain(|(k, _)| !keys.contains(k));
    }
    Ok(String::new())
}

fn op_describe_load_balancer_attributes(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::internal("'NoneType' object has no attribute 'attributes'"));
    };
    Ok(attributes_xml(lb))
}

fn op_modify_load_balancer_attributes(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::internal("'NoneType' object has no attribute 'attributes'"));
    };
    let attrs = p.get("LoadBalancerAttributes").cloned().unwrap_or(ElbVal::None);
    if let ElbVal::M(_) = &attrs {
        if let Some(cz) = attrs.get("CrossZoneLoadBalancing") {
            lb.attributes.cross_zone_enabled = cz.get("Enabled").and_then(|v| v.opt_bool()).unwrap_or(false);
        }
        if let Some(al) = attrs.get("AccessLog") {
            let has = val_non_empty(&al);
            if has {
                lb.attributes.access_log_enabled = al.get("Enabled").and_then(|v| v.opt_bool()).unwrap_or(false);
                lb.attributes.access_log_bucket = al.get("S3BucketName").and_then(|v| v.string()).map(|s| s.to_string());
                lb.attributes.access_log_emit_interval = al.get("EmitInterval").and_then(|v| v.string()).map(|s| s.to_string());
                lb.attributes.access_log_prefix = al.get("S3BucketPrefix").and_then(|v| v.string()).map(|s| s.to_string());
            }
        }
        if let Some(dr) = attrs.get("ConnectionDraining") {
            if val_non_empty(&dr) {
                lb.attributes.connection_draining_enabled = dr.get("Enabled").and_then(|v| v.opt_bool()).unwrap_or(false);
                lb.attributes.connection_draining_timeout = dr
                    .get("Timeout")
                    .and_then(|v| v.string())
                    .map(|s| s.to_string())
                    .or_else(|| Some("300".to_string()));
            }
        }
        if let Some(st) = attrs.get("ConnectionSettings") {
            if val_non_empty(&st) {
                lb.attributes.idle_timeout = st.get("IdleTimeout").and_then(|v| v.opt_i64()).unwrap_or(60);
            }
        }
        let additional: Vec<BTreeMap<String, ElbVal>> = attrs
            .get("AdditionalAttributes")
            .map(|v| v.map_list())
            .unwrap_or_default();
        if !additional.is_empty() {
            lb.attributes.additional_attributes = additional
                .iter()
                .filter_map(|m| {
                    let k = m.get("Key").and_then(|v| v.string())?.to_string();
                    let v = m
                        .get("Value")
                        .and_then(|v| v.string())
                        .unwrap_or_default()
                        .to_string();
                    Some((k, v))
                })
                .collect();
        } else {
            // moto's quirk: every modify without AdditionalAttributes appends
            // the desync-mitigation default (duplicates accumulate).
            lb.attributes
                .additional_attributes
                .push(("elb.http.desyncmitigationmode".to_string(), "defensive".to_string()));
        }
    }
    Ok(format!(
        "{}{}",
        tag_esc("LoadBalancerName", &lb.name),
        attributes_xml(lb)
    ))
}
fn op_set_load_balancer_listener_ssl_certificate(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let lb_port = p_string(p, "LoadBalancerPort").unwrap_or_default().to_string();
    let cert = p_string(p, "SSLCertificateId").unwrap_or_default().to_string();
    if let Some(lb) = lb_by(state, &name) {
        for listener in lb.listeners.iter_mut() {
            if listener.load_balancer_port == lb_port {
                listener.ssl_certificate_id = Some(cert.clone());
            }
        }
    }
    Ok(String::new())
}

fn op_create_load_balancer_policy(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let policy_name = p_string(p, "PolicyName").unwrap_or_default().to_string();
    let policy_type = p_string(p, "PolicyTypeName").unwrap_or_default().to_string();
    let attrs: Vec<(String, String)> = p_struct_list(p, "PolicyAttributes")
        .iter()
        .map(|m| {
            (
                m.get("AttributeName").and_then(|v| v.string()).unwrap_or_default().to_string(),
                m.get("AttributeValue").and_then(|v| v.string()).unwrap_or_default().to_string(),
            )
        })
        .collect();
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::internal("'NoneType' object has no attribute 'policies'"));
    };
    if !lb.policies.iter().any(|p| p.name() == policy_name) {
        lb.policies.push(ElbPolicy::Other {
            policy_name,
            policy_type_name: policy_type,
            attrs,
        });
    }
    Ok(String::new())
}

fn op_create_app_cookie_stickiness_policy(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let policy_name = p_string(p, "PolicyName").unwrap_or_default().to_string();
    let cookie_name = p_string(p, "CookieName").unwrap_or_default().to_string();
    if let Some(lb) = lb_by(state, &name) {
        lb.policies.push(ElbPolicy::AppCookie {
            policy_name,
            cookie_name,
        });
    }
    Ok(String::new())
}

fn op_create_lb_cookie_stickiness_policy(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let policy_name = p_string(p, "PolicyName").unwrap_or_default().to_string();
    let cookie_expiration_period = p_i64(p, "CookieExpirationPeriod");
    if let Some(lb) = lb_by(state, &name) {
        lb.policies.push(ElbPolicy::LbCookie {
            policy_name,
            cookie_expiration_period,
        });
    }
    Ok(String::new())
}

fn op_describe_load_balancer_policies(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let policy_names = p_list(p, "PolicyNames");
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::internal("'NoneType' object has no attribute 'policies'"));
    };
    policies_descriptions_xml(lb, &policy_names)
}

fn op_describe_load_balancer_policy_types(p: &BTreeMap<String, ElbVal>) -> String {
    policy_types_xml(&p_list(p, "PolicyTypeNames"))
}

fn op_delete_load_balancer_policy(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let policy_name = p_string(p, "PolicyName").unwrap_or_default().to_string();
    if let Some(lb) = lb_by(state, &name) {
        lb.policies.retain(|p| p.name() != policy_name);
    }
    Ok(String::new())
}

fn op_set_load_balancer_policies_of_listener(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let lb_port = p_string(p, "LoadBalancerPort").unwrap_or_default().to_string();
    let policy_names = p_list(p, "PolicyNames");
    if let Some(lb) = lb_by(state, &name) {
        for listener in lb.listeners.iter_mut() {
            if listener.load_balancer_port == lb_port {
                listener.policy_names = policy_names.clone();
            }
        }
    }
    Ok(String::new())
}

fn op_set_load_balancer_policies_for_backend_server(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let instance_port = p_string(p, "InstancePort").unwrap_or_default().to_string();
    let policy_names = p_list(p, "PolicyNames");
    if let Some(lb) = lb_by(state, &name) {
        for backend in lb.backends.iter_mut() {
            if backend.instance_port == instance_port {
                backend.policy_names = policy_names.clone();
            }
        }
    }
    Ok(String::new())
}

fn op_describe_account_limits() -> String {
    let limits = [
        ("classic-load-balancers", "20"),
        ("classic-listeners", "100"),
        ("classic-registered-instances", "1000"),
    ];
    let mut body = String::new();
    for (name, max) in limits {
        body.push_str(&format!("<member><Name>{name}</Name><Max>{max}</Max></member>"));
    }
    tag_raw("Limits", &body)
}

fn op_enable_availability_zones(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let zones = p_list(p, "AvailabilityZones");
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::svc(
            "LoadBalancerNotFound",
            format!("The specified load balancer does not exist: {name}"),
        ));
    };
    let mut set: Vec<String> = lb.availability_zones.clone();
    for az in &zones {
        if !set.contains(az) {
            set.push(az.clone());
        }
    }
    set.sort();
    lb.availability_zones = set.clone();
    let mut items = String::new();
    for az in &set {
        items.push_str(&tag_esc("member", az));
    }
    Ok(tag_raw("AvailabilityZones", &items))
}

fn op_disable_availability_zones(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let zones = p_list(p, "AvailabilityZones");
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::svc(
            "LoadBalancerNotFound",
            format!("The specified load balancer does not exist: {name}"),
        ));
    };
    let set: Vec<String> = lb
        .availability_zones
        .iter()
        .filter(|az| !zones.contains(az))
        .cloned()
        .collect();
    let mut set = set;
    set.sort();
    lb.availability_zones = set.clone();
    let mut items = String::new();
    for az in &set {
        items.push_str(&tag_esc("member", az));
    }
    Ok(tag_raw("AvailabilityZones", &items))
}

fn op_apply_security_groups(req: &Request, state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let sgs = p_list(p, "SecurityGroups");
    for sg in &sgs {
        if !sg_exists(req, sg) {
            return Err(ElbError::svc(
                "ValidationError",
                "One or more of the specified security groups do not exist.",
            ));
        }
    }
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::internal("'NoneType' object has no attribute 'security_groups'"));
    };
    lb.security_groups = sgs.clone();
    let mut items = String::new();
    for sg in &sgs {
        items.push_str(&tag_esc("member", sg));
    }
    Ok(tag_raw("SecurityGroups", &items))
}

fn op_attach_load_balancer_to_subnets(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let subnets = p_list(p, "Subnets");
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::internal("'NoneType' object has no attribute 'subnets'"));
    };
    let mut set: Vec<String> = lb.subnets.clone();
    for s in &subnets {
        if !set.contains(s) {
            set.push(s.clone());
        }
    }
    lb.subnets = set.clone();
    let mut items = String::new();
    for s in &set {
        items.push_str(&tag_esc("member", s));
    }
    Ok(tag_raw("Subnets", &items))
}

fn op_detach_load_balancer_from_subnets(state: &mut ElbState, p: &BTreeMap<String, ElbVal>) -> OpResult {
    let name = p_string(p, "LoadBalancerName").unwrap_or_default().to_string();
    let subnets = p_list(p, "Subnets");
    let Some(lb) = lb_by(state, &name) else {
        return Err(ElbError::internal("'NoneType' object has no attribute 'subnets'"));
    };
    let set: Vec<String> = lb
        .subnets
        .iter()
        .filter(|s| !subnets.contains(s))
        .cloned()
        .collect();
    lb.subnets = set.clone();
    let mut items = String::new();
    for s in &set {
        items.push_str(&tag_esc("member", s));
    }
    Ok(tag_raw("Subnets", &items))
}

// ---------------------------------------------------------------------------
// Dispatch.
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn run_op(
    req: &Request,
    state: &mut ElbState,
    op: &str,
    p: &BTreeMap<String, ElbVal>,
) -> OpResult {
    match op {
        "CreateLoadBalancer" => op_create_load_balancer(req, state, p),
        "DescribeLoadBalancers" => op_describe_load_balancers(state, p),
        "DeleteLoadBalancer" => op_delete_load_balancer(state, p),
        "CreateLoadBalancerListeners" => op_create_load_balancer_listeners(state, p),
        "DeleteLoadBalancerListeners" => op_delete_load_balancer_listeners(state, p),
        "ConfigureHealthCheck" => op_configure_health_check(state, p),
        "RegisterInstancesWithLoadBalancer" => op_register_instances(state, p),
        "DeregisterInstancesFromLoadBalancer" => op_deregister_instances(state, p),
        "DescribeInstanceHealth" => op_describe_instance_health(req, state, p),
        "AddTags" => op_add_tags(state, p),
        "DescribeTags" => op_describe_tags(state, p),
        "RemoveTags" => op_remove_tags(state, p),
        "DescribeLoadBalancerAttributes" => op_describe_load_balancer_attributes(state, p),
        "ModifyLoadBalancerAttributes" => op_modify_load_balancer_attributes(state, p),
        "SetLoadBalancerListenerSSLCertificate" => {
            op_set_load_balancer_listener_ssl_certificate(state, p)
        }
        "CreateLoadBalancerPolicy" => op_create_load_balancer_policy(state, p),
        "CreateAppCookieStickinessPolicy" => op_create_app_cookie_stickiness_policy(state, p),
        "CreateLBCookieStickinessPolicy" => op_create_lb_cookie_stickiness_policy(state, p),
        "DescribeLoadBalancerPolicies" => op_describe_load_balancer_policies(state, p),
        "DescribeLoadBalancerPolicyTypes" => Ok(op_describe_load_balancer_policy_types(p)),
        "DeleteLoadBalancerPolicy" => op_delete_load_balancer_policy(state, p),
        "SetLoadBalancerPoliciesOfListener" => {
            op_set_load_balancer_policies_of_listener(state, p)
        }
        "SetLoadBalancerPoliciesForBackendServer" => {
            op_set_load_balancer_policies_for_backend_server(state, p)
        }
        "DescribeAccountLimits" => Ok(op_describe_account_limits()),
        "EnableAvailabilityZonesForLoadBalancer" => op_enable_availability_zones(state, p),
        "DisableAvailabilityZonesForLoadBalancer" => op_disable_availability_zones(state, p),
        "ApplySecurityGroupsToLoadBalancer" => op_apply_security_groups(req, state, p),
        "AttachLoadBalancerToSubnets" => op_attach_load_balancer_to_subnets(state, p),
        "DetachLoadBalancerFromSubnets" => op_detach_load_balancer_from_subnets(state, p),
        other => Err(ElbError::not_implemented(other)),
    }
}

fn result_xml(op: &str, body: &str) -> String {
    let result = if body.is_empty() {
        format!("<{op}Result/>")
    } else {
        format!("<{op}Result>{body}</{op}Result>")
    };
    format!(
        "<{op}Response xmlns=\"{XMLNS}\">{result}<ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata></{op}Response>",
        rid = uuid::Uuid::new_v4(),
    )
}

pub fn dispatch(req: &Request) -> Option<Response> {
    let raw = String::from_utf8_lossy(&req.body);
    if !raw.contains("Action=") && !req.query_string.contains("Action=") {
        return None;
    }
    let op = req.operation.trim().to_string();
    if op.is_empty() {
        return None;
    }
    let p = parse_params(req);
    let slots = store::slot::<ElbState>("elb");
    let mut map = slots.write();
    let key = (req.account, req.region.clone());
    let state = map.entry(key).or_default();
    Some(match run_op(req, state, &op, &p) {
        Ok(body) => Response::xml(200, result_xml(&op, &body).into_bytes()),
        Err(e) => e.response(XMLNS),
    })
}

// ---------------------------------------------------------------------------
// Unit tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn req_for(op: &str, body: &str) -> Request {
        let full = format!("Action={op}&Version=2012-06-01&{body}");
        Request {
            service: "elb".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: Bytes::from(full),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn listener() -> ElbClassicListener {
        ElbClassicListener {
            load_balancer_port: "80".into(),
            instance_port: "80".into(),
            protocol: "HTTP".into(),
            ssl_certificate_id: None,
            policy_names: vec![],
        }
    }

    fn lb_with_policies() -> ElbClassicLb {
        ElbClassicLb {
            name: "clb".into(),
            dns_name: "clb.us-east-1.elb.amazonaws.com".into(),
            health_check: None,
            instance_ids: vec!["i-abc".into()],
            availability_zones: vec!["us-east-1a".into()],
            listeners: vec![listener()],
            backends: vec![ElbBackend {
                instance_port: "80".into(),
                policy_names: vec!["sticky".into()],
            }],
            created_time: iso_micro(),
            scheme: Some("internet-facing".into()),
            attributes: ElbAttributes::default(),
            policies: vec![
                ElbPolicy::LbCookie {
                    policy_name: "sticky".into(),
                    cookie_expiration_period: Some(60),
                },
                ElbPolicy::AppCookie {
                    policy_name: "appc".into(),
                    cookie_name: "JSESSIONID".into(),
                },
                ElbPolicy::Other {
                    policy_name: "cust".into(),
                    policy_type_name: "SSLNegotiationPolicyType".into(),
                    attrs: vec![("Protocol-TLSv1.2".into(), "true".into())],
                },
            ],
            security_groups: vec![],
            subnets: vec![],
            vpc_id: None,
            tags: Vec::new(),
        }
    }

    #[test]
    fn create_lb_duplicate_and_validation() {
        let slots = store::slot::<ElbState>("elb");
        let mut map = slots.write();
        let st = map.entry((123456789001, "us-east-1".into())).or_default();
        let req = req_for(
            "CreateLoadBalancer",
            "LoadBalancerName=lb1&Listeners.member.1.Protocol=HTTP&Listeners.member.1.LoadBalancerPort=80&Listeners.member.1.InstanceProtocol=HTTP&Listeners.member.1.InstancePort=80&AvailabilityZones.member.1=us-east-1a",
        );
        let p = parse_params(&req);
        // default SG creation goes through loopback, which has no server in
        // unit tests; the moto error path is preserved by the same code path.
        let _ = op_create_load_balancer(&req, st, &p);
        // duplicate name check fires before anything else
        let err = op_create_load_balancer(&req, st, &p).unwrap_err();
        assert!(matches!(err, ElbError::Service(400, c, _) if c == "DuplicateLoadBalancerName"));
    }

    #[test]
    fn policies_transform_matches_moto() {
        let lb = lb_with_policies();
        let xml = policies_xml(&lb);
        assert!(xml.contains("<Policies><AppCookieStickinessPolicies><member><PolicyName>appc</PolicyName><CookieName>JSESSIONID</CookieName></member></AppCookieStickinessPolicies>"));
        assert!(xml.contains("<LBCookieStickinessPolicies><member><PolicyName>sticky</PolicyName><CookieExpirationPeriod>60</CookieExpirationPeriod></member></LBCookieStickinessPolicies>"));
        assert!(xml.contains("<OtherPolicies><member>cust</member></OtherPolicies>"));
    }

    #[test]
    fn policy_descriptions_filter_and_error() {
        let lb = lb_with_policies();
        let all = policies_descriptions_xml(&lb, &[]).unwrap();
        assert!(all.contains("<PolicyName>sticky</PolicyName><PolicyTypeName>LbCookieStickinessPolicy</PolicyTypeName>"));
        assert!(all.contains("<PolicyAttributeDescriptions><member><AttributeName>Protocol-TLSv1.2</AttributeName><AttributeValue>true</AttributeValue></member></PolicyAttributeDescriptions>"));
        let filtered = policies_descriptions_xml(&lb, &["sticky".to_string()]).unwrap();
        assert_eq!(
            filtered,
            "<PolicyDescriptions><member><PolicyName>sticky</PolicyName><PolicyTypeName>LbCookieStickinessPolicy</PolicyTypeName></member></PolicyDescriptions>"
        );
        let ghost = policies_descriptions_xml(&lb, &["ghost".to_string()]).unwrap_err();
        assert!(matches!(ghost, ElbError::Service(400, c, _) if c == "PolicyNotFound"));
    }

    #[test]
    fn policy_types_table_and_filter() {
        let all = policy_types_xml(&[]);
        assert!(all.contains("SSLNegotiationPolicyType"));
        assert!(all.contains("BackendServerAuthenticationPolicyType"));
        let filtered = policy_types_xml(&["ProxyProtocolPolicyType".to_string()]);
        assert!(filtered.starts_with("<PolicyTypeDescriptions><member><PolicyTypeName>ProxyProtocolPolicyType</PolicyTypeName>"));
        assert!(filtered.contains("<AttributeName>ProxyProtocol</AttributeName><AttributeType>Boolean</AttributeType><Cardinality>ONE</Cardinality>"));
        // moto drops unknown names silently
        let ghost = policy_types_xml(&["Nope".to_string()]);
        assert_eq!(ghost, "<PolicyTypeDescriptions/>");
    }

    #[test]
    fn attributes_xml_matches_moto_defaults() {
        let lb = lb_with_policies();
        let xml = attributes_xml(&lb);
        assert_eq!(
            xml,
            "<LoadBalancerAttributes><CrossZoneLoadBalancing><Enabled>false</Enabled></CrossZoneLoadBalancing><AccessLog><Enabled>false</Enabled></AccessLog><ConnectionDraining><Enabled>false</Enabled></ConnectionDraining><ConnectionSettings><IdleTimeout>60</IdleTimeout></ConnectionSettings><AdditionalAttributes/></LoadBalancerAttributes>"
        );
    }

    #[test]
    fn duplicate_tag_keys_tuple_message() {
        let slots = store::slot::<ElbState>("elb");
        let mut map = slots.write();
        let st = map.entry((123456789002, "us-east-1".into())).or_default();
        let mut lb = lb_with_policies();
        lb.name = "clb2".into();
        st.lbs.push(lb);
        let err = add_tags_checked(st.lbs.last_mut().unwrap(), &[("k".into(), "a".into()), ("k".into(), "b".into())])
            .unwrap_err();
        assert!(matches!(&err, ElbError::Service(400, c, _) if c == "DuplicateTagKeys"));
        // moto interpolates the (key, count) tuple
        let msg = match &err {
            ElbError::Service(_, _, m) => m.clone(),
            _ => String::new(),
        };
        assert_eq!(msg, "Tag key was specified more than once: ('k', 2)");
    }

    #[test]
    fn instance_health_unknown_state_for_unregistered() {
        let slots = store::slot::<ElbState>("elb");
        let mut map = slots.write();
        let st = map.entry((123456789003, "us-east-1".into())).or_default();
        let req = req_for("DescribeInstanceHealth", "LoadBalancerName=clb&Instances.member.1.InstanceId=i-ghost");
        let p = parse_params(&req);
        let mut lb = lb_with_policies();
        lb.instance_ids.push("i-ghost".into());
        st.lbs.push(lb);
        // ghost instance: moto skips unknown EC2 ids when registered renders empty
        let body = op_describe_instance_health(&req, st, &p).unwrap();
        assert_eq!(body, "<InstanceStates/>");
        // unregistered instance ids report Unknown
        let lb = st.lbs.last_mut().unwrap();
        lb.instance_ids.clear();
        let body = op_describe_instance_health(&req, st, &p).unwrap();
        assert!(body.contains("<State>Unknown</State>"));
        assert!(body.contains("<ReasonCode>N/A</ReasonCode>"));
    }

    #[test]
    fn modify_attributes_quirk_appends_desync() {
        let slots = store::slot::<ElbState>("elb");
        let mut map = slots.write();
        let st = map.entry((123456789004, "us-east-1".into())).or_default();
        let lb = lb_with_policies();
        st.lbs.push(lb);
        let req = req_for(
            "ModifyLoadBalancerAttributes",
            "LoadBalancerName=clb&LoadBalancerAttributes.CrossZoneLoadBalancing.Enabled=true",
        );
        let p = parse_params(&req);
        let body = op_modify_load_balancer_attributes(st, &p).unwrap();
        assert!(body.contains("<LoadBalancerName>clb</LoadBalancerName>"));
        assert!(body.contains("<CrossZoneLoadBalancing><Enabled>true</Enabled></CrossZoneLoadBalancing>"));
        assert!(body.contains("<AdditionalAttributes><member><Key>elb.http.desyncmitigationmode</Key><Value>defensive</Value></member></AdditionalAttributes>"));
    }

    #[test]
    fn account_limits_table() {
        let xml = op_describe_account_limits();
        assert!(xml.contains("<Name>classic-load-balancers</Name><Max>20</Max>"));
        assert!(xml.contains("<Name>classic-listeners</Name><Max>100</Max>"));
        assert!(xml.contains("<Name>classic-registered-instances</Name><Max>1000</Max>"));
    }

    #[test]
    fn health_check_target_validation() {
        let slots = store::slot::<ElbState>("elb");
        let mut map = slots.write();
        let st = map.entry((123456789005, "us-east-1".into())).or_default();
        st.lbs.push(lb_with_policies());
        let req = req_for(
            "ConfigureHealthCheck",
            "LoadBalancerName=clb&HealthCheck.Target=FTP:80/&HealthCheck.Interval=30",
        );
        let p = parse_params(&req);
        let err = op_configure_health_check(st, &p).unwrap_err();
        assert!(matches!(err, ElbError::Service(400, c, _) if c == "ValidationError"));
    }
}
