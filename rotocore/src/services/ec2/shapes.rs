//! Shape-driven EC2 response rendering.
//!
//! `shapes.json` carries botocore's own output shapes for the ops this
//! backend serves from generic families. Rendering a response by walking
//! the shape guarantees botocore keys (`Ipams`, `SpotFleetRequestId`, ...)
//! match the wire exactly — no hand-rolled root layouts.

use crate::proto::xml_escape;
use serde_json::Value;
use std::collections::HashMap;

/// A fill provided by the family handler for one python member key.
#[derive(Clone)]
pub enum Fill {
    /// Scalar value — rendered as `<wire>escaped</wire>`.
    S(String),
    /// Pre-rendered inner XML placed inside the member's wrapper.
    X(String),
}

pub type Fills = HashMap<String, Fill>;

static SHAPES: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
    serde_json::from_str(include_str!("shapes.json")).unwrap_or_else(|_| serde_json::json!({}))
});

pub fn shape_of(op: &str) -> Option<&'static Value> {
    SHAPES.get("ops").and_then(|o| o.get(op))
}

/// Render a member node from the shape tree.
fn render_node(shape: &Value, pykey: &str, fills: &Fills, wire: &str) -> String {
    match shape.get("t").and_then(|t| t.as_str()).unwrap_or("") {
        "string" | "long" | "integer" | "boolean" | "timestamp" | "double" | "" => {
            match fills.get(pykey) {
                Some(Fill::S(v)) => format!("<{wire}>{}</{wire}>", xml_escape(v)),
                Some(Fill::X(inner)) => format!("<{wire}>{inner}</{wire}>"),
                None => String::new(),
            }
        }
        "structure" => {
            let inner = render_members(shape, fills);
            if inner.is_empty() && !fills.contains_key(pykey) {
                return String::new();
            }
            let extra = match fills.get(pykey) {
                Some(Fill::X(x)) if inner.is_empty() => x.clone(),
                _ => String::new(),
            };
            format!("<{wire}>{inner}{extra}</{wire}>")
        }
        "list" => {
            // items pre-rendered: Fill::X holds the <item>...</item> concat
            let items = match fills.get(pykey) {
                Some(Fill::X(x)) => x.clone(),
                _ => String::new(),
            };
            let elem = shape
                .get("i")
                .map(|i| {
                    i.get("n")
                        .and_then(|n| n.as_str())
                        .unwrap_or("item")
                        .to_string()
                })
                .unwrap_or_else(|| "item".to_string());
            if items.is_empty() {
                format!("<{wire}/>")
            } else {
                format!("<{wire}>{}</{wire}>", wrap_items(items, &elem))
            }
        }
        _ => String::new(),
    }
}

fn wrap_items(inner: String, elem: &str) -> String {
    // content already produced as full `<item>` blocks (e.g. stored item_xml
    // with embedded tagSets) must pass through untouched — re-splitting at
    // every `</item>` would mangle nested list elements.
    let _ = elem;
    let trimmed = inner.trim();
    if trimmed.starts_with("<item>") && trimmed.ends_with("</item>") {
        return inner;
    }
    let mut out = String::new();
    for chunk in inner.split("</item>") {
        let trimmed = chunk.trim();
        if trimmed.is_empty() {
            continue;
        }
        let body = format!("{trimmed}</item>");
        if body.starts_with("<item>") {
            out.push_str(&body);
        } else {
            out.push_str(&format!("<item>{body}"));
        }
    }
    out
}

/// Render every member of a structure node, using its members' wire names
/// and matching fills by the python key recorded in the shape.
fn render_members(shape: &Value, fills: &Fills) -> String {
    let mut out = String::new();
    if let Some(members) = shape.get("m").and_then(|m| m.as_object()) {
        for (_wire, member) in members {
            let pykey = member
                .get("k")
                .and_then(|k| k.as_str())
                .unwrap_or_default()
                .to_string();
            let wiren = member
                .get("n")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string();
            if wiren.is_empty() {
                continue;
            }
            let rendered = if member.get("f").is_some() {
                // flattened: items appear without a set wrapper
                match fills.get(&pykey) {
                    Some(Fill::X(x)) => x.clone(),
                    _ => String::new(),
                }
            } else {
                render_node(member, &pykey, fills, &wiren)
            };
            out.push_str(&rendered);
        }
    }
    out
}

/// Full XML document for an op: `<XResponse xmlns=..>{members}{meta}</XResponse>`
pub fn render_response(op: &str, fills: &Fills, region: &str) -> Option<String> {
    let shape = shape_of(op)?;
    let mut body = render_members(shape, fills);
    if body.is_empty() {
        // still emit a well-formed (empty) result so keys like
        // `<xxxSet/>` are present when the shape has a list member
        if let Some(members) = shape.get("m").and_then(|m| m.as_object()) {
            for (_wire, member) in members {
                let pykey = member.get("k").and_then(|k| k.as_str()).unwrap_or_default();
                if let Some(Fill::X(x)) = fills.get(pykey) {
                    body.push_str(x);
                }
            }
        }
    }
    let _ = region;
    let xml = format!(
        r#"<{root}Response xmlns="{ns}">{body}<requestId>{rid}</requestId></{root}Response>"#,
        root = op,
        ns = "http://ec2.amazonaws.com/doc/2016-11-15/",
        rid = uuid::Uuid::new_v4()
    );
    Some(format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{xml}"))
}

/// Render an op whose output root is a single list member: each item is
/// rendered from its element (structure) node with the given fills, then
/// wrapped `<item>..</item>` inside the set's wire wrapper.
pub fn render_list_response(
    op: &str,
    list_pykey: &str,
    items: &[Fills],
    _region: &str,
) -> Option<String> {
    let shape = shape_of(op)?;
    let members = shape.get("m")?.as_object()?;
    let mut wire = String::new();
    let mut elem_shape: Option<&Value> = None;
    for (w, member) in members {
        if member
            .get("k")
            .and_then(|k| k.as_str())
            .map(|k| k == list_pykey)
            .unwrap_or(false)
        {
            wire = w.clone();
            if member.get("t").and_then(|t| t.as_str()) == Some("list") {
                elem_shape = member.get("i");
            }
            break;
        }
    }
    if wire.is_empty() || elem_shape.is_none() {
        return None;
    }
    let elem = elem_shape.unwrap();
    let elem_wire = elem
        .get("n")
        .and_then(|n| n.as_str())
        .unwrap_or("item")
        .to_string();
    let mut inner = String::new();
    for item_fills in items {
        let body = render_members(elem, item_fills);
        if elem_wire == "item" {
            inner.push_str(&format!("<item>{body}</item>"));
        } else {
            inner.push_str(&body);
        }
    }
    let flattened = members.iter().any(|(_w, m)| {
        m.get("k")
            .and_then(|k| k.as_str())
            .map(|k| k == list_pykey)
            .unwrap_or(false)
            && m.get("f").is_some()
    });
    let body_out = if flattened {
        inner
    } else {
        format!("<{wire}>{inner}</{wire}>")
    };
    Some(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root}Response xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">{body_out}<requestId>{rid}</requestId></{root}Response>",
        root = op,
        rid = uuid::Uuid::new_v4()
    ))
}

// ---------------------------------------------------------------------------
// Generic shape-driven synthesis for gap arms (the terminator for the per-op
// 501 catch-all): ops without their own family generator render exactly the
// member set botocore's model declares, values synthesized from the shapes.
// ---------------------------------------------------------------------------

use super::Params;
use crate::Response;

pub struct SynthCtx<'a> {
    pub params: &'a Params,
    pub account: u64,
    pub region: &'a str,
    /// Tags from the request's TagSpecifications (`<tagSet>` members).
    pub tags: Vec<(String, String)>,
}

/// `[(wire, node)]` pairs of a structure node's members.
fn members_of(node: &Value) -> Vec<(String, Value)> {
    node.get("m")
        .and_then(|m| m.as_object())
        .map(|m| m.iter().map(|(w, v)| (w.clone(), v.clone())).collect())
        .unwrap_or_default()
}

fn enum_of(node: &Value) -> Vec<String> {
    node.get("e")
        .and_then(|e| e.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// `<item><key>..</key><value>..</value></item>` rows for a Tags member.
fn synth_tag_items(tags: &[(String, String)]) -> String {
    let mut out = String::new();
    for (k, v) in tags {
        out.push_str(&format!(
            "<item><key>{k}</key><value>{v}</value></item>",
            k = xml_escape(k),
            v = xml_escape(v)
        ));
    }
    out
}

/// Known EC2 id prefixes by python member name (synthetic ids for members
/// whose ids the request did not echo).
fn synth_id(pykey: &str) -> String {
    let p = |prefix: &str| format!("{prefix}-{}", super::state::idhex::hex17());
    for (kw, prefix) in [
        ("InstanceId", "i"),
        ("ImageId", "ami"),
        ("SnapshotId", "snap"),
        ("VolumeId", "vol"),
        ("VpcId", "vpc"),
        ("SubnetId", "subnet"),
        ("SecurityGroupId", "sg"),
        ("RouteTableId", "rtb"),
        ("InternetGatewayId", "igw"),
        ("NatGatewayId", "nat"),
        ("NetworkInterfaceId", "eni"),
        ("AllocationId", "eipalloc"),
        ("VpcEndpointId", "vpce"),
        ("VpcEndpointServiceId", "vpce-svc"),
        ("TransitGatewayId", "tgw"),
        ("TransitGatewayRouteTableId", "tgw-rtb"),
        ("TransitGatewayAttachmentId", "tgw-attach"),
        ("TransitGatewayMulticastDomainId", "tgw-mcast-domain"),
        ("TransitGatewayPolicyTableId", "tgw-policy-table"),
        ("TransitGatewayMeteringPolicyId", "tgw-mp"),
        ("RouteServerId", "route-server"),
        ("RouteServerEndpointId", "route-server-endpoint"),
        ("RouteServerPeerId", "route-server-peer"),
        ("VpnConnectionId", "vpn"),
        ("VpnGatewayId", "vgw"),
        ("VpnConcentratorId", "vpn-concentrator"),
        ("CustomerGatewayId", "cgw"),
        ("KeyPairId", "key"),
        ("CapacityReservationId", "cr"),
        ("CapacityReservationFleetId", "crf"),
        ("IpamId", "ipam"),
        ("IpamScopeId", "ipam-scope"),
        ("IpamPoolId", "ipam-pool"),
        ("IpamResourceDiscoveryId", "ipam-res-disc"),
        ("IpamPrefixListResolverId", "ipam-plr"),
        ("IpamPrefixListResolverTargetId", "ipam-plrt"),
        ("IpamPolicyId", "ipam-policy"),
        ("ClientVpnEndpointId", "cvpn-endpoint"),
        ("FpgaImageId", "afi"),
        ("FpgaImageGlobalId", "afi-ig"),
        ("LaunchTemplateId", "lt"),
        ("SpotFleetRequestId", "sfr"),
        ("SpotInstanceRequestId", "sir"),
        ("FleetId", "fleet"),
        ("ReservedInstancesId", "r"),
        ("ConversionTaskId", "import"),
        ("ExportTaskId", "export-i"),
        ("SecondaryNetworkId", "sernet"),
        ("SecondarySubnetId", "sersub"),
        ("CapacityManagerDataExportId", "cmde"),
        ("DeclarativePoliciesReportId", "dp-report"),
    ] {
        if pykey == kw {
            return p(prefix);
        }
    }
    if pykey.ends_with("Id") {
        let stem = pykey
            .trim_end_matches("Id")
            .to_lowercase()
            .trim_matches('_')
            .to_lowercase();
        return p(&stem[..stem.len().min(20)]);
    }
    String::new()
}

/// Lifecycle tier for state-ish enum members; "" for non-enum strings.
fn synth_state_str(_k: &str, enumv: &[String]) -> String {
    for pref in [
        "available",
        "active",
        "succeeded",
        "associated",
        "compliant",
        "provisioned",
        "enabled",
        "pending",
    ] {
        if enumv.iter().any(|v| v == pref) {
            return pref.to_string();
        }
    }
    enumv.first().cloned().unwrap_or_default()
}

/// One scalar member value, python key `k`, botocore type `t`.
fn synth_scalar(k: &str, t: &str, enumv: &[String], ctx: &SynthCtx) -> String {
    let echo = ctx.params.get(k).map(|v| v.to_string());
    match t {
        "boolean" => {
            if k == "Return" || k == "Success" || k == "Result" {
                return "true".into();
            }
            if let Some(v) = echo {
                if v == "true" || v == "false" {
                    return v;
                }
            }
            // butterfly booleans read true where the model names them so
            if k.contains("Found")
                || k.contains("Enabled")
                || k.contains("Supported")
                || k.contains("Publicly")
            {
                return "true".into();
            }
            "false".into()
        }
        "integer" | "long" | "double" => {
            if let Some(v) = echo {
                if !v.is_empty() && v.parse::<i64>().is_ok() {
                    return v;
                }
            }
            if k.contains("Depth") || k.contains("Cost") || k.contains("Charge") {
                return "0".into();
            }
            if k.contains("Price") {
                return "0.0".into();
            }
            "1".into()
        }
        _ => {
            if t == "timestamp" {
                return super::now_iso();
            }
            if !enumv.is_empty() {
                return synth_state_str(k, enumv);
            }
            if let Some(v) = echo {
                if !v.is_empty() {
                    return v;
                }
            }
            if k == "OwnerId" || k.ends_with("OwnerId") || k == "AccountId" {
                return ctx.account.to_string();
            }
            if k == "State" || k.ends_with("State") {
                return "available".into();
            }
            if k.ends_with("Time")
                || k.ends_with("Date")
                || k.ends_with("Timestamp")
                || k == "CreationTime"
            {
                return super::now_iso();
            }
            if k.ends_with("Id") {
                return synth_id(k);
            }
            if k.contains("Message") {
                return String::new();
            }
            String::new()
        }
    }
}

/// One `<item>` inner for a list member sampled as its element node.
fn synth_list(elem: &Value, k: &str, ctx: &SynthCtx, overrides: &Fills) -> String {
    if let Some(Fill::X(x)) = overrides.get(k) {
        return x.clone();
    }
    let et = elem
        .get("t")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    if et == "structure" {
        if k == "Tags" {
            return synth_tag_items(&ctx.tags);
        }
        // scalar element members echo from the request where filled
        let members = members_of(elem);
        let body = synth_children(&members, ctx, overrides);
        if body.is_empty() {
            return String::new();
        }
        return format!("<item>{body}</item>");
    }
    // scalar element list: echo the request's same-member flat list
    let wname = elem
        .get("n")
        .and_then(|n| n.as_str())
        .unwrap_or("item")
        .to_string();
    let mut out = String::new();
    for v in ctx.params.list(k) {
        out.push_str(&format!("<{wname}>{}</{wname}>", xml_escape(v)));
    }
    out
}

/// Inner XML for a structure's members (no wrapper).
fn synth_children(members: &[(String, Value)], ctx: &SynthCtx, overrides: &Fills) -> String {
    let mut out = String::new();
    for (wire, node) in members {
        let k = node
            .get("k")
            .and_then(|k| k.as_str())
            .unwrap_or("")
            .to_string();
        let t = node
            .get("t")
            .and_then(|t| t.as_str())
            .unwrap_or("string")
            .to_string();
        let flat = node.get("f").is_some();
        if let Some(fill) = overrides.get(&k) {
            match fill {
                Fill::X(x) => out.push_str(x),
                Fill::S(s) => out.push_str(&format!("<{wire}>{}</{wire}>", xml_escape(s))),
            }
            continue;
        }
        match t.as_str() {
            "structure" => {
                let inner = members_of(node);
                if inner.is_empty() {
                    continue;
                }
                let body = synth_children(&inner, ctx, overrides);
                out.push_str(&format!("<{wire}>{body}</{wire}>"));
            }
            "list" => {
                let empty = serde_json::Value::Null;
                let elem = node.get("i").unwrap_or(&empty);
                let body = synth_list(elem, &k, ctx, overrides);
                if flat {
                    out.push_str(&body);
                } else {
                    out.push_str(&format!("<{wire}>{body}</{wire}>"));
                }
            }
            _ => {
                let enumv = enum_of(node);
                let v = synth_scalar(&k, &t, &enumv, ctx);
                out.push_str(&format!("<{wire}>{}</{wire}>", xml_escape(&v)));
            }
        }
    }
    out
}

/// `OpResponse` document from the op's botocore output shape; `overrides`
/// win per python key. None when the model has no shape for the op.
pub fn synth_response(op: &str, ctx: &SynthCtx, overrides: &Fills) -> Option<Response> {
    let shape = shape_of(op)?;
    let members = members_of(shape);
    let body = synth_children(&members, ctx, overrides);
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root}Response xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">{body}<requestId>{rid}</requestId></{root}Response>",
        root = op,
        rid = uuid::Uuid::new_v4()
    );
    Some(Response::xml(200, xml.into_bytes()))
}
