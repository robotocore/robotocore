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
    SHAPES
        .get("ops")
        .and_then(|o| o.get(op))
}

/// Render a member node from the shape tree.
fn render_node(
    shape: &Value,
    pykey: &str,
    fills: &Fills,
    wire: &str,
) -> String {
    match shape.get("t").and_then(|t| t.as_str()).unwrap_or("") {
        "string" | "long" | "integer" | "boolean" | "timestamp" | "double" => {
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
    // inner recorded as bare member list; wrap once in <item> if not wrapped
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
    let _ = elem;
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
                let pykey = member
                    .get("k")
                    .and_then(|k| k.as_str())
                    .unwrap_or_default();
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
    Some(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{xml}"
    ))
}
