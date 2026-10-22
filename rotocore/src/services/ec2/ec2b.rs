#![allow(dead_code)]

//! XML field builders shared across the ec2 handler groups.

use super::{
    state::{LaunchTemplate, Snapshot, Volume},
    tags_xml,
};
use crate::proto::xml_escape;
use std::fmt::Write as _;

/// Fields inside a `<volume>` element (no wrapping `<item>`, no closing tag).
pub fn volume_fields(v: &Volume) -> String {
    let mut out = String::new();
    let _ = write!(out, "<volumeId>{}</volumeId>", xml_escape(&v.id));
    let _ = write!(out, "<size>{}</size>", v.size);
    let _ = write!(
        out,
        "<snapshotId>{}</snapshotId>",
        xml_escape(&v.snapshot_id)
    );
    let _ = write!(
        out,
        "<availabilityZone>{}</availabilityZone>",
        xml_escape(&v.az)
    );
    let _ = write!(out, "<state>{}</state>", xml_escape(&v.state));
    let _ = write!(
        out,
        "<createTime>{}</createTime>",
        xml_escape(&v.create_time)
    );
    if v.encrypted {
        out.push_str("<encrypted>true</encrypted>");
    }
    if let Some(i) = v.iops {
        let _ = write!(out, "<iops>{}</iops>", i);
    }
    if !v.vtype.is_empty() {
        let _ = write!(out, "<volumeType>{}</volumeType>", xml_escape(&v.vtype));
    }
    if v.multi_attach {
        out.push_str("<multiAttachEnabled>true</multiAttachEnabled>");
    }
    if let Some(t) = v.throughput {
        let _ = write!(out, "<throughput>{}</throughput>", t);
    }
    if let Some(k) = &v.kms_key_id {
        let _ = write!(out, "<kmsKeyId>{}</kmsKeyId>", xml_escape(k));
    }
    if let Some(a) = &v.attachment {
        out.push_str("<attachmentSet><item>");
        let _ = write!(out, "<volumeId>{}</volumeId>", xml_escape(&v.id));
        let _ = write!(
            out,
            "<instanceId>{}</instanceId>",
            xml_escape(&a.instance_id)
        );
        let _ = write!(out, "<device>{}</device>", xml_escape(&a.device));
        let _ = write!(out, "<status>{}</status>", xml_escape(&a.status));
        let _ = write!(
            out,
            "<attachTime>{}</attachTime>",
            xml_escape(&a.attach_time)
        );
        let _ = write!(
            out,
            "<deleteOnTermination>{}</deleteOnTermination>",
            a.delete_on_termination
        );
        out.push_str("</item></attachmentSet>");
    } else {
        out.push_str("<attachmentSet/>");
    }
    out.push_str(&tags_xml(&v.tags));
    out
}

/// Full `<volume>...</volume>` element.
pub fn volume_body(v: &Volume) -> String {
    format!("<volume>{}</volume>", volume_fields(v))
}

/// Fields inside a `<snapshot>` item.
pub fn snapshot_fields(s: &Snapshot) -> String {
    let mut out = String::new();
    let _ = write!(out, "<snapshotId>{}</snapshotId>", xml_escape(&s.id));
    let _ = write!(out, "<volumeId>{}</volumeId>", xml_escape(&s.volume_id));
    let _ = write!(out, "<status>{}</status>", xml_escape(&s.state));
    let _ = write!(out, "<startTime>{}</startTime>", xml_escape(&s.start_time));
    let _ = write!(out, "<progress>{}%</progress>", xml_escape(&s.progress));
    let _ = write!(out, "<ownerId>{}</ownerId>", s.owner_id);
    let _ = write!(
        out,
        "<description>{}</description>",
        xml_escape(&s.description)
    );
    let _ = write!(out, "<volumeSize>{}</volumeSize>", s.volume_size);
    if let Some(a) = &s.owner_alias {
        let _ = write!(out, "<ownerAlias>{}</ownerAlias>", xml_escape(a));
    }
    if s.encrypted {
        out.push_str("<encrypted>true</encrypted>");
    }
    if let Some(k) = &s.kms_key_id {
        let _ = write!(out, "<kmsKeyId>{}</kmsKeyId>", xml_escape(k));
    }
    if s.storage_tier.is_empty() {
        out.push_str("<storageTier>standard</storageTier>");
    } else {
        let _ = write!(
            out,
            "<storageTier>{}</storageTier>",
            xml_escape(&s.storage_tier)
        );
    }
    out.push_str(&tags_xml(&s.tags));
    out
}

/// The `<launchTemplate>` inner label (id + name).
pub fn template_label(lt: &LaunchTemplate) -> String {
    format!(
        "<launchTemplateId>{}</launchTemplateId><latestVersionNumber>{}</latestVersionNumber><launchTemplateName>{}</launchTemplateName><defaultVersionNumber>{}</defaultVersionNumber><createdBy>arn:aws:sts::123456789012:root</createdBy><createTime>{}</createTime>",
        xml_escape(&lt.id),
        lt.latest_version,
        xml_escape(&lt.name),
        lt.default_version,
        xml_escape(&lt.created)
    )
}
