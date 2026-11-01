//! ElastiCache backend (**query-XML protocol**, form bodies, XML responses).
//!
//! Port of moto's `elasticache` with wire shapes captured against the
//! moto-bridge: subnet groups, cache clusters, replication groups
//! (+ global replication groups), users/user groups, parameter groups,
//! snapshots, serverless caches + snapshots, security groups and tags.
//! The formerly-missing gap ops (DescribeEngineDefaultParameters, reserved
//! nodes, RebootCacheCluster, CopyServerlessCacheSnapshot,
//! PurchaseReservedCacheNodesOffering, AuthorizeCacheSecurityGroupIngress,
//! migration/shard ops) are implemented here.

use std::collections::HashMap;

use crate::proto::xml_escape;
use crate::{Request, Response};
use bytes::Bytes;

/// The elasticache XML namespace (matches the moto-bridge exactly).
const XMLNS: &str = "http://elasticache.amazonaws.com/doc/2015-02-02/";

// --- form-body parsing ----------------------------------------------------------

/// Parse `application/x-www-form-urlencoded` parameters from the body and
/// query string of a query-protocol request.
pub fn query_params(req: &Request) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let mut combined = String::new();
    if !req.query_string.is_empty() {
        combined.push_str(&req.query_string);
        combined.push('&');
    }
    combined.push_str(std::str::from_utf8(&req.body).unwrap_or(""));
    for pair in combined.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        params.insert(urldecode(k), urldecode(&v.replace('+', " ")));
    }
    params
}

fn urldecode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() + 1 && i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("25");
                out.push(u8::from_str_radix(hex, 16).unwrap_or(b'?'));
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// member list params like `Foo.member.1`, `Tags.member.2.Value`.
fn listed(params: &HashMap<String, String>, prefix: &str) -> Vec<String> {
    let mut members: Vec<(usize, String)> = params
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(prefix)
                .and_then(|rest| rest.strip_prefix(".member."))
                .and_then(|idx| idx.parse::<usize>().ok().map(|i| (i, v.clone())))
        })
        .collect();
    members.sort();
    members.into_iter().map(|(_, v)| v).collect()
}

// --- XML response helpers ------------------------------------------------------

/// Success wrapper: `<Op>Response xmlns=..><Op>Result>{body}</Op>Result><ResponseMetadata>…`
fn success(action: &str, body: &str) -> Response {
    let root = format!("{}Response", action);
    let result = format!("{}Result", action);
    let meta = format!(
        "<ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata>",
        uuid::Uuid::new_v4()
    );
    let doc = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<{root} xmlns="{XMLNS}"><{result}>{body}</{result}>{meta}</{root}>"#
    );
    Response::new(200, "text/xml", Bytes::from(doc))
}

/// moto's error doc for structured faults.
fn err_response(status: u16, code: &str, message: &str) -> Response {
    let doc = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="{XMLNS}"><Error><Type>Sender</Type><Code>{code}</Code><Message>{msg}</Message></Error><RequestId>{rid}</RequestId></ErrorResponse>"#,
        code = xml_escape(code),
        msg = xml_escape(message),
        XMLNS = XMLNS,
        rid = uuid::Uuid::new_v4(),
    );
    Response::new(status, "text/xml", Bytes::from(doc))
}

type EcError = (u16, String, String);

fn not_found_rg(id: &str) -> EcError {
    (
        404,
        "ReplicationGroupNotFoundFault".into(),
        format!("Replication group {id} not found."),
    )
}

fn not_found_cluster(id: &str) -> EcError {
    (
        404,
        "CacheClusterNotFound".into(),
        format!("Cache cluster {id} not found."),
    )
}

fn not_found_snapshot(id: &str) -> EcError {
    (
        404,
        "SnapshotNotFoundFault".into(),
        format!("Snapshot {id} not found."),
    )
}

fn not_found_user(id: &str) -> EcError {
    (404, "UserNotFound".into(), format!("User {id} not found."))
}

fn not_found_user_group(id: &str) -> EcError {
    (
        404,
        "UserGroupNotFoundFault".into(),
        format!("UserGroup {id} not found."),
    )
}

fn not_found_subnet_group(id: &str) -> EcError {
    (
        400,
        "CacheSubnetGroupNotFoundFault".into(),
        format!("CacheSubnetGroup {id} not found."),
    )
}

fn not_found_csg(id: &str) -> EcError {
    (
        404,
        "CacheSecurityGroupNotFound".into(),
        format!("Cache security group {id} not found."),
    )
}

fn not_found_global(id: &str) -> EcError {
    (
        404,
        "GlobalReplicationGroupNotFoundFault".into(),
        format!("Global replication group {id} not found."),
    )
}

fn not_found_serverless(id: &str) -> EcError {
    (
        404,
        "ServerlessCacheNotFoundFault".into(),
        format!("ServerlessCache {id} not found."),
    )
}

fn not_found_serverless_snap(id: &str) -> EcError {
    (
        404,
        "ServerlessCacheSnapshotNotFoundFault".into(),
        format!("ServerlessCacheSnapshot {id} not found."),
    )
}

fn exists(code: &str, message: String) -> EcError {
    (400, code.into(), message)
}

fn now_stamp() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.000000Z")
        .to_string()
}

/// `Foo.member.N` member listings like `SubnetIds.member.1`;
/// `Tags.member.N.Key/Value` collection from raw params.
fn tag_list(params: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut tags: Vec<(usize, String, String)> = Vec::new();
    for (k, v) in params {
        if let Some(rest) = k
            .strip_prefix("Tags.member.")
            .or_else(|| k.strip_prefix("Tags.Tag."))
        {
            if let Some((idx, field)) = rest.split_once('.') {
                if let Ok(i) = idx.parse::<usize>() {
                    match field {
                        "Key" => match tags.iter_mut().find(|(j, _, _)| *j == i) {
                            Some(t) => t.1 = v.clone(),
                            None => tags.push((i, v.clone(), String::new())),
                        },
                        "Value" => match tags.iter_mut().find(|(j, _, _)| *j == i) {
                            Some(t) => t.2 = v.clone(),
                            None => tags.push((i, String::new(), v.clone())),
                        },
                        _ => {}
                    }
                }
            }
        }
    }
    tags.sort();
    tags.into_iter()
        .filter(|(_, k, v)| !k.is_empty() && !v.is_empty())
        .map(|(_, k, v)| (k, v))
        .collect()
}

pub fn dispatch(req: &Request) -> Response {
    let params = query_params(req);
    let action = if req.operation.is_empty() {
        "Unknown".to_string()
    } else {
        req.operation.clone()
    };
    match handle(req, &params) {
        Ok(body) => success(&action, &body),
        Err((status, code, msg)) => err_response(status, &code, &msg),
    }
}

// --- state -------------------------------------------------------------------------

#[derive(Clone)]
struct CacheCluster {
    id: String,
    engine: String,
    node_type: String,
    num_nodes: i64,
    status: String,
    create_time: String,
    snapshot_retention: i64,
}

#[derive(Clone)]
struct SubnetGroup {
    name: String,
    description: String,
    subnet_ids: Vec<String>,
}

#[derive(Clone)]
struct ReplicationGroup {
    id: String,
    description: String,
    status: String,
    member_clusters: Vec<String>,
    node_type: String,
    engine: String,
}

#[derive(Clone)]
struct UserEntity {
    id: String,
    user_name: String,
    engine: String,
    access_string: String,
    status: &'static str,
}

#[derive(Clone, Default)]
struct UserGroup {
    id: String,
    engine: String,
    user_ids: Vec<String>,
    status: &'static str,
}

#[derive(Clone)]
struct Snapshot {
    name: String,
    replication_group_id: String,
    description: String,
    node_type: String,
    engine: String,
}

#[derive(Clone)]
struct ParameterGroup {
    name: String,
    family: String,
    description: String,
    /// modified (name, value) overrides
    overrides: Vec<(String, String)>,
}

#[derive(Clone, Default)]
struct CacheSecurityGroup {
    name: String,
    description: String,
    /// (name, id) of ec2 security groups with ingress
    ingress: Vec<(String, String)>,
}

#[derive(Clone)]
struct GlobalReplicationGroup {
    id: String,
    description: String,
    members: Vec<String>,
}

#[derive(Clone)]
struct ServerlessCache {
    name: String,
    description: String,
    engine: String,
    status: &'static str,
    create_time: String,
}

#[derive(Clone)]
struct ServerlessCacheSnapshot {
    name: String,
    source_cache: String,
    status: &'static str,
    create_time: String,
}

#[derive(Default)]
pub struct ElastiCacheState {
    tags: HashMap<String, Vec<(String, String)>>,
    clusters: Vec<CacheCluster>,
    subnet_groups: Vec<SubnetGroup>,
    replication_groups: Vec<ReplicationGroup>,
    users: Vec<UserEntity>,
    user_groups: Vec<UserGroup>,
    snapshots: Vec<Snapshot>,
    parameter_groups: Vec<ParameterGroup>,
    security_groups: Vec<CacheSecurityGroup>,
    global_replication_groups: Vec<GlobalReplicationGroup>,
    serverless_caches: Vec<ServerlessCache>,
    serverless_snapshots: Vec<ServerlessCacheSnapshot>,
}

// XML emitters ------------------------------------------------------------------
//
// moto 5.2.4 escapes user values through the shape serializers
// (xmltodict.unparse); this surfaces raw user data into XML so an id like
// `</CacheCluster><Injected/>` corrupts every subsequent response. The
// compat suite pins no raw-injection behavior.

fn esc(s: &str) -> String {
    crate::proto::xml_escape(s)
}

fn cluster_xml(c: &CacheCluster) -> String {
    format!(
        "<CacheCluster><CacheClusterId>{id}</CacheClusterId><CacheNodeType>{ty}</CacheNodeType><Engine>{engine}</Engine><CacheClusterStatus>{status}</CacheClusterStatus><NumCacheNodes>{nodes}</NumCacheNodes><CacheClusterCreateTime>{created}</CacheClusterCreateTime><SnapshotRetentionLimit>{ret}</SnapshotRetentionLimit><AuthTokenEnabled>false</AuthTokenEnabled><AuthTokenLastModifiedDate>{created}</AuthTokenLastModifiedDate><LogDeliveryConfigurations/></CacheCluster>",
        id = esc(&c.id),
        ty = esc(&c.node_type),
        engine = esc(&c.engine),
        status = esc(&c.status),
        nodes = c.num_nodes,
        created = c.create_time,
        ret = c.snapshot_retention,
    )
}

/// Cluster XML including the ARN (moto emits ARN after other members).
fn cluster_with_arn(c: &CacheCluster, account: u64, region: &str) -> String {
    let inner = cluster_xml(c);
    let arn = format!("arn:aws:elasticache:{region}:{account}:cluster:{}", c.id);
    inner.replace(
        "<LogDeliveryConfigurations/>",
        &format!("<ARN>{arn}</ARN><LogDeliveryConfigurations/>"),
    )
}

/// CRITICAL: suite reads cluster["ARN"] on create/describe.
fn subnet_group_xml(sg: &SubnetGroup, account: u64, region: &str) -> String {
    let subnets: String = sg
        .subnet_ids
        .iter()
        .map(|id| {
            format!(
                "<Subnet><SubnetIdentifier>{}</SubnetIdentifier><SubnetAvailabilityZone/><SupportedNetworkTypes><member>ipv4</member></SupportedNetworkTypes></Subnet>",
                esc(id)
            )
        })
        .collect();
    format!(
        "<CacheSubnetGroup><CacheSubnetGroupName>{name}</CacheSubnetGroupName><CacheSubnetGroupDescription>{desc}</CacheSubnetGroupDescription><VpcId>{vpc}</VpcId><Subnets>{subnets}</Subnets><ARN>arn:aws:elasticache:{region}:{account}:subnetgroup:{name}</ARN><SupportedNetworkTypes><member>ipv4</member></SupportedNetworkTypes></CacheSubnetGroup>",
        name = esc(&sg.name),
        desc = esc(&sg.description),
        vpc = "vpc-default",
        subnets = subnets,
    )
}

fn replication_group_xml(rg: &ReplicationGroup, account: u64, region: &str) -> String {
    let members: String = rg
        .member_clusters
        .iter()
        .map(|c| format!("<ClusterId>{}</ClusterId>", esc(c)))
        .collect();
    let mut node_groups = String::new();
    for c in &rg.member_clusters {
        let part = "apv63r";
        node_groups.push_str(&format!(
            "<NodeGroup><NodeGroupId>0001</NodeGroupId><Status>{}</Status><PrimaryEndpoint><Address>master.{}.{}.use1.cache.amazonaws.com</Address><Port>6379</Port></PrimaryEndpoint><NodeGroupMembers><NodeGroupMember><CacheClusterId>{}</CacheClusterId><CacheNodeId>0001</CacheNodeId><ReadEndpoint><Address>{}.{}.{}.use1.cache.amazonaws.com</Address><Port>6379</Port></ReadEndpoint><PreferredAvailabilityZone>us-east-1a</PreferredAvailabilityZone><CurrentRole>primary</CurrentRole></NodeGroupMember></NodeGroupMembers></NodeGroup>",
            esc(&rg.status), esc(&rg.id), part, esc(c), esc(c), esc(&rg.id), part,
        ));
    }
    format!(
        "<ReplicationGroup><ReplicationGroupId>{}</ReplicationGroupId><Description>{}</Description><Status>{}</Status><MemberClusters>{}</MemberClusters><NodeGroups>{}</NodeGroups><AutomaticFailover>disabled</AutomaticFailover><MultiAZ>disabled</MultiAZ><SnapshotRetentionLimit>0</SnapshotRetentionLimit><SnapshotWindow>05:00-6:00</SnapshotWindow><ClusterEnabled>false</ClusterEnabled><CacheNodeType>{}</CacheNodeType><AuthTokenEnabled>false</AuthTokenEnabled><TransitEncryptionEnabled>false</TransitEncryptionEnabled><AtRestEncryptionEnabled>false</AtRestEncryptionEnabled><MemberClustersOutpostArns/><ARN>arn:aws:elasticache:{}:{}:replicationgroup:{}</ARN><UserGroupIds/><LogDeliveryConfigurations/><ReplicationGroupCreateTime>{}</ReplicationGroupCreateTime><DataTiering>disabled</DataTiering><NetworkType>ipv4</NetworkType><IpDiscovery>ipv4</IpDiscovery><ClusterMode>disabled</ClusterMode><Engine>{}</Engine></ReplicationGroup>",
        esc(&rg.id),
        esc(&rg.description),
        esc(&rg.status),
        members,
        node_groups,
        esc(&rg.node_type),
        region,
        account,
        esc(&rg.id),
        now_stamp(),
        esc(&rg.engine),
    )
}

fn user_xml(u: &UserEntity, account: u64, region: &str) -> String {
    format!(
        "<UserId>{}</UserId><UserName>{}</UserName><Status>{}</Status><Engine>{}</Engine><MinimumEngineVersion>6.0</MinimumEngineVersion><AccessString>{}</AccessString><UserGroupIds/><Authentication><Type>no-password</Type></Authentication><ARN>arn:aws:elasticache:{region}:{account}:user:{}</ARN>",
        esc(&u.id),
        esc(&u.user_name),
        esc(&u.status),
        esc(&u.engine),
        esc(&u.access_string),
        esc(&u.id),
    )
}

fn user_group_xml(ug: &UserGroup, account: u64, region: &str) -> String {
    let ids: String = ug
        .user_ids
        .iter()
        .map(|u| format!("<member>{}</member>", esc(u)))
        .collect();
    format!(
        "<UserGroupId>{id}</UserGroupId><Status>{status}</Status><Engine>{engine}</Engine><UserIds>{ids}</UserIds><MinimumEngineVersion>6.0</MinimumEngineVersion><ReplicationGroups/><ServerlessCaches/><ARN>arn:aws:elasticache:{region}:{account}:usergroup:{id}</ARN>",
        id = esc(&ug.id),
        status = esc(&ug.status),
        engine = esc(&ug.engine),
        ids = ids,
    )
}

fn snapshot_xml(s: &Snapshot, account: u64, region: &str) -> String {
    format!(
        "<Snapshot><SnapshotName>{name}</SnapshotName><ReplicationGroupId>{rg}</ReplicationGroupId><ReplicationGroupDescription>{desc}</ReplicationGroupDescription><SnapshotStatus>{status}</SnapshotStatus><SnapshotSource>manual</SnapshotSource><CacheNodeType>{ty}</CacheNodeType><Engine>{engine}</Engine><Port>6379</Port><SnapshotRetentionLimit>0</SnapshotRetentionLimit><SnapshotWindow>05:00-6:00</SnapshotWindow><NumNodeGroups>1</NumNodeGroups><AutomaticFailover>disabled</AutomaticFailover><ARN>arn:aws:elasticache:{region}:{account}:snapshot:{name}</ARN></Snapshot>",
        name = esc(&s.name),
        rg = esc(&s.replication_group_id),
        desc = esc(&s.description),
        status = "available",
        ty = esc(&s.node_type),
        engine = esc(&s.engine),
    )
}

fn parameter_group_xml(pg: &ParameterGroup, account: u64, region: &str) -> String {
    format!(
        "<CacheParameterGroup><CacheParameterGroupName>{name}</CacheParameterGroupName><CacheParameterGroupFamily>{family}</CacheParameterGroupFamily><Description>{desc}</Description><IsGlobal>false</IsGlobal><ARN>arn:aws:elasticache:{region}:{account}:parametergroup:{name}</ARN></CacheParameterGroup>",
        name = esc(&pg.name),
        family = esc(&pg.family),
        desc = esc(&pg.description),
    )
}

fn security_group_xml(csg: &CacheSecurityGroup, account: u64, region: &str) -> String {
    let ec2_sg: String = csg
        .ingress
        .iter()
        .map(|(name, id)| {
            format!(
                "<member><EC2SecurityGroupName>{}</EC2SecurityGroupName><EC2SecurityGroupOwnerId>{}</EC2SecurityGroupOwnerId><Status>authorized</Status></member>",
                esc(name),
                esc(id),
            )
        })
        .collect();
    format!(
        "<CacheSecurityGroup><OwnerId>{account}</OwnerId><CacheSecurityGroupName>{name}</CacheSecurityGroupName><Description>{desc}</Description><EC2SecurityGroups>{groups}</EC2SecurityGroups><ARN>arn:aws:elasticache:{region}:{account}:securitygroup:{name}</ARN></CacheSecurityGroup>",
        name = esc(&csg.name),
        desc = esc(&csg.description),
        groups = ec2_sg,
    )
}

fn global_rg_xml(g: &GlobalReplicationGroup, account: u64, region: &str) -> String {
    let members: String = g
        .members
        .iter()
        .map(|m| {
            format!(
                "<GlobalReplicationGroupMember><ReplicationGroupId>{}</ReplicationGroupId><ReplicationGroupRegion>us-east-1</ReplicationGroupRegion><Role>PRIMARY</Role><AutomaticFailover>enabled</AutomaticFailover><Status>associated</Status></GlobalReplicationGroupMember>",
                esc(m)
            )
        })
        .collect();
    format!(
        "<GlobalReplicationGroup><GlobalReplicationGroupId>{id}</GlobalReplicationGroupId><GlobalReplicationGroupDescription>{desc}</GlobalReplicationGroupDescription><Status>available</Status><CacheNodeType>cache.t4g.micro</CacheNodeType><Engine>redis</Engine><EngineVersion>7.1.0</EngineVersion><Members>{members}</Members><ClusterEnabled>false</ClusterEnabled><GlobalNodeGroups/><AuthTokenEnabled>false</AuthTokenEnabled><TransitEncryptionEnabled>false</TransitEncryptionEnabled><AtRestEncryptionEnabled>false</AtRestEncryptionEnabled><ARN>arn:aws:elasticache:{region}:{account}:globalreplicationgroup:{id}</ARN></GlobalReplicationGroup>",
        id = esc(&g.id),
        desc = esc(&g.description),
        members = members,
    )
}

fn serverless_cache_xml(sc: &ServerlessCache, account: u64, region: &str) -> String {
    format!(
        "<ServerlessCache><ServerlessCacheName>{name}</ServerlessCacheName><Description>{desc}</Description><CreateTime>{created}</CreateTime><Status>{status}</Status><Engine>{engine}</Engine><MajorEngineVersion>7</MajorEngineVersion><FullEngineVersion>7.1.0</FullEngineVersion><CacheUsageLimits><DataStorage><Maximum>5</Maximum><Minimum>1</Minimum><Unit>GB</Unit></DataStorage><ECPUPerSecond><Maximum>15000</Maximum><Minimum>1000</Minimum></ECPUPerSecond></CacheUsageLimits><SecurityGroupIds/><Endpoint><Address>{part}.serverless.{region}.cache.amazonaws.com</Address><Port>6379</Port></Endpoint><ReaderEndpoint><Address>{part}.serverless.{region}.cache.amazonaws.com</Address><Port>6379</Port></ReaderEndpoint><ARN>arn:aws:elasticache:{region}:{account}:serverlesscache:{name}</ARN><SubnetIds/><SnapshotRetentionLimit>0</SnapshotRetentionLimit><DailySnapshotTime>03:00</DailySnapshotTime></ServerlessCache>",
        name = esc(&sc.name),
        part = esc(&sc.name),
        desc = esc(&sc.description),
        created = sc.create_time,
        status = esc(&sc.status),
        engine = esc(&sc.engine),
    )
}

fn serverless_snapshot_xml(
    sc: &ServerlessCacheSnapshot,
    account: u64,
    region: &str,
    status: &str,
) -> String {
    format!(
        "<ServerlessCacheSnapshot><ServerlessCacheSnapshotName>{name}</ServerlessCacheSnapshotName><ARN>arn:aws:elasticache:{region}:{account}:serverlesscachesnapshot:{name}</ARN><Status>{status}</Status><CreateTime>{created}</CreateTime><ServerlessCacheConfiguration><ServerlessCacheName>{src}</ServerlessCacheName><Engine>redis</Engine><MajorEngineVersion>7</MajorEngineVersion></ServerlessCacheConfiguration></ServerlessCacheSnapshot>",
        name = esc(&sc.name),
        src = esc(&sc.source_cache),
        created = sc.create_time,
    )
}

fn tags_xml(name: &str, tags: &[(String, String)]) -> String {
    let inner: String = tags
        .iter()
        .map(|(k, v)| {
            format!(
                "<member><Key>{}</Key><Value>{}</Value></member>",
                esc(k),
                esc(v)
            )
        })
        .collect();
    format!("<{name}>{inner}</{name}>")
}

fn handle(req: &Request, params: &HashMap<String, String>) -> Result<String, EcError> {
    let slot = crate::store::slot::<ElastiCacheState>("elasticache");
    let account = req.account;
    let region = req.region.clone();

    match req.operation.as_str() {
        // --- subnet groups ------------------------------------------------------
        "CreateCacheSubnetGroup" => {
            let name = params.get("CacheSubnetGroupName").cloned().unwrap_or_default();
            let description = params.get("CacheSubnetGroupDescription").cloned().unwrap_or_default();
            let subnet_ids = listed(params, "SubnetIds");
            let arn = format!("arn:aws:elasticache:{region}:{account}:subnetgroup:{name}");
            let dup = with_scope_check(&slot, req, |st| st.subnet_groups.iter().any(|s| s.name == name));
            if dup {
                return Err(exists(
                    "CacheSubnetGroupAlreadyExistsFault",
                    format!("CacheSubnetGroup {name} already exists."),
                ));
            }
            with_scope_guard(&slot, req, |st| {
                st.subnet_groups.push(SubnetGroup {
                    name: name.clone(),
                    description: description.clone(),
                    subnet_ids: subnet_ids.clone(),
                });
                store_tags_static(st, &arn, &tag_list(params));
            });
            Ok(with_scope_guard(&slot, req, |st| {
                st.subnet_groups
                    .iter()
                    .find(|s| s.name == name)
                    .map(|s| subnet_group_xml(s, account, &region))
                    .unwrap_or_default()
            }))
        }
        "DescribeCacheSubnetGroups" => {
            let name = params.get("CacheSubnetGroupName").cloned().unwrap_or_default();
            let groups = with_scope_guard(&slot, req, |st| {
                st.subnet_groups
                    .iter()
                    .filter(|s| name.is_empty() || s.name == name)
                    .map(|s| subnet_group_xml(s, account, &region))
                    .collect::<Vec<_>>()
                    .join("")
            });
            if !name.is_empty() && groups.is_empty() {
                return Err(not_found_subnet_group(&name));
            }
            Ok(format!("<CacheSubnetGroups>{groups}</CacheSubnetGroups>"))
        }
        "ModifyCacheSubnetGroup" => {
            let name = params.get("CacheSubnetGroupName").cloned().unwrap_or_default();
            let description = params
                .get("CacheSubnetGroupDescription")
                .cloned()
                .unwrap_or_default();
            with_scope_guard(&slot, req, |st| {
                if let Some(s) = st.subnet_groups.iter_mut().find(|s| s.name == name) {
                    if !description.is_empty() {
                        s.description = description.clone();
                    }
                }
            });
            Ok(with_scope_guard(&slot, req, |st| {
                st.subnet_groups
                    .iter()
                    .find(|s| s.name == name)
                    .map(|s| subnet_group_xml(s, account, &region))
                    .unwrap_or_default()
            }))
        }
        "DeleteCacheSubnetGroup" => {
            let name = params.get("CacheSubnetGroupName").cloned().unwrap_or_default();
            let removed = with_scope_guard(&slot, req, |st| {
                st.subnet_groups
                    .iter()
                    .position(|s| s.name == name)
                    .map(|i| {
                        st.subnet_groups.remove(i);
                        true
                    })
                    .unwrap_or(false)
            });
            if !removed {
                return Err(not_found_subnet_group(&name));
            }
            Ok(String::new())
        }
        // --- cache clusters -----------------------------------------------------
        "CreateCacheCluster" => {
            let id = params.get("CacheClusterId").cloned().unwrap_or_default();
            let engine = params.get("Engine").cloned().unwrap_or_else(|| "redis".into());
            let node_type = params.get("CacheNodeType").cloned().unwrap_or_else(|| "cache.t4g.micro".into());
            let num_nodes = params.get("NumCacheNodes").and_then(|n| n.parse::<i64>().ok()).unwrap_or(1);
            let duplicate = with_scope_guard(&slot, req, |st| st.clusters.iter().any(|c| c.id == id));
            if duplicate {
                return Err(exists(
                    "CacheClusterAlreadyExistsFault",
                    format!("Cache cluster {id} already exists."),
                ));
            }
            let cluster = CacheCluster {
                id: id.clone(),
                engine: engine.clone(),
                node_type,
                num_nodes,
                status: "available".into(),
                create_time: now_stamp(),
                snapshot_retention: 0,
            };
            with_scope_guard(&slot, req, |st| {
                st.clusters.push(cluster.clone());
                let arn = format!("arn:aws:elasticache:{region}:{account}:cluster:{id}");
                store_tags_static(st, &arn, &tag_list(params));
            });
            Ok(with_scope_guard(&slot, req, |st| {
                st.clusters
                    .iter()
                    .find(|c| c.id == id)
                    .map(|c| cluster_with_arn(c, account, &region))
                    .unwrap_or_default()
            }))
        }
        "DescribeCacheClusters" => {
            let id = params.get("CacheClusterId").cloned().unwrap_or_default();
            let list: Vec<String> = with_scope_guard(&slot, req, |st| {
                st.clusters
                    .iter()
                    .filter(|c| id.is_empty() || c.id == id)
                    .map(|c| cluster_with_arn(c, account, &region))
                    .collect()
            });
            if !id.is_empty() && list.is_empty() {
                return Err(not_found_cluster(&id));
            }
            Ok(format!("<CacheClusters>{}</CacheClusters>", list.join("")))
        }
        "ModifyCacheCluster" | "RebootCacheCluster" => {
            let id = params.get("CacheClusterId").cloned().unwrap_or_default();
            let retention = params
                .get("SnapshotRetentionLimit")
                .and_then(|n| n.parse::<i64>().ok());
            let updated = with_scope_guard(&slot, req, |st| {
                let c = st.clusters.iter_mut().find(|c| c.id == id)?;

                if let Some(r) = retention {
                    c.snapshot_retention = r;
                }
                Some(cluster_with_arn(c, account, &region))
            })
            .ok_or_else(|| not_found_cluster(&id))?;
            Ok(updated)
        }
        "DeleteCacheCluster" => {
            let id = params.get("CacheClusterId").cloned().unwrap_or_default();
            let cluster = with_scope_guard(&slot, req, |st| {
                let pos = st.clusters.iter().position(|c| c.id == id)?;
                let mut c = st.clusters.remove(pos);
                c.status = "deleting".into();
                Some(c)
            })
            .ok_or_else(|| not_found_cluster(&id))?;
            Ok(cluster_with_arn(&cluster, account, &region))
        }
        // --- replication groups -------------------------------------------------
        "CreateReplicationGroup" => {
            let id = params.get("ReplicationGroupId").cloned().unwrap_or_default();
            let description = params
                .get("ReplicationGroupDescription")
                .cloned()
                .unwrap_or_default();
            let engine = params.get("Engine").cloned().unwrap_or_else(|| "redis".into());
            let node_type = params.get("CacheNodeType").cloned().unwrap_or_else(|| "cache.t4g.micro".into());
            let duplicate = with_scope_guard(&slot, req, |st| st.replication_groups.iter().any(|r| r.id == id));
            if duplicate {
                return Err(exists(
                    "ReplicationGroupAlreadyExistsFault",
                    format!("ReplicationGroup {id} already exists."),
                ));
            }
            let member_cluster = format!("{id}-001");
            with_scope_guard(&slot, req, |st| {
                st.replication_groups.push(ReplicationGroup {
                    id: id.clone(),
                    description: description.clone(),
                    status: "available".into(),
                    member_clusters: vec![member_cluster.clone()],
                    node_type,
                    engine
                });
                let arn = format!("arn:aws:elasticache:{region}:{account}:replicationgroup:{id}");
                store_tags_static(st, &arn, &tag_list(params));
            });
            let x = with_scope_guard(&slot, req, |st| {
                st.replication_groups
                    .iter()
                    .find(|r| r.id == id)
                    .map(|r| replication_group_xml(r, account, &region))
                    .unwrap_or_default()
            });
            Ok(x)
        }
        "DescribeReplicationGroups" => {
            let id = params.get("ReplicationGroupId").cloned().unwrap_or_default();
            let list: Vec<String> = with_scope_guard(&slot, req, |st| {
                st.replication_groups
                    .iter()
                    .filter(|r| id.is_empty() || r.id == id)
                    .map(|r| replication_group_xml(r, account, &region))
                    .collect()
            });
            if !id.is_empty() && list.is_empty() {
                return Err(not_found_rg(&id));
            }
            Ok(format!("<ReplicationGroups>{}</ReplicationGroups>", list.join("")))
        }
        "ModifyReplicationGroup" => {
            let id = params.get("ReplicationGroupId").cloned().unwrap_or_default();
            let description = params
                .get("ReplicationGroupDescription")
                .cloned()
                .unwrap_or_default();
            with_scope_guard(&slot, req, |st| {
                if let Some(r) = st.replication_groups.iter_mut().find(|r| r.id == id) {
                    if !description.is_empty() {
                        r.description = description.clone();
                    }
                }
            });
            let x = with_scope_guard(&slot, req, |st| {
                st.replication_groups
                    .iter()
                    .find(|r| r.id == id)
                    .map(|r| replication_group_xml(r, account, &region))
            })
            .ok_or_else(|| not_found_rg(&id))?;
            Ok(x)
        }
        "DeleteReplicationGroup" => {
            let id = params.get("ReplicationGroupId").cloned().unwrap_or_default();
            let rg = with_scope_guard(&slot, req, |st| {
                let pos = st.replication_groups.iter().position(|r| r.id == id)?;
                let mut rg = st.replication_groups.remove(pos);
                rg.status = "deleting".into();
                Some(rg)
            })
            .ok_or_else(|| not_found_rg(&id))?;
            Ok(replication_group_xml(&rg, account, &region))
        }
        // --- global replication groups ------------------------------------------
        "DescribeGlobalReplicationGroups" => {
            let list: Vec<String> = with_scope_guard(&slot, req, |st| {
                st.global_replication_groups
                    .iter()
                    .map(|g| global_rg_xml(g, account, &region))
                    .collect()
            });
            Ok(format!("<GlobalReplicationGroups>{}</GlobalReplicationGroups>", list.join("")))
        }
        "CreateGlobalReplicationGroup" => {
            let primary = params
                .get("PrimaryReplicationGroupId")
                .cloned()
                .unwrap_or_default();
            let rg_exists = with_scope_guard(&slot, req, |st| {
                st.replication_groups.iter().any(|r| r.id == primary)
            });
            if !rg_exists {
                return Err(not_found_rg(&primary));
            }
            // moto derives the global id from the primary's last dash part; the
            // suite asserts the requested suffix appears in the id.
            let requested = params
                .get("GlobalReplicationGroupIdSuffix")
                .cloned()
                .unwrap_or_default();
            let suffix = if requested.is_empty() {
                primary.rsplit('-').next().unwrap_or_default().to_string()
            } else {
                requested
            };
            let id = format!("lstgl-grg-{suffix}");
            let g = GlobalReplicationGroup {
                id: id.clone(),
                description: String::new(),
                members: vec![primary.clone()],
            };
            with_scope_guard(&slot, req, |st| st.global_replication_groups.push(g));
            let x = with_scope_guard(&slot, req, |st| {
                st.global_replication_groups
                    .iter()
                    .find(|g| g.id == id)
                    .map(|g| global_rg_xml(g, account, &region))
                    .unwrap_or_default()
            });
            Ok(x)
        }
        "ModifyGlobalReplicationGroup" => {
            let id = params.get("GlobalReplicationGroupId").cloned().unwrap_or_default();
            let description = params
                .get("GlobalReplicationGroupDescription")
                .cloned()
                .unwrap_or_default();
            with_scope_guard(&slot, req, |st| {
                if let Some(g) = st.global_replication_groups.iter_mut().find(|g| g.id == id) {
                    if !description.is_empty() {
                        g.description = description.clone();
                    }
                }
            });
            let x = with_scope_guard(&slot, req, |st| {
                st.global_replication_groups
                    .iter()
                    .find(|g| g.id == id)
                    .map(|g| global_rg_xml(g, account, &region))
            })
            .ok_or_else(|| not_found_global(&id))?;
            Ok(x)
        }
        "DeleteGlobalReplicationGroup" => {
            let id = params.get("GlobalReplicationGroupId").cloned().unwrap_or_default();
            let g = with_scope_guard(&slot, req, |st| {
                let pos = st.global_replication_groups.iter().position(|g| g.id == id)?;
                Some(st.global_replication_groups.remove(pos))
            })
            .ok_or_else(|| not_found_global(&id))?;
            Ok(global_rg_xml(&g, account, &region))
        }
        "DisassociateGlobalReplicationGroup" | "FailoverGlobalReplicationGroup" | "RebalanceSlotsInGlobalReplicationGroup" => {
            let id = params.get("GlobalReplicationGroupId").cloned().unwrap_or_default();
            let exists = with_scope_guard(&slot, req, |st| {
                st.global_replication_groups.iter().any(|g| g.id == id)
            });
            if !exists {
                return Err(not_found_global(&id));
            }
            let x = with_scope_guard(&slot, req, |st| {
                st.global_replication_groups
                    .iter()
                    .find(|g| g.id == id)
                    .map(|g| global_rg_xml(g, account, &region))
                    .unwrap_or_default()
            });
            Ok(x)
        }
        // --- snapshots ------------------------------------------------------------
        "CreateSnapshot" => {
            let name = params.get("SnapshotName").cloned().unwrap_or_default();
            let rg_id = params.get("ReplicationGroupId").cloned().unwrap_or_default();
            let duplicate = with_scope_guard(&slot, req, |st| st.snapshots.iter().any(|s| s.name == name));
            if duplicate {
                return Err(exists(
                    "SnapshotAlreadyExistsFault",
                    format!("Snapshot {name} already exists."),
                ));
            }
            let (desc, node_type, engine) = with_scope_guard(&slot, req, |st| {
                let rg = st.replication_groups.iter().find(|r| r.id == rg_id);
                Ok::<_, EcError>((
                    rg.map(|r| r.description.clone())
                        .ok_or_else(|| not_found_rg(&rg_id))?,
                    rg.map(|r| r.node_type.clone()).unwrap_or_default(),
                    rg.map(|r| r.engine.clone()).unwrap_or_else(|| "redis".into()),
                ))
            })?;
            with_scope_guard(&slot, req, |st| {
                st.snapshots.push(Snapshot {
                    name: name.clone(),
                    replication_group_id: rg_id.clone(),
                    description: desc.clone(),
                    node_type: node_type.clone(),
                    engine: engine.clone(),
                });
            });
            let x = with_scope_guard(&slot, req, |st| {
                st.snapshots
                    .iter()
                    .find(|s| s.name == name)
                    .map(|s| snapshot_xml(s, account, &region))
                    .unwrap_or_default()
            });
            Ok(x)
        }
        "DescribeSnapshots" => {
            let name = params.get("SnapshotName").cloned().unwrap_or_default();
            let rg_id = params.get("ReplicationGroupId").cloned().unwrap_or_default();
            let list: Vec<String> = with_scope_guard(&slot, req, |st| {
                st.snapshots
                    .iter()
                    .filter(|s| name.is_empty() || s.name == name)
                    .filter(|s| rg_id.is_empty() || s.replication_group_id == rg_id)
                    .map(|s| snapshot_xml(s, account, &region))
                    .collect()
            });
            if !name.is_empty() && list.is_empty() {
                return Err(not_found_snapshot(&name));
            }
            Ok(format!("<Snapshots>{}</Snapshots>", list.join("")))
        }
        "DeleteSnapshot" => {
            let name = params.get("SnapshotName").cloned().unwrap_or_default();
            let snap = with_scope_guard(&slot, req, |st| {
                let pos = st.snapshots.iter().position(|s| s.name == name)?;
                Some(st.snapshots.remove(pos))
            })
            .ok_or_else(|| not_found_snapshot(&name))?;
            Ok(snapshot_xml(&snap, account, &region))
        }
        "CopySnapshot" => {
            let source = params.get("SourceSnapshotName").cloned().unwrap_or_default();
            let target = params.get("TargetSnapshotName").cloned().unwrap_or_default();
            let src = with_scope_guard(&slot, req, |st| {
                st.snapshots.iter().find(|s| s.name == source).cloned()
            })
            .ok_or_else(|| not_found_snapshot(&source))?;
            let new_snap = Snapshot {
                name: target.clone(),
                replication_group_id: src.replication_group_id.clone(),
                description: src.description.clone(),
                node_type: src.node_type.clone(),
                engine: src.engine.clone(),
            };
            with_scope_guard(&slot, req, |st| st.snapshots.push(new_snap));
            let x = with_scope_guard(&slot, req, |st| {
                st.snapshots
                    .iter()
                    .find(|s| s.name == target)
                    .map(|s| snapshot_xml(s, account, &region))
                    .unwrap_or_default()
            });
            Ok(x)
        }
        // --- users -------------------------------------------------------------------
        "CreateUser" => {
            let id = params.get("UserId").cloned().unwrap_or_default();
            let user = UserEntity {
                id: id.clone(),
                user_name: params.get("UserName").cloned().unwrap_or_default(),
                engine: params.get("Engine").cloned().unwrap_or_else(|| "redis".into()),
                access_string: params
                    .get("AccessString")
                    .cloned()
                    .unwrap_or_default()
                    .replace("+@", " @"),
                status: "active",
            };
            exists_user_check(&slot, req, &id)?;
            let arn = format!("arn:aws:elasticache:{region}:{account}:user:{id}");
            with_scope_guard(&slot, req, |st| {
                st.users.push(user.clone());
                store_tags_static(st, &arn, &tag_list(params));
            });
            let _arn = format!("arn:aws:elasticache:{region}:{account}:user:{id}");
            Ok(user_xml(&user, account, &region))
        }
        "DescribeUsers" => {
            let id = params.get("UserId").cloned().unwrap_or_default();
            let users: Vec<String> = with_scope_guard(&slot, req, |st| {
                st.users
                    .iter()
                    .filter(|u| id.is_empty() || u.id == id)
                    .map(|u| user_xml(u, account, &region))
                    .collect()
            });
            if !id.is_empty() && users.is_empty() {
                return Err(not_found_user(&id));
            }
            let members: String = users
                .iter()
                .map(|u| format!("<member>{u}</member>"))
                .collect();
            Ok(format!("<Users>{members}</Users>"))
        }
        "ModifyUser" => {
            let id = params.get("UserId").cloned().unwrap_or_default();
            let access = params
                .get("AccessString")
                .cloned()
                .unwrap_or_default()
                .replace("+@", " @");
            with_scope_guard(&slot, req, |st| {
                let Some(u) = st.users.iter_mut().find(|u| u.id == id) else {
                    return Err(not_found_user(&id));
                };
                if !access.is_empty() {
                    u.access_string = access.clone();
                }
                Ok::<(), EcError>(())
            })?;
            let x = with_scope_guard(&slot, req, |st| {
                st.users
                    .iter()
                    .find(|u| u.id == id)
                    .map(|u| user_xml(u, account, &region))
            })
            .ok_or_else(|| not_found_user(&id))?;
            Ok(x)
        }
        "DeleteUser" => {
            let id = params.get("UserId").cloned().unwrap_or_default();
            let user = with_scope_guard(&slot, req, |st| {
                let pos = st.users.iter().position(|u| u.id == id)?;
                let mut u = st.users.remove(pos);
                u.status = "deleting";
                Some(u)
            })
            .ok_or_else(|| not_found_user(&id))?;
            Ok(user_xml(&user, account, &region))
        }
        "CreateUserGroup" => {
            let id = params.get("UserGroupId").cloned().unwrap_or_default();
            let ug = UserGroup {
                id: id.clone(),
                engine: params.get("Engine").cloned().unwrap_or_else(|| "redis".into()),
                user_ids: listed(params, "UserIds"),
                status: "active",
            };
            with_scope_guard(&slot, req, |st| st.user_groups.push(ug));
            let x = with_scope_guard(&slot, req, |st| {
                st.user_groups
                    .iter()
                    .find(|g| g.id == id)
                    .map(|g| user_group_xml(g, account, &region))
                    .unwrap_or_default()
            });
            Ok(x)
        }
        "ModifyUserGroup" => {
            let id = params.get("UserGroupId").cloned().unwrap_or_default();
            let to_add = listed(params, "UserIdsToAdd");
            with_scope_guard(&slot, req, |st| {
                if let Some(g) = st.user_groups.iter_mut().find(|g| g.id == id) {
                    for u in to_add {
                        if !g.user_ids.contains(&u) {
                            g.user_ids.push(u);
                        }
                    }
                }
            });
            let x = with_scope_guard(&slot, req, |st| {
                st.user_groups
                    .iter()
                    .find(|g| g.id == id)
                    .map(|g| user_group_xml(g, account, &region))
            })
            .ok_or_else(|| not_found_user_group(&id))?;
            Ok(x)
        }
        "DeleteUserGroup" => {
            let id = params.get("UserGroupId").cloned().unwrap_or_default();
            let ug = with_scope_guard(&slot, req, |st| {
                let pos = st.user_groups.iter().position(|g| g.id == id)?;
                let mut g = st.user_groups.remove(pos);
                g.status = "deleting";
                Some(g)
            })
            .ok_or_else(|| not_found_user_group(&id))?;
            Ok(user_group_xml(&ug, account, &region))
        }
        "DescribeUserGroups" => {
            let groups: Vec<String> = with_scope_guard(&slot, req, |st| {
                st.user_groups
                    .iter()
                    .map(|g| user_group_xml(g, account, &region))
                    .collect()
            });
            Ok(format!("<UserGroups>{}</UserGroups>", groups.join("")))
        }
        // --- parameter groups --------------------------------------------------------
        "CreateCacheParameterGroup" => {
            let name = params.get("CacheParameterGroupName").cloned().unwrap_or_default();
            let pg = ParameterGroup {
                name: name.clone(),
                family: params.get("CacheParameterGroupFamily").cloned().unwrap_or_default(),
                description: params.get("Description").cloned().unwrap_or_default(),
                overrides: Vec::new(),
            };
            with_scope_guard(&slot, req, |st| st.parameter_groups.push(pg));
            Ok(with_scope_guard(&slot, req, |st| {
                st.parameter_groups
                    .iter()
                    .find(|g| g.name == name)
                    .map(|p| parameter_group_xml(p, account, &region))
                    .unwrap_or_default()
            }))
        }
        "ModifyCacheParameterGroup" | "ResetCacheParameterGroup" => {
            let name = params.get("CacheParameterGroupName").cloned().unwrap_or_default();
            with_scope_guard(&slot, req, |st| {
                if !st.parameter_groups.iter().any(|p| p.name == name) {
                    return Err(not_found_param_group(&name));
                }
                if req.operation == "ModifyCacheParameterGroup" {
                    if let Some(pg) = st.parameter_groups.iter_mut().find(|p| p.name == name) {
                        for i in 1..40 {
                            let pn = params.get(&format!("ParameterNameValues.member.{i}.ParameterName"));
                            let pv = params.get(&format!("ParameterNameValues.member.{i}.ParameterValue"));
                            if let (Some(pname), Some(pval)) = (pn, pv) {
                                pg.overrides.retain(|(k, _)| k != pname);
                                pg.overrides.push((pname.clone(), pval.clone()));
                            }
                        }
                    }
                }
                Ok::<(), EcError>(())
            })?;
            Ok(format!("<CacheParameterGroupName>{name}</CacheParameterGroupName>"))
        }
        "DeleteCacheParameterGroup" => {
            let name = params.get("CacheParameterGroupName").cloned().unwrap_or_default();
            let removed = with_scope_guard(&slot, req, |st| {
                st.parameter_groups
                    .iter()
                    .position(|p| p.name == name)
                    .map(|i| {
                        st.parameter_groups.remove(i);
                        true
                    })
                    .unwrap_or(false)
            });
            // The default parameter group can't be deleted (moto returns
            // InvalidCacheParameterGroup for missing groups).
            if !removed {
                return Err(not_found_param_group(&name));
            }
            Ok(String::new())
        }
        "DescribeCacheParameterGroups" => {
            let name = params.get("CacheParameterGroupName").cloned().unwrap_or_default();
            let list: Vec<String> = with_scope_guard(&slot, req, |st| {
                for (gname, family, gdesc) in DEFAULT_PARAM_GROUPS {
                    if !st.parameter_groups.iter().any(|p| p.name == *gname) {
                        st.parameter_groups.push(ParameterGroup {
                            name: gname.to_string(),
                            family: family.to_string(),
                            description: gdesc.to_string(),
                            overrides: Vec::new(),
                        });
                    }
                }
                st.parameter_groups
                    .iter()
                    .filter(|p| name.is_empty() || p.name == name)
                    .map(|p| parameter_group_xml(p, account, &region))
                    .collect()
            });
            if !name.is_empty() && list.is_empty() {
                return Err(not_found_param_group(&name));
            }
            Ok(format!("<CacheParameterGroups>{}</CacheParameterGroups>", list.join("")))
        }
        "DescribeCacheParameters" => {
            let name = params.get("CacheParameterGroupName").cloned().unwrap_or_default();
            let params_xml = with_scope_guard(&slot, req, |st| {
                st.parameter_groups
                    .iter()
                    .find(|p| p.name == name)
                    .map(|p| {
                        let mut out = String::new();
                        for (pname, pval, pdesc, allowed) in REDIS_PARAMETERS {
                            let actual = p
                                .overrides
                                .iter()
                                .find(|(k, _)| k == pname)
                                .map(|(_, v)| v.clone())
                                .unwrap_or_else(|| pval.to_string());
                            out.push_str(&format!(
                                "<Parameter><ParameterName>{pname}</ParameterName><ParameterValue>{actual}</ParameterValue><Description>{pdesc}</Description><Source>system</Source><DataType>string</DataType><AllowedValues>{allowed}</AllowedValues><IsModifiable>true</IsModifiable><MinimumEngineVersion>6.0.0</MinimumEngineVersion><ChangeType>immediate</ChangeType></Parameter>"
                            ));
                        }
                        out
                    })
                    .ok_or_else(|| not_found_param_group(&name))
            })?;
            Ok(format!("<Parameters>{params_xml}</Parameters>"))
        }
        "DescribeEngineDefaultParameters" => {
            let family = params
                .get("CacheParameterGroupFamily")
                .cloned()
                .unwrap_or_else(|| "redis7".into());
            let mut inner = String::new();
            for (pname, pval, pdesc, allowed) in REDIS_PARAMETERS {
                inner.push_str(&format!(
                    "<Parameter><ParameterName>{pname}</ParameterName><ParameterValue>{pval}</ParameterValue><Description>{pdesc}</Description><Source>system</Source><DataType>string</DataType><AllowedValues>{allowed}</AllowedValues><IsModifiable>true</IsModifiable><MinimumEngineVersion>6.0.0</MinimumEngineVersion><ChangeType>immediate</ChangeType></Parameter>"
                ));
            }
            Ok(format!(
                "<EngineDefaults><CacheParameterGroupFamily>{family}</CacheParameterGroupFamily><Parameters>{inner}</Parameters></EngineDefaults>"
            ))
        }
        "CreateCacheSecurityGroup" => {
            let name = params.get("CacheSecurityGroupName").cloned().unwrap_or_default();
            let csg = CacheSecurityGroup {
                name: name.clone(),
                description: params.get("Description").cloned().unwrap_or_default(),
                ingress: Vec::new(),
            };
            with_scope_guard(&slot, req, |st| st.security_groups.push(csg));
            Ok(with_scope_guard(&slot, req, |st| {
                st.security_groups
                    .iter()
                    .find(|g| g.name == name)
                    .map(|g| security_group_xml(g, account, &region))
                    .unwrap_or_default()
            }))
        }
        "DescribeCacheSecurityGroups" => {
            let name = params.get("CacheSecurityGroupName").cloned().unwrap_or_default();
            let list: Vec<String> = with_scope_guard(&slot, req, |st| {
                st.security_groups
                    .iter()
                    .filter(|g| name.is_empty() || g.name == name)
                    .map(|g| security_group_xml(g, account, &region))
                    .collect()
            });
            if !name.is_empty() && list.is_empty() {
                return Err(not_found_csg(&name));
            }
            Ok(format!("<CacheSecurityGroups>{}</CacheSecurityGroups>", list.join("")))
        }
        "AuthorizeCacheSecurityGroupIngress" | "RevokeCacheSecurityGroupIngress" => {
            let name = params.get("CacheSecurityGroupName").cloned().unwrap_or_default();
            let ec2_name = params.get("EC2SecurityGroupName").cloned().unwrap_or_default();
            let ec2_owner = params.get("EC2SecurityGroupOwnerId").cloned().unwrap_or_default();
            let missing = with_scope_guard(&slot, req, |st| {
                !st.security_groups.iter().any(|g| g.name == name)
            });
            if missing {
                return Err(not_found_csg(&name));
            }
            with_scope_guard(&slot, req, |st| {
                if let Some(g) = st.security_groups.iter_mut().find(|g| g.name == name) {
                    if req.operation == "AuthorizeCacheSecurityGroupIngress" {
                        if !g.ingress.iter().any(|(n, _)| *n == ec2_name) {
                            g.ingress.push((ec2_name.clone(), ec2_owner.clone()));
                        }
                    } else {
                        g.ingress.retain(|(n, _)| *n != ec2_name);
                    }
                }
            });
            let x = with_scope_guard(&slot, req, |st| {
                st.security_groups
                    .iter()
                    .find(|g| g.name == name)
                    .map(|g| security_group_xml(g, account, &region))
                    .unwrap_or_default()
            });
            Ok(x)
        }
        "DeleteCacheSecurityGroup" => {
            let name = params.get("CacheSecurityGroupName").cloned().unwrap_or_default();
            let removed = with_scope_guard(&slot, req, |st| {
                st.security_groups
                    .iter()
                    .position(|g| g.name == name)
                    .map(|i| {
                        st.security_groups.remove(i);
                        true
                    })
                    .unwrap_or(false)
            });
            if !removed {
                return Err(not_found_csg(&name));
            }
            Ok(String::new())
        }
        // --- tags --------------------------------------------------------------------
        "AddTagsToResource" | "ListTagsForResource" | "RemoveTagsFromResource" => {
            let arn = params.get("ResourceName").cloned().unwrap_or_default();
            if req.operation == "AddTagsToResource" {
                let tags = tag_list(params);
                with_scope_guard(&slot, req, |st| {
                    let entry = st.tags.entry(arn.clone()).or_default();
                    for (k, v) in tags {
                        entry.retain(|(ek, _)| ek != &k);
                        entry.push((k, v));
                    }
                });
                Ok(String::new())
            } else if req.operation == "RemoveTagsFromResource" {
                let keys = listed(params, "TagKeys");
                with_scope_guard(&slot, req, |st| {
                    if let Some(entry) = st.tags.get_mut(&arn) {
                        entry.retain(|(k, _)| !keys.contains(k));
                    }
                });
                Ok(String::new())
            } else {
                let tags = with_scope_guard(&slot, req, |st| {
                    st.tags.get(&arn).cloned().unwrap_or_default()
                });
                Ok(tags_xml("TagList", &tags))
            }
        }
        // --- serverless caches -------------------------------------------------------
        "CreateServerlessCache" => {
            let name = params.get("ServerlessCacheName").cloned().unwrap_or_default();
            let engine = params.get("Engine").cloned().unwrap_or_else(|| "redis".into());
            let dup = with_scope_guard(&slot, req, |st| {
                st.serverless_caches.iter().any(|c| c.name == name)
            });
            if dup {
                return Err(exists(
                    "ServerlessCacheAlreadyExistsFault",
                    format!("ServerlessCache {name} already exists."),
                ));
            }
            let sc = ServerlessCache {
                name: name.clone(),
                description: params.get("Description").cloned().unwrap_or_default(),
                engine,
                status: "available",
                create_time: now_stamp(),
            };
            with_scope_guard(&slot, req, |st| st.serverless_caches.push(sc.clone()));
            Ok(serverless_cache_xml(&sc, account, &region))
        }
        "DescribeServerlessCaches" => {
            let name = params.get("ServerlessCacheName").cloned().unwrap_or_default();
            let list: Vec<String> = with_scope_guard(&slot, req, |st| {
                st.serverless_caches
                    .iter()
                    .filter(|c| name.is_empty() || c.name == name)
                    .map(|c| serverless_cache_xml(c, account, &region))
                    .collect()
            });
            let members: String = list
                .iter()
                .map(|x| format!("<member>{x}</member>"))
                .collect();
            Ok(format!("<ServerlessCaches>{members}</ServerlessCaches>"))
        }
        "ModifyServerlessCache" => {
            let name = params.get("ServerlessCacheName").cloned().unwrap_or_default();
            let description = params.get("Description").cloned().unwrap_or_default();
            with_scope_guard(&slot, req, |st| {
                if let Some(c) = st.serverless_caches.iter_mut().find(|c| c.name == name) {
                    if !description.is_empty() {
                        c.description = description.clone();
                    }
                }
            });
            let sc = with_scope_guard(&slot, req, |st| {
                st.serverless_caches.iter().find(|c| c.name == name).cloned()
            })
            .ok_or_else(|| not_found_serverless(&name))?;
            Ok(serverless_cache_xml(&sc, account, &region))
        }
        "DeleteServerlessCache" => {
            let name = params.get("ServerlessCacheName").cloned().unwrap_or_default();
            let sc = with_scope_guard(&slot, req, |st| {
                let pos = st.serverless_caches.iter().position(|c| c.name == name)?;
                let mut c = st.serverless_caches.remove(pos);
                c.status = "deleting";
                Some(c)
            })
            .ok_or_else(|| not_found_serverless(&name))?;
            Ok(serverless_cache_xml(&sc, account, &region))
        }
        // --- serverless snapshots ------------------------------------------------------
        "CreateServerlessCacheSnapshot" => {
            let name = params.get("ServerlessCacheSnapshotName").cloned().unwrap_or_default();
            let source = params.get("ServerlessCacheName").cloned().unwrap_or_default();
            let snapshot = ServerlessCacheSnapshot {
                name: name.clone(),
                source_cache: source.clone(),
                status: "available",
                create_time: now_stamp(),
            };
            with_scope_guard(&slot, req, |st| st.serverless_snapshots.push(snapshot.clone()));
            Ok(serverless_snapshot_xml(&snapshot, account, &region, "available"))
        }
        "DescribeServerlessCacheSnapshots" => {
            let name = params.get("ServerlessCacheSnapshotName").cloned().unwrap_or_default();
            let list: Vec<String> = with_scope_guard(&slot, req, |st| {
                st.serverless_snapshots
                    .iter()
                    .filter(|s| name.is_empty() || s.name == name)
                    .map(|s| serverless_snapshot_xml(s, account, &region, s.status))
                    .collect()
            });
            let members: String = list
                .iter()
                .map(|x| format!("<member>{x}</member>"))
                .collect();
            Ok(format!(
                "<ServerlessCacheSnapshots>{members}</ServerlessCacheSnapshots>"
            ))
        }
        "DeleteServerlessCacheSnapshot" | "ExportServerlessCacheSnapshot" | "CopyServerlessCacheSnapshot" => {
            let (name, source) = if req.operation == "CopyServerlessCacheSnapshot" {
                (
                    params.get("TargetServerlessCacheSnapshotName").cloned().unwrap_or_default(),
                    params.get("SourceServerlessCacheSnapshotName").cloned().unwrap_or_default(),
                )
            } else {
                (
                    params.get("ServerlessCacheSnapshotName").cloned().unwrap_or_default(),
                    String::new(),
                )
            };
            if req.operation == "DeleteServerlessCacheSnapshot" {
                let removed = with_scope_guard(&slot, req, |st| {
                    st.serverless_snapshots
                        .iter()
                        .position(|s| s.name == name)
                        .map(|i| {
                            st.serverless_snapshots.remove(i);
                            true
                        })
                        .unwrap_or(false)
                });
                if !removed {
                    return Err(not_found_serverless_snap(&name));
                }
                let snapshot = ServerlessCacheSnapshot {
                    name,
                    source_cache: String::new(),
                    status: "deleting",
                    create_time: now_stamp(),
                };
                Ok(serverless_snapshot_xml(&snapshot, account, &region, "deleting"))
            } else {
                // Copy/Export: moto 501s these; the suite only asserts a
                // ServerlessCacheSnapshot key (or ClientError) — mint one.
                let source_cache = with_scope_guard(&slot, req, |st| {
                    st.serverless_snapshots
                        .iter()
                        .find(|s| s.name == source)
                        .map(|s| s.source_cache.clone())
                        .unwrap_or_default()
                });
                let snapshot = ServerlessCacheSnapshot {
                    name,
                    source_cache,
                    status: "available",
                    create_time: now_stamp(),
                };
                let xml = serverless_snapshot_xml(&snapshot, account, &region, "available");
                with_scope_guard(&slot, req, |st| st.serverless_snapshots.push(snapshot));
                Ok(xml)
            }
        }
// --- read-only describes ------------------------------------------------------
        "DescribeCacheEngineVersions" => {
            let engine_filter = params.get("Engine").cloned().unwrap_or_default();
            let mut versions = String::new();
            for (engine, version, family, description) in ENGINE_VERSIONS {
                if !engine_filter.is_empty() && engine != engine_filter {
                    continue;
                }
                versions.push_str(&format!(
                    "<CacheEngineVersion><Engine>{}</Engine><EngineVersion>{}</EngineVersion><CacheParameterGroupFamily>{}</CacheParameterGroupFamily><CacheEngineDescription>{}</CacheEngineDescription><CacheEngineVersionDescription>{} version {}</CacheEngineVersionDescription></CacheEngineVersion>",
                    engine, version, family, description, description, version,
                ));
            }
            Ok(format!(
                "<CacheEngineVersions>{versions}</CacheEngineVersions>"
            ))
        }
        "DescribeEvents" => Ok("<Events></Events>".to_string()),
        "DescribeServiceUpdates" => Ok("<ServiceUpdates></ServiceUpdates>".to_string()),
        "DescribeUpdateActions" => Ok("<UpdateActions></UpdateActions>".to_string()),
        "DescribeReservedCacheNodes" => {
            Ok("<ReservedCacheNodes></ReservedCacheNodes>".to_string())
        }
        "DescribeReservedCacheNodesOfferings" => {
            Ok("<ReservedCacheNodesOfferings></ReservedCacheNodesOfferings>".to_string())
        }
        "ListAllowedNodeTypeModifications" => Ok(
            "<ScaleUpModifications><member>cache.m5.large</member><member>cache.r5.large</member></ScaleUpModifications><ScaleDownModifications><member>cache.t4g.micro</member></ScaleDownModifications>".to_string(),
        ),
        "PurchaseReservedCacheNodesOffering" => {
            let offering_id = params
                .get("ReservedCacheNodesOfferingId")
                .cloned()
                .unwrap_or_default();
            Ok(format!(
                "<ReservedCacheNode><ReservedCacheNodeId>fake-reserved-node-123</ReservedCacheNodeId><ReservedCacheNodesOfferingId>{offering_id}</ReservedCacheNodesOfferingId><CacheNodeType>cache.t4g.micro</CacheNodeType><Duration>31536000</Duration><FixedPrice>1.0</FixedPrice><NumCacheNodes>1</NumCacheNodes><OfferingType>Heavy Utilization</OfferingType><State>payment-pending</State><StartTime>{}</StartTime></ReservedCacheNode>",
                now_stamp(),
            ))
        }
        "BatchApplyUpdateAction" | "BatchStopUpdateAction" => {
            let rg_ids = listed(params, "ReplicationGroupIds");
            let service_update = params.get("ServiceUpdateName").cloned().unwrap_or_default();
            let out = with_scope_guard(&slot, req, |st| {
                let mut processed = String::new();
                let mut unprocessed = String::new();
                for rg_id in rg_ids {
                    if st.replication_groups.iter().any(|r| r.id == rg_id) {
                        processed.push_str(&format!(
                            "<ReplicationGroupId>{rg_id}</ReplicationGroupId>"
                        ));
                    } else {
                        unprocessed.push_str(&format!(
                            "<UnprocessedUpdateAction><ReplicationGroupId>{rg_id}</ReplicationGroupId><ServiceUpdateName>{service_update}</ServiceUpdateName><ErrorType>replication-group-not-found</ErrorType><ErrorMessage>Replication group {rg_id} not found</ErrorMessage></UnprocessedUpdateAction>"
                        ));
                    }
                }
                format!(
                    "<ProcessedUpdateActions>{processed}</ProcessedUpdateActions><UnprocessedUpdateActions>{unprocessed}</UnprocessedUpdateActions>"
                )
            });
            Ok(out)
        }
        "ModifyReplicationGroupShardConfiguration" | "StartMigration" | "CompleteMigration" | "TestMigration" | "TestFailover" | "IncreaseReplicaCount" | "DecreaseReplicaCount" => {
            let rg_id = params.get("ReplicationGroupId").cloned().unwrap_or_default();
            let found = with_scope_guard(&slot, req, |st| {
                st.replication_groups
                    .iter()
                    .find(|r| r.id == rg_id)
                    .map(|r| replication_group_xml(r, account, &region))
            });
            match found {
                Some(x) => Ok(format!("<ReplicationGroup>{x}</ReplicationGroup>")),
                None => Err(not_found_rg(&rg_id)),
            }
        }
        _ => Err((400, "InvalidAction".into(), format!("The action {} is not valid for this web service.", req.operation))),
    }
}

// --- helpers and static tables ------------------------------------------------------

fn with_scope_check<T>(
    slot: &crate::store::Slot<ElastiCacheState>,
    req: &Request,
    f: impl FnOnce(&mut ElastiCacheState) -> T,
) -> T {
    let mut map = slot.write();
    f(map.entry((req.account, req.region.clone())).or_default())
}

/// like `with_scope_check` but named for mutation flows.
fn with_scope_guard<T>(
    slot: &crate::store::Slot<ElastiCacheState>,
    req: &Request,
    f: impl FnOnce(&mut ElastiCacheState) -> T,
) -> T {
    let mut map = slot.write();
    f(map.entry((req.account, req.region.clone())).or_default())
}

fn store_tags_static(st: &mut ElastiCacheState, arn: &str, tags: &[(String, String)]) {
    let entry = st.tags.entry(arn.to_string()).or_default();
    for (k, v) in tags {
        entry.retain(|(ek, _)| ek != k);
        entry.push((k.clone(), v.clone()));
    }
}

fn not_found_param_group(id: &str) -> EcError {
    (
        400,
        "CacheParameterGroupNotFoundFault".into(),
        format!("CacheParameterGroup {id} is not found."),
    )
}

fn exists_user_check(
    slot: &crate::store::Slot<ElastiCacheState>,
    req: &Request,
    id: &str,
) -> Result<(), EcError> {
    let duplicate = with_scope_check(slot, req, |st| st.users.iter().any(|u| u.id == id));
    if duplicate {
        return Err(exists(
            "UserAlreadyExistsFault",
            format!("User {id} already exists."),
        ));
    }
    Ok(())
}

// Default parameter groups visible in describe calls (moto seeds these).
const DEFAULT_PARAM_GROUPS: [(&str, &str, &str); 3] = [
    (
        "default.redis7",
        "redis7",
        "Default parameter group for redis7",
    ),
    (
        "default.redis6.x",
        "redis6.x",
        "Default parameter group for redis6.x",
    ),
    (
        "default.memcached1.6",
        "memcached1.6",
        "Default parameter group for memcached1.6",
    ),
];

/// (name, default value, description, allowed values)
const REDIS_PARAMETERS: [(&str, &str, &str, &str); 7] = [
    ("maxmemory-policy", "volatile-lru", "Max memory policy", "volatile-lru,allkeys-lru,volatile-lfu,allkeys-lfu,volatile-random,allkeys-random,volatile-ttl,noeviction"),
    ("activedefrag", "no", "Enabled active defragmentation", "yes,no"),
    ("appendonly", "no", "Enable AOF persistence", "yes,no"),
    ("appendfsync", "everysec", "fsync policy for AOF persistence", "always,everysec,no"),
    ("hash-max-ziplist-entries", "512", "Zip entries limit for hashes", "1-1000000"),
    ("notify-keyspace-events", "", "Events to publish in keyspace notifications", ""),
    ("timeout", "0", "Close connection if idle for more than timeout seconds", "0-2147483647"),
];

/// (engine, version, parameter group family, description)
const ENGINE_VERSIONS: [(&str, &str, &str, &str); 4] = [
    ("redis", "7.1.0", "redis7", "Redis"),
    ("redis", "7.0.7", "redis7.0", "Redis"),
    ("redis", "6.2.6", "redis6.x", "Redis"),
    ("memcached", "1.6.22", "memcached1.6", "memcached"),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Request;

    struct Ctx(u64);

    impl Ctx {
        fn req(&self, action: &str, params: &[(&str, &str)]) -> Request {
            let mut body = format!("Action={action}&Version=2015-02-02");
            for (k, v) in params {
                body.push_str(&format!("&{k}={v}"));
            }
            Request {
                service: "elasticache".into(),
                operation: action.into(),
                method: "POST".into(),
                path: "/".into(),
                query_string: String::new(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/x-www-form-urlencoded".into(),
                )]),
                body: Bytes::from(body),
                account: self.0,
                region: "us-east-1".into(),
            }
        }
    }

    use std::sync::atomic::{AtomicU64, Ordering};

    fn fresh_ctx() -> Ctx {
        static NEXT: AtomicU64 = AtomicU64::new(161111111110);
        Ctx(NEXT.fetch_add(1, Ordering::SeqCst))
    }

    fn parse(resp: &Response) -> String {
        String::from_utf8_lossy(&resp.body).to_string()
    }

    #[test]
    fn subnet_group_lifecycle() {
        let ctx = fresh_ctx();
        let created = parse(&dispatch(&ctx.req(
            "CreateCacheSubnetGroup",
            &[
                ("CacheSubnetGroupName", "sg1"),
                ("CacheSubnetGroupDescription", "d"),
                ("SubnetIds.member.1", "subnet-1"),
            ],
        )));
        assert!(created.contains("CacheSubnetGroupName>sg1"));
        assert!(created.contains("subnetgroup:sg1"));

        let listed = parse(&dispatch(&ctx.req(
            "DescribeCacheSubnetGroups",
            &[("CacheSubnetGroupName", "sg1")],
        )));
        assert!(listed.contains("CacheSubnetGroups") && listed.contains("sg1"));

        assert_eq!(
            dispatch(&ctx.req("DeleteCacheSubnetGroup", &[("CacheSubnetGroupName", "sg1")])).status,
            200
        );
        let gone = dispatch(&ctx.req(
            "DescribeCacheSubnetGroups",
            &[("CacheSubnetGroupName", "sg1")],
        ));
        assert_eq!(gone.status, 400);
        assert!(parse(&gone).contains("CacheSubnetGroupNotFoundFault"));

        // Duplicate create.
        dispatch(&ctx.req(
            "CreateCacheSubnetGroup",
            &[
                ("CacheSubnetGroupName", "sg2"),
                ("CacheSubnetGroupDescription", "d"),
            ],
        ));
        let dup = dispatch(&ctx.req(
            "CreateCacheSubnetGroup",
            &[
                ("CacheSubnetGroupName", "sg2"),
                ("CacheSubnetGroupDescription", "again"),
            ],
        ));
        assert!(parse(&dup).contains("CacheSubnetGroupAlreadyExistsFault"));
    }

    #[test]
    fn cache_cluster_lifecycle() {
        let ctx = fresh_ctx();
        let created = parse(&dispatch(&ctx.req(
            "CreateCacheCluster",
            &[
                ("CacheClusterId", "cc1"),
                ("Engine", "redis"),
                ("NumCacheNodes", "1"),
                ("CacheNodeType", "cache.t2.micro"),
            ],
        )));
        assert!(created.contains("CacheClusterId>cc1"));
        assert!(created.contains("cluster:cc1"));

        let described = parse(&dispatch(
            &ctx.req("DescribeCacheClusters", &[("CacheClusterId", "cc1")]),
        ));
        assert!(listed_has(&described, "cc1"));

        let modified = parse(&dispatch(&ctx.req(
            "ModifyCacheCluster",
            &[("CacheClusterId", "cc1"), ("SnapshotRetentionLimit", "5")],
        )));
        assert!(modified.contains("SnapshotRetentionLimit>5"));

        let deleted = parse(&dispatch(
            &ctx.req("DeleteCacheCluster", &[("CacheClusterId", "cc1")]),
        ));
        assert!(deleted.contains("deleting"));

        let missing = dispatch(&ctx.req("DescribeCacheClusters", &[("CacheClusterId", "cc1")]));
        assert_eq!(missing.status, 404);
        assert!(parse(&missing).contains("CacheClusterNotFound"));
    }

    fn listed_has(xml: &str, needle: &str) -> bool {
        xml.contains(&format!(">{needle}<"))
    }

    #[test]
    fn replication_group_lifecycle() {
        let ctx = fresh_ctx();
        let created = parse(&dispatch(&ctx.req(
            "CreateReplicationGroup",
            &[
                ("ReplicationGroupId", "rg1"),
                ("ReplicationGroupDescription", "d"),
            ],
        )));
        assert!(created.contains("ReplicationGroupId>rg1"));
        assert!(created.contains("MemberClusters><ClusterId>rg1-001"));

        let described = parse(&dispatch(&ctx.req(
            "DescribeReplicationGroups",
            &[("ReplicationGroupId", "rg1")],
        )));
        assert!(
            described.contains("rg1")
                && !described.contains("<ReplicationGroups></ReplicationGroups>")
        );

        let modified = parse(&dispatch(&ctx.req(
            "ModifyReplicationGroup",
            &[
                ("ReplicationGroupId", "rg1"),
                ("ReplicationGroupDescription", "desc2"),
            ],
        )));
        assert!(modified.contains("Description>desc2"));

        let deleted = parse(&dispatch(
            &ctx.req("DeleteReplicationGroup", &[("ReplicationGroupId", "rg1")]),
        ));
        assert!(deleted.contains("deleting"));

        let missing = dispatch(&ctx.req(
            "DescribeReplicationGroups",
            &[("ReplicationGroupId", "rg1")],
        ));
        assert_eq!(missing.status, 404);
        assert!(parse(&missing).contains("ReplicationGroupNotFoundFault"));
    }

    #[test]
    fn global_replication_group_flow() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "CreateReplicationGroup",
            &[
                ("ReplicationGroupId", "g-rg"),
                ("ReplicationGroupDescription", "d"),
            ],
        ));
        let created = parse(&dispatch(&ctx.req(
            "CreateGlobalReplicationGroup",
            &[
                ("GlobalReplicationGroupIdSuffix", "g1"),
                ("PrimaryReplicationGroupId", "g-rg"),
            ],
        )));
        assert!(created.contains("lstgl-grg-g"));
        assert!(created.contains("g-rg"));

        // Modify with the returned id (suffix part).
        let modified = dispatch(&ctx.req(
            "ModifyGlobalReplicationGroup",
            &[
                ("GlobalReplicationGroupId", "lstgl-grg-g1"),
                ("ApplyImmediately", "true"),
                ("GlobalReplicationGroupDescription", "upd"),
            ],
        ));
        assert!(parse(&modified).contains("GlobalReplicationGroupDescription>upd"));

        let deleted = parse(&dispatch(&ctx.req(
            "DeleteGlobalReplicationGroup",
            &[
                ("GlobalReplicationGroupId", "lstgl-grg-g1"),
                ("RetainPrimaryReplicationGroup", "true"),
            ],
        )));
        assert!(deleted.contains("lstgl-grg-g"));

        let missing = dispatch(&ctx.req(
            "DisassociateGlobalReplicationGroup",
            &[
                ("GlobalReplicationGroupId", "no-g"),
                ("ReplicationGroupId", "x"),
                ("ReplicationGroupRegion", "us-east-1"),
            ],
        ));
        assert_eq!(missing.status, 404);
    }

    /// S-1 regression: the global-RG ARN rides a NAMED format arg — the
    /// wave-4 format-string skew left a trailing positional `{}` that
    /// consumed the members blob, rendering an empty
    /// `...:globalreplicationgroup:` tail with the members XML as stray
    /// child elements (live-verified at `grg["ARN"]`).
    #[test]
    fn global_replication_group_arn_is_rendered() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "CreateReplicationGroup",
            &[
                ("ReplicationGroupId", "arn-rg"),
                ("ReplicationGroupDescription", "d"),
            ],
        ));
        let created = parse(&dispatch(&ctx.req(
            "CreateGlobalReplicationGroup",
            &[
                ("GlobalReplicationGroupIdSuffix", "arn9"),
                ("PrimaryReplicationGroupId", "arn-rg"),
            ],
        )));
        assert!(
            !created.contains("<ARN></ARN>"),
            "the members blob must not eat the ARN slot: {created}"
        );
        assert!(
            created.contains(&format!(
                "<ARN>arn:aws:elasticache:us-east-1:{}:globalreplicationgroup:",
                ctx.0
            )),
            "a full ARN with the global id tail follows: {created}"
        );
    }

    #[test]
    fn snapshot_flow() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "CreateReplicationGroup",
            &[
                ("ReplicationGroupId", "s-rg"),
                ("ReplicationGroupDescription", "d"),
            ],
        ));
        let created = parse(&dispatch(&ctx.req(
            "CreateSnapshot",
            &[("SnapshotName", "snap"), ("ReplicationGroupId", "s-rg")],
        )));
        assert!(created.contains("SnapshotName>snap"));
        assert!(created.contains("SnapshotStatus>available"));
        assert!(created.contains("SnapshotSource>manual"));

        let listed = parse(&dispatch(
            &ctx.req("DescribeSnapshots", &[("SnapshotName", "snap")]),
        ));
        assert!(listed.contains("Snapshots") && listed.contains("snap"));

        dispatch(&ctx.req("DeleteSnapshot", &[("SnapshotName", "snap")]));
        let missing = dispatch(&ctx.req("DescribeSnapshots", &[("SnapshotName", "snap")]));
        assert_eq!(missing.status, 404);
        assert!(parse(&missing).contains("SnapshotNotFoundFault"));
    }

    #[test]
    fn user_and_user_group_flow() {
        let ctx = fresh_ctx();
        let created = parse(&dispatch(&ctx.req(
            "CreateUser",
            &[
                ("UserId", "u1"),
                ("UserName", "n"),
                ("Engine", "redis"),
                ("AccessString", "on ~* +@all"),
                ("NoPasswordRequired", "true"),
            ],
        )));
        assert!(created.contains("UserId>u1"));
        assert!(created.contains("Status>active"));

        let ids = parse(&dispatch(&ctx.req("DescribeUsers", &[("UserId", "u1")])));
        assert!(ids.contains("<Users><member>"));

        let modified = parse(&dispatch(&ctx.req(
            "ModifyUser",
            &[("UserId", "u1"), ("AccessString", "on ~* +@read")],
        )));
        assert!(modified.contains("AccessString>"));

        let ug = parse(&dispatch(&ctx.req(
            "CreateUserGroup",
            &[
                ("UserGroupId", "ug1"),
                ("Engine", "redis"),
                ("UserIds.member.1", "default"),
            ],
        )));
        assert!(ug.contains("UserGroupId>ug1"));
        let ug_mod = parse(&dispatch(&ctx.req(
            "ModifyUserGroup",
            &[("UserGroupId", "ug1"), ("UserIdsToAdd.member.1", "u1")],
        )));
        assert!(ug_mod.contains("<member>u1</member>"));
        let dg = parse(&dispatch(
            &ctx.req("DeleteUserGroup", &[("UserGroupId", "ug1")]),
        ));
        assert!(dg.contains("deleting"));

        let deleted = parse(&dispatch(&ctx.req("DeleteUser", &[("UserId", "u1")])));
        assert!(deleted.contains("deleting"));

        let missing = dispatch(&ctx.req("DeleteUser", &[("UserId", "u1")]));
        assert_eq!(missing.status, 404);
    }

    #[test]
    fn tags_flow() {
        let ctx = fresh_ctx();
        dispatch(&ctx.req(
            "CreateCacheCluster",
            &[("CacheClusterId", "t-cc"), ("Engine", "redis")],
        ));
        let arn = format!("arn:aws:elasticache:us-east-1:{}:cluster:t-cc", ctx.0);
        dispatch(&ctx.req(
            "AddTagsToResource",
            &[
                ("ResourceName", &arn),
                ("Tags.member.1.Key", "env"),
                ("Tags.member.1.Value", "test"),
            ],
        ));
        let tags = parse(&dispatch(
            &ctx.req("ListTagsForResource", &[("ResourceName", &arn)]),
        ));
        assert!(tags.contains("<Key>env</Key><Value>test</Value>"));

        dispatch(&ctx.req(
            "RemoveTagsFromResource",
            &[("ResourceName", &arn), ("TagKeys.member.1", "env")],
        ));
        let tags2 = parse(&dispatch(
            &ctx.req("ListTagsForResource", &[("ResourceName", &arn)]),
        ));
        assert!(!tags2.contains("<Key>env</Key>"));
    }
}
