//! AWS Direct Connect backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `directconnect`
//! (`X-Amz-Target: DirectConnect_20121206.<Op>` on
//! `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! moto's `DirectConnectBackend` (moto/directconnect): connections, LAGs and
//! a tagger with moto's `TaggingService` semantics.
//!
//! The deployed moto fork's surface for this service is 11 handlers —
//! DescribeConnections, CreateConnection, DeleteConnection, UpdateConnection,
//! AssociateMacSecKey, DisassociateMacSecKey, CreateLag, DescribeLags,
//! TagResource, UntagResource, DescribeTags — all implemented 1:1 here,
//! including the fork's quirks:
//!
//! - connection/LAG ids are ARN-flavoured (`dx-con/dx-moto-<name>-<ts>`,
//!   `dxlag/dxlag-moto-<name>-<ts>`, `%Y%m%d%H%M%S` timestamps),
//! - static mock fields (`awsDevice`: `mock_device`, `partnerName`:
//!   `mock_partner`, `portEncryptionStatus`: `Encryption Down`, `vlan`: 0),
//! - `DeleteConnection` marks the record `deleted` instead of dropping it,
//! - `CreateLag` spawns `numberOfConnections` child connections named
//!   `Requested Connection <n> for Lag <lag id>` (also registered at the
//!   top level); `CreateLag` with a `connectionId` is moto's unguarded
//!   `NotImplementedError` crash → a 500 `InternalError`,
//! - `MacSecKey` records render `{secretARN, ckn, state, startOn}` (never
//!   `cak`); a missing `secretARN` coalesces to `mock_secret_arn`,
//! - associating a MacSec key with a LAG id (`dxlag-`) keys the LAG and
//!   re-binds each member connection's key list to the LAG's — the visible
//!   effect of the fork's shared-list aliasing,
//! - `MacSecKeyNotFound` only surfaces while keys remain; an empty key list
//!   (or unknown id, whose lookup hides behind an empty list) is moto's
//!   `ConnectionNotFound`.
//!
//! The remaining botocore operations (`wave 12`) are armed here against the
//! botocore wire model alone (the moto fork is absent for them). State is
//! the same per-(account, region) store extended with interconnects, virtual
//! interfaces, Direct Connect gateways/associations/proposals/attachments,
//! BGP failover tests and stored customer agreements. Ids stay ARN-flavoured,
//! deterministic `{dx-moto|dxgw-moto|dxassoc-moto|dxproposal-moto|…}-<name>-<ts>`
//! with `%Y%m%d%H%M%S` timestamps. Deterministic model choices (no async in
//! the twin): virtual interfaces are created `pending` (public: `verifying`)
//! and move to `available` on their Confirm* call; associations are created
//! `associated` and answer `disassociating` on delete; proposals are created
//! `requested` and move to `accepted`/`deleted`; BGP failover tests start
//! `running` and stop `stopped` with the virtual interface state moving
//! `available`→`testing`→`available`. The botocore-declared
//! `DuplicateTagKeysException`/`TooManyTagsException` never fire for the
//! connection family (the fork's TaggingService overwrites duplicate keys),
//! so the created families follow moto's silent-overwrite tagger too.
//! `loaIssueTime`/`startOn` mirror moto's `%Y-%m-%d %H:%M:%S` strings; UTC
//! is used where moto uses naive local time (containers run UTC).
//!
//! Contract: `tests/compatibility/test_directconnect_compat.py` (the 11
//! fork-handler operations) plus unit tests below.

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, DxStore>>>;

fn stores() -> Stores {
    store::slot::<DxStore>("directconnect")
}

/// Per-(account, region) in-memory state for Direct Connect.
#[derive(Default)]
struct DxStore {
    /// connection id -> connection
    connections: BTreeMap<String, Connection>,
    /// lag id -> lag
    lags: BTreeMap<String, Lag>,
    /// arn -> {tag key: value} — moto's `TaggingService`
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
    /// interconnect id -> interconnect
    interconnects: BTreeMap<String, Interconnect>,
    /// virtual interface id -> virtual interface
    virtual_interfaces: BTreeMap<String, VirtualInterfaceRecord>,
    /// direct connect gateway id -> gateway
    gateways: BTreeMap<String, DxGateway>,
    /// association id -> association
    associations: BTreeMap<String, DxAssociation>,
    /// proposal id -> association proposal
    proposals: BTreeMap<String, DxProposal>,
    /// `<gateway id>\u{1f}<virtualInterface id>` -> attachment
    attachments: BTreeMap<String, DxAttachment>,
    /// test id -> BGP failover test history row
    tests: BTreeMap<String, ViTest>,
    /// agreement name -> status (ConfirmCustomerAgreement rows)
    agreements: BTreeMap<String, String>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut DxStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// One MacSec key (fork's `MacSecKey` dataclass minus the never-rendered
/// `cak` member).
#[derive(Clone)]
struct MacSecKey {
    secret_arn: String,
    ckn: String,
    state: String,
    start_on: String,
}

impl MacSecKey {
    /// The mock pair moto's `CreateConnection`/`CreateLag` plant.
    fn default_mock() -> Self {
        MacSecKey {
            secret_arn: "mock_secret_arn".to_string(),
            ckn: "mock_ckn".to_string(),
            state: "associated".to_string(),
            start_on: dx_time(),
        }
    }

    /// moto's `to_dict`: falsy `secret_arn`/`ckn` coalesce to `""`; `cak`
    /// stays off the wire.
    fn to_dict(&self) -> Value {
        json!({
            "secretARN": self.secret_arn,
            "ckn": self.ckn,
            "state": self.state,
            "startOn": self.start_on,
        })
    }
}

/// A Direct Connect connection (fork's `Connection` dataclass).
#[derive(Clone)]
struct Connection {
    connection_id: String,
    connection_name: String,
    connection_state: String,
    bandwidth: String,
    location: String,
    lag_id: Option<String>,
    loa_issue_time: String,
    provider_name: Option<String>,
    /// Rendered verbatim — `null` unless the caller sent `requestMACSec`.
    mac_sec_capable: Value,
    encryption_mode: String,
    mac_sec_keys: Vec<MacSecKey>,
    region: String,
    /// Hosted/interconnect-allocated connections carry the owner account they
    /// were allocated to and the id of the connection/interconnect hosting
    /// them (`AllocateHostedConnection`/`AllocateConnectionOnInterconnect`).
    owner_account: Option<String>,
    parent_id: Option<String>,
    /// The caller's `vlan` member (hosted allocations); `None` renders the
    /// fork's static `0`.
    vlan: Option<i64>,
}

/// The MacSec key list a connection renders: LAG members show the LAG's
/// shared list (moto re-binds each member's list to the LAG's).
fn lag_member_keys<'a>(st: &'a DxStore, conn: &'a Connection) -> &'a [MacSecKey] {
    if let Some(lag_id) = &conn.lag_id {
        if let Some(lag) = st.lags.get(lag_id) {
            if lag.connections.contains(&conn.connection_id) {
                return &lag.mac_sec_keys;
            }
        }
    }
    &conn.mac_sec_keys
}

/// moto's `Connection.to_dict()`: the full record, null members included.
/// Member connections of a LAG render the LAG's MacSec key list — moto
/// re-binds each member's list to the LAG's shared list.
fn connection_dict(st: &DxStore, conn: &Connection) -> Value {
    let mac_sec_keys = lag_member_keys(st, conn)
        .iter()
        .map(MacSecKey::to_dict)
        .collect::<Vec<_>>();
    let mut dict = json!({
        "awsDevice": "mock_device",
        "awsDeviceV2": "mock_device_v2",
        "awsLogicalDeviceId": "mock_logical_device_id",
        "bandwidth": conn.bandwidth,
        "connectionId": conn.connection_id,
        "connectionName": conn.connection_name,
        "connectionState": conn.connection_state,
        "encryptionMode": conn.encryption_mode,
        "hasLogicalRedundancy": false,
        "jumboFrameCapable": false,
        "lagId": conn.lag_id,
        "loaIssueTime": conn.loa_issue_time,
        "location": conn.location,
        "macSecCapable": conn.mac_sec_capable,
        "macSecKeys": mac_sec_keys,
        "partnerName": "mock_partner",
        "portEncryptionStatus": "Encryption Down",
        "providerName": conn.provider_name,
        "region": conn.region,
        "tags": list_tags_for_resource(st, &conn.connection_id),
        "vlan": conn.vlan.unwrap_or(0),
    });
    // Hosted/allocated connections expose their owner like botocore's
    // `ownerAccount` member; the fork's own connections never carry it.
    if let Some(owner) = &conn.owner_account {
        dict["ownerAccount"] = json!(owner);
    }
    dict
}

/// A Direct Connect LAG (fork's `LAG` dataclass). Member connections are
/// stored by id: moto's python LAG keeps shared object references, so every
/// render reads the member mutating through the connections map.
#[derive(Clone)]
struct Lag {
    lag_id: String,
    lag_name: String,
    lag_state: String,
    connections_bandwidth: String,
    number_of_connections: i64,
    minimum_links: i64,
    connections: Vec<String>,
    location: String,
    provider_name: Option<String>,
    mac_sec_capable: Value,
    encryption_mode: String,
    mac_sec_keys: Vec<MacSecKey>,
    region: String,
}

/// moto's `LAG.to_dict()`.
fn lag_dict(st: &DxStore, lag: &Lag) -> Value {
    json!({
        "awsDevice": "mock_device",
        "awsDeviceV2": "mock_device_v2",
        "awsLogicalDeviceId": "mock_logical_device_id",
        "connectionsBandwidth": lag.connections_bandwidth,
        "numberOfConnections": lag.number_of_connections,
        "minimumLinks": lag.minimum_links,
        "connections": lag
            .connections
            .iter()
            .filter_map(|id| st.connections.get(id))
            .map(|c| connection_dict(st, c))
            .collect::<Vec<_>>(),
        "lagId": lag.lag_id,
        "lagName": lag.lag_name,
        "lagState": lag.lag_state,
        "encryptionMode": lag.encryption_mode,
        "hasLogicalRedundancy": false,
        "jumboFrameCapable": false,
        "location": lag.location,
        "macSecCapable": lag.mac_sec_capable,
        "macSecKeys": lag
            .mac_sec_keys
            .iter()
            .map(MacSecKey::to_dict)
            .collect::<Vec<_>>(),
        "providerName": lag.provider_name,
        "region": lag.region,
        "tags": list_tags_for_resource(st, &lag.lag_id),
    })
}

// ---------------------------------------------------------------------------
// wave-12 records — interconnects, virtual interfaces, gateways
// ---------------------------------------------------------------------------

/// A Direct Connect interconnect (botocore `Interconnect` shape).
#[derive(Clone)]
struct Interconnect {
    interconnect_id: String,
    interconnect_name: String,
    interconnect_state: String,
    bandwidth: String,
    location: String,
    lag_id: Option<String>,
    provider_name: Option<String>,
    mac_sec_capable: Value,
    encryption_mode: String,
    mac_sec_keys: Vec<MacSecKey>,
    region: String,
}

/// botocore `Interconnect` rendering, statics shared with the fork's
/// connections (`mock_device` et al).
fn interconnect_dict(st: &DxStore, ic: &Interconnect) -> Value {
    json!({
        "awsDevice": "mock_device",
        "awsDeviceV2": "mock_device_v2",
        "awsLogicalDeviceId": "mock_logical_device_id",
        "bandwidth": ic.bandwidth,
        "encryptionMode": ic.encryption_mode,
        "hasLogicalRedundancy": "unknown",
        "interconnectId": ic.interconnect_id,
        "interconnectName": ic.interconnect_name,
        "interconnectState": ic.interconnect_state,
        "jumboFrameCapable": false,
        "lagId": ic.lag_id,
        "loaIssueTime": dx_time(),
        "location": ic.location,
        "macSecCapable": ic.mac_sec_capable,
        "macSecKeys": ic.mac_sec_keys.iter().map(MacSecKey::to_dict).collect::<Vec<_>>(),
        "portEncryptionStatus": "Encryption Down",
        "providerName": ic.provider_name,
        "region": ic.region,
        "tags": list_tags_for_resource(st, &ic.interconnect_id),
    })
}

/// One BGP peer on a virtual interface (botocore `BGPPeer`).
#[derive(Clone)]
struct BgpPeer {
    bgp_peer_id: String,
    asn: Option<i64>,
    asn_long: Option<i64>,
    auth_key: Option<String>,
    address_family: Option<String>,
    amazon_address: Option<String>,
    customer_address: Option<String>,
    bgp_peer_state: String,
    bgp_status: String,
}

fn bgp_peer_dict(peer: &BgpPeer) -> Value {
    json!({
        "asn": peer.asn,
        "asnLong": peer.asn_long,
        "addressFamily": peer.address_family,
        "amazonAddress": peer.amazon_address,
        "authKey": peer.auth_key,
        "awsDeviceV2": "mock_device_v2",
        "awsLogicalDeviceId": "mock_logical_device_id",
        "bgpPeerId": peer.bgp_peer_id,
        "bgpPeerState": peer.bgp_peer_state,
        "bgpStatus": peer.bgp_status,
        "customerAddress": peer.customer_address,
    })
}

/// A virtual interface (botocore `VirtualInterface`).
#[derive(Clone)]
struct VirtualInterfaceRecord {
    virtual_interface_id: String,
    virtual_interface_name: String,
    /// private | public | transit
    virtual_interface_type: String,
    virtual_interface_state: String,
    connection_id: String,
    location: String,
    vlan: Option<i64>,
    asn: Option<i64>,
    asn_long: Option<i64>,
    amazon_side_asn: Option<i64>,
    auth_key: Option<String>,
    amazon_address: Option<String>,
    customer_address: Option<String>,
    address_family: Option<String>,
    mtu: Option<i64>,
    virtual_gateway_id: Option<String>,
    direct_connect_gateway_id: Option<String>,
    route_filter_prefixes: Vec<String>,
    bgp_peers: Vec<BgpPeer>,
    site_link_enabled: Option<bool>,
    owner_account: Option<String>,
    region: String,
}

/// The mock customer router document `DescribeRouterConfiguration` serves.
const ROUTER_VENDOR: &str = "Cisco";
const ROUTER_PLATFORM: &str = "2921";
const ROUTER_SOFTWARE: &str = "IOS";

fn customer_router_config() -> String {
    format!(
        "<customerRouterConfig><vendor>{ROUTER_VENDOR}</vendor>\
<platform>{ROUTER_PLATFORM}</platform><software>{ROUTER_SOFTWARE}</software></customerRouterConfig>"
    )
}

fn virtual_interface_dict(st: &DxStore, vif: &VirtualInterfaceRecord) -> Value {
    json!({
        "asn": vif.asn,
        "asnLong": vif.asn_long,
        "amazonAddress": vif.amazon_address,
        "amazonSideAsn": vif.amazon_side_asn,
        "addressFamily": vif.address_family,
        "authKey": vif.auth_key,
        "awsDeviceV2": "mock_device_v2",
        "awsLogicalDeviceId": "mock_logical_device_id",
        "bgpPeers": vif.bgp_peers.iter().map(bgp_peer_dict).collect::<Vec<_>>(),
        "connectionId": vif.connection_id,
        "customerAddress": vif.customer_address,
        "customerRouterConfig": customer_router_config(),
        "directConnectGatewayId": vif.direct_connect_gateway_id,
        "jumboFrameCapable": false,
        "location": vif.location,
        "mtu": vif.mtu,
        "ownerAccount": vif.owner_account,
        "region": vif.region,
        "routeFilterPrefixes": vif.route_filter_prefixes.iter()
            .map(|cidr| json!({ "cidr": cidr }))
            .collect::<Vec<_>>(),
        "siteLinkEnabled": vif.site_link_enabled,
        "tags": list_tags_for_resource(st, &vif.virtual_interface_id),
        "virtualGatewayId": vif.virtual_gateway_id,
        "virtualInterfaceId": vif.virtual_interface_id,
        "virtualInterfaceName": vif.virtual_interface_name,
        "virtualInterfaceState": vif.virtual_interface_state,
        "virtualInterfaceType": vif.virtual_interface_type,
        "vlan": vif.vlan,
    })
}

/// One `DescribeDirectConnectGatewayAttachment` row.
#[derive(Clone)]
struct DxAttachment {
    direct_connect_gateway_id: String,
    virtual_interface_id: String,
    virtual_interface_region: String,
    virtual_interface_owner_account: String,
    attachment_state: String,
    attachment_type: String,
}

fn attachment_dict(a: &DxAttachment) -> Value {
    json!({
        "attachmentState": a.attachment_state,
        "attachmentType": a.attachment_type,
        "directConnectGatewayId": a.direct_connect_gateway_id,
        "stateChangeError": Value::Null,
        "virtualInterfaceId": a.virtual_interface_id,
        "virtualInterfaceOwnerAccount": a.virtual_interface_owner_account,
        "virtualInterfaceRegion": a.virtual_interface_region,
    })
}

/// The `AssociatedGateway` facet shared by associations and proposals.
#[derive(Clone)]
struct AssociatedGateway {
    id: String,
    /// virtualPrivateGateway | transitGateway — inferred from the `tgw-` id
    /// prefix (the twin does not model the EC2/VPC side of these gateways).
    type_: String,
    owner_account: String,
    region: String,
}

fn associated_gateway_dict(g: &AssociatedGateway) -> Value {
    json!({
        "id": g.id,
        "ownerAccount": g.owner_account,
        "region": g.region,
        "type": g.type_,
    })
}

fn gateway_type_for(gateway_id: &str) -> String {
    if gateway_id.starts_with("tgw-") {
        "transitGateway".to_string()
    } else {
        "virtualPrivateGateway".to_string()
    }
}

/// A Direct Connect gateway (botocore `DirectConnectGateway`).
#[derive(Clone)]
struct DxGateway {
    direct_connect_gateway_id: String,
    direct_connect_gateway_name: String,
    amazon_side_asn: Option<i64>,
    direct_connect_gateway_state: String,
    owner_account: String,
}

fn gateway_dict(st: &DxStore, gw: &DxGateway) -> Value {
    json!({
        "amazonSideAsn": gw.amazon_side_asn,
        "directConnectGatewayId": gw.direct_connect_gateway_id,
        "directConnectGatewayName": gw.direct_connect_gateway_name,
        "directConnectGatewayState": gw.direct_connect_gateway_state,
        "ownerAccount": gw.owner_account,
        "stateChangeError": Value::Null,
        "tags": list_tags_for_resource(st, &gw.direct_connect_gateway_id),
    })
}

/// A Direct Connect gateway association (botocore `DirectConnectGatewayAssociation`).
#[derive(Clone)]
struct DxAssociation {
    association_id: String,
    direct_connect_gateway_id: String,
    direct_connect_gateway_owner_account: String,
    association_state: String,
    associated_gateway: AssociatedGateway,
    allowed_prefixes: Vec<String>,
    /// Set when the association is via a classic virtual private gateway;
    /// transit/private gateways come through `associated_gateway` alone.
    virtual_gateway_id: Option<String>,
    virtual_gateway_region: Option<String>,
}

fn association_dict(a: &DxAssociation) -> Value {
    json!({
        "allowedPrefixesToDirectConnectGateway": a.allowed_prefixes.iter()
            .map(|cidr| json!({ "cidr": cidr }))
            .collect::<Vec<_>>(),
        "associationId": a.association_id,
        "associationState": a.association_state,
        "associatedGateway": associated_gateway_dict(&a.associated_gateway),
        "directConnectGatewayId": a.direct_connect_gateway_id,
        "directConnectGatewayOwnerAccount": a.direct_connect_gateway_owner_account,
        "stateChangeError": Value::Null,
        "virtualGatewayId": a.virtual_gateway_id,
        "virtualGatewayOwnerAccount": a.associated_gateway.owner_account,
        "virtualGatewayRegion": a.virtual_gateway_region,
    })
}

/// A Direct Connect gateway association proposal (botocore
/// `DirectConnectGatewayAssociationProposal`).
#[derive(Clone)]
struct DxProposal {
    proposal_id: String,
    /// requested | accepted | deleted
    proposal_state: String,
    direct_connect_gateway_id: String,
    direct_connect_gateway_owner_account: String,
    associated_gateway: AssociatedGateway,
    requested_allowed_prefixes: Vec<String>,
}

fn proposal_dict(p: &DxProposal) -> Value {
    json!({
        "directConnectGatewayId": p.direct_connect_gateway_id,
        "directConnectGatewayOwnerAccount": p.direct_connect_gateway_owner_account,
        "existingAllowedPrefixesToDirectConnectGateway": json!([]),
        "proposalId": p.proposal_id,
        "proposalState": p.proposal_state,
        "requestedAllowedPrefixesToDirectConnectGateway": p.requested_allowed_prefixes.iter()
            .map(|cidr| json!({ "cidr": cidr }))
            .collect::<Vec<_>>(),
        "associatedGateway": associated_gateway_dict(&p.associated_gateway),
    })
}

/// One BGP failover test history row (botocore `VirtualInterfaceTestHistory`).
#[derive(Clone)]
struct ViTest {
    test_id: String,
    virtual_interface_id: String,
    bgp_peers: Vec<String>,
    status: String,
    owner_account: String,
    test_duration_in_minutes: i64,
    start_time: String,
    end_time: String,
}

fn test_dict(t: &ViTest) -> Value {
    json!({
        "bgpPeers": t.bgp_peers,
        "endTime": t.end_time,
        "ownerAccount": t.owner_account,
        "startTime": t.start_time,
        "status": t.status,
        "testDurationInMinutes": t.test_duration_in_minutes,
        "testId": t.test_id,
        "virtualInterfaceId": t.virtual_interface_id,
    })
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

/// Operations the backend answers — the full botocore surface (63 ops): the
/// 11 fork handlers plus the wave-12 arms below.
pub const OPERATIONS: [&str; 63] = [
    "DescribeConnections",
    "CreateConnection",
    "DeleteConnection",
    "UpdateConnection",
    "AssociateMacSecKey",
    "DisassociateMacSecKey",
    "CreateLag",
    "DescribeLags",
    "TagResource",
    "UntagResource",
    "DescribeTags",
    "AcceptDirectConnectGatewayAssociationProposal",
    "AllocateConnectionOnInterconnect",
    "AllocateHostedConnection",
    "AllocatePrivateVirtualInterface",
    "AllocatePublicVirtualInterface",
    "AllocateTransitVirtualInterface",
    "AssociateConnectionWithLag",
    "AssociateHostedConnection",
    "AssociateVirtualInterface",
    "ConfirmConnection",
    "ConfirmCustomerAgreement",
    "ConfirmPrivateVirtualInterface",
    "ConfirmPublicVirtualInterface",
    "ConfirmTransitVirtualInterface",
    "CreateBGPPeer",
    "CreateDirectConnectGateway",
    "CreateDirectConnectGatewayAssociation",
    "CreateDirectConnectGatewayAssociationProposal",
    "CreateInterconnect",
    "CreatePrivateVirtualInterface",
    "CreatePublicVirtualInterface",
    "CreateTransitVirtualInterface",
    "DeleteBGPPeer",
    "DeleteDirectConnectGateway",
    "DeleteDirectConnectGatewayAssociation",
    "DeleteDirectConnectGatewayAssociationProposal",
    "DeleteInterconnect",
    "DeleteLag",
    "DeleteVirtualInterface",
    "DescribeConnectionLoa",
    "DescribeConnectionsOnInterconnect",
    "DescribeCustomerMetadata",
    "DescribeDirectConnectGatewayAssociationProposals",
    "DescribeDirectConnectGatewayAssociations",
    "DescribeDirectConnectGatewayAttachments",
    "DescribeDirectConnectGateways",
    "DescribeHostedConnections",
    "DescribeInterconnectLoa",
    "DescribeInterconnects",
    "DescribeLoa",
    "DescribeLocations",
    "DescribeRouterConfiguration",
    "DescribeVirtualGateways",
    "DescribeVirtualInterfaces",
    "DisassociateConnectionFromLag",
    "ListVirtualInterfaceTestHistory",
    "StartBgpFailoverTest",
    "StopBgpFailoverTest",
    "UpdateDirectConnectGateway",
    "UpdateDirectConnectGatewayAssociation",
    "UpdateLag",
    "UpdateVirtualInterfaceAttributes",
];

/// True when `operation` is a Direct Connect operation.
pub fn handles_operation(operation: &str) -> bool {
    OPERATIONS.contains(&operation)
}

pub fn dispatch(req: &Request) -> Option<Response> {
    // The gateway router maps directconnect's botocore targetPrefix
    // `OvertureService` to `support` (a python-era registry row that
    // predates this backend), so Direct Connect requests arrive labeled
    // `support` with one of `OPERATIONS` — support's own operations are
    // disjoint. The crate dispatcher consults `handles_operation`.
    match req.service.as_str() {
        "directconnect" => Some(with_store(req, |st| route(req, st))),
        "support" if handles_operation(req.operation.as_str()) => {
            Some(with_store(req, |st| route(req, st)))
        }
        _ => None,
    }
}

fn route(req: &Request, st: &mut DxStore) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        // --- connections ---------------------------------------------------------
        "DescribeConnections" => describe_connections(st, &body, &req.region),
        "CreateConnection" => {
            let conn = create_connection_inner(st, req.account, &req.region, &body, None);
            ok(connection_dict(st, &conn))
        }
        "DeleteConnection" => delete_connection(st, &body, &req.region),
        "UpdateConnection" => update_connection(st, &body, &req.region),
        "AssociateMacSecKey" => associate_mac_sec_key(st, &body, &req.region),
        "DisassociateMacSecKey" => disassociate_mac_sec_key(st, &body, &req.region),
        // --- lags ---------------------------------------------------------------
        "CreateLag" => create_lag(st, req.account, &req.region, &body),
        "DescribeLags" => describe_lags(st, &body, &req.region),
        // --- tags -----------------------------------------------------------------
        "TagResource" => {
            let tags = body
                .get("tags")
                .and_then(Value::as_array)
                .filter(|t| !t.is_empty())
                .cloned();
            tag_resource(
                st,
                string_at(&body, "resourceArn").unwrap_or(""),
                tags.as_ref(),
            );
            ok_empty()
        }
        "UntagResource" => {
            let keys = str_list(body.get("tagKeys"));
            untag_resource(st, string_at(&body, "resourceArn").unwrap_or(""), &keys);
            ok_empty()
        }
        "DescribeTags" => describe_tags(st, &body),
        // --- wave 12: interconnects ----------------------------------------
        "CreateInterconnect" => create_interconnect(st, req.account, &req.region, &body),
        "DescribeInterconnects" => describe_interconnects(st, &body, &req.region),
        "DeleteInterconnect" => delete_interconnect(st, &body, &req.region),
        "DescribeConnectionsOnInterconnect" => {
            describe_connections_on_interconnect(st, &body, &req.region)
        }
        "AllocateConnectionOnInterconnect" => {
            allocate_connection_on_interconnect(st, req.account, &req.region, &body)
        }
        "DescribeInterconnectLoa" => describe_interconnect_loa(st, &body, &req.region),
        // --- wave 12: virtual interfaces ------------------------------------
        "CreatePrivateVirtualInterface" | "CreatePublicVirtualInterface"
        | "CreateTransitVirtualInterface" | "AllocatePrivateVirtualInterface"
        | "AllocatePublicVirtualInterface" | "AllocateTransitVirtualInterface" => {
            create_or_allocate_virtual_interface(st, req.account, req, &body)
        }
        "ConfirmConnection" => confirm_connection(st, &body, &req.region),
        "ConfirmPrivateVirtualInterface" | "ConfirmPublicVirtualInterface"
        | "ConfirmTransitVirtualInterface" => confirm_virtual_interface(st, &body),
        "DescribeVirtualInterfaces" => describe_virtual_interfaces(st, &body),
        "DeleteVirtualInterface" => delete_virtual_interface(st, &body),
        "AssociateVirtualInterface" => associate_virtual_interface(st, &body, &req.region),
        "UpdateVirtualInterfaceAttributes" => update_virtual_interface_attributes(st, &body),
        "CreateBGPPeer" => create_bgp_peer(st, &body),
        "DeleteBGPPeer" => delete_bgp_peer(st, &body),
        "StartBgpFailoverTest" => start_bgp_failover_test(st, req.account, &body),
        "StopBgpFailoverTest" => stop_bgp_failover_test(st, &body),
        "ListVirtualInterfaceTestHistory" => list_virtual_interface_test_history(st, &body),
        "DescribeRouterConfiguration" => describe_router_configuration(st, &body),
        // --- wave 12: Direct Connect gateways --------------------------------
        "CreateDirectConnectGateway" => create_direct_connect_gateway(st, req.account, &body),
        "DeleteDirectConnectGateway" => delete_direct_connect_gateway(st, &body),
        "DescribeDirectConnectGateways" => describe_direct_connect_gateways(st, &body),
        "UpdateDirectConnectGateway" => update_direct_connect_gateway(st, &body),
        "CreateDirectConnectGatewayAssociation" => {
            create_direct_connect_gateway_association(st, req.account, &req.region, &body)
        }
        "DeleteDirectConnectGatewayAssociation" => {
            delete_direct_connect_gateway_association(st, &body)
        }
        "DescribeDirectConnectGatewayAssociations" => {
            describe_direct_connect_gateway_associations(st, &body)
        }
        "UpdateDirectConnectGatewayAssociation" => {
            update_direct_connect_gateway_association(st, &body)
        }
        "CreateDirectConnectGatewayAssociationProposal" => {
            create_direct_connect_gateway_association_proposal(
                st,
                req.account,
                &req.region,
                &body,
            )
        }
        "AcceptDirectConnectGatewayAssociationProposal" => {
            accept_direct_connect_gateway_association_proposal(st, &body, &req.region)
        }
        "DescribeDirectConnectGatewayAssociationProposals" => {
            describe_direct_connect_gateway_association_proposals(st, &body)
        }
        "DeleteDirectConnectGatewayAssociationProposal" => {
            delete_direct_connect_gateway_association_proposal(st, &body)
        }
        "DescribeDirectConnectGatewayAttachments" => {
            describe_direct_connect_gateway_attachments(st, &body)
        }
        // --- wave 12: LAG extras + hosted connections --------------------------
        "DeleteLag" => delete_lag(st, &body, &req.region),
        "UpdateLag" => update_lag(st, &body, &req.region),
        "AssociateConnectionWithLag" => associate_connection_with_lag(st, &body, &req.region),
        "DisassociateConnectionFromLag" => {
            disassociate_connection_from_lag(st, &body, &req.region)
        }
        "DescribeHostedConnections" => describe_hosted_connections(st, &body, &req.region),
        "AllocateHostedConnection" => allocate_hosted_connection(st, req.account, &req.region, &body),
        "AssociateHostedConnection" => associate_hosted_connection(st, &body, &req.region),
        // --- wave 12: metadata, agreements, locations, Loa ---------------------
        "DescribeLocations" => describe_locations(),
        "DescribeVirtualGateways" => describe_virtual_gateways(),
        "DescribeCustomerMetadata" => describe_customer_metadata(st),
        "ConfirmCustomerAgreement" => confirm_customer_agreement(st, &body),
        "DescribeLoa" => describe_loa(st, &body, &req.region),
        "DescribeConnectionLoa" => describe_connection_loa(st, &body, &req.region),
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Operation helpers
// ---------------------------------------------------------------------------

/// moto's `describe_connections`: unknown filter id raises, empty stays all.
fn describe_connections(st: &DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        let conns: Vec<Value> = st
            .connections
            .values()
            .map(|c| connection_dict(st, c))
            .collect();
        return ok(json!({ "connections": conns }));
    };
    match st.connections.get(connection_id) {
        Some(conn) => ok(json!({ "connections": [connection_dict(st, conn)] })),
        None => connection_not_found(connection_id, region),
    }
}

/// Build a connection as the fork's `create_connection` does; `lag_id` is
/// only set by `CreateLag`'s child spawns. Tags land in the store when the
/// member is truthy, exactly like moto's `if tags:` guard.
fn build_connection(
    st: &mut DxStore,
    account: u64,
    region: &str,
    params: &Value,
    lag_id: Option<String>,
) -> Connection {
    let connection_name = string_at(params, "connectionName")
        .unwrap_or("")
        .to_string();
    let request_mac_sec = params.get("requestMACSec").cloned().unwrap_or(Value::Null);
    let (encryption_mode, mac_sec_keys) = if truthy(&request_mac_sec) {
        ("must_encrypt".to_string(), vec![MacSecKey::default_mock()])
    } else {
        ("no_encrypt".to_string(), Vec::new())
    };
    let connection = Connection {
        connection_id: dx_arn(
            region,
            account,
            "dx-con",
            &format!("dx-moto-{connection_name}-{}", dx_time_ts()),
        ),
        connection_name,
        connection_state: "available".to_string(),
        bandwidth: string_at(params, "bandwidth").unwrap_or("").to_string(),
        location: string_at(params, "location").unwrap_or("").to_string(),
        lag_id,
        loa_issue_time: dx_time(),
        provider_name: params
            .get("providerName")
            .and_then(Value::as_str)
            .map(String::from),
        // moto params.get(...) — absent/explicit-null renders `null`.
        mac_sec_capable: if request_mac_sec.is_null() {
            Value::Null
        } else {
            request_mac_sec
        },
        encryption_mode,
        mac_sec_keys,
        region: region.to_string(),
        owner_account: None,
        parent_id: None,
        vlan: None,
    };
    if let Some(tags) = truthy_array(params.get("tags")) {
        tag_resource(st, &connection.connection_id, Some(tags));
    }
    connection
}

/// moto's `create_connection` — the record joins the shared dict.
fn create_connection_inner(
    st: &mut DxStore,
    account: u64,
    region: &str,
    body: &Value,
    lag_id: Option<String>,
) -> Connection {
    let conn = build_connection(st, account, region, body, lag_id);
    st.connections
        .insert(conn.connection_id.clone(), conn.clone());
    conn
}

fn delete_connection(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = string_at(body, "connectionId") else {
        return connection_id_missing();
    };
    match st.connections.get_mut(connection_id) {
        Some(conn) => {
            conn.connection_state = "deleted".to_string();
            let record = conn.clone();
            ok(connection_dict(st, &record))
        }
        None => connection_not_found(connection_id, region),
    }
}

fn update_connection(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = string_at(body, "connectionId") else {
        return connection_id_missing();
    };
    match st.connections.get_mut(connection_id) {
        Some(conn) => {
            if let Some(name) = truthy_str(body, "connectionName") {
                conn.connection_name = name.to_string();
            }
            if let Some(mode) = truthy_str(body, "encryptionMode") {
                conn.encryption_mode = mode.to_string();
            }
            let record = conn.clone();
            ok(connection_dict(st, &record))
        }
        None => connection_not_found(connection_id, region),
    }
}

/// moto's `associate_mac_sec_key`: `dxlag-` ids route to the LAG and re-bind
/// each member connection's key list; everything else is a connection.
fn associate_mac_sec_key(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let connection_id = string_at(body, "connectionId").unwrap_or("");
    let mac_sec_key = MacSecKey {
        // moto: `secret_arn or "mock_secret_arn"`, `ckn` as sent (may be "").
        secret_arn: string_at(body, "secretARN")
            .filter(|s| !s.is_empty())
            .unwrap_or("mock_secret_arn")
            .to_string(),
        ckn: string_at(body, "ckn").unwrap_or("").to_string(),
        state: "associated".to_string(),
        start_on: dx_time(),
    };
    if connection_id.contains("dxlag-") {
        let Some(lag) = st.lags.get_mut(connection_id) else {
            return lag_not_found(connection_id, region);
        };
        // moto re-binds member connections to the LAG's shared list; members
        // are stored by id here, so the lag's list is the shared view.
        lag.mac_sec_keys.push(mac_sec_key);
        let keys = lag.mac_sec_keys.clone();
        return ok(json!({
            "connectionId": connection_id,
            "macSecKeys": keys.iter().map(MacSecKey::to_dict).collect::<Vec<_>>(),
        }));
    }
    let Some(conn) = st.connections.get_mut(connection_id) else {
        return connection_not_found(connection_id, region);
    };
    conn.mac_sec_keys.push(mac_sec_key);
    let keys = conn.mac_sec_keys.clone();
    ok(json!({
        "connectionId": connection_id,
        "macSecKeys": keys.iter().map(MacSecKey::to_dict).collect::<Vec<_>>(),
    }))
}

/// moto's `disassociate_mac_sec_key`: pops the key matching `secretARN`
/// (casefold) after marking it `disassociated`. An unknown id or an emptied
/// key list is `ConnectionNotFound`; no key matches and it's
/// `MacSecKeyNotFound`.
fn disassociate_mac_sec_key(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let connection_id = string_at(body, "connectionId").unwrap_or("");
    let secret_arn = string_at(body, "secretARN").unwrap_or("");
    let is_lag = connection_id.contains("dxlag-");
    let keys = if is_lag {
        st.lags.get(connection_id).map(|l| l.mac_sec_keys.clone())
    } else {
        st.connections
            .get(connection_id)
            .map(|c| c.mac_sec_keys.clone())
    };
    let Some(keys) = keys else {
        return connection_not_found(connection_id, region);
    };
    if keys.is_empty() {
        return connection_not_found(connection_id, region);
    }
    let wanted = secret_arn.to_lowercase();
    let Some(pos) = keys
        .iter()
        .position(|k| k.secret_arn.to_lowercase() == wanted)
    else {
        return mac_sec_key_not_found(secret_arn, connection_id);
    };
    let mut key = keys[pos].clone();
    key.state = "disassociated".to_string();
    if is_lag {
        let lag = st.lags.get_mut(connection_id).expect("checked above");
        lag.mac_sec_keys.remove(pos);
    } else {
        let conn = st
            .connections
            .get_mut(connection_id)
            .expect("checked above");
        conn.mac_sec_keys.remove(pos);
    }
    ok(json!({
        "connectionId": connection_id,
        "macSecKeys": [key.to_dict()],
    }))
}

/// moto's `create_lag`: spawns children named
/// `Requested Connection <n> for Lag <lag id>`; a `connectionId` member is
/// moto's unguarded `NotImplementedError` → 500 `InternalError`.
fn create_lag(st: &mut DxStore, account: u64, region: &str, body: &Value) -> Response {
    // moto: `if connection_id:` — an empty member keeps the code path.
    if truthy_str(body, "connectionId").is_some() {
        return internal_error_500(
            "creating a lag with a connection_id is not currently supported by moto",
        );
    }
    let number_of_connections = match body.get("numberOfConnections").and_then(Value::as_i64) {
        Some(n) => n,
        // moto: `range(None)` TypeError crash (the suite always sends a count).
        None => {
            return internal_error_500(
                "RangeError: 'NoneType' object cannot be interpreted as an integer",
            )
        }
    };
    let request_mac_sec = body.get("requestMACSec").cloned().unwrap_or(Value::Null);
    let mac_sec = if truthy(&request_mac_sec) {
        ("must_encrypt".to_string(), Some(MacSecKey::default_mock()))
    } else {
        ("no_encrypt".to_string(), None)
    };
    let lag_id = dx_arn(
        region,
        account,
        "dxlag",
        &format!(
            "dxlag-moto-{}-{}",
            string_at(body, "lagName").unwrap_or(""),
            dx_time_ts()
        ),
    );
    let mut lag = Lag {
        lag_id: lag_id.clone(),
        lag_name: string_at(body, "lagName").unwrap_or("").to_string(),
        lag_state: "available".to_string(),
        connections_bandwidth: string_at(body, "connectionsBandwidth")
            .unwrap_or("")
            .to_string(),
        number_of_connections,
        minimum_links: 0,
        connections: Vec::new(),
        location: string_at(body, "location").unwrap_or("").to_string(),
        provider_name: body
            .get("providerName")
            .and_then(Value::as_str)
            .map(String::from),
        mac_sec_capable: if request_mac_sec.is_null() {
            Value::Null
        } else {
            request_mac_sec.clone()
        },
        encryption_mode: mac_sec.0.clone(),
        mac_sec_keys: mac_sec.1.clone().into_iter().collect(),
        region: region.to_string(),
    };
    for i in 0..number_of_connections {
        // moto creates children through `create_connection` with
        // `connection_name=f"Requested Connection {i + 1} for Lag {lag.lag_id}"`.
        let mut child_params = Map::new();
        child_params.insert(
            "location".into(),
            body.get("location").cloned().unwrap_or(Value::Null),
        );
        child_params.insert(
            "bandwidth".into(),
            body.get("connectionsBandwidth")
                .cloned()
                .unwrap_or(Value::Null),
        );
        child_params.insert(
            "connectionName".into(),
            json!(format!("Requested Connection {} for Lag {lag_id}", i + 1)),
        );
        child_params.insert(
            "tags".into(),
            body.get("childConnectionTags")
                .cloned()
                .unwrap_or(Value::Null),
        );
        child_params.insert(
            "providerName".into(),
            body.get("providerName").cloned().unwrap_or(Value::Null),
        );
        child_params.insert("requestMACSec".into(), Value::Bool(false));
        let mut child = build_connection(
            st,
            account,
            region,
            &Value::Object(child_params),
            Some(lag_id.clone()),
        );
        // moto then pushes the LAG's MACSec mode onto each child.
        if truthy(&request_mac_sec) {
            child.mac_sec_capable = Value::Bool(true);
            child.mac_sec_keys = mac_sec.1.clone().into_iter().collect();
            child.encryption_mode = mac_sec.0.clone();
        }
        // moto spawns children through `create_connection`, so they also
        // register at the top level; the LAG keeps their ids.
        st.connections
            .insert(child.connection_id.clone(), child.clone());
        lag.connections.push(child.connection_id);
    }
    // moto: `if tags:` — an absent/empty list never reaches the tagger.
    if let Some(tags) = truthy_array(body.get("tags")) {
        tag_resource(st, &lag_id, Some(tags));
    }
    let rendered = lag_dict(st, &lag);
    st.lags.insert(lag_id, lag);
    ok(rendered)
}

/// moto's `describe_lags`: unknown filter id raises, empty stays all.
fn describe_lags(st: &DxStore, body: &Value, region: &str) -> Response {
    let Some(lag_id) = truthy_str(body, "lagId") else {
        let lags: Vec<Value> = st.lags.values().map(|l| lag_dict(st, l)).collect();
        return ok(json!({ "lags": lags }));
    };
    match st.lags.get(lag_id) {
        Some(lag) => ok(json!({ "lags": [lag_dict(st, lag)] })),
        None => lag_not_found(lag_id, region),
    }
}

/// moto's `describe_tags` — a row per requested arn, empty tags included.
fn describe_tags(st: &DxStore, body: &Value) -> Response {
    let rows: Vec<Value> = str_list(body.get("resourceArns"))
        .into_iter()
        .map(|arn| {
            json!({
                "resourceArn": arn,
                "tags": list_tags_for_resource(st, &arn),
            })
        })
        .collect();
    ok(json!({ "resourceTags": rows }))
}

// ---------------------------------------------------------------------------
// wave-12 operations — interconnects
// ---------------------------------------------------------------------------

/// Create an interconnect the way the fork builds connections (mock device,
/// `available` state, MACSec fields from `requestMACSec`).
fn create_interconnect(st: &mut DxStore, account: u64, region: &str, body: &Value) -> Response {
    let interconnect_name = string_at(body, "interconnectName").unwrap_or("").to_string();
    let request_mac_sec = body.get("requestMACSec").cloned().unwrap_or(Value::Null);
    let mac_sec = if truthy(&request_mac_sec) {
        ("must_encrypt".to_string(), vec![MacSecKey::default_mock()])
    } else {
        ("no_encrypt".to_string(), Vec::new())
    };
    let ic = Interconnect {
        interconnect_id: dx_arn(
            region,
            account,
            "dxcon",
            &format!("dx-moto-{interconnect_name}-{}", dx_time_ts()),
        ),
        interconnect_name,
        interconnect_state: "available".to_string(),
        bandwidth: string_at(body, "bandwidth").unwrap_or("").to_string(),
        location: string_at(body, "location").unwrap_or("").to_string(),
        lag_id: string_at(body, "lagId").map(String::from),
        provider_name: body.get("providerName").and_then(Value::as_str).map(String::from),
        mac_sec_capable: if request_mac_sec.is_null() {
            Value::Null
        } else {
            request_mac_sec
        },
        encryption_mode: mac_sec.0,
        mac_sec_keys: mac_sec.1,
        region: region.to_string(),
    };
    let id = ic.interconnect_id.clone();
    if let Some(tags) = truthy_array(body.get("tags")) {
        tag_resource(st, &id, Some(tags));
    }
    st.interconnects.insert(id.clone(), ic.clone());
    ok(interconnect_dict(st, &ic))
}

/// botocore `DescribeInterconnects`: an id filter answers only that
/// interconnect (unknown id → `DirectConnectClientException`), empty stays all.
fn describe_interconnects(st: &DxStore, body: &Value, region: &str) -> Response {
    if let Some(id) = truthy_str(body, "interconnectId") {
        return match st.interconnects.get(id) {
            Some(ic) => ok(json!({
                "interconnects": [interconnect_dict(st, ic)],
            })),
            None => interconnect_not_found(id, region),
        };
    }
    let all: Vec<(String, Value)> = st
        .interconnects
        .values()
        .map(|ic| (ic.interconnect_id.clone(), interconnect_dict(st, ic)))
        .collect();
    let items = page_pairs(
        all,
        int_at(body, "maxResults"),
        string_at(body, "nextToken"),
    );
    ok(json!({
        "interconnects": items.0,
        "nextToken": items.1,
    }))
}

/// Mirrors the fork's `DeleteConnection`: mark deleted, keep the record.
fn delete_interconnect(st: &mut DxStore, body: &Value, region: &str) -> Response {
    match string_at(body, "interconnectId") {
        Some(id) => match st.interconnects.get_mut(id) {
            Some(ic) => {
                ic.interconnect_state = "deleted".to_string();
                let dict = interconnect_dict(st, ic);
                ok(json!({ "interconnectState": dict["interconnectState"].clone() }))
            }
            None => interconnect_not_found(id, region),
        },
        None => interconnect_not_found("", region),
    }
}

/// Connections allocated onto this interconnect (`AllocateConnectionOnInterconnect`).
fn describe_connections_on_interconnect(st: &DxStore, body: &Value, region: &str) -> Response {
    let Some(interconnect_id) = truthy_str(body, "interconnectId") else {
        return interconnect_not_found("", region);
    };
    if !st.interconnects.contains_key(interconnect_id) {
        return interconnect_not_found(interconnect_id, region);
    }
    let conns: Vec<Value> = st
        .connections
        .values()
        .filter(|c| c.parent_id.as_deref() == Some(interconnect_id))
        .map(|c| connection_dict(st, c))
        .collect();
    ok(json!({ "connections": conns }))
}

/// Allocate a hosted connection on an interconnect: `ordering` state, the
/// requested owner account and `vlan`, parented to the interconnect.
fn allocate_connection_on_interconnect(
    st: &mut DxStore,
    _account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let Some(interconnect_id) = truthy_str(body, "interconnectId") else {
        return interconnect_not_found("", region);
    };
    if !st.interconnects.contains_key(interconnect_id) {
        return interconnect_not_found(interconnect_id, region);
    }
    let Some(location) = st.interconnects.get(interconnect_id).map(|i| i.location.clone()) else {
        return interconnect_not_found(interconnect_id, region);
    };
    let mut params = Map::new();
    params.insert("bandwidth".into(), body.get("bandwidth").cloned().unwrap_or(Value::Null));
    params.insert(
        "connectionName".into(),
        body.get("connectionName").cloned().unwrap_or(Value::Null),
    );
    let mut conn = build_connection(st, _account, region, &Value::Object(params), None);
    conn.location = location;
    conn.connection_state = "ordering".to_string();
    conn.owner_account = string_at(body, "ownerAccount").map(String::from);
    conn.parent_id = Some(interconnect_id.to_string());
    conn.vlan = body.get("vlan").and_then(Value::as_i64);
    if let Some(tags) = truthy_array(body.get("tags")) {
        tag_resource(st, &conn.connection_id, Some(tags));
    }
    st.connections.insert(conn.connection_id.clone(), conn.clone());
    ok(connection_dict(st, &conn))
}

/// botocore `DescribeInterconnectLoa` — the twin serves a deterministic mock
/// document; the interconnect (and its Loa) is the only modelled state.
fn describe_interconnect_loa(st: &DxStore, body: &Value, region: &str) -> Response {
    let Some(interconnect_id) = truthy_str(body, "interconnectId") else {
        return interconnect_not_found("", region);
    };
    match st.interconnects.get(interconnect_id) {
        Some(_) => ok(json!({ "loa": mock_loa() })),
        None => interconnect_not_found(interconnect_id, region),
    }
}

// ---------------------------------------------------------------------------
// wave-12 operations — virtual interfaces
// ---------------------------------------------------------------------------

/// The (op name → kind, params key, is-allocation) table for the six
/// Create*/Allocate*xVirtualInterface dispatch arms.
fn vif_input(req: &Request) -> (&'static str, &'static str, bool) {
    match req.operation.as_str() {
        "CreatePrivateVirtualInterface" => ("private", "newPrivateVirtualInterface", false),
        "CreatePublicVirtualInterface" => ("public", "newPublicVirtualInterface", false),
        "CreateTransitVirtualInterface" => ("transit", "newTransitVirtualInterface", false),
        "AllocatePrivateVirtualInterface" => {
            ("private", "newPrivateVirtualInterfaceAllocation", true)
        }
        "AllocatePublicVirtualInterface" => ("public", "newPublicVirtualInterfaceAllocation", true),
        "AllocateTransitVirtualInterface" => {
            ("transit", "newTransitVirtualInterfaceAllocation", true)
        }
        _ => unreachable!("dispatched by the create/allocate VIF arm"),
    }
}

fn create_or_allocate_virtual_interface(
    st: &mut DxStore,
    account: u64,
    req: &Request,
    body: &Value,
) -> Response {
    let (kind, params_key, allocation) = vif_input(req);
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        return connection_not_found("", &req.region);
    };
    let Some(conn) = st.connections.get(connection_id) else {
        return connection_not_found(connection_id, &req.region);
    };
    let params = body.get(params_key).cloned().unwrap_or(Value::Null);
    let owner = allocation
        .then(|| string_at(body, "ownerAccount").map(String::from))
        .flatten();
    let vif = build_virtual_interface(
        st,
        account,
        &req.region,
        connection_id,
        conn.location.clone(),
        params,
        kind,
        owner,
    );
    st.virtual_interfaces
        .insert(vif.virtual_interface_id.clone(), vif.clone());
    ok(virtual_interface_dict(st, &vif))
}

/// Build the virtual interface record from the `new*VirtualInterface[Allocation]`
/// member. Created VIFs sit `pending` (public: `verifying`) until their
/// Confirm* call flips them to `available`; public VIFs render their
/// `routeFilterPrefixes`, private/transit VIFs register a gateway attachment
/// when `directConnectGatewayId` is set.
fn build_virtual_interface(
    st: &mut DxStore,
    account: u64,
    region: &str,
    connection_id: &str,
    location: String,
    params: Value,
    kind: &str,
    owner_account: Option<String>,
) -> VirtualInterfaceRecord {
    let name = string_at(&params, "virtualInterfaceName").unwrap_or("").to_string();
    let state = if kind == "public" {
        "verifying"
    } else {
        "pending"
    };
    let mut vif = VirtualInterfaceRecord {
        virtual_interface_id: dx_arn(
            region,
            account,
            "dxvif",
            &format!("dx-moto-{name}-{}", dx_time_ts()),
        ),
        virtual_interface_name: name,
        virtual_interface_type: kind.to_string(),
        virtual_interface_state: state.to_string(),
        connection_id: connection_id.to_string(),
        location,
        vlan: params.get("vlan").and_then(Value::as_i64),
        asn: params.get("asn").and_then(Value::as_i64),
        asn_long: params.get("asnLong").and_then(Value::as_i64),
        amazon_side_asn: None,
        auth_key: string_at(&params, "authKey").map(String::from),
        amazon_address: string_at(&params, "amazonAddress").map(String::from),
        customer_address: string_at(&params, "customerAddress").map(String::from),
        address_family: string_at(&params, "addressFamily").map(String::from),
        mtu: params.get("mtu").and_then(Value::as_i64),
        virtual_gateway_id: string_at(&params, "virtualGatewayId").map(String::from),
        direct_connect_gateway_id: string_at(&params, "directConnectGatewayId").map(String::from),
        route_filter_prefixes: str_list(params.get("routeFilterPrefixes")),
        bgp_peers: Vec::new(),
        site_link_enabled: params.get("enableSiteLink").and_then(Value::as_bool),
        owner_account: owner_account.or_else(|| Some(account.to_string())),
        region: region.to_string(),
    };
    // Transit/private VIFs bound to a Direct Connect gateway add an
    // attachment row for DescribeDirectConnectGatewayAttachments.
    if let Some(gateway_id) = &vif.direct_connect_gateway_id {
        let attach_type = if kind == "transit" {
            "TransitVirtualInterface"
        } else {
            "PrivateVirtualInterface"
        };
        st.attachments.insert(
            attachment_key(gateway_id, &vif.virtual_interface_id),
            DxAttachment {
                direct_connect_gateway_id: gateway_id.clone(),
                virtual_interface_id: vif.virtual_interface_id.clone(),
                virtual_interface_region: region.to_string(),
                virtual_interface_owner_account: vif.owner_account.clone().unwrap_or_default(),
                attachment_state: "attached".to_string(),
                attachment_type: attach_type.to_string(),
            },
        );
    }
    if let Some(tags) = truthy_array(params.get("tags")) {
        tag_resource(st, &vif.virtual_interface_id, Some(tags));
    }
    vif
}

/// The attachments map key: `<gateway id>?<virtualInterface id>`.
fn attachment_key(gateway_id: &str, vif_id: &str) -> String {
    format!("{gateway_id}\u{1}{vif_id}")
}

/// `ConfirmConnection`: the connection answers with its (unchanged) state.
fn confirm_connection(st: &DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        return connection_not_found("", region);
    };
    match st.connections.get(connection_id) {
        Some(conn) => ok(json!({ "connectionState": conn.connection_state.clone() })),
        None => connection_not_found(connection_id, region),
    }
}

/// The three Confirm*VirtualInterface arms move the VIF to `available`.
fn confirm_virtual_interface(st: &mut DxStore, body: &Value) -> Response {
    let Some(vif_id) = truthy_str(body, "virtualInterfaceId") else {
        return vif_not_found("");
    };
    match st
        .virtual_interfaces
        .get_mut(vif_id)
        .filter(|v| v.virtual_interface_state != "deleted")
    {
        Some(vif) => {
            vif.virtual_interface_state = "available".to_string();
            ok(json!({
                "virtualInterfaceState": vif.virtual_interface_state.clone(),
            }))
        }
        None => vif_not_found(vif_id),
    }
}

/// botocore `DescribeVirtualInterfaces` — connectionId / virtualInterfaceId
/// filters; unknown single-id filters raise like the fork's describes.
fn describe_virtual_interfaces(st: &DxStore, body: &Value) -> Response {
    let wanted: Vec<&VirtualInterfaceRecord> =
        if let Some(vif_id) = truthy_str(body, "virtualInterfaceId") {
            match st.virtual_interfaces.get(vif_id) {
                Some(vif) => vec![vif],
                None => return vif_not_found(vif_id),
            }
        } else {
            let mut all: Vec<&VirtualInterfaceRecord> =
                st.virtual_interfaces.values().collect();
            if let Some(connection_id) = truthy_str(body, "connectionId") {
                all.retain(|v| v.connection_id == connection_id);
            }
            all
        };
    let vifs: Vec<Value> = wanted
        .iter()
        .map(|v| virtual_interface_dict(st, v))
        .collect();
    ok(json!({ "virtualInterfaces": vifs }))
}

/// The fork's `DeleteConnection` keeps the record; virtual interfaces do the
/// same with the `deleted` state.
fn delete_virtual_interface(st: &mut DxStore, body: &Value) -> Response {
    let Some(vif_id) = truthy_str(body, "virtualInterfaceId") else {
        return vif_not_found("");
    };
    match st.virtual_interfaces.get_mut(vif_id) {
        Some(vif) => {
            vif.virtual_interface_state = "deleted".to_string();
            ok(json!({ "virtualInterfaceState": "deleted" }))
        }
        None => vif_not_found(vif_id),
    }
}

/// Re-point a virtual interface at another connection.
fn associate_virtual_interface(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(vif_id) = truthy_str(body, "virtualInterfaceId") else {
        return vif_not_found("");
    };
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        return connection_not_found("", region);
    };
    let Some(conn) = st.connections.get(connection_id) else {
        return connection_not_found(connection_id, region);
    };
    let location = conn.location.clone();
    match st.virtual_interfaces.get_mut(vif_id) {
        Some(vif) => {
            vif.connection_id = connection_id.to_string();
            vif.location = location;
            ok(virtual_interface_dict(st, vif))
        }
        None => vif_not_found(vif_id),
    }
}

/// `UpdateVirtualInterfaceAttributes` renames and sets mtu / enableSiteLink.
fn update_virtual_interface_attributes(st: &mut DxStore, body: &Value) -> Response {
    let Some(vif_id) = truthy_str(body, "virtualInterfaceId") else {
        return vif_not_found("");
    };
    match st.virtual_interfaces.get_mut(vif_id) {
        Some(vif) => {
            if let Some(name) = truthy_str(body, "virtualInterfaceName") {
                vif.virtual_interface_name = name.to_string();
            }
            if let Some(mtu) = body.get("mtu").and_then(Value::as_i64) {
                vif.mtu = Some(mtu);
            }
            if let Some(enable) = body.get("enableSiteLink").and_then(Value::as_bool) {
                vif.site_link_enabled = Some(enable);
            }
            ok(virtual_interface_dict(st, vif))
        }
        None => vif_not_found(vif_id),
    }
}

/// `CreateBGPPeer` appends one peer to the virtual interface's peer list.
fn create_bgp_peer(st: &mut DxStore, body: &Value) -> Response {
    let Some(vif_id) = truthy_str(body, "virtualInterfaceId") else {
        return vif_not_found("");
    };
    let params = body.get("newBGPPeer").cloned().unwrap_or(Value::Null);
    let asn = params.get("asn").and_then(Value::as_i64);
    let peer = BgpPeer {
        bgp_peer_id: format!(
            "dxpeer-moto-{}-{}",
            asn.unwrap_or(0),
            dx_time_ts()
        ),
        asn,
        asn_long: params.get("asnLong").and_then(Value::as_i64),
        auth_key: string_at(&params, "authKey").map(String::from),
        address_family: string_at(&params, "addressFamily").map(String::from),
        amazon_address: string_at(&params, "amazonAddress").map(String::from),
        customer_address: string_at(&params, "customerAddress").map(String::from),
        bgp_peer_state: "available".to_string(),
        bgp_status: "unknown".to_string(),
    };
    match st.virtual_interfaces.get_mut(vif_id) {
        Some(vif) => {
            vif.bgp_peers.push(peer);
            ok(json!({ "virtualInterface": virtual_interface_dict(st, vif) }))
        }
        None => vif_not_found(vif_id),
    }
}

/// `DeleteBGPPeer` drops the peer matching `bgpPeerId` (or asn/customerAddress):
/// the id filter wins, then `customerAddress`, then `asn`/`asnLong`.
fn delete_bgp_peer(st: &mut DxStore, body: &Value) -> Response {
    let Some(vif_id) = truthy_str(body, "virtualInterfaceId") else {
        return vif_not_found("");
    };
    let want_id = truthy_str(body, "bgpPeerId").map(String::from);
    let want_asn = body
        .get("asn")
        .and_then(Value::as_i64)
        .or_else(|| body.get("asnLong").and_then(Value::as_i64));
    let want_addr = truthy_str(body, "customerAddress").map(String::from);
    let Some(vif) = st.virtual_interfaces.get_mut(vif_id) else {
        return vif_not_found(vif_id);
    };
    let matches = |p: &BgpPeer| -> bool {
        if let Some(id) = &want_id {
            return id == &p.bgp_peer_id;
        }
        if let Some(addr) = &want_addr {
            return p.customer_address.as_deref() == Some(addr.as_str());
        }
        if let Some(asn) = want_asn {
            return p.asn == Some(asn);
        }
        false // nothing submitted — nothing matches
    };
    let Some(pos) = vif.bgp_peers.iter().position(matches) else {
        if want_id.is_none() && want_addr.is_none() && want_asn.is_none() {
            return dx_client_error(
                "Either asn or bgpPeerId must be provided.",
            );
        }
        return dx_client_error("No BGPPeer matches the submitted parameters.");
    };
    vif.bgp_peers.remove(pos);
    ok(json!({ "virtualInterface": virtual_interface_dict(st, vif) }))
}

/// A typical 3DES-capable default keeps the API surface honest: the model
/// declares no default, so 145 minutes is used (the long-standing public
/// default for this op).
const BGP_FAILOVER_TEST_DEFAULT_MINUTES: i64 = 145;

/// `StartBgpFailoverTest`: record a running test, move the VIF to `testing`.
fn start_bgp_failover_test(st: &mut DxStore, account: u64, body: &Value) -> Response {
    let Some(vif_id) = truthy_str(body, "virtualInterfaceId") else {
        return vif_not_found("");
    };
    let Some(vif) = st.virtual_interfaces.get_mut(vif_id) else {
        return vif_not_found(vif_id);
    };
    let minutes = body.get("testDurationInMinutes").and_then(Value::as_i64);
    let test = ViTest {
        test_id: format!("dxtest-moto-{}", dx_time_ts()),
        virtual_interface_id: vif_id.to_string(),
        bgp_peers: str_list(body.get("bgpPeers")),
        status: "running".to_string(),
        owner_account: account.to_string(),
        test_duration_in_minutes: minutes.unwrap_or(BGP_FAILOVER_TEST_DEFAULT_MINUTES),
        start_time: dx_time(),
        end_time: None,
    };
    let dict = test_dict(&test);
    st.tests.insert(test.test_id.clone(), test);
    vif.virtual_interface_state = "testing".to_string();
    ok(json!({ "virtualInterfaceTest": dict }))
}

/// `StopBgpFailoverTest`: the latest running test stops; the VIF returns to
/// `available`. No running test → `DirectConnectClientException`.
fn stop_bgp_failover_test(st: &mut DxStore, body: &Value) -> Response {
    let Some(vif_id) = truthy_str(body, "virtualInterfaceId") else {
        return vif_not_found("");
    };
    let Some(vif) = st.virtual_interfaces.get_mut(vif_id) else {
        return vif_not_found(vif_id);
    };
    let Some(test_id) = st
        .tests
        .values()
        .rev()
        .find(|t| t.virtual_interface_id == vif_id && t.status == "running")
        .map(|t| t.test_id.clone())
    else {
        return dx_client_error(format!(
            "No running BGP failover test for virtual interface {vif_id}."
        ));
    };
    let Some(test) = st.tests.get_mut(&test_id) else {
        unreachable!("test_id came from the map");
    };
    test.status = "stopped".to_string();
    test.end_time = Some(dx_time());
    vif.virtual_interface_state = "available".to_string();
    let dict = test_dict(test);
    ok(json!({ "virtualInterfaceTest": dict }))
}

/// botocore `ListVirtualInterfaceTestHistory` with testId / virtualInterfaceId
/// / status / bgpPeers filters and shape-driven pagination.
fn list_virtual_interface_test_history(st: &DxStore, body: &Value) -> Response {
    let mut rows: Vec<(String, Value)> = st
        .tests
        .values()
        .filter(|t| {
            truthy_str(body, "testId").map_or(true, |id| id == t.test_id.as_str())
                && truthy_str(body, "virtualInterfaceId")
                    .map_or(true, |id| id == t.virtual_interface_id.as_str())
                && truthy_str(body, "status").map_or(true, |s| s == t.status.as_str())
        })
        .filter(|t| match str_list(body.get("bgpPeers")) {
            empty if empty.is_empty() => true,
            wanted => wanted.iter().any(|w| t.bgp_peers.contains(w)),
        })
        .map(|t| (t.start_time.clone(), test_dict(t)))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let items = page_pairs(rows, int_at(body, "maxResults"), string_at(body, "nextToken"));
    ok(json!({
        "virtualInterfaceTestHistory": items.0,
        "nextToken": items.1,
    }))
}

/// `DescribeRouterConfiguration` — the mock router constants against an
/// existing virtual interface.
fn describe_router_configuration(st: &DxStore, body: &Value) -> Response {
    let Some(vif_id) = truthy_str(body, "virtualInterfaceId") else {
        return vif_not_found("");
    };
    let Some(vif) = st.virtual_interfaces.get(vif_id) else {
        return vif_not_found(vif_id);
    };
    ok(json!({
        "customerRouterConfig": customer_router_config(),
        "router": {
            "platform": ROUTER_PLATFORM,
            "routerTypeIdentifier": format!("{ROUTER_VENDOR}-{ROUTER_PLATFORM}-{ROUTER_SOFTWARE}"),
            "software": ROUTER_SOFTWARE,
            "vendor": ROUTER_VENDOR,
            "xsltTemplateName": "customer-router.xml",
            "xsltTemplateNameForMacSec": "customer-router-macsec.xml",
        },
        "virtualInterfaceId": vif.virtual_interface_id,
        "virtualInterfaceName": vif.virtual_interface_name,
    }))
}

// ---------------------------------------------------------------------------
// wave-12 operations — Direct Connect gateways
// ---------------------------------------------------------------------------

fn create_direct_connect_gateway(st: &mut DxStore, account: u64, body: &Value) -> Response {
    let name = string_at(body, "directConnectGatewayName").unwrap_or("").to_string();
    let gw = DxGateway {
        direct_connect_gateway_id: format!("dxgw-moto-{name}-{}", dx_time_ts()),
        direct_connect_gateway_name: name,
        amazon_side_asn: body.get("amazonSideAsn").and_then(Value::as_i64),
        direct_connect_gateway_state: "available".to_string(),
        owner_account: account.to_string(),
    };
    if let Some(tags) = truthy_array(body.get("tags")) {
        tag_resource(st, &gw.direct_connect_gateway_id, Some(tags));
    }
    st.gateways.insert(gw.direct_connect_gateway_id.clone(), gw.clone());
    ok(json!({ "directConnectGateway": gateway_dict(st, &gw) }))
}

fn delete_direct_connect_gateway(st: &mut DxStore, body: &Value) -> Response {
    let Some(gw_id) = truthy_str(body, "directConnectGatewayId") else {
        return gateway_not_found("");
    };
    match st.gateways.get_mut(gw_id) {
        Some(gw) => {
            gw.direct_connect_gateway_state = "deleted".to_string();
            ok(json!({ "directConnectGateway": gateway_dict(st, gw) }))
        }
        None => gateway_not_found(gw_id),
    }
}

fn describe_direct_connect_gateways(st: &DxStore, body: &Value) -> Response {
    let all: Vec<(String, Value)> = st
        .gateways
        .values()
        .filter(|gw| {
            truthy_str(body, "directConnectGatewayId")
                .map_or(true, |id| id == gw.direct_connect_gateway_id.as_str())
        })
        .map(|gw| (gw.direct_connect_gateway_id.clone(), gateway_dict(st, gw)))
        .collect();
    let items = page_pairs(
        all,
        int_at(body, "maxResults"),
        string_at(body, "nextToken"),
    );
    ok(json!({
        "directConnectGateways": items.0,
        "nextToken": items.1,
    }))
}

fn update_direct_connect_gateway(st: &mut DxStore, body: &Value) -> Response {
    let Some(gw_id) = truthy_str(body, "directConnectGatewayId") else {
        return gateway_not_found("");
    };
    let Some(name) = truthy_str(body, "newDirectConnectGatewayName") else {
        return dx_client_error("newDirectConnectGatewayName is required.");
    };
    match st.gateways.get_mut(gw_id) {
        Some(gw) => {
            gw.direct_connect_gateway_name = name.to_string();
            ok(json!({ "directConnectGateway": gateway_dict(st, gw) }))
        }
        None => gateway_not_found(gw_id),
    }
}

/// Create an association for a gateway the caller owns. The associated
/// gateway (`virtualGatewayId` or `gatewayId`) is stored as sent — the twin
/// does not model the EC2/VPC side.
fn create_direct_connect_gateway_association(
    st: &mut DxStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let Some(gw_id) = truthy_str(body, "directConnectGatewayId") else {
        return gateway_not_found("");
    };
    if !st.gateways.contains_key(gw_id) {
        return gateway_not_found(gw_id);
    }
    let gateway_id = string_at(body, "gatewayId").map(String::from);
    let virtual_gateway_id = string_at(body, "virtualGatewayId").map(String::from);
    if gateway_id.is_none() && virtual_gateway_id.is_none() {
        return dx_client_error("Either gatewayId or virtualGatewayId must be provided.");
    }
    let associated = match gateway_id.as_deref() {
        Some(id) => AssociatedGateway {
            id: id.to_string(),
            type_: gateway_type_for(&id),
            owner_account: account.to_string(),
            region: region.to_string(),
        },
        None => AssociatedGateway {
            id: virtual_gateway_id.clone().unwrap_or_default(),
            type_: "virtualPrivateGateway".to_string(),
            owner_account: account.to_string(),
            region: region.to_string(),
        },
    };
    let association = DxAssociation {
        association_id: format!("dxassoc-moto-{}", dx_time_ts()),
        direct_connect_gateway_id: gw_id.to_string(),
        direct_connect_gateway_owner_account: account.to_string(),
        association_state: "associated".to_string(),
        associated_gateway: associated,
        allowed_prefixes: str_list(body.get("addAllowedPrefixesToDirectConnectGateway")),
        virtual_gateway_id,
        virtual_gateway_region: Some(region.to_string()),
    };
    st.associations
        .insert(association.association_id.clone(), association.clone());
    ok(json!({
        "directConnectGatewayAssociation": association_dict(&association),
    }))
}

/// Delete via associationId, or via (directConnectGatewayId, virtualGatewayId).
/// The record is removed; the response mirrors AWS's one-shot `disassociating`.
fn delete_direct_connect_gateway_association(st: &mut DxStore, body: &Value) -> Response {
    let by_id = truthy_str(body, "associationId").map(String::from);
    let found = match by_id {
        Some(id) => st.associations.remove(&id).filter(|a| {
            string_at(body, "directConnectGatewayId").map_or(true, |g| {
                a.direct_connect_gateway_id == g
            })
        }),
        None => {
            let gateway = string_at(body, "directConnectGatewayId");
            let vgw = string_at(body, "virtualGatewayId").map(String::from);
            let key = st
                .associations
                .values()
                .find(|a| {
                    string_at(body, "directConnectGatewayId")
                        .map_or(true, |g| a.direct_connect_gateway_id == g)
                        && vgw
                            .as_ref()
                            .map_or(true, |v| a.virtual_gateway_id.as_deref() == Some(v.as_str()))
                        && a.association_state != "deleted"
                })
                .map(|a| a.association_id.clone());
            key.and_then(|k| st.associations.remove(&k))
        }
    };
    let Some(mut association) = found else {
        return dx_client_error("The specified association does not exist.");
    };
    association.association_state = "disassociating".to_string();
    ok(json!({
        "directConnectGatewayAssociation": association_dict(&association),
    }))
}

fn describe_direct_connect_gateway_associations(st: &DxStore, body: &Value) -> Response {
    // A single associationId filter is a direct lookup; unknown → client error.
    if let Some(association_id) = truthy_str(body, "associationId") {
        return match st.associations.get(association_id) {
            Some(a) => ok(json!({
                "directConnectGatewayAssociations": [association_dict(a)],
            })),
            None => dx_client_error(format!(
                "The association {association_id} is not present."
            )),
        };
    }
    let gateway_filter = truthy_str(body, "directConnectGatewayId").map(String::from);
    let vgw_filter = truthy_str(body, "virtualGatewayId").map(String::from);
    let agw_filter = truthy_str(body, "associatedGatewayId").map(String::from);
    let all: Vec<(String, Value)> = st
        .associations
        .values()
        .filter(|a| {
            gateway_filter
                .as_ref()
                .map_or(true, |g| g == &a.direct_connect_gateway_id)
                && vgw_filter
                    .as_ref()
                    .map_or(true, |v| a.virtual_gateway_id.as_deref() == Some(v.as_str()))
                && agw_filter
                    .as_ref()
                    .map_or(true, |g| g == &a.associated_gateway.id)
        })
        .map(|a| (a.association_id.clone(), association_dict(a)))
        .collect();
    let items = page_pairs(
        all,
        int_at(body, "maxResults"),
        string_at(body, "nextToken"),
    );
    ok(json!({
        "directConnectGatewayAssociations": items.0,
        "nextToken": items.1,
    }))
}

/// `UpdateDirectConnectGatewayAssociation` adds/removes allowed prefixes.
fn update_direct_connect_gateway_association(st: &mut DxStore, body: &Value) -> Response {
    let Some(association_id) = truthy_str(body, "associationId") else {
        return dx_client_error("associationId is required.");
    };
    let Some(a) = st.associations.get_mut(association_id) else {
        return dx_client_error(format!(
            "The association {association_id} is not present."
        ));
    };
    for cidr in str_list(body.get("removeAllowedPrefixesToDirectConnectGateway")) {
        a.allowed_prefixes.retain(|c| c != &cidr);
    }
    for cidr in str_list(body.get("addAllowedPrefixesToDirectConnectGateway")) {
        if !a.allowed_prefixes.contains(&cidr) {
            a.allowed_prefixes.push(cidr);
        }
    }
    a.association_state = "updating".to_string();
    ok(json!({
        "directConnectGatewayAssociation": association_dict(a),
    }))
}

fn create_direct_connect_gateway_association_proposal(
    st: &mut DxStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let Some(gw_id) = truthy_str(body, "directConnectGatewayId") else {
        return gateway_not_found("");
    };
    if !st.gateways.contains_key(gw_id) {
        return gateway_not_found(gw_id);
    }
    let Some(gateway_id) = truthy_str(body, "gatewayId") else {
        return dx_client_error("gatewayId is required.");
    };
    let dx_owner = string_at(body, "directConnectGatewayOwnerAccount")
        .map(String::from)
        .unwrap_or_else(|| account.to_string());
    let proposal = DxProposal {
        proposal_id: format!("dxproposal-moto-{}", dx_time_ts()),
        proposal_state: "requested".to_string(),
        direct_connect_gateway_id: gw_id.to_string(),
        direct_connect_gateway_owner_account: dx_owner,
        associated_gateway: AssociatedGateway {
            id: gateway_id.to_string(),
            type_: gateway_type_for(gateway_id),
            owner_account: account.to_string(),
            region: region.to_string(),
        },
        requested_allowed_prefixes: str_list(body.get("addAllowedPrefixesToDirectConnectGateway")),
    };
    st.proposals.insert(proposal.proposal_id.clone(), proposal.clone());
    ok(json!({
        "directConnectGatewayAssociationProposal": proposal_dict(&proposal),
    }))
}

/// Accepting a proposal materialises the association immediately and marks
/// the proposal `accepted`.
fn accept_direct_connect_gateway_association_proposal(
    st: &mut DxStore,
    body: &Value,
    region: &str,
) -> Response {
    let Some(gw_id) = truthy_str(body, "directConnectGatewayId") else {
        return gateway_not_found("");
    };
    if !st.gateways.contains_key(gw_id) {
        return gateway_not_found(gw_id);
    }
    let Some(proposal_id) = truthy_str(body, "proposalId") else {
        return dx_client_error("proposalId is required.");
    };
    let Some(proposal) = st.proposals.get(proposal_id) else {
        return dx_client_error(format!("The proposal {proposal_id} is not present."));
    };
    if proposal.direct_connect_gateway_id != gw_id {
        return dx_client_error(format!(
            "The proposal {proposal_id} was not created for gateway {gw_id}."
        ));
    }
    let prefixes = match body.get("overrideAllowedPrefixesToDirectConnectGateway") {
        Some(list) => str_list(Some(list)),
        None => proposal.requested_allowed_prefixes.clone(),
    };
    let owner_account = string_at(body, "associatedGatewayOwnerAccount")
        .unwrap_or(&proposal.direct_connect_gateway_owner_account)
        .to_string();
    let association = DxAssociation {
        association_id: format!("dxassoc-moto-{}-{gw_id}", dx_time_ts()),
        direct_connect_gateway_id: gw_id.to_string(),
        direct_connect_gateway_owner_account: proposal.direct_connect_gateway_owner_account.clone(),
        association_state: "associated".to_string(),
        associated_gateway: proposal.associated_gateway.clone(),
        allowed_prefixes: prefixes,
        virtual_gateway_id: Some(proposal.associated_gateway.id.clone()),
        virtual_gateway_region: Some(region.to_string()),
    };
    st.proposals
        .get_mut(proposal_id)
        .expect("checked above")
        .proposal_state = "accepted".to_string();
    st.associations
        .insert(association.association_id.clone(), association.clone());
    ok(json!({
        "directConnectGatewayAssociation": association_dict(&association),
    }))
}

fn describe_direct_connect_gateway_association_proposals(st: &DxStore, body: &Value) -> Response {
    if let Some(proposal_id) = truthy_str(body, "proposalId") {
        return match st.proposals.get(proposal_id) {
            Some(p) => ok(json!({
                "directConnectGatewayAssociationProposals": [proposal_dict(p)],
            })),
            None => dx_client_error(format!("The proposal {proposal_id} is not present.")),
        };
    }
    let gateway_filter = truthy_str(body, "directConnectGatewayId").map(String::from);
    let agw_filter = truthy_str(body, "associatedGatewayId").map(String::from);
    let all: Vec<(String, Value)> = st
        .proposals
        .values()
        .filter(|p| {
            gateway_filter
                .as_ref()
                .map_or(true, |g| g == &p.direct_connect_gateway_id)
                && agw_filter
                    .as_ref()
                    .map_or(true, |g| g == &p.associated_gateway.id)
        })
        .map(|p| (p.proposal_id.clone(), proposal_dict(p)))
        .collect();
    let items = page_pairs(
        all,
        int_at(body, "maxResults"),
        string_at(body, "nextToken"),
    );
    ok(json!({
        "directConnectGatewayAssociationProposals": items.0,
        "nextToken": items.1,
    }))
}

/// Deleting a proposal marks it `deleted` (mirrors the fork's keep-marker
/// delete style).
fn delete_direct_connect_gateway_association_proposal(st: &mut DxStore, body: &Value) -> Response {
    let Some(proposal_id) = truthy_str(body, "proposalId") else {
        return dx_client_error("proposalId is required.");
    };
    match st.proposals.get_mut(proposal_id) {
        Some(p) => {
            p.proposal_state = "deleted".to_string();
            ok(json!({
                "directConnectGatewayAssociationProposal": proposal_dict(p),
            }))
        }
        None => dx_client_error(format!("The proposal {proposal_id} is not present.")),
    }
}

fn describe_direct_connect_gateway_attachments(st: &DxStore, body: &Value) -> Response {
    let all: Vec<(String, Value)> = st
        .attachments
        .values()
        .filter(|a| {
            truthy_str(body, "directConnectGatewayId").map_or(true, |g| g == a.direct_connect_gateway_id)
                && truthy_str(body, "virtualInterfaceId")
                    .map_or(true, |v| v == a.virtual_interface_id)
        })
        .map(|a| (attachment_key(&a.direct_connect_gateway_id, &a.virtual_interface_id), attachment_dict(a)))
        .collect();
    let items = page_pairs(
        all,
        int_at(body, "maxResults"),
        string_at(body, "nextToken"),
    );
    ok(json!({
        "directConnectGatewayAttachments": items.0,
        "nextToken": items.1,
    }))
}

// ---------------------------------------------------------------------------
// wave-12 operations — LAG extras and hosted connections
// ---------------------------------------------------------------------------

/// `DeleteLag`: the LAG and its member connections go away — members are
/// marked `deleted` like the fork's `DeleteConnection`, the LAG record drops.
fn delete_lag(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(lag_id) = truthy_str(body, "lagId") else {
        return lag_not_found("", region);
    };
    let Some(lag) = st.lags.remove(lag_id) else {
        return lag_not_found(lag_id, region);
    };
    for id in &lag.connections {
        if let Some(conn) = st.connections.get_mut(id) {
            conn.connection_state = "deleted".to_string();
        }
    }
    ok(json!({
        "lagId": lag.lag_id,
        "lagState": "deleted",
    }))
}

/// `UpdateLag` renames, retunes `minimumLinks` and the MACSec mode.
fn update_lag(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(lag_id) = truthy_str(body, "lagId") else {
        return lag_not_found("", region);
    };
    match st.lags.get_mut(lag_id) {
        Some(lag) => {
            if let Some(name) = truthy_str(body, "lagName") {
                lag.lag_name = name.to_string();
            }
            if let Some(minimum) = int_at(body, "minimumLinks") {
                lag.minimum_links = minimum;
            }
            if let Some(mode) = truthy_str(body, "encryptionMode") {
                lag.encryption_mode = mode.to_string();
            }
            ok(lag_dict(st, lag))
        }
        None => lag_not_found(lag_id, region),
    }
}

fn associate_connection_with_lag(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(lag_id) = truthy_str(body, "lagId") else {
        return lag_not_found("", region);
    };
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        return connection_not_found("", region);
    };
    if !st.lags.contains_key(lag_id) {
        return lag_not_found(lag_id, region);
    }
    let Some(conn) = st.connections.get_mut(connection_id) else {
        return connection_not_found(connection_id, region);
    };
    conn.lag_id = Some(lag_id.to_string());
    st.lags
        .get_mut(lag_id)
        .map(|l| l.connections.push(conn_id(conn)))
        .expect("lag checked above");
    let record = conn.clone();
    ok(connection_dict(st, &record))
}

/// Downcast the just-borrowed connection's id — `connection_id` is not
/// hash-keyed here, the helpers borrow in sequence.
fn conn_id(conn: &Connection) -> String {
    conn.connection_id.clone()
}

/// The inverse of `associate_connection_with_lag`.
fn disassociate_connection_from_lag(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(lag_id) = truthy_str(body, "lagId") else {
        return lag_not_found("", region);
    };
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        return connection_not_found("", region);
    };
    let Some(lag) = st.lags.get_mut(lag_id) else {
        return lag_not_found(lag_id, region);
    };
    lag.connections.retain(|id| id != connection_id);
    let Some(conn) = st.connections.get_mut(connection_id) else {
        return connection_not_found(connection_id, region);
    };
    conn.lag_id = None;
    let record = conn.clone();
    ok(connection_dict(st, &record))
}

/// Hosted connections allocated on this connection
/// (`AllocateHostedConnection` rows whose parent is `connectionId`).
fn describe_hosted_connections(st: &DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        return connection_not_found("", region);
    };
    if !st.connections.contains_key(connection_id) {
        return connection_not_found(connection_id, region);
    }
    let conns: Vec<Value> = st
        .connections
        .values()
        .filter(|c| c.parent_id.as_deref() == Some(connection_id))
        .filter(|c| c.owner_account.is_some())
        .map(|c| connection_dict(st, c))
        .collect();
    ok(json!({ "connections": conns }))
}

/// Allocate a hosted connection: `ordering` state under its parent, owned by
/// the requested `ownerAccount`, carrying the caller's `vlan`.
fn allocate_hosted_connection(
    st: &mut DxStore,
    _account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let Some(parent_id) = truthy_str(body, "connectionId") else {
        return connection_not_found("", region);
    };
    let Some(location) = st.connections.get(parent_id).map(|c| c.location.clone()) else {
        return connection_not_found(parent_id, region);
    };
    let params = body
        .get("connectionName")
        .and_then(Value::as_str)
        .map(|n| json!({"connectionName": n}))
        .unwrap_or(Value::Null);
    let mut conn = build_connection(st, _account, region, &params, None);
    conn.location = location;
    conn.bandwidth = string_at(body, "bandwidth").unwrap_or("").to_string();
    conn.connection_state = "ordering".to_string();
    conn.owner_account = string_at(body, "ownerAccount").map(String::from);
    conn.parent_id = Some(parent_id.to_string());
    conn.vlan = int_at(body, "vlan");
    if let Some(tags) = truthy_array(body.get("tags")) {
        tag_resource(st, &conn.connection_id, Some(tags));
    }
    st.connections.insert(conn.connection_id.clone(), conn.clone());
    ok(connection_dict(st, &conn))
}

/// Move a hosted connection under another connection.
fn associate_hosted_connection(st: &mut DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        return connection_not_found("", region);
    };
    let Some(parent_id) = truthy_str(body, "parentConnectionId") else {
        return connection_not_found("", region);
    };
    let Some(location) = st.connections.get(parent_id).map(|c| c.location.clone()) else {
        return connection_not_found(parent_id, region);
    };
    let Some(conn) = st.connections.get_mut(connection_id) else {
        return connection_not_found(connection_id, region);
    };
    conn.parent_id = Some(parent_id.to_string());
    conn.location = location;
    let record = conn.clone();
    ok(connection_dict(st, &record))
}

// ---------------------------------------------------------------------------
// wave-12 operations — metadata, agreements, locations, Loa
// ---------------------------------------------------------------------------

/// The location catalogue: a static, deterministic subset of the real list.
const LOCATIONS: [(&str, &str, &str); 4] = [
    ("EqDC2", "US, Ashburn", "us-east-1"),
    ("EqSV5", "US, Palo Alto", "us-west-1"),
    ("EqSeD", "US, Seattle", "us-west-2"),
    ("EuC1", "EU, Frankfurt", "eu-central-1"),
];

fn describe_locations() -> Response {
    let locations: Vec<Value> = LOCATIONS
        .iter()
        .map(|(code, name, region)| {
            json!({
                "availableMacSecPortSpeeds": ["10Gbps", "100Gbps"],
                "availablePortSpeeds": ["1Gbps", "10Gbps", "100Gbps"],
                "availableProviders": ["Equinix", "Interxion", "Digital Realty"],
                "locationCode": code,
                "locationName": name,
                "region": region,
            })
        })
        .collect();
    ok(json!({ "locations": locations }))
}

/// Classic virtual private gateways are an EC2-side concept the twin does not
/// model; the describe answers the declared shape with an empty list.
fn describe_virtual_gateways() -> Response {
    ok(json!({ "virtualGateways": [] }))
}

fn describe_customer_metadata(st: &DxStore) -> Response {
    let agreements: Vec<Value> = st
        .agreements
        .iter()
        .map(|(name, status)| json!({ "agreementName": name, "status": status }))
        .collect();
    ok(json!({
        "agreements": agreements,
        "nniPartnerType": "nonPartner",
    }))
}

fn confirm_customer_agreement(st: &mut DxStore, body: &Value) -> Response {
    let name = truthy_str(body, "agreementName")
        .unwrap_or("AWS Direct Connect Customer Agreement")
        .to_string();
    st.agreements.insert(name, "signed".to_string());
    ok(json!({ "status": "signed" }))
}

/// The deterministic mock Loa document (botocore `Loa`).
fn mock_loa() -> Value {
    // base64("mock LOA document bytes (rotocore twin)") — stable bytes so
    // responses rehearse byte-for-byte.
    json!({
        "loaContent": "bW9jayBMT0EgZG9jdW1lbnQgYnl0ZXMgKHJvdG9jb3JlIHR3aW4p",
        "loaContentType": "application/pdf",
    })
}

/// botocore `DescribeLoa` returns the Loa structure directly.
fn describe_loa(st: &DxStore, body: &Value, region: &str) -> Response {
    let Some(connection_id) = truthy_str(body, "connectionId") else {
        return connection_not_found("", region);
    };
    match st.connections.get(connection_id) {
        Some(_) => ok(mock_loa()),
        None => connection_not_found(connection_id, region),
    }
}

/// botocore `DescribeConnectionLoa` wraps the Loa in `{loa: ...}`.
fn describe_connection_loa(st: &DxStore, body: &Value, region: &str) -> Response {
    match describe_loa(st, body, region) {
        r if r.status == 200 => ok(json!({ "loa": mock_loa() })),
        r => r,
    }
}



// ---------------------------------------------------------------------------
// Tags (moto's TaggingService semantics)
// ---------------------------------------------------------------------------

/// `TaggingService.tag_resource`: keys overwrite, members without a value
/// store null. `directconnect` calls it with `tags if tags else []`, so an
/// empty list still creates an (untagged) row, just like moto.
fn tag_resource(st: &mut DxStore, arn: &str, tags: Option<&Vec<Value>>) {
    let Some(tags) = tags else {
        return;
    };
    let row = st.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        let Some(key) = tag
            .get("key")
            .and_then(Value::as_str)
            .or_else(|| tag.get("Key").and_then(Value::as_str))
        else {
            continue;
        };
        // A member without `value` (or with a null value) stores null.
        let value = tag
            .get("value")
            .and_then(Value::as_str)
            .or_else(|| tag.get("Value").and_then(Value::as_str))
            .map(String::from);
        row.insert(key.to_string(), value);
    }
}

fn untag_resource(st: &mut DxStore, arn: &str, tag_keys: &[String]) {
    if let Some(row) = st.tags.get_mut(arn) {
        for key in tag_keys {
            row.remove(key);
        }
    }
}

/// moto's `DirectConnectBackend.list_tags_for_resource`:
/// `[{key, value}]`, empty when the row is absent or untagged.
fn list_tags_for_resource(st: &DxStore, arn: &str) -> Vec<Value> {
    st.tags
        .get(arn)
        .map(|row| {
            row.iter()
                .map(|(k, v)| json!({ "key": k, "value": tag_value(v.as_deref()) }))
                .collect()
        })
        .unwrap_or_default()
}

fn tag_value(value: Option<&str>) -> Value {
    match value {
        Some(v) => json!(v),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn string_at<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    body.get(key).and_then(Value::as_str)
}

/// Only a present, non-empty string passes moto's `if x:` check.
fn truthy_str<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    string_at(body, key).filter(|s| !s.is_empty())
}

fn truthy_array(value: Option<&Value>) -> Option<&Vec<Value>> {
    value.and_then(Value::as_array).filter(|a| !a.is_empty())
}

/// moto truthiness: null/false/0/empty-string/empty-list/empty-object falsy.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn str_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

/// First `maxResults` pairs with an opaque `nextToken`: the token is the id
/// of the first entry of the following page, and resumption emits everything
/// at or after it (so unknown tokens yield an empty, valid page).
fn page_pairs(
    items: Vec<(String, Value)>,
    max_results: Option<i64>,
    next_token: Option<&str>,
) -> (Vec<Value>, Value) {
    let start = next_token
        .map(|token| items.partition_point(|(id, _)| id.as_str() < token))
        .unwrap_or(0);
    let window = &items[start..];
    let mut len = window.len();
    let mut token = Value::Null;
    if let Some(max) = max_results.filter(|m| *m >= 0 && (*m as usize) < window.len()) {
        len = max as usize;
        token = json!(window[len].0.clone());
    }
    (
        window[..len].iter().map(|(_, v)| v.clone()).collect(),
        token,
    )
}

/// Integer body members (`maxResults`, `vlan`, `minimumLinks`, ...).
fn int_at(body: &Value, key: &str) -> Option<i64> {
    body.get(key).and_then(Value::as_i64)
}

/// `arn:aws:directconnect:{region}:{account}:{resource}` — moto's id prefix.
fn dx_arn(region: &str, account: u64, resource: &str, id: &str) -> String {
    format!("arn:aws:directconnect:{region}:{account}:{resource}/{id}")
}

/// moto's `datetime.now().strftime("%Y-%m-%d %H:%M:%S")`.
fn dx_time() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

/// moto's id timestamp `strftime("%Y%m%d%H%M%S")`.
fn dx_time_ts() -> String {
    chrono::Utc::now().format("%Y%m%d%H%M%S").to_string()
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

fn connection_not_found(connection_id: &str, region: &str) -> Response {
    json_error(
        400,
        "ConnectionNotFound",
        &format!("{connection_id} does not match any connections in region {region}."),
    )
}

fn lag_not_found(lag_id: &str, region: &str) -> Response {
    json_error(
        400,
        "LagNotFound",
        &format!("{lag_id} does not match any LAGs in region {region}."),
    )
}

fn mac_sec_key_not_found(secret_arn: &str, connection_id: &str) -> Response {
    json_error(
        400,
        "MacSecKeyNotFound",
        &format!("Secret ARN '{secret_arn}' not found for connection {connection_id}."),
    )
}

fn connection_id_missing() -> Response {
    json_error(400, "ConnectionIdMissing", "The connection ID is missing.")
}

/// botocore's declared `DirectConnectClientException` — the 400 shape for the
/// wave-12 families (the fork's own ConnectionNotFound vocabulary stays for
/// connections/LAGs).
fn dx_client_error(message: impl AsRef<str>) -> Response {
    json_error(400, "DirectConnectClientException", message.as_ref())
}

fn interconnect_not_found(interconnect_id: &str, region: &str) -> Response {
    dx_client_error(format!(
        "{interconnect_id} does not match any interconnects in region {region}."
    ))
}

fn vif_not_found(vif_id: &str) -> Response {
    dx_client_error(format!("{vif_id} does not match any virtual interfaces."))
}

fn gateway_not_found(gateway_id: &str) -> Response {
    dx_client_error(format!(
        "{gateway_id} does not match any Direct Connect gateways."
    ))
}

/// moto's crash-shaped 500 for its unguarded `NotImplementedError`.
fn internal_error_500(message: &str) -> Response {
    json_error(500, "InternalError", message)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(200_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "directconnect".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("directconnect dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn create_connection(account: u64, name: &str) -> Value {
        let (status, body) = send(
            account,
            "CreateConnection",
            json!({
                "location": "EqDC2",
                "bandwidth": "1Gbps",
                "connectionName": name,
            }),
        );
        assert_eq!(status, 200, "create failed: {body}");
        body
    }

    #[test]
    fn connection_lifecycle() {
        let account = next_account();
        let created = create_connection(account, "life-conn");
        assert_eq!(created["connectionState"], "available");
        let cid = created["connectionId"].as_str().unwrap().to_string();
        assert!(cid.starts_with("arn:aws:directconnect:us-east-1:"));
        assert!(cid.contains(":dx-con/dx-moto-life-"));
        // moto's static mock fields.
        assert_eq!(created["awsDevice"], "mock_device");
        assert_eq!(created["awsDeviceV2"], "mock_device_v2");
        assert_eq!(created["awsLogicalDeviceId"], "mock_logical_device_id");
        assert_eq!(created["partnerName"], "mock_partner");
        assert_eq!(created["portEncryptionStatus"], "Encryption Down");
        assert_eq!(created["hasLogicalRedundancy"], false);
        assert_eq!(created["jumboFrameCapable"], false);
        assert_eq!(created["vlan"], 0);
        assert_eq!(created["region"], "us-east-1");
        assert!(created["loaIssueTime"].as_str().unwrap().len() == 19);
        // lagId / providerName absent from the request → moto's null members.
        assert!(created.get("lagId").map(Value::is_null).unwrap_or(false));

        let (status, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": cid }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connections"][0]["bandwidth"], "1Gbps");
        assert_eq!(body["connections"][0]["connectionName"], "life-conn");

        // Update renames; delete marks deleted but keeps the record.
        let (status, body) = send(
            account,
            "UpdateConnection",
            json!({ "connectionId": cid, "connectionName": "renamed" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connectionName"], "renamed");

        let (status, _) = send(account, "DeleteConnection", json!({ "connectionId": cid }));
        assert_eq!(status, 200);

        let (status, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": cid }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connections"][0]["connectionState"], "deleted");
        assert_eq!(body["connections"][0]["connectionName"], "renamed");
    }

    #[test]
    fn describe_unknown_is_connection_not_found() {
        let account = next_account();
        let (status, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": "dxcon-nope" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");
        assert_eq!(
            body["message"],
            "dxcon-nope does not match any connections in region us-east-1."
        );
    }

    #[test]
    fn describe_empty_lists_all() {
        let account = next_account();
        create_connection(account, "first");
        create_connection(account, "second");
        let (status, body) = send(account, "DescribeConnections", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["connections"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn accounts_are_isolated() {
        let a = next_account();
        let b = next_account();
        let a_conn = create_connection(a, "mine");
        let b_conn = create_connection(b, "mine");
        // a's record is invisible in b's account scope.
        let (status, body) = send(
            b,
            "DescribeConnections",
            json!({ "connectionId": a_conn["connectionId"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");
        // Blind (no-id) describe in b lists only b's own record.
        let (_, body) = send(b, "DescribeConnections", json!({}));
        let conns = body["connections"].as_array().unwrap();
        assert_eq!(conns.len(), 1);
        assert_eq!(conns[0]["connectionId"], b_conn["connectionId"]);
    }

    #[test]
    fn create_connection_macsec_shape() {
        let account = next_account();
        let (_, plain) = send(
            account,
            "CreateConnection",
            json!({"location": "L", "bandwidth": "B", "connectionName": "plain"}),
        );
        // Absent requestMACSec renders moto's null member (botocore strips it).
        assert!(plain
            .get("macSecCapable")
            .map(|v| v.is_null())
            .unwrap_or(true));
        assert_eq!(plain["encryptionMode"], "no_encrypt");
        assert_eq!(plain["macSecKeys"], json!([]));
        assert_eq!(plain["portEncryptionStatus"], "Encryption Down");

        let (_, macsec) = send(
            account,
            "CreateConnection",
            json!({
                "location": "L",
                "bandwidth": "B",
                "connectionName": "macsec",
                "requestMACSec": true,
            }),
        );
        assert_eq!(macsec["encryptionMode"], "must_encrypt");
        assert_eq!(macsec["macSecCapable"], true);
        let key = &macsec["macSecKeys"][0];
        assert_eq!(key["secretARN"], "mock_secret_arn");
        assert_eq!(key["ckn"], "mock_ckn");
        assert_eq!(key["state"], "associated");
        assert_eq!(key["startOn"].as_str().unwrap().len(), 19);
        assert!(key.get("cak").is_none(), "cak never renders");
    }

    #[test]
    fn tag_roundtrip() {
        let account = next_account();
        // Tags on create land in the tagger and the record's own dict.
        let (_, created) = send(
            account,
            "CreateConnection",
            json!({
                "location": "L",
                "bandwidth": "B",
                "connectionName": "tagged",
                "tags": [{"key": "env", "value": "dev"}],
            }),
        );
        let cid = created["connectionId"].as_str().unwrap().to_string();
        assert_eq!(created["tags"], json!([{ "key": "env", "value": "dev" }]));

        // The tagging shows up on describe_tags too.
        let (status, body) = send(account, "DescribeTags", json!({ "resourceArns": [cid] }));
        assert_eq!(status, 200);
        assert_eq!(
            body["resourceTags"][0]["tags"],
            json!([{ "key": "env", "value": "dev" }])
        );

        send(
            account,
            "TagResource",
            json!({ "resourceArn": cid, "tags": [{"key": "k1", "value": "v1"}] }),
        );
        // TagResource adds keys; the earlier create-tag survives. An
        // untagged arn still renders a row with an empty tag list.
        let (_, body) = send(
            account,
            "DescribeTags",
            json!({ "resourceArns": [cid, "arn:unknown"] }),
        );
        assert_eq!(body["resourceTags"][0]["resourceArn"], json!(cid));
        assert_eq!(
            body["resourceTags"][0]["tags"],
            json!([
                { "key": "env", "value": "dev" },
                { "key": "k1", "value": "v1" }
            ])
        );
        assert_eq!(body["resourceTags"][1]["tags"], json!([]));

        let (_, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": cid }),
        );
        assert_eq!(
            body["connections"][0]["tags"],
            json!([
                { "key": "env", "value": "dev" },
                { "key": "k1", "value": "v1" }
            ])
        );

        send(
            account,
            "UntagResource",
            json!({ "resourceArn": cid, "tagKeys": ["k1"] }),
        );
        let (_, body) = send(account, "DescribeTags", json!({ "resourceArns": [cid] }));
        assert_eq!(
            body["resourceTags"][0]["tags"],
            json!([{ "key": "env", "value": "dev" }])
        );
    }

    #[test]
    fn mac_sec_associate_and_disassociate() {
        let account = next_account();
        let created = create_connection(account, "mac");
        let cid = created["connectionId"].as_str().unwrap().to_string();

        let (status, body) = send(
            account,
            "AssociateMacSecKey",
            json!({"connectionId": cid, "ckn": "ckn", "cak": "cak"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connectionId"], json!(cid));
        assert_eq!(body["macSecKeys"][0]["secretARN"], "mock_secret_arn");
        assert_eq!(body["macSecKeys"][0]["state"], "associated");

        let (status, body) = send(
            account,
            "DisassociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "mock_secret_arn"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["macSecKeys"][0]["state"], "disassociated");

        // The key list is empty now — a second disassociate is moto's
        // ConnectionNotFound.
        let (status, body) = send(
            account,
            "DisassociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "arn:gone"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");

        // Unknown connection on both operations.
        for op in ["AssociateMacSecKey", "DisassociateMacSecKey"] {
            let (status, body) = send(
                account,
                op,
                json!({"connectionId": "dxcon-nope", "secretARN": "s"}),
            );
            assert_eq!(status, 400);
            assert_eq!(body["__type"], "ConnectionNotFound", "{op}");
        }
    }

    #[test]
    fn mac_sec_key_not_found_error_shape() {
        let account = next_account();
        let created = create_connection(account, "mknf");
        let cid = created["connectionId"].as_str().unwrap().to_string();
        send(
            account,
            "AssociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "a", "ckn": "c", "cak": "k"}),
        );
        send(
            account,
            "AssociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "b", "ckn": "c", "cak": "k"}),
        );
        let (status, body) = send(
            account,
            "DisassociateMacSecKey",
            json!({"connectionId": cid, "secretARN": "gone"}),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "MacSecKeyNotFound");
        assert_eq!(
            body["message"],
            format!("Secret ARN 'gone' not found for connection {cid}.")
        );
    }

    #[test]
    fn lag_lifecycle_and_children() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateLag",
            json!({
                "numberOfConnections": 2,
                "location": "EqDC2",
                "connectionsBandwidth": "1Gbps",
                "lagName": "life-lag",
            }),
        );
        assert_eq!(status, 200);
        let lag_id = body["lagId"].as_str().unwrap().to_string();
        assert!(lag_id.contains(":dxlag/dxlag-moto-life-lag-"));
        assert_eq!(body["lagState"], "available");
        assert_eq!(body["connectionsBandwidth"], "1Gbps");
        assert_eq!(body["numberOfConnections"], 2);
        assert_eq!(body["minimumLinks"], 0);
        assert_eq!(body["connections"].as_array().unwrap().len(), 2);
        for (i, child) in body["connections"].as_array().unwrap().iter().enumerate() {
            assert_eq!(
                child["connectionName"],
                format!("Requested Connection {} for Lag {lag_id}", i + 1)
            );
            assert_eq!(child["bandwidth"], "1Gbps");
            assert_eq!(child["lagId"], json!(lag_id));
            // Children also register at the top level, like moto's dict put.
            let (_, desc) = send(
                account,
                "DescribeConnections",
                json!({ "connectionId": child["connectionId"] }),
            );
            assert_eq!(
                desc["connections"][0]["connectionName"],
                child["connectionName"]
            );
        }

        let (status, body) = send(account, "DescribeLags", json!({ "lagId": lag_id }));
        assert_eq!(status, 200);
        assert_eq!(body["lags"][0]["lagId"], json!(lag_id));

        let (status, body) = send(account, "DescribeLags", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["lags"].as_array().unwrap().len(), 1);

        let (status, body) = send(account, "DescribeLags", json!({ "lagId": "dxlag-nope" }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "LagNotFound");
        assert_eq!(
            body["message"],
            "dxlag-nope does not match any LAGs in region us-east-1."
        );
    }

    #[test]
    fn lag_mac_sec_keys_propagate_to_children() {
        let account = next_account();
        let (_, lag) = send(
            account,
            "CreateLag",
            json!({
                "numberOfConnections": 1,
                "location": "L",
                "connectionsBandwidth": "B",
                "lagName": "lagkey",
            }),
        );
        let lag_id = lag["lagId"].as_str().unwrap().to_string();
        let child_id = lag["connections"][0]["connectionId"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, body) = send(
            account,
            "AssociateMacSecKey",
            json!({"connectionId": lag_id, "secretARN": "arn:s", "ckn": "c", "cak": "k"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["connectionId"], json!(lag_id));
        assert_eq!(body["macSecKeys"][0]["secretARN"], "arn:s");

        // moto re-binds the member's list to the LAG's on every call.
        let (_, child) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": child_id }),
        );
        assert_eq!(
            child["connections"][0]["macSecKeys"][0]["secretARN"],
            "arn:s"
        );

        let (status, body) = send(
            account,
            "DisassociateMacSecKey",
            json!({"connectionId": lag_id, "secretARN": "arn:s"}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["macSecKeys"][0]["state"], "disassociated");

        // The pop drops it from the LAG and its members (shared-list alias).
        let (_, child) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": child_id }),
        );
        assert_eq!(child["connections"][0]["macSecKeys"], json!([]));
    }

    #[test]
    fn lag_child_tags_and_macsec_propagation() {
        let account = next_account();
        let (_, lag) = send(
            account,
            "CreateLag",
            json!({
                "numberOfConnections": 1,
                "location": "L",
                "connectionsBandwidth": "B",
                "lagName": "mlag",
                "requestMACSec": true,
                "tags": [{"key": "lt", "value": "lv"}],
                "childConnectionTags": [{"key": "ct", "value": "cv"}],
            }),
        );
        assert_eq!(lag["encryptionMode"], "must_encrypt");
        assert_eq!(lag["macSecCapable"], true);
        assert_eq!(lag["macSecKeys"][0]["secretARN"], "mock_secret_arn");
        assert_eq!(lag["tags"], json!([{ "key": "lt", "value": "lv" }]));
        let child = &lag["connections"][0];
        assert_eq!(child["tags"], json!([{ "key": "ct", "value": "cv" }]));
        assert_eq!(child["macSecCapable"], true);
        assert_eq!(child["encryptionMode"], "must_encrypt");
        assert_eq!(child["macSecKeys"][0]["secretARN"], "mock_secret_arn");
    }

    #[test]
    fn lag_with_connection_id_is_moto_500() {
        let account = next_account();
        let created = create_connection(account, "lagcid");
        let cid = created["connectionId"].as_str().unwrap().to_string();
        let (status, body) = send(
            account,
            "CreateLag",
            json!({
                "numberOfConnections": 1,
                "location": "L",
                "connectionsBandwidth": "B",
                "lagName": "x",
                "connectionId": cid,
            }),
        );
        assert_eq!(status, 500);
        assert_eq!(body["__type"], "InternalError");
        assert_eq!(
            body["message"],
            "creating a lag with a connection_id is not currently supported by moto"
        );
    }

    #[test]
    fn update_connection_unknown_is_not_found() {
        let account = next_account();
        let (status, body) = send(
            account,
            "UpdateConnection",
            json!({ "connectionId": "dxcon-nope", "connectionName": "n" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");
        assert_eq!(
            body["message"],
            "dxcon-nope does not match any connections in region us-east-1."
        );
    }

    #[test]
    fn unsupported_operations_answer_501() {
        let account = next_account();
        let (status, body) = send(account, "DescribeLocations", json!({}));
        assert_eq!(status, 501);
        assert_eq!(body["__type"], "NotImplemented");
        assert_eq!(
            body["message"],
            "The DescribeLocations action has not been implemented"
        );

        let (status, body) = send(
            account,
            "CreatePrivateVirtualInterface",
            json!({ "connectionId": "dxcon-x", "newPrivateVirtualInterface": {} }),
        );
        assert_eq!(status, 501);
        assert_eq!(
            body["message"],
            "The CreatePrivateVirtualInterface action has not been implemented"
        );
    }

    #[test]
    fn regions_are_isolated() {
        let account = next_account();
        let request_body = json!({"location": "L", "bandwidth": "B", "connectionName": "r"});
        let request = |region: &str| Request {
            service: "directconnect".to_string(),
            operation: "CreateConnection".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&request_body).unwrap()),
            account,
            region: region.to_string(),
        };
        let east = serde_json::from_slice::<Value>(
            &dispatch(&request("us-east-1")).expect("dispatch").body,
        )
        .unwrap();
        let west = serde_json::from_slice::<Value>(
            &dispatch(&request("eu-west-1")).expect("dispatch").body,
        )
        .unwrap();
        assert_eq!(east["region"], "us-east-1");
        assert_eq!(west["region"], "eu-west-1");
        // west's record is invisible in the us-east-1 scope.
        let (status, body) = send(
            account,
            "DescribeConnections",
            json!({ "connectionId": west["connectionId"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ConnectionNotFound");
        // Region-scoped stores: west's id embeds its region, so identical
        // requests in two regions are distinct records.
        let (_, body) = send(account, "DescribeConnections", json!({}));
        let conns = body["connections"].as_array().unwrap();
        assert_eq!(conns.len(), 1);
        assert_eq!(conns[0]["connectionId"], east["connectionId"]);
    }
}
