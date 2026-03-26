"""DirectConnect compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def dc():
    return make_client("directconnect")


@pytest.fixture
def connection(dc):
    conn = dc.create_connection(
        location="EqDC2",
        bandwidth="1Gbps",
        connectionName="test-compat-conn",
    )
    conn_id = conn["connectionId"]
    yield conn_id
    try:
        dc.delete_connection(connectionId=conn_id)
    except ClientError:
        pass


@pytest.fixture
def lag(dc):
    lag = dc.create_lag(
        numberOfConnections=1,
        location="EqDC2",
        connectionsBandwidth="1Gbps",
        lagName="test-compat-lag",
    )
    lag_id = lag["lagId"]
    yield lag_id
    try:
        dc.delete_lag(lagId=lag_id)
    except ClientError:
        pass


@pytest.fixture
def gateway(dc):
    gw = dc.create_direct_connect_gateway(
        directConnectGatewayName="test-compat-gw",
        amazonSideAsn=64512,
    )
    gw_id = gw["directConnectGateway"]["directConnectGatewayId"]
    yield gw_id
    try:
        dc.delete_direct_connect_gateway(directConnectGatewayId=gw_id)
    except ClientError:
        pass


@pytest.fixture
def interconnect(dc):
    ic = dc.create_interconnect(
        interconnectName="test-compat-ic",
        bandwidth="1Gbps",
        location="EqDC2",
    )
    ic_id = ic["interconnectId"]
    yield ic_id
    try:
        dc.delete_interconnect(interconnectId=ic_id)
    except ClientError:
        pass


@pytest.fixture
def private_vif(dc, connection):
    vif = dc.create_private_virtual_interface(
        connectionId=connection,
        newPrivateVirtualInterface={
            "virtualInterfaceName": "test-compat-vif",
            "vlan": 100,
            "asn": 65000,
            "addressFamily": "ipv4",
        },
    )
    vif_id = vif["virtualInterfaceId"]
    yield vif_id
    try:
        dc.delete_virtual_interface(virtualInterfaceId=vif_id)
    except ClientError:
        pass


# Connections


def test_describe_connections_empty(dc):
    result = dc.describe_connections()
    assert "connections" in result


def test_create_and_describe_connection(dc, connection):
    result = dc.describe_connections(connectionId=connection)
    assert len(result["connections"]) == 1
    assert result["connections"][0]["connectionId"] == connection
    assert result["connections"][0]["bandwidth"] == "1Gbps"


def test_create_connection_returns_connection_id(dc):
    conn = dc.create_connection(
        location="EqDC2",
        bandwidth="10Gbps",
        connectionName="test-new-conn",
    )
    assert "connectionId" in conn
    assert "connectionState" in conn
    dc.delete_connection(connectionId=conn["connectionId"])


def test_delete_connection(dc):
    conn = dc.create_connection(
        location="EqDC2",
        bandwidth="1Gbps",
        connectionName="test-delete-conn",
    )
    result = dc.delete_connection(connectionId=conn["connectionId"])
    assert result["connectionId"] == conn["connectionId"]


def test_update_connection(dc, connection):
    result = dc.update_connection(
        connectionId=connection,
        connectionName="updated-name",
    )
    assert result["connectionName"] == "updated-name"


def test_confirm_connection(dc, connection):
    result = dc.confirm_connection(connectionId=connection)
    assert result["connectionState"] == "available"


def test_describe_hosted_connections(dc, connection):
    result = dc.describe_hosted_connections(connectionId=connection)
    assert "connections" in result


def test_connection_not_found(dc):
    with pytest.raises(ClientError) as exc_info:
        dc.describe_connections(connectionId="nonexistent-id")
    assert exc_info.value.response["Error"]["Code"] == "ConnectionNotFound"


# LAGs


def test_create_and_describe_lag(dc, lag):
    result = dc.describe_lags(lagId=lag)
    assert len(result["lags"]) == 1
    assert result["lags"][0]["lagId"] == lag
    assert result["lags"][0]["connectionsBandwidth"] == "1Gbps"


def test_describe_lags_empty(dc):
    result = dc.describe_lags()
    assert "lags" in result


def test_delete_lag(dc):
    lag = dc.create_lag(
        numberOfConnections=1,
        location="EqDC2",
        connectionsBandwidth="1Gbps",
        lagName="test-delete-lag",
    )
    result = dc.delete_lag(lagId=lag["lagId"])
    assert result["lagId"] == lag["lagId"]


def test_update_lag(dc, lag):
    result = dc.update_lag(lagId=lag, lagName="updated-lag-name", minimumLinks=1)
    assert result["lagName"] == "updated-lag-name"
    assert result["minimumLinks"] == 1


def test_associate_connection_with_lag(dc, connection, lag):
    result = dc.associate_connection_with_lag(
        connectionId=connection,
        lagId=lag,
    )
    assert result["connectionId"] == connection
    assert result["lagId"] == lag


def test_disassociate_connection_from_lag(dc, connection, lag):
    dc.associate_connection_with_lag(connectionId=connection, lagId=lag)
    result = dc.disassociate_connection_from_lag(connectionId=connection, lagId=lag)
    assert result["connectionId"] == connection


# Virtual Interfaces


def test_describe_virtual_interfaces_empty(dc):
    result = dc.describe_virtual_interfaces()
    assert "virtualInterfaces" in result


def test_create_private_virtual_interface(dc, private_vif):
    result = dc.describe_virtual_interfaces(virtualInterfaceId=private_vif)
    assert len(result["virtualInterfaces"]) == 1
    assert result["virtualInterfaces"][0]["virtualInterfaceId"] == private_vif
    assert result["virtualInterfaces"][0]["virtualInterfaceType"] == "private"


def test_create_public_virtual_interface(dc, connection):
    vif = dc.create_public_virtual_interface(
        connectionId=connection,
        newPublicVirtualInterface={
            "virtualInterfaceName": "test-public-vif",
            "vlan": 200,
            "asn": 65001,
            "addressFamily": "ipv4",
            "routeFilterPrefixes": [{"cidr": "10.0.0.0/8"}],
        },
    )
    assert vif["virtualInterfaceType"] == "public"
    assert "virtualInterfaceId" in vif
    dc.delete_virtual_interface(virtualInterfaceId=vif["virtualInterfaceId"])


def test_create_transit_virtual_interface(dc, connection, gateway):
    result = dc.create_transit_virtual_interface(
        connectionId=connection,
        newTransitVirtualInterface={
            "virtualInterfaceName": "test-transit-vif",
            "vlan": 300,
            "asn": 65002,
            "addressFamily": "ipv4",
            "directConnectGatewayId": gateway,
        },
    )
    assert "virtualInterface" in result
    vif = result["virtualInterface"]
    assert vif["virtualInterfaceType"] == "transit"
    dc.delete_virtual_interface(virtualInterfaceId=vif["virtualInterfaceId"])


def test_delete_virtual_interface(dc, connection):
    vif = dc.create_private_virtual_interface(
        connectionId=connection,
        newPrivateVirtualInterface={
            "virtualInterfaceName": "test-delete-vif",
            "vlan": 400,
            "asn": 65003,
        },
    )
    result = dc.delete_virtual_interface(virtualInterfaceId=vif["virtualInterfaceId"])
    assert result["virtualInterfaceState"] == "deleted"


def test_update_virtual_interface_attributes(dc, private_vif):
    result = dc.update_virtual_interface_attributes(
        virtualInterfaceId=private_vif,
        virtualInterfaceName="updated-vif-name",
    )
    assert result["virtualInterfaceName"] == "updated-vif-name"


def test_create_bgp_peer(dc, private_vif):
    result = dc.create_bgp_peer(
        virtualInterfaceId=private_vif,
        newBGPPeer={
            "asn": 65100,
            "addressFamily": "ipv4",
        },
    )
    assert "virtualInterface" in result
    assert len(result["virtualInterface"]["bgpPeers"]) >= 1


def test_delete_bgp_peer(dc, private_vif):
    result = dc.delete_bgp_peer(
        virtualInterfaceId=private_vif,
        asn=65000,
    )
    assert "virtualInterface" in result


# Direct Connect Gateways


def test_create_and_describe_gateway(dc, gateway):
    result = dc.describe_direct_connect_gateways(directConnectGatewayId=gateway)
    assert len(result["directConnectGateways"]) == 1
    assert result["directConnectGateways"][0]["directConnectGatewayId"] == gateway


def test_describe_direct_connect_gateways_empty(dc):
    result = dc.describe_direct_connect_gateways()
    assert "directConnectGateways" in result


def test_delete_direct_connect_gateway(dc):
    gw = dc.create_direct_connect_gateway(
        directConnectGatewayName="test-delete-gw",
        amazonSideAsn=64513,
    )
    gw_id = gw["directConnectGateway"]["directConnectGatewayId"]
    result = dc.delete_direct_connect_gateway(directConnectGatewayId=gw_id)
    assert result["directConnectGateway"]["directConnectGatewayState"] == "deleted"


def test_update_direct_connect_gateway(dc, gateway):
    result = dc.update_direct_connect_gateway(
        directConnectGatewayId=gateway,
        newDirectConnectGatewayName="updated-gw-name",
    )
    assert result["directConnectGateway"]["directConnectGatewayName"] == "updated-gw-name"


def test_create_and_describe_gateway_association(dc, gateway):
    assoc = dc.create_direct_connect_gateway_association(
        directConnectGatewayId=gateway,
        gatewayId="vgw-test123",
        addAllowedPrefixesToDirectConnectGateway=[{"cidr": "10.0.0.0/8"}],
    )
    assoc_id = assoc["directConnectGatewayAssociation"]["associationId"]
    result = dc.describe_direct_connect_gateway_associations(
        associationId=assoc_id,
    )
    assert len(result["directConnectGatewayAssociations"]) == 1
    assert result["directConnectGatewayAssociations"][0]["associationId"] == assoc_id
    dc.delete_direct_connect_gateway_association(associationId=assoc_id)


def test_describe_gateway_attachments(dc, gateway):
    result = dc.describe_direct_connect_gateway_attachments(
        directConnectGatewayId=gateway,
    )
    assert "directConnectGatewayAttachments" in result


def test_create_and_describe_gateway_association_proposal(dc, gateway):
    proposal = dc.create_direct_connect_gateway_association_proposal(
        directConnectGatewayId=gateway,
        directConnectGatewayOwnerAccount="123456789012",
        gatewayId="vgw-proposal123",
        addAllowedPrefixesToDirectConnectGateway=[{"cidr": "192.168.0.0/16"}],
    )
    proposal_id = proposal["directConnectGatewayAssociationProposal"]["proposalId"]
    result = dc.describe_direct_connect_gateway_association_proposals(
        proposalId=proposal_id,
    )
    assert len(result["directConnectGatewayAssociationProposals"]) == 1
    dc.delete_direct_connect_gateway_association_proposal(proposalId=proposal_id)


# Interconnects


def test_create_and_describe_interconnect(dc, interconnect):
    result = dc.describe_interconnects(interconnectId=interconnect)
    assert len(result["interconnects"]) == 1
    assert result["interconnects"][0]["interconnectId"] == interconnect
    assert result["interconnects"][0]["bandwidth"] == "1Gbps"


def test_describe_interconnects_empty(dc):
    result = dc.describe_interconnects()
    assert "interconnects" in result


def test_delete_interconnect(dc):
    ic = dc.create_interconnect(
        interconnectName="test-delete-ic",
        bandwidth="1Gbps",
        location="EqDC2",
    )
    result = dc.delete_interconnect(interconnectId=ic["interconnectId"])
    assert result["interconnectState"] == "deleted"


def test_describe_connections_on_interconnect(dc, interconnect):
    result = dc.describe_connections_on_interconnect(interconnectId=interconnect)
    assert "connections" in result


# Misc


def test_describe_locations(dc):
    result = dc.describe_locations()
    assert "locations" in result
    assert len(result["locations"]) > 0
    loc = result["locations"][0]
    assert "locationCode" in loc
    assert "locationName" in loc


def test_describe_virtual_gateways(dc):
    result = dc.describe_virtual_gateways()
    assert "virtualGateways" in result


def test_describe_customer_metadata(dc):
    result = dc.describe_customer_metadata()
    assert "agreements" in result
    assert "nniPartnerType" in result


def test_confirm_customer_agreement(dc):
    result = dc.confirm_customer_agreement()
    assert result["status"] == "signed"


def test_describe_tags(dc, connection):
    result = dc.describe_tags(resourceArns=[connection])
    assert "resourceTags" in result
    assert len(result["resourceTags"]) == 1
    assert result["resourceTags"][0]["resourceArn"] == connection


def test_list_virtual_interface_test_history(dc):
    result = dc.list_virtual_interface_test_history()
    assert "virtualInterfaceTestHistory" in result


def test_describe_loa(dc):
    result = dc.describe_loa(connectionId="fake-conn-id")
    assert "loaContent" in result
    assert "loaContentType" in result


def test_describe_virtual_interfaces_by_connection(dc, connection, private_vif):
    result = dc.describe_virtual_interfaces(connectionId=connection)
    assert "virtualInterfaces" in result
    assert len(result["virtualInterfaces"]) >= 1


def test_allocate_hosted_connection(dc, connection):
    result = dc.allocate_hosted_connection(
        connectionId=connection,
        ownerAccount="123456789012",
        bandwidth="500Mbps",
        connectionName="hosted-conn",
        vlan=100,
    )
    assert "connectionId" in result
    assert result["bandwidth"] == "500Mbps"
    dc.delete_connection(connectionId=result["connectionId"])


def test_allocate_connection_on_interconnect(dc, interconnect):
    result = dc.allocate_connection_on_interconnect(
        bandwidth="1Gbps",
        connectionName="alloc-conn",
        ownerAccount="123456789012",
        interconnectId=interconnect,
        vlan=200,
    )
    assert "connectionId" in result
    assert result["bandwidth"] == "1Gbps"


def test_start_and_stop_bgp_failover_test(dc, private_vif):
    start = dc.start_bgp_failover_test(virtualInterfaceId=private_vif)
    assert "virtualInterfaceTest" in start
    assert start["virtualInterfaceTest"]["status"] == "IN_PROGRESS"

    stop = dc.stop_bgp_failover_test(virtualInterfaceId=private_vif)
    assert "virtualInterfaceTest" in stop
    assert stop["virtualInterfaceTest"]["status"] == "COMPLETED"


def test_router_configuration(dc, private_vif):
    result = dc.describe_router_configuration(virtualInterfaceId=private_vif)
    assert "customerRouterConfig" in result
    assert "virtualInterfaceId" in result
    assert result["virtualInterfaceId"] == private_vif
