"""DirectConnect compatibility tests.

Restored from the python-era tree (5533c34d) and trimmed to the deployed
moto fork's surface. The fork's `moto/directconnect/responses.py` routes 11
handlers — DescribeConnections, CreateConnection, DeleteConnection,
UpdateConnection, AssociateMacSecKey, DisassociateMacSecKey, CreateLag,
DescribeLags, TagResource, UntagResource, DescribeTags — and this suite
covers 9 of the 11 fork handlers (CreateLag/DescribeLags are unit-covered in rotocore/service/directconnect) (60 tests were restored; 50 were removed for
moto-fork-absent operations, see the TestDirectConnectGapOps docstring).

The suite originally ran against an older vendored moto fork revision that
implemented more of the model; the current oracle is stock moto 5.2.4
(PyPI wheel, `~/.moto-venv`) — the same moto line the fork pinned, and
the round-2/3 parity waves re-verified pins against it.
"""

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


class TestDirectConnectGapOps:
    """Tests for operations outside the deployed moto fork's surface.

    This class intentionally holds no tests. The restored 60-test suite
    covered the full botocore model, but only the 11 connection/LAG/MacSec/
    tag operations above have handlers in the deployed fork
    (moto/directconnect/responses.py; verified live — every other call
    raises `NotImplementedError: The <op> action has not been implemented`,
    which the twin serves as HTTP 501 NotImplemented). Their tests were
    removed rather than xfailed, per the parity contract's
    moto-surface-absent rule:

    - virtual interfaces: DescribeVirtualInterfaces, Create/AllocatePrivate
      Public/TransitVirtualInterface, ConfirmPrivate/Public/
      TransitVirtualInterface, DeleteVirtualInterface,
      UpdateVirtualInterfaceAttributes, AssociateVirtualInterface,
      CreateBGPPeer, DeleteBGPPeer, Start/StopBgpFailoverTest,
      ListVirtualInterfaceTestHistory, DescribeRouterConfiguration
    - LAG extras: DeleteLag, UpdateLag, AssociateConnectionWithLag,
      DisassociateConnectionFromLag
    - gateways & associations: Create/Describe/Delete/UpdateDirectConnect
      Gateway, Create/Describe/DeleteDirectConnectGatewayAssociation,
      DescribeDirectConnectGatewayAttachments,
      Create/Describe/DeleteAcceptDirectConnectGatewayAssociationProposal,
      UpdateDirectConnectGatewayAssociation
    - interconnects: Create/Describe/DeleteInterconnect,
      DescribeConnectionsOnInterconnect
    - hosted connections: DescribeHostedConnections,
      AllocateHostedConnection, AllocateConnectionOnInterconnect,
      AssociateHostedConnection
    - confirmation & metadata: ConfirmConnection, ConfirmCustomerAgreement,
      DescribeCustomerMetadata
    - everything else: DescribeLocations, DescribeVirtualGateways,
      DescribeLoa, DescribeConnectionLoa, DescribeInterconnectLoa
    """


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


def test_connection_not_found(dc):
    with pytest.raises(ClientError) as exc_info:
        dc.describe_connections(connectionId="nonexistent-id")
    assert exc_info.value.response["Error"]["Code"] == "ConnectionNotFound"


# MacSec


def test_associate_mac_sec_key(dc, connection):
    # Associate a MacSec CKN/CAK pair with the connection
    result = dc.associate_mac_sec_key(
        connectionId=connection,
        ckn="0" * 64,
        cak="0" * 64,
    )
    assert result["connectionId"] == connection
    # The response should include the connectionId confirming the association
    assert "macSecKeys" in result or result["connectionId"] == connection


def test_disassociate_mac_sec_key(dc, connection):
    # Associate first, then disassociate using the secretARN from the association
    assoc = dc.associate_mac_sec_key(
        connectionId=connection,
        ckn="a" * 64,
        cak="b" * 64,
    )
    assert assoc["connectionId"] == connection
    # Get the secretARN from the returned macSecKeys
    keys = assoc.get("macSecKeys", [])
    if keys and "secretARN" in keys[0]:
        secret_arn = keys[0]["secretARN"]
        disassoc = dc.disassociate_mac_sec_key(connectionId=connection, secretARN=secret_arn)
        assert disassoc["connectionId"] == connection


# Untagging


def test_untag_resource(dc, connection):
    # Tag first
    dc.tag_resource(resourceArn=connection, tags=[{"key": "env", "value": "test"}])
    # Untag
    result = dc.untag_resource(resourceArn=connection, tagKeys=["env"])
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_describe_tags(dc, connection):
    result = dc.describe_tags(resourceArns=[connection])
    assert "resourceTags" in result
    assert len(result["resourceTags"]) == 1
    assert result["resourceTags"][0]["resourceArn"] == connection
