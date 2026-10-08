---
session: "fidelity-loop-cycle-vpce"
timestamp: "2026-10-08T21:00:00Z"
model: claude-opus-4-6
reconstructed: true
---

## Human

Continuing the fidelity loop: re-rank the estate's remaining failure groups and
work the top one. The current top blocker is workspace drift on VPC endpoints —
every Terraform plan marks `aws_vpc_endpoint` resources for replacement because
the read-back after apply is missing computed fields (`ip_address_type`,
`dns_options`), so Terraform sees them as unknown after every write. Fix
generically, open upstream PRs, don't special-case the caller.

## Assistant

## Key decisions

**Reproduction**: Isolated snippet against a scratch robotocore server on
127.0.0.1 (private port in the 46xxx range): created a VPC, subnet, security
group, then `create_vpc_endpoint(VpcEndpointType="Interface", ...)` followed by
`describe_vpc_endpoints` — the Describe response omitted `IpAddressType` and
`DnsOptions` while the botocore service model declares both on the `VpcEndpoint`
shape. The genealogy of the bug: the local gateway routes `DescribeVpcEndpoints`
through the vendored moto fork, whose `VPCEndPoint` model never stored either
field, and whose create/modify handlers never read them from the request.

**Where to fix**: rejected a transcription of my first approach — a
hand-written `_describe_vpc_endpoints` XML override in robotocore's EC2
provider. Hand-rolled query-protocol XML drifts from the botocore shape the
moment a member name is guessed (the first attempt emitted a legacy
`DnsOptions` member layout that botocore silently parsed to an empty dict).
The generic fix is additive fields on moto's `VPCEndPoint` model: moto's
`ActionResult` serializer picks response members from the botocore operation
model, so adding the attributes makes create AND describe correct with no
XML written by hand. Deleted the robotocore override entirely.

**AWS semantics checked against botocore's service model** (not guessed):
`CreateVpcEndpoint` input has `IpAddressType` and `DnsOptions={DnsRecordIpType,
PrivateDnsOnlyForInboundResolverEndpoint, ...}`; the response `VpcEndpoint`
member set includes both. Gateway endpoints carry neither field. `ModifyVpcEndpoint`
output is `Return` (not the endpoint object), so moto's existing
`{"Return": True}` was already correct and stayed.

**Two additional round-trip defects found while testing:**
1. `VpcEndpointType` omitted at create → AWS defaults to Gateway and reports
   it; moto kept it unset, so read-back dropped the attribute and snapshot
   tools saw the resource as different. Fixed with an explicit default in the
   response handler.
2. The `groups` property dereferenced security groups deleted while still
   referenced by an endpoint (moto does not enforce the DependencyViolation
   AWS would raise), crashing the whole Describe with AttributeError. Now
   filters missing groups out of the read-back.

**Serializer nuance**: `ip_address_type` is stored normalized to AWS's
response casing ("IPv4"/"Dualstack"/"IPv6") from the lowercase input enum,
and the default `DnsRecordIpType` derives from the endpoint's IP address type,
so repeated read-backs are stable.

**Tests**: 5 live-server integration tests (in-process uvicorn fixture on an
ephemeral port) covering default and dualstack interface round-trips, gateway
shape, modify, and deleted-SG resilience; plus 3 moto tests in the vendored
fork test file. No tests for unimplemented operations were added.

**Placement discipline**: the moto fix went to the fork branch tracked by
robotocore's vendored pin; robotocore ships the commit as a submodule + lock
bump with its own CHANGELOG entry.
