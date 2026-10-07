---
session: "admin-fabrication"
timestamp: "2026-10-06T23:10:00Z"
model: claude-opus-4-6
---

## Human (autonomous loop)

An estate-level migration rehearsal cannot seed the workspaces that peer to VPCs with literal ids —
the peer exists outside Terraform, so `CreateVpcPeeringConnection` fails `InvalidVpcID.NotFound`,
every depending workspace is skipped, and rehearsal coverage stays low. Give the twin a way to
fabricate a resource with a caller-chosen identity, driven from an estate-seeding tool's prereq synthesizer.

## Assistant

## Key decisions

**Where the capability lives**: the AWS wire protocol cannot express "create with this id", so this
is an admin-plane endpoint (`POST /_robotocore/ec2/fabricate`), not a request-parsing change. The
motivation is the same shape as the Organizations account-id preset: a rehearsing harness needs to
pre-place objects the code under test refers to by literal id; every caller sees ordinary, real
backend objects afterwards.

**Object creation**: reuse `Ec2Backend.create_vpc` so the default route table, main network ACL and
default security group are built exactly as AWS builds them, then re-bind the caller-chosen id on
the VPC and every object that references the generated one (route tables, network ACLs, the default
security group). Creating under a patched id generator would be a process-global mutation that a
concurrent CreateVpc call could land on; re-binding touches only the fabricated object's state.

**Tests**: unit tests cover id/cidr/tag validation, idempotency, and that a failed batch restores
the re-binding; compat tests run the endpoint against the live server and prove AWS behaviour
(`InvalidVpcID.NotFound` for a missing peer, 200 peering to the fabricated id).

**Scope**: only `kind: "vpc"` today. Other literal-referenced resources (route tables, security
groups) can follow the same pattern without new machinery.
