# Known parity-lane artifacts (not fidelity gaps)

Three EC2 suite tests (`TestEC2VPCEndpointServiceConfig*`) create their ELBv2
load balancer with `endpoint_url="http://localhost:4566"` hardcoded
(`_create_nlb`). Against the Python ground-truth lane they pass (one server
serves everything), but in the Rust lane the load balancer lands on the
Python server while the EC2 resources live on the Rust/sidecar processes, so
moto sees a subnet from another backend. This is a test-environment artifact,
not Rust fidelity loss.

The parity metric therefore floors at 99.6% for EC2. The remaining three
tests cannot pass on any Rust server not bound to :4566 without editing the
suite itself. Everything else in the map holds 100%+.

Fourth EC2 suite test, same artifact class (2026-10-19 lane):
`TestEC2RejectVpcEndpointConnections.test_reject_vpc_endpoint_connections`
also calls ELBv2 `CreateLoadBalancer` (via `make_client("elbv2")`) after
creating its VPC/subnet on the native Rust server. The sidecar's moto backend
does not see the Rust EC2 state, so moto rejects the subnet
(`InvalidSubnetID.NotFound`). The three remaining floor tests are the
documented `TestEC2VPCEndpointServiceConfig*` trio; the floor is 752/756 on
the w3z EC2 lane (749 passing + 3 IPv6-CIDR skips) — or 753/756 if a
rust->moto EC2 state mirror runs before elbv2 dispatch. One was prototyped
(scripts/moto_sidecar.py + ROBOTOCORE_CALLBACK_URL in
scripts/start_parity_servers.sh) and withheld from the lane for harness-edit
review; it demonstrably fixed `TestEC2VPCEndpointServiceConfig
.test_create_describe_delete_vpc_endpoint_service_config` and
`test_modify_vpc_endpoint_service_configuration` in a probe run.
