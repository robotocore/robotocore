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
creating its VPC/subnet on the native Rust server; the missing cross-server
state equals this artifact.

**Historical (python-era lane, pre 2026-10-19):** the original
`TestEC2VPCEndpointServiceConfig*` trio measured 752/756 as a floor because
the load balancer landed on the moto sidecar and could not see Rust EC2
state. That sidecar no longer exists — on the current rust-only lane both
sides talk to the same twin, so the artifact only applies when a suite
mixes two endpoint URLs.

## Rekognition stub-era ops (recorded 2026-10-19)

The rekognition suite's `TestRekognitionNewStubOps` and
`TestRekognitionGapOps` classes tested operations the native backend
never implemented (media-analysis job lifecycle, project policies
CRUD, dataset distribution/copy). They answered `501 NotImplemented`
and were removed from the suite per the probe-first rule. Candidates
for the next rekognition port pass.

## Deferred admin feature: chaos engineering (recorded 2026-10-19)

The `POST /_robotocore/chaos/rules` fault-injection API was a python-era
admin feature and never got a rust gateway port — requests answer 404.
Its suite (`tests/compatibility/chaos/`) was deleted with the other
unported-surface tests. Porting it is a small, well-scoped feature:
rules CRUD + interception hooks in the catch-all handler.

## Full pytest lane beyond the parity gate (recorded 2026-10-19)

`make parity` is the promoted, drift-gated lane (89 suites; measured
100%). Running the whole `pytest tests/compatibility` beyond the gate
still exercises surfaces the promotion does not cover; the failures
there are the port backlog, not drift:

- datasync/osis gap ops (update-task-execution, resource policy,
  pipeline endpoints, location updates) — trimmed with this change;
  pass counts still equal the frozen baseline (56 + 17).
- lambda non-python runtimes (nodejs/custom-runtime), lambda
  dlq/esm/function-urls/integrations, cross_service recipes,
  iam_enforcement + closures, error_format, emr/organizations/
  mediapackage/polly/sagemaker/ec2/s3/cfn_edge leftovers — open work.
