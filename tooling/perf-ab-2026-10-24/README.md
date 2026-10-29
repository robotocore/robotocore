# perf-ab-2026-10-24 — released python image vs new rust image

Comparison of the currently released `ghcr.io/robotocore/robotocore:latest`
(2026.10.16.dev0+g19331db35, arm64, python 3.12.15 gateway, 48 native + 108
moto-routed services) against the new pure-Rust image built from rust
HEAD (distroless default target, commit 4d97b05b).

Overlapping functionality = the six API families served natively in BOTH
releases (STS/S3/SQS/DynamoDB/IAM/EC2 — the released image lists them
`type: native`, i.e. run by the same Rust core inside its python gateway).
Lambda *code execution* is not overlap: the released image answers
CreateFunction with a 400 while the rust twin executes handler code
(in-process python/nodejs/custom, or every AWS runtime image via
LAMBDA_EXECUTOR=docker), so execution was excluded from A/B.

Driver: bench.py (adapted from the perf-ab-2026-10-21 lane: identical
boto3 workloads serial + 4-thread concurrency; client-side timing;
docker-stats sampler thread for cpu%/mem peaks). results.json /
results2.json hold raw percentiles; the resources block computes
docker stats samples every ~0.6s during each phase.

Caveats: client-side timing includes the python client's own cost,
equal on both sides; the released image's fast paths ride its same
Rust core, so deltas below measure the python event-loop shell around
it (plus its sidecar routing for the 108 moto-typed services).
