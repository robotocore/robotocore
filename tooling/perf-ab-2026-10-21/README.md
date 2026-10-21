# perf A/B: robotocore (Rust) vs moto 5.2.4 (Python) — 2026-10-21

Run on the same host, one harness process, boto3 client-only timing
(identical workloads against both servers; `results.json` has the raw
numbers, `bench.py` is the harness). servers:

- rust:  `robotocore-rust` (this repo, release build, fresh state, :4566)
- moto:  standalone `moto_server` 5.2.4 from the deployed JackDanger/moto
  fork lineage (`python -m moto.server`, :4799)

Client process: CPython 3.13 (repo venv), boto3 1.5x, region us-east-1,
`retries max_attempts=1`. Each op warmed up, N=400 (S3 writes 200,
16 KiB body), serial first; then a 4-thread mixed cycle
(SQS send+receive+delete + DDB put+get + S3 put+get).

## Serial latency (ms)

| op | rust p50 | moto p50 | ratio | rust mean | moto mean |
|---|---|---|---|---|---|
| sts.get_caller_identity | 0.431 | 1.525 | 3.5x | 0.438 | 3.148 |
| s3.put_object 16K | 0.626 | 1.753 | 2.8x | 0.630 | 1.775 |
| s3.get_object 16K | 0.564 | 1.646 | 2.9x | 0.568 | 1.658 |
| s3.list_objects_v2 | 0.588 | 1.854 | 3.2x | 0.592 | 1.880 |
| sqs.send_message | 0.411 | 2.425 | 5.9x | 0.414 | 2.413 |
| sqs.receive_message | 0.425 | 5.052 | 11.9x | 0.429 | 5.854 |
| sqs.delete_message | 0.414 | 1.822 | 4.4x | 0.417 | 2.343 |
| dynamodb.put_item | 0.417 | 1.616 | 3.9x | 0.421 | 1.649 |
| dynamodb.get_item | 0.412 | 1.315 | 3.2x | 0.415 | 1.360 |
| dynamodb.scan Limit=10 | 1.086 | 2.603 | 2.4x | 1.098 | 2.642 |
| ec2.describe_vpcs | 0.452 | 2.270 | 5.0x | 0.457 | 2.276 |
| ec2.describe_instances | 0.412 | 6.270 | 15.2x | 0.415 | 6.269 |
| iam.list_users | 0.426 | 1.583 | 3.7x | 0.430 | 1.698 |

Reading notes:
- the rust p50 floor (~0.41 ms) is the *boto3 client* cost on the same
  harness (client CPU + SigV4), not server time — the twin's own
  `duration_ms` p50 for the same workloads is ~0.03–0.11 ms.
- moto's tails: `sqs.receive` p95 12.9 ms and `ec2.describe_instances`
  6.3 ms at p50 come from Python GC + re-serialization of the *full*
  call book each time.
- identical per-op count (n=400/200) for both backends.

## Concurrency (4 threads, 100 mixed cycles each = 1600 wire ops)

| | throughput (wire ops/s) | wall | RSS |
|---|---|---|---|
| rust | 945.2 | 1.69 s | 27.7 MB |
| moto | 467.3 | 3.42 s | 220.0 MB |

rust sustains 2.0x moto's mixed concurrent throughput while holding
the fleet's full service surface in ~1/8 the memory.

## Startup + footprint

| | binary/python | RSS idle | RSS after A/B |
|---|---|---|---|
| rust | 42.6 MB stripped binary | 8.5 MB | 27.7 MB |
| moto | python 3.13 + moto site, ~189 source modules | 200 MB | 220 MB |

Harness: `bench.py` in this directory (copy of /tmp/perf-ab). Medians
are client-side; the twin's own audit `duration_ms` corroborates the
server-side story (S3 get p50 0.03 ms; put 16K ~0.11 ms).
