"""S3 metamorphic relations (see conftest.py for the approach)."""

import random

import pytest

from tests.metamorphic.conftest import (
    canonical,
    client,
    fresh_account,
    s3_bodies,
    unique,
)

pytestmark = [pytest.mark.metamorphic]

rng = random.Random(99)


def _bucket(account: str) -> tuple[object, str]:
    s3 = client("s3", account)
    name = unique("meta-s3")
    s3.create_bucket(Bucket=name)
    return s3, name


@pytest.mark.parametrize("body", s3_bodies(), ids=lambda b: f"{len(b)}B")
def test_put_then_get_is_the_exact_body(body):
    """GET returns byte-for-byte what PUT stored — any body."""
    s3, bucket = _bucket(fresh_account())
    key = unique("k")
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert got == body


def test_range_get_is_the_slice_of_the_full_get():
    """For random bodies and random ranges: bytes(r) == full[r]."""
    s3, bucket = _bucket(fresh_account())
    for i, body in enumerate(s3_bodies()):
        key = f"range/{i}"
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        size = len(body)
        ranges = [(0, size - 1)] if size else []
        if size > 2:
            a = rng.randrange(0, size - 1)
            b = rng.randrange(a, size)
            ranges += [(a, b), (0, 0), (size - 1, size - 1), (size // 2, size // 2)]
        for start, end in ranges:
            got = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")["Body"].read()
            assert got == body[start : end + 1], f"bytes={start}-{end} of a {size}-byte body"
        # Suffix form: last N bytes.
        if size:
            n = min(7, size)
            got = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=-{n}")["Body"].read()
            assert got == body[-n:]


def test_v1_v2_and_versions_listings_agree():
    """After mixed writes, ListObjects (v1), ListObjectsV2 and
    ListObjectVersions must agree on the key set they report."""
    s3, bucket = _bucket(fresh_account())
    keys = {f"dir{i}/k{j}.txt" for i in range(2) for j in range(3)} | {"root.txt"}
    for k in keys:
        s3.put_object(Bucket=bucket, Key=k, Body=b"x")
    s3.put_object(Bucket=bucket, Key="dir0/k0.txt", Body=b"x2")  # overwrite

    v1 = {o["Key"] for o in s3.list_objects(Bucket=bucket).get("Contents", [])}
    v2 = {o["Key"] for o in s3.list_objects_v2(Bucket=bucket).get("Contents", [])}
    versions = {o["Key"] for o in s3.list_object_versions(Bucket=bucket).get("Versions", [])}
    # Versions adds nothing for non-versioned buckets and hides nothing.
    assert v1 == keys, "ListObjects must show exactly the stored keys"
    assert v2 == keys, "ListObjectsV2 must agree with ListObjects"
    assert versions == keys, "ListObjectVersions must agree with the others"


def test_pagination_equals_the_full_listing():
    """Walking pages must visit exactly the full single-shot listing,
    in the same order, with no repeats and no omissions."""
    s3, bucket = _bucket(fresh_account())
    keys = [f"{i:02d}/obj-{j}" for i in range(3) for j in range(7)]
    for k in keys:
        s3.put_object(Bucket=bucket, Key=k, Body=b"p")
    keys.sort()  # S3 lists in UTF-8 binary order; these sort the same

    full = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket).get("Contents", [])]
    assert full == keys

    walked = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "MaxKeys": 4}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        walked += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated"):
            break
        token = resp["NextContinuationToken"]
    assert walked == keys, "paged walk must reproduce the full listing exactly"


def test_delete_removes_the_key_from_every_listing():
    s3, bucket = _bucket(fresh_account())
    keep, gone = unique("keep"), unique("gone")
    for k in (keep, gone):
        s3.put_object(Bucket=bucket, Key=k, Body=b"d")
    s3.delete_object(Bucket=bucket, Key=gone)
    for listing in (
        lambda: {o["Key"] for o in s3.list_objects(Bucket=bucket).get("Contents", [])},
        lambda: {o["Key"] for o in s3.list_objects_v2(Bucket=bucket).get("Contents", [])},
        lambda: {o["Key"] for o in s3.list_object_versions(Bucket=bucket).get("Versions", [])},
    ):
        keys = listing()
        assert keep in keys
        assert gone not in keys


def _sub(obj, needle: str, replacement: str):
    """Recursively replace a literal (the script's own random names) in
    every string of a parsed response structure."""
    if isinstance(obj, dict):
        return {k: _sub(v, needle, replacement) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sub(v, needle, replacement) for v in obj]
    if isinstance(obj, str) and needle in obj:
        return obj.replace(needle, replacement)
    return obj


def test_script_replay_across_accounts_is_canonically_identical():
    """The same script against two fresh accounts yields identical
    (canonicalized) responses — no cross-account state bleed, no
    account-shaped formatting drift."""

    def script(account: str) -> tuple[list[str], list[dict]]:
        s3 = client("s3", account)
        sqs = client("sqs", account)
        ssm = client("ssm", account)
        bucket = unique("replay")
        queue = unique("replay")
        param = unique("replay/param")
        out = []
        out.append(s3.create_bucket(Bucket=bucket))
        s3.put_object(Bucket=bucket, Key="a/b", Body=b"replay")
        out.append(s3.head_object(Bucket=bucket, Key="a/b"))
        out.append(s3.get_object(Bucket=bucket, Key="a/b")["Body"].read().hex())
        out.append(s3.list_objects_v2(Bucket=bucket, Prefix="a/"))
        out.append(sqs.create_queue(QueueName=queue))
        queue_url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
        sqs.send_message(QueueUrl=queue_url, MessageBody="m1")
        sqs.send_message(QueueUrl=queue_url, MessageBody="m2")
        out.append(sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=2))
        out.append(ssm.put_parameter(Name=param, Value="v", Type="String"))
        out.append(ssm.get_parameters(Names=["x-nonexistent"]))
        return [bucket, queue, param], out

    a, b = fresh_account(), fresh_account()
    names_a, ra = script(a)
    names_b, rb = script(b)
    assert len(ra) == len(rb)
    for i, (x, y) in enumerate(zip(ra, rb)):
        if isinstance(x, bytes):  # body readbacks are equal raw bytes
            assert x == y
            continue
        # Canonicalize each side against ITS OWN names + account, then diff.
        ca = canonical(x, a)
        cb = canonical(y, b)
        for name in names_a:
            ca = _sub(ca, name, "{NAME}")
        for name in names_b:
            cb = _sub(cb, name, "{NAME}")
        assert ca == cb, f"replay step {i} diverges after canonicalization:\n  A: {ca}\n  B: {cb}"
