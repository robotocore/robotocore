"""Cross-engine differential: run the same corpus against robotocore and
moto (its in-process server) and diff the canonicalized responses.

The twin claims moto 5.x-line semantics — moto is the reference oracle
for everything both engines implement, no live AWS needed. Every
divergence printed by this script is either a robotocore fidelity bug, a
moto bug, or an intentional divergence (record those in
scripts/differential_known_diffs.json as {"corpus.step": reason} so the
gate stays honest).

Usage:
    uv run python scripts/differential_moto.py                 # all corpora
    uv run python scripts/differential_moto.py --corpus s3    # one corpus
"""

import argparse
import datetime
import json
import pathlib as _pathlib
import re
import subprocess
import sys
import time
import urllib.request
import uuid
import zlib
from collections.abc import Callable
from typing import Any

import boto3
from botocore.config import Config

ROBOTOCORE = "http://localhost:4566"
MOTO = "http://localhost:5000"
MOTO_START_TIMEOUT = 90

_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?")
_EPOCH_RE = re.compile(r"\b1[0-9]{12}(?:\.\d+)?\b")
# SQS attribute timestamps are epoch SECONDS with an optional fraction.
_EPOCH_SEC_RE = re.compile(r"\b1[0-9]{9}(?:\.\d+)?\b")
# Both engines append a random 6-char suffix to secret ARNs.
_SECRET_ARN_RE = re.compile(r"(secret:[^\s]*-)[A-Za-z0-9]{6}")
# KMS key ids are per-engine minted state (32-hex vs uuid shapes).
_KMS_KEY_RE = re.compile(r"(arn:aws:kms:[^\s:]*:[^\s:]*:key/)[0-9a-f-]+")
_ARN_ID_RE = re.compile(r"arn:aws:[a-z0-9-]+:[^:]*:(\d{12}):")
_HOST_RE = re.compile(r"https?://[^/]+")

OPAQUE_VALUE_KEYS = {
    "ResponseMetadata",
    "ETag",
    "Marker",
    "NextMarker",
    "ReceiptHandle",
    "AccessKeyId",
    # Per-run minted values: the engines differ in how they mint them and
    # nothing downstream can depend on the exact bytes.
    "SecretAccessKey",
    "CiphertextBlob",
    "UserId",
    "Plaintext",
}


def canonical(obj):
    """Normalize everything that legitimately differs between engines:
    hosts/ports, ids, timestamps, account ids in ARNs, opaque tokens."""
    if isinstance(obj, dict):
        return {k: ("{OPAQUE}" if k in OPAQUE_VALUE_KEYS else canonical(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [canonical(v) for v in obj]
    if isinstance(obj, datetime.datetime):
        return canonical(obj.isoformat())
    if isinstance(obj, bytes):
        return "{BYTES}"
    if isinstance(obj, str):
        s = _HOST_RE.sub("{HOST}", obj)
        s = _SECRET_ARN_RE.sub(r"\1{SUFFIX}", s)
        s = _KMS_KEY_RE.sub(r"\1{KEYID}", s)
        s = _ARN_ID_RE.sub(lambda m: m.group(0).replace(m.group(1), "{ACCOUNT}"), s)
        s = _UUID_RE.sub("{UUID}", s)
        s = _TS_RE.sub("{TS}", s)
        s = _EPOCH_RE.sub("{EPOCH}", s)
        s = _EPOCH_SEC_RE.sub("{EPOCH}", s)
        return s
    return obj


class Engine:
    def __init__(self, engine_name: str, endpoint: str, salt: str):
        self.name = engine_name
        self.endpoint = endpoint
        self.salt = salt  # deterministic per (corpus) — names must MATCH across engines

    def client(self, service: str) -> Any:
        kwargs = {
            "endpoint_url": self.endpoint,
            "region_name": "us-east-1",
            "aws_access_key_id": "123456789012",
            "aws_secret_access_key": "differential",
        }
        if service == "s3":
            kwargs["config"] = Config(s3={"addressing_style": "path"})
        return boto3.client(service, **kwargs)

    def res_name(self, prefix: str) -> str:
        """Deterministic resource name — identical on both engines."""
        return f"{prefix}-{zlib.crc32(self.salt.encode()) & 0xFFFFFFFF:08x}"


def err_wrap(fn):
    """Call a step now; engine errors become comparable error rows (the
    error CODE is itself an observable to match across engines)."""
    try:
        return fn()
    except Exception as e:
        resp = getattr(e, "response", None)
        if resp and "Error" in resp:
            return {"__error__": resp["Error"].get("Code")}
        return {"__error__": type(e).__name__}


CORPORA: dict[str, Callable[[Engine], list[tuple[str, Any]]]] = {}


def corpus(name):
    def reg(fn):
        CORPORA[name] = fn
        return fn

    return reg


@corpus("s3")
def corpus_s3(engine):
    s3 = engine.client("s3")
    b = engine.res_name("diff-bucket")
    out = [("create_bucket", err_wrap(lambda: s3.create_bucket(Bucket=b)))]
    s3.put_object(Bucket=b, Key="a/b.txt", Body=b"hello")
    s3.put_object(Bucket=b, Key="a/c.txt", Body=b"world")
    out += [
        (
            "get_object",
            err_wrap(lambda: s3.get_object(Bucket=b, Key="a/b.txt")["Body"].read().hex()),
        ),
        ("head_object", err_wrap(lambda: s3.head_object(Bucket=b, Key="a/b.txt"))),
        ("list_v2_prefix", err_wrap(lambda: s3.list_objects_v2(Bucket=b, Prefix="a/"))),
        ("list_v1", err_wrap(lambda: s3.list_objects(Bucket=b))),
        (
            "copy_object",
            err_wrap(
                lambda: s3.copy_object(
                    Bucket=b, Key="a/d.txt", CopySource={"Bucket": b, "Key": "a/b.txt"}
                )
            ),
        ),
        (
            "get_copied",
            err_wrap(lambda: s3.get_object(Bucket=b, Key="a/d.txt")["Body"].read().hex()),
        ),
        ("delete_object", err_wrap(lambda: s3.delete_object(Bucket=b, Key="a/c.txt"))),
        ("get_missing", err_wrap(lambda: s3.get_object(Bucket=b, Key="a/c.txt"))),
        ("head_missing", err_wrap(lambda: s3.head_object(Bucket=b, Key="a/c.txt"))),
        ("missing_bucket_get", err_wrap(lambda: s3.get_object(Bucket=b + "-none", Key="x"))),
        # -- conditional requests (moto responses.py:2188-2219, 2286-2296) --
        (
            "get_if_match_wrong",
            err_wrap(lambda: s3.get_object(Bucket=b, Key="a/b.txt", IfMatch='"deadbeef"')),
        ),
        (
            "get_if_modified_since_future",
            err_wrap(
                lambda: s3.get_object(
                    Bucket=b, Key="a/b.txt", IfModifiedSince="Wed, 01 Jan 2031 00:00:00 GMT"
                )
            ),
        ),
        (
            "get_if_none_match_current",
            err_wrap(lambda: s3.get_object(Bucket=b, Key="a/b.txt", IfNoneMatch='"deadbeef"')),
        ),
        (
            "put_if_none_match_star",
            err_wrap(lambda: s3.put_object(Bucket=b, Key="a/b.txt", Body=b"x", IfNoneMatch="*")),
        ),
        (
            "delete_if_match_wrong",
            err_wrap(lambda: s3.delete_object(Bucket=b, Key="a/b.txt", IfMatch='"deadbeef"')),
        ),
        # -- CopyObject contract (moto models.py:3066-3083) -----------------
        (
            "copy_in_place",
            err_wrap(
                lambda: s3.copy_object(
                    Bucket=b, Key="a/b.txt", CopySource={"Bucket": b, "Key": "a/b.txt"}
                )
            ),
        ),
        (
            "copy_in_place_replace",
            err_wrap(
                lambda: s3.copy_object(
                    Bucket=b,
                    Key="a/b.txt",
                    CopySource={"Bucket": b, "Key": "a/b.txt"},
                    MetadataDirective="REPLACE",
                )
            ),
        ),
        (
            "copy_missing_version",
            err_wrap(
                lambda: s3.copy_object(
                    Bucket=b,
                    Key="dst.txt",
                    CopySource={
                        "Bucket": b,
                        "Key": "a/b.txt",
                        "VersionId": "never-existed",
                    },
                )
            ),
        ),
        # -- range semantics (moto responses.py:1785-1836) ----------------
        (
            "range_unsatisfiable",
            err_wrap(lambda: s3.get_object(Bucket=b, Key="a/b.txt", Range="bytes=100-")),
        ),
        (
            "range_suffix_zero",
            err_wrap(lambda: s3.get_object(Bucket=b, Key="a/b.txt", Range="bytes=-0")),
        ),
        (
            "range_suffix",
            err_wrap(
                lambda: (
                    s3.get_object(Bucket=b, Key="a/b.txt", Range="bytes=-2")["Body"].read().hex()
                )
            ),
        ),
        (
            "range_bounded",
            err_wrap(
                lambda: (
                    s3.get_object(Bucket=b, Key="a/b.txt", Range="bytes=0-1")["Body"].read().hex()
                )
            ),
        ),
        (
            "range_open",
            err_wrap(
                lambda: (
                    s3.get_object(Bucket=b, Key="a/b.txt", Range="bytes=1-")["Body"].read().hex()
                )
            ),
        ),
        (
            "range_multiples_200",
            err_wrap(
                lambda: (
                    s3.get_object(Bucket=b, Key="a/b.txt", Range="bytes=0-1,3-4")["Body"]
                    .read()
                    .hex()
                )
            ),
        ),
        # -- versioned DeleteObjects (moto models.py:2942-3046) ------------
    ]
    vb = engine.res_name("diff-ver-bucket")
    s3.create_bucket(Bucket=vb)
    s3.put_bucket_versioning(Bucket=vb, VersioningConfiguration={"Status": "Enabled"})
    v1 = s3.put_object(Bucket=vb, Key="v.txt", Body=b"first")["VersionId"]
    s3.put_object(Bucket=vb, Key="v.txt", Body=b"second")
    out += [
        (
            "delete_objects_markers",
            err_wrap(lambda: s3.delete_objects(Bucket=vb, Delete={"Objects": [{"Key": "v.txt"}]})),
        ),
        (
            "get_after_marker",
            err_wrap(lambda: s3.get_object(Bucket=vb, Key="v.txt")),
        ),
        (
            "list_versions_after_marker",
            err_wrap(lambda: s3.list_object_versions(Bucket=vb)),
        ),
        (
            "delete_objects_explicit_version",
            err_wrap(
                lambda: s3.delete_objects(
                    Bucket=vb,
                    Delete={"Objects": [{"Key": "v.txt", "VersionId": v1["VersionId"]}]},
                )
            ),
        ),
        (
            "list_versions_after_version_delete",
            err_wrap(lambda: s3.list_object_versions(Bucket=vb)),
        ),
        (
            "delete_objects_unknown_key",
            err_wrap(lambda: s3.delete_objects(Bucket=vb, Delete={"Objects": [{"Key": "ghost"}]})),
        ),
        (
            "delete_objects_empty",
            err_wrap(lambda: s3.delete_objects(Bucket=b, Delete={"Objects": []})),
        ),
        # -- multipart validation (moto models.py:473-515) ------------------
    ]
    mp = engine.res_name("diff-mp-bucket")
    s3.create_bucket(Bucket=mp)
    up = s3.create_multipart_upload(Bucket=mp, Key="mp.txt")
    uid = up["UploadId"]
    big = s3.upload_part(Bucket=mp, Key="mp.txt", PartNumber=1, UploadId=uid, Body=b"z" * 5242880)[
        "ETag"
    ]
    tail = s3.upload_part(Bucket=mp, Key="mp.txt", PartNumber=2, UploadId=uid, Body=b"tail")["ETag"]
    out += [
        (
            "complete_multipart",
            err_wrap(
                lambda: s3.complete_multipart_upload(
                    Bucket=mp,
                    Key="mp.txt",
                    UploadId=uid,
                    MultipartUpload={
                        "Parts": [{"PartNumber": 1, "ETag": big}, {"PartNumber": 2, "ETag": tail}]
                    },
                )
            ),
        ),
        (
            "get_multipart_object",
            err_wrap(lambda: s3.get_object(Bucket=mp, Key="mp.txt")["Body"].read()[:16].hex()),
        ),
        (
            "complete_multipart_bad_etag",
            err_wrap(
                lambda: s3.complete_multipart_upload(
                    Bucket=mp,
                    Key="mp2.txt",
                    UploadId=s3.create_multipart_upload(Bucket=mp, Key="mp2.txt")["UploadId"],
                    MultipartUpload={
                        "Parts": [{"PartNumber": 1, "ETag": '"deadbeefdeadbeefdeadbeefdeadbeef"'}]
                    },
                )
            ),
        ),
        (
            "complete_multipart_empty_list",
            err_wrap(
                lambda: s3.complete_multipart_upload(
                    Bucket=mp,
                    Key="mp3.txt",
                    UploadId=s3.create_multipart_upload(Bucket=mp, Key="mp3.txt")["UploadId"],
                    MultipartUpload={"Parts": []},
                )
            ),
        ),
    ]
    # moto models.py:493-495 — a non-final listed part under
    # S3_UPLOAD_PART_MIN_SIZE raises EntityTooSmall
    up4 = s3.create_multipart_upload(Bucket=mp, Key="mp4.txt")["UploadId"]
    small_etag = s3.upload_part(
        Bucket=mp, Key="mp4.txt", PartNumber=1, UploadId=up4, Body=b"small"
    )["ETag"]
    out += [
        (
            "complete_multipart_entity_too_small",
            err_wrap(
                lambda: s3.complete_multipart_upload(
                    Bucket=mp,
                    Key="mp4.txt",
                    UploadId=up4,
                    MultipartUpload={
                        "Parts": [
                            {"PartNumber": 1, "ETag": small_etag},
                            {"PartNumber": 2, "ETag": '"deadbeefdeadbeefdeadbeefdeadbeef"'},
                        ]
                    },
                )
            ),
        ),
        (
            # moto responses.py:3109-3116 — a listed part number <= the
            # previous one raises InvalidPartOrder before any lookup
            "complete_multipart_invalid_part_order",
            err_wrap(
                lambda: s3.complete_multipart_upload(
                    Bucket=mp,
                    Key="mp2.txt",
                    UploadId=s3.create_multipart_upload(Bucket=mp, Key="mp2.txt")["UploadId"],
                    MultipartUpload={
                        "Parts": [
                            {"PartNumber": 3, "ETag": '"00000000000000000000000000000000"'},
                            {"PartNumber": 2, "ETag": '"00000000000000000000000000000000"'},
                        ]
                    },
                )
            ),
        ),
        (
            "upload_part_number_too_big",
            err_wrap(
                lambda: s3.upload_part(
                    Bucket=b,
                    Key="pn.txt",
                    PartNumber=10001,
                    UploadId=s3.create_multipart_upload(Bucket=b, Key="pn.txt")["UploadId"],
                    Body=b"x" * 16,
                )
            ),
        ),
        (
            "abort_multipart_unknown",
            err_wrap(lambda: s3.abort_multipart_upload(Bucket=b, Key="pn.txt", UploadId="gone")),
        ),
        (
            "upload_part_copy_ranged",
            err_wrap(
                lambda: s3.upload_part_copy(
                    Bucket=b,
                    Key="cpart",
                    PartNumber=1,
                    UploadId=s3.create_multipart_upload(Bucket=b, Key="cpart")["UploadId"],
                    CopySource={"Bucket": b, "Key": "a/b.txt"},
                    CopySourceRange="bytes=1-3",
                )
            ),
        ),
    ]
    return out


@corpus("sqs")
def corpus_sqs(engine):
    sqs = engine.client("sqs")
    q = engine.res_name("diff-q")
    out = [("create_queue", err_wrap(lambda: sqs.create_queue(QueueName=q)))]
    url = sqs.get_queue_url(QueueName=q)["QueueUrl"]
    out += [
        (
            "attrs_default",
            err_wrap(lambda: sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])),
        ),
        ("send", err_wrap(lambda: sqs.send_message(QueueUrl=url, MessageBody="body-1"))),
    ]
    first = sqs.receive_message(QueueUrl=url, AttributeNames=["All"])
    out.append(("receive", first))
    out.append(("receive_immediate_recheck", err_wrap(lambda: sqs.receive_message(QueueUrl=url))))
    handle = first["Messages"][0]["ReceiptHandle"]
    out += [
        (
            "set_visibility_zero",
            err_wrap(
                lambda: sqs.change_message_visibility(
                    QueueUrl=url, ReceiptHandle=handle, VisibilityTimeout=0
                )
            ),
        ),
        ("receive_after_zero", err_wrap(lambda: sqs.receive_message(QueueUrl=url))),
        ("delete_queue", err_wrap(lambda: sqs.delete_queue(QueueUrl=url))),
        ("send_to_deleted", err_wrap(lambda: sqs.send_message(QueueUrl=url, MessageBody="x"))),
    ]
    return out


@corpus("dynamodb")
def corpus_dynamodb(engine):
    d = engine.client("dynamodb")
    t = engine.res_name("diff-ddb")
    item = {
        "pk": {"S": "p1"},
        "sk": {"N": "1"},
        "payload": {"M": {"x": {"S": "y"}}},
        "nums": {"L": [{"N": "1"}, {"N": "2.5"}]},
    }
    key = {"pk": {"S": "p1"}, "sk": {"N": "1"}}
    out = [
        (
            "create_table",
            err_wrap(
                lambda: d.create_table(
                    TableName=t,
                    KeySchema=[
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "sk", "KeyType": "RANGE"},
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "sk", "AttributeType": "N"},
                    ],
                    ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
                )
            ),
        )
    ]
    out += [
        ("describe_table", err_wrap(lambda: d.describe_table(TableName=t))),
        ("put_item", err_wrap(lambda: d.put_item(TableName=t, Item=item))),
        ("get_item", err_wrap(lambda: d.get_item(TableName=t, Key=key))),
        (
            "update_item",
            err_wrap(
                lambda: d.update_item(
                    TableName=t,
                    Key=key,
                    UpdateExpression="SET #a = :v ADD ctr :one",
                    ExpressionAttributeNames={"#a": "payload"},
                    ExpressionAttributeValues={":v": {"S": "z"}, ":one": {"N": "5"}},
                    ReturnValues="ALL_NEW",
                )
            ),
        ),
        (
            "query_sk_between",
            err_wrap(
                lambda: d.query(
                    TableName=t,
                    KeyConditionExpression="pk = :p AND sk BETWEEN :lo AND :hi",
                    ExpressionAttributeValues={
                        ":p": {"S": "p1"},
                        ":lo": {"N": "0"},
                        ":hi": {"N": "10"},
                    },
                )
            ),
        ),
        (
            "query_wrong_types",
            err_wrap(
                lambda: d.query(
                    TableName=t,
                    KeyConditionExpression="pk = :p AND sk BETWEEN :lo AND :hi",
                    ExpressionAttributeValues={
                        ":p": {"S": "p1"},
                        ":lo": {"S": "0"},
                        ":hi": {"S": "10"},
                    },
                )
            ),
        ),
        (
            "conditional_fail",
            err_wrap(
                lambda: d.put_item(
                    TableName=t, Item=dict(item), ConditionExpression="attribute_not_exists(pk)"
                )
            ),
        ),
        # -- tranche-5 pins (semantic-audit 2026-11-06 dynamodb findings) ----
        (
            "put_key_type_mismatch",
            err_wrap(lambda: d.put_item(TableName=t, Item={"pk": {"S": "p2"}, "sk": {"S": "5"}})),
        ),
        (
            "put_missing_key",
            err_wrap(lambda: d.put_item(TableName=t, Item={"pk": {"S": "p3"}})),
        ),
        (
            "put_empty_string_key",
            err_wrap(lambda: d.put_item(TableName=t, Item={"pk": {"S": ""}, "sk": {"N": "1"}})),
        ),
        (
            "put_empty_set",
            err_wrap(
                lambda: d.put_item(
                    TableName=t, Item={"pk": {"S": "p4"}, "sk": {"N": "1"}, "tags": {"SS": []}}
                )
            ),
        ),
        (
            "add_onto_string_attr",
            err_wrap(
                lambda: d.update_item(
                    TableName=t,
                    Key=key,
                    UpdateExpression="ADD payload :one",
                    ExpressionAttributeValues={":one": {"N": "5"}},
                )
            ),
        ),
        (
            "add_empty_set_operand",
            err_wrap(
                lambda: d.update_item(
                    TableName=t,
                    Key=key,
                    UpdateExpression="ADD tags :empty",
                    ExpressionAttributeValues={":empty": {"SS": []}},
                )
            ),
        ),
        (
            "delete_set_to_empty_removes_attr",
            err_wrap(
                lambda: d.update_item(
                    TableName=t,
                    Key=key,
                    UpdateExpression="DELETE payload :v",
                    ExpressionAttributeValues={":v": {"S": "z"}},
                )
            ),
        ),
        (
            "update_unresolved_name",
            err_wrap(
                lambda: d.update_item(
                    TableName=t,
                    Key=key,
                    UpdateExpression="SET #nope = :v",
                    ExpressionAttributeValues={":v": {"S": "x"}},
                )
            ),
        ),
        (
            "get_item_duplicate_projection",
            err_wrap(
                lambda: d.get_item(
                    TableName=t,
                    Key=key,
                    ProjectionExpression="payload, #p",
                    ExpressionAttributeNames={"#p": "payload"},
                )
            ),
        ),
        ("scan", err_wrap(lambda: d.scan(TableName=t))),
        (
            "delete_item",
            err_wrap(lambda: d.delete_item(TableName=t, Key=key, ReturnValues="ALL_OLD")),
        ),
        ("delete_table", err_wrap(lambda: d.delete_table(TableName=t))),
        ("describe_deleted", err_wrap(lambda: d.describe_table(TableName=t))),
    ]
    return out


@corpus("kms_secrets_ssm")
def corpus_kms_secrets_ssm(engine):
    out = []
    kms = engine.client("kms")
    key_id = kms.create_key(KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT")["KeyMetadata"][
        "KeyId"
    ]
    alias = engine.res_name("alias/diff-")
    out += [
        ("create_alias", err_wrap(lambda: kms.create_alias(AliasName=alias, TargetKeyId=key_id))),
        ("encrypt", err_wrap(lambda: kms.encrypt(KeyId=key_id, Plaintext=b"plaintext-secret"))),
    ]
    ct = kms.encrypt(KeyId=key_id, Plaintext=b"plaintext-secret")["CiphertextBlob"]
    out += [
        ("decrypt_roundtrip", err_wrap(lambda: kms.decrypt(CiphertextBlob=ct)["Plaintext"].hex())),
        (
            "sign_with_encrypt_key",
            err_wrap(
                lambda: kms.sign(
                    KeyId=key_id,
                    Message=b"m",
                    MessageType="RAW",
                    SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA256",
                )
            ),
        ),
    ]

    sm = engine.client("secretsmanager")
    sname = engine.res_name("diff/secret")
    out += [
        ("create_secret", err_wrap(lambda: sm.create_secret(Name=sname, SecretString="s3cret"))),
        ("get_secret_value", err_wrap(lambda: sm.get_secret_value(SecretId=sname)["SecretString"])),
        ("rotate", err_wrap(lambda: sm.put_secret_value(SecretId=sname, SecretString="s3cret-v2"))),
        (
            "get_previous",
            err_wrap(
                lambda: sm.get_secret_value(SecretId=sname, VersionStage="AWSPREVIOUS")[
                    "SecretString"
                ]
            ),
        ),
        ("get_missing_secret", err_wrap(lambda: sm.get_secret_value(SecretId=sname + "-nope"))),
    ]

    ssm = engine.client("ssm")
    pname = engine.res_name("/diff/param")
    out += [
        (
            "put_parameter",
            err_wrap(lambda: ssm.put_parameter(Name=pname, Value="v1", Type="String")),
        ),
        ("get_parameter", err_wrap(lambda: ssm.get_parameter(Name=pname))),
        ("get_missing_parameter", err_wrap(lambda: ssm.get_parameter(Name=pname + "x"))),
    ]
    return out


@corpus("iam_sts")
def corpus_iam_sts(engine):
    out = []
    iam = engine.client("iam")
    user = engine.res_name("diff-user")
    out += [
        ("create_user", err_wrap(lambda: iam.create_user(UserName=user))),
        ("create_user_again", err_wrap(lambda: iam.create_user(UserName=user))),
        (
            "put_policy",
            err_wrap(
                lambda: iam.put_user_policy(
                    UserName=user,
                    PolicyName="p",
                    PolicyDocument=json.dumps(
                        {
                            "Version": "2012-10-17",
                            "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                        }
                    ),
                )
            ),
        ),
        ("get_policy", err_wrap(lambda: iam.get_user_policy(UserName=user, PolicyName="p"))),
        ("create_access_key", err_wrap(lambda: iam.create_access_key(UserName=user))),
        ("list_access_keys", err_wrap(lambda: iam.list_access_keys(UserName=user))),
        ("list_users", err_wrap(lambda: iam.list_users())),
        (
            "delete_missing_policy",
            err_wrap(lambda: iam.delete_user_policy(UserName=user, PolicyName="nope")),
        ),
        ("delete_user_with_key", err_wrap(lambda: iam.delete_user(UserName=user))),
    ]
    keys = iam.list_access_keys(UserName=user)["AccessKeyMetadata"]
    iam.delete_access_key(UserName=user, AccessKeyId=keys[0]["AccessKeyId"])
    iam.delete_user_policy(UserName=user, PolicyName="p")
    out += [
        ("delete_user_clean", err_wrap(lambda: iam.delete_user(UserName=user))),
        ("get_deleted_user", err_wrap(lambda: iam.get_user(UserName=user))),
    ]
    sts = engine.client("sts")
    out += [
        ("get_caller_identity", err_wrap(lambda: sts.get_caller_identity())),
        (
            "assume_role_fake",
            err_wrap(
                lambda: sts.assume_role(
                    RoleArn="arn:aws:iam::123456789012:role/does-not-exist", RoleSessionName="s"
                )
            ),
        ),
    ]
    return out


def start_moto() -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-p", "5000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + MOTO_START_TIMEOUT
    while time.time() < deadline:
        try:
            urllib.request.urlopen("http://localhost:5000/moto-api/", timeout=2)
            return proc
        except Exception:
            time.sleep(0.5)
    proc.kill()
    raise RuntimeError("moto server did not come up")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", help="run one corpus (default: all)")
    args = parser.parse_args()

    try:
        urllib.request.urlopen(ROBOTOCORE + "/_robotocore/health", timeout=2)
    except Exception:
        print(f"robotocore is not running on {ROBOTOCORE}; start it first", file=sys.stderr)
        return 2

    moto_proc = start_moto()
    try:
        names = [args.corpus] if args.corpus else list(CORPORA)
        known_path = _pathlib.Path(__file__).parent / "differential_known_diffs.json"
        known = json.loads(known_path.read_text()) if known_path.exists() else {}
        failures = 0
        unexpected = {}
        observed = set()
        for name in names:
            # A run-unique salt (kept identical across the two engines)
            # so deterministic names never collide with a previous run's
            # persistent state on the shared robotocore account.
            salt = f"{name}-{uuid.uuid4().hex[:8]}"
            rows = {}
            for engine_name, endpoint in (("robotocore", ROBOTOCORE), ("moto", MOTO)):
                engine = Engine(engine_name, endpoint, salt)
                rows[engine_name] = dict(CORPORA[name](engine))  # corpus raises = engine-level bug
            print(f"\n== {name} ==")
            for label, a in rows["robotocore"].items():
                observed.add(f"{name}.{label}")
                b = rows["moto"].get(label, "<missing step>")
                ca, cb = canonical(a), canonical(b)
                if ca == cb:
                    print(f"  ok      {label}")
                    continue
                key = f"{name}.{label}"
                reason = known.get(key)
                if reason:
                    print(f"  known   {label}: {reason[:110]}")
                    continue
                failures += 1
                print(f"  DIFF    {label}  (full diff in differential_report.json)")
                unexpected[f"{name}.{label}"] = {"robotocore": ca, "moto": cb}
        report_path = "differential_report.json"
        with open(report_path, "w") as f:
            json.dump(unexpected, f, indent=1, sort_keys=True, default=str)
        # Rot guard: a known-diffs key that matched NO observed step this
        # run is either a stale label (the corpus moved on) or a masked
        # rename — surface it loudly instead of silently masking forever.

        dead = sorted(k for k in known if k not in observed)
        for k in dead:
            print(f"  WARNING known-diffs key {k} matched no step this run — update it")
        print(f"\n{failures} unexpected divergences across {len(names)} corpora → {report_path}")
        if dead:
            print(f"{len(dead)} stale known-diffs keys need attention")
        return 0 if failures == 0 else 1
    finally:
        moto_proc.terminate()


if __name__ == "__main__":
    sys.exit(main())
