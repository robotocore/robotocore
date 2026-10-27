#!/usr/bin/env python3
"""HUNTER-K2 continuation sweep: side-by-side moto-fork vs twin sampling.

Same method as the Section K continuation sweep: for a wlue of services in the
upper-branch candidate list we run a small sequential op flow against scratch
moto (the pinned fork, :4801) and scratch twin (release build, :4800), then
classify each op:

  faithful-cap        same outcome class (OK / same ClientError code / both 501)
  difference          both sides answer, but with different outcome classes
  501-and-moto-serves twin answers 501 NotImplemented while moto serves (gap)
  absence-both        both sides 501 (mirrored absence — faithful cot)

Usage:
  uv run python tooling/hunter-k2-moto-seq.py \
      --moto http://127.0.0.1:4801 --twin http://127.0.0.1:4800 \
      [--only glacier,kafka] [--json out.json]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from pathlib import Path

import boto3
import botocore.config
import botocore.exceptions

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.lib.param_filler import to_snake_case  # noqa: E402

def _png() -> bytes:
    import struct
    import zlib

    def chunk(tag: bytes, data: bytes) -> bytes:
        return (len(data)).to_bytes(4, "big") + tag + data + zlib.crc32(tag + data).to_bytes(4, "big")

    ihdr = (1).to_bytes(4, "big") + (1).to_bytes(4, "big") + bytes([8, 2, 0, 0, 0])
    row = b"\x00\x40\x20\x10"  # one RGB pixel
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr)
            + chunk(b"IDAT", zlib.compress(row)) + chunk(b"IEND", b""))


PNG_1X1 = _png()
ACCOUNT = "123456789012"
ARN_PROJECT = f"arn:aws:rekognition:us-east-1:{ACCOUNT}:project/k2proj"

RUN = uuid.uuid4().hex[:8]  # per-run suffix: both sides see the same fresh names

def _N(basename: str) -> str:
    return f"{basename}-{RUN}"

# (bn, [(op, params)]). Ops self-contained; cross-side state avoided by having
# each run its own independent call (per-side servers, same names).
FLOWS: dict[str, list[tuple[str, dict | None]]] = {
    "glacier": [
        ("CreateVault", {"accountId": "-", "vaultName": _N("k2v")}),
        ("ListVaults", {"accountId": "-"}),
        ("DescribeVault", {"accountId": "-", "vaultName": _N("k2v")}),
        ("GetVaultNotifications", {"accountId": "-", "vaultName": _N("k2v")}),
        ("UploadArchive", {"accountId": "-", "vaultName": "k2-vault", "body": b"k2"}),
    ],
    "kafka": [
        (
            "CreateCluster",
            {
                "ClusterName": _N("k2clu"),
                "KafkaVersion": "3.4.0",
                "NumberOfBrokerNodes": 2,
                "BrokerNodeGroupInfo": {
                    "InstanceType": "kafka.m5.large",
                    "ClientSubnets": ["subnet-00000000000001", "subnet-00000000000002"],
                },
            },
        ),
        ("ListClusters", {}),
        ("CheckpointCluster", None),
        ("ListKafkaVersions", {}),
    ],
    "quicksight": [
        ("DescribeAccountSubscription", {"AwsAccountId": ACCOUNT}),
        ("CreateGroup", {"AwsAccountId": ACCOUNT, "GroupName": _N("k2grp"), "Namespace": "default"}),
        ("ListGroups", {"AwsAccountId": ACCOUNT, "Namespace": "default"}),
        ("RegisterUser", {
            "IdentityType": "QUICKSIGHT", "Email": "k2@example.com",
            "UserRole": "READER", "AwsAccountId": ACCOUNT, "Namespace": "default",
        }),
    ],
    "rekognition": [
        ("CreateCollection", {"CollectionId": _N("k2coll")}),
        ("ListCollections", {}),
        ("DescribeCollection", {"CollectionId": _N("k2coll")}),
        ("IndexFaces", {"CollectionId": _N("k2coll"), "Image": {"Bytes": PNG_1X1}}),
        ("ListProjectPolicies", {"ProjectArn": ARN_PROJECT}),
        ("PutProjectPolicy", {"ProjectArn": ARN_PROJECT, "PolicyName": "p1", "PolicyDocument": "{}"}),
        ("DeleteProjectPolicy", {"ProjectArn": ARN_PROJECT, "PolicyName": "p1"}),
        ("ListMediaAnalysisJobs", {}),
        ("GetMediaAnalysisJob", {"JobId": "k2job"}),
        ("StartMediaAnalysisJob", {"OperationsConfig": {"TaskType": "MODERATION"}, "Input": {"S3Object": {"Bucket": "b", "Key": "k"}}, "Name": "j", "OutputConfig": {"S3Bucket": "b"}}),
        ("CopyProjectVersion", {"DestinationProjectArn": ARN_PROJECT, "SourceProjectVersionArn": "arn:aws:rekognition:us-east-1:123456789012:project/k2proj/version/1"}),
        ("UpdateDatasetEntries", {"DatasetType": "TRAIN", "Changes": {"Changes": []}, "ProjectArn": ARN_PROJECT}),
    ],
    "servicecatalog": [
        ("CreatePortfolio", {"DisplayName": _N("k2pf"), "ProviderName": "k2", "IdempotencyToken": str(uuid.uuid4())}),
        ("ListPortfolios", {}),
        ("CreateProduct", {"Name": _N("k2prod"), "Owner": "k2", "ProductType": "CLOUD_FORMATION_TEMPLATE", "IdempotencyToken": str(uuid.uuid4())}),
        ("SearchProducts", {}),
        ("SearchProductsAsAdmin", {}),
    ],
    # ---- moto also lacks these backends -----------------------------------
    "healthlake": [
        ("ListFHIRDatastores", {"DatastoreIdList": []}),
        ("DescribeFHIRDatastore", {"DatastoreId": "d1"}),
        ("ListTagsForResource", {"ResourceARN": f"arn:aws:healthlake:us-east-1:{ACCOUNT}:datastore/d1"}),
    ],
    "imagebuilder": [
        ("ListImages", {}),
        ("ListImagePipelines", {}),
        ("ListImageRecipes", {}),
    ],
    "license-manager": [
        ("CreateLicenseConfiguration", {"Name": "k2lic"}),
        ("ListLicenseConfigurations", {}),
        ("GetLicenseUsage", {"LicenseConfigurationArn": f"arn:aws:license-manager:us-east-1:{ACCOUNT}:license-configuration/k2"}),
    ],
    "pinpoint-email": [
        ("ListConfigurationSets", {}),
        ("CreateConfigurationSet", {"ConfigurationSetName": "k2cs"}),
        ("PutEmailIdentityDkimSigningKey", {"EmailIdentity": "k2.example.com", "PrivateKey": "x"}),
    ],
    "s3outposts": [
        ("ListEndpoints", {}),
        ("ListSharedEndpoints", {}),
    ],
}


def run(client, opname, params) -> dict:
    meth = getattr(client, to_snake_case(opname), None)
    if meth is None:
        return {"kind": "no-method", "code": "", "status": None, "keys": []}
    try:
        resp = meth(**(params or {})) or {}
        meta = resp.pop("ResponseMetadata", {})
        return {"kind": "OK", "code": "", "status": meta.get("HTTPStatusCode", 200),
                "body": _clean(resp)}
    except botocore.exceptions.ClientError as e:
        meta = e.response.get("ResponseMetadata", {})
        return {"kind": "ClientError", "code": e.response["Error"]["Code"],
                "status": meta.get("HTTPStatusCode"),
                "keys": [], "msg": str(e.response["Error"].get("Message", ""))[:110]}
    except botocore.exceptions.ParamValidationError as e:
        return {"kind": "ParamValidation", "code": "", "status": None, "keys": [], "msg": str(e)[:90]}
    except botocore.exceptions.ParseError as e:
        return {"kind": "ParseError", "code": "", "status": None, "keys": [], "msg": str(e)[:90]}
    except Exception as e:  # noqa: BLE001 — record and keep sweeping
        return {"kind": type(e).__name__, "code": "", "status": None, "keys": [], "msg": str(e)[:90]}


# Fields that legitimately vary between two independent servers (ids, ARNs
# with generated uuids, timestamps, quotas, hmacs).
VOLATILE = {
    "NextToken", "PageToken", "Marker", "RequestId", "requestId",
    "@metadata", "ResponseMetadata", "CreationTime", "CreateTime",
    "CreatedTime", "LastModifiedTimestamp", "LastUpdatedDateTime",
    "PasswordLastUsedTimestamp", "LastStatusChangeTime", "LastGenerationTime",
    "LastUpdatedTime", "LastScanTime", "CompletedAt", "StartedAt",
    "ExpirationDate", "ExpiresAt", "IngestionTime", "NotificationDate",
}


def _clean(obj, path=""):
    """Strip volatile leaves and normalize volatile values to '<v>'."""
    if isinstance(obj, dict):
        return {k: _clean(v, f"{path}.{k}") if k not in VOLATILE else "<v>"
                for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(v, f"{path}[]") for v in obj]
    if isinstance(obj, str):
        # volatile values: generated uuids (anywhere), random hexes, iso
        # timestamps, arn hash tails produced by either server
        s = re.sub(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<uuid>", obj)
        s = re.sub(r"^[0-9a-f]{10}$", "<rand>", s)
        s = re.sub(r"^20\d\d-\d\d-\d\dT?[0-9:.+ Z-]+$", "<ts>", s)
        if obj.startswith("arn:"):
            s = re.sub(r"(portfolio|product|user|group|cluster|vault|collection|datastore|ingestion)"
                       r"/[0-9a-fA-F-]{8,64}\b", r"\1/<id>", s)
        return s
    return obj


def short(r: dict) -> str:
    k = r["kind"]
    if k == "OK":
        return f"OK @ {r['status']}"
    if k == "ClientError":
        return f"{r['code']} @ {r['status']}"
    return f"{k}:{r.get('msg','')}"


def verdict(m: dict, t: dict) -> str:
    t501 = t["kind"] == "ClientError" and t["status"] == 501
    m_harness_crash = (
        m["kind"] == "ClientError"
        and m["status"] in (404, 500, 502, 503)
        and "<!doctype html>" in (m.get("msg") or "")
    )
    if t501:
        if m_harness_crash:
            return "absence-both(moto-harness-crash)"
        if m["kind"] == "ClientError" and m["status"] == 501:
            return "absence-both(501)"
        if m["kind"] == "ClientError" and m["status"] in (404, 500):
            # moto routes to a real backend but crashes/404s: still not serving
            return "absence-both(moto-crash)"
        return "501-and-moto-serves"
    if m["kind"] == "ClientError" and m["status"] == 501 and t["status"] != 501:
        return "twin-serves-moto-501"
    same = m["kind"] == t["kind"] and (m["kind"] != "ClientError" or m["code"] == t["code"])
    if not same:
        if m["kind"] == "ClientError" and m["status"] == 500:
            return "difference(moto-crashes-twin-serves)"
        return "difference"
    if m["kind"] == "OK":
        if m["body"] == t["body"]:
            return "faithful-body"
        return "faithful-code-value-diff"
    # same ClientError code, status may differ
    if m["status"] != t["status"]:
        return "faithful-code-status-diff"
    return "faithful-cap"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--moto", default="http://127.0.0.1:4801")
    ap.add_argument("--twin", default="http://127.0.0.1:4800")
    ap.add_argument("--only", default="")
    ap.add_argument("--json", default="")
    args = ap.parse_args()
    want = [s for s in args.only.split(",") if s] or list(FLOWS)

    cfg = botocore.config.Config(retries={"total_max_attempts": 1}, read_timeout=60)
    report: dict[str, list[dict]] = {}
    for bn in want:
        if bn not in FLOWS:
            print(f"== {bn}: no flow")
            continue
        ops = FLOWS[bn]
        kwargs = dict(region_name="us-east-1", aws_access_key_id="testing",
                      aws_secret_access_key="testing", config=cfg)
        cm = boto3.client(bn, endpoint_url=args.moto, **kwargs)
        ct = boto3.client(bn, endpoint_url=args.twin, **kwargs)
        print("=" * 100)
        print(f"== {bn}")
        rows = []
        for opname, params in ops:
            m = run(cm, opname, params)
            t = run(ct, opname, params)
            v = verdict(m, t)
            rows.append({"op": opname, "moto": m, "twin": t, "verdict": v})
            mark = "  <<<" if v in ("501-and-moto-serves", "difference", "twin-serves-moto-501") else ""
            print(f"- {opname}: {short(m)} | {short(t)} -> {v}{mark}")
            if v == "faithful-code-value-diff":
                print(f"    moto body: {json.dumps(m.get('body'), default=str)[:400]}")
                print(f"    twin body: {json.dumps(t.get('body'), default=str)[:400]}")
        report[bn] = rows
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=1, default=str))


if __name__ == "__main__":
    main()
