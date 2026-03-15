from __future__ import annotations

from robotocore.semantic.s3_audit import (
    PROJECT_ROOT,
    _baseline_implemented_ops,
    audit_catalog,
    build_connectivity_matrix,
    load_catalog,
    validate_catalog,
)


def test_catalog_validates():
    catalog = load_catalog()
    validate_catalog(catalog)


def test_audit_is_deterministic():
    catalog = load_catalog()
    report_a = audit_catalog(catalog)
    report_b = audit_catalog(catalog)

    assert report_a == report_b
    assert report_a["summary"]["service"] == "s3"
    assert report_a["summary"]["feature_count"] >= 10


def test_connectivity_matrix_contains_known_edges():
    catalog = load_catalog()
    report = audit_catalog(catalog)
    matrix = build_connectivity_matrix(report)

    assert "# S3 Connectivity Matrix" in matrix
    assert "`s3->sqs`" in matrix
    assert "`s3->eventbridge`" in matrix


def test_catalog_evidence_files_exist():
    catalog = load_catalog()
    for feature in catalog["features"]:
        for relpath in feature.get("evidence_files", []):
            assert (PROJECT_ROOT / relpath).exists(), relpath


def test_falls_back_to_compat_when_parity_is_native_only():
    implemented_ops, source = _baseline_implemented_ops(
        compat={"covered_ops": ["PutObject", "GetObject", "PutBucketWebsite"]},
        parity={"implemented_ops": ["CreateSession"], "delegates_to_moto": True},
    )

    assert source == "compat_fallback"
    assert implemented_ops == {"PutObject", "GetObject", "PutBucketWebsite"}
