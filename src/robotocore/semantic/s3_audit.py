"""Feature-level S3 semantic audit built on top of the existing parity tools."""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CATALOG_PATH = PROJECT_ROOT / "data" / "s3_launch_catalog.yaml"
DEFAULT_REPORT_PATH = PROJECT_ROOT / "docs" / "s3-semantic-audit-report.json"
DEFAULT_MATRIX_PATH = PROJECT_ROOT / "docs" / "s3-connectivity-matrix.md"

VALID_FEATURE_STATUSES = {"active", "retired", "replaced", "out_of_scope"}
VALID_AUDIT_STATUSES = {"pass", "fail", "retired", "out_of_scope", "needs_manual_review"}


@dataclass(frozen=True)
class FeatureAudit:
    feature_id: str
    canonical_name: str
    launch_date: str
    launch_url: str
    current_status: str
    service_edges: list[str]
    aws_operations: list[str]
    status: str
    notes: list[str]
    missing_operations: list[str]
    missing_test_coverage: list[str]
    missing_evidence_files: list[str]
    robotocore_components: list[str]
    evidence_files: list[str]
    evidence_urls: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "feature_id": self.feature_id,
            "canonical_name": self.canonical_name,
            "launch_date": self.launch_date,
            "launch_url": self.launch_url,
            "current_status": self.current_status,
            "service_edges": self.service_edges,
            "aws_operations": self.aws_operations,
            "status": self.status,
            "notes": self.notes,
            "missing_operations": self.missing_operations,
            "missing_test_coverage": self.missing_test_coverage,
            "missing_evidence_files": self.missing_evidence_files,
            "robotocore_components": self.robotocore_components,
            "evidence_files": self.evidence_files,
            "evidence_urls": self.evidence_urls,
        }


def _load_script_module(path: Path, module_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_catalog(path: Path = DEFAULT_CATALOG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def validate_catalog(catalog: dict[str, Any]) -> None:
    features = catalog.get("features", [])
    if not isinstance(features, list) or not features:
        raise ValueError("Catalog must contain a non-empty 'features' list")

    seen_feature_ids: set[str] = set()
    launch_urls: set[str] = set()

    for feature in features:
        feature_id = feature["feature_id"]
        if feature_id in seen_feature_ids:
            raise ValueError(f"Duplicate feature_id: {feature_id}")
        seen_feature_ids.add(feature_id)

        status = feature["current_status"]
        if status not in VALID_FEATURE_STATUSES:
            raise ValueError(f"Invalid current_status for {feature_id}: {status}")

        launch_url = feature["launch_url"]
        if not launch_url.startswith("https://"):
            raise ValueError(f"launch_url must be https for {feature_id}")
        if launch_url in launch_urls:
            raise ValueError(f"Duplicate launch_url: {launch_url}")
        launch_urls.add(launch_url)

        if status == "replaced" and not feature.get("replacement_feature_id"):
            raise ValueError(f"replaced feature requires replacement_feature_id: {feature_id}")

        for field in ("service_edges", "aws_operations", "robotocore_components", "evidence_urls"):
            value = feature.get(field)
            if not isinstance(value, list):
                raise ValueError(f"{field} must be a list for {feature_id}")

    for feature in features:
        replacement_id = feature.get("replacement_feature_id")
        if replacement_id and replacement_id not in seen_feature_ids:
            raise ValueError(
                "replacement_feature_id references unknown feature: "
                f"{feature['feature_id']} -> {replacement_id}"
            )


def _repo_relative_paths_exist(paths: list[str], repo_root: Path) -> list[str]:
    missing = []
    for relpath in paths:
        if not (repo_root / relpath).exists():
            missing.append(relpath)
    return missing


def _load_s3_operation_baseline() -> tuple[dict[str, Any], dict[str, Any]]:
    compat_module = _load_script_module(
        PROJECT_ROOT / "scripts" / "compat_coverage.py",
        "compat_coverage_module",
    )
    parity_module = _load_script_module(
        PROJECT_ROOT / "scripts" / "generate_parity_report.py",
        "generate_parity_report_module",
    )
    compat = compat_module.analyze_service("s3", "s3", compat_module.find_test_file("s3"))
    parity = parity_module.build_report(filter_service="s3")["services"]["s3"]
    return compat, parity


def _baseline_implemented_ops(
    compat: dict[str, Any], parity: dict[str, Any]
) -> tuple[set[str], str]:
    parity_ops = set(parity.get("implemented_ops", []))
    compat_ops = set(compat.get("covered_ops", []))

    # In worktrees without vendor/moto/, the parity script can collapse to a
    # tiny native-only count. Fall back to compat-backed ops so the semantic
    # audit still reflects the repo's tested S3 surface instead of reporting
    # every feature as unimplemented.
    if (
        parity.get("delegates_to_moto")
        and len(parity_ops) < 10
        and len(compat_ops) > len(parity_ops)
    ):
        return compat_ops, "compat_fallback"
    return parity_ops, "parity_report"


def audit_catalog(
    catalog: dict[str, Any],
    repo_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    validate_catalog(catalog)
    compat, parity = _load_s3_operation_baseline()
    implemented_ops, operation_source = _baseline_implemented_ops(compat, parity)
    tested_ops = set(parity["tested_ops"])

    feature_results: list[FeatureAudit] = []

    sorted_features = sorted(
        catalog["features"], key=lambda item: (item["launch_date"], item["feature_id"])
    )
    for feature in sorted_features:
        current_status = feature["current_status"]
        missing_evidence_files = _repo_relative_paths_exist(
            feature.get("evidence_files", []), repo_root
        )
        missing_operations = sorted(set(feature["aws_operations"]) - implemented_ops)
        missing_test_coverage = sorted(set(feature["aws_operations"]) - tested_ops)
        notes: list[str] = []

        if current_status == "retired":
            audit_status = "retired"
        elif current_status == "out_of_scope":
            audit_status = "out_of_scope"
        else:
            if missing_evidence_files:
                notes.append("One or more evidence files referenced by the catalog do not exist.")
            if missing_operations:
                notes.append("One or more required operations are not currently implemented.")
            if missing_test_coverage:
                notes.append(
                    "One or more required operations are not covered by existing "
                    "compat/parity tests."
                )

            if missing_evidence_files or missing_operations:
                audit_status = "fail"
            elif missing_test_coverage:
                audit_status = "needs_manual_review"
            else:
                audit_status = "pass"

        if audit_status not in VALID_AUDIT_STATUSES:
            raise ValueError(
                f"Invalid audit status produced for {feature['feature_id']}: {audit_status}"
            )

        feature_results.append(
            FeatureAudit(
                feature_id=feature["feature_id"],
                canonical_name=feature["canonical_name"],
                launch_date=feature["launch_date"],
                launch_url=feature["launch_url"],
                current_status=current_status,
                service_edges=list(feature["service_edges"]),
                aws_operations=list(feature["aws_operations"]),
                status=audit_status,
                notes=notes,
                missing_operations=missing_operations,
                missing_test_coverage=missing_test_coverage,
                missing_evidence_files=missing_evidence_files,
                robotocore_components=list(feature["robotocore_components"]),
                evidence_files=list(feature.get("evidence_files", [])),
                evidence_urls=list(feature["evidence_urls"]),
            )
        )

    summary = {
        "catalog_version": catalog["catalog_version"],
        "service": catalog["service"],
        "feature_count": len(feature_results),
        "status_counts": {
            status: sum(1 for item in feature_results if item.status == status)
            for status in sorted(VALID_AUDIT_STATUSES)
        },
        "operation_baseline": {
            "implemented_operation_source": operation_source,
            "compat_total_ops": compat["total_ops"],
            "compat_covered_ops": compat["covered"],
            "compat_missing_ops": compat["missing_ops"],
            "parity_total_ops": parity["total_aws_ops"],
            "parity_implemented_ops": parity["implemented_count"],
            "parity_tested_ops": parity["tested_count"],
        },
    }

    return {
        "summary": summary,
        "features": [item.as_dict() for item in feature_results],
    }


def build_connectivity_matrix(report: dict[str, Any]) -> str:
    lines = [
        "# S3 Connectivity Matrix",
        "",
        "| Feature | Edge | Required operations | Current status | Semantic assertions |",
        "|---|---|---|---|---|",
    ]

    for feature in report["features"]:
        if not feature["service_edges"]:
            continue
        assertions = (
            "; ".join(feature["notes"]) if feature["notes"] else "Catalog evidence files present"
        )
        ops = ", ".join(feature["aws_operations"])
        for edge in feature["service_edges"]:
            lines.append(
                f"| {feature['canonical_name']} | `{edge}` | `{ops}` "
                f"| `{feature['status']}` | {assertions} |"
            )

    if len(lines) == 4:
        lines.append("| _None_ | _None_ | _None_ | _None_ | _None_ |")

    return "\n".join(lines) + "\n"


def write_report_files(
    report: dict[str, Any],
    report_path: Path = DEFAULT_REPORT_PATH,
    matrix_path: Path = DEFAULT_MATRIX_PATH,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    matrix_path.parent.mkdir(parents=True, exist_ok=True)
    matrix_path.write_text(build_connectivity_matrix(report), encoding="utf-8")
