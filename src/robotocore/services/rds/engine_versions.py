"""AWS-owned RDS engine version catalog for DescribeDBEngineVersions.

Moto knows two versions per engine, matches EngineVersion exactly, and ignores Filters and
DefaultOnly, so ``data.aws_rds_engine_version`` (engine + major version + engine-mode filter)
finds nothing. Customers cannot create engine versions, so the twin has to carry the catalog.
This module installs a broader catalog, AWS's prefix matching ("16" and "16.4" both match
"16.4"), the engine-mode / status / engine-version / parameter-group-family filters, and
DefaultOnly. Versions listed are a representative, real subset per engine (newest last).
"""

from typing import Any

_CATALOG: dict[str, list[str]] = {
    "aurora-postgresql": [
        "13.18",
        "13.20",
        "13.21",
        "13.23",
        "14.13",
        "14.15",
        "14.17",
        "14.18",
        "14.19",
        "15.8",
        "15.10",
        "15.12",
        "15.13",
        "15.14",
        "15.18",
        "16.4",
        "16.6",
        "16.8",
        "16.9",
        "16.10",
        "16.14",
        "17.4",
        "17.5",
        "17.6",
    ],
    "aurora-mysql": [
        "5.7.mysql_aurora.2.11.6",
        "5.7.mysql_aurora.2.12.5",
        "8.0.mysql_aurora.3.05.2",
        "8.0.mysql_aurora.3.08.2",
        "8.0.mysql_aurora.3.09.0",
        "8.0.mysql_aurora.3.10.0",
    ],
    "postgres": [
        "13.20",
        "13.23",
        "14.17",
        "14.19",
        "15.12",
        "15.14",
        "15.18",
        "16.8",
        "16.10",
        "16.14",
        "17.4",
        "17.6",
    ],
    "mysql": ["8.0.40", "8.0.41", "8.0.42", "8.4.4", "8.4.5"],
    "mariadb": ["10.11.11", "11.4.5"],
    "neptune": ["1.2.0.0", "1.2.0.1", "1.2.0.2", "1.2.1.0", "1.3.0.0", "1.4.5.0"],
}
_ENGINE_MODES = {
    "aurora-postgresql": ["provisioned", "serverless"],
    "aurora-mysql": ["provisioned", "serverless", "parallelquery", "global"],
}


def _family(engine: str, version: str) -> str:
    major = version.split(".")[0]
    if engine == "aurora-mysql":
        return "aurora-mysql8.0" if major == "8" else "aurora-mysql5.7"
    if engine in ("mysql", "mariadb"):
        return f"{engine}{'.'.join(version.split('.')[:2])}"
    if engine == "neptune":
        return "neptune1.3" if version >= "1.3" else "neptune1.2"
    return f"{engine}{major}"


def _matches_version(candidate: str, wanted: str) -> bool:
    return candidate == wanted or candidate.startswith(wanted + ".")


def catalog_versions(
    engine: str | None,
    engine_version: str | None,
    filters: dict[str, list[str]],
    default_only: bool,
) -> list[dict[str, Any]]:
    out = []
    for eng, versions in _CATALOG.items():
        if engine and eng != engine:
            continue
        chosen = [v for v in versions if not engine_version or _matches_version(v, engine_version)]
        if default_only and chosen:
            chosen = [chosen[-1]]
        for ver in chosen:
            modes = _ENGINE_MODES.get(eng, ["provisioned"])
            item = {
                "Engine": eng,
                "EngineVersion": ver,
                "DBParameterGroupFamily": _family(eng, ver),
                "DBEngineDescription": f"{eng} database engine",
                "DBEngineVersionDescription": f"{eng} {ver}",
                "ValidUpgradeTarget": [],
                "ExportableLogTypes": ["postgresql"] if "postgres" in eng else [],
                "SupportsLogExportsToCloudwatchLogs": True,
                "SupportsReadReplica": True,
                "SupportedEngineModes": modes,
                "SupportedFeatureNames": [],
                "Status": "available",
                "SupportsParallelQuery": False,
                "SupportsGlobalDatabases": eng.startswith("aurora") or eng == "neptune",
                "SupportsBabelfish": eng == "aurora-postgresql",
                "SupportsLimitlessDatabase": False,
                "SupportsCertificateRotationWithoutRestart": True,
                "SupportsIntegrations": False,
                "SupportsLocalWriteForwarding": eng.startswith("aurora"),
                "SupportedCACertificateIdentifiers": ["rds-ca-rsa2048-g1"],
                "DefaultCharacterSet": {"CharacterSetName": "utf8mb4"} if "mysql" in eng else {},
                "SupportedCharacterSets": [],
                "SupportedTimezones": [],
            }
            if not _passes(item, filters):
                continue
            out.append(item)
    return out


def _passes(item: dict[str, Any], filters: dict[str, list[str]]) -> bool:
    for name, values in filters.items():
        if name == "engine-mode" and not set(values) & set(item["SupportedEngineModes"]):
            return False
        if name == "status" and item["Status"] not in values:
            return False
        if name == "engine-version" and item["EngineVersion"] not in values:
            return False
        if name == "engine" and item["Engine"] not in values:
            return False
        if name == "db-parameter-group-family" and item["DBParameterGroupFamily"] not in values:
            return False
    return True


def install() -> None:
    """Replace Moto's DescribeDBEngineVersions with the catalog (keeps custom engine versions)."""
    from moto.core.responses import PaginatedResult
    from moto.rds.models import RDSBackend
    from moto.rds.responses import RDSResponse

    if getattr(RDSResponse.describe_db_engine_versions, "_robotocore", False):
        return

    def describe_db_engine_versions(self) -> Any:  # type: ignore[no-untyped-def]
        engine = self.params.get("Engine")
        engine_version = self.params.get("EngineVersion")
        filters = {f["Name"]: f.get("Values", []) for f in self.params.get("Filters", []) or []}
        default_only = str(self.params.get("DefaultOnly", "")).lower() == "true"
        versions = catalog_versions(engine, engine_version, filters, default_only)
        backend: RDSBackend = self.backend
        versions.extend(
            backend._describe_custom_engine_versions(engine=engine, engine_version=engine_version)
        )
        return PaginatedResult({"DBEngineVersions": versions})

    describe_db_engine_versions._robotocore = True  # type: ignore[attr-defined]
    RDSResponse.describe_db_engine_versions = describe_db_engine_versions  # type: ignore[method-assign]
