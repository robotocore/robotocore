"""Native RDS provider with real database engine support.

Intercepts CreateDBInstance/DeleteDBInstance to create/destroy SQLite databases.
Delegates all other RDS operations to Moto for metadata management.
Uses query protocol (Action from form body or query string).
"""

import logging
import threading
from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.rds.engine import DatabaseEngine, create_engine

logger = logging.getLogger(__name__)

# Database engines keyed by (account_id, region, db_identifier)
_db_engines: dict[tuple[str, str, str], DatabaseEngine] = {}
_engines_lock = threading.Lock()


def get_engine(account_id: str, region: str, db_identifier: str) -> DatabaseEngine | None:
    """Get the database engine for a given DB instance."""
    with _engines_lock:
        return _db_engines.get((account_id, region, db_identifier))


def _create_engine_for_instance(
    account_id: str, region: str, db_identifier: str, engine_type: str
) -> DatabaseEngine:
    """Create and register a database engine for an RDS instance."""
    engine = create_engine(engine_type, db_identifier)
    with _engines_lock:
        _db_engines[(account_id, region, db_identifier)] = engine
    logger.info(
        "Created database engine for %s/%s/%s (type=%s)",
        account_id,
        region,
        db_identifier,
        engine_type,
    )
    return engine


def _destroy_engine_for_instance(account_id: str, region: str, db_identifier: str) -> None:
    """Destroy the database engine for an RDS instance."""
    with _engines_lock:
        engine = _db_engines.pop((account_id, region, db_identifier), None)
    if engine:
        engine.close()
        logger.info("Destroyed database engine for %s/%s/%s", account_id, region, db_identifier)


async def handle_rds_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an RDS API request (query protocol)."""
    body = await request.body()
    content_type = request.headers.get("content-type", "")

    if "x-www-form-urlencoded" in content_type:
        parsed = parse_qs(body.decode(), keep_blank_values=True)
    else:
        parsed = parse_qs(str(request.url.query), keep_blank_values=True)
    params = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
    action = params.get("Action", "")

    if action == "CreateDBInstance":
        return await _handle_create_db_instance(request, params, region, account_id)
    elif action == "DeleteDBInstance":
        return await _handle_delete_db_instance(request, params, region, account_id)
    elif action == "CreateDBCluster":
        return await _handle_create_db_cluster(request, params, region, account_id)
    elif action == "DeleteDBCluster":
        return await _handle_delete_db_cluster(request, params, region, account_id)

    # All other operations go to Moto
    return await forward_to_moto(request, "rds", account_id=account_id)


async def _handle_create_db_instance(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """Intercept CreateDBInstance to spin up a real SQLite database."""
    db_identifier = params.get("DBInstanceIdentifier", "")
    engine_type = params.get("Engine", "mysql")

    # Forward to Moto first for metadata
    response = await forward_to_moto(request, "rds", account_id=account_id)

    # If Moto succeeded, create the real database engine
    if response.status_code == 200:
        _create_engine_for_instance(account_id, region, db_identifier, engine_type)

    return response


async def _handle_delete_db_instance(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """Intercept DeleteDBInstance to tear down the SQLite database."""
    db_identifier = params.get("DBInstanceIdentifier", "")

    # Forward to Moto first
    response = await forward_to_moto(request, "rds", account_id=account_id)

    # If Moto succeeded, destroy the engine
    if response.status_code == 200:
        _destroy_engine_for_instance(account_id, region, db_identifier)

    return response


async def _handle_create_db_cluster(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """Intercept CreateDBCluster to spin up a real SQLite database for Aurora."""
    cluster_identifier = params.get("DBClusterIdentifier", "")
    engine_type = params.get("Engine", "aurora-mysql")

    response = await forward_to_moto(request, "rds", account_id=account_id)

    if response.status_code == 200:
        _create_engine_for_instance(account_id, region, cluster_identifier, engine_type)

    return response


async def _handle_delete_db_cluster(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """Intercept DeleteDBCluster to tear down the SQLite database."""
    cluster_identifier = params.get("DBClusterIdentifier", "")

    response = await forward_to_moto(request, "rds", account_id=account_id)

    if response.status_code == 200:
        _destroy_engine_for_instance(account_id, region, cluster_identifier)

    return response
