"""Native ElastiCache provider with real Redis-compatible stores.

Intercepts CreateCacheCluster/DeleteCacheCluster to create/destroy in-memory
Redis-compatible stores. Delegates all other operations to Moto.
Uses query protocol (Action from form body or query string).
"""

import logging
import threading
from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.elasticache.redis_compat import RedisCompatStore

logger = logging.getLogger(__name__)

# Redis stores keyed by (account_id, region, cluster_id)
_stores: dict[tuple[str, str, str], RedisCompatStore] = {}
_stores_lock = threading.Lock()


def get_store(account_id: str, region: str, cluster_id: str) -> RedisCompatStore | None:
    """Get the Redis-compatible store for a given cache cluster."""
    with _stores_lock:
        return _stores.get((account_id, region, cluster_id))


def _create_store_for_cluster(account_id: str, region: str, cluster_id: str) -> RedisCompatStore:
    """Create and register a Redis-compatible store for a cache cluster."""
    store = RedisCompatStore()
    with _stores_lock:
        _stores[(account_id, region, cluster_id)] = store
    logger.info("Created Redis store for %s/%s/%s", account_id, region, cluster_id)
    return store


def _destroy_store_for_cluster(account_id: str, region: str, cluster_id: str) -> None:
    """Destroy the Redis-compatible store for a cache cluster."""
    with _stores_lock:
        _stores.pop((account_id, region, cluster_id), None)
    logger.info("Destroyed Redis store for %s/%s/%s", account_id, region, cluster_id)


async def handle_elasticache_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an ElastiCache API request (query protocol)."""
    body = await request.body()
    content_type = request.headers.get("content-type", "")

    if "x-www-form-urlencoded" in content_type:
        parsed = parse_qs(body.decode(), keep_blank_values=True)
    else:
        parsed = parse_qs(str(request.url.query), keep_blank_values=True)
    params = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
    action = params.get("Action", "")

    if action == "CreateCacheCluster":
        return await _handle_create_cache_cluster(request, params, region, account_id)
    elif action == "DeleteCacheCluster":
        return await _handle_delete_cache_cluster(request, params, region, account_id)
    elif action == "CreateReplicationGroup":
        return await _handle_create_replication_group(request, params, region, account_id)
    elif action == "DeleteReplicationGroup":
        return await _handle_delete_replication_group(request, params, region, account_id)

    # All other operations go to Moto
    return await forward_to_moto(request, "elasticache", account_id=account_id)


async def _handle_create_cache_cluster(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """Intercept CreateCacheCluster to spin up a Redis-compatible store."""
    cluster_id = params.get("CacheClusterId", "")

    # Forward to Moto first for metadata
    response = await forward_to_moto(request, "elasticache", account_id=account_id)

    # If Moto succeeded, create the Redis store
    if response.status_code == 200:
        _create_store_for_cluster(account_id, region, cluster_id)

    return response


async def _handle_delete_cache_cluster(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """Intercept DeleteCacheCluster to tear down the Redis store."""
    cluster_id = params.get("CacheClusterId", "")

    response = await forward_to_moto(request, "elasticache", account_id=account_id)

    if response.status_code == 200:
        _destroy_store_for_cluster(account_id, region, cluster_id)

    return response


async def _handle_create_replication_group(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """Intercept CreateReplicationGroup to spin up a Redis store."""
    group_id = params.get("ReplicationGroupId", "")

    response = await forward_to_moto(request, "elasticache", account_id=account_id)

    if response.status_code == 200:
        _create_store_for_cluster(account_id, region, group_id)

    return response


async def _handle_delete_replication_group(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """Intercept DeleteReplicationGroup to tear down the Redis store."""
    group_id = params.get("ReplicationGroupId", "")

    response = await forward_to_moto(request, "elasticache", account_id=account_id)

    if response.status_code == 200:
        _destroy_store_for_cluster(account_id, region, group_id)

    return response
