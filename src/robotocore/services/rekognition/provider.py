"""Native Rekognition provider.

Implements collection CRUD and tagging that Moto doesn't support.
Forwards other operations (face search, text detection, etc.) to Moto.
"""

import json
import time

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

# In-memory stores keyed by (account_id, region)
# collections: {(acct, region): {collection_id: {...metadata...}}}
_collections: dict[tuple[str, str], dict[str, dict]] = {}
# tags: {arn: {key: value}}
_tags: dict[str, dict[str, str]] = {}

_JSON_TYPE = "application/x-amz-json-1.1"


def _get_collections(account_id: str, region: str) -> dict[str, dict]:
    key = (account_id, region)
    if key not in _collections:
        _collections[key] = {}
    return _collections[key]


def _collection_arn(account_id: str, region: str, collection_id: str) -> str:
    return f"arn:aws:rekognition:{region}:{account_id}:collection/{collection_id}"


async def handle_rekognition_request(request: Request, region: str, account_id: str) -> Response:
    """Handle Rekognition requests, intercepting unimplemented operations."""
    target = request.headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    handler = _ACTION_MAP.get(action)
    if handler:
        body = await request.body()
        params = json.loads(body) if body else {}
        result = handler(params, region, account_id)
        if isinstance(result, tuple):
            # (status_code, body_dict)
            return Response(
                content=json.dumps(result[1]),
                status_code=result[0],
                media_type=_JSON_TYPE,
            )
        return Response(
            content=json.dumps(result),
            status_code=200,
            media_type=_JSON_TYPE,
        )

    return await forward_to_moto(request, "rekognition")


def _create_collection(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id in store:
        return (
            400,
            {
                "__type": "ResourceAlreadyExistsException",
                "Message": "A collection with the specified ID already exists.",
            },
        )

    arn = _collection_arn(account_id, region, collection_id)
    now = time.time()
    store[collection_id] = {
        "CollectionId": collection_id,
        "CollectionArn": arn,
        "CreationTimestamp": now,
        "FaceCount": 0,
        "FaceModelVersion": "6.0",
    }
    _tags[arn] = dict(params.get("Tags", {}))

    return {
        "StatusCode": 200,
        "CollectionArn": arn,
        "FaceModelVersion": "6.0",
    }


def _describe_collection(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return (
            400,
            {
                "__type": "ResourceNotFoundException",
                "Message": f"The collection id: {collection_id} does not exist",
            },
        )

    col = store[collection_id]
    return {
        "FaceCount": col["FaceCount"],
        "FaceModelVersion": col["FaceModelVersion"],
        "CollectionARN": col["CollectionArn"],
        "CreationTimestamp": col["CreationTimestamp"],
    }


def _list_collections(params: dict, region: str, account_id: str) -> dict:
    store = _get_collections(account_id, region)
    max_results = params.get("MaxResults", 1000)
    next_token = params.get("NextToken")

    all_ids = sorted(store.keys())

    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0

    end = start + max_results
    result_ids = all_ids[start:end]

    resp: dict = {
        "CollectionIds": result_ids,
        "FaceModelVersions": [store[cid]["FaceModelVersion"] for cid in result_ids],
    }
    if end < len(all_ids):
        resp["NextToken"] = str(end)
    return resp


def _delete_collection(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return (
            400,
            {
                "__type": "ResourceNotFoundException",
                "Message": f"The collection id: {collection_id} does not exist",
            },
        )

    arn = store[collection_id]["CollectionArn"]
    del store[collection_id]
    _tags.pop(arn, None)

    return {"StatusCode": 200}


def _tag_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("ResourceArn", "")
    tags = params.get("Tags", {})

    # Verify resource exists
    if not _resource_exists(arn, region, account_id):
        return (
            400,
            {
                "__type": "ResourceNotFoundException",
                "Message": "The resource with the specified ARN was not found.",
            },
        )

    if arn not in _tags:
        _tags[arn] = {}
    _tags[arn].update(tags)
    return {}


def _list_tags_for_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("ResourceArn", "")

    if not _resource_exists(arn, region, account_id):
        return (
            400,
            {
                "__type": "ResourceNotFoundException",
                "Message": "The resource with the specified ARN was not found.",
            },
        )

    return {"Tags": _tags.get(arn, {})}


def _untag_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("ResourceArn", "")
    tag_keys = params.get("TagKeys", [])

    if not _resource_exists(arn, region, account_id):
        return (
            400,
            {
                "__type": "ResourceNotFoundException",
                "Message": "The resource with the specified ARN was not found.",
            },
        )

    if arn in _tags:
        for key in tag_keys:
            _tags[arn].pop(key, None)
    return {}


def _resource_exists(arn: str, region: str, account_id: str) -> bool:
    """Check if a resource ARN corresponds to a known collection."""
    store = _get_collections(account_id, region)
    for col in store.values():
        if col["CollectionArn"] == arn:
            return True
    return False


_ACTION_MAP = {
    "CreateCollection": _create_collection,
    "DescribeCollection": _describe_collection,
    "ListCollections": _list_collections,
    "DeleteCollection": _delete_collection,
    "TagResource": _tag_resource,
    "ListTagsForResource": _list_tags_for_resource,
    "UntagResource": _untag_resource,
}
