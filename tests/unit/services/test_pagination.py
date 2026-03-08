"""Pagination correctness tests for native providers.

Tests that services with pagination support correctly return paged results
with NextToken present when more data exists and absent on the last page,
with no duplicates across pages, and all resources returned.

Services WITHOUT native pagination (all results returned at once):
- Lambda list_functions: returns all functions, no MaxResults/NextToken
- S3: forwards to Moto (no native pagination layer)
- IAM list_users/list_roles: forwards to Moto (no native pagination layer)
- DynamoDB scan: forwards to Moto (no native pagination layer)
- SQS list_queues: returns all queues, no pagination tokens
- SNS list_topics: returns all topics, no pagination tokens
- Events list_rules: returns all rules, no pagination tokens
- CloudWatch list_metrics: forwards to Moto (no native pagination layer)

Services WITH native pagination (tested below):
- Kinesis ListShards: NextToken + MaxResults
- Rekognition ListCollections: NextToken + MaxResults
- Cognito ListUserPools: MaxResults (truncation only, no NextToken)
"""

import pytest

from robotocore.services.kinesis.models import KinesisStore
from robotocore.services.kinesis.provider import _list_shards
from robotocore.services.rekognition.provider import (
    _collections,
    _create_collection,
    _list_collections,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_rekognition_store():
    """Reset Rekognition global store between tests."""
    _collections.clear()
    yield
    _collections.clear()


# ---------------------------------------------------------------------------
# Kinesis ListShards pagination
# ---------------------------------------------------------------------------


class TestKinesisListShardsPagination:
    """Kinesis ListShards supports NextToken + MaxResults pagination."""

    def _make_store_with_stream(self, shard_count: int) -> KinesisStore:
        store = KinesisStore()
        store.create_stream("test-stream", shard_count, "us-east-1", "123456789012")
        return store

    def test_list_shards_all_at_once(self):
        """With default MaxResults, all shards are returned without NextToken."""
        store = self._make_store_with_stream(3)
        result = _list_shards(store, {"StreamName": "test-stream"}, "us-east-1", "123456789012")

        assert len(result["Shards"]) == 3
        assert "NextToken" not in result

    def test_list_shards_paginate_one_at_a_time(self):
        """Paginate through 3 shards with MaxResults=1."""
        store = self._make_store_with_stream(3)

        # Page 1
        result1 = _list_shards(
            store,
            {"StreamName": "test-stream", "MaxResults": 1},
            "us-east-1",
            "123456789012",
        )
        assert len(result1["Shards"]) == 1
        assert "NextToken" in result1

        # Page 2
        result2 = _list_shards(
            store,
            {"StreamName": "test-stream", "MaxResults": 1, "NextToken": result1["NextToken"]},
            "us-east-1",
            "123456789012",
        )
        assert len(result2["Shards"]) == 1
        assert "NextToken" in result2

        # Page 3 (last)
        result3 = _list_shards(
            store,
            {"StreamName": "test-stream", "MaxResults": 1, "NextToken": result2["NextToken"]},
            "us-east-1",
            "123456789012",
        )
        assert len(result3["Shards"]) == 1
        assert "NextToken" not in result3

        # Verify all shards returned, no duplicates
        all_shard_ids = [
            s["ShardId"]
            for page in [result1, result2, result3]
            for s in page["Shards"]
        ]
        assert len(all_shard_ids) == 3
        assert len(set(all_shard_ids)) == 3  # no duplicates

    def test_list_shards_paginate_two_at_a_time(self):
        """Paginate through 5 shards with MaxResults=2."""
        store = self._make_store_with_stream(5)

        all_shard_ids = []
        next_token = None
        page_count = 0

        while True:
            params: dict = {"StreamName": "test-stream", "MaxResults": 2}
            if next_token:
                params["NextToken"] = next_token

            result = _list_shards(store, params, "us-east-1", "123456789012")
            page_count += 1

            for s in result["Shards"]:
                all_shard_ids.append(s["ShardId"])

            if "NextToken" not in result:
                break
            next_token = result["NextToken"]

        assert page_count == 3  # 2+2+1
        assert len(all_shard_ids) == 5
        assert len(set(all_shard_ids)) == 5  # no duplicates

    def test_list_shards_single_shard_no_pagination(self):
        """A single shard should return without NextToken."""
        store = self._make_store_with_stream(1)
        result = _list_shards(
            store,
            {"StreamName": "test-stream", "MaxResults": 1},
            "us-east-1",
            "123456789012",
        )
        assert len(result["Shards"]) == 1
        assert "NextToken" not in result

    def test_list_shards_max_results_equals_count(self):
        """MaxResults equal to shard count should return all without NextToken."""
        store = self._make_store_with_stream(3)
        result = _list_shards(
            store,
            {"StreamName": "test-stream", "MaxResults": 3},
            "us-east-1",
            "123456789012",
        )
        assert len(result["Shards"]) == 3
        assert "NextToken" not in result

    def test_list_shards_max_results_exceeds_count(self):
        """MaxResults larger than shard count should return all without NextToken."""
        store = self._make_store_with_stream(2)
        result = _list_shards(
            store,
            {"StreamName": "test-stream", "MaxResults": 100},
            "us-east-1",
            "123456789012",
        )
        assert len(result["Shards"]) == 2
        assert "NextToken" not in result

    def test_list_shards_next_token_encodes_stream_name(self):
        """NextToken should encode stream name so pagination works without StreamName param."""
        store = self._make_store_with_stream(3)

        result1 = _list_shards(
            store,
            {"StreamName": "test-stream", "MaxResults": 1},
            "us-east-1",
            "123456789012",
        )
        token = result1["NextToken"]
        assert "test-stream" in token

        # Using just the token (no StreamName) should still work
        result2 = _list_shards(
            store,
            {"MaxResults": 1, "NextToken": token},
            "us-east-1",
            "123456789012",
        )
        assert len(result2["Shards"]) == 1


# ---------------------------------------------------------------------------
# Rekognition ListCollections pagination
# ---------------------------------------------------------------------------


class TestRekognitionListCollectionsPagination:
    """Rekognition ListCollections supports NextToken + MaxResults pagination."""

    REGION = "us-east-1"
    ACCOUNT = "123456789012"

    def _create_collections(self, count: int):
        for i in range(count):
            _create_collection(
                {"CollectionId": f"col-{i:03d}"},
                self.REGION,
                self.ACCOUNT,
            )

    def test_list_collections_all_at_once(self):
        """Default MaxResults returns all collections without NextToken."""
        self._create_collections(3)
        result = _list_collections({}, self.REGION, self.ACCOUNT)

        assert len(result["CollectionIds"]) == 3
        assert "NextToken" not in result

    def test_list_collections_paginate_one_at_a_time(self):
        """Paginate through 3 collections with MaxResults=1."""
        self._create_collections(3)

        # Page 1
        result1 = _list_collections({"MaxResults": 1}, self.REGION, self.ACCOUNT)
        assert len(result1["CollectionIds"]) == 1
        assert "NextToken" in result1

        # Page 2
        result2 = _list_collections(
            {"MaxResults": 1, "NextToken": result1["NextToken"]},
            self.REGION,
            self.ACCOUNT,
        )
        assert len(result2["CollectionIds"]) == 1
        assert "NextToken" in result2

        # Page 3 (last)
        result3 = _list_collections(
            {"MaxResults": 1, "NextToken": result2["NextToken"]},
            self.REGION,
            self.ACCOUNT,
        )
        assert len(result3["CollectionIds"]) == 1
        assert "NextToken" not in result3

        # All returned, no duplicates
        all_ids = (
            result1["CollectionIds"] + result2["CollectionIds"] + result3["CollectionIds"]
        )
        assert len(all_ids) == 3
        assert len(set(all_ids)) == 3

    def test_list_collections_paginate_two_at_a_time(self):
        """Paginate through 5 collections with MaxResults=2."""
        self._create_collections(5)

        all_ids: list[str] = []
        next_token = None
        page_count = 0

        while True:
            params: dict = {"MaxResults": 2}
            if next_token:
                params["NextToken"] = next_token

            result = _list_collections(params, self.REGION, self.ACCOUNT)
            page_count += 1
            all_ids.extend(result["CollectionIds"])

            if "NextToken" not in result:
                break
            next_token = result["NextToken"]

        assert page_count == 3  # 2+2+1
        assert len(all_ids) == 5
        assert len(set(all_ids)) == 5

    def test_list_collections_face_model_versions_match(self):
        """FaceModelVersions list should match CollectionIds on each page."""
        self._create_collections(3)

        result = _list_collections({"MaxResults": 2}, self.REGION, self.ACCOUNT)
        assert len(result["CollectionIds"]) == len(result["FaceModelVersions"])
        assert len(result["CollectionIds"]) == 2

    def test_list_collections_single_no_pagination(self):
        """A single collection with MaxResults=1 should have no NextToken."""
        self._create_collections(1)
        result = _list_collections({"MaxResults": 1}, self.REGION, self.ACCOUNT)
        assert len(result["CollectionIds"]) == 1
        assert "NextToken" not in result

    def test_list_collections_max_results_equals_count(self):
        """MaxResults equal to count should return all without NextToken."""
        self._create_collections(3)
        result = _list_collections({"MaxResults": 3}, self.REGION, self.ACCOUNT)
        assert len(result["CollectionIds"]) == 3
        assert "NextToken" not in result

    def test_list_collections_returns_sorted(self):
        """Collections should be returned in sorted order."""
        # Create out of order
        for name in ["col-c", "col-a", "col-b"]:
            _create_collection({"CollectionId": name}, self.REGION, self.ACCOUNT)

        result = _list_collections({}, self.REGION, self.ACCOUNT)
        assert result["CollectionIds"] == ["col-a", "col-b", "col-c"]


# ---------------------------------------------------------------------------
# Cognito ListUserPools truncation
# ---------------------------------------------------------------------------


class TestCognitoListUserPoolsTruncation:
    """Cognito ListUserPools supports MaxResults truncation (no NextToken)."""

    @pytest.fixture(autouse=True)
    def _setup_store(self):
        from robotocore.services.cognito.provider import CognitoStore, _stores

        _stores.clear()
        yield
        _stores.clear()

    def _create_pools(self, count: int, region: str = "us-east-1"):
        from robotocore.services.cognito.provider import _create_user_pool, _get_store

        store = _get_store(region)
        for i in range(count):
            _create_user_pool(
                store,
                {"PoolName": f"pool-{i:03d}"},
                region,
                "123456789012",
            )
        return store

    def test_list_user_pools_returns_all_by_default(self):
        """Default MaxResults (60) returns all pools when fewer than 60."""
        from robotocore.services.cognito.provider import _list_user_pools

        store = self._create_pools(5)
        result = _list_user_pools(store, {}, "us-east-1", "123456789012")
        assert len(result["UserPools"]) == 5

    def test_list_user_pools_truncates_with_max_results(self):
        """MaxResults truncates the result list."""
        from robotocore.services.cognito.provider import _list_user_pools

        store = self._create_pools(5)
        result = _list_user_pools(
            store, {"MaxResults": 2}, "us-east-1", "123456789012"
        )
        assert len(result["UserPools"]) == 2

    def test_list_user_pools_max_results_exceeds_count(self):
        """MaxResults larger than pool count returns all pools."""
        from robotocore.services.cognito.provider import _list_user_pools

        store = self._create_pools(3)
        result = _list_user_pools(
            store, {"MaxResults": 100}, "us-east-1", "123456789012"
        )
        assert len(result["UserPools"]) == 3


# ---------------------------------------------------------------------------
# Native providers that return all results (no pagination)
# Verify they return everything when many resources exist.
# ---------------------------------------------------------------------------


class TestNativeProvidersReturnAllResults:
    """Verify that native providers without pagination return all resources."""

    def test_sqs_list_queues_returns_all(self):
        """SQS list_queues returns all queues without pagination."""
        from robotocore.services.sqs.models import SqsStore
        from robotocore.services.sqs.provider import _list_queues

        store = SqsStore()
        for i in range(10):
            store.create_queue(f"queue-{i}", "us-east-1", "123456789012")

        request = _make_dummy_request()
        result = _list_queues(store, {}, "us-east-1", "123456789012", request)
        assert len(result["QueueUrls"]) == 10

    def test_sns_list_topics_returns_all(self):
        """SNS list_topics returns all topics without pagination."""
        from robotocore.services.sns.models import SnsStore
        from robotocore.services.sns.provider import _list_topics

        store = SnsStore()
        for i in range(10):
            store.create_topic(f"topic-{i}", "us-east-1", "123456789012")

        request = _make_dummy_request()
        result = _list_topics(store, {}, "us-east-1", "123456789012", request)
        assert len(result["Topics"]) == 10

    def test_events_list_rules_returns_all(self):
        """Events list_rules returns all rules without pagination."""
        from robotocore.services.events.models import EventsStore
        from robotocore.services.events.provider import _list_rules

        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")
        for i in range(10):
            store.put_rule(
                f"rule-{i}",
                "default",
                "us-east-1",
                "123456789012",
                event_pattern={"source": [f"test-{i}"]},
            )

        result = _list_rules(store, {}, "us-east-1", "123456789012")
        assert len(result["Rules"]) == 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dummy_request():
    """Create a minimal Starlette Request for provider functions that require one."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [],
    }

    async def receive():
        return {"type": "http.request", "body": b""}

    return Request(scope, receive)
