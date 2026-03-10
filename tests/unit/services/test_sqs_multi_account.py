"""Tests for SQS multi-account isolation."""

from robotocore.services.sqs.provider import _get_store


class TestSqsStoreMultiAccount:
    def test_different_accounts_get_different_stores(self):
        """Each (account_id, region) pair gets its own SqsStore."""
        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")
        assert store_a is not store_b

    def test_same_account_same_region_reuses_store(self):
        """The same (account_id, region) pair returns the same SqsStore."""
        store1 = _get_store("us-west-2", "333333333333")
        store2 = _get_store("us-west-2", "333333333333")
        assert store1 is store2

    def test_same_account_different_region_separate_stores(self):
        """Different regions for the same account are separate."""
        store_east = _get_store("us-east-1", "444444444444")
        store_west = _get_store("us-west-2", "444444444444")
        assert store_east is not store_west

    def test_queue_isolation_between_accounts(self):
        """Queues created in one account are not visible in another."""
        store_a = _get_store("eu-west-1", "555555555555")
        store_b = _get_store("eu-west-1", "666666666666")

        store_a.create_queue("my-queue", "eu-west-1", "555555555555")

        # Account A can see the queue
        assert store_a.get_queue("my-queue") is not None

        # Account B cannot see the queue
        assert store_b.get_queue("my-queue") is None

    def test_default_account_id_backward_compat(self):
        """Calling _get_store with only region uses the default account."""
        store_default = _get_store("ap-southeast-1")
        store_explicit = _get_store("ap-southeast-1", "123456789012")
        assert store_default is store_explicit
