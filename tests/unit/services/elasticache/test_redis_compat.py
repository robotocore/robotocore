"""Unit tests for the Redis-compatible in-memory store."""

import time

import pytest

from robotocore.services.elasticache.redis_compat import RedisCompatStore, RedisError


class TestStringCommands:
    """Tests for string commands."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_get_set(self):
        assert self.store.execute_command("SET", "key1", "value1") == "OK"
        assert self.store.execute_command("GET", "key1") == "value1"

    def test_get_nonexistent(self):
        assert self.store.execute_command("GET", "missing") is None

    def test_setnx_new_key(self):
        assert self.store.execute_command("SETNX", "key1", "val1") == 1
        assert self.store.execute_command("GET", "key1") == "val1"

    def test_setnx_existing_key(self):
        self.store.execute_command("SET", "key1", "old")
        assert self.store.execute_command("SETNX", "key1", "new") == 0
        assert self.store.execute_command("GET", "key1") == "old"

    def test_setex(self):
        assert self.store.execute_command("SETEX", "key1", 10, "val1") == "OK"
        assert self.store.execute_command("GET", "key1") == "val1"
        ttl = self.store.execute_command("TTL", "key1")
        assert 0 < ttl <= 10

    def test_mget(self):
        self.store.execute_command("SET", "a", "1")
        self.store.execute_command("SET", "b", "2")
        result = self.store.execute_command("MGET", "a", "b", "c")
        assert result == ["1", "2", None]

    def test_mset(self):
        self.store.execute_command("MSET", "a", "1", "b", "2")
        assert self.store.execute_command("GET", "a") == "1"
        assert self.store.execute_command("GET", "b") == "2"

    def test_incr(self):
        self.store.execute_command("SET", "counter", "10")
        assert self.store.execute_command("INCR", "counter") == 11
        assert self.store.execute_command("GET", "counter") == "11"

    def test_incr_new_key(self):
        assert self.store.execute_command("INCR", "newctr") == 1

    def test_incr_non_integer(self):
        self.store.execute_command("SET", "key", "abc")
        with pytest.raises(RedisError, match="not an integer"):
            self.store.execute_command("INCR", "key")

    def test_decr(self):
        self.store.execute_command("SET", "counter", "10")
        assert self.store.execute_command("DECR", "counter") == 9

    def test_append(self):
        self.store.execute_command("SET", "key", "hello")
        length = self.store.execute_command("APPEND", "key", " world")
        assert length == 11
        assert self.store.execute_command("GET", "key") == "hello world"

    def test_append_new_key(self):
        length = self.store.execute_command("APPEND", "new", "val")
        assert length == 3


class TestHashCommands:
    """Tests for hash commands."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_hset_hget(self):
        self.store.execute_command("HSET", "h", "f1", "v1")
        assert self.store.execute_command("HGET", "h", "f1") == "v1"

    def test_hget_nonexistent(self):
        assert self.store.execute_command("HGET", "h", "f1") is None

    def test_hdel(self):
        self.store.execute_command("HSET", "h", "f1", "v1", "f2", "v2")
        assert self.store.execute_command("HDEL", "h", "f1") == 1
        assert self.store.execute_command("HGET", "h", "f1") is None
        assert self.store.execute_command("HGET", "h", "f2") == "v2"

    def test_hgetall(self):
        self.store.execute_command("HSET", "h", "f1", "v1", "f2", "v2")
        result = self.store.execute_command("HGETALL", "h")
        assert result == {"f1": "v1", "f2": "v2"}

    def test_hgetall_empty(self):
        assert self.store.execute_command("HGETALL", "empty") == {}

    def test_hmset_hmget(self):
        self.store.execute_command("HMSET", "h", "a", "1", "b", "2")
        result = self.store.execute_command("HMGET", "h", "a", "b", "c")
        assert result == ["1", "2", None]

    def test_hexists(self):
        self.store.execute_command("HSET", "h", "f1", "v1")
        assert self.store.execute_command("HEXISTS", "h", "f1") == 1
        assert self.store.execute_command("HEXISTS", "h", "f2") == 0

    def test_hkeys(self):
        self.store.execute_command("HSET", "h", "a", "1", "b", "2")
        keys = self.store.execute_command("HKEYS", "h")
        assert sorted(keys) == ["a", "b"]

    def test_hvals(self):
        self.store.execute_command("HSET", "h", "a", "1", "b", "2")
        vals = self.store.execute_command("HVALS", "h")
        assert sorted(vals) == ["1", "2"]

    def test_hlen(self):
        self.store.execute_command("HSET", "h", "a", "1", "b", "2")
        assert self.store.execute_command("HLEN", "h") == 2


class TestListCommands:
    """Tests for list commands."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_lpush_rpush_lrange(self):
        self.store.execute_command("LPUSH", "list", "b")
        self.store.execute_command("LPUSH", "list", "a")
        self.store.execute_command("RPUSH", "list", "c")
        result = self.store.execute_command("LRANGE", "list", 0, -1)
        assert result == ["a", "b", "c"]

    def test_lpop(self):
        self.store.execute_command("RPUSH", "list", "a", "b", "c")
        assert self.store.execute_command("LPOP", "list") == "a"

    def test_rpop(self):
        self.store.execute_command("RPUSH", "list", "a", "b", "c")
        assert self.store.execute_command("RPOP", "list") == "c"

    def test_lpop_empty(self):
        assert self.store.execute_command("LPOP", "empty") is None

    def test_llen(self):
        self.store.execute_command("RPUSH", "list", "a", "b", "c")
        assert self.store.execute_command("LLEN", "list") == 3

    def test_llen_empty(self):
        assert self.store.execute_command("LLEN", "empty") == 0

    def test_lrange_subset(self):
        self.store.execute_command("RPUSH", "list", "a", "b", "c", "d")
        result = self.store.execute_command("LRANGE", "list", 1, 2)
        assert result == ["b", "c"]

    def test_lrange_negative(self):
        self.store.execute_command("RPUSH", "list", "a", "b", "c")
        result = self.store.execute_command("LRANGE", "list", -2, -1)
        assert result == ["b", "c"]


class TestSetCommands:
    """Tests for set commands."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_sadd_smembers(self):
        assert self.store.execute_command("SADD", "s", "a", "b", "c") == 3
        members = self.store.execute_command("SMEMBERS", "s")
        assert members == {"a", "b", "c"}

    def test_sadd_duplicates(self):
        self.store.execute_command("SADD", "s", "a", "b")
        assert self.store.execute_command("SADD", "s", "b", "c") == 1

    def test_srem(self):
        self.store.execute_command("SADD", "s", "a", "b", "c")
        assert self.store.execute_command("SREM", "s", "b") == 1
        members = self.store.execute_command("SMEMBERS", "s")
        assert members == {"a", "c"}

    def test_sismember(self):
        self.store.execute_command("SADD", "s", "a")
        assert self.store.execute_command("SISMEMBER", "s", "a") == 1
        assert self.store.execute_command("SISMEMBER", "s", "b") == 0

    def test_scard(self):
        self.store.execute_command("SADD", "s", "a", "b", "c")
        assert self.store.execute_command("SCARD", "s") == 3

    def test_scard_empty(self):
        assert self.store.execute_command("SCARD", "empty") == 0


class TestKeyCommands:
    """Tests for key management commands."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_del(self):
        self.store.execute_command("SET", "a", "1")
        self.store.execute_command("SET", "b", "2")
        assert self.store.execute_command("DEL", "a", "b", "c") == 2

    def test_exists(self):
        self.store.execute_command("SET", "a", "1")
        assert self.store.execute_command("EXISTS", "a") == 1
        assert self.store.execute_command("EXISTS", "missing") == 0

    def test_keys_all(self):
        self.store.execute_command("SET", "a", "1")
        self.store.execute_command("SET", "b", "2")
        keys = self.store.execute_command("KEYS", "*")
        assert sorted(keys) == ["a", "b"]

    def test_keys_pattern(self):
        self.store.execute_command("SET", "user:1", "a")
        self.store.execute_command("SET", "user:2", "b")
        self.store.execute_command("SET", "item:1", "c")
        keys = self.store.execute_command("KEYS", "user:*")
        assert sorted(keys) == ["user:1", "user:2"]

    def test_type_string(self):
        self.store.execute_command("SET", "k", "v")
        assert self.store.execute_command("TYPE", "k") == "string"

    def test_type_hash(self):
        self.store.execute_command("HSET", "k", "f", "v")
        assert self.store.execute_command("TYPE", "k") == "hash"

    def test_type_list(self):
        self.store.execute_command("RPUSH", "k", "v")
        assert self.store.execute_command("TYPE", "k") == "list"

    def test_type_set(self):
        self.store.execute_command("SADD", "k", "v")
        assert self.store.execute_command("TYPE", "k") == "set"

    def test_type_none(self):
        assert self.store.execute_command("TYPE", "missing") == "none"

    def test_rename(self):
        self.store.execute_command("SET", "old", "val")
        self.store.execute_command("RENAME", "old", "new")
        assert self.store.execute_command("GET", "old") is None
        assert self.store.execute_command("GET", "new") == "val"

    def test_rename_nonexistent(self):
        with pytest.raises(RedisError, match="no such key"):
            self.store.execute_command("RENAME", "missing", "new")

    def test_unknown_command(self):
        with pytest.raises(RedisError, match="unknown command"):
            self.store.execute_command("BOGUS")


class TestTTL:
    """Tests for key expiry (TTL)."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_expire_and_ttl(self):
        self.store.execute_command("SET", "k", "v")
        assert self.store.execute_command("EXPIRE", "k", 100) == 1
        ttl = self.store.execute_command("TTL", "k")
        assert 0 < ttl <= 100

    def test_ttl_no_expiry(self):
        self.store.execute_command("SET", "k", "v")
        assert self.store.execute_command("TTL", "k") == -1

    def test_ttl_nonexistent(self):
        assert self.store.execute_command("TTL", "missing") == -2

    def test_persist(self):
        self.store.execute_command("SET", "k", "v")
        self.store.execute_command("EXPIRE", "k", 100)
        assert self.store.execute_command("PERSIST", "k") == 1
        assert self.store.execute_command("TTL", "k") == -1

    def test_persist_no_expiry(self):
        self.store.execute_command("SET", "k", "v")
        assert self.store.execute_command("PERSIST", "k") == 0

    def test_lazy_expiry(self):
        self.store.execute_command("SET", "k", "v")
        # Manually set a past expiry
        self.store._expiry["k"] = time.time() - 1
        # Key should be expired on access
        assert self.store.execute_command("GET", "k") is None
        assert self.store.execute_command("EXISTS", "k") == 0

    def test_expire_nonexistent(self):
        assert self.store.execute_command("EXPIRE", "missing", 100) == 0


class TestWrongType:
    """Tests for WRONGTYPE errors when using wrong command on wrong type."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_string_op_on_hash(self):
        self.store.execute_command("HSET", "h", "f", "v")
        with pytest.raises(RedisError, match="WRONGTYPE"):
            self.store.execute_command("GET", "h")

    def test_hash_op_on_string(self):
        self.store.execute_command("SET", "s", "v")
        with pytest.raises(RedisError, match="WRONGTYPE"):
            self.store.execute_command("HGET", "s", "f")

    def test_list_op_on_string(self):
        self.store.execute_command("SET", "s", "v")
        with pytest.raises(RedisError, match="WRONGTYPE"):
            self.store.execute_command("LPUSH", "s", "x")
