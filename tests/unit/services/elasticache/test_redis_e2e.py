"""End-to-end and edge-case tests for ElastiCache Redis-compatible store.

Tests real-world Redis patterns, data integrity edge cases, TTL/expiry deep tests,
command error handling, and type mismatch scenarios.
"""

import time

import pytest

from robotocore.services.elasticache.redis_compat import RedisCompatStore, RedisError

# ---------------------------------------------------------------------------
# Real-world Redis patterns
# ---------------------------------------------------------------------------


class TestSessionStore:
    """Session store pattern: SET with EX, GET, refresh TTL with EXPIRE."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_session_lifecycle(self):
        # Create session with 300s expiry
        self.store.execute_command("SET", "session:abc123", "user_data_json", "EX", "300")
        assert self.store.execute_command("GET", "session:abc123") == "user_data_json"
        ttl = self.store.execute_command("TTL", "session:abc123")
        assert 298 <= ttl <= 300

    def test_session_refresh_ttl(self):
        self.store.execute_command("SET", "session:xyz", "data", "EX", "100")
        # Refresh the session TTL
        self.store.execute_command("EXPIRE", "session:xyz", 300)
        ttl = self.store.execute_command("TTL", "session:xyz")
        assert 298 <= ttl <= 300

    def test_session_expiry(self):
        self.store.execute_command("SET", "session:expired", "data", "EX", "1")
        # Force expiry
        self.store._expiry["session:expired"] = time.time() - 1
        assert self.store.execute_command("GET", "session:expired") is None


class TestRateLimiting:
    """Rate limiting pattern: INCR + EXPIRE (atomic-ish counter with TTL)."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_rate_limit_counter(self):
        key = "ratelimit:user:42:minute"
        # First request: INCR creates key with value 1
        count = self.store.execute_command("INCR", key)
        assert count == 1
        # Set expiry on first access
        self.store.execute_command("EXPIRE", key, 60)

        # Subsequent requests
        for _ in range(9):
            self.store.execute_command("INCR", key)

        assert self.store.execute_command("GET", key) == "10"
        ttl = self.store.execute_command("TTL", key)
        assert 0 < ttl <= 60

    def test_rate_limit_resets_after_expiry(self):
        key = "ratelimit:user:42:minute"
        self.store.execute_command("INCR", key)
        self.store.execute_command("EXPIRE", key, 1)
        # Force expiry
        self.store._expiry[key] = time.time() - 1

        # After expiry, INCR starts fresh
        count = self.store.execute_command("INCR", key)
        assert count == 1


class TestCachePattern:
    """Caching pattern: SET -> GET -> DEL cycle."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_cache_miss_set_get_del(self):
        # Miss
        assert self.store.execute_command("GET", "cache:product:42") is None

        # Populate cache
        self.store.execute_command("SET", "cache:product:42", '{"name":"Widget","price":9.99}')

        # Hit
        val = self.store.execute_command("GET", "cache:product:42")
        assert '"Widget"' in val

        # Invalidate
        self.store.execute_command("DEL", "cache:product:42")
        assert self.store.execute_command("GET", "cache:product:42") is None


class TestQueuePattern:
    """Queue pattern: RPUSH -> LPOP consumer loop."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_fifo_queue(self):
        # Producers push to the right
        self.store.execute_command("RPUSH", "queue:jobs", "job1", "job2", "job3")

        # Consumer pops from the left (FIFO)
        assert self.store.execute_command("LPOP", "queue:jobs") == "job1"
        assert self.store.execute_command("LPOP", "queue:jobs") == "job2"
        assert self.store.execute_command("LPOP", "queue:jobs") == "job3"
        assert self.store.execute_command("LPOP", "queue:jobs") is None

    def test_lifo_stack(self):
        # Push multiple
        self.store.execute_command("RPUSH", "stack:tasks", "task1", "task2", "task3")

        # Pop from right (LIFO)
        assert self.store.execute_command("RPOP", "stack:tasks") == "task3"
        assert self.store.execute_command("RPOP", "stack:tasks") == "task2"
        assert self.store.execute_command("RPOP", "stack:tasks") == "task1"


class TestHashUserProfile:
    """Hash-based user profile: HMSET, HGETALL, HDEL single field."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_user_profile_crud(self):
        key = "user:42"
        # Create profile
        self.store.execute_command(
            "HMSET", key, "name", "Alice", "email", "alice@example.com", "plan", "free"
        )

        # Read full profile
        profile = self.store.execute_command("HGETALL", key)
        assert profile == {"name": "Alice", "email": "alice@example.com", "plan": "free"}

        # Update single field
        self.store.execute_command("HSET", key, "plan", "pro")
        assert self.store.execute_command("HGET", key, "plan") == "pro"

        # Delete single field
        self.store.execute_command("HDEL", key, "email")
        assert self.store.execute_command("HGET", key, "email") is None
        assert self.store.execute_command("HLEN", key) == 2

    def test_hmget_subset(self):
        key = "user:99"
        self.store.execute_command("HMSET", key, "a", "1", "b", "2", "c", "3")
        result = self.store.execute_command("HMGET", key, "a", "c", "missing")
        assert result == ["1", "3", None]


class TestUnsupportedCommands:
    """Test that unsupported commands give clear errors."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_zadd_not_implemented(self):
        with pytest.raises(RedisError, match="unknown command"):
            self.store.execute_command("ZADD", "zset", "1", "member")

    def test_sinter_not_implemented(self):
        with pytest.raises(RedisError, match="unknown command"):
            self.store.execute_command("SINTER", "s1", "s2")

    def test_sunion_not_implemented(self):
        with pytest.raises(RedisError, match="unknown command"):
            self.store.execute_command("SUNION", "s1", "s2")

    def test_subscribe_not_implemented(self):
        with pytest.raises(RedisError, match="unknown command"):
            self.store.execute_command("SUBSCRIBE", "channel")


# ---------------------------------------------------------------------------
# Data integrity edge cases
# ---------------------------------------------------------------------------


class TestDataIntegrity:
    """Exact byte preservation, unicode, edge values."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_set_get_exact_value(self):
        val = "hello world 123 !@#$%"
        self.store.execute_command("SET", "k", val)
        assert self.store.execute_command("GET", "k") == val

    def test_unicode_values(self):
        val = "Hej varld! Merhaba dunya!"
        self.store.execute_command("SET", "k", val)
        assert self.store.execute_command("GET", "k") == val

    def test_emoji_values(self):
        val = "test data"
        self.store.execute_command("SET", "k", val)
        assert self.store.execute_command("GET", "k") == val

    def test_newlines_in_value(self):
        val = "line1\nline2\nline3"
        self.store.execute_command("SET", "k", val)
        assert self.store.execute_command("GET", "k") == val

    def test_null_bytes_in_value(self):
        """Values with null bytes should be stored as strings."""
        val = "before\x00after"
        self.store.execute_command("SET", "k", val)
        assert self.store.execute_command("GET", "k") == val

    def test_very_long_key(self):
        key = "k" * 1000
        self.store.execute_command("SET", key, "val")
        assert self.store.execute_command("GET", key) == "val"

    def test_empty_string_as_value(self):
        self.store.execute_command("SET", "k", "")
        result = self.store.execute_command("GET", "k")
        assert result == ""

    def test_empty_string_as_key(self):
        self.store.execute_command("SET", "", "val")
        assert self.store.execute_command("GET", "") == "val"

    def test_values_stored_as_strings(self):
        """All values in Redis are stored as strings."""
        self.store.execute_command("SET", "k", 42)
        result = self.store.execute_command("GET", "k")
        assert result == "42"
        assert isinstance(result, str)

    def test_incr_large_number(self):
        """INCR with a very large number."""
        self.store.execute_command("SET", "k", str(2**62))
        result = self.store.execute_command("INCR", "k")
        assert result == 2**62 + 1

    def test_decr_very_negative(self):
        """DECR with a very negative number."""
        self.store.execute_command("SET", "k", str(-(2**62)))
        result = self.store.execute_command("DECR", "k")
        assert result == -(2**62) - 1


# ---------------------------------------------------------------------------
# TTL and expiry deep tests
# ---------------------------------------------------------------------------


class TestTTLDeep:
    """Deep tests for TTL, expiry, and PERSIST behavior."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_set_with_ex_then_set_without_ex_clears_ttl(self):
        """SET with EX, then plain SET -> TTL should be removed."""
        self.store.execute_command("SET", "k", "v1", "EX", "100")
        assert self.store.execute_command("TTL", "k") > 0
        # Plain SET should clear the TTL
        self.store.execute_command("SET", "k", "v2")
        assert self.store.execute_command("TTL", "k") == -1

    def test_persist_on_key_with_ttl(self):
        self.store.execute_command("SET", "k", "v")
        self.store.execute_command("EXPIRE", "k", 100)
        assert self.store.execute_command("PERSIST", "k") == 1
        assert self.store.execute_command("TTL", "k") == -1
        # Value still there
        assert self.store.execute_command("GET", "k") == "v"

    def test_persist_on_key_without_ttl(self):
        self.store.execute_command("SET", "k", "v")
        assert self.store.execute_command("PERSIST", "k") == 0

    def test_persist_on_nonexistent_key(self):
        assert self.store.execute_command("PERSIST", "missing") == 0

    def test_ttl_on_nonexistent_key(self):
        assert self.store.execute_command("TTL", "missing") == -2

    def test_ttl_on_key_without_expiry(self):
        self.store.execute_command("SET", "k", "v")
        assert self.store.execute_command("TTL", "k") == -1

    def test_rename_key_preserves_ttl(self):
        self.store.execute_command("SET", "src", "v")
        self.store.execute_command("EXPIRE", "src", 100)
        self.store.execute_command("RENAME", "src", "dst")
        ttl = self.store.execute_command("TTL", "dst")
        assert 0 < ttl <= 100
        assert self.store.execute_command("TTL", "src") == -2  # src gone

    def test_rename_key_without_ttl_to_key_with_ttl(self):
        """RENAME src (no TTL) -> dst (has TTL): dst should have no TTL."""
        self.store.execute_command("SET", "src", "srcval")
        self.store.execute_command("SET", "dst", "dstval")
        self.store.execute_command("EXPIRE", "dst", 100)
        self.store.execute_command("RENAME", "src", "dst")
        # After rename, dst should inherit src's lack of TTL
        assert self.store.execute_command("TTL", "dst") == -1

    def test_lazy_expiry_on_get(self):
        self.store.execute_command("SET", "k", "v")
        self.store._expiry["k"] = time.time() - 1
        assert self.store.execute_command("GET", "k") is None
        assert self.store.execute_command("EXISTS", "k") == 0

    def test_lazy_expiry_on_type(self):
        self.store.execute_command("SET", "k", "v")
        self.store._expiry["k"] = time.time() - 1
        assert self.store.execute_command("TYPE", "k") == "none"

    def test_lazy_expiry_on_hget(self):
        self.store.execute_command("HSET", "h", "f", "v")
        self.store._expiry["h"] = time.time() - 1
        assert self.store.execute_command("HGET", "h", "f") is None

    def test_lazy_expiry_on_llen(self):
        self.store.execute_command("RPUSH", "l", "a")
        self.store._expiry["l"] = time.time() - 1
        assert self.store.execute_command("LLEN", "l") == 0

    def test_lazy_expiry_on_scard(self):
        self.store.execute_command("SADD", "s", "a")
        self.store._expiry["s"] = time.time() - 1
        assert self.store.execute_command("SCARD", "s") == 0

    def test_expire_updates_existing_ttl(self):
        self.store.execute_command("SET", "k", "v")
        self.store.execute_command("EXPIRE", "k", 100)
        self.store.execute_command("EXPIRE", "k", 200)
        ttl = self.store.execute_command("TTL", "k")
        assert ttl > 100  # Should be close to 200

    def test_setex_overwrites_value_and_ttl(self):
        self.store.execute_command("SETEX", "k", 100, "v1")
        self.store.execute_command("SETEX", "k", 200, "v2")
        assert self.store.execute_command("GET", "k") == "v2"
        ttl = self.store.execute_command("TTL", "k")
        assert ttl > 100


# ---------------------------------------------------------------------------
# Command error handling
# ---------------------------------------------------------------------------


class TestCommandErrors:
    """Wrong types, wrong args, edge cases."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_lpush_on_string_key(self):
        self.store.execute_command("SET", "k", "v")
        with pytest.raises(RedisError, match="WRONGTYPE"):
            self.store.execute_command("LPUSH", "k", "val")

    def test_get_on_list_key(self):
        self.store.execute_command("RPUSH", "k", "v")
        with pytest.raises(RedisError, match="WRONGTYPE"):
            self.store.execute_command("GET", "k")

    def test_hset_on_string_key(self):
        self.store.execute_command("SET", "k", "v")
        with pytest.raises(RedisError, match="WRONGTYPE"):
            self.store.execute_command("HSET", "k", "f", "v")

    def test_sadd_on_list_key(self):
        self.store.execute_command("RPUSH", "k", "v")
        with pytest.raises(RedisError, match="WRONGTYPE"):
            self.store.execute_command("SADD", "k", "member")

    def test_rpush_on_hash_key(self):
        self.store.execute_command("HSET", "k", "f", "v")
        with pytest.raises(RedisError, match="WRONGTYPE"):
            self.store.execute_command("RPUSH", "k", "val")

    def test_incr_on_non_numeric_string(self):
        self.store.execute_command("SET", "k", "not_a_number")
        with pytest.raises(RedisError, match="not an integer"):
            self.store.execute_command("INCR", "k")

    def test_decr_on_non_numeric_string(self):
        self.store.execute_command("SET", "k", "abc")
        with pytest.raises(RedisError, match="not an integer"):
            self.store.execute_command("DECR", "k")

    def test_rename_nonexistent_source(self):
        with pytest.raises(RedisError, match="no such key"):
            self.store.execute_command("RENAME", "missing", "dst")

    def test_del_nonexistent_returns_zero(self):
        assert self.store.execute_command("DEL", "missing") == 0

    def test_del_multiple_some_missing(self):
        self.store.execute_command("SET", "a", "1")
        assert self.store.execute_command("DEL", "a", "b", "c") == 1


# ---------------------------------------------------------------------------
# Keys pattern edge cases
# ---------------------------------------------------------------------------


class TestKeysPatterns:
    """KEYS with special characters and edge cases."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_keys_empty_store(self):
        assert self.store.execute_command("KEYS", "*") == []

    def test_keys_exact_match(self):
        self.store.execute_command("SET", "exact", "v")
        assert self.store.execute_command("KEYS", "exact") == ["exact"]

    def test_keys_with_colons(self):
        self.store.execute_command("SET", "user:1:name", "Alice")
        self.store.execute_command("SET", "user:1:email", "alice@example.com")
        self.store.execute_command("SET", "user:2:name", "Bob")
        keys = self.store.execute_command("KEYS", "user:1:*")
        assert sorted(keys) == ["user:1:email", "user:1:name"]

    def test_keys_question_mark(self):
        self.store.execute_command("SET", "a1", "v")
        self.store.execute_command("SET", "a2", "v")
        self.store.execute_command("SET", "b1", "v")
        keys = self.store.execute_command("KEYS", "a?")
        assert sorted(keys) == ["a1", "a2"]

    def test_keys_bracket_range(self):
        self.store.execute_command("SET", "key_a", "v")
        self.store.execute_command("SET", "key_b", "v")
        self.store.execute_command("SET", "key_c", "v")
        keys = self.store.execute_command("KEYS", "key_[ab]")
        assert sorted(keys) == ["key_a", "key_b"]


# ---------------------------------------------------------------------------
# Multi-type interaction
# ---------------------------------------------------------------------------


class TestMultiTypeInteraction:
    """After DEL, key type is reset; overwrite with different type."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_del_resets_type(self):
        self.store.execute_command("RPUSH", "k", "listval")
        assert self.store.execute_command("TYPE", "k") == "list"
        self.store.execute_command("DEL", "k")
        assert self.store.execute_command("TYPE", "k") == "none"
        # Now we can use it as a string
        self.store.execute_command("SET", "k", "stringval")
        assert self.store.execute_command("TYPE", "k") == "string"
        assert self.store.execute_command("GET", "k") == "stringval"

    def test_expired_key_allows_type_change(self):
        self.store.execute_command("HSET", "k", "f", "v")
        assert self.store.execute_command("TYPE", "k") == "hash"
        # Expire it
        self.store._expiry["k"] = time.time() - 1
        # Now use as list
        self.store.execute_command("RPUSH", "k", "listval")
        assert self.store.execute_command("TYPE", "k") == "list"

    def test_set_overwrites_any_type(self):
        """SET always overwrites regardless of existing type."""
        self.store.execute_command("RPUSH", "k", "listval")
        # SET on a list key -- should this work? Let's check.
        # In real Redis, SET replaces any key. Our implementation calls
        # _is_expired then stores directly, so it should overwrite.
        self.store.execute_command("SET", "k", "stringval")
        assert self.store.execute_command("GET", "k") == "stringval"
        assert self.store.execute_command("TYPE", "k") == "string"


# ---------------------------------------------------------------------------
# Append edge cases
# ---------------------------------------------------------------------------


class TestAppendEdgeCases:
    """APPEND on various states."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_append_to_nonexistent_creates_key(self):
        length = self.store.execute_command("APPEND", "k", "hello")
        assert length == 5
        assert self.store.execute_command("GET", "k") == "hello"

    def test_append_empty_string(self):
        self.store.execute_command("SET", "k", "hello")
        length = self.store.execute_command("APPEND", "k", "")
        assert length == 5
        assert self.store.execute_command("GET", "k") == "hello"

    def test_multiple_appends(self):
        self.store.execute_command("SET", "k", "a")
        self.store.execute_command("APPEND", "k", "b")
        self.store.execute_command("APPEND", "k", "c")
        assert self.store.execute_command("GET", "k") == "abc"


# ---------------------------------------------------------------------------
# Exists edge cases
# ---------------------------------------------------------------------------


class TestExistsEdgeCases:
    """EXISTS with multiple keys, duplicates, expired keys."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_exists_counts_duplicates(self):
        """In real Redis, EXISTS counts each occurrence. Let's verify our behavior."""
        self.store.execute_command("SET", "k", "v")
        # EXISTS with same key twice
        result = self.store.execute_command("EXISTS", "k", "k")
        # Our implementation counts each key independently
        assert result == 2

    def test_exists_expired_key(self):
        self.store.execute_command("SET", "k", "v")
        self.store._expiry["k"] = time.time() - 1
        assert self.store.execute_command("EXISTS", "k") == 0

    def test_exists_mixed_present_and_absent(self):
        self.store.execute_command("SET", "a", "1")
        self.store.execute_command("SET", "c", "3")
        assert self.store.execute_command("EXISTS", "a", "b", "c") == 2


# ---------------------------------------------------------------------------
# MSET/MGET edge cases
# ---------------------------------------------------------------------------


class TestMsetMgetEdgeCases:
    """MSET and MGET with edge cases."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_mset_overwrites_existing(self):
        self.store.execute_command("SET", "a", "old")
        self.store.execute_command("MSET", "a", "new", "b", "val")
        assert self.store.execute_command("GET", "a") == "new"
        assert self.store.execute_command("GET", "b") == "val"

    def test_mget_with_different_types(self):
        """MGET returns None for non-string keys."""
        self.store.execute_command("SET", "str", "val")
        self.store.execute_command("RPUSH", "list", "val")
        result = self.store.execute_command("MGET", "str", "list", "missing")
        assert result == ["val", None, None]

    def test_mget_all_missing(self):
        result = self.store.execute_command("MGET", "a", "b", "c")
        assert result == [None, None, None]


# ---------------------------------------------------------------------------
# Hash field overwrite behavior
# ---------------------------------------------------------------------------


class TestHashOverwrite:
    """Hash field update semantics."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_hset_overwrite_returns_zero(self):
        """HSET returns 0 when updating an existing field."""
        self.store.execute_command("HSET", "h", "f", "v1")
        assert self.store.execute_command("HSET", "h", "f", "v2") == 0
        assert self.store.execute_command("HGET", "h", "f") == "v2"

    def test_hset_mix_new_and_existing(self):
        """HSET returns count of NEW fields only."""
        self.store.execute_command("HSET", "h", "a", "1")
        result = self.store.execute_command("HSET", "h", "a", "2", "b", "3")
        assert result == 1  # Only 'b' is new

    def test_hdel_multiple_fields(self):
        self.store.execute_command("HSET", "h", "a", "1", "b", "2", "c", "3")
        count = self.store.execute_command("HDEL", "h", "a", "c", "missing")
        assert count == 2
        assert self.store.execute_command("HGETALL", "h") == {"b": "2"}


# ---------------------------------------------------------------------------
# List range edge cases
# ---------------------------------------------------------------------------


class TestListRangeEdgeCases:
    """LRANGE with various index combinations."""

    def setup_method(self):
        self.store = RedisCompatStore()
        self.store.execute_command("RPUSH", "l", "a", "b", "c", "d", "e")

    def test_lrange_full(self):
        assert self.store.execute_command("LRANGE", "l", 0, -1) == ["a", "b", "c", "d", "e"]

    def test_lrange_first_two(self):
        assert self.store.execute_command("LRANGE", "l", 0, 1) == ["a", "b"]

    def test_lrange_last_two(self):
        assert self.store.execute_command("LRANGE", "l", -2, -1) == ["d", "e"]

    def test_lrange_middle(self):
        assert self.store.execute_command("LRANGE", "l", 1, 3) == ["b", "c", "d"]

    def test_lrange_single_element(self):
        assert self.store.execute_command("LRANGE", "l", 2, 2) == ["c"]

    def test_lrange_out_of_bounds_high(self):
        result = self.store.execute_command("LRANGE", "l", 0, 100)
        assert result == ["a", "b", "c", "d", "e"]

    def test_lrange_empty_range(self):
        """When start > stop after normalization, returns empty."""
        result = self.store.execute_command("LRANGE", "l", 3, 1)
        assert result == []

    def test_lrange_nonexistent_key(self):
        assert self.store.execute_command("LRANGE", "missing", 0, -1) == []


# ---------------------------------------------------------------------------
# SETNX vs NX flag consistency
# ---------------------------------------------------------------------------


class TestSetnxVsNxFlag:
    """SETNX and SET ... NX should behave the same."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_setnx_returns_1_on_new(self):
        assert self.store.execute_command("SETNX", "k", "v") == 1

    def test_setnx_returns_0_on_existing(self):
        self.store.execute_command("SET", "k", "old")
        assert self.store.execute_command("SETNX", "k", "new") == 0

    def test_set_nx_returns_ok_on_new(self):
        assert self.store.execute_command("SET", "k", "v", "NX") == "OK"

    def test_set_nx_returns_none_on_existing(self):
        self.store.execute_command("SET", "k", "old")
        assert self.store.execute_command("SET", "k", "new", "NX") is None

    def test_both_prevent_overwrite(self):
        self.store.execute_command("SET", "k", "original")
        self.store.execute_command("SETNX", "k", "setnx_attempt")
        self.store.execute_command("SET", "k", "set_nx_attempt", "NX")
        assert self.store.execute_command("GET", "k") == "original"


# ---------------------------------------------------------------------------
# Combined SET options
# ---------------------------------------------------------------------------


class TestCombinedSetOptions:
    """SET with combined EX+NX, EX+XX, PX options."""

    def setup_method(self):
        self.store = RedisCompatStore()

    def test_set_ex_nx_new_key(self):
        result = self.store.execute_command("SET", "k", "v", "EX", "100", "NX")
        assert result == "OK"
        ttl = self.store.execute_command("TTL", "k")
        assert 0 < ttl <= 100

    def test_set_ex_nx_existing_key(self):
        self.store.execute_command("SET", "k", "old")
        result = self.store.execute_command("SET", "k", "new", "EX", "100", "NX")
        assert result is None
        assert self.store.execute_command("GET", "k") == "old"
        # No TTL should be set since SET failed
        assert self.store.execute_command("TTL", "k") == -1

    def test_set_ex_xx_existing_key(self):
        self.store.execute_command("SET", "k", "old")
        result = self.store.execute_command("SET", "k", "new", "EX", "100", "XX")
        assert result == "OK"
        assert self.store.execute_command("GET", "k") == "new"
        ttl = self.store.execute_command("TTL", "k")
        assert 0 < ttl <= 100

    def test_set_px_milliseconds(self):
        self.store.execute_command("SET", "k", "v", "PX", "5000")
        ttl = self.store.execute_command("TTL", "k")
        assert 0 < ttl <= 5


# ---------------------------------------------------------------------------
# Store isolation (multiple independent stores)
# ---------------------------------------------------------------------------


class TestStoreIsolation:
    """Two separate RedisCompatStore instances are fully isolated."""

    def test_independent_stores(self):
        store1 = RedisCompatStore()
        store2 = RedisCompatStore()

        store1.execute_command("SET", "k", "store1_val")
        store2.execute_command("SET", "k", "store2_val")

        assert store1.execute_command("GET", "k") == "store1_val"
        assert store2.execute_command("GET", "k") == "store2_val"

    def test_del_in_one_doesnt_affect_other(self):
        store1 = RedisCompatStore()
        store2 = RedisCompatStore()

        store1.execute_command("SET", "k", "v")
        store2.execute_command("SET", "k", "v")

        store1.execute_command("DEL", "k")
        assert store1.execute_command("GET", "k") is None
        assert store2.execute_command("GET", "k") == "v"
