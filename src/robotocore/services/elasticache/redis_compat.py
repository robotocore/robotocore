"""Redis-compatible in-memory store for ElastiCache emulation.

Provides a Python dict-backed implementation of core Redis commands,
giving behavioral fidelity beyond Moto's metadata-only mock.
"""

import fnmatch
import threading
import time
from typing import Any


class RedisCompatStore:
    """In-memory Redis-compatible data store.

    Supports core Redis commands using Python dicts. Thread-safe via a per-store lock.
    Implements lazy expiry: keys are checked on access and expired if past their TTL.
    """

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._types: dict[str, str] = {}  # key -> type name
        self._expiry: dict[str, float] = {}  # key -> expiry timestamp
        self._lock = threading.Lock()

    def _is_expired(self, key: str) -> bool:
        """Check if a key has expired (lazy expiry)."""
        if key in self._expiry:
            if time.time() >= self._expiry[key]:
                self._delete_key(key)
                return True
        return False

    def _delete_key(self, key: str) -> None:
        """Remove a key and its metadata."""
        self._data.pop(key, None)
        self._types.pop(key, None)
        self._expiry.pop(key, None)

    def _check_type(self, key: str, expected: str) -> None:
        """Raise if key exists with wrong type."""
        if key in self._types and self._types[key] != expected:
            raise RedisError("WRONGTYPE Operation against a key holding the wrong kind of value")

    def execute_command(self, cmd: str, *args: Any) -> Any:
        """Dispatch a Redis command."""
        cmd_upper = cmd.upper()
        handler = _COMMAND_MAP.get(cmd_upper)
        if handler is None:
            raise RedisError(f"ERR unknown command '{cmd}'")
        with self._lock:
            return handler(self, *args)

    # ------------------------------------------------------------------
    # String commands
    # ------------------------------------------------------------------

    def _cmd_get(self, key: str) -> str | None:
        if self._is_expired(key):
            return None
        self._check_type(key, "string")
        return self._data.get(key)

    def _cmd_set(self, key: str, value: str, *args: Any) -> str | None:
        self._is_expired(key)  # Clean up expired keys first

        # Parse options before setting
        nx = False
        xx = False
        ex_seconds = None
        px_millis = None
        idx = 0
        while idx < len(args):
            opt = str(args[idx]).upper()
            if opt == "EX" and idx + 1 < len(args):
                ex_seconds = int(args[idx + 1])
                idx += 2
            elif opt == "PX" and idx + 1 < len(args):
                px_millis = int(args[idx + 1])
                idx += 2
            elif opt == "NX":
                nx = True
                idx += 1
            elif opt == "XX":
                xx = True
                idx += 1
            else:
                idx += 1

        # NX: only set if key does NOT exist
        if nx and key in self._data:
            return None
        # XX: only set if key DOES exist
        if xx and key not in self._data:
            return None

        self._data[key] = str(value)
        self._types[key] = "string"
        # Clear any previous expiry
        self._expiry.pop(key, None)

        if ex_seconds is not None:
            self._expiry[key] = time.time() + ex_seconds
        elif px_millis is not None:
            self._expiry[key] = time.time() + px_millis / 1000

        return "OK"

    def _cmd_setnx(self, key: str, value: str) -> int:
        if self._is_expired(key):
            pass
        if key in self._data:
            return 0
        self._data[key] = str(value)
        self._types[key] = "string"
        return 1

    def _cmd_setex(self, key: str, seconds: int, value: str) -> str:
        seconds = int(seconds)
        if seconds <= 0:
            raise RedisError("ERR invalid expire time in 'setex' command")
        self._data[key] = str(value)
        self._types[key] = "string"
        self._expiry[key] = time.time() + seconds
        return "OK"

    def _cmd_mget(self, *keys: str) -> list[str | None]:
        result = []
        for key in keys:
            if self._is_expired(key):
                result.append(None)
            elif key in self._data and self._types.get(key) == "string":
                result.append(self._data[key])
            else:
                result.append(None)
        return result

    def _cmd_mset(self, *args: Any) -> str:
        for i in range(0, len(args), 2):
            key = args[i]
            value = str(args[i + 1])
            self._data[key] = value
            self._types[key] = "string"
        return "OK"

    def _cmd_incr(self, key: str) -> int:
        if self._is_expired(key):
            pass
        self._check_type(key, "string")
        val = self._data.get(key, "0")
        try:
            new_val = int(val) + 1
        except (ValueError, TypeError):
            raise RedisError("ERR value is not an integer or out of range")
        self._data[key] = str(new_val)
        self._types[key] = "string"
        return new_val

    def _cmd_decr(self, key: str) -> int:
        if self._is_expired(key):
            pass
        self._check_type(key, "string")
        val = self._data.get(key, "0")
        try:
            new_val = int(val) - 1
        except (ValueError, TypeError):
            raise RedisError("ERR value is not an integer or out of range")
        self._data[key] = str(new_val)
        self._types[key] = "string"
        return new_val

    def _cmd_append(self, key: str, value: str) -> int:
        if self._is_expired(key):
            pass
        self._check_type(key, "string")
        existing = self._data.get(key, "")
        new_val = str(existing) + str(value)
        self._data[key] = new_val
        self._types[key] = "string"
        return len(new_val)

    # ------------------------------------------------------------------
    # Hash commands
    # ------------------------------------------------------------------

    def _cmd_hset(self, key: str, *args: Any) -> int:
        if self._is_expired(key):
            pass
        self._check_type(key, "hash")
        if key not in self._data:
            self._data[key] = {}
            self._types[key] = "hash"
        count = 0
        for i in range(0, len(args), 2):
            field = args[i]
            value = args[i + 1]
            if field not in self._data[key]:
                count += 1
            self._data[key][field] = str(value)
        return count

    def _cmd_hget(self, key: str, field: str) -> str | None:
        if self._is_expired(key):
            return None
        self._check_type(key, "hash")
        h = self._data.get(key, {})
        return h.get(field)

    def _cmd_hdel(self, key: str, *fields: str) -> int:
        if self._is_expired(key):
            return 0
        self._check_type(key, "hash")
        h = self._data.get(key, {})
        count = 0
        for field in fields:
            if field in h:
                del h[field]
                count += 1
        return count

    def _cmd_hgetall(self, key: str) -> dict[str, str]:
        if self._is_expired(key):
            return {}
        self._check_type(key, "hash")
        return dict(self._data.get(key, {}))

    def _cmd_hmset(self, key: str, *args: Any) -> str:
        self._cmd_hset(key, *args)
        return "OK"

    def _cmd_hmget(self, key: str, *fields: str) -> list[str | None]:
        if self._is_expired(key):
            return [None] * len(fields)
        self._check_type(key, "hash")
        h = self._data.get(key, {})
        return [h.get(f) for f in fields]

    def _cmd_hexists(self, key: str, field: str) -> int:
        if self._is_expired(key):
            return 0
        self._check_type(key, "hash")
        h = self._data.get(key, {})
        return 1 if field in h else 0

    def _cmd_hkeys(self, key: str) -> list[str]:
        if self._is_expired(key):
            return []
        self._check_type(key, "hash")
        return list(self._data.get(key, {}).keys())

    def _cmd_hvals(self, key: str) -> list[str]:
        if self._is_expired(key):
            return []
        self._check_type(key, "hash")
        return list(self._data.get(key, {}).values())

    def _cmd_hlen(self, key: str) -> int:
        if self._is_expired(key):
            return 0
        self._check_type(key, "hash")
        return len(self._data.get(key, {}))

    # ------------------------------------------------------------------
    # List commands
    # ------------------------------------------------------------------

    def _cmd_lpush(self, key: str, *values: str) -> int:
        if self._is_expired(key):
            pass
        self._check_type(key, "list")
        if key not in self._data:
            self._data[key] = []
            self._types[key] = "list"
        for v in values:
            self._data[key].insert(0, str(v))
        return len(self._data[key])

    def _cmd_rpush(self, key: str, *values: str) -> int:
        if self._is_expired(key):
            pass
        self._check_type(key, "list")
        if key not in self._data:
            self._data[key] = []
            self._types[key] = "list"
        for v in values:
            self._data[key].append(str(v))
        return len(self._data[key])

    def _cmd_lpop(self, key: str) -> str | None:
        if self._is_expired(key):
            return None
        self._check_type(key, "list")
        lst = self._data.get(key, [])
        if not lst:
            return None
        return lst.pop(0)

    def _cmd_rpop(self, key: str) -> str | None:
        if self._is_expired(key):
            return None
        self._check_type(key, "list")
        lst = self._data.get(key, [])
        if not lst:
            return None
        return lst.pop()

    def _cmd_lrange(self, key: str, start: int, stop: int) -> list[str]:
        if self._is_expired(key):
            return []
        self._check_type(key, "list")
        lst = self._data.get(key, [])
        start = int(start)
        stop = int(stop)
        # Redis uses inclusive ranges with negative index support
        length = len(lst)
        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        return lst[start : stop + 1]

    def _cmd_llen(self, key: str) -> int:
        if self._is_expired(key):
            return 0
        self._check_type(key, "list")
        return len(self._data.get(key, []))

    # ------------------------------------------------------------------
    # Set commands
    # ------------------------------------------------------------------

    def _cmd_sadd(self, key: str, *members: str) -> int:
        if self._is_expired(key):
            pass
        self._check_type(key, "set")
        if key not in self._data:
            self._data[key] = set()
            self._types[key] = "set"
        count = 0
        for m in members:
            if str(m) not in self._data[key]:
                self._data[key].add(str(m))
                count += 1
        return count

    def _cmd_srem(self, key: str, *members: str) -> int:
        if self._is_expired(key):
            return 0
        self._check_type(key, "set")
        s = self._data.get(key, set())
        count = 0
        for m in members:
            if str(m) in s:
                s.discard(str(m))
                count += 1
        return count

    def _cmd_smembers(self, key: str) -> set[str]:
        if self._is_expired(key):
            return set()
        self._check_type(key, "set")
        return set(self._data.get(key, set()))

    def _cmd_sismember(self, key: str, member: str) -> int:
        if self._is_expired(key):
            return 0
        self._check_type(key, "set")
        return 1 if str(member) in self._data.get(key, set()) else 0

    def _cmd_scard(self, key: str) -> int:
        if self._is_expired(key):
            return 0
        self._check_type(key, "set")
        return len(self._data.get(key, set()))

    # ------------------------------------------------------------------
    # Key commands
    # ------------------------------------------------------------------

    def _cmd_del(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self._data:
                self._delete_key(key)
                count += 1
        return count

    def _cmd_exists(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if not self._is_expired(key) and key in self._data:
                count += 1
        return count

    def _cmd_keys(self, pattern: str = "*") -> list[str]:
        # Clean expired keys first
        for key in list(self._data.keys()):
            self._is_expired(key)
        return [k for k in self._data if fnmatch.fnmatch(k, pattern)]

    def _cmd_expire(self, key: str, seconds: int) -> int:
        seconds = int(seconds)
        if seconds <= 0:
            # Redis deletes the key if EXPIRE is called with <= 0
            if key in self._data:
                self._delete_key(key)
                return 1
            return 0
        if self._is_expired(key):
            return 0
        if key not in self._data:
            return 0
        self._expiry[key] = time.time() + seconds
        return 1

    def _cmd_ttl(self, key: str) -> int:
        if self._is_expired(key):
            return -2
        if key not in self._data:
            return -2
        if key not in self._expiry:
            return -1
        remaining = self._expiry[key] - time.time()
        return max(0, int(remaining))

    def _cmd_persist(self, key: str) -> int:
        if self._is_expired(key):
            return 0
        if key not in self._data:
            return 0
        if key in self._expiry:
            del self._expiry[key]
            return 1
        return 0

    def _cmd_type(self, key: str) -> str:
        if self._is_expired(key):
            return "none"
        return self._types.get(key, "none")

    def _cmd_rename(self, key: str, newkey: str) -> str:
        if self._is_expired(key):
            raise RedisError("ERR no such key")
        if key not in self._data:
            raise RedisError("ERR no such key")
        # Delete the target key if it exists (overwrite)
        if newkey in self._data and newkey != key:
            self._delete_key(newkey)
        self._data[newkey] = self._data.pop(key)
        self._types[newkey] = self._types.pop(key)
        if key in self._expiry:
            self._expiry[newkey] = self._expiry.pop(key)
        else:
            # If source had no expiry, remove any expiry on target
            self._expiry.pop(newkey, None)
        return "OK"


class RedisError(Exception):
    """Redis protocol error."""

    pass


# Command dispatch map — must be defined after the class
_COMMAND_MAP: dict[str, Any] = {
    # Strings
    "GET": RedisCompatStore._cmd_get,
    "SET": RedisCompatStore._cmd_set,
    "SETNX": RedisCompatStore._cmd_setnx,
    "SETEX": RedisCompatStore._cmd_setex,
    "MGET": RedisCompatStore._cmd_mget,
    "MSET": RedisCompatStore._cmd_mset,
    "INCR": RedisCompatStore._cmd_incr,
    "DECR": RedisCompatStore._cmd_decr,
    "APPEND": RedisCompatStore._cmd_append,
    # Hashes
    "HSET": RedisCompatStore._cmd_hset,
    "HGET": RedisCompatStore._cmd_hget,
    "HDEL": RedisCompatStore._cmd_hdel,
    "HGETALL": RedisCompatStore._cmd_hgetall,
    "HMSET": RedisCompatStore._cmd_hmset,
    "HMGET": RedisCompatStore._cmd_hmget,
    "HEXISTS": RedisCompatStore._cmd_hexists,
    "HKEYS": RedisCompatStore._cmd_hkeys,
    "HVALS": RedisCompatStore._cmd_hvals,
    "HLEN": RedisCompatStore._cmd_hlen,
    # Lists
    "LPUSH": RedisCompatStore._cmd_lpush,
    "RPUSH": RedisCompatStore._cmd_rpush,
    "LPOP": RedisCompatStore._cmd_lpop,
    "RPOP": RedisCompatStore._cmd_rpop,
    "LRANGE": RedisCompatStore._cmd_lrange,
    "LLEN": RedisCompatStore._cmd_llen,
    # Sets
    "SADD": RedisCompatStore._cmd_sadd,
    "SREM": RedisCompatStore._cmd_srem,
    "SMEMBERS": RedisCompatStore._cmd_smembers,
    "SISMEMBER": RedisCompatStore._cmd_sismember,
    "SCARD": RedisCompatStore._cmd_scard,
    # Keys
    "DEL": RedisCompatStore._cmd_del,
    "EXISTS": RedisCompatStore._cmd_exists,
    "KEYS": RedisCompatStore._cmd_keys,
    "EXPIRE": RedisCompatStore._cmd_expire,
    "TTL": RedisCompatStore._cmd_ttl,
    "PERSIST": RedisCompatStore._cmd_persist,
    "TYPE": RedisCompatStore._cmd_type,
    "RENAME": RedisCompatStore._cmd_rename,
}
