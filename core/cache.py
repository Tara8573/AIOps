import hashlib
import json
import threading
import time
from typing import Any, Dict, Optional, Tuple

from core.observability import metrics


class TTLCache:
    """Simple in-process TTL cache for hot-path reuse."""

    def __init__(self, ttl_seconds: int = 300, max_size: int = 512):
        self.ttl_seconds = max(1, ttl_seconds)
        self.max_size = max(1, max_size)
        self._store: Dict[str, Tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            payload = self._store.get(key)
            if payload is None:
                return None
            expires_at, value = payload
            if expires_at <= now:
                self._store.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any) -> None:
        expires_at = time.time() + self.ttl_seconds
        with self._lock:
            self._evict_expired_locked()
            if len(self._store) >= self.max_size:
                oldest_key = min(self._store, key=lambda item: self._store[item][0])
                self._store.pop(oldest_key, None)
            self._store[key] = (expires_at, value)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def stats(self) -> Dict[str, int]:
        with self._lock:
            self._evict_expired_locked()
            return {"size": len(self._store), "ttl_seconds": self.ttl_seconds}

    def _evict_expired_locked(self) -> None:
        now = time.time()
        expired = [key for key, (expires_at, _) in self._store.items() if expires_at <= now]
        for key in expired:
            self._store.pop(key, None)


def stable_cache_key(*parts: Any) -> str:
    serialized = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(serialized.encode("utf-8")).hexdigest()


class RedisTTLCache:
    """Redis 后端 TTL 缓存（FR-2.1），接口与 TTLCache 完全一致。

    - 值走 JSON 序列化（多实例共享热点缓存）
    - 过期由 Redis TTL 兕底；max_size 仅作接口兼容（Redis 侧不限制，靠 TTL）
    - Redis 故障 → 回落本地 TTLCache 实例 + 记指标（系统降级不崩溃）
    """

    def __init__(self, redis_client, ttl_seconds: int = 300, max_size: int = 512,
                 prefix: str = "ttlcache:"):
        self._redis = redis_client
        self.ttl_seconds = max(1, ttl_seconds)
        self.max_size = max(1, max_size)
        self._prefix = prefix
        self._fallback = TTLCache(ttl_seconds=ttl_seconds, max_size=max_size)

    def get(self, key: str) -> Optional[Any]:
        try:
            raw = self._redis.get(f"{self._prefix}{key}")
        except Exception as exc:
            metrics.incr("cache.redis_errors")
            return self._fallback.get(key)
        if raw is None:
            self._fallback.get(key)  # 同步本地降级缓存（无害）
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None

    def set(self, key: str, value: Any) -> None:
        self._fallback.set(key, value)  # 降级缓存同步写入（Redis 故障时可用）
        try:
            serialized = json.dumps(value, ensure_ascii=False, default=str)
            self._redis.setex(f"{self._prefix}{key}", self.ttl_seconds, serialized)
            metrics.incr("cache.redis_set")
        except Exception as exc:
            metrics.incr("cache.redis_errors")

    def clear(self) -> None:
        self._fallback.clear()
        try:
            keys = self._redis.keys(f"{self._prefix}*")
            if keys:
                self._redis.delete(*keys)
        except Exception:
            metrics.incr("cache.redis_errors")

    def stats(self) -> Dict[str, int]:
        return {"size": self._fallback.stats()["size"], "ttl_seconds": self.ttl_seconds}


def cache_factory(ttl_seconds: int = 300, max_size: int = 512, redis_client=None):
    """缓存工厂：Redis 可用 → RedisTTLCache（多实例共享）；否则本地 TTLCache。"""
    if redis_client is None:
        from plugins.redis_client import get_redis
        redis_client = get_redis()
    if redis_client is not None:
        return RedisTTLCache(redis_client, ttl_seconds=ttl_seconds, max_size=max_size)
    return TTLCache(ttl_seconds=ttl_seconds, max_size=max_size)
