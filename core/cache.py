import hashlib
import json
import threading
import time
from typing import Any, Dict, Optional, Tuple


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
