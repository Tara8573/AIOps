"""告警去重存储（FR-2.1 状态外置 — 迁移点 1）。

现 AlertCleaner._seen 进程内 dict 迁移为可外置存储：
- InMemoryDedupStore: 现 _seen 逻辑抽出（行为不变，保回归）
- RedisDedupStore:    SET key 1 NX EX window —— 多实例共享去重
                      （同一告警只被一个实例诊断）
"""

import hashlib
import os
import threading
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional

from core.observability import metrics


def dedup_key_for(alert_title: str, alert_content: str) -> str:
    """去重键（口径与现 AlertCleaner._is_duplicate 一致：md5(title|content)）。"""
    return hashlib.md5(f"{alert_title}|{alert_content}".encode()).hexdigest()


class IDedupStore(ABC):
    """告警去重存储接口。"""

    @abstractmethod
    def is_duplicate(self, dedup_key: str, window_seconds: int) -> bool:
        """窗口内重复返回 True；首次出现记录并返回 False。"""
        pass


class InMemoryDedupStore(IDedupStore):
    """进程内实现（原 AlertCleaner._seen 逻辑，行为不变）。线程安全。"""

    def __init__(self):
        self._seen: Dict[str, float] = {}
        self._lock = threading.Lock()

    def is_duplicate(self, dedup_key: str, window_seconds: int) -> bool:
        now = time.time()
        with self._lock:
            last_seen = self._seen.get(dedup_key)
            if last_seen is not None and (now - last_seen) < window_seconds:
                return True
            self._seen[dedup_key] = now
            # 清理过期
            expired = [
                k for k, v in self._seen.items()
                if (now - v) > window_seconds * 2
            ]
            for k in expired:
                del self._seen[k]
        return False


class RedisDedupStore(IDedupStore):
    """Redis 实现（多实例共享去重）。SET NX EX。"""

    def __init__(self, redis_client, prefix: str = "dedup:"):
        self._redis = redis_client
        self._prefix = prefix

    def is_duplicate(self, dedup_key: str, window_seconds: int) -> bool:
        try:
            key = f"{self._prefix}{dedup_key}"
            # SET NX EX：设置成功 = 首次出现；失败 = 窗口内重复
            created = self._redis.set(key, 1, nx=True, ex=max(1, window_seconds))
            metrics.incr("state.redis_dedup_hits" if not created else "state.redis_dedup_new")
            return not created
        except Exception as exc:
            # Redis 故障：降级本地实现（保守判重，避免重复诊断）
            metrics.incr("state.redis_errors")
            return _local_fallback().is_duplicate(dedup_key, window_seconds)


_local = None
_local_lock = threading.Lock()


def _local_fallback() -> IDedupStore:
    """Redis 故障时的本地降级实例。"""
    global _local
    with _local_lock:
        if _local is None:
            _local = InMemoryDedupStore()
        return _local


def dedup_store_factory(redis_client: Optional[object] = None) -> IDedupStore:
    """去重存储工厂：Redis 可用 → RedisDedupStore；否则本地实现。"""
    if redis_client is None:
        from plugins.redis_client import get_redis
        redis_client = get_redis()
    if redis_client is not None:
        return RedisDedupStore(redis_client)
    return InMemoryDedupStore()
