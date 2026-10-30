"""关联告警缓存（FR-2.1 状态外置 — 迁移点 2）。

现 AIOpsPipeline._recent_alerts 进程内 list 迁移为可外置存储：
- InMemoryCorrelationStore: 现 _recent_alerts/_find_correlated_alerts 逻辑抽出（行为不变）
- RedisCorrelationStore:    per-host list（LPUSH + LTRIM + EXPIRE）—— 同主机告警
                            被哈希到不同实例时，关联分析仍生效
"""

import json
import threading
import time
from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional

from core.observability import metrics


class ICorrelationStore(ABC):
    """关联告警缓存接口。"""

    @abstractmethod
    def add(self, alert, window_seconds: int) -> None:
        """记录一条告警（按主机分组）。"""
        pass

    @abstractmethod
    def find_same_host(self, alert, window_seconds: int) -> List[Dict]:
        """查找窗口内同主机的其他告警（精简 dict 列表）。"""
        pass

    @abstractmethod
    def prune(self, window_seconds: int) -> None:
        """清理过期条目（Redis 实现可为 no-op，EXPIRE 兜底）。"""
        pass


class InMemoryCorrelationStore(ICorrelationStore):
    """进程内实现（原 _recent_alerts 逻辑，行为不变）。线程安全。"""

    def __init__(self, extract_host_fn: Optional[Callable] = None):
        self._extract_host = extract_host_fn or (lambda alert: "")
        self._items: List[Dict] = []   # [{alert, host, seen_at}]
        self._lock = threading.Lock()

    def add(self, alert, window_seconds: int) -> None:
        with self._lock:
            self._items.append({
                "alert": alert,
                "host": self._extract_host(alert),
                "seen_at": time.time(),
            })

    def find_same_host(self, alert, window_seconds: int) -> List[Dict]:
        host = self._extract_host(alert)
        if not host:
            return []
        cutoff = time.time() - window_seconds
        correlated = []
        with self._lock:
            for item in self._items:
                prev = item["alert"]
                if prev.alert_id == alert.alert_id:
                    continue
                if item["host"] and item["host"] == host and item["seen_at"] > cutoff:
                    correlated.append({
                        "alert_id": prev.alert_id,
                        "title": prev.title,
                        "level": prev.level,
                        "content": prev.content,
                    })
        return correlated

    def prune(self, window_seconds: int) -> None:
        cutoff = time.time() - window_seconds
        with self._lock:
            self._items = [i for i in self._items if i["seen_at"] > cutoff]


class RedisCorrelationStore(ICorrelationStore):
    """Redis 实现（多实例共享关联窗口）。per-host list，EXPIRE 兜底过期。"""

    def __init__(self, redis_client, extract_host_fn: Optional[Callable] = None,
                 prefix: str = "correlation:", max_per_host: int = 200):
        self._redis = redis_client
        self._extract_host = extract_host_fn or (lambda alert: "")
        # 本地降级实例（同 extract_host_fn，Redis 故障时回落）
        self._local = InMemoryCorrelationStore(extract_host_fn=self._extract_host)
        self._prefix = prefix
        self._max_per_host = max_per_host

    def _key(self, host: str) -> str:
        return f"{self._prefix}{host}"

    def add(self, alert, window_seconds: int) -> None:
        host = self._extract_host(alert)
        if not host:
            return
        try:
            key = self._key(host)
            entry = {
                "alert_id": alert.alert_id,
                "title": alert.title,
                "level": alert.level,
                "content": alert.content,
                "seen_at": time.time(),
            }
            pipe = self._redis.pipeline()
            pipe.lpush(key, json.dumps(entry, ensure_ascii=False))
            pipe.ltrim(key, 0, self._max_per_host - 1)
            pipe.expire(key, max(1, window_seconds))
            pipe.execute()
            metrics.incr("state.redis_correlation_add")
        except Exception as exc:
            metrics.incr("state.redis_errors")
            self._local.add(alert, window_seconds)

    def find_same_host(self, alert, window_seconds: int) -> List[Dict]:
        host = self._extract_host(alert)
        if not host:
            return []
        try:
            raw = self._redis.lrange(self._key(host), 0, -1)
        except Exception as exc:
            metrics.incr("state.redis_errors")
            return self._local.find_same_host(alert, window_seconds)

        cutoff = time.time() - window_seconds
        correlated = []
        for item in raw:
            try:
                entry = json.loads(item)
            except (json.JSONDecodeError, TypeError):
                continue
            if entry.get("alert_id") == alert.alert_id:
                continue
            if entry.get("seen_at", 0) > cutoff:
                correlated.append({
                    "alert_id": entry.get("alert_id"),
                    "title": entry.get("title"),
                    "level": entry.get("level"),
                    "content": entry.get("content"),
                })
        return correlated

    def prune(self, window_seconds: int) -> None:
        self._local.prune(window_seconds)  # 本地降级同步清理


def correlation_store_factory(extract_host_fn=None, redis_client=None) -> ICorrelationStore:
    """关联缓存工厂：Redis 可用 → RedisCorrelationStore；否则本地实现。"""
    if redis_client is None:
        from plugins.redis_client import get_redis
        redis_client = get_redis()
    if redis_client is not None:
        return RedisCorrelationStore(redis_client, extract_host_fn=extract_host_fn)
    return InMemoryCorrelationStore(extract_host_fn=extract_host_fn)
