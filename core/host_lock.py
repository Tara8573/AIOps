"""同主机执行互斥锁（FR-5）。

Redis 分布式锁：lock:host:{host}（SET NX PX + token 校验释放，TTL 兜底崩溃）。
同一 host 同时只有一个自动执行 —— 多实例下也成立（规格 FR-5）。
无 Redis 时返回 None（不做互斥，由上层工单兜底；生产必须配 Redis）。
"""

import os
import uuid
from typing import Optional

from core.observability import metrics

from core.observability import metrics


class HostLock:
    """Redis 主机执行锁。"""

    def __init__(self, redis_client, prefix: str = "lock:host:", ttl_margin_seconds: int = 30):
        self._redis = redis_client
        self._prefix = prefix
        self._ttl_margin = ttl_margin_seconds

    def _key(self, host: str) -> str:
        return f"{self._prefix}{host}"

    def acquire(self, host: str, ttl_seconds: int) -> Optional[str]:
        """获取主机锁；成功返回 token（释放时校验），失败（已有执行）返回 None。"""
        if self._redis is None:
            metrics.incr("host_lock.unavailable")
            return None  # 无 Redis：不做互斥（上层转工单兜底）
        token = uuid.uuid4().hex
        ttl_ms = max(1000, (ttl_seconds + self._ttl_margin) * 1000)
        try:
            # SET NX PX：原子加锁 + TTL 兜底（持有者崩溃后锁自动过期）
            created = self._redis.set(self._key(host), token, nx=True, px=ttl_ms)
            if created:
                metrics.incr("host_lock.acquired")
                return token
            metrics.incr("host_lock.conflict")
            return None
        except Exception:
            metrics.incr("state.redis_errors")
            return None  # Redis 故障：保守拒绝（不并发执行）

    def release(self, host: str, token: str) -> bool:
        """释放主机锁（token 校验防误释放他人锁）。"""
        if self._redis is None or not token or token == "degraded":
            return False
        key = self._key(host)
        try:
            # GET 比对 + DEL（token 校验防误释放；比对-删除竞态窗口可接受）
            current = self._redis.get(key)
            if isinstance(current, bytes):  # 未开 decode_responses 的客户端
                current = current.decode("utf-8", errors="replace")
            if current == token:
                self._redis.delete(key)
                metrics.incr("host_lock.released")
                return True
            return False
        except Exception:
            metrics.incr("state.redis_errors")
            return False
