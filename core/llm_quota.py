"""全局 LLM 并发配额（FR-2.2）。

Redis 分布式信号量（方案 B）：多实例打到 LLM 的总并发 ≤ 全局配额。
- acquire: INCR → 超限则 DECR 回退；每次 acquire 刷新 EXPIRE（lease 兜底：
  实例崩溃后 lease 到期自动归零，下次 acquire 也会修复无 TTL 的残留键）
- release: finally 中 DECR（下限 0）
- 获取等待上限 AIOPS_LLM_SEMAPHORE_WAIT（默认 10s），超时抛 QuotaExceeded
  → 调用方走既有降级路径（不压垮网关）

网关限流（方案 A，主闸）由运维按 AIOPS_LLM_GLOBAL_CONCURRENT 配额实施；
本信号量为实例侧第二道闸（防 Redis 抖动放大）。
"""

import os
import threading
import time
from typing import Optional

from core.observability import metrics


class QuotaExceeded(RuntimeError):
    """全局配额等待超时（调用方走降级路径）。"""


class RedisLLMConcurrencyLimiter:
    """Redis 分布式信号量 LLM 包装器（保持 ILLMClient.generate_proposal 签名）。"""

    def __init__(
        self,
        wrapped,
        redis_client,
        max_concurrent: Optional[int] = None,
        lease_seconds: Optional[int] = None,
        wait_seconds: Optional[float] = None,
        prefix: str = "sem:llm",
    ):
        self._wrapped = wrapped
        self._redis = redis_client
        self._max = max(1, max_concurrent if max_concurrent is not None else int(
            os.getenv("AIOPS_LLM_GLOBAL_CONCURRENT", "10")))
        self._lease = max(1, lease_seconds if lease_seconds is not None else int(
            os.getenv("AIOPS_LLM_SEMAPHORE_LEASE_SECONDS", "300")))
        self._wait = wait_seconds if wait_seconds is not None else float(
            os.getenv("AIOPS_LLM_SEMAPHORE_WAIT", "10"))
        self._key = prefix

    def _acquire(self) -> Optional[str]:
        """尝试获取一个配额槽位；成功返回 token。"""
        try:
            count = self._redis.incr(self._key)
            if count > self._max:
                self._redis.decr(self._key)
                metrics.incr("pipeline.llm.quota_wait")
                return None
            # 每次获取刷新 lease —— 持有期间 TTL 续期；实例崩溃后 lease 到期自动归零
            self._redis.expire(self._key, self._lease)
            return "1"
        except Exception:
            # Redis 故障：放行（第二道闸失效不阻塞主链路，网关限流兜底）
            metrics.incr("state.redis_errors")
            return "degraded"

    def _release(self, token: str):
        if token == "degraded":
            return
        try:
            count = self._redis.decr(self._key)
            if count < 0:
                self._redis.set(self._key, 0)  # 下限 0（防 DECR 负值泄漏）
        except Exception:
            pass

    def generate_proposal(self, prompt: str) -> str:
        deadline = time.time() + self._wait
        token = None
        while True:
            token = self._acquire()
            if token is not None:
                break
            if time.time() >= deadline:
                metrics.incr("pipeline.llm.quota_rejected")
                raise QuotaExceeded(
                    f"全局 LLM 配额已满（max={self._max}），等待 {self._wait}s 超时"
                )
            time.sleep(0.05)

        try:
            return self._wrapped.generate_proposal(prompt)
        finally:
            self._release(token)
