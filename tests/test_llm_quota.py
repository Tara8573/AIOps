"""FR-2.2：全局 LLM 并发配额验收测试。

验收标准（规格 FR-2.2）：
- N 个实例同时运行时，打到 LLM 的总并发 ≤ 全局配额
- 超配额时请求排队/拒绝，不压垮网关
"""

import threading
import time

import fakeredis
import pytest

from core.llm_quota import QuotaExceeded, RedisLLMConcurrencyLimiter
from interfaces.base import ILLMClient


class SlowLLM(ILLMClient):
    """慢 LLM（记录峰值并发）。"""

    def __init__(self, delay=0.2):
        self.delay = delay
        self.concurrent = 0
        self.max_concurrent = 0
        self._lock = threading.Lock()

    def generate_proposal(self, prompt: str) -> str:
        with self._lock:
            self.concurrent += 1
            self.max_concurrent = max(self.max_concurrent, self.concurrent)
        time.sleep(self.delay)
        with self._lock:
            self.concurrent -= 1
        return "ok"


class TestGlobalQuota:
    def test_total_concurrency_bounded_by_quota(self):
        """4 实例线程并发，总并发 ≤ 配额（max=2）。"""
        r = fakeredis.FakeRedis()
        llm = SlowLLM(delay=0.2)
        limiter = RedisLLMConcurrencyLimiter(llm, redis_client=r, max_concurrent=2, wait_seconds=5)

        threads = [
            threading.Thread(target=lambda: limiter.generate_proposal("p"))
            for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert llm.max_concurrent <= 2, f"总并发 {llm.max_concurrent} 超过全局配额 2"

    def test_wait_timeout_rejected(self):
        """配额满 + 等待超时 → QuotaExceeded（调用方走降级，不压垮网关）。"""
        r = fakeredis.FakeRedis()
        llm = SlowLLM(delay=1.0)
        limiter = RedisLLMConcurrencyLimiter(llm, redis_client=r, max_concurrent=1, wait_seconds=0.2)

        holder = threading.Thread(target=lambda: limiter.generate_proposal("p"))
        holder.start()
        time.sleep(0.1)  # 让 holder 占住配额

        with pytest.raises(QuotaExceeded):
            limiter.generate_proposal("p")

        holder.join(timeout=5)

    def test_semaphore_key_has_lease_ttl(self):
        """信号量 key 带 lease TTL（实例崩溃后自动归零兜底）。"""
        r = fakeredis.FakeRedis()
        limiter = RedisLLMConcurrencyLimiter(
            SlowLLM(delay=0.05), redis_client=r, max_concurrent=2, lease_seconds=300,
        )
        limiter.generate_proposal("p")
        ttl = r.ttl("sem:llm")
        assert 0 < ttl <= 300

    def test_sequential_calls_share_slot(self):
        """串行调用复用配额（finally 释放）。"""
        r = fakeredis.FakeRedis()
        limiter = RedisLLMConcurrencyLimiter(
            SlowLLM(delay=0.01), redis_client=r, max_concurrent=1, wait_seconds=2,
        )
        for _ in range(5):
            assert limiter.generate_proposal("p") == "ok"
        # 全部释放后计数归零
        assert int(r.get("sem:llm") or 0) == 0
