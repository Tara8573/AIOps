"""FR-1.3 / NFR 4.4：LLM 熔断器验收测试。

验收标准（规格 4.4）：LLM 连续 N 次超时/5xx 熔断 60s，期间走降级。
"""

import time

from core.circuit_breaker import CircuitBreakerLLM, CircuitOpenError
from interfaces.base import ILLMClient


class FlakyLLM(ILLMClient):
    """前 fail_times 次调用抛 503，之后成功。"""

    def __init__(self, fail_times: int = 0):
        self.calls = 0
        self.fail_times = fail_times

    def generate_proposal(self, prompt: str) -> str:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("HTTP 503 service unavailable")
        return "ok"


class TestCircuitBreaker:
    def test_opens_after_threshold(self):
        """连续 N 次失败 → OPEN，快速失败（不打爆网关）。"""
        breaker = CircuitBreakerLLM(FlakyLLM(fail_times=10), threshold=3, cooldown_seconds=60)
        for _ in range(3):
            with __import__("pytest").raises(RuntimeError):
                breaker.generate_proposal("p")
        assert breaker.state == "open"
        # OPEN 期间快速失败
        with __import__("pytest").raises(CircuitOpenError):
            breaker.generate_proposal("p")
        assert breaker.health_check() is False

    def test_half_open_then_closed_after_cooldown(self):
        """冷却期满 → HALF_OPEN → 试探成功 → CLOSED。"""
        breaker = CircuitBreakerLLM(FlakyLLM(fail_times=3), threshold=3, cooldown_seconds=0.2)
        for _ in range(3):
            with __import__("pytest").raises(RuntimeError):
                breaker.generate_proposal("p")
        assert breaker.state == "open"

        time.sleep(0.3)  # 冷却期满
        assert breaker.health_check() is True  # half_open
        assert breaker.state == "half_open"
        # 试探成功 → CLOSED
        assert breaker.generate_proposal("p") == "ok"
        assert breaker.state == "closed"

    def test_half_open_failure_reopens(self):
        """HALF_OPEN 试探失败 → 重新 OPEN。"""
        breaker = CircuitBreakerLLM(FlakyLLM(fail_times=10), threshold=1, cooldown_seconds=0.1)
        with __import__("pytest").raises(RuntimeError):
            breaker.generate_proposal("p")
        assert breaker.state == "open"

        time.sleep(0.15)
        assert breaker.health_check() is True  # 懒转换：调用触发状态迁移
        assert breaker.state == "half_open"
        with __import__("pytest").raises(RuntimeError):
            breaker.generate_proposal("p")  # 试探失败
        assert breaker.state == "open"

    def test_success_resets_failure_count(self):
        """成功重置失败计数：2 失败 + 1 成功 + 2 失败 → 不熔断（阈值 3）。"""
        llm = FlakyLLM(fail_times=2)
        breaker = CircuitBreakerLLM(llm, threshold=3, cooldown_seconds=60)
        for _ in range(2):
            with __import__("pytest").raises(RuntimeError):
                breaker.generate_proposal("p")
        assert breaker.state == "closed"
        assert breaker.generate_proposal("p") == "ok"  # 成功 → 重置

        # 再失败 2 次（累计未达阈值 3）
        llm.fail_times = 99
        for _ in range(2):
            with __import__("pytest").raises(RuntimeError):
                breaker.generate_proposal("p")
        assert breaker.state == "closed"

    def test_non_llm_errors_not_counted(self):
        """非 LLM 故障（如 TypeError）不计入熔断。"""
        class TypeErrorLLM(ILLMClient):
            def generate_proposal(self, prompt):
                raise TypeError("bad json")

        breaker = CircuitBreakerLLM(TypeErrorLLM(), threshold=2, cooldown_seconds=60)
        for _ in range(3):
            with __import__("pytest").raises(TypeError):
                breaker.generate_proposal("p")
        assert breaker.state == "closed"

    def test_timeout_rate_signal(self):
        """滑动窗口失败率作为降级信号。"""
        breaker = CircuitBreakerLLM(FlakyLLM(fail_times=5), threshold=100, cooldown_seconds=60)
        for _ in range(5):
            with __import__("pytest").raises(RuntimeError):
                breaker.generate_proposal("p")
        # 5 次全失败 → rate 1.0
        assert breaker.timeout_rate() == 1.0
        llm = breaker._wrapped
        llm.fail_times = 0
        breaker.generate_proposal("p")
        # 5 失败 + 1 成功 → 5/6
        assert abs(breaker.timeout_rate() - 5 / 6) < 0.01
