"""LLM 熔断器（FR-1.3 / NFR 4.4 可靠性）。

连续 N 次超时/5xx → OPEN 冷却 60s → HALF_OPEN 试探：
- CLOSED:  正常放行；失败连续计数
- OPEN:    快速失败（抛 CircuitOpenError）→ 调用方走既有降级路径（规则快路径/工单），
           不打爆网关；冷却期满 → HALF_OPEN
- HALF_OPEN: 放行一个试探请求；成功 → CLOSED，失败 → 重新 OPEN

同时暴露 timeout_rate()（60s 滑动窗口失败占比）作为降级控制器的信号。
"""

import os
import threading
import time
from collections import deque
from typing import Optional

from core.audit import AuditEmitter
from core.observability import metrics

_WINDOW_SECONDS = 60.0

# LLMO-3.2: 熔断状态审计发射器（模块级懒创建，无 store → loguru 落地）
_audit_emitter: Optional[AuditEmitter] = None


def _get_audit_emitter() -> AuditEmitter:
    global _audit_emitter
    if _audit_emitter is None:
        from core.audit import get_audit_store
        _audit_emitter = AuditEmitter(store=get_audit_store())
    return _audit_emitter


def _audit_emit(event_type: str, severity: str, detail: str) -> None:
    """LLMO-3.2: 熔断状态变化写审计事件；观测失败不影响熔断主流程。"""
    try:
        _get_audit_emitter().emit(
            event_type, category="operational", severity=severity,
            payload={"detail": detail},
        )
    except Exception:
        pass


class CircuitOpenError(RuntimeError):
    """熔断开启期间的快速失败异常（调用方走降级路径）。"""

    def __init__(self, retry_after: float):
        super().__init__(f"circuit open, retry after {retry_after:.0f}s")
        self.retry_after = retry_after


def _is_llm_failure(exc: Exception) -> bool:
    """判断是否计入熔断的 LLM 故障（超时/连接错误/5xx/429）。"""
    try:
        import httpx
        if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError)):
            return True
    except ImportError:
        pass
    if isinstance(exc, CircuitOpenError):
        return False  # 熔断自身异常不重复计数
    if isinstance(exc, RuntimeError):
        msg = str(exc)
        return any(code in msg for code in ("503", "502", "504", "429"))
    return False


class CircuitBreakerLLM:
    """LLM 客户端熔断包装器（保持 ILLMClient.generate_proposal 签名）。"""

    def __init__(self, wrapped, threshold: Optional[int] = None, cooldown_seconds: Optional[float] = None):
        self._wrapped = wrapped
        self._threshold = max(1, threshold if threshold is not None else int(
            os.getenv("AIOPS_CIRCUIT_BREAK_THRESHOLD", "5")))
        self._cooldown = cooldown_seconds if cooldown_seconds is not None else float(
            os.getenv("AIOPS_CIRCUIT_BREAK_COOLDOWN_SECONDS", "60"))
        self._state = "closed"  # closed | open | half_open
        self._failures = 0
        self._opened_at = 0.0
        self._probe_inflight = False  # HALF_OPEN 单试探在途标志（防探针风暴）
        self._window: deque = deque()  # (timestamp, is_failure) 滑动窗口
        self._lock = threading.Lock()

    # ── ILLMClient ──

    def generate_proposal(self, prompt: str) -> str:
        with self._lock:
            self._transition_on_call()
            if self._state == "open":
                retry_after = max(0.0, self._cooldown - (time.time() - self._opened_at))
                metrics.incr("pipeline.llm.circuit.rejected")
                raise CircuitOpenError(retry_after)
            if self._state == "half_open":
                # 单试探护栏：HALF_OPEN 已有试探在途时快速失败，防并发探针风暴
                if self._probe_inflight:
                    metrics.incr("pipeline.llm.circuit.rejected")
                    raise CircuitOpenError(1.0)
                self._probe_inflight = True

        try:
            result = self._wrapped.generate_proposal(prompt)
        except Exception as exc:
            self._record_outcome(ok=False, exc=exc)
            raise
        self._record_outcome(ok=True)
        return result

    def health_check(self) -> bool:
        """熔断开启时视为不健康（调用方走降级）。"""
        with self._lock:
            self._transition_on_call()
            return self._state != "open"

    # ── 状态机 ──

    def _transition_on_call(self):
        now = time.time()
        if self._state == "open" and (now - self._opened_at) >= self._cooldown:
            self._state = "half_open"
            metrics.incr("pipeline.llm.circuit.half_open")

    def _record_outcome(self, ok: bool, exc: Optional[Exception] = None):
        now = time.time()
        with self._lock:
            # 无论结果如何，试探已结束（含非熔断类异常），释放单试探位
            self._probe_inflight = False
            self._window.append((now, not ok))
            self._prune_window(now)
            if ok:
                if self._state == "half_open":
                    self._state = "closed"
                    metrics.incr("pipeline.llm.circuit.closed")
                    # LLMO-3.2: 恢复状态持久化事件
                    _audit_emit(
                        "llm.circuit_recovered", "info",
                        "LLM 熔断恢复（half_open 试探成功 → closed）",
                    )
                self._failures = 0
                return
            is_failure = _is_llm_failure(exc) if exc is not None else True
            if not is_failure:
                return
            self._failures += 1
            metrics.incr("pipeline.llm.circuit.failure")
            if self._state == "half_open" or self._failures >= self._threshold:
                self._state = "open"
                self._opened_at = now
                metrics.incr("pipeline.llm.circuit.opened")
                # LLMO-3.2: 开启状态持久化事件
                _audit_emit(
                    "llm.circuit_opened", "warning",
                    f"LLM 熔断开启（连续失败 {self._failures} 次，冷却 {self._cooldown:.0f}s）",
                )

    def _prune_window(self, now: float):
        while self._window and (now - self._window[0][0]) > _WINDOW_SECONDS:
            self._window.popleft()

    # ── 降级信号 ──

    def timeout_rate(self) -> float:
        """60s 滑动窗口内 LLM 失败占比（0.0~1.0）。"""
        now = time.time()
        with self._lock:
            self._prune_window(now)
            if not self._window:
                return 0.0
            failures = sum(1 for _, bad in self._window if bad)
            return round(failures / len(self._window), 4)

    @property
    def state(self) -> str:
        return self._state
