"""自动降级控制器（FR-1.3）。

风暴检测 + 三档降级（NORMAL / L1 / L2）+ 迟滞防抖 + 手动覆盖 + 审计：

检测信号（refresh()，消费循环每轮调用）:
  - kafka_lag        消费滞后（alert_source.lag()）
  - worker_backlog   队列深度 + 风暴暂存深度
  - llm_timeout_rate LLM 60s 窗口失败占比（熔断器共享）

档位:
  - L1: lag 超阈值或 timeout_rate > 0.3 → rule_engine 命中直接执行（现有快路径），
        LLM 诊断只对 Critical/Warning 组代表
  - L2: L1 持续超 AIOPS_DEGRADE_L2_AFTER_SECONDS 且 lag 仍超，或 timeout_rate > 0.6
        → 只处理 Critical；LLM 关迭代修正（k=1）；verify 失败不回环诊断
  - 恢复: 信号连续 3 个周期低于恢复阈值 → 逐级回退 L2→L1→NORMAL（规格 4.2 自动恢复）
  - 手动覆盖: forced_level 非空时强制（FR-7 热开关，自动检测让位）
"""

import os
import threading
import time
from typing import Any, Optional

from core.logger import get_logger
from core.observability import metrics

logger = get_logger("degradation")

# 档位常量
NORMAL = "NORMAL"
L1 = "L1"
L2 = "L2"

_LEVEL_ORDER = {NORMAL: 0, L1: 1, L2: 2}


class DegradationController:
    """风暴检测 + 三档降级状态机。线程安全。"""

    def __init__(
        self,
        alert_source: Optional[Any] = None,
        queue: Optional[Any] = None,
        breaker: Optional[Any] = None,
        emitter: Optional[Any] = None,
        config_provider: Optional[Any] = None,
    ):
        self._alert_source = alert_source
        self._queue = queue
        self._breaker = breaker
        self._emitter = emitter
        self._config_provider = config_provider  # FR-7: 热开关（degradation.forced_level）
        self.level = NORMAL
        self._forced_level: str = ""          # 手动覆盖（FR-7 热开关）
        self._level_since = time.time()       # 当前档位起始时间
        self._recovery_streak = 0             # 连续低信号周期数（迟滞恢复）
        self._lock = threading.Lock()

    # ── 信号采集 ──

    def signals(self) -> dict:
        """采集当前信号快照。"""
        lag = -1
        if self._alert_source is not None and hasattr(self._alert_source, "lag"):
            try:
                lag = self._alert_source.lag()
            except Exception:
                lag = -1
        backlog = 0
        if self._queue is not None:
            backlog = self._queue.qsize() + self._queue.storm_parked_size()
        timeout_rate = self._breaker.timeout_rate() if self._breaker is not None else 0.0
        return {"kafka_lag": lag, "worker_backlog": backlog, "llm_timeout_rate": timeout_rate}

    def _lag_threshold(self) -> int:
        return int(os.getenv("AIOPS_DEGRADE_LAG_THRESHOLD", "500"))

    def _lag_recover(self) -> int:
        return int(os.getenv("AIOPS_DEGRADE_LAG_RECOVER", "100"))

    def _timeout_rate_threshold(self) -> float:
        return float(os.getenv("AIOPS_DEGRADE_TIMEOUT_RATE", "0.3"))

    def _timeout_rate_l2(self) -> float:
        return float(os.getenv("AIOPS_DEGRADE_TIMEOUT_RATE_L2", "0.6"))

    def _l2_after_seconds(self) -> int:
        return int(os.getenv("AIOPS_DEGRADE_L2_AFTER_SECONDS", "120"))

    # ── 档位评估（迟滞）──

    def refresh(self) -> str:
        """采集信号并评估档位；档位变化时审计 + 指标。返回当前档位。"""
        with self._lock:
            # FR-7: 热开关 —— runtime_config 表的 degradation.forced_level 优先
            if self._config_provider is not None:
                try:
                    provider_forced = str(
                        self._config_provider.get("degradation.forced_level", "") or ""
                    ).strip().upper()
                    if provider_forced != self._forced_level:
                        self._forced_level = provider_forced
                        if provider_forced:
                            logger.logger.info("[降级] 热开关生效: forced={}", provider_forced)
                except Exception:
                    pass

            sig = self.signals()
            lag = sig["kafka_lag"]
            rate = sig["llm_timeout_rate"]
            now = time.time()

            effective = self._forced_level or self.level
            order = _LEVEL_ORDER[effective]

            if self._forced_level:
                # 手动覆盖：仅审计一次进入
                if self.level != effective:
                    self._enter(effective, now, manual=True)
                metrics.gauge("aiops_degradation_level", _LEVEL_ORDER[effective])
                return effective

            degraded = (lag >= 0 and lag >= self._lag_threshold()) or rate >= self._timeout_rate_threshold()
            severe = (lag >= 0 and lag >= self._lag_threshold() and rate >= self._timeout_rate_l2()) or \
                     rate >= self._timeout_rate_l2()
            recovered = (lag < 0 or lag < self._lag_recover()) and rate < self._timeout_rate_threshold() * 0.5

            if severe and order < 2:
                self._enter(L2, now)
            elif degraded and order < 1:
                self._enter(L1, now)
            elif not degraded:
                if recovered:
                    self._recovery_streak += 1
                else:
                    self._recovery_streak = 0
                # 连续 3 个低信号周期 → 逐级回退
                if self._recovery_streak >= 3 and order > 0:
                    nxt = L1 if effective == L2 else NORMAL
                    self._exit(nxt, now)
                    self._recovery_streak = 0

            # L2 升级条件：L1 持续超时且 lag 仍超
            if effective == L1 and (now - self._level_since) > self._l2_after_seconds() \
                    and lag >= 0 and lag >= self._lag_threshold():
                self._enter(L2, now)

            current = self._forced_level or self.level
            metrics.gauge("aiops_degradation_level", _LEVEL_ORDER[current])
            metrics.gauge("aiops_degradation_kafka_lag", max(0, lag))
            return current

    def _enter(self, level: str, now: float, manual: bool = False):
        prev, self.level = self.level, level
        self._level_since = now
        self._recovery_streak = 0
        metrics.incr(f"aiops_degradation_entered.{level.lower()}")
        logger.logger.warning(
            "[降级] {} → {}{} | 原因={}", prev, level,
            "（手动）" if manual else "（自动）", self.signals(),
        )
        if self._emitter:
            self._emitter.emit(
                "degradation.entered",
                actor="manual" if manual else "system",
                severity="warning",
                outcome="success",
                payload={"from": prev, "to": level, **self.signals()},
            )

    def _exit(self, level: str, now: float):
        prev, self.level = self.level, level
        self._level_since = now
        metrics.incr(f"aiops_degradation_exited.{level.lower()}")
        logger.logger.info("[降级] {} → {}（恢复）", prev, level)
        if self._emitter:
            self._emitter.emit(
                "degradation.exited",
                severity="info",
                outcome="success",
                payload={"from": prev, "to": level},
            )

    # ── 手动覆盖（FR-7 热开关）──

    def force_level(self, level: str):
        """手动强制降级档（"" 恢复自动检测）。"""
        if level not in ("", NORMAL, L1, L2):
            raise ValueError(f"非法降级档: {level}")
        with self._lock:
            self._forced_level = level
            if level and level != self.level:
                self._enter(level, time.time(), manual=True)
            elif not level and self.level != NORMAL:
                self._exit(NORMAL, time.time())

    # ── 档位行为查询（节点接线）──

    def is_l2(self) -> bool:
        return (self._forced_level or self.level) == L2

    def is_l1_or_higher(self) -> bool:
        return _LEVEL_ORDER[self._forced_level or self.level] >= 1

    def allows_level(self, alert_level: str) -> bool:
        """L2 档只处理 Critical（过滤节点用）。"""
        if not self.is_l2():
            return True
        return alert_level == "Critical"
