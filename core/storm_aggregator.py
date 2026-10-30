"""告警风暴聚合器（FR-1.2）。

在 batch 之后、process 之前按根因签名聚类：同签名合并为一个 Incident，
只诊断一次（组代表），结果广播到组内全部成员。

签名 = 意图类别（AlertIntentClassifier，零 LLM）+ 主机（_extract_host）+ 关键实体
（标题去数字后的实体指纹，"使用率 95%/96%" 归同一签名）。

时序设计（不拖慢正常路径）：
  - 新签名 → 立即创建组，首条告警为代表 → 本轮即处理（正常流量零附加延迟）
  - 窗口内同签名晚到 → 入组为成员；代表结果已出 → 立即广播给晚到成员
  - 窗口过期 → 组销毁（后续同签名 → 新 Incident）

GroupStore: M1 内存实现（实例内）；M2 换 Redis 实现跨实例共享。
"""

import hashlib
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.logger import get_logger
from core.models import Alert
from core.observability import metrics

logger = get_logger("storm_aggregator")

_LEVEL_ORDER = {"Critical": 2, "Warning": 1, "Info": 0}

_DIGITS = re.compile(r"\d+")


def storm_signature(alert: Alert, intent_category: str, host: str) -> str:
    """根因签名：意图类别 + 主机 + 关键实体（标题去数字）。"""
    entity = _DIGITS.sub("#", (alert.title or "").strip().lower())[:120]
    return hashlib.md5(f"{intent_category}|{host}|{entity}".encode("utf-8")).hexdigest()


@dataclass
class StormIncident:
    """一次聚合 incident：一个签名一组，代表只诊断一次。"""
    group_id: str
    signature: str
    intent_category: str
    host: str
    created_at: float = field(default_factory=time.time)
    representative: Optional[Alert] = None
    members: List[Alert] = field(default_factory=list)
    broadcast_ids: set = field(default_factory=set)   # 已广播结果的 alert_id
    result: Optional[Dict[str, Any]] = None           # 代表的 run_pipeline 最终 state
    closed: bool = False                              # 组满后关闭（后续同签名开新组）

    @property
    def ticket_id(self) -> str:
        return (self.result or {}).get("ticket_id") or ""

    @property
    def execution_success(self) -> Optional[bool]:
        return (self.result or {}).get("execution_success")


class InMemoryGroupStore:
    """进程内组存储（M1）。线程安全。"""

    def __init__(self, window_seconds: int = 30):
        self._window = max(1, window_seconds)
        self._groups: Dict[str, StormIncident] = {}   # signature -> open group
        self._lock = threading.Lock()

    def get_open(self, signature: str) -> Optional[StormIncident]:
        with self._lock:
            group = self._groups.get(signature)
            if group is None or group.closed:
                return None
            return group

    def create(self, signature: str, intent_category: str, host: str) -> StormIncident:
        with self._lock:
            group = StormIncident(
                group_id=f"inc-{uuid.uuid4().hex[:12]}",
                signature=signature,
                intent_category=intent_category,
                host=host,
            )
            self._groups[signature] = group
            return group

    def expire(self, now: Optional[float] = None) -> int:
        """销毁已关闭或过窗口的组；返回销毁数。"""
        now = now or time.time()
        with self._lock:
            expired_sigs = [
                sig for sig, g in self._groups.items()
                if g.closed or (now - g.created_at) > self._window
            ]
            for sig in expired_sigs:
                del self._groups[sig]
            return len(expired_sigs)

    def open_count(self) -> int:
        with self._lock:
            return len(self._groups)


class StormAggregator:
    """风暴聚合器：签名聚类 + 组管理。"""

    def __init__(
        self,
        intent_classifier,
        extract_host_fn,
        group_store: Optional[Any] = None,
        window_seconds: Optional[int] = None,
        max_group: Optional[int] = None,
    ):
        self._intent_classifier = intent_classifier
        self._extract_host = extract_host_fn
        self._window = max(1, window_seconds if window_seconds is not None else int(
            os.getenv("AIOPS_AGG_WINDOW_SECONDS", "30")))
        self._max_group = max(1, max_group if max_group is not None else int(
            os.getenv("AIOPS_AGG_MAX_GROUP", "200")))
        self._store = group_store or InMemoryGroupStore(window_seconds=self._window)

    @property
    def window_seconds(self) -> int:
        return self._window

    def aggregate(self, alerts: List[Alert]) -> List[StormIncident]:
        """把告警归组，返回本轮有工作要做的 incident 列表。

        - 新签名：创建组，首条为代表 → 返回（本轮处理）
        - 同签名晚到：入组为成员；代表结果已出且未广播 → 返回（本轮广播）
        - 组满：关闭组，后续同签名开新组
        """
        self._store.expire()
        work: List[StormIncident] = []
        seen_in_batch: Dict[str, StormIncident] = {}

        for alert in alerts:
            intent = self._intent_classifier.classify(alert)
            host = self._extract_host(alert)
            sig = storm_signature(alert, intent.category, host)

            incident = seen_in_batch.get(sig) or self._store.get_open(sig)
            if incident is None:
                incident = self._store.create(sig, intent.category, host)
                incident.representative = alert
                incident.members.append(alert)
                seen_in_batch[sig] = incident
                metrics.incr("aiops_aggregator_groups_total")
                work.append(incident)
            else:
                incident.members.append(alert)
                seen_in_batch.setdefault(sig, incident)
                if incident.result is not None and alert.alert_id not in incident.broadcast_ids:
                    # 代表结果已出，晚到成员待广播
                    work.append(incident)
                metrics.incr("aiops_aggregator_members_total")

            if len(incident.members) >= self._max_group:
                incident.closed = True  # 组满：后续同签名开新组
                metrics.incr("aiops_aggregator_group_closed_full")

        total_members = sum(len(i.members) for i in seen_in_batch.values())
        if total_members > 0:
            metrics.gauge(
                "aiops_aggregator_convergence_ratio",
                round(total_members / max(1, len(seen_in_batch)), 2),
            )
        return work

    def broadcast(self, incident: StormIncident, producer=None, commit_cb=None) -> int:
        """把代表结果广播到组内未广播的成员；返回本次广播数。

        broadcast_cb(member_alert, incident) -> None：成员级处理回调
        （mark_processed / 结果发布），由调用方注入。
        """
        broadcasted = 0
        for member in list(incident.members):
            if member.alert_id in incident.broadcast_ids:
                continue
            incident.broadcast_ids.add(member.alert_id)
            broadcasted += 1
            if commit_cb:
                try:
                    commit_cb(member, incident)
                except Exception as exc:
                    logger.logger.warning(
                        "[聚合] 成员广播回调失败 | alert_id={} | error={}", member.alert_id, exc,
                    )
        if broadcasted:
            metrics.incr("aiops_aggregator_broadcast_total", broadcasted)
        return broadcasted

    def open_count(self) -> int:
        return self._store.open_count()
